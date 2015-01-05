/*
**
* BEGIN_COPYRIGHT
*
* This file is part of SciDB.
* Copyright (C) 2008-2014 SciDB, Inc.
*
* SciDB is free software: you can redistribute it and/or modify
* it under the terms of the AFFERO GNU General Public License as published by
* the Free Software Foundation.
*
* SciDB is distributed "AS-IS" AND WITHOUT ANY WARRANTY OF ANY KIND,
* INCLUDING ANY IMPLIED WARRANTY OF MERCHANTABILITY,
* NON-INFRINGEMENT, OR FITNESS FOR A PARTICULAR PURPOSE. See
* the AFFERO GNU General Public License for the complete license terms.
*
* You should have received a copy of the AFFERO GNU General Public License
* along with SciDB.  If not, see <http://www.gnu.org/licenses/agpl-3.0.html>
*
* END_COPYRIGHT
*/
// About:
//
//   This file contains the implementation of the methods making up the 
//   NumericOperations class. The idea is to provide an object that implements 
//   a number of common, useful numeric operations.  
//
//   They're either a) taken from "Numerical Recipes in 'C'", and adapted to 
//   our needs, or else they're implemented based on other examples we found on 
//   the web. None of this is in any way origonal. 
//
//   NOTES:
//
//  1. Implements a set of numeric functions such; gamma, gamma-log, beta, 
//     factorial, factorial-log, and the binomial coeffcient. 
//
//  2. Implements a uniform distribution random number generator which is both 
//     fast and 'good enough' for most purposes. This uniform random number 
//     generator is then used in the generation of other random, non-uniform 
//     distributions, such as the exponential, normal (or gaussian), gamma, 
//     poisson and binomial. 
//
//  3. Internally, the class maintains several values which help either in the 
//     question of function--for example, we maintain an array of random 
//     values allocated by the uniform() method and "shuffle" them to return a 
//     value on a particular call--or else for efficiency--the gaussian 
//     distribution generator actually generates two values for each call, so 
//     we keep one of them, only computing a pair of values every other call. 
//
//  4. For non-uniform deviates (Exponential, Normal (Gaussian), Gamma, 
//     Poisson, Binomial) we build upon this uniform deviate method. The idea 
//     is either to transform the uniform deviate, or else to use what is 
//     known as the 'rejection method', where the idea is that each time we 
//     want to generate a random value according to a non-uniform distribution,
//     we first pick a uniform random variable (over, say [0.0, 1.0) ) and 
//     then test that variable to see if it also complies with the non-uniform 
//     distribution. If it fails the inclusion test, we try again with a new 
//     random value. 
//
//  5. All of these random variants are 'standard': that is, they do not 
//     include the 'location', and 'scale' arguments, which are typically 
//     needed by any particular application. 
//
#include "NumericOps.h"

//
//  Constructor:
//
//   The basic idea of this class is to use a good 'uniform deviate' random 
//   number generator as the basis for a set of random number generator for 
//   non-uniform distributions. 
//
//   To make the uniform deviate work well, we need to initialize an array and 
//   place into that array a set of random values. Then, at run-time, we pick 
//   a value from that array (and replace it) more or less at random. The idea 
//   is that the 'simple' generator of uniform deviates has a potential problem 
//   relating to correlations in the values of subsequent random values. Using 
//   the array and 'shuffling' its values eliminates this problem. 
//
//   Hence: The constructor method populates the shuffle array. Then the 
//   uniform() method grabs values from the array, replacing them as it 
//   proceeds. 
//
NumericOperations::NumericOperations ( const int nSeedArg ) 
{
	int	nI;

	dbOldP = dbOldMean = -1.0;
	nOldTrials     =   -1;
	nGPrev    =    0;

	//
	//  Plonk zeroes into the Factorial Table. As the factorial function is 
	//  called, it will populate this table with the results it has already 
	//  computed, if the value already in this table is zero. That is, each 
	//  calculation of a factorial-log or a factorial is done once for up 
	//  to FACT_ARRAY_LEN. 
	//
	for(nI=0;nI<FACT_ARRAY_LEN;nI++)
		dbarFactLn[nI]=0.0;
	//
	//  Populate the gamma log coefficients table.
	//
	dbCoef[0] = 76.18009172947146;
	dbCoef[1] = -86.50532032941677; 
	dbCoef[2] = 24.01409824083091;
	dbCoef[3] = -1.231739572450155; 
	dbCoef[4] = 0.1208650973866179e-2;
	dbCoef[5] = -0.5395239384953e-5;
	//
	// Populate the random number generator's a[], and m[] arrays. 
	//
	a[0]      =  45991;
	a[1]      = 207707;
	a[2]      = 138556;
	a[3]      =  49689;
	//
	// Populate the 
	//
	m[0]  = 2147483647;
	m[1]  = 2147483543; 
	m[2]  = 2147483423; 
	m[3]  = 2147483323;

	ResetSeed ( nSeedArg );
}
//
//  NumericOperations::ResetSeed() - reset the seed of the generator. 
//
//	 For a given seed, the generator produces the same sequence of values. 
//  It might be useful, from time to time, to be able to reset the seed of the 
//  generator either to generate a new set of random values, or else to replay 
//  the same ones again. 
//
//	This method accomplishes a reset.
//
void
NumericOperations::ResetSeed ( const int nSeedArg )
{ 
	int	j,k;

	if (0 > nSeedArg) 
		nSeed = -1 * nSeedArg;
	else if (0 == nSeedArg) 
		nSeed = 1;
	else
		nSeed = nSeedArg;

	for (j=SHUFFLE_ARRAY_SIZE+7;j >= 0;j--) {
		k=nSeed/SCHRAGE_Q;
		nSeed=MULTIPLIER*(nSeed-k*SCHRAGE_Q)-SCHRAGE_R*k;
		if (nSeed < 0)
			nSeed += INT_MAX;
		if (j < SHUFFLE_ARRAY_SIZE)
			narShuffle[j] = nSeed;
	}

	this->nLastVal = narShuffle[0];

	InitRG(nSeedArg,nSeedArg*113);
}
//
//  NumericOperations::gammaln(const double) - ln(gamma(xx))
//
//  The Gamma Function is widely used in the kinds of numerical operations we 
//  are struggling with in this class. As we usually do, we defer to the good 
//  people at Numerical Necipes in 'C' for our design, which is due to 
//  Lanczos (apparently). 
//
//  When dealing with integers, the Gamma Function is just the factorial 
//  function, offset by one.  That is; 
//     n! = gamma ( n + 1 ); 
//
//  However, the Gamma function extends over the domain of reals (and complex 
//  numbers, according to Mathematica, but that concept just gives me a 
//  headache.) It turns out that conmputing the natural log of the gamma 
//  function is both easier, and yields a result that can be used in a variety 
//  of other operations. 
//
double
NumericOperations::gammaln(const double xx)
{
	double	x,y,tmp,ser = 1.000000000190015;
	int	j;

	y=x=xx;

	tmp=x+5.5;

	tmp -= (x+0.5)*log(tmp);

	for (j=0;j<=5;j++) 
		ser += dbCoef[j]/++y;

	return -tmp+log(2.5066282746310005*ser/x);
}
//
//  NumericOperations::beta ( const double, const double ) - 
//                                             beta function B(z,w)
//
//  The beta function (AKA Euler's First Integral) is;
//
//    Beta (z, w) = Beta (w, z) = Gamma( z ) . Gamma ( w ) / Gamma ( z + w ) 
//
//   So we can compute it from the gammaln() function as follows. (Remember how 
//   we said that gamma-log was often more useful than just gamma?)
//
double
NumericOperations::beta( const double z, const double w)
{
	return exp(gammaln(z)+gammaln(w)-gammaln(z+w));
}
//
//  Continued function evaluation. 
//
//  This method is used in the incomplete beta function, incbeta(). 
//
#define MAX_ITERATIONS 100

inline double
NumericOperations::betacf(const double z, const double a, const double b )
{
    int m, m2;
    double  aa,c,d,del,h,qab,qam,qap;

    qab=a+b;
    qam=a-1.0;
	qap=a+1.0;
    c=1.0;
    d=1.0-qab*z/qap;
    if (fabs(d) < DBL_MIN) 
        d=DBL_MIN;
    d=1.0/d;
    h=d;
    for (m=1;m<=MAX_ITERATIONS;m++) {
        m2=2*m;
        aa=m*(b-m)*z/((qam+m2)*(a+m2));
        d=1.0+aa*d;
        if (fabs(d) < DBL_MIN) 
            d=DBL_MIN;
        c=1.0+aa/c;
        if (fabs(c) < DBL_MIN) 
            c=DBL_MIN;
        d=1.0/d;
        h *= d*c;
        aa = -(a+m)*(qab+m)*z/((a+m2)*(qap+m2));
        d=1.0+aa*d;
        if (fabs(d) < DBL_MIN) 
            d=DBL_MIN;
        c=1.0+aa/c;
        if (fabs(c) < DBL_MIN) 
            c=DBL_MIN;
        d=1.0/d;
        del=d*c;
        h *= del;
        if (fabs(del-1.0) < DBL_EPSILON) 
            break;
    }
    if (m > MAX_ITERATIONS) 
		seterr ( NUMERIC_ERR_STATE_INC_BETA_ITER_OUT );
    return h;
}
//
//  NumericOperations::incbeta (z, a, b) - incomplete beta function. 
//
//  Returns the incomplete beta function Iz(a, b).
// 
double
NumericOperations::incbeta(const double z, const double a, const double b)
{

	double	bt;
	if (z < 0.0 || z > 1.0)
		seterr ( NUMERIC_ERR_STATE_BAD_ARGS );
	else { 

		if (z == 0.0 || z == 1.0) 
			bt=0.0;
		else 
			bt=exp(gammaln(a+b)-gammaln(a)-gammaln(b)+a*log(z)+b*log(1.0-z));

		if (z < (a+1.0)/(a+b+2.0))
			return bt*betacf(a,b,z)/a;
		else
			return 1.0-bt*betacf(b,a,1.0-z)/b;
	}
	return 0.0;
}
//

//  NumericOperations::gammaser(double *, double, double, double *)  - 
//  							gamma series
//
void
NumericOperations::gammaser ( double * gamser, const double a, 
							  const double x, double *gln )
{
	int n;
    double  sum,del,ap;

    *gln=gammaln(a);

    if (x <= 0.0) {
        if (x < 0.0) 
			seterr( NUMERIC_ERR_STATE_BAD_ARGS );
        *gamser=0.0;
        return;
    } else {
        ap=a;
        del=sum=1.0/a;
        for (n=1;n<=SER_ITER_MAX;n++) {
            ++ap;
            del *= x/ap;
            sum += del;
            if (fabs(del) < fabs(sum)*DBL_EPSILON) {
                *gamser=sum*exp(-x+a*log(x)-(*gln));
                return;
            }
        }
		seterr(NUMERIC_ERR_STATE_GSER_A_TOO_LARGE);
        return;
    }
}
//
// NumericOperations::gammacf ( double *, double, double, double *)  - gamma series
//
// Returns the incomplete gamma function Q(a, x) evaluated by its continued fraction 
// representation as gammacf. Also returns ln(a) as gln.
//
void 
NumericOperations::gammacf( double *gammcf, const double a, 
			    		    const double x, double *gln )
{
    int  i;
    double    an,b,c,d,del,h;

    *gln=gammaln(a);
    b=x+1.0-a;
    c=1.0/DBL_MIN;
    d=1.0/b;
    h=d;
    for (i=1;i<=SER_ITER_MAX;i++) {
        an = -i*(i-a);
        b += 2.0;
        d=an*d+b;
        if (fabs(d) < DBL_MIN)
            d=DBL_MIN;
        c=b+an/c;
        if (fabs(c) < DBL_MIN)
            c=DBL_MIN;
        d=1.0/d;
        del=d*c;
        h *= del;
        if (fabs(del-1.0) < DBL_EPSILON)
            break;
    }
    if (i > SER_ITER_MAX)
		seterr(NUMERIC_ERR_STATE_GCF_A_TOO_LARGE);
    *gammcf=exp(-x+a*log(x)-(*gln))*h;
}
//
// NumericOperations::gammap(double, double) - incomplete gamma function P(a,x)
//
//
double
NumericOperations::gammap(const double a, const double x)
{
	double gamser,gammcf,gln;
  	if (x < 0.0 || a <= 0.0) {
		seterr(NUMERIC_ERR_STATE_GAMMA_P_BAD_ARGS);
	} else if (x < (a+1.0)) {
		gammaser(&gamser,a,x,&gln);
		return gamser;
	} else {
		gammacf(&gammcf,a,x,&gln);
		return 1.0-gammcf; 
 	}
	//
	// here to shut up compiler warnings. 
	//
	return 0.0;
}
//
// NmericOperations::gammaq(double, double) - complement of gammap()
//
// Returns the incomplete gamma function Q(a, x) =  1 -  P(a, x).
//
double
NumericOperations::gammaq(const double a, const double x)
{
        double gamser,gammcf,gln;
        if (x < 0.0 || a <= 0.0) { 
				seterr(NUMERIC_ERR_STATE_GAMMA_Q_BAD_ARGS);
        } else if (x < (a+1.0)) {
                gammaser(&gamser,a,x,&gln);
                return 1.0-gamser; 
        } else {
                gammacf(&gammcf,a,x,&gln);
                return gammcf;
        }
	//
	// here to shut up compiler warnings. 
	//
	return 0.0;
}
//
//  NumericOperations::factorialln(int) - log(arg!) 
//
//  The factorial of an integer, n, is the product of that number and every number 
//  less than it. That is: 
//  
//   n! = n. (n-1). (n-2). (n-3).  ...  2.1 
//
//  What we do here is to use the gamma-log function (see above) to compute the 
//  factorial-log. Later, we use the factorial-log to compute the factorial. 
//
//  Computing factorial-log ain't cheap or easy. so what we do here is to compute 
//  the thing only once, and to store it in a table for later use. The idea is that we 
//  want to provide fast answers for those cases where we computed the answer multiple
//  times. 
//
double
NumericOperations::factorialln( const int n)
{
	if (n <= 1) 
		return 0.0;
	else if (n <= FACT_ARRAY_LEN)
		return (0.0 != dbarFactLn[n]) ? dbarFactLn[n] : (dbarFactLn[n]=gammaln(n+1.0));
	else
		return gammaln(n+1.0);
}
//
// NumericOperations::factorial ( int ) - return arg!
//
// This is the same function as the factorialln(), only it computes the exponent of the 
// factorial before returning. 
// 
double 
NumericOperations::factorial(const int n)
{
	return floor(0.5 + exp(factorialln(n)));
}
//
//  NumericalOperations::binomialcoef ( int, int) - n!k as FLOAT
//
//  The binomial coefficient is a basic building block in combinatorial 
//  mathematics. The idea is that you want to figure out how many ways there 
//  are to (say) pick 5 cards from a deck of 52. This method does that 
//  calculation. 
//
//  binomialcoef(n,k) returns the number of ways that k values can be choosen
//  from n. 
//
//
double 
NumericOperations::binomialcoef( const int n, const int k )
{
	if (k > n) 
		return 0.0;
	else
		return floor(0.5+exp(factorialln(n)-factorialln(k)-factorialln(n-k)));
}
//
//  NumericalOperations::poissoncum( int k, int x) - cumulative 
//                                                               poisson distr
//
//  The probability that the number of Poisson Random Events occuring will be 
//  between 0 and k, given that the expected number is x. 
//
double        
NumericOperations::poissoncum ( const int k, const int x) 
{
	return gammaq((double)k, (double)x );
}
//
//  NumericOperations::chisqrdistp( const double dbCS, const int nDF) - 
//					chi-squared dist (for degrees of freedom nDF)
//
// This method computes the probability that an observed Chi^2 statistic is 
// less than the value dbCS, given nDF degrees of freedom. We also compute the 
// complement (the probability that an observed Chi^2 *exceeds* the dbCS.)
//
double
NumericOperations::chisqrdistp( const double dbCS, const int nDF)
{
	return gammap (((double)nDF)/2.0, dbCS/2.0);
}
double
NumericOperations::chisqrdistq( const double dbCS, const int nDF)
{
	return gammaq (((double)nDF)/2.0, dbCS/2.0);
}
//
//   NumericOperations::uniform() - uniform distribution random numbers.
//
//	This method is a source of random values over the range [0.0 1.0). It 
//  needs to be both fast (as it is used as a building block for a number of 
//  other methods) and it needs to produce good quality random-ness.  
//
//  According to _Numerical_Recipes_in_C_, you can make a multiplicative 
//  congruent random number generator (that is, a random number generator 
//  which works on the principle;
//
//	 Val (next) = ( MULTIPLIER * Val (prev) + CONSTANT ) MOD Max
//
//  work well enough if you are very careful about your selection of 
//  multiplier, constant and modulus. Unhappily, the best multiplier and 
//  modulus are such that is is impossible to compute the result and still 
//  stay within a 32-bit integer, so they recommend using a variation on the 
//  theme owing to one Schrage for performing the multiply. 
// 
//  They also recommend using an array of pre-computed random numbers (called 
//  here the shuffle array) to avoid correlations in values in the low-order 
//  bits. The idea is to pre-compute the shuffle array, and then, at run-time, 
//  to pick values from the array more or less at random, replacing them as 
//  we go. 
//
//   NOTE: Peter H. shipped me better code for this. See below. 
//
double
NumericOperations::uniform() 
{
	int	j,k;
	double	dbTmp;

	k=nSeed/SCHRAGE_Q;

	nSeed=MULTIPLIER*(nSeed-k*SCHRAGE_Q)-SCHRAGE_R*k; 

	if (nSeed < 0)  
		nSeed += INT_MAX;

	j=nLastVal/(1 + ((INT_MAX - 1)/SHUFFLE_ARRAY_SIZE));
	nLastVal=narShuffle[j];
	narShuffle[j] = nSeed;

	if ((dbTmp=(1.0/INT_MAX)*nLastVal) > RANDOM_MAX) 
		return RANDOM_MAX;
	else 
		return dbTmp;
}
//
// This is a slight variation on the above theme. The loop ensures that we do 
// not have the corner cases in the unform deviate: [0,1). That is, p > 0.0 
// and p < 1.0. We call it the 'no tails' uniform. 
//
double
NumericOperations::ntuniform()
{
	double	dbRetVal;
	do { 
		dbRetVal = uniform();
	} while ((0.0 == dbRetVal) || (RANDOM_MAX == dbRetVal));
	return dbRetVal;
}
//
//  Computes the dbProp quantile of a normal distribution. 
//
double
NumericOperations::qtilenorm(double prob)
{                               
    double p = 0, x = 0;
	//
	// Checks. 
	//
	
	if ((0.0 >= prob) || 
		(1.0 <= prob)) {
		//
		// It makes no sense for the p to be out of the range [0,1]. 
		//
	} else if (0.5 == prob) {
		x = 0.0;
	} else {
		p = ((0.5 <= prob) ? ( 1.0 - prob ) : ( prob ));
		if ( 1.0E-09 >= p )
			return ((0.5 >= prob ) ? ( -6.0 ) : ( 6.0 ));
		//
		// Enough! The work starts here.
		//
		x = sqrt(log(1.0/(p*p)));
		x += (((((-0.453642210148E-4) *x +(-0.0204231210245)) * x
                        +(-0.342242088547)
                ) *x -1.0) *x +(-0.322232431088)) /
        	  (((((0.0038560700634) *x +(0.103537752850)) *x
                    	+(0.531103462366)
                ) *x +(0.588581570495)) *x + (0.0993484626060)
        	  );
	}
	return ((0.5 >= prob ) ? ( -1.0 * x ) : ( x ) );
} 
//
// NumericOperations::MultModM(s,t,M) -  Returns (s*t) MOD M.  
//                                       Assumes that -M < s < M and 
//                                       -M < t < M.
//
// Taken from Pierre L'Ecuyer and Serge Cote, "Implementing a Random Number 
// Package with Splitting Facilities": ACM TOMACS, no. 1, vol. 17, pp. 98-111, 
// March, 1991.
//
int 
NumericOperations::MultModM( int s, int t, int M) 
{
  int R, S0, S1, q, qh, rh, k;

  if( s<0) s+=M;
  if( t<0) t+=M;
  if( s<H) { S0=s; R=0;}
  else {
    S1=s/H; S0=s-H*S1;
    qh=M/H; rh=M-H*qh;
    if( S1>=H) {
      S1-=H; k=t/qh; R=H*(t-k*qh)-k*rh;
      while( R<0) R+=M;
    }
    else R=0;
    if( S1!=0) {
      q=M/S1; k=t/q; R-=k*(M-S1*q);
      if( R>0) R-=M;
      R += S1*(t-k*q);
      while( R<0) R+=M;
    }
    k=R/qh; R=H*(R-k*qh)-k*rh;
    while( R<0) R+=M;
  }
  if( S0!=0) {
    q=M/S0; k=t/q; R-=k*(M-S0*q);
    if( R>0) R-=M;
    R+=(S0*(t-k*q));
    while( R<0) R+=M;
  }
  return R;
}

void        
NumericOperations::ResetSeedRG     ( unsigned g, int s[4] )
{
	if( g>MAXGEN) 
		seterr ( GENVAL_RG_OUT_OF_RANGE  );
	else { 
  		for( int j=0; j<4; j++) 
			Ig[j][g]=s[j];
  		InitGeneratorRG( g, InitialSeed);
	}
}

void        
NumericOperations::GetStateRG      ( unsigned g, int s[4] )
{
	for(int j=0; j<4; j++) s[j]=Cg[j][g];
}

void        
NumericOperations::InitGeneratorRG ( unsigned g, SeedType  st   )
{
	if( g>MAXGEN) 
		seterr ( GENVAL_RG_OUT_OF_RANGE  );
	else { 
		for( int j=0; j<4; j++) {
			switch (st) {
				case InitialSeed :
					Lg[j][g]=Ig[j][g]; 
				 break;
				case NewSeed :
					Lg[j][g]=MultModM( aw[j], Lg[j][g], m[j]); 
				 break;
				case LastSeed :
				 break;
    		}
    		Cg[j][g]=Lg[j][g];
  		}
	}
}

void        
NumericOperations::SetSeedInitRG ( int s[4] )
{
	unsigned g;
    
	for( int j=0; j<4; j++) Ig[j][0]=s[j];
		InitGeneratorRG( 0, InitialSeed);

	for( g=1; g<=MAXGEN; g++) {
		for( int j=0; j<4; j++) 
			Ig[j][g]=MultModM( avw[j], Ig[j][g-1], m[j]);
		InitGeneratorRG( g, InitialSeed );
	} 
}

void        
NumericOperations::InitRG          ( int v, int w)
{
	int    sd[4];
	int	vc, wc;

	vc=v%63;
	wc=w%67;

	sd[0]=v;
	sd[1]=v^0X55595555;
	sd[2]=w;
	sd[3]=w^0X55595555;

	for( j=0; j<4; j++) {
		for( aw[j]=a[j],i=1;i<=wc; i++) 
			aw[j]=MultModM( aw[j], aw[j], m[j]);
		for( avw[j]=aw[j],i=1; i<=vc; i++) 
			avw[j]=MultModM( avw[j], avw[j], m[j]);
	}

	SetSeedInitRG (sd);
}

double 
NumericOperations::uniformRG   	   ( unsigned g )
{
	int k,s;
	double     u=0.0;

	if( g>MAXGEN) 
		seterr ( GENVAL_RG_OUT_OF_RANGE  );
	else { 

  		s=Cg[0][g]; k=s/46693;
		s=45991*(s-k*46693)-k*25884;
		if( s<0) s+=2147483647;
		Cg[0][g]=s;
		u+=(4.65661287524579692e-10*s);

		s=Cg[1][g]; k=s/10339;
		s=207707*(s-k*10339)-k*870;
		if( s<0) s+=2147483543;
		Cg[1][g]=s;
		u-=(4.65661310075985993e-10*s);
		if( u<0) u+=1.0;

		s=Cg[2][g]; k=s/15499;
		s=138556*(s-k*15499)-k*3979;
		if( s<0.0) s+=2147483423;
		Cg[2][g]=s;
		u+=(4.65661336096842131e-10*s);
		if( u>=1.0) u-=1.0;

		s=Cg[3][g]; k=s/43218;
		s=49689*(s-k*43218)-k*24121;
		if( s<0) s+=2147483323;
		Cg[3][g]=s;
		u-=(4.65661357780891134e-10*s);
		if( u<0) u+=1.0;
	}
	return (u);
} 
//
// NumericOperations::exponentialdev() - random values, exponential distribution
//
//  This method returns random values that follow an exponential distribution 
//  of unit (1.0) mean. 
//
double 
NumericOperations::exponentialdev()
{
	double dbRan;
	do
	{
		dbRan=uniform();
	} while (0.0 == dbRan);
	return -log(dbRan);
}
//
//   RandomNumerGenerator::gammadev(nWait) - random values, gamma distribution
//
//   The gamma distribution of integer order (nWait) is the waiting time to the 
//  nWait-th event in a Poisson random process of unit (1.0) mean. This method 
//  computes the 'standard' Gamma deviate: that is, the shape paramater is 1.0,
//  the location parameter is 0.0, and the scale parameter is 1.0. 
//
//
double
NumericOperations::gammadev( const int nWait )
{
	int j;
  	double  am,e,s,v1,v2,x,y;

 	if (6 > nWait) {
		x=1.0;
		for (j=1;j<=nWait;j++) 
			x *= uniform();
 			x = -log(x);
	} else {
		do {
			do {
				do {
					v1=uniform();
					v2=2.0*uniform()-1.0;
				} while (v1*v1+v2*v2 > 1.0);
				y=v2/v1;
				am=nWait-1;
				s=sqrt(2.0*am+1.0);
				x=s*y+am;
			} while (x <= 0.0);
		e=(1.0+y*y)*exp(am*log(x/am)-s*y);
		} while (uniform() > e);
	}
	return floor(x);
}
//
//  NumericOperations::poissondev() - random ints values, poisson dist.
//
//   The Poisson Distribution (not named after the fish) is conceptually 
//   similar to the Gamma Distribution. It reports the probability of a certain 
//   numnber of unit rate Poisson random events occur during a given interval 
//   of time. 
//
//   This method (styled after the Numerical Recipes function) reports an 
//   integer which is a random value from from a Poisson distribution of mean 
//   dbMean
//   
double
NumericOperations::poissondev(const double dbMean)
{

	static double sq,aldbMean,g,oldm=(-1.0);
	double em,t,y;

		if (dbMean < 12.0) {
				if (dbMean != oldm) {
						oldm=dbMean;
						g=exp(-dbMean);
				}
				em = -1;
				t=1.0;
				do {
						++em;
						t *= uniform();
				} while (t > g);
		} else {
				if (dbMean != oldm) {
						sq=sqrt(2.0*dbMean);
						aldbMean=log(dbMean);
						g=dbMean*aldbMean-gammaln(dbMean+1.0);
				}
				do {
						do {
								y=tan(PI*uniform());
								em=sq*y+dbMean;
						} while (em < 0.0);
						em=floor(em);
						t=0.9*(1.0+y*y)*exp(em*aldbMean-gammaln(em+1.0)-g);
				} while (uniform() > t);
		}
		return em;
}
//
// NumericOperations::binomialdev( double prob, int nTrials) 
//
//  This function computes a binomial random deviate. That is, given nTrials 
//  with probability dbProb, this function returns a random integer variable 
//  with over the range [0, nTrials) with an expected value of (dbPP * nTrials).
//
double
NumericOperations::binomialdev( const double dbPP, const int nTrials )
{
	int j;
	double  dbAMean,em,g,angle,p,bnl,sq,t,y;
 
	p=(dbPP <= 0.5 ? dbPP : 1.0-dbPP); 
	dbAMean=(double)nTrials*p; 

	if (25 >= nTrials) {
		bnl=0.0;
		for (j=1;j<=nTrials;j++)
			if (uniform() < p) ++bnl;
	} else if (dbAMean < 1.0) {
		g=exp(-dbAMean);
		t=1.0;
		for (j=0;j<=nTrials;j++) {
			t *= uniform();
			if (t < g)
				break;
		}
		bnl=(j <= nTrials ? j : nTrials);
	} else {
		if (nTrials != nOldTrials) {
			dbEn=nTrials;
			dbOldGamma=gammaln(dbEn+1.0);
			nOldTrials=nTrials;
		}
		if (p != dbOldP) {
			dbPc=1.0-p;
			dbPLog=log(p);
			dbPCLog=log(dbPc);
			dbOldP=p;
		}
		sq=sqrt(2.0*dbAMean*dbPc);
		do {
			do {
				angle=PI*uniform();
				y=tan(angle);
				em=sq*y+dbAMean;
			} while (em < 0.0 || em >= (dbEn+1.0)); 
			em=floor(em);
			t=1.2*sq*(1.0+y*y)*exp(dbOldGamma-gammaln(em+1.0)-
				gammaln(dbEn-em+1.0)+em*dbPLog+(dbEn-em)*dbPCLog);
		} while (uniform() > t);
		bnl=em;
	}

	if (p != dbPP)
		bnl=nTrials-bnl;

	return bnl;
}
//
// NumericOperations::geomdist () - random variable, geometric distribution. 
// 
// Generates a random variable that follows a geometric distribution. Given a prob 
// 'p' for some event, this function returns a (random) number of trials that would be 
// expected before the event occured. For example, tossing a (weighter) coin, or 
// rolling a '1' on a single dice, or dealing an Ace of Spades from the top of a 
// shuffled deck.
int
NumericOperations::geomdist      ( const double p )
{
    return int(floor(log(uniform()) / log ( 1 - p ) )) + 1;
}
//
// NumericOperations::gaussiansdev() - random variable, gaussian (normal) 
// 										distribution
//
//  This method returns a random variable distributed according to the normal (or 
//  Gaussian) distribution, with a mean of 0.0 and a variance of 1.0. 
double
NumericOperations::gaussiandev()
{
        double fac,rsq,v1,v2;

        if (nGPrev == 0) { 
                do {
                        v1=2.0*uniform()-1.0; 
                        v2=2.0*uniform()-1.0;
                        rsq=v1*v1+v2*v2; 
                } while (rsq >= 1.0 || rsq == 0.0); 
                fac=sqrt(-2.0*log(rsq)/rsq);
                dbGausPrev=v1*fac;
                nGPrev=1; 
                return v2*fac;
        } else { 
                nGPrev=0; 
                return dbGausPrev; 
        }
}
//
//
double 
NumericOperations::zipfdeviate ( const double x1, 
								 const double x2, 
								 const double p )
{
	double  x;
	int i;
	double  lp, r, sum;
	int V = 100000;
	double  HsubV;

	//
	// Hand calculated a number of HsubV values for 
	// the range 1 through 100000
	//
	if ((p > 0.0) && (p < 0.5))
		HsubV = 630.997;
	else if ((p >= 0.5) && (p < 0.6))
		HsubV = 248.048;
	else if ((p >= 0.6) && (p < 0.7))
		HsubV = 102.631;
	else if ((p >= 0.7) && (p < 0.8))
		HsubV = 45.5625;
	else if ((p >= 0.8) && (p < 0.9))
		HsubV = 22.1927;
	else if ((p >= 0.9) && (p < 1.0))
		HsubV = 12.0901;
	else if ((p >= 1.0) && (p < 1.005))
		HsubV = 11.7654;
	else if ((p >= 1.0005) && (p < 1.01))
		HsubV = 11.4529;
	else if ((p >= 1.01) && (p < 1.1))
		HsubV = 7.42217;
	else if ((p >= 1.1) && (p < 1.2))
		HsubV = 6.09158;
	else if ((p >= 1.2) && (p < 1.3))
		HsubV = 3.82654;
	else if ((p >= 1.3) && (p < 1.4))
		HsubV = 3.08055;
	else if ((p >= 1.4) && (p < 1.5))
		HsubV = 2.60605;
	else if ((p >= 1.5) && (p < 2.0))
		HsubV = 1.64492;
	else if ((p >= 2.0) && (p < 3.0))
		HsubV = 1.20206;
	else 
		HsubV = 1.1;
	//
	//	V = 20 + (int)(uniform() * 50.0);
	//
	// To make this a bit more 'random', we add an epsilon to the 
	// p. The point being that most folk will give us seed 
	// numbers that are 1.005 say, and repeatedly finding powers 
	// of such numbers makes a mess of things.
	//
	// lp = p + ((V%2)?(-1.0):(1.0))*DBL_EPSILON;
	// calculate the V-th harmonic number HsubV. WARNING: V>1
	// HsubV = 0.0;
	// for(i=1; i<=V; i++) 
	// 		HsubV += 1.0/pow((double)i, lp);
	//
	lp = p;
	r = uniform()*HsubV;

	sum = 1.0; 
	i=1;
	while( sum < r ){
		i++;
		sum += 1.0/pow( (double)i, lp);
	}

	// i follows Zipf distribution and lies between 1 and V
	// x lies between 0. and 1. and then between x1 and x2
	// printf("i = %d, V = 100000\n", i);

	x = ( (double)i - 1.0 ) / ( (double) V - 1.0 );
	x = (x2 - x1) * x + x1;
	return(x);
}

double
NumericOperations::zipfdeviate (const double dbA )
{
	return zipfdeviate(0.0,1.0,dbA);
}
//
// NumericOperations::GetErrString( char * ) - get error string. 
//
// If the error status of the object has been set, then this method is 
// used to retrieve the error string, and to reset the error code. 
//
int
NumericOperations::GetErrString ( char * pchErrMsgBuf )
{
	int nRetVal;

	nRetVal = nErrorState;

	if (pchErrMsgBuf) { 
		switch(nRetVal)
		{
			case NUMERIC_ERR_STATE_GAMMA_Q_BAD_ARGS:
				strcpy(pchErrMsgBuf,
						"NumericOperations::gammaq(a,x) - x < 0.0 or a <= 0.0 illegal");
			 break;
			case NUMERIC_ERR_STATE_GAMMA_P_BAD_ARGS:
				strcpy(pchErrMsgBuf,
						"NumericOperations::gammap(a,x) - x < 0.0 or a <= 0.0 illegal");
			 break;
			case NUMERIC_ERR_STATE_GCF_A_TOO_LARGE:
				strcpy(pchErrMsgBuf,
						"NumericOperations::gammacf(?,a,x,?) - a too large");
			 break;

			case NUMERIC_ERR_STATE_GSER_A_TOO_LARGE:
				strcpy(pchErrMsgBuf,
						"NumericOperations::gammaser(?,a,x,?) - a too large");
			 break;

			case NUMERIC_ERR_STATE_GSER_X_ZERO:
				strcpy(pchErrMsgBuf,
						"NumericOperations::gammaser(?,a,x,?) - x cannot be 0.0");
			 break;

			case NUMERIC_ERR_STATE_INC_BETA_ITER_OUT:
				strcpy(pchErrMsgBuf,
						"NumericOperations::betacf(z,a,b) - a or b too big, or not enough iterations to solve");
			 break;

			case NUMERIC_ERR_STATE_BAD_ARGS:
				strcpy(pchErrMsgBuf,
						"NumericOperations:: Invalid or NULL arguments");
			 break;

			case NUMERIC_ERR_STATE_OK:
				strcpy(pchErrMsgBuf,"NumericOperations: OK");
			 break;

		}
	}
	nErrorState = NUMERIC_ERR_STATE_OK;
	return nRetVal;
}
