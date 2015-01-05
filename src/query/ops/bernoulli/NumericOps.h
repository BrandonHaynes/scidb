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

//
//  About:  
//
//    This is the header file for a NumericOps class, which implements random 
//    number generators for a variety of distributions.
//    
#if !defined(__NUMERIC_OPS_HPP__)
#define __NUMERIC_OPS_HPP__

// using namespace std;

#include <math.h>
#include <stdlib.h>
#include <string.h>
#include <float.h>   
#include <limits.h> 

#define SHUFFLE_ARRAY_SIZE (32)
#define MULTIPLIER (16807)
#define SCHRAGE_Q (127773)
#define SCHRAGE_R (2836)
#define FACT_ARRAY_LEN (101)
#define PI (3.141592654)
#define SER_ITER_MAX (100)

#define RANDOM_MAX (1.0-1.2E-7)

#define MAXGEN (100)
#define H      (32768)               // 2^15 : use in MultModM

typedef enum {InitialSeed, LastSeed, NewSeed} SeedType;
//
//  These #defines are used to hold the sizes and states of various 
//  things to do with error handling on the Class. 
//
#define GENVAL_RG_OUT_OF_RANGE  -8
#define NUMERIC_ERR_STATE_INC_BETA_ITER_OUT -7
#define NUMERIC_ERR_STATE_GAMMA_Q_BAD_ARGS -6
#define NUMERIC_ERR_STATE_GAMMA_P_BAD_ARGS -5
#define NUMERIC_ERR_STATE_GCF_A_TOO_LARGE -4
#define NUMERIC_ERR_STATE_GSER_A_TOO_LARGE -3
#define NUMERIC_ERR_STATE_GSER_X_ZERO -2
#define NUMERIC_ERR_STATE_BAD_ARGS -1

#define NUMERIC_ERR_STATE_OK	1

#define NUMERIC_ERR_STRING_LEN 80

class NumericOperations
{ 

public:
	//
	// Constructors. 
	//
	NumericOperations (const int);
	~NumericOperations() {};
	//
	// Reset Seed.
	//
	void ResetSeed ( const int nSeedArg );
	//
	// These methods are useful in computing a variety of internal values 
	// used wihin this class. We also provide them as public methods. 
	//
	double 	gammaln       ( const double  );
	double 	beta	      ( const double, const double );
	//
	//  Incomplete Gamma Functions 
	//
	double	gammap	      ( const double, const double );
	double	gammaq	      ( const double, const double );
	//
	//  Incomplete beta function.
	//
	double	betacf		  ( const double z, const double a, 
								const double b);

	double    incbeta       ( const double z, const double a, 
								const double b);

	double	factorialln   ( const int );
	double	factorial     ( const int );

	double	binomialcoef  ( const int, const int );
    int	    geomdist      ( const double );

	double	poissoncum    ( const int, const int );
	double	chisqrdistp   ( const double,  const int );
	double	chisqrdistq   ( const double,  const int );
	//
	// Return random values which vary according to the respective distributions
	// and the arguments supplied.
	//
	double	uniform       ();
	double    ntuniform     ();
	//
	//  Various "table" methods.
	//
	//  Computes the dbProb quantile for the normal distribution over the 
	//	range [0,1]. 
	//
	double	qtilenorm	 	( double	dbProb );
	//
	// Methods relating to the soopa-doopa random number generator given to 
	// moi by Peter Haas. 
	//
	void		ResetSeedRG     ( unsigned g, int s[4] );
	void		GetStateRG      ( unsigned g, int s[4] );
	void		InitGeneratorRG ( unsigned g, SeedType  st   );
	void		SetSeedInitRG   ( int s[4] );
	void 		InitRG			( int v, int w);


	double	uniformRG  		( unsigned g );


	double  	exponentialdev();
	double 	gaussiandev   ();

	double	gammadev      ( const int );
	double	poissondev    ( const double  );
	double	binomialdev   ( const double, const int );

	double 	zipfdeviate   ( const double, const double, 
								const double );
	double 	zipfdeviate   ( const double );

	int	nErrorState;
	int	GetErrString ( char * );
	void		ResetError   ( );

private :
	//
	//  Set the error condition.
	//
	void		seterr	     ( const int nErr ) { nErrorState = nErr; }
	//
	//  Methods used within the public Gamma function methods 
	//  declared above. 
	//
	void		gammaser      ( double *, const double, 
					const double, double * );
	void		gammacf       ( double *, const double, 
					const double, double * );
	//
	// Used for the uniform distribution.
	//
	int	 nSeed;
	int	 nLastVal;
	int	 narShuffle[SHUFFLE_ARRAY_SIZE];
	//
	// 
	//
	int aw[4], avw[4], a[4], m[4];
	int Ig[4][MAXGEN+1], Lg[4][MAXGEN+1], Cg[4][MAXGEN+1];
	int i, j;

	inline int MultModM ( int s, int t, int M );
	//
	// Array of pre-calculated factorial values used by the factorialln()
	// method. The idea is to pre-compute a large number of these 
	// values to make the most common cases as efficient as possible. 
	//
	double	dbarFactLn[FACT_ARRAY_LEN];
	//
	// Used by the gamma-log method. This array is initiali
	//
	double	dbCoef[6];
	//
	// Used by the Poisson deviation generator.
	//
	double	dbOldMean;
	double	dbSqrt;
	double	sq,aldbMean,g;
	//
	// Used by the binomial deviation generator.
	//
	int	nOldTrials;
	double	dbOldP, dbPc, dbPLog, dbPCLog, dbEn, dbOldGamma;
	//	
	// Used by the gaussian deviation
	//
	int	nGPrev;
	double	dbGausPrev;

};

#endif
