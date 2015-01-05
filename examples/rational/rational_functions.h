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

/*
 * @file rational_functions.h
 *
 * @author paul@scidb.org
 *
 * @brief Example of the implementation of a new UDT.
 *
 *
 */

#ifndef RATIONAL_FUNCTIONS_H
#define RATIONAL_FUNCTIONS_H

enum
{
    RATIONAL_E_CANT_CONVERT_TO_RATIONAL = SCIDB_USER_ERROR_CODE_START
};

//
// Note that in this directory there is a small test harness that exercises 
// these functions, without  requiring that they be compiled and linked into 
// the engine. Having this kind of test harness is good practice. If only to 
// help us distinguish between faults in the extension, and faults in SciDB.
//

//
// For this implementation, we will just make use of the boost::rational
// template, and make it all work on pairs of 64 bit signed ints. Note 
// that we may well need to implement an arbitrary length integer at some
// point, and doing so will require rennovations to this class. 
#include <boost/rational.hpp>
#include <inttypes.h>
//
// This is the struct used to store the data inside SciDB. 
typedef struct _Rational
{
	int64_t num;
	int64_t denom;

    _Rational(int n = 0): num(n), denom(1) {}

    /** C++ operators implementation for aggregating */
    _Rational operator*(const _Rational& b) const
    {
        rational<int64_t>ra(this->num, this->denom);
        rational<int64_t>rb(b.num, b.denom);
        ra *= rb;
        _Rational r;
        r.num = ra.numerator();
        r.denom = ra.denominator();
        return r;
    }

    _Rational operator*(uint64_t v) const
    {
        rational<int64_t>ra(this->num, this->denom);
        rational<int64_t>rb(v);
        ra *= rb;
        _Rational r;
        r.num = ra.numerator();
        r.denom = ra.denominator();
        return r;
    }

    _Rational operator/(uint64_t v) const
    {
        rational<int64_t>ra(this->num, this->denom);
        rational<int64_t>rb(v);
        ra /= rb;
        _Rational r;
        r.num = ra.numerator();
        r.denom = ra.denominator();
        return r;
    }

    _Rational operator-(const _Rational& b) const
    {
        rational<int64_t>ra(this->num, this->denom);
        rational<int64_t>rb(b.num, b.denom);
        ra -= rb;
        _Rational r;
        r.num = ra.numerator();
        r.denom = ra.denominator();
        return r;
    }

    bool operator>(const _Rational& b) const
    {
        rational<int64_t>ra(this->num, this->denom);
        rational<int64_t>rb(b.num, b.denom);
        return ra > rb;
    }

    bool operator<(const _Rational& b) const
    {
        rational<int64_t>ra(this->num, this->denom);
        rational<int64_t>rb(b.num, b.denom);
        return ra < rb;
    }

    _Rational& operator+=(const _Rational& b)
    {
        rational<int64_t>ra(this->num, this->denom);
        rational<int64_t>rb(b.num, b.denom);
        ra += rb;
        this->num = ra.numerator();
        this->denom = ra.denominator();
        return *this;
    }
} SciDB_Rational;

//
// PGB: Inside SciDB, certain operations make assumptions about the
//	  nature of 'zero'. For example, the operations that
//	  implement sum() begin with a value that is set to 0 and
//	  add a succession of type instances to it using this '+'.
//
//	  The problem is that boost throws a trap when it encounters
//	  a rational with a denominator of '0'. So what we do here
//	  is to catch this case (as we would catch a divide by zero
//	  in doubles) and prevent it.
inline void check_zero ( SciDB_Rational * r )
{
    if ((0 == r->denom) && (0 == r->num))
            r->denom = 1;
}

//
void construct_rational(const Value** args, Value* res, void*)
{
    *(SciDB_Rational *)res->data() = SciDB_Rational();
}

//
// The type has three "constructor" functions: 
//	String -> Rational
//	Int -> Rational
//	Int, Int -> Rational 

void str2Rational(const Value** args, Value* res, void*)
{
	int64_t n, d;
    SciDB_Rational* r = (SciDB_Rational*)res->data();

    if (sscanf(args[0]->getString(), "(%"PRIi64"/%"PRIi64")", &n, &d) != 2)
        throw PLUGIN_USER_EXCEPTION("librational", SCIDB_SE_UDO, RATIONAL_E_CANT_CONVERT_TO_RATIONAL)
            << args[0]->getString();

	if ((0 == d) && (0 == n))
		d = 1;
		
	boost::rational<int64_t>rp0(n, d);
	r->num   = rp0.numerator();
	r->denom = rp0.denominator();

}

void int2Rational(const Value** args, Value* res, void*)
{
    SciDB_Rational * r = (SciDB_Rational*)res->data();

    r->num   = args[0]->getInt64();
	r->denom = 1;
}

void ints2Rational(const Value** args, Value* res, void*)
{
	int64_t n, d;

    SciDB_Rational * r = (SciDB_Rational*)res->data();

    n = args[0]->getInt64();
    d = args[1]->getInt64();

	if ((0 == d) && (0 == n))
		d = 1;

	boost::rational<int64_t>rp0(n, d);
	r->num   = rp0.numerator();
	r->denom = rp0.denominator();

}
//
// To get the data out of the type, we proviode three UDFs. The first 
// simply converts the internals of the type's data to a string. This 
// will be the "lowest common denominator" (pardon the pun) converter. 
void rational2Str(const Value** args, Value* res, void*)
{
    SciDB_Rational* r = (SciDB_Rational*)args[0]->data();

	stringstream ss;
	ss << '(' << r->num << '/' << r->denom << ')';

    res->setString(ss.str().c_str());
}

//
// To get the numerator, and the denominator, we provide the following
// pair of UDFs. 
void rationalGetNumerator(const Value** args, Value* res, void*)
{
    SciDB_Rational * r = (SciDB_Rational*)args[0]->data();
    res->setInt64(r->num);
}

void rationalGetDenominator(const Value** args, Value* res, void*)
{
    SciDB_Rational * r = (SciDB_Rational*)args[0]->data();
    res->setInt64(r->denom);
}

//
// Working with Rational numbers isn't complex, but it's as well to have 
// some more widely used framework to exploit. In this case, Boost. 
//
// PGB: Inside SciDB, certain operations make assumptions about the 
//	  nature of 'zero'. For example, the operations that 
//	  implement sum() begin with a value that is set to 0 and 
//	  add a succession of type instances to it using this '+'.
//
//	  The problem is that boost throws a trap when it encounters 
//	  a rational with a denominator of '0'. So what we do here 
//	  is to catch this case (as we would catch a divide by zero 
//	  in doubles) and prevent it. 
void rationalPlus(const Value** args, Value* res, void*)
{
    SciDB_Rational* r0 = (SciDB_Rational*)args[0]->data();
    SciDB_Rational* r1 = (SciDB_Rational*)args[1]->data();
    SciDB_Rational* r = (SciDB_Rational*)res->data();

	check_zero ( r0 );
	check_zero ( r1 );

	boost::rational<int64_t>rp0(r0->num, r0->denom);
	boost::rational<int64_t>rp1(r1->num, r1->denom);

	rp0+=rp1;

	r->num   = rp0.numerator();
	r->denom = rp0.denominator();
}

void rationalMinus(const Value** args, Value* res, void*)
{
    SciDB_Rational* r0 = (SciDB_Rational*)args[0]->data();
    SciDB_Rational* r1 = (SciDB_Rational*)args[1]->data();
    SciDB_Rational* r = (SciDB_Rational*)res->data();

	check_zero ( r0 );
	check_zero ( r1 );

	boost::rational<int64_t>rp0(r0->num, r0->denom);
	boost::rational<int64_t>rp1(r1->num, r1->denom);

	rp0-=rp1;

	r->num   = rp0.numerator();
	r->denom = rp0.denominator();
}

void rationalTimes(const Value** args, Value* res, void*)
{
    SciDB_Rational* r0 = (SciDB_Rational*)args[0]->data();
    SciDB_Rational* r1 = (SciDB_Rational*)args[1]->data();
    SciDB_Rational* r = (SciDB_Rational*)res->data();

	check_zero ( r0 );
	check_zero ( r1 );

	boost::rational<int64_t>rp0(r0->num, r0->denom);
	boost::rational<int64_t>rp1(r1->num, r1->denom);

	rp0*=rp1;

	r->num   = rp0.numerator();
	r->denom = rp0.denominator();
}

void rationalDivide(const Value** args, Value* res, void*)
{
    SciDB_Rational* r0 = (SciDB_Rational*)args[0]->data();
    SciDB_Rational* r1 = (SciDB_Rational*)args[1]->data();
    SciDB_Rational* r = (SciDB_Rational*)res->data();

	check_zero ( r0 );
	check_zero ( r1 );

	boost::rational<int64_t>rp0(r0->num, r0->denom);
	boost::rational<int64_t>rp1(r1->num, r1->denom);

	rp0/=rp1;

	r->num   = rp0.numerator();
	r->denom = rp0.denominator();
}

void rationalIntDivide(const Value** args, Value* res, void*)
{
    SciDB_Rational* r0 = (SciDB_Rational*)args[0]->data();
    int64_t		  d = args[1]->getInt64();

    SciDB_Rational * r = (SciDB_Rational*)res->data();

	boost::rational<int64_t>rp0(r0->num, r0->denom);
	boost::rational<int64_t>rp1(d, 1);

	rp0/=rp1;

	r->num   = rp0.numerator();
	r->denom = rp0.denominator();
}

//
// Use the boost::rational<> to provide the comparisons
void rationalLT(const Value** args, Value* res, void * v)
{
    SciDB_Rational* r0 = (SciDB_Rational*)args[0]->data();
    SciDB_Rational* r1 = (SciDB_Rational*)args[1]->data();

	check_zero ( r0 );
	check_zero ( r1 );

	boost::rational<int64_t>rp0(r0->num, r0->denom);
	boost::rational<int64_t>rp1(r1->num, r1->denom);

	if ( rp0 < rp1 ) 
        res->setBool(true);
	else
        res->setBool(false);
}

void rationalEQ(const Value** args, Value* res, void * v)
{
    SciDB_Rational* r0 = (SciDB_Rational*)args[0]->data();
    SciDB_Rational* r1 = (SciDB_Rational*)args[1]->data();

	check_zero ( r0 );
	check_zero ( r1 );

	boost::rational<int64_t>rp0(r0->num, r0->denom);
	boost::rational<int64_t>rp1(r1->num, r1->denom);

	if ( rp0 == rp1 ) 
        res->setBool(true);
	else
        res->setBool(false);
}

void rationalLTEQ(const Value** args, Value* res, void * v)
{
    SciDB_Rational* r0 = (SciDB_Rational*)args[0]->data();
    SciDB_Rational* r1 = (SciDB_Rational*)args[1]->data();

	boost::rational<int64_t>rp0(r0->num, r0->denom);
	boost::rational<int64_t>rp1(r1->num, r1->denom);

	if ( rp0 <= rp1 ) 
        res->setBool(true);
	else
        res->setBool(false);
}

void rationalGT(const Value** args, Value* res, void * v)
{
    SciDB_Rational* r0 = (SciDB_Rational*)args[0]->data();
    SciDB_Rational* r1 = (SciDB_Rational*)args[1]->data();

	boost::rational<int64_t>rp0(r0->num, r0->denom);
	boost::rational<int64_t>rp1(r1->num, r1->denom);

	if ( rp0 > rp1 ) 
        res->setBool(true);
	else
        res->setBool(false);
}

void rationalGTEQ(const Value** args, Value* res, void * v)
{
    SciDB_Rational* r0 = (SciDB_Rational*)args[0]->data();
    SciDB_Rational* r1 = (SciDB_Rational*)args[1]->data();

	boost::rational<int64_t>rp0(r0->num, r0->denom);
	boost::rational<int64_t>rp1(r1->num, r1->denom);

	if ( rp0 >= rp1 ) 
        res->setBool(true);
	else
        res->setBool(false);
}

#endif // RATIONAL_FUNCTIONS_H
