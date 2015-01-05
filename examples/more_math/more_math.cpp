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
 * @file more_math.cpp
 *
 * @author sunny_jeswani@persistent.co.in
 *
 * @brief Shared library that loads into SciDB some mathematical functions.
 */

#include <vector>
#include <boost/assign.hpp>
#include <math.h>

#include "query/Operator.h"
#include "query/FunctionLibrary.h"
#include "query/FunctionDescription.h"
#include "query/TypeSystem.h"

using namespace std;
using namespace scidb;
using namespace boost::assign;

void constructfact(const Value** args, Value* res, void*)
{

	int64_t i,n;
	uint64_t f;
	stringstream ss;

    n = args[0]->getInt64();

	f=1;
	for(i=2;i<=n;i++)
		f *= i;

	if(f==0)
		ss << "very large number";
	else
 		ss << f;

    res->setString(ss.str().c_str());

}


void constructmylog(const Value** args, Value* res, void*)
{
	double a,b,r;

    a = args[0]->getDouble();
    b = args[1]->getDouble();

	r = log(a)/log(b);
    res->setDouble(r);
}


void checkisprime(const Value** args, Value* res, void*)
{
	int64_t i,n;
	int8_t r;
	stringstream ss;

    n = args[0]->getInt64();
	r = 0;
	ss << n;

	if(n>1)
		for(i=2;i<=sqrt(n);i++)
			if(n%i==0)
			{
				r = 1;
				break;
			}

	if(r==0 && n>1)
		ss << " :prime";
	else
 		ss << " :not prime";

    res->setString(ss.str().c_str());

}
double
Lasso ( const double Z,
        const double delta,
        const double scale )

{
    /* return ((SIGN(Z)*(fabs(Z)>=delta)*(fabs(Z)-delta))*scale); */
    double one = 1.0;
    return ((copysign(one, Z)*(fabs(Z)>=delta)*(fabs(Z)-delta))*scale);
}


void jameslasso(const Value** args, Value* res, void*)
{
    res->setDouble ( Lasso ( args[0]->getDouble(),
                            args[1]->getDouble(),
                            args[2]->getDouble() ));
}

REGISTER_FUNCTION(fact, list_of("int64"), "string", constructfact);
REGISTER_FUNCTION(mylog, list_of("double")("double"), "double", constructmylog);
REGISTER_FUNCTION(lasso, list_of("double")("double")("double"), "double", jameslasso);
REGISTER_FUNCTION(isprime, list_of("int64"), "string", checkisprime);
