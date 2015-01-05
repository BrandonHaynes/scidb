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
 * PhysicalApply.cpp
 *
 *  Created on: Apr 04, 2012
 *      Author: Knizhnik
 */

#include <math.h>
#include <boost/assign.hpp>

#include "query/Operator.h"
#include "array/Metadata.h"
#include "BestMatchArray.h"

using namespace std;
using namespace boost;
using namespace boost::assign;
using namespace boost::assign;

namespace scidb {

class PhysicalBestMatch: public PhysicalOperator
{
  public:
    PhysicalBestMatch(const string& logicalName, const string& physicalName, const Parameters& parameters, const ArrayDesc& schema):
        PhysicalOperator(logicalName, physicalName, parameters, schema)
    {}

    virtual DistributionRequirement getDistributionRequirement (const std::vector< ArrayDesc> & inputSchemas) const
    {
        return DistributionRequirement(DistributionRequirement::Collocated);
    }

    virtual PhysicalBoundaries getOutputBoundaries(const std::vector<PhysicalBoundaries> & inputBoundaries,
                                                   const std::vector< ArrayDesc> & inputSchemas) const
    {
        return inputBoundaries[0];
    }

    /***
     * BestMatch is a pipelined operator, hence it executes by returning an iterator-based array to the consumer
     * that overrides the chunkiterator method.
     */
    boost::shared_ptr<Array> execute(vector< boost::shared_ptr<Array> >& inputArrays, boost::shared_ptr<Query> query)
    {
        assert(inputArrays.size() == 2);
        int64_t error = ((boost::shared_ptr<OperatorParamPhysicalExpression>&)_parameters[0])->getExpression()->evaluate().getInt64();
        return boost::shared_ptr<Array>(new BestMatchArray(_schema, inputArrays[0], inputArrays[1], error));
    }
};

REGISTER_PHYSICAL_OPERATOR_FACTORY(PhysicalBestMatch, "bestmatch", "physicalBestMatch");

#define DEGREE_TO_RADIAN(g) ((g)*0.0174532925199432957692369076848861271344)

/**
 * Convert RA/DEC coordinates to 3D coordinates {x,y,z}:
 *   x=cos(ra)*cos(dec)
 *   y=sin(ra)*cos(dec)
 *   z=sin(dec)
 * The idea of such mapping is to change coordinate system in such way that for near stars we will have
 * |x1-x2| < e, |y1-y2| < e, |z1-z2| < e where e is some small number.
 * It will allow us to split array into chunks with fixed overlap and proceed each chunk independently (and concurrently).
 * Also it allows as to split chunk into cubes and use hash function for fast location of matched stars.
 * Looks like it is not possible to map equatorial RA/DEC coordinates to any 2D coordinates so that this requirement is satisfied.
 * So we have to use 3D.
 * @param ra right ascension (degrees)
 * @param dec declination (degrees)
 * @return x: [-1,1]
 */
static void radec2x(const Value** args, Value* res, void*)
{
    double ra = DEGREE_TO_RADIAN(args[0]->getDouble());
    double dec = DEGREE_TO_RADIAN(args[1]->getDouble());
    double x = cos(ra)*cos(dec);
    res->setDouble(x);
}

/**
 * Convert RA/DEC coordinates to 3D coordinates {x,y,z}:
 *   x=cos(ra)*cos(dec)
 *   y=sin(ra)*cos(dec)
 *   z=sin(dec)
 * The idea of such mapping is to change coordinate system in such way that for near stars we will have
 * |x1-x2| < e, |y1-y2| < e, |z1-z2| < e where e is some small number.
 * It will allow us to split array into chunks with fixed overlap and proceed each chunk independently (and concurrently).
 * Also it allows as to split chunk into cubes and use hash function for fast location of matched stars.
 * Looks like it is not possible to map equatorial RA/DEC coordinates to any 2D coordinates so that this requirement is satisfied.
 * So we have to use 3D.
 * @param ra right ascension (degrees)
 * @param dec declination (degrees)
 * @return y: [-1,1]
 */
static void radec2y(const Value** args, Value* res, void*)
{
    double ra = DEGREE_TO_RADIAN(args[0]->getDouble());
    double dec = DEGREE_TO_RADIAN(args[1]->getDouble());
    double y = sin(ra)*cos(dec);
    res->setDouble(y);
}

/**
 * Convert RA/DEC coordinates to 3D coordinates {x,y,z}:
 *   x=cos(ra)*cos(dec)
 *   y=sin(ra)*cos(dec)
 *   z=sin(dec)
 * The idea of such mapping is to change coordinate system in such way that for near stars we will have
 * |x1-x2| < e, |y1-y2| < e, |z1-z2| < e where e is some small number.
 * It will allow us to split array into chunks with fixed overlap and proceed each chunk independently (and concurrently).
 * Also it allows as to split chunk into cubes and use hash function for fast location of matched stars.
 * Looks like it is not possible to map equatorial RA/DEC coordinates to any 2D coordinates so that this requirement is satisfied.
 * So we have to use 3D.
 * @param ra right ascension (degrees)
 * @param dec declination (degrees)
 * @return z: [-1,1]
 */
static void radec2z(const Value** args, Value* res, void*)
{
    double dec = DEGREE_TO_RADIAN(args[1]->getDouble());
    double z = sin(dec);
    res->setDouble(z);
}

/**
 * Calculate distance between stars at spehere
 * @param ra1 right ascension of first star (degrees)
 * @param dec1 declination of first star (degrees)
 * @param ra2 right ascension of second star (degrees)
 * @param dec2 declination of second star (degrees)
 * @return sin of distance between stars
 */
static void radec_sindist(const Value** args, Value* res, void*)
{
    double ra1 = args[0]->getDouble();
    double dec1 = args[1]->getDouble();
    double ra2 = args[2]->getDouble();
    double dec2 = args[3]->getDouble();

    double x, y, z;
    x = sin (DEGREE_TO_RADIAN((ra1 - ra2) / 2));
    x *= x;
    y = sin (DEGREE_TO_RADIAN((dec1 - dec2) / 2));
    y *= y;
    
    /* Seem to be more precise :) */
    z = cos (DEGREE_TO_RADIAN((dec1 + dec2)/2));
    z*=z;
    
    res->setDouble(x * (z - y) + y);
}

REGISTER_FUNCTION(radec2x, list_of(TID_DOUBLE)(TID_DOUBLE), TID_DOUBLE, radec2x);
REGISTER_FUNCTION(radec2y, list_of(TID_DOUBLE)(TID_DOUBLE), TID_DOUBLE, radec2y);
REGISTER_FUNCTION(radec2z, list_of(TID_DOUBLE)(TID_DOUBLE), TID_DOUBLE, radec2z);
REGISTER_FUNCTION(radec_sindist, list_of(TID_DOUBLE)(TID_DOUBLE)(TID_DOUBLE)(TID_DOUBLE), TID_DOUBLE, radec_sindist);

}  // namespace scidb
