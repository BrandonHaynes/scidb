/*
**
* BEGIN_COPYRIGHT
*
* This file is part of SciDB.
* Copyright (C) 2014 SciDB, Inc.
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

#ifndef COORDINATE_H_
#define COORDINATE_H_

/****************************************************************************/

#include <vector>                                        // For vector
#include <inttypes.h>                                    // For int64_t etc
#include <util/PointerRange.h>                           // For PointerRange

/****************************************************************************/
namespace scidb {
/****************************************************************************/

typedef int64_t                         position_t;
typedef int64_t                         Coordinate;
typedef std::vector <Coordinate>        Coordinates;
typedef PointerRange<Coordinate const>  CoordinateCRange;
typedef PointerRange<Coordinate      >  CoordinateRange;

/****************************************************************************/

const Coordinate  MAX_COORDINATE    =  uint64_t(-1) >> 2;
const Coordinate  MIN_COORDINATE    = -MAX_COORDINATE;
const uint64_t    INFINITE_LENGTH   =  MAX_COORDINATE;

/****************************************************************************/

/**
 *  Compare two points in row-major order and return a number indicating how
 *  they differ.
 *
 *  @param  a   the left side of the comparison
 *  @param  b   the right side of the comparison
 *  @return negative if a is less than b;
 *          positive if b is less than a,
 *          0 if both are equal.
 */
inline int64_t coordinatesCompare(CoordinateCRange a,CoordinateCRange b)
{
    assert(a.size() == b.size());

    for (size_t i=0, n=a.size(); i != n; ++i)
    {
        if (int64_t d = a[i] - b[i])
        {
            return d;
        }
    }

    return 0;
}

/**
 *  Compare two points in colum-major order and return a number indicating how
 *  they differ.
 *
 *  @param  a   the left side of the comparison
 *  @param  b   the right side of the comparison
 *  @return negative if a is less than b;
 *          positive if b is less than a,
 *          0 if both are equal.
 */
inline int64_t coordinatesCompareCMO(CoordinateCRange a,CoordinateCRange b)
{
    assert(a.size() == b.size());

    for (size_t i = a.size(); i-- > 0; )
    {
        if (int64_t d = a[i] - b[i])
        {
            return d;
        }
    }

    return 0;
}

/**
 * Compare two points in row-major order.
 */
struct CoordinatesLess
{
    bool operator()(CoordinateCRange a,CoordinateCRange b) const
    {
        return coordinatesCompare(a,b) < 0;
    }
};

/**
 *  Compare two points in column-major order.
 */
struct CoordinatesLessCMO
{
    bool operator()(CoordinateCRange a,CoordinateCRange b) const
    {
        return coordinatesCompareCMO(a,b) < 0;
    }
};

/****************************************************************************/

std::ostream& operator<<(std::ostream&,CoordinateCRange);

/****************************************************************************/

typedef CoordinateCRange   CoordsToStr;                  // Obsolete name
typedef CoordinatesLess    CoordinatesComparator;        // Obsolete name
typedef CoordinatesLessCMO CoordinatesComparatorCMO;     // Obsolete name

/****************************************************************************/
}
/****************************************************************************/
#endif
/****************************************************************************/
