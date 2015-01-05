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
 * SpatialType.h
 *  This file defines some spatial types.
 *
 *  Created on: Aug 15, 2014
 *      Author: Donghui Zhang
 */
#ifndef SPATIALTYPE_H_
#define SPATIALTYPE_H_

#include <vector>
#include <assert.h>
#include <array/RLE.h>

namespace scidb
{
/**
 * Point A dominates point B, if in all dimensions A has a larger or equal coordinate.
 * The dominance relationship is strict, if in at least one dimension the coordinates are not equal.
 *
 * Comparing two multi-dimensional points (with the same dimensionality), the following cases are possible:
 *   - They are exactly equal in all dimensions.
 *   - A strictly dominates B.
 *   - A is strictly dominated by B.
 *   - Neither of them dominates the other.
 */
enum DominanceRelationship
{
    EQUALS,
    STRICTLY_DOMINATES,
    IS_STRICTLY_DOMINATED_BY,
    HAS_NO_DOMINANCE_WITH
};

/**
 * Given two points, tell their dominance relationship.
 * @param left  one point.
 * @param right another point.
 * @return a DominanceRelationship value.
 * @assert the points must have the same dimensionality, which must be at least one.
 */
DominanceRelationship calculateDominance(Coordinates const& left, Coordinates const& right);

/**
 * @return whether left is dominated by right.
 * @left  a point.
 * @right another point.
 */
bool isDominatedBy(Coordinates const& left, Coordinates const& right);

/**
 * A spatial range is composed of a low Coordinates and a high Coordinates.
 */
class SpatialRange
{
public:
    /**
     * The low point.
     */
    Coordinates _low;

    /**
     * The high point.
     */
    Coordinates _high;

    /**
     * This version of the constructor only allocates space for _low and _high.
     * it is the caller's responsibility to assign values to the Coordinates.
     * @param numDims   the number of dimensions. Must be positive.
     */
    SpatialRange(size_t numDims = 0)
    : _low(numDims), _high(numDims)
    {
    }

    /**
     * This version of the constructor assigns the values to the low & high coordinates.
     * @param low  the low coordinates.
     * @param high the high coordinates.
     * @assert low must be dominated by high.
     */
    SpatialRange(Coordinates const& low, Coordinates const& high)
    : _low(low), _high(high)
    {
        assert(valid());
    }

    /**
     * A range is valid, if _low is dominated by _high.
     * @assert _low and _high have the same positive size.
     * @return whether the range is valid.
     */
    bool valid() const
    {
        return isDominatedBy(_low, _high);
    }

    /**
     * @return whether I intersect with the other range.
     * @param other  the other range.
     * @note Two ranges intersect, iff the low point of each range is dominated by the high point of the other range.
     * @assert: the other range and I must have the same dimensionality and must both be valid.
     */
    bool intersects(SpatialRange const& other) const
    {
        assert(valid() && other.valid() && _low.size()==other._low.size());
        return isDominatedBy(_low, other._high) && isDominatedBy(other._low, _high);
    }

    /**
     * @return whether I spatially contain a given point.
     * @param point  a point.
     */
    bool contains(Coordinates const& point) const
    {
        assert(valid() && _low.size()==point.size());
        return isDominatedBy(_low, point) && isDominatedBy(point, _high);
    }

    /**
     * @return whether I spatially contain a given range.
     * @note I fully contain a range, if I contain both its low point and its high point.
     * @param other    a range
     * @assert: the other ranges and I must have the same dimensionality and must both be valid.
     */
    bool contains(SpatialRange const& other) const
    {
        assert(valid() && other.valid() && _low.size()==other._low.size());
        return contains(other._low) && contains(other._high);
    }
};

/**
 * The class SpatialRanges is essentially a vector of SpatialRange objects,
 * with some additional capabilities.
 */
class SpatialRanges
{
public:
    /**
     * Number of dimensions.
     */
    const size_t _numDims;

    /**
     * A vector of SpatialRange objects.
     */
    std::vector<SpatialRange> _ranges;

    /**
     * Every newly added SpatialRange object will have numDims dimensions.
     */
    SpatialRanges(size_t numDims)
    : _numDims(numDims)
    {}

    /**
     * Append a new SpatialRange object at the end, and return a reference to it.
     * @return a reference to the newly added SpatialRange object.
     * @note the members _low and _high of the new SpatialRange already have numDims cells allocated.
     */
    SpatialRange& addOne()
    {
        _ranges.push_back(SpatialRange(_numDims));
        return *(_ranges.rbegin());
    }

    /**
     * @return whether at least one stored range intersects the query range.
     * @param queryRange  the query range.
     * @param[inout] hint  the index to look first; will be changed to the index in _ranges (successful search), or -1.
     */
    bool findOneThatIntersects(SpatialRange const& queryRange, size_t& hint) const;

    /**
     * @return whether at least one stored range contains a query point.
     * @param queryPoint  the query point.
     * @param[inout] hint  the index to look first; will be changed to the index in _ranges (successful search), or -1.
     */
    bool findOneThatContains(Coordinates const& queryPoint, size_t& hint) const;

    /**
     * @return whether at least one stored range contains a query range.
     * @param queryRange  the query range.
     * @param[inout] hint  the index to look first; will be changed to the index in _ranges (successful search), or -1.
     */
    bool findOneThatContains(SpatialRange const& queryRange, size_t& hint) const;
};

}

#endif
