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

/*
 * SpatialType.cpp
 *
 *  Created on: Aug 15, 2014
 *      Author: Donghui Zhang
 */

#include <util/SpatialType.h>

namespace scidb
{
DominanceRelationship calculateDominance(Coordinates const& left, Coordinates const& right)
{
    assert(left.size() == right.size() && !left.empty());
    bool isDominating = true;
    bool isDominatedBy = true;
    for (size_t i=0, n=left.size(); i<n; ++i) {
        if (left[i] > right[i]) {
            isDominatedBy = false;
        }
        else if (left[i] < right[i]) {
            isDominating = false;
        }
    }
    if (isDominating && isDominatedBy) {
        return EQUALS;
    }
    else if (!isDominating && !isDominatedBy) {
        return HAS_NO_DOMINANCE_WITH;
    }
    return isDominating ? STRICTLY_DOMINATES : IS_STRICTLY_DOMINATED_BY;
}

bool isDominatedBy(Coordinates const& left, Coordinates const& right)
{
    DominanceRelationship dr = calculateDominance(left, right);
    return dr==EQUALS || dr==IS_STRICTLY_DOMINATED_BY;
}

bool SpatialRanges::findOneThatIntersects(SpatialRange const& queryRange, size_t& hint) const
{
    if (hint>0 && hint<_ranges.size()) {
        if (_ranges[hint].intersects(queryRange)) {
            return true;
        }
    }
    for (size_t i=0, n=_ranges.size(); i<n; ++i) {
        if (_ranges[i].intersects(queryRange)) {
            hint = i;
            return true;
        }
    }
    hint = -1;
    return false;
}

bool SpatialRanges::findOneThatContains(Coordinates const& queryPoint, size_t& hint) const
{
    if (hint>0 && hint<_ranges.size()) {
        if (_ranges[hint].contains(queryPoint)) {
            return true;
        }
    }
    for (size_t i=0, n=_ranges.size(); i<n; ++i) {
        if (_ranges[i].contains(queryPoint)) {
            hint = i;
            return true;
        }
    }
    hint = -1;
    return false;
}

bool SpatialRanges::findOneThatContains(SpatialRange const& queryRange, size_t& hint) const
{
    if (hint>0 && hint<_ranges.size()) {
        if (_ranges[hint].contains(queryRange)) {
            return true;
        }
    }
    for (size_t i=0, n=_ranges.size(); i<n; ++i) {
        if (_ranges[i].contains(queryRange)) {
            hint = i;
            return true;
        }
    }
    hint = -1;
    return false;
}

}

