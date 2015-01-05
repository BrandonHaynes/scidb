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
 * SpatialRangesChunkPosIterator.cpp
 *
 *  Created on: Aug 15, 2014
 *      Author: Donghui Zhang
 */

#include <array/SpatialRangesChunkPosIterator.h>
using namespace boost;

namespace scidb
{
SpatialRangesChunkPosIterator::SpatialRangesChunkPosIterator(boost::shared_ptr<SpatialRanges> const& spatialRanges, ArrayDesc const& schema)
: _numRanges(spatialRanges->_ranges.size()),
  _spatialRanges(spatialRanges),
  _schema(schema),
  _rawIterators(_numRanges),
  _lowPositionsForRawIterators(_numRanges),
  _highPositionsForRawIterators(_numRanges),
  _intervals(schema.getDimensions().size())
{
    for (size_t i=0; i<_numRanges; ++i) {
        // first pos in chunks covering 'lowPos' of the ranges
        _lowPositionsForRawIterators[i] = _spatialRanges->_ranges[i]._low;
        _schema.getChunkPositionFor(_lowPositionsForRawIterators[i]);

        // last pos in chunks covering 'highPos' of the ranges
        _highPositionsForRawIterators[i] = _spatialRanges->_ranges[i]._high;
    }

    for (size_t i=0, n=_intervals.size(); i<n; ++i) {
        _intervals[i] = _schema.getDimensions()[i].getChunkInterval();
    }

    reset();
}

bool SpatialRangesChunkPosIterator::end()
{
    return _wrapperIterator->end();
}

void SpatialRangesChunkPosIterator::operator ++()
{
    assert(!end());
    ++( *_wrapperIterator);
}

Coordinates const& SpatialRangesChunkPosIterator::getPosition()
{
    assert(!end());
    return _wrapperIterator->getPosition();
}

bool SpatialRangesChunkPosIterator::setPosition(Coordinates const& pos)
{
    ASSERT_EXCEPTION(false, "SpatialRangeChunkPosIterator::setPosition() not supported!");
    return false;
}

void SpatialRangesChunkPosIterator::reset()
{
    for (size_t i=0; i<_numRanges; ++i) {
        // If the original spatial ranges are invalid, don't bother creating a rawIterator.
        if (! _spatialRanges->_ranges[i].valid()) {
            _rawIterators[i].reset();
        }
        else {
            _rawIterators[i] = make_shared<RegionCoordinatesIterator>(
                    _lowPositionsForRawIterators[i],
                    _highPositionsForRawIterators[i],
                    _intervals);
        }
    }

    // Note: the code below does not compile with make_shared.
    _wrapperIterator = shared_ptr<MultiConstIterators>(new MultiConstIterators(_rawIterators));
}

bool SpatialRangesChunkPosIterator::advancePositionToAtLeast(Coordinates const& newPos)
{
    if (end() || getPosition() >= newPos) {
        return false;
    }

    // TO-DO: An optimization may be possible here.
    // That is, call operator++() once, and return if the new position reaches newPos.
    // If someone wants to explore the optimization, some performance study may be performed.
    // --- Note from Donghui Z. 8/14/2014.

    for (size_t i=0; i<_numRanges; ++i) {
        if (!_rawIterators[i]) continue;
        dynamic_pointer_cast<RegionCoordinatesIterator>(_rawIterators[i])->advanceToAtLeast(newPos);
    }

    // Note: the code below does not compile with make_shared.
    _wrapperIterator = shared_ptr<MultiConstIterators>(new MultiConstIterators(_rawIterators));
    return true;
}

}
