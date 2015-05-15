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

/**
 * ArrayCoordinatesMapper.h
 *
 *  Created on: Aug 8, 2013
 *      Author: sfridella
 */

#ifndef ARRAY_COORDINATES_MAPPER_H_
#define ARRAY_COORDINATES_MAPPER_H_

#include <assert.h>
#include <boost/shared_ptr.hpp>
#include <boost/scoped_array.hpp>
#include <array/Metadata.h>

namespace scidb
{
/**
 * Map coordinates to offset within chunk and vice versa, while the chunk is given at run time.
 * The difference from CoordinatesMapper is that this object can be created once and reused for all chunks in the array.
 *
 * PERFORMANCE CRITICAL! Do not move the implementation of these functions to a CPP file.
 * On 9/16/2014, Donghui did a small benchmark study using:
 *    ./sgmb.py perf-test -t spgemm -s 4
 * and found moving them all to the CPP file slowed down performance by 13%.
 */
class ArrayCoordinatesMapper
{
protected:
    const Dimensions  _dims;

public:
    /**
     * Constructor.
     * @param  dims   the dimensions
     */
    ArrayCoordinatesMapper(Dimensions const& dims)
        : _dims(dims)
    {
        assert(!dims.empty());
    }

    /**
     * Convert chunkPos to lows and intervals.
     * Without overlap, lows[i] = chunkPos[i]; but with overlap, lows[i] may be smaller (but can't be smaller than dims[i].getStartMin().
     * Similar with highs[i] (may be larger than the last logical coord, but can't be larger than dims[i].getEndMax().
     * The interval is just highs[i]-lows[i]+1.
     * @param chunkPos   the chunk position
     * @param lows       [out] the low point
     * @param intervals  [out] the intervals of each dim
     */
    void chunkPos2LowsAndIntervals(CoordinateCRange chunkPos, CoordinateRange lows, CoordinateRange intervals) const
    {
        assert(chunkPos.size()==_dims.size());
        assert(lows.size()==_dims.size());
        assert(intervals.size()==_dims.size());

        for (size_t i=0; i<_dims.size(); ++i) {
            lows[i] = chunkPos[i] - _dims[i].getChunkOverlap();
            if (lows[i] < _dims[i].getStartMin()) {
                lows[i] = _dims[i].getStartMin();
            }
            Coordinate high = chunkPos[i] + _dims[i].getChunkInterval()+_dims[i].getChunkOverlap() - 1;
            if (high > _dims[i].getEndMax()) {
                high = _dims[i].getEndMax();
            }
            intervals[i] = high - lows[i] + 1;

            assert(intervals[i]>0);
        }
    }

    /**
     * Given a position in a chunk, and the chunkPos, compute the coordinate.
     * @param chunkPos   the chunk position
     * @param pos        position in a chunk, as returned by coord2pos
     * @param coord      [out] the coordinate
     */
    void pos2coord(CoordinateCRange chunkPos, position_t pos, CoordinateRange coord) const
    {
        assert(pos>=0);
        assert(coord.size() == _dims.size());

        for (size_t i=_dims.size(); i>0; ) {
            --i;
            const Coordinate low  = std::max(Coordinate(chunkPos[i] - _dims[i].getChunkOverlap()),
                                             _dims[i].getStartMin());

            const Coordinate high = std::min(Coordinate(chunkPos[i] + _dims[i].getChunkInterval() + 
                                                           _dims[i].getChunkOverlap() - 1),
                                             _dims[i].getEndMax());

            Coordinate interval = high - low + 1;
            assert(interval>0);

            coord[i] = low + (pos % interval);
            pos /= interval;
        }
        assert(pos == 0);
        // coord was ouput

    }

    /**
     * Upon repeated call to pos2coord with the same chunkPos, the performance can be improved by
     * converting chunkPos to lows and intervals, then call pos2coord with lows and intervals.
     * @param lows       the low point of the chunk
     * @param intervals  the intervals of the chunk
     * @param pos        position in a chunk, as returned by coord2pos
     * @param coord      [out] the coordinate
     */
    void pos2coordWithLowsAndIntervals(CoordinateCRange lows, CoordinateCRange intervals, position_t pos, CoordinateRange coord) const
    {
        assert(pos>=0);
        assert(lows.size() == _dims.size());
        assert(intervals.size() == _dims.size());
        assert(coord.size() == _dims.size());

        size_t i = _dims.size();
        while (i>0) {
            --i;
            coord[i] = lows[i] + (pos % intervals[i]);
            pos /= intervals[i];
        }
        assert(pos == 0);
    }

    /**
     * Given a coord in a chunk, and the chunkPos, compute the position in the chunk.
     * @param chunkPos the chunk position
     * @param coord    the coord in the chunk
     * @return the position
     */
    position_t coord2pos(CoordinateCRange chunkPos, CoordinateCRange coord) const
    {
        assert(chunkPos.size()==_dims.size());
        assert(coord.size() == _dims.size());

        position_t pos = 0;
        for (size_t i=0; i<_dims.size(); ++i) {
            const Coordinate low  = std::max(Coordinate(chunkPos[i] - _dims[i].getChunkOverlap()),
                                             _dims[i].getStartMin());

            const Coordinate high = std::min(chunkPos[i] + _dims[i].getChunkInterval()+
                                                           _dims[i].getChunkOverlap() - 1,
                                             _dims[i].getEndMax());

            Coordinate interval = high - low + 1;
            assert(interval>0);

            pos *= interval;
            pos += coord[i] - low;
        }

        return pos;
    }

    /**
     * Upon repeated call to coord2pos with the same chunkPos, the performance can be improved by
     * converting chunkPos to lows and intervals, then call coord2pos with lows and intervals.
     * @param lows       the low point of the chunk
     * @param intervals  the intervals of the chunk
     * @param coord      the coord in the chunk
     * @return the position
     */
    position_t coord2posWithLowsAndIntervals(CoordinateCRange lows, CoordinateCRange intervals, CoordinateCRange coord) const
    {
        assert(lows.size() == _dims.size());
        assert(intervals.size() == _dims.size());
        assert(coord.size() == _dims.size());

        position_t pos = 0;
        for (size_t i = 0; i < _dims.size(); ++i) {
            pos *= intervals[i];
            pos += coord[i] - lows[i];
        }
        return pos;
    }

    /**
     * Return a constant reference to the dimensions used by this mapper
     * @return the dimensions
     */
    Dimensions const& getDims() const
    {
        return _dims;
    }

    /**
     * Encode a chunkPos to an integer.
     * @param chunkPos  a chunk position.
     * @return a single integer, as the "sequence number" of the chunk.
     */
    position_t chunkPos2pos(CoordinateCRange chunkPos) const
    {
        assert(chunkPos.size() == _dims.size());

        position_t pos = 0;
        for (size_t i = 0, n = _dims.size(); i < n; ++i) {
            size_t numChunksInDim = (_dims[i].getEndMax() - _dims[i].getStartMin() + _dims[i].getChunkInterval() - 1) / _dims[i].getChunkInterval();
            assert( (chunkPos[i] - _dims[i].getStartMin()) % _dims[i].getChunkInterval() == 0);
            size_t chunkNoInDim = ((chunkPos[i] - _dims[i].getStartMin()) / _dims[i].getChunkInterval());
            pos *= numChunksInDim;
            pos += chunkNoInDim;
        }
        return pos;
    }

    /**
     * Decode a chunkPos from an integer.
     * @param pos  an integer calculated from a previous call to chunkPos2pos
     * @param[out] chunkPos  a chunk position with prealocated space.
     */
    void pos2chunkPos(position_t pos, CoordinateRange chunkPos ) const
    {
        assert(chunkPos.size() == _dims.size());

        for (size_t i=_dims.size(); i>0; ) {
            --i;
            size_t numChunksInDim = (_dims[i].getEndMax() - _dims[i].getStartMin() + _dims[i].getChunkInterval() - 1) / _dims[i].getChunkInterval();
            size_t chunkNoInDim = pos % numChunksInDim;
            chunkPos[i] = chunkNoInDim * _dims[i].getChunkInterval() + _dims[i].getStartMin();
            pos /= numChunksInDim;
        }
        assert(pos == 0);
    }
};

}

#endif /* ARRAY_COORDINATES_MAPPER_H */
