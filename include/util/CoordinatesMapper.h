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
 * CoordinatesMapper.h
 *
 *  Created on: Aug 8, 2013
 *      Author: sfridella
 */

#ifndef COORDINATES_MAPPER_H_
#define COORDINATES_MAPPER_H_

#include <array/Array.h>
#include <assert.h>
#include <boost/shared_ptr.hpp>
#include <boost/scoped_array.hpp>



namespace scidb
{

/**
 * Map coordinates to offset within chunk
 */
class CoordinatesMapper
{
protected:
    size_t      _nDims;
    uint64_t    _logicalChunkSize;
    Coordinates _origin;
    Coordinates _chunkIntervals;

    // Internal init function that is shared by the constructors.
    void init(Coordinates const& firstPosition, Coordinates const& lastPosition)
    {
        assert(firstPosition.size() == lastPosition.size());
        assert(firstPosition.size() > 0);

        _origin = firstPosition;
        _nDims = _origin.size();
        _chunkIntervals.resize(_nDims);
        _logicalChunkSize = 1;

        for (size_t i = 0; i < _nDims; i++)
        {
            assert ( lastPosition[i] >= _origin[i] );
            _chunkIntervals[i] = lastPosition[i]-_origin[i]+1;
            _logicalChunkSize *= _chunkIntervals[i];
        }

        assert(_origin.size()>0);
    }

public:
    /**
     * Constructor.
     * @param  chunk  A chunk from which the first position and last position will be used.
     */
    CoordinatesMapper(ConstChunk const& chunk)
    {
        init(chunk.getFirstPosition(true), chunk.getLastPosition(true)); // true - withOverlap
    }

    /**
     * Constructor.
     * @param  firstPosition  The first position of the chunk.
     * @param  lastPosition   The last position of the chunk.
     */
    CoordinatesMapper(Coordinates const& firstPosition, Coordinates const& lastPosition)
    {
        init(firstPosition, lastPosition);
    }

    /**
     * Constructor
     * @param  chunkPos    The chunk position without overlap.
     * @param  dims        The dimensions.
     */
    CoordinatesMapper(Coordinates const& chunkPos, Dimensions const& dims)
    {
        init(computeFirstChunkPosition(chunkPos, dims), computeLastChunkPosition(chunkPos, dims));
    }

    /**
     * Default copy Constructor
     */
    /**
     * Deafult assignment operator
     */

    /// convert logical chunk position (in row-major order)  to array coordinates
    void pos2coord(position_t pos, Coordinates& coord) const
    {
        assert(pos>=0);
        assert(!_origin.empty());
        assert(_nDims>0);
        coord.resize(_nDims);

        if (_nDims == 1) {
            coord[0] = _origin[0] + pos;
            assert(pos < _chunkIntervals[0]);
        } else if (_nDims == 2) {
            coord[1] = _origin[1] + (pos % _chunkIntervals[1]);
            pos /= _chunkIntervals[1];
            coord[0] = _origin[0] + pos;
            assert(pos < _chunkIntervals[0]);
        } else {
            for (ssize_t i = static_cast<ssize_t>(_nDims); --i >= 0;) {
                coord[i] = _origin[i] + (pos % _chunkIntervals[i]);
                pos /= _chunkIntervals[i];
            }
            assert(pos == 0);
        }
    }

    /// convert array coordinates to the logical chunk position (in row-major order)
    position_t coord2pos(Coordinates const& coord) const
    {
        assert(coord.size() == _nDims);
        position_t pos(-1);

        if (_nDims == 1) {
            pos = coord[0] - _origin[0];
            assert(pos < _chunkIntervals[0]);
        } else if (_nDims == 2) {
            pos = (coord[0] - _origin[0])*_chunkIntervals[1] + (coord[1] - _origin[1]);
        } else {
            pos = 0;
            for (size_t i = 0, n = _nDims; i < n; i++) {
                pos *= _chunkIntervals[i];
                pos += coord[i] - _origin[i];
            }
        }
        assert(pos >= 0 && static_cast<uint64_t>(pos)<_logicalChunkSize);
        return pos;
    }

    /**
     * Retrieve the number of dimensions used by the mapper.
     * @return the number of dimensions
     */
    size_t getNumDims() const
    {
        return _nDims;
    }

    /**
     * Retrieve the chunkinterval for the given dimension
     * @return the chunkinterval for the given dimension
     */
    Coordinate const& getChunkInterval(size_t dim) const
    {
        return _chunkIntervals[dim];
    }
};

/**
 * Map coordinates to offset within chunk and vice versa, while the chunk is given at run time.
 * The difference from CoordinatesMapper is that this object can be created once and reused for all chunks in the array.
 */
class ArrayCoordinatesMapper
{
protected:
    Dimensions  _dims;

public:
    /**
     * Constructor.
     * @param  dims   the dimensions
     */
    ArrayCoordinatesMapper(Dimensions const& dims)
        : _dims(dims)
    {
        assert(dims.size()>0);
    }

    /**
     * Convert chunkPos to lows and intervals.
     * Without overlap, lows[i] = chunkPos[i]; but with overlap, lows[i] may be smaller (but can't be smaller than dims[i].getStart().
     * Similar with highs[i] (may be larger than the last logical coord, but can't be larger than dims[i].getEndMax().
     * The interval is just highs[i]-lows[i]+1.
     * @param chunkPos   the chunk position
     * @param lows       [out] the low point
     * @param intervals  [out] the intervals of each dim
     */
    inline void chunkPos2LowsAndIntervals(Coordinates const& chunkPos, Coordinates& lows, Coordinates& intervals) const
    {
        assert(chunkPos.size()==_dims.size());
        assert(lows.size()==_dims.size());
        assert(intervals.size()==_dims.size());

        for (size_t i=0; i<_dims.size(); ++i) {
            lows[i] = chunkPos[i] - _dims[i].getChunkOverlap();
            if (lows[i] < _dims[i].getStart()) {
                lows[i] = _dims[i].getStart();
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
    void pos2coord(Coordinates const& chunkPos, position_t pos, Coordinates& coord) const
    {
        assert(pos>=0);
        Coordinates lows(chunkPos.size());
        Coordinates intervals(chunkPos.size());
        chunkPos2LowsAndIntervals(chunkPos, lows, intervals);
        pos2coordWithLowsAndIntervals(lows, intervals, pos, coord);
    }

    /**
     * Upon repeated call to pos2coord with the same chunkPos, the performance can be improved by
     * converting chunkPos to lows and intervals, then call pos2coord with lows and intervals.
     * @param lows       the low point of the chunk
     * @param intervals  the intervals of the chunk
     * @param pos        position in a chunk, as returned by coord2pos
     * @param coord      [out] the coordinate
     */
    void pos2coordWithLowsAndIntervals(Coordinates const& lows, Coordinates const& intervals, position_t pos, Coordinates& coord) const
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
    position_t coord2pos(Coordinates const& chunkPos, Coordinates const& coord) const
    {
        Coordinates lows(_dims.size());
        Coordinates intervals(_dims.size());
        chunkPos2LowsAndIntervals(chunkPos, lows, intervals);
        return coord2posWithLowsAndIntervals(lows, intervals, coord);
    }

    /**
     * Upon repeated call to coord2ps with the same chunkPos, the performance can be improved by
     * converting chunkPos to lows and intervals, then call coord2pos with lows and intervals.
     * @param lows       the low point of the chunk
     * @param intervals  the intervals of the chunk
     * @param coord      the coord in the chunk
     * @return the position
     */
    position_t coord2posWithLowsAndIntervals(Coordinates const& lows, Coordinates const& intervals, Coordinates const& coord) const
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
};

}

#endif /* COORDINATES_MAPPER_H */
