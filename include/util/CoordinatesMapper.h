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

#ifndef COORDINATES_MAPPER_H_
#define COORDINATES_MAPPER_H_

/****************************************************************************/

#include <array/Coordinate.h>

/****************************************************************************/
namespace scidb {
/****************************************************************************/

class ConstChunk;

/**
 *  Map coordinates to offset within chunk.
 */
class CoordinatesMapper
{
protected:
    size_t      _nDims;
    uint64_t    _logicalChunkSize;
    Coordinates _origin;
    Coordinates _chunkIntervals;

    // Internal init function that is shared by the constructors.
    void init(CoordinateCRange,CoordinateCRange);

public:
    /**
     *  Construct a mapper from the first and last positions within a chunk.
     *
     *  @param  f   The first position of the chunk.
     *  @param  l   The last position of the chunk.
     */
    CoordinatesMapper(CoordinateCRange f,CoordinateCRange l)
    {
        init(f,l);
    }

    /**
     *  Construct a mapper fromt the first and last positions within a chunk.
     *
     *  @param  chunk  A chunk from which the first position and last position will be used.
     */
    CoordinatesMapper(ConstChunk const& chunk);

    /**
     *  Convert a logical chunk position in row-major order to array coordinates.
     */
    void pos2coord(position_t pos,Coordinates& coord) const
    {
        assert(pos >= 0);

        coord.resize(_nDims);

        pos2coord(pos,pointerRange(coord));
    }

    /**
     *  Convert logical chunk position (in row-major order)  to array coordinates
     */
    void pos2coord(position_t pos,CoordinateRange coord) const
    {
        assert(pos >= 0);
        assert(coord.size() == _nDims);

        if (_nDims == 1) {
            coord[0] = _origin[0] + pos;
            assert(pos < _chunkIntervals[0]);
        } else if (_nDims == 2) {
            coord[1] = _origin[1] + (pos % _chunkIntervals[1]);
            pos /= _chunkIntervals[1];
            coord[0] = _origin[0] + pos;
            assert(pos < _chunkIntervals[0]);
        } else {
            for (int i=_nDims; --i>=0;) {
                coord[i] = _origin[i] + (pos % _chunkIntervals[i]);
                pos /= _chunkIntervals[i];
            }
            assert(pos == 0);
        }
    }

    /**
     *  Convert array coordinates to the logical chunk position (in row-major order)
     */
    position_t coord2pos(CoordinateCRange coord) const
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
     *  Return the number of dimensions used by the mapper.
     *
     *  @return the number of dimensions
     */
    size_t getNumDims() const
    {
        return _nDims;
    }

    /**
     *  Return the chunk interval for the given dimension.
     *
     *  @return the chunk interval for the given dimension
     */
    Coordinate getChunkInterval(size_t dim) const
    {
        return _chunkIntervals[dim];
    }
};

/****************************************************************************/
}
/****************************************************************************/
#endif
/****************************************************************************/
