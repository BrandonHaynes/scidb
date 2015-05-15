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

/****************************************************************************/

#include <util/CoordinatesMapper.h>
#include <array/Array.h>

/****************************************************************************/
namespace scidb {
/****************************************************************************/

CoordinatesMapper::CoordinatesMapper(ConstChunk const& chunk)
{
    init(chunk.getFirstPosition(true),
         chunk.getLastPosition(true)); // true - withOverlap
}

void CoordinatesMapper::init(CoordinateCRange f,CoordinateCRange l)
{
    assert(f.size() == l.size());
    assert(!f.empty());

    _origin.assign(f.begin(),f.end());
    _nDims  = f.size();
    _chunkIntervals.resize(f.size());
    _logicalChunkSize = 1;

    for (size_t i=0; i!=_nDims; ++i)
    {
        assert(l[i] >= _origin[i]);

        _chunkIntervals[i] = l[i] - _origin[i] + 1;
        _logicalChunkSize *= _chunkIntervals[i];
    }

    assert(!_origin.empty());
}

/****************************************************************************/
}
/****************************************************************************/
