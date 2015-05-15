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
 * SyntheticDimChunkMerger.cpp
 *
 *  This file contains helper routines for the synthetic dimension.
 */
#include "SyntheticDimChunkMerger.h"

namespace scidb
{

SyntheticDimChunkMerger::SyntheticDimAdjuster::SyntheticDimAdjuster(size_t dimSynthetic,
                                                                    Coordinate dimStartSynthetic)
  : _dimSynthetic(dimSynthetic),
    _dimStartSynthetic(dimStartSynthetic)
{ }

void
SyntheticDimChunkMerger::SyntheticDimAdjuster::updateMapCoordToCount(MemChunk const* chunk,
                                                                     ConstChunkIterator* chunkIter)
{
    // Note that default values can't be ignored. Otherwise the coordinate in the synthetic dimension would mess up.
    shared_ptr<ConstChunkIterator> localIter;
    if (!chunkIter) {
        localIter = chunk->getConstIterator(ChunkIterator::IGNORE_EMPTY_CELLS|
                                            ChunkIterator::APPEND_CHUNK);
        chunkIter = localIter.get();
    }
    while (!chunkIter->end()) {
        updateCount(chunkIter->getPosition());
        ++(*chunkIter);
    }
}

void
SyntheticDimChunkMerger::SyntheticDimAdjuster::updateCount(const Coordinates& coords)
{
    _collapsed = coords;
    useStartForSyntheticDim(_collapsed);
    MapCoordToCount::iterator mapIter = _coord2Count.find(_collapsed);
    if (mapIter==_coord2Count.end()) {
        _coord2Count.insert(std::pair<Coordinates, size_t>(_collapsed, 1));
    } else {
        ++(mapIter->second);
    }
}

void
SyntheticDimChunkMerger::SyntheticDimAdjuster::calcNewCoord(Coordinates& coord)
{
    _collapsed = coord;
    useStartForSyntheticDim(_collapsed);
    MapCoordToCount::iterator mapIter = _coord2Count.find(_collapsed);
    if (mapIter == _coord2Count.end()) {
        return;
    }
    increaseSyntheticDim(coord, mapIter->second);
}

SyntheticDimChunkMerger::SyntheticDimChunkMerger(const RedimInfo* redimInfo,
                                                 size_t numInstances)
: _syntheticDimHelper(redimInfo->_dimSynthetic, redimInfo->_dim.getStartMin()),
  _partialChunks(numInstances)
{
    assert(redimInfo->_hasSynthetic);
}

void
SyntheticDimChunkMerger::clear()
{
    _syntheticDimHelper.clear();
    for(std::vector<shared_ptr<MemChunk> >::iterator i = _partialChunks.begin();
        i !=_partialChunks.end(); ++i) {
        (*i).reset();
    }
    _currChunkPos.clear();
}

bool
SyntheticDimChunkMerger::mergePartialChunk(InstanceID instanceId,
                                           AttributeID attId,
                                           shared_ptr<MemChunk>& chunk,
                                           const shared_ptr<Query>& query)
{
    assert(chunk);
    static const bool withoutOverlap = false;
    if (isDebug()) {
        if (_currChunkPos.empty()) {
            _currChunkPos = chunk->getFirstPosition(withoutOverlap);
        } else {
            assert(_currChunkPos == chunk->getFirstPosition(withoutOverlap));
        }
    }
    assert(instanceId < _partialChunks.size());
    _partialChunks[instanceId].swap(chunk);
    assert(!chunk);
    return false;
}

shared_ptr<MemChunk>
SyntheticDimChunkMerger::getMergedChunk(AttributeID attId,
                                        const shared_ptr<Query>& query)
{
    shared_ptr<MemChunk> result;
    shared_ptr<ChunkIterator> dstIterator;

    for (std::vector<shared_ptr<MemChunk> >::iterator chunkIt =  _partialChunks.begin();
         chunkIt !=  _partialChunks.end(); ++chunkIt) {
        shared_ptr<MemChunk>& chunk = *chunkIt;
        if (!chunk) {
            continue;
        }
        if (!result) {
            result = chunk;
            // During redim, there is always a empty tag, and the chunk can't be sparse.
            assert(result->getArrayDesc().getEmptyBitmapAttribute());
            continue;
        }
        result->setCount(0); // unknown

        if (!dstIterator) {
            _syntheticDimHelper.updateMapCoordToCount(result.get());
            dstIterator = result->getIterator(query,
                                              ChunkIterator::APPEND_CHUNK|
                                              ChunkIterator::APPEND_EMPTY_BITMAP|
                                              ChunkIterator::NO_EMPTY_CHECK);
        }
        mergeChunks(dstIterator, chunk);
        chunk.reset();
    }
    if (dstIterator) {
        dstIterator->flush();
        dstIterator.reset();
    }
    clear();
    checkChunkMagic(*result);
    return result;
}

void
SyntheticDimChunkMerger::mergeChunks(shared_ptr<ChunkIterator>& dstIterator,
                                     shared_ptr<MemChunk>& src)
{
    shared_ptr<ConstChunkIterator> srcIterator = src->getConstIterator(ChunkIterator::IGNORE_EMPTY_CELLS);

    while (!srcIterator->end()) {
        _coord = srcIterator->getPosition();
        _syntheticDimHelper.calcNewCoord(_coord);
        if (!dstIterator->setPosition(_coord)) {
            throw USER_EXCEPTION(SCIDB_SE_OPERATOR, SCIDB_LE_OP_REDIMENSION_STORE_ERROR7);
        }
        Value const& value = srcIterator->getItem();
        dstIterator->writeItem(value);
        ++(*srcIterator);
    }
    srcIterator->reset();
    _syntheticDimHelper.updateMapCoordToCount(src.get(),srcIterator.get());
}

}
