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
#ifndef SPARSE_ACCUMULATOR_UTILS_H_
#define SPARSE_ACCUMULATOR_UTILS_H_
/*
 * SpAccumulatorUtils.h
 *
 *  Created on: Nov 4, 2013
 */


// scidb
#include "array/Array.h"
// local
#include "SpAccumulator.h"
#include "SpgemmTimes.h"

namespace scidb
{

/** @file **/
/**
 * Copy non-zero elements from the Sparse Accumulator (SPA) to the optionally provided chunk, consuming
 * them as they are traversed (which optimizes cache re-use for implementations of SpAccumulator that
 * group their "in use" flags near their index values.)  On return from this function, the SPA
 * will be reset to state containing no no-zeros, ready for re-use to accumulate another row.
 *
 * It is important to re-use an SPA as much as possible since its creation time is O(n), n=logical size of the SPA,
 * but each time it is used, it is likely to accumulate only O(1) values, since matrices may have only O(1) values per row.
 *
 * @param spa              a SpAccumulator<Val_tt, OpAdd_tt>, where Val_tt is the C++ type corresponding to the scidbTypeEnum and scidbType arguments
 * @param rowNum           the row Coordinate of the row in the output array.
 * @param resultArray      the Array from which new resultChunks will be allocated
 * @param resultChunkIn    NULL if there is no current chunk, othewise, a pointer to the chunk returned from a prior call.
 * @param chunkPos         the Coordinates of the chunk, if chunk creation is required.
 * @param scidbTypeEnum    ScidbTypeEnum of the attribute.
 * @param scidbType        ScidbType of the attribute.
 * @param query            current query
 * @return                 resultChunkIn, or if null and the SPA is not empty, a newly created chunk.
 */
template<class IdAdd_tt, class SpAccumulator_tt>
shared_ptr<scidb::ChunkIterator>
spAccumulatorFlushToChunk(SpAccumulator_tt& spa, scidb::Coordinate rowNum,
                          shared_ptr<scidb::ArrayIterator>& resultArray, shared_ptr<scidb::ChunkIterator> resultChunkIn, scidb::Coordinates chunkPos,
                          scidb::TypeEnum scidbTypeEnum, scidb::Type scidbType, shared_ptr<scidb::Query>& query, SpgemmTimes& times)
{
    typedef typename SpAccumulator_tt::Val_t Val_t;

    using namespace boost;
    using namespace scidb;

    // quick return if there is nothing to write, to avoid cluttering the caller with a test.
    if(spa.empty()) return resultChunkIn ;

   // sort _indicesUsed, so we can output the row in-order
    times.blockMultSPAFlushSortStart();
    spa.sort();
    times.blockMultSPAFlushSortStop();

    // now iterate over the row's indices and values, writing only non-zeros values
    Coordinates cellCoords(2);
    cellCoords[0] = rowNum;   // loop invariant

    for (typename SpAccumulator_tt::iterator it = spa.begin(); it != spa.end(); ++it)
    {
#if DBG_TIMING
        double topStart=getDbgMonotonicrawSecs() ;
#endif
        typename SpAccumulator_tt::IdxValPair spaPair = it.consume();
        if (spaPair.value == IdAdd_tt::value()) {
            continue; // 'zeros' can be formed in the accumulator by cancellation, and should not be present in the output.
        }

        if (!resultChunkIn) { // must allocate a chunk to hold the non-zero
            Chunk& resultChunk = resultArray->newChunk(chunkPos);
            resultChunkIn = resultChunk.getIterator(query, ChunkIterator::SEQUENTIAL_WRITE);
        }

        // setpos
        cellCoords[1] = spaPair.index;
        bool retSetPosition = resultChunkIn->setPosition(cellCoords);
        SCIDB_ASSERT(retSetPosition);

#if DBG_TIMING
        times.blockMultSPAFlushTopHalfSecs.back() += (getDbgMonotonicrawSecs() - topStart) ;
#endif

#if DBG_TIMING
        double itemStart=getDbgMonotonicrawSecs() ;
#endif
        // writeItem
        scidb::Value dbVal(scidbType);
        dbVal.set<Val_t>(spaPair.value);
        resultChunkIn->writeItem(dbVal);
#if DBG_TIMING
        times.blockMultSPAFlushSetValWriteItemSecs.back() += (getDbgMonotonicrawSecs() - itemStart) ;
#endif
    }

    times.blockMultSPAFlushClearStart();
    spa.clearIndices();  // _valsUsed was cleared as values were read, this is cleared in O(1) time afterwards
                         // at this point, the SPA is ready for re-use
    times.blockMultSPAFlushClearStop();

    return resultChunkIn;
}

} // end namespace scidb

#endif // SPARSE_ACCUMULATOR_UTILS_H__

