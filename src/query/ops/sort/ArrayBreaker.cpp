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
 * ArrayBreaker.cpp
 *
 *  Created on: Nov 25, 2014
 *      Author: Donghui Zhang
 */

#include <util/SchemaUtils.h>
#include "ArrayBreaker.h"

using namespace boost;
using namespace std;

namespace scidb
{
typedef vector<shared_ptr<ArrayIterator> > ArrayIterators;
typedef vector<shared_ptr<ChunkIterator> > ChunkIterators;

void breakOneArrayIntoMultiple(
        boost::shared_ptr<Array> const& inputArray,
        std::vector<boost::shared_ptr<Array> >& outputArrays,
        boost::shared_ptr<Query>& query,
        BreakerOnCoordinates breaker,
        bool isBreakerConsecutive,
        void* additionalInfo)
{
    SchemaUtils schemaUtils(inputArray);
    size_t nOutputArrays = outputArrays.size();

    // Get nAttrs input array iterators.
    vector<shared_ptr<ConstArrayIterator> > inputArrayIterators(schemaUtils._nAttrsWithET);
    for (size_t attr=0; attr<schemaUtils._nAttrsWithET; ++attr) {
        inputArrayIterators[attr] = inputArray->getConstIterator(attr);
    }

    // For each output array, get nAttrs output array iterators.
    vector<ArrayIterators> outputArrayIterators(nOutputArrays);
    for (size_t i=0; i<nOutputArrays; ++i) {
        outputArrayIterators[i].resize(schemaUtils._nAttrsWithET);
        for (size_t attr = 0; attr<schemaUtils._nAttrsWithET; ++attr) {
            outputArrayIterators[i][attr] = outputArrays[i]->getIterator(attr);
        }
    }

    // This is the result of the call to the breaker() function, telling which instance a cell should be sent to.
    // The value is sent back to the next call to the breaker() function, as a hint where to start searching.
    size_t which = 0;

    // Iterate through the chunks in the input array, using attribute 0.
    while (! inputArrayIterators[0]->end())
    {
        // The chunk position.
        Coordinates const& chunkPos = inputArrayIterators[0]->getPosition();

        // Shortcut: if the whole chunk belongs to one instance, copy the chunk.
        const bool withOverlap = false;
        Coordinates lastPosInChunk = computeLastChunkPosition(chunkPos, inputArray->getArrayDesc().getDimensions(), withOverlap);
        const size_t instForFirstCell = breaker(chunkPos, which, query, schemaUtils._dims, additionalInfo);
        const size_t instForLastCell = breaker(lastPosInChunk, instForFirstCell, query, schemaUtils._dims, additionalInfo);

        if (isBreakerConsecutive && instForFirstCell == instForLastCell) {
            which = instForLastCell;
            for (size_t attr = 0; attr < schemaUtils._nAttrsWithET; attr++)
            {
                boost::shared_ptr<ArrayIterator> dst = outputArrayIterators[which][attr];
                boost::shared_ptr<ConstArrayIterator> src = inputArrayIterators[attr];
                dst->copyChunk(src->getChunk());
                ++(*src);
            }
            continue;
        }

        // Input chunk iterators.
        vector<shared_ptr<ConstChunkIterator> > inputChunkIterators(schemaUtils._nAttrsWithET);
        for (size_t attr=0; attr<schemaUtils._nAttrsWithET; ++attr) {
            inputChunkIterators[attr] = inputArrayIterators[attr]->getChunk().getConstIterator();
        }

        // Declare output chunk iterators, but don't assign values yet. They will be assigned lazily.
        vector<ChunkIterators> outputChunkIterators(nOutputArrays);
        for (size_t i=0; i<nOutputArrays; ++i) {
            outputChunkIterators[i].resize(schemaUtils._nAttrsWithET);
        }

        // Iterate through the cell positions in the chunk.
        while (! inputChunkIterators[0]->end()) {
            Coordinates const& cellPos = inputChunkIterators[0]->getPosition();
            which = breaker(cellPos, which, query, schemaUtils._dims, additionalInfo);
            assert(which < nOutputArrays);

            // Make sure the output array's chunk iterators exist. If not, create new chunks first!
            if (! outputChunkIterators[which][0]) {
                for (size_t attr=0; attr<schemaUtils._nAttrsWithET; ++attr) {
                    outputChunkIterators[which][attr] = outputArrayIterators[which][attr]->newChunk(chunkPos)
                            .getIterator(query, ConstChunkIterator::SEQUENTIAL_WRITE);
                }
            }

            // Copy the items.
            for (size_t attr=0; attr<schemaUtils._nAttrsWithET; ++attr) {
                outputChunkIterators[which][attr]->setPosition(cellPos);
                outputChunkIterators[which][attr]->writeItem( inputChunkIterators[attr]->getItem() );
            }

            // Advance to the next cell.
            for (size_t attr=0; attr<schemaUtils._nAttrsWithET; ++attr) {
                ++(*inputChunkIterators[attr]);
            }
        }

        // Flush all chunks.
        for (size_t i=0; i<nOutputArrays; ++i) {
            if (! outputChunkIterators[i][0]) {
                continue;
            }

            for (size_t attr=0; attr<schemaUtils._nAttrsWithET; ++attr) {
                outputChunkIterators[i][attr]->flush();
                outputChunkIterators[i][attr].reset();
            }
        }

        // Advance to the next chunk in the input array.
        for (size_t attr=0; attr<schemaUtils._nAttrsWithET; ++attr) {
            ++(*inputArrayIterators[attr]);
        }
    } // while (! inputArrayIterators[0]->end())
}

} // namespace scidb
