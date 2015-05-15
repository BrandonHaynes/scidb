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
 * SGChunkReceiver.cpp
 *
 *  Created on: Jun 5, 2013
 *      Author: Donghui
 */

#include <query/SGChunkReceiver.h>
#include <boost/scoped_ptr.hpp>
using namespace boost;

namespace scidb
{
/**
 * Given a chunk received over the network, initialize a MemChunk.
 * @param[inout] pTmpChunk the pre-existing MemChunk, to be filled using data in compressedBuffer
 * @param[out] pinTmpChunk the PinBuffer to protect pTmpChunk
 * @param[in]  array       the array
 * @param[in]  coordinates the chunk position
 * @param[in]  attributeID
 * @param[in]  compMethod  the compression method
 * @param[in]  sparse
 * @param[in]  rle
 * @param[inout] compressedBuffer  the buffer received over the network; the function may free compressedBuffer->data.
 *
 * @note MemChunk::decompress(CompressedBuffer const& compressedBuffer) may result at compressedBuffer->data be freed and set to NULL.
 */
void initMemChunkFromNetwork(
        shared_ptr<MemChunk>& pTmpChunk,
        shared_ptr<PinBuffer>& pinTmpChunk,
        shared_ptr<Array> const& array,
        Coordinates const& coordinates,
        AttributeID attributeID,
        int compMethod,
        shared_ptr<CompressedBuffer>& compressedBuffer
        )
{
    assert(pTmpChunk);
    assert(compressedBuffer && compressedBuffer->getData());

    pinTmpChunk = make_shared<PinBuffer>(*pTmpChunk);
    Address chunkAddr(attributeID, coordinates);
    pTmpChunk->initialize(array.get(), &array->getArrayDesc(), chunkAddr, compMethod);
    pTmpChunk->decompress(*compressedBuffer);
}

SGChunkReceiver::SGChunkReceiver(
        bool cachingLastEmptyBitmap,
        bool cachingReceivedChunks,
        ArrayDesc const& schema,
        boost::shared_ptr<Query>& query
        ):
    _cachingLastEmptyBitmap(cachingLastEmptyBitmap),
    _cachingReceivedChunks(cachingReceivedChunks),
    _nInstances(query->getInstancesCount()),
    _nAttrs(schema.getAttributes().size()),
    _myInstanceID(query->getInstanceID()),
    _schema(schema),
    _query(query),
    _lastEmptyBitmapCache(cachingLastEmptyBitmap ? query->getInstancesCount() : 0),
    _arrayIteratorsForReceivedChunkCache(
            cachingReceivedChunks ? query->getInstancesCount() * schema.getAttributes().size() : 0)
{
    assert(!cachingReceivedChunks || !_cachingLastEmptyBitmap); // can't cache both; fine to cache none

    // The received-chunk cache.
    if (cachingReceivedChunks) {
        _receivedChunkCache.resize(_nInstances);
        _arrayIteratorsForReceivedChunkCache.resize(_nInstances * _nAttrs);
        for (InstanceID i=0; i<_nInstances; ++i) {
            if (i == _myInstanceID) {
                continue;
            }
            _receivedChunkCache[i] = make_shared<MemArray>(schema, query);
            for (AttributeID a=0; a<_nAttrs; ++a) {
                _arrayIteratorsForReceivedChunkCache[indexInstAttr(i, a)] = _receivedChunkCache[i]->getIterator(a);
            }
        }
    }
}

void SGChunkReceiver::setCachedEmptyBitmapChunk(
        size_t sourceId, boost::shared_ptr<MemChunk>& bitmapChunk, Coordinates const& coordinates)
{
    assert(_cachingLastEmptyBitmap);
    assert(_lastEmptyBitmapCache.size()>sourceId);

    _lastEmptyBitmapCache[sourceId]._pin = make_shared<PinBuffer>(*bitmapChunk);
    _lastEmptyBitmapCache[sourceId]._chunk = bitmapChunk;
    _lastEmptyBitmapCache[sourceId]._bitmap = bitmapChunk->getEmptyBitmap();

    assert(_lastEmptyBitmapCache[sourceId].exists());
}

/**
 * Retrieve a cached bitmap.
 * @param[in]  sourceId              the sender of the bitmap chunk
 * @param[in]  expectedCcoordinates  the coordinates of the real-attribute chunk; must match those of the bitmap chunk!
 * @return     the bitmap
 */
boost::shared_ptr<ConstRLEEmptyBitmap> SGChunkReceiver::getCachedEmptyBitmap(
        size_t sourceId, Coordinates const& expectedCoordinates)
{
    assert(_cachingLastEmptyBitmap);
    assert(_lastEmptyBitmapCache.size()>sourceId);
    assert(_lastEmptyBitmapCache[sourceId].exists());

    return _lastEmptyBitmapCache[sourceId]._bitmap;
}

// We use a two-level MultiConstIterators.
// Note that each MultiConstIterators is a wrapper for a vector of ConstIterators.
// The root-level MultiConstIterators includes a vector of second-level MultiConstIterators, one per sender instance.
// Each second-level MultiConstIterators includes a vector of MemArrayIterators, one per attribute.
//
// The benefit of synchronously scanning multiple attributes of an input array (from one particular sender instance) is to reuse the
// empty-bitmap chunk again and again.
// The benefit of synchronously scanning chunks from multiple senders (for one particular attribute) is to try to reuse the output chunk,
// before it is flushed to disk.
//
// So the number of open chunks is O(nInstances * nAttrs).
// TO-DO:
// In the future, when we scale to many instances and many attributes, we could modify the algorithm to use multiple passes.
// We could process a subset of sender instances in each pass.
// We could also process a subset of the attributes in each pass (loading the empty-bitmap chunks again and again for different passes).
//
void SGChunkReceiver::generateOutputFromReceivedChunkCache()
{
    assert(_cachingReceivedChunks);
    assert(_receivedChunkCache.size() == _nInstances);

    // Close the received-chunk cache -- we will create read iterators below.
    _arrayIteratorsForReceivedChunkCache.clear();

    shared_ptr<Query> query(Query::getValidQueryPtr(_query));

    boost::shared_ptr<SGContext> sgCtx = dynamic_pointer_cast<SGContext>(query->getOperatorContext());
    if (sgCtx == NULL) {
        throw (SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_UNKNOWN_CTX)
               << typeid(*query->getOperatorContext()).name());
    }

    vector<shared_ptr<ArrayIterator> > outputIters(_nAttrs); // assigned later, under the mutex protection

    // the two-level MultiConstIterators

    vector<shared_ptr<ConstIterator> > inputItersParent(_nInstances);  // each ConstIterator is a second-level MultiConstIterators

    vector<vector<shared_ptr<ConstIterator> > > inputItersChildren(_nInstances); // each ConstIterator is a MemArrayIterator

    for (InstanceID i=0; i<_nInstances; ++i) {
        if (i == _myInstanceID) {
            continue;
        }
        inputItersChildren[i].resize(_nAttrs);
        for (AttributeID a=0; a<_nAttrs; ++a) {
            inputItersChildren[i][a] = shared_ptr<MemArrayIterator>(new MemArrayIterator(*_receivedChunkCache[i], a));
        }
        inputItersParent[i] = shared_ptr<MultiConstIterators>(new MultiConstIterators(inputItersChildren[i]));
    }

    // Scan through the MemArrays across sender instances synchronously.
    for (MultiConstIterators multiIters(inputItersParent); !multiIters.end(); ++multiIters) {
        const Coordinates& coordinates = multiIters.getPosition();  // the min chunk position among the sender instances

        vector<size_t> IDs;
        multiIters.getIDsAtMinPosition(IDs);

        for (vector<size_t>::const_iterator it = IDs.begin(); it != IDs.end(); ++it) {
            InstanceID i = *it;

            // Get the MultiConstIterators for sender instance i.
            shared_ptr<MultiConstIterators> multiItersChild = dynamic_pointer_cast<MultiConstIterators>(inputItersParent[i]);

            // sanity check: we already know this instance "has data" (at the current chunk position).
            assert(! multiItersChild->end());
            assert(coordinatesCompare(coordinates, multiItersChild->getPosition()) == 0);

            generateOutputForOneInstance(multiItersChild, inputItersChildren[i], outputIters, coordinates, sgCtx, query);
        } // while (it != IDs.end())
    } // while (!multiIters.end())
}

//
// Generate output for one instance.
// Note that the MultiConstIterators object is not incremented here.
// The increment will be triggered via the outside loop
//
void SGChunkReceiver::generateOutputForOneInstance(
        shared_ptr<MultiConstIterators> multiItersChild,
        vector<shared_ptr<ConstIterator> > & inputIters,
        vector<shared_ptr<ArrayIterator> > & outputIters,
        Coordinates const& coordinates,
        shared_ptr<SGContext>& sgCtx,
        shared_ptr<Query>& query
        )
{
    bool isEmptyable = (_schema.getEmptyBitmapAttribute() != NULL);
    shared_ptr<Array> outputArray = sgCtx->_resultSG;

    // sharedEmptyBitmap is the empty bitmap at a given chunkPos, to be shared by all the attributes.
    shared_ptr<ConstRLEEmptyBitmap> sharedEmptyBitmap;

    // Scan through the attributes of this sender instance synchronously, at this chunk position.
    vector<size_t> IDs;
    multiItersChild->getIDsAtMinPosition(IDs);

    for (vector<size_t>::const_reverse_iterator it = IDs.rbegin(); it != IDs.rend(); ++it) {
        AttributeID attrId = *it;
        Query::validateQueryPtr(query);

        const ConstChunk& chunk = dynamic_pointer_cast<ConstArrayIterator>(inputIters[attrId])->getChunk();

        bool isEmptyIndicator = (isEmptyable && attrId+1 == _nAttrs);

        if (chunk.getSize() == 0) {
            continue;
        }

        if (isEmptyable) {
            if (attrId+1 == _nAttrs) {
                assert(!sharedEmptyBitmap);
                sharedEmptyBitmap = chunk.getEmptyBitmap();
            }
            assert(sharedEmptyBitmap);
        }

        // There is no need to synchronize, because this is from a single thread.
        // As a comparison, in redistributeAggregate(), synchronization is needed because fragments of a chunk
        // may be received over the network.
        //
        //ScopedMutexLock cs(query->resultCS);

        if (!outputIters[attrId]) {
            outputIters[attrId] = outputArray->getIterator(attrId);
        }

        if (outputIters[attrId]->setPosition(coordinates)) {
            const ConstChunk* srcChunk = &chunk;
            scoped_ptr<PinBuffer> pinClosure;
            MemChunk closure;

            if (isEmptyable && !isEmptyIndicator) {
                shared_ptr<ConstRLEEmptyBitmap> emptyBitmap;
                emptyBitmap = sharedEmptyBitmap;
                pinClosure.reset(new PinBuffer(closure));
                PinBuffer pinChunk(chunk);
                closure.initialize(chunk);
                chunk.makeClosure(closure, emptyBitmap);
                srcChunk = &closure;
            }

            Chunk& dstChunk = outputIters[attrId]->updateChunk();
            std::vector<AggregatePtr> const& aggs = sgCtx->_aggregateList;
            if (aggs[attrId].get())
            {
                if (_schema.getEmptyBitmapAttribute() == NULL)
                {
                    dstChunk.nonEmptyableAggregateMerge(*srcChunk, aggs[attrId], query);
                }
                else
                {
                    dstChunk.aggregateMerge(*srcChunk, aggs[attrId], query);
                }
            }
            else
            {
                dstChunk.merge(*srcChunk, query);
            }
        }
        else
        {
            shared_ptr<ConstRLEEmptyBitmap> emptyBitmap;
            if (isEmptyable && !isEmptyIndicator) {
                emptyBitmap = sharedEmptyBitmap;
            }
            outputIters[attrId]->copyChunk(chunk, emptyBitmap);
        } // if (outputIters[attrId]->setPosition(coordinates))
    } // for (vector<size_t>::const_reverse_iterator it = IDs.rbegin(); it != IDs.rend(); ++it)
}

void SGChunkReceiver::processReceivedChunkAtExistingPos(
        boost::shared_ptr<SGContext>& sgCtx,
        bool isAggregateChunk,
        InstanceID sourceId,
        boost::shared_ptr<CompressedBuffer>& compressedBuffer,
        int compMethod,
        size_t decompressedSize,
        AttributeID attributeID,
        size_t count,
        Coordinates& coordinates,
        boost::shared_ptr<Array> outputArray,
        boost::shared_ptr<ArrayIterator> outputIter,
        boost::shared_ptr<Query>& query,
        bool isEmptyIndicator,
        bool isEmptyable
        )
{
    // temporary MemArray objects
    shared_ptr<MemChunk> pTmpChunk = make_shared<MemChunk>();  // make it a shared pointer, because the bitmap chunk needs to be preserved across threads.
    MemChunk closure;

    // the PinBuffer objects protect tmpChunk and closure, respectively.
    shared_ptr<PinBuffer> pinTmpChunk, pinClosure;

    Chunk* outChunk = &outputIter->updateChunk();

    if (! isAggregateChunk) {
        outChunk->setCount(0); // unknown
    }

    // If there is no local chunk at the position already, and if src chunk is not compressed, copy from the source chunk.
    char* dst = static_cast<char*>(outChunk->getData());
    if (dst == NULL && compMethod == 0)
    {
        char const* src = (char const*)compressedBuffer->getData();

        // Special care is needed if _cachingLastEmptyBitmap.
        // - If this is the empty bitmap, store it in the SGContext.
        // - Otherwise, add the empty bitmap from the SGContext to the chunk's data.
        if (_cachingLastEmptyBitmap) {
            initMemChunkFromNetwork(pTmpChunk, pinTmpChunk, outputArray, coordinates, attributeID,
                                    compMethod, compressedBuffer);

            if (isEmptyIndicator) {
                setCachedEmptyBitmapChunk(sourceId, pTmpChunk, coordinates);
            } else {
                shared_ptr<ConstRLEEmptyBitmap> cachedBitmap = getCachedEmptyBitmap(sourceId, coordinates);
                assert(cachedBitmap);
                pinClosure = make_shared<PinBuffer>(closure);
                closure.initialize(*pTmpChunk);
                pTmpChunk->makeClosure(closure, cachedBitmap);
                src = static_cast<char const*>(closure.getData());
            }
        }

        outChunk->allocateAndCopy(src, decompressedSize, count, query);
    }
    // Otherwise, perform chunk merge.
    else {
        initMemChunkFromNetwork(pTmpChunk, pinTmpChunk, outputArray, coordinates, attributeID,
                compMethod, compressedBuffer);

        ConstChunk const* srcChunk = &(*pTmpChunk);

        // Special care is needed if _cachingLastEmptyBitmap.
        // - If this is the empty bitmap, store it in the SGContext.
        // - Otherwise, add the empty bitmap from the SGContext to the chunk's data.
        if (_cachingLastEmptyBitmap) {
            if (isEmptyIndicator) {
                setCachedEmptyBitmapChunk(sourceId, pTmpChunk, coordinates);
            } else {
                shared_ptr<ConstRLEEmptyBitmap> cachedBitmap = getCachedEmptyBitmap(sourceId, coordinates);
                assert(cachedBitmap);
                pinClosure = make_shared<PinBuffer>(closure);
                closure.initialize(*pTmpChunk);
                pTmpChunk->makeClosure(closure, cachedBitmap);
                srcChunk = &closure;
            }
        }

        if (isAggregateChunk) {
            AggregatePtr aggregate = sgCtx->_aggregateList[attributeID];
            if (!isEmptyable) {
                assert(!_cachingLastEmptyBitmap);
                assert(srcChunk==&(*pTmpChunk));
                outChunk->nonEmptyableAggregateMerge(*srcChunk, aggregate, query);
            } else {
                outChunk->aggregateMerge(*srcChunk, aggregate, query);
            }
        } else {
            outChunk->merge(*srcChunk, query);
        }
    }

    checkChunkMagic(*outChunk);
}

void SGChunkReceiver::processReceivedChunkAtNewPos(
        boost::shared_ptr<SGContext>& sgCtx,
        bool isAggregateChunk,
        InstanceID sourceId,
        boost::shared_ptr<CompressedBuffer>& compressedBuffer,
        int compMethod,
        size_t decompressedSize,
        AttributeID attributeID,
        size_t count,
        Coordinates& coordinates,
        boost::shared_ptr<Array> outputArray,
        boost::shared_ptr<ArrayIterator> outputIter,
        boost::shared_ptr<Query>& query,
        bool isEmptyIndicator,
        bool isEmptyable
        )
{
    // temporary MemArray objects
    shared_ptr<MemChunk> pTmpChunk = make_shared<MemChunk>();  // make it a shared pointer, because the bitmap chunk needs to be preserved across threads.
    MemChunk closure;

    // the PinBuffer objects protect tmpChunk and closure, respectively.
    shared_ptr<PinBuffer> pinTmpChunk, pinClosure;

    Chunk* outChunk = &outputIter->newChunk(coordinates);
    shared_ptr<CompressedBuffer> myCompressedBuffer = compressedBuffer;

    // Special care is needed if _cachingLastEmptyBitmap.
    // - If this is the empty bitmap, store it in the SGContext.
    // - Otherwise, add the empty bitmap from the SGContext to the chunk's data.
    if (_cachingLastEmptyBitmap) {
        initMemChunkFromNetwork(pTmpChunk, pinTmpChunk, outputArray, coordinates, attributeID,
                compMethod, compressedBuffer);
        if (isEmptyIndicator) {
            setCachedEmptyBitmapChunk(sourceId, pTmpChunk, coordinates);
        } else {
            shared_ptr<ConstRLEEmptyBitmap> cachedBitmap = getCachedEmptyBitmap(sourceId, coordinates);
            assert(cachedBitmap);
            myCompressedBuffer = make_shared<CompressedBuffer>();
            pTmpChunk->compress(*myCompressedBuffer, cachedBitmap);
        }
    }
    outChunk->decompress(*myCompressedBuffer);
    outChunk->setCount(count);
    outChunk->write(query);

    checkChunkMagic(*outChunk);
}

void SGChunkReceiver::handleReceivedChunk(
        shared_ptr<SGContext>& sgCtx,
        bool isAggregateChunk,
        InstanceID sourceId,
        shared_ptr<CompressedBuffer>& compressedBuffer,
        int compMethod,
        size_t decompressedSize,
        AttributeID attributeID,
        size_t count,
        Coordinates& coordinates
        )
{
    shared_ptr<Query> query(Query::getValidQueryPtr(_query));

    shared_ptr<Array> outputArray = sgCtx->_resultSG;

    const ArrayDesc& desc = outputArray->getArrayDesc();
    const bool isEmptyable = (desc.getEmptyBitmapAttribute() != NULL);
    const bool isEmptyIndicator = isEmptyable && (attributeID+1==desc.getAttributes().size());

    if (isAggregateChunk) {
        assert(! isEmptyIndicator);
    }

    if (!isAggregateChunk && sgCtx->_targetVersioned)
    {
        sgCtx->_newChunks.insert(coordinates);
    }

    // special case when no data is received
    if (!compressedBuffer) {
        return;
    }

    assert(compressedBuffer->getData());

    compressedBuffer->setCompressionMethod(compMethod);
    compressedBuffer->setDecompressedSize(decompressedSize);

    shared_ptr<ArrayIterator> outputIter = outputArray->getIterator(attributeID);

    if ( _cachingReceivedChunks ) { // don't merge yet; cache instead
        // The chunk cannot already exist in the received-chunk cache.
        assert( ! _arrayIteratorsForReceivedChunkCache[indexInstAttr(sourceId, attributeID)]->setPosition(coordinates));

        Chunk* outChunk = &_arrayIteratorsForReceivedChunkCache[indexInstAttr(sourceId, attributeID)]->newChunk(coordinates);
        shared_ptr<CompressedBuffer> myCompressedBuffer = compressedBuffer;
        outChunk->decompress(*myCompressedBuffer);
        outChunk->setCount(count);
        outChunk->write(query);
        checkChunkMagic(*outChunk);
    } else if (outputIter->setPosition(coordinates)) { // merge into an existing chunk
        processReceivedChunkAtExistingPos(
                sgCtx,
                isAggregateChunk,
                sourceId,
                compressedBuffer,
                compMethod,
                decompressedSize,
                attributeID,
                count,
                coordinates,
                outputArray,
                outputIter,
                query,
                isEmptyIndicator,
                isEmptyable
                );
    } else { // merge into a new chunk
        processReceivedChunkAtNewPos(
                sgCtx,
                isAggregateChunk,
                sourceId,
                compressedBuffer,
                compMethod,
                decompressedSize,
                attributeID,
                count,
                coordinates,
                outputArray,
                outputIter,
                query,
                isEmptyIndicator,
                isEmptyable
                );
    } // end if (outputIter->setPosition(coordinates))
}

} // namespace
