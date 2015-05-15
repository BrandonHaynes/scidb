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
 * SGChunkReceiver.h
 *
 *  Created on: Jun 5, 2013
 *      Author: Donghui
 */

#ifndef SG_CHUNK_RECEIVER_H_
#define SG_CHUNK_RECEIVER_H_

#include <assert.h>
#include <boost/weak_ptr.hpp>
#include <query/Operator.h>
#include <util/MultiConstIterators.h>

namespace scidb
{
typedef std::vector<boost::shared_ptr<ArrayIterator> > ArrayIterators;
typedef std::vector<boost::shared_ptr<ConstArrayIterator> > ConstArrayIterators;

class SGContext;

/**
 * The manager for chunks received during redistribution.
 *
 * CAUTION: The behavior must match Operator.cpp::redistributeAggregate().
 *
 * Several factors control the behavior of a received chunk:
 *   - _cachingReceivedChunks: whether we cache received chunks, instead of merging them into the output array as they arrive.
 *     This applies only to the received chunks: Donghui chose to merge local chunks into the output array as soon as they arrive,
 *     to simplify the code.
 *   - _cachingLastEmptyBitmap: whether the last-received empty bitmap (from each sender instance) needs to be cached.
 *     This applies to empty bitmaps that are received from the network.
 *   - Note that at most one of _cachingReceivedChunks or _cachingLastEmptyBitmpa may be true.
 *   - Whether a chunk is an empty bitmap.
 *
 * The decision flow, to handle a chunk received from the network, is as follows:
 *
 *     if ( _cachingReceivedChunks )
 *         Cache the chunk into _arrayIteratorsForReceivedChunkCache.
 *     else // merge on the fly
 *         if ( _cachingLastEmptyBitmap )
 *             if ( chunk is empty bitmap )
 *                 Cache the chunk into _lastEmptyBitmapCache.
 *             else
 *                 Retrieve the matching empty bitmap.
 *             endif
 *         endif
 *         Merge the chunk into the output array.
 *     endif
 *
 * For completeness, below is an example illustrating why we may need to deal with the case of NOT merging on the fly.
 * In redimension_store, if we always merge on the fly, data may be 'scrambled' in that, at a receiver instance, chunks of different attributes
 * (at the same logical position) may be generated without using the same ordering of going through the sender instances.
 * Consider the following example, where a receiver instances receives two chunks (for two attributes at the same position) from each of two sender instances:
 *   - receive(sender_2, attr_2)
 *   - receive(sender_1, attr_2)
 *   - receive(sender_1, attr_1)
 *   - receive(sender_2, attr_1)
 * Suppose the receiver resolves conflict by keeping the last-received chunk. It will eventually produce "scrambled" result
 * by composing attr_1 from sender_2 and attr_2 from sender 1. To the user, such records may seem to come from nowhere.
 */
class SGChunkReceiver
{
private:
    /**
     * Whether the last-received empty bitmap, from every sender, needs to be cached.
     */
    const bool _cachingLastEmptyBitmap;

    /**
     * Whether received chunks should be merged on the fly.
     * Assertion: mutually exclusive with _cachingLastEmptyBitmap.
     */
    const bool _cachingReceivedChunks;

    const size_t _nInstances;

    const size_t _nAttrs;

    const InstanceID _myInstanceID;

    const ArrayDesc& _schema;

    const boost::weak_ptr<Query> _query;

    struct LastEmptyBitmapInfo
    {
        boost::shared_ptr<MemChunk>             _chunk;      // the bitmap chunk
        boost::shared_ptr<ConstRLEEmptyBitmap>  _bitmap;     // the bitmap acquired from the bitmap chunk
        boost::shared_ptr<PinBuffer>            _pin;        // the pin for the bitmap chunk

        bool exists()
        {
            return _chunk && _bitmap && _pin;
        }
    };

    /**
     * When _cachingLastEmptyBitmap is true:
     * A receiver stores, for each sender, the most recent empty-bitmap chunk that was received from the particular sender.
     * The usage is that, to process each follow-up real-attribute chunk from the same sender, the empty bitmap can be reused.
     * The goal is to eliminate the need to embed the empty tag in the real-attribute chunks.
     */
    std::vector<LastEmptyBitmapInfo> _lastEmptyBitmapCache;

    /**
     * When _cachingReceivedChunks is true:
     * A receiver instance stores a vector of MemArrays, to cache the chunks (including empty bitmaps) received from the network.
     */
    std::vector<boost::shared_ptr<MemArray> > _receivedChunkCache;

    /**
     * index is [instId * _nInstances + attrId]
     */
    std::vector<boost::shared_ptr<ArrayIterator> > _arrayIteratorsForReceivedChunkCache;

    size_t indexInstAttr(InstanceID instId, AttributeID attrId)
    {
        return instId * _nAttrs + attrId;
    }

public:

    /**
     * Cache a last-received bitmap.
     * @param[in]  sourceId     the sender of the bitmap chunk
     * @param[in]  bitmapChunk
     * @param[in]  coordinates  the coordinates of the chunk, for debug purpose -- later if the bitmap is used on the real attribute chunk, the chunk positions must match!
     */
    void setCachedEmptyBitmapChunk(size_t sourceId, boost::shared_ptr<MemChunk>& bitmapChunk, Coordinates const& coordinates);

    /**
     * Retrieve a cached bitmap.
     * @param[in]  sourceId              the sender of the bitmap chunk
     * @param[in]  expectedCcoordinates  the coordinates of the real-attribute chunk; must match those of the bitmap chunk!
     * @return     the bitmap
     */
    boost::shared_ptr<ConstRLEEmptyBitmap> getCachedEmptyBitmap(size_t sourceId, Coordinates const& expectedCoordinates);

    /**
     * @param[in] cachingLastEmptyBitmap        whether the last-received empty bitmaps (from each instance) should be cached
     * @praam[in] cachingReceivedChunks         whether received chunks should be cached
     * @param[in] schema                        array schema
     * @param[in] query                         the query context
     */
    SGChunkReceiver(bool cachingLastEmptyBitmap, bool cachingReceivedChunks, ArrayDesc const& schema, boost::shared_ptr<Query>& query);

    /**
     * Deal with a received chunk.
     * @param[in] sgCtx             the SG context
     * @param[in] isAggregateChunk  whether this chunk was received using a mtAggregateChunk message, vs mtChunk message
     * @param[in] sourceId          the sender ID
     * @param[in] compressedBuffer  the chunk data
     *
     * Other parameters are fields from chunk header.
     */
    void handleReceivedChunk(
            shared_ptr<SGContext>& sgCtx,
            bool isAggregateChunk,
            InstanceID sourceId,
            shared_ptr<CompressedBuffer>& compressedBuffer,
            int compMethod,
            size_t decompressedSize,
            AttributeID attributeID,
            size_t count,
            Coordinates& coordinates
            );

    /**
     * Read data from the received-chunk cache, merge, and write to the output array.
     */
    void generateOutputFromReceivedChunkCache();


private:
    /**
     * Inner loop of generateOutputFromReceivedChunkCache, to reduce the number of nested levels.
     */
    void generateOutputForOneInstance(
            boost::shared_ptr<MultiConstIterators> multiItersChild,
            std::vector<shared_ptr<ConstIterator> > & inputIters,
            std::vector<shared_ptr<ArrayIterator> > & outputIters,
            Coordinates const& coordinates,
            boost::shared_ptr<SGContext>& sgCtx,
            boost::shared_ptr<Query>& query
            );

    /**
     * Process a chunk at an existing position.
     */
    void processReceivedChunkAtExistingPos(
            boost::shared_ptr<SGContext>& sgCtx,
            bool isAggregateChunk,
            InstanceID sourceId,
            boost::shared_ptr<CompressedBuffer>& compressedBuffer,
            int compMethod,
            size_t decompressedSize,
            AttributeID attributeID,
            size_t count,
            Coordinates& coordinates,
            boost::shared_ptr<Array>,
            boost::shared_ptr<ArrayIterator>,
            boost::shared_ptr<Query>& query,
            bool isEmptyIndicator,
            bool isEmptyable
    );

    /**
     * Process a chunk at a new position.
     */
    void processReceivedChunkAtNewPos(
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
            );
};

} // namespace
#endif // CHUNK_RECEIVER
