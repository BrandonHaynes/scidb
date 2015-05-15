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
 * @file RemoteArray.h
 *
 * @author roman.simakov@gmail.com
 */

#ifndef REMOTEARRAY_H_
#define REMOTEARRAY_H_

#include <boost/enable_shared_from_this.hpp>

#include <array/Metadata.h>
#include <array/StreamArray.h>
#include <network/BaseConnection.h>
#include <query/Query.h>
#include <util/Mutex.h>

namespace scidb
{

class Statistics;

/**
 * A sub-class of OperatorContext, to share data using RemoteArray among instances.
 * Even though multiple threads access the RemoteArrayContext, no synchronization is needed, for the following reasons.
 * There are two types of threads:
 *   - A "writer" thread which creates this context, and calls query->setOperatorContext/unsetOperatorContext;
 *   - and multiple "reader" threads on either side of the channel of RemoteArray:
 *     * A pullee responds to a mtFetch message, by returning a chunk from _outboundArrays.
 *     * A puller responds to a mtRemoteChunk message, by adding the received chunk to an array referenced in _inboundArrays.
 * The synchronization of the writer thread and the reader threads should be protected using syncBarrier in the writer thread itself as:
 *   1. syncBarrier(0)
 *   2. prepare a shared_ptr<RemoteArrayContext>
 *   3. query->setOperatorContext()
 *   4. NOW mtFetch/mtRemoteChunk MESSAGES MAY BE EXCHANGED.
 *   5. syncBarrier(1)
 *   6. query->unsetOperatorContext()
 */
class RemoteArrayContext: public Query::OperatorContext
{
public:
    /**
     * @param numInstances  the number of SciDB instances.
     */
    RemoteArrayContext(size_t numInstances);

    /**
     * Given a source instance, get the remote array to pull data from the instance.
     * @param logicalSrcInstanceID  the logical source instance ID.
     * @return a RemoteArray to pull data from.
     */
    boost::shared_ptr<RemoteArray> getInboundArray(InstanceID logicalSrcInstanceID) const;

    /**
     * Given a source instance, and an array, take a note that the array is meant to pull data from that instance.
     * @param logicalSrcInstanceID  the logical source instance ID.
     * @param array          a RemoteArray to pull data from.
     */
    void setInboundArray(InstanceID logicalSrcInstanceID, const boost::shared_ptr<RemoteArray>& array);

    /**
     * Given a destination instance, get the outbound array to be sent to the instance.
     * @param logicalDestInstanceID  the logical destination instance ID.
     * @return an outbound array prepared for the instance.
     */
    boost::shared_ptr<Array> getOutboundArray(const InstanceID& logicalDestInstanceID) const;

    /**
     * Given a destination instance, and an array, take a note that the array is meant to be sent to the instance.
     * @param logicalDestInstanceID  the logical destination instance ID.
     * @param array           an SciDB array to be sent to the instance.
     */
    void setOutboundArray(const InstanceID& logicalDestInstanceID, const boost::shared_ptr<Array>& array);

private:
    /**
     * A vector of RemoteArrays, to pull data from each remote instance.
     */
    std::vector<boost::shared_ptr<RemoteArray> > _inboundArrays;

    /**
     * A vector of outbound arrays, to send data to each remote instance.
     */
    std::vector<boost::shared_ptr<Array> > _outboundArrays;
};

/**
 * Class implement fetching chunks from current result array of remote instance.
 */
class RemoteArray: public StreamArray
{
public:
     /// scidb_msg::Chunk/Fetch::obj_type
    static const uint32_t REMOTE_ARRAY_OBJ_TYPE = 0;

    void handleChunkMsg(boost::shared_ptr< MessageDesc>& chunkDesc);

    /**
     * Create a RemoteArray object, store it in remoteArrayContext, and return it.
     * @param[inout] remoteArrayContext   an RemoteArrayContext object, whose _inboundArrays[logicalSrcInstanceID] will be set.
     * @param[in]    arrayDesc            the schema of the RemoteArray.
     * @param[in]    queryId              the ID of the query context.
     * @param[in]    logicalSrcInstanceId the logical ID of the instance to pull data from.
     * @return a shared_ptr to the RemoteArray object (which is already stored in the RemoteArrayContext).
     */
    static boost::shared_ptr<RemoteArray> create(
            boost::shared_ptr<RemoteArrayContext>& remoteArrayContext,
            const ArrayDesc& arrayDesc, QueryID queryId, InstanceID instanceID);

    static boost::shared_ptr<RemoteArrayContext> getContext(boost::shared_ptr<Query>&);

private:
    bool proceedChunkMsg(AttributeID attId, MemChunk& chunk);
    void requestNextChunk(AttributeID attId);

    /**
     * This is private because the caller is supposed to call RemoteArray::create to create a RemoteArray object.
     */
    RemoteArray(const ArrayDesc& arrayDesc, QueryID queryId, InstanceID logicalSrcInstanceID);

    QueryID _queryId;
    InstanceID _instanceID;
    std::vector<Semaphore> _received;
    std::vector<boost::shared_ptr<MessageDesc> > _messages;
    std::vector<bool> _requested;

    // overloaded method
    virtual ConstChunk const* nextChunk(AttributeID attId, MemChunk& chunk);
};

#ifndef SCIDB_CLIENT

/**
 * RemoteMergedArray collects partial chunks from other instances and merges them to produce complete chunks.
 * Its nextChunk() is allowed to throw RetryException
 * indicating that nextChunk() should be called again to obtain the chunk.
 */
class RemoteMergedArray: public MultiStreamArray
{

public:

    /// scidb_msg::Chunk/Fetch::obj_type
    static const uint32_t MERGED_ARRAY_OBJ_TYPE = 1;

    /**
     * Handle a remote instance message containing a chunk and/or position
     * @param chunkDesc the message structure
     */
    void handleChunkMsg(boost::shared_ptr<MessageDesc>& chunkDesc);

    /// Factory method
    static boost::shared_ptr<RemoteMergedArray> create(const ArrayDesc& arrayDesc, QueryID queryId, Statistics& statistics);

    /**
     * @see scidb::ConstArrayIterator
     * This implementation always returns the same iterator object.
     * It is created on the first invocation and incremented(operator++()) on the subsequent
     */
    boost::shared_ptr<ConstArrayIterator> getConstIterator(AttributeID attId) const ;

    /**
     * Callback to invoke when a remote chunk becomes available
     * @param error if not NULL, specifies an error preventing retrieval of the remote chunk
     */
    typedef boost::function<void(const scidb::Exception* error)> RescheduleCallback;

    /**
     * Remove the callback for chunks of a given attribute
     * @param attId attribute ID
     * @return the removed callback
     */
    RescheduleCallback resetCallback(AttributeID attId)
    {
        RescheduleCallback cb;
        return resetCallback(attId,cb);
    }

    /**
     * Replace the callback for chunks of a given attribute
     * @param attId attribute ID
     * @param newCb the new callback
     * @return the old callback
     */
    RescheduleCallback resetCallback(AttributeID attId, const RescheduleCallback& newCb)
    {
        assert(attId<_callbacks.size());
        RescheduleCallback oldCb;
        {
            ScopedMutexLock lock(_mutexes[attId % _mutexes.size()]);
            _callbacks[attId].swap(oldCb);
            _callbacks[attId] = newCb;
        }
        return oldCb;
    }

protected:

    /**
     * Get the next chunk from a given stream/instance
     * @param stream ID which corresponds to a source instance
     * @param attId attribute ID
     * @param chunk that can be used to copy the result
     * @return the requested chunk (may not be the same as the 'chunk' argument)
     * @throws scidb::MultiStreamArray::RetryException if the chunk is not yet ready
     */
    virtual ConstChunk const* nextChunkBody(size_t stream,
                                            AttributeID attId,
                                            MemChunk& chunk);

    /**
     * Get the next chunk position from a given stream/instance
     * @param stream ID which corresponds to a source instance
     * @param attId attribute ID
     * @param pos [out] requested position
     * @param destStream [out] destination for the chunk @ pos
     * @return false if no more positions are available (EOF); true otherwise
     * @throws scidb::MultiStreamArray::RetryException if the position is not yet ready
     */
    virtual bool nextChunkPos(size_t stream,
                              AttributeID attId,
                              Coordinates& pos,
                              size_t& destStream);
private:

    /**
     * Request the next chunk or position from a remote stream.
     * This implementation piggy-backs the position on the chunk message.
     * Every chunk message carries the position of the subsequent chunk.
     * @param stream ID which corresponds to a remote instance
     * @param attId attribute ID
     * @param positionOnly is set to true if only the position is requested
     * @return
     */
    void requestNextChunk(size_t stream, AttributeID attId, bool positionOnly);

    /**
     * Construct and return the current chunk from a given remote stream/instance
     * @param stream ID which corresponds to a remote instance
     * @param attId attribute ID
     * @param chunk [out] chunk to be populated
     * @return false if no more chunks are available (EOF), true otherwise
     */
    bool getChunk(size_t stream, AttributeID attId, MemChunk* chunk);

    /**
     * Get the next remote chunk position
     * @param stream ID which corresponds to a remote instance
     * @param attId attribute ID
     * @param pos [out] position to be populated
     * @return false if no more positions are available (EOF), true otherwise
     */
    bool getPos(size_t stream, AttributeID attId, Coordinates& pos);

    /**
     * Get the next chunk position (local or remote)
     * @param stream ID which corresponds to a remote instance
     * @param attId attribute ID
     * @param position [out] position to be populated
     * @return false if no more positions are available (EOF), true otherwise
     */
    bool fetchPosition(size_t stream, AttributeID attId, Coordinates& position);

    /**
     * Construct and return the current chunk (local or remote)
     * @param stream ID which corresponds to a remote instance
     * @param attId attribute ID
     * @param chunk [out] chunk to be populated
     * @return false if no more chunks are available (EOF), true otherwise
     */
    bool fetchChunk(size_t stream, AttributeID attId, MemChunk* chunk);

    /**
     * Constructor
     * @param arrayDesc array descriptor (aka schema)
     * @param query the query context
     * @param statistics unused???
     */
    RemoteMergedArray(const ArrayDesc& arrayDesc,
                      const boost::shared_ptr<Query>& query,
                      Statistics& statistics);

    std::vector<RescheduleCallback > _callbacks;
    boost::shared_ptr<Query> _query;
    std::vector<Mutex> _mutexes;
    struct MessageState
    {
        MessageState() : _hasPosition(true) {}
        boost::shared_ptr<MessageDesc> _message;
        bool _hasPosition; // false if position has been requested but not yet available (except for the very first time)
    };
    friend std::ostream& operator << (std::ostream& out,
                                      RemoteMergedArray::MessageState& state);

    std::vector< std::vector< MessageState > > _messages;
    boost::shared_ptr< Array> _localArray;
};

#endif

} // namespace

#endif /* REMOTEARRAY_H_ */
