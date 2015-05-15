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
 * @file PullSGArray.h
 *
 * @brief Pull-based Scatter/Gather Array
 */

#ifndef PULL_SG_ARRAY_H_
#define PULL_SG_ARRAY_H_
#include <boost/unordered_set.hpp>
#include <log4cxx/logger.h>
#include <array/Metadata.h>
#include <array/StreamArray.h>
#include <network/BaseConnection.h>
#include <util/Platform.h>
#include <query/Query.h>

namespace scidb
{
/**
 * PullSGArray collects partial chunks from other instances
 * and merges them to produce complete chunks.
 * Its nextChunk() is allowed to throw RetryException
 * indicating that nextChunk() should be called again to obtain the chunk.
 * The chunks are returned to the array consumer in the same order
 * of chunk positions as they are produced by the source(s).
 * The positions of the chunks produced by the sources are expected to be
 * GLOBALLY ordered (the default order of iteration is row-major).
 * That prooperty allows for streaming the chunks to the array consumer,
 * i.e. a chunk can be released when the current
 * positions at all the sources are greater than the chunk's position.
 */
class PullSGArray : public MultiStreamArray
{
public:

    static log4cxx::LoggerPtr _logger;

    /// scidb_msg::Chunk/Fetch::obj_type
    static const uint32_t SG_ARRAY_OBJ_TYPE = 2;

    virtual ~PullSGArray() {}

    /**
     * Handle a remote instance message containing a chunk and/or position
     * @param chunkDesc the message structure
     * @param sourceInstance logical source instance ID
     */
    void handleChunkMsg(const boost::shared_ptr<MessageDesc>& chunkDesc,
                        const InstanceID sourceInstance);

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
    RescheduleCallback resetCallback(AttributeID attId);

    /**
     * Replace the callback for chunks of a given attribute
     * @param attId attribute ID
     * @param newCb the new callback
     * @return the old callback
     */
    RescheduleCallback resetCallback(AttributeID attId,
                                     const RescheduleCallback& newCb);
protected:

    /**
     * Constructor
     * @param arrayDesc array descriptor (aka schema)
     * @param query the query context
     * @param chunkPrefetchPerAttribute number of chunks to prefetch for each attribute;
     * if 0, CONFIG_SG_RECEIVE_QUEUE_SIZE is used instead
     */
    PullSGArray(const ArrayDesc& arrayDesc,
                const boost::shared_ptr<Query>& query,
                bool enforceDataIntegrity,
                uint32_t chunkPrefetchPerAttribute=0);
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
     * @param lastKnownPosition available to the caller
     */
    void requestNextChunk(size_t stream, AttributeID attId,
                          bool positionOnly, const Coordinates& lastKnownPosition);

    /**
     * Construct and return the current chunk from a given remote stream/instance
     * @param stream ID which corresponds to a remote instance
     * @param attId attribute ID
     * @param chunk [out] chunk to be populated
     * @param lastKnownPosition available to the caller
     * @return false if no more chunks are available (EOF), true otherwise
     */
    bool getChunk(size_t stream, AttributeID attId,
                  const Coordinates& lastKnownPosition, MemChunk* chunk);

    /**
     * Get the next remote chunk position
     * @param stream ID which corresponds to a remote instance
     * @param attId attribute ID
     * @param pos [out] position to be populated
     * @param destInstance [out] destination instance for the chunk @ pos
     * (may be different than the local instance)
     * @return false if no more positions are available (EOF), true otherwise
     */
    bool getPosition(size_t stream, AttributeID attId, Coordinates& pos, size_t& destInstance);

    /**
     * Create a message containing only the position of next chunk
     * (as ordered by the source instance/stream) if such information is available
     * @param chunkMessage to extruct the next position from
     * @return a message contating only the next chunk coordinates or NULL
     * (if the next position is not available or the next chunk is already queued locally)
     */
    boost::shared_ptr<MessageDesc>
    toPositionMesg(const boost::shared_ptr<MessageDesc>& chunkMsg);

    /**
     * Remove redundant position-only messages from the queue.
     * Position-only requests may generate extra 'out-of-band' messages
     * containing position information already present in previous chunks.
     * Such messages can be dropped.
     * @param stream ID which corresponds to a remote instance
     * @param attId attribute ID
     * @param lastKnownPosition available to the caller
     */
    void pruneRedundantPositions(size_t stream, AttributeID attId,
                                 const Coordinates& lastKnownPosition);

    /**
     * @return the prefetch size to communicate to the chunk source/producer, 0 is possible
     * @param attId attribute ID
     * @param stream ID which corresponds to a remote instance/stream
     * @param positionOnly true if a position only reply is acceptable from the source
     */
    uint32_t getPrefetchSize(AttributeID attId, size_t stream, bool positionOnly);

    RescheduleCallback getCallback(AttributeID attId);

    /// Helper class to maintain stream (i.e. chunk source/producer) bookkeeping
    class StreamState
    {
    public:
        StreamState()
        : _requested(0), _cachedSize(0), _currMsgId(0),
          _lastPositionOnlyId(0), _lastRemoteId(0), _isPending(false)
        {}
        uint64_t getNextMsgId() { return ++_currMsgId; }
        uint64_t getLastPositionOnlyId() const { return _lastPositionOnlyId; }
        void setLastPositionOnlyId(uint64_t msgId)
        {
            _lastPositionOnlyId = msgId;
            ASSERT_EXCEPTION((_lastPositionOnlyId<=_currMsgId),
                             "StreamState::setLastPositionOnlyId: ");
        }
        uint64_t getLastRemoteId() const { return _lastRemoteId; }
        void setLastRemoteId(uint64_t msgId)
        {
            _lastRemoteId = msgId;
            ASSERT_EXCEPTION((_lastRemoteId<=_currMsgId),
                             "StreamState::setLastRemoteId: ");
        }
        void setRequested(uint64_t num) { _requested = num; }
        uint64_t getRequested() const   { return _requested; }
        bool isEmpty() const            { return _msgs.empty(); }
        size_t size()  const            { return _msgs.size(); }
        uint64_t cachedSize() const     { return _cachedSize; }
        bool isPending() const          { return _isPending; }
        void setPending(bool bit)       { _isPending = bit; }
        void push(const boost::shared_ptr<MessageDesc>& msg)
        {
            _msgs.push_back(msg);
            if (msg->getBinary()) { ++_cachedSize; }
        }
        const boost::shared_ptr<MessageDesc>& head()
        {
            return _msgs.front();
        }
        boost::shared_ptr<MessageDesc> pop();

    private:
        std::deque<boost::shared_ptr<MessageDesc> > _msgs;
        uint64_t _requested;  // number of *DATA* chunks requested but not yet available,
                              // position information can be piggy-backed on chunks (but does not have to be)
        uint64_t _cachedSize; // number of messages with chunk bodies (i.e. with binary data),
                              // position & EOF messages dont count
        uint64_t _currMsgId; // message ID assigned to every outbound message
        uint64_t _lastPositionOnlyId; // message ID of the last positionOnly request sent to the source
        uint64_t _lastRemoteId; // as seen by the remote source
        bool _isPending; // whether the caller of nextChunk() is waiting for data

        friend std::ostream& operator << (std::ostream& out,
                                          PullSGArray::StreamState& state);
    };

    friend std::ostream& operator << (std::ostream& out,
                                      PullSGArray::StreamState& state);

    const QueryID _queryId;
    std::vector<RescheduleCallback > _callbacks;
    std::vector<Mutex> _sMutexes;
    std::vector<Mutex> _aMutexes;

    std::vector< std::vector< StreamState > > _messages;

    std::vector<uint32_t> _cachedChunks; // debug only
    std::vector<uint32_t> _requestedChunks; // debug only
    std::vector<uint64_t> _numSent; // debug only
    std::vector<uint64_t> _numRecvd; // debug only

    std::vector<uint32_t> _commonChunks;

    uint32_t _maxCommonChunks;
    uint32_t _maxChunksPerStream;
    uint32_t _maxChunksPerAttribute;

private:
    PullSGArray();
    PullSGArray(const PullSGArray&);
    PullSGArray& operator=(const PullSGArray&);
};

/**
 * The Array used by the SG consumer to pull redistributed chunk data
 * It is also Synchable because the consumer is expected to call sync()
 * immediately after consuming all the data
 */
class PullSGArrayBlocking : public SynchableArray, public PullSGArray,
                            public boost::enable_shared_from_this<PullSGArrayBlocking>
{
public:
    /**
     * Constructor
     * @param arrayDesc array descriptor (aka schema)
     * @param query the query context
     * @param chunkPrefetchPerAttribute number of chunks to prefetch for each attribute;
     * if 0, CONFIG_SG_RECEIVE_QUEUE_SIZE is used instead
     */
    PullSGArrayBlocking(const ArrayDesc& arrayDesc,
                        const boost::shared_ptr<Query>& query,
                        const boost::shared_ptr<Array>& inputSGArray,
                        bool enforceDataIntegrity,
                        uint32_t chunkPrefetchPerAttribute=0);

    virtual ~PullSGArrayBlocking() {}
    /**
     * @see scidb::MultiStreamArray::getNext()
     * @throws no expected exceptions (i.e. scidb::MultiStreamArray::RetryException is not thrown)
     */
    virtual ConstChunk const* nextChunk(AttributeID attId, MemChunk& chunk);

    /// To be called immediately after consuming all the chunks
    virtual void sync();

    virtual boost::shared_ptr<ConstArrayIterator> getConstIterator(AttributeID attId) const ;

    /**
     * An INTERNAL helper template function for continually draining of MultiStreamArray
     * @param attributesToPull a set of attributes to pull from the array
     *        NOTE that this parameter is mutable, its contents are undefined upon return
     * @param func a chunk handling functor
     */
    template <class ChunkHandler_tt>
    void pullAttributes(boost::unordered_set<AttributeID>& attributesToPull,
                        ChunkHandler_tt& func);
private:

    /**
     * Gets the next chunk from PullSGArray
     */
    template<typename ChunkHandler_tt>
    bool pullChunk(ChunkHandler_tt& chunkHandler, const AttributeID attId);

    bool isInputSinglePass() const { return _sgInputAccess==Array::SINGLE_PASS; }

    void validateIncomingChunk(ConstChunk const* chunk, const AttributeID attId);

    boost::shared_ptr<Array> _inputSGArray;
    const Array::Access _sgInputAccess;
    bool _nonBlockingMode;

/**
 * A helper class for scheduling chunk processing as they become available
 */
class SyncCtx
{
private:

    Mutex _mutex;
    Event _ev;
    bool _cond;
    Event::ErrorChecker _ec;
    boost::shared_ptr<Exception> _error;
    boost::unordered_set<AttributeID> _activeAttributes;

public:

    SyncCtx(const shared_ptr<Query>& query) :
    _cond(false),
    _ec(boost::bind(&Query::getValidQueryPtr, boost::weak_ptr<Query>(query)))
    {}

    SyncCtx(const weak_ptr<Query>& query) :
    _cond(false),
    _ec(boost::bind(&Query::getValidQueryPtr, query))
    {}

    void signal(AttributeID attrId, const Exception* error);
    void waitForActiveAttributes(boost::unordered_set<AttributeID>& activeAttributes);
private:
    SyncCtx();
    SyncCtx(const SyncCtx& );
    SyncCtx& operator=(const SyncCtx& );
};

};

template <class ChunkHandler_tt>
void PullSGArrayBlocking::pullAttributes(boost::unordered_set<AttributeID>& attributesToPull,
                                         ChunkHandler_tt& func)
{
    _nonBlockingMode = true;
    const static char* funcName =  "PullSGArrayBlocking::pullAttributes: ";
    if (isInputSinglePass()) {
        if (attributesToPull.size() != _iterators.size()) {
            stringstream ss; ss << funcName << "all attributes are required for SINGLE_PASS array";
            ASSERT_EXCEPTION(false, ss.str());
        }
        SinglePassArray* spa = dynamic_cast<SinglePassArray*>(_inputSGArray.get());
        if (spa==NULL || !spa->isEnforceHorizontalIteration()) {
            stringstream ss; ss << funcName << "SinglePassArray is required with horizontal iteration enforced";
            ASSERT_EXCEPTION(false, ss.str());
        }
    }

    shared_ptr<SyncCtx> ctx = boost::make_shared<SyncCtx>(_query);
    for (boost::unordered_set<AttributeID>::const_iterator i = attributesToPull.begin();
         i != attributesToPull.end(); ++i) {
        const AttributeID attId = *i;
        assert(attId<_iterators.size());
        if (_iterators[attId])  {
            stringstream ss; ss << funcName << "attribute "<< attId << " already pulled";
            ASSERT_EXCEPTION(false, ss.str());
        }
        PullSGArray::RescheduleCallback cb = boost::bind(&SyncCtx::signal, ctx, attId, _1);
        resetCallback(attId, cb);
    }

    boost::unordered_set<AttributeID> activeAttributes(attributesToPull);
    while (!attributesToPull.empty()) {
        LOG4CXX_TRACE(PullSGArray::_logger, funcName
                      << " active attrs size="<<activeAttributes.size());
        for (boost::unordered_set<AttributeID>::iterator iter = activeAttributes.begin();
             iter != activeAttributes.end(); ) {
            const AttributeID attId = *iter;
            bool eof = false;
            try {
                eof = pullChunk(func,attId);
            } catch (const scidb::MultiStreamArray::RetryException& ) {
                boost::unordered_set<AttributeID>::iterator iterToErase = iter;
                ++iter;
                activeAttributes.erase(iterToErase);
                continue;
            }

            if (eof) {
                boost::unordered_set<AttributeID>::iterator iterToErase = iter;
                ++iter;
                activeAttributes.erase(iterToErase);
                resetCallback(attId);
                attributesToPull.erase(attId);
                LOG4CXX_DEBUG(PullSGArray::_logger, funcName
                              << "EOF attId="<< attId
                              <<", remain="<<attributesToPull.size());
                continue;
            }
            ++iter;
        }
        if (!attributesToPull.empty() &&
            activeAttributes.empty()) {
            LOG4CXX_TRACE(PullSGArray::_logger,  funcName
                          << "waiting, active attrs size="<<activeAttributes.size());
            ctx->waitForActiveAttributes(activeAttributes);
        }
    }
    _nonBlockingMode = false;
}

template<typename ChunkHandler_tt>
bool PullSGArrayBlocking::pullChunk(ChunkHandler_tt& chunkHandler,
                                    const AttributeID attId)
{
    const static char* funcName =  "PullSGArrayBlocking::consumeChunk: ";
    if (isDebug()) {
        LOG4CXX_TRACE(PullSGArray::_logger, funcName << "trying to consume chunk for attId="<<attId);
    }
    boost::shared_ptr<ConstArrayIterator> arrIter = PullSGArray::getConstIterator(attId);
    if (arrIter->end()) {
        LOG4CXX_DEBUG(PullSGArray::_logger,  funcName << "EOF attId="<<attId);
        return true;
    }
    const ConstChunk& chunk = arrIter->getChunk();
    validateIncomingChunk(&chunk, attId);

    shared_ptr<Query> query = Query::getValidQueryPtr(_query);
    chunkHandler(attId, chunk, query);

    if (isDebug()) {
        LOG4CXX_TRACE(PullSGArray::_logger, funcName << "advanced attId="<<attId);
    }
    return false;
}

} // namespace

#endif /* PULL_SG_ARRAY_H_ */
