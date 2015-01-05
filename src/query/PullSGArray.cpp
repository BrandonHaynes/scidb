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
 * @file PullSGArray.cpp
 *
 * @brief mplementation of an array that returns redistributed chunks by the means of pull-based scater/gather
 */
#include <boost/unordered_set.hpp>
#include <boost/shared_ptr.hpp>
#include <boost/make_shared.hpp>
#include <boost/enable_shared_from_this.hpp>

#include <system/Config.h>
#include <system/SciDBConfigOptions.h>
#include <network/proto/scidb_msg.pb.h>
#include <network/NetworkManager.h>
#include <network/MessageHandleJob.h>
#include <query/QueryProcessor.h>
#include <query/PullSGArray.h>
#include <query/PullSGContext.h>
#include <system/Exceptions.h>

using namespace std;
using namespace boost;

namespace scidb
{

log4cxx::LoggerPtr PullSGArray::_logger(log4cxx::Logger::getLogger("scidb.qproc.pullsgarray"));

namespace {
template<typename T>
void logMatrix(std::vector<std::vector<T> >& matrix, const std::string& prefix)
{
    if (!PullSGArray::_logger->isTraceEnabled()) {
        return;
    }
    stringstream ss;
    for (size_t i=0; i<matrix.size(); ++i) {
        std::vector<T>& row = matrix[i];
        for (size_t j=0; j<row.size(); ++j) {
            ss << "["<<i<<","<<j<<"] = "<<row[j]<<",";
        }
        ss << " ; ";
    }
    LOG4CXX_TRACE(PullSGArray::_logger, prefix << ": " << ss.str());
}
}

PullSGArray::PullSGArray(const ArrayDesc& arrayDesc,
                         const boost::shared_ptr<Query>& query,
                         uint32_t chunkPrefetchPerAttribute)
: MultiStreamArray(query->getInstancesCount(), query->getInstanceID(), arrayDesc, query),
  _queryId(query->getQueryID()),
  _callbacks(arrayDesc.getAttributes().size()),
  _messages(arrayDesc.getAttributes().size(), vector< StreamState >(getStreamCount())),
  _commonChunks(arrayDesc.getAttributes().size(), 0),
  _maxChunksPerStream(0),
  _maxChunksPerAttribute(64)
{
    _query = query;
    if (isDebug()) {
        _cachedChunks.resize(arrayDesc.getAttributes().size(), 0);
        _requestedChunks.resize(arrayDesc.getAttributes().size(), 0);
        _numSent.resize(arrayDesc.getAttributes().size(), 0);
        _numRecvd.resize(arrayDesc.getAttributes().size(), 0);
    }

    static const size_t MAX_MUTEX_NUM = 100;
    const uint32_t nMutexes = std::min(arrayDesc.getAttributes().size(), MAX_MUTEX_NUM);
    _mutexes.resize(nMutexes);

    static const uint32_t DEFAULT_PREFETCH_CACHE_SIZE=64;
    _maxChunksPerAttribute = DEFAULT_PREFETCH_CACHE_SIZE;

    int n = Config::getInstance()->getOption<int>(CONFIG_SG_RECEIVE_QUEUE_SIZE);
    if (n>0) {
        _maxChunksPerAttribute = n;
    }
    if (chunkPrefetchPerAttribute > 0) {
        _maxChunksPerAttribute = chunkPrefetchPerAttribute;
    }
    _maxChunksPerStream = _maxChunksPerAttribute / getStreamCount() / 2;
    _maxCommonChunks = _maxChunksPerAttribute - (_maxChunksPerStream * getStreamCount());
}

std::ostream& operator << (std::ostream& out,
                           PullSGArray::StreamState& state)
{
    out << "["
        << state.getLastPositionOnlyId() <<";"
        << state.getLastRemoteId() <<";"
        << state.getRequested()<<";"
        << state.cachedSize()<<";"
        << state.size()<<";" ;
    out << "]";
    return out;
}

uint32_t PullSGArray::getPrefetchSize(AttributeID attId, size_t stream, bool positionOnly)
{
    static const char* funcName = "PullSGArray::getPrefetchSize: ";
    assert((_messages[attId][stream].cachedSize() + _messages[attId][stream].getRequested())
           <= (_maxChunksPerStream+_commonChunks[attId]));
    assert(_requestedChunks[attId] +_cachedChunks[attId]
           <= (_maxChunksPerAttribute+getStreamCount()));

    uint32_t prefetchSize = 0;
    uint32_t outstanding = _messages[attId][stream].cachedSize() + _messages[attId][stream].getRequested();
    if (_maxChunksPerStream > outstanding) {
        // there is space for more chunks
        prefetchSize = _maxChunksPerStream - outstanding;
    } else if (_commonChunks[attId] < _maxCommonChunks &&
               _messages[attId][stream].getRequested()<1) {
        // per-stream limit is reached, but the common pool can be used
        prefetchSize = (_maxCommonChunks - _commonChunks[attId]) / getStreamCount();
        prefetchSize = prefetchSize < 1 ? 1 : prefetchSize;
        _commonChunks[attId] += prefetchSize;
        LOG4CXX_TRACE(_logger, funcName << "attId=" << attId
                          << ", commonChunks=" << _commonChunks[attId]
                          << ", stream=" << stream);
    } else if (!positionOnly && outstanding < 1 &&
               _messages[attId][stream].isEmpty()) {
        // if the cache size is smaller than the number of streams,
        // we are not going to do any prefetching
        // but we have to request at least one chunk to make progress
        prefetchSize = 1;
        ++_commonChunks[attId]; // billing against _commonChunks
    }

    assert((_requestedChunks[attId] +_cachedChunks[attId] + prefetchSize)
           <= (_maxChunksPerAttribute+getStreamCount()));
    assert(_commonChunks[attId] <= (_maxCommonChunks+getStreamCount()));

    LOG4CXX_TRACE(_logger, funcName << "attId=" << attId
                  << ", prefetchSize=" << prefetchSize
                  << ", stream=" << stream);

    return prefetchSize;
}

void
PullSGArray::requestNextChunk(size_t stream, AttributeID attId, bool positionOnly, const Coordinates& lastKnownPosition)
{
    static const char* funcName = "PullSGArray::requestNextChunk: ";
    uint32_t prefetchSize=0;
    uint64_t fetchId=~0;
    {
        ScopedMutexLock lock(_mutexes[stream % _mutexes.size()]);

        logMatrix(_messages, "PullSGArray::requestNextChunk(): before _messages");

        PullSGArray::StreamState& streamState = _messages[attId][stream];

        if (!positionOnly) {
            pruneRedundantPositions(stream, attId, lastKnownPosition);
        }

        if (!streamState.isEmpty()) {
            shared_ptr<scidb_msg::Chunk> chunkRecord =
               streamState.head()->getRecord<scidb_msg::Chunk>();
            if (chunkRecord->eof()) {
                // nothing to request
                LOG4CXX_DEBUG(_logger, funcName << " already @ EOF attId=" << attId
                              << (positionOnly? ", position only" : ", full")
                              << ", stream=" << stream);
                if (isDebug()) {
                    LOG4CXX_DEBUG(_logger, funcName << " stats attId=" << attId
                                  << ", stream=" << stream
                                  << ", numSent=" << _numSent[attId]
                                  << ", numRecvd=" << _numRecvd[attId]);
                }
                return;
            }
        }

        bool isPositionReqInFlight = (streamState.getLastPositionOnlyId() >
                                      streamState.getLastRemoteId());
        {
            ScopedMutexLock cLock(_mutexes[attId % _mutexes.size()]);
            prefetchSize = getPrefetchSize(attId, stream, positionOnly);
        }
        if (prefetchSize <= 0) {
            if (!streamState.isEmpty() ) {
                // already received something, needs to be consumed first before prefetching
                LOG4CXX_TRACE(_logger, funcName << "nothing to request, already have data attId=" << attId
                              << (positionOnly? ", position only" : ", full")
                              << ", stream=" << stream);
                return;
            }
            if (!positionOnly) {
                // cannot prefetch any more
                LOG4CXX_TRACE(_logger, funcName << "nothing to request, already requested data attId=" << attId
                              << (positionOnly? ", position only" : ", full")
                                  << ", stream=" << stream);
                return;
            } else if (isPositionReqInFlight) {
                // already have an outstanding position request
                LOG4CXX_TRACE(_logger, funcName << "nothing to request, already requested position attId=" << attId
                              << (positionOnly? ", position only" : ", full")
                              << ", last PO request=" << streamState.getLastPositionOnlyId()
                              << ", last request from source=" << streamState.getLastRemoteId()
                              << ", stream=" << stream);
                return;
            }
        }

        if (!streamState.isEmpty() || isPositionReqInFlight) {
            assert(prefetchSize > 0);
            // no need to ask for a bare position unless we have to make progress
            positionOnly = false;
        }

        if (positionOnly) {
            fetchId = streamState.getNextMsgId();
            streamState.setLastPositionOnlyId(fetchId);

        } else if (streamState.getRequested()>0) {
            LOG4CXX_TRACE(_logger, funcName << "nothing to request, too many outstanding attId=" << attId
                          << (positionOnly? ", position only" : ", full")
                          << ", prefetch="<<prefetchSize
                          << ", requested="<<streamState.getRequested()
                          << ", stream=" << stream);
            return;
        } else {
            fetchId = streamState.getNextMsgId();
        }
        if (isDebug()) {
            ScopedMutexLock cLock(_mutexes[attId % _mutexes.size()]);
            _requestedChunks[attId] += prefetchSize;
            ++_numSent[attId];
        }
        streamState.setRequested(prefetchSize + streamState.getRequested());

        logMatrix(_messages, "PullSGArray::requestNextChunk(): after _messages");
    }

    LOG4CXX_TRACE(_logger, funcName << " request next chunk attId=" << attId
                  << (positionOnly? ", position only" : ", full")
                  << ", stream=" << stream
                  << ", prefetch=" << prefetchSize);

    boost::shared_ptr<MessageDesc> fetchDesc = boost::make_shared<MessageDesc>(mtFetch);
    boost::shared_ptr<scidb_msg::Fetch> fetchRecord = fetchDesc->getRecord<scidb_msg::Fetch>();
    fetchDesc->setQueryID(_queryId);
    assert(fetchId != uint64_t(~0));
    fetchRecord->set_fetch_id(fetchId);
    fetchRecord->set_attribute_id(attId);
    fetchRecord->set_position_only(positionOnly);
    fetchRecord->set_prefetch_size(prefetchSize);
    fetchRecord->set_obj_type(SG_ARRAY_OBJ_TYPE);

    const InstanceID logicalId = stream;
    if (getLocalStream() == logicalId) {
        // local
        shared_ptr<Query> query = Query::getValidQueryPtr(_query);
        const InstanceID physicalId = query->mapLogicalToPhysical(logicalId);
        fetchDesc->setSourceInstanceID(physicalId);
        shared_ptr<MessageHandleJob> job = boost::make_shared<ServerMessageHandleJob>(fetchDesc);
        shared_ptr<WorkQueue> rq = NetworkManager::getInstance()->getRequestQueue();
        shared_ptr<WorkQueue> wq = NetworkManager::getInstance()->getWorkQueue();
        job->dispatch(rq, wq);
    } else {
        // remote
        NetworkManager::getInstance()->send(logicalId, fetchDesc);
    }
}

void
PullSGArray::handleChunkMsg(const boost::shared_ptr<MessageDesc>& chunkDesc,
                            const InstanceID logicalSourceId)
{
    static const char* funcName = "PullSGArray::handleChunkMsg: ";
    assert(chunkDesc->getMessageType() == mtRemoteChunk);
    ASSERT_EXCEPTION((chunkDesc->getQueryID()==_queryId), funcName);

    boost::shared_ptr<scidb_msg::Chunk> chunkMsg = chunkDesc->getRecord<scidb_msg::Chunk>();
    ASSERT_EXCEPTION((chunkMsg->has_attribute_id()), funcName);
    AttributeID attId = chunkMsg->attribute_id();
    ASSERT_EXCEPTION((chunkMsg->has_fetch_id()), funcName);
    uint64_t fetchId = chunkMsg->fetch_id();
    ASSERT_EXCEPTION((fetchId>0 && fetchId<uint64_t(~0)), funcName);

    size_t stream = logicalSourceId;

    assert(stream < getStreamCount());
    assert(attId < _messages.size());

    RescheduleCallback cb;
    {
        ScopedMutexLock lock(_mutexes[stream % _mutexes.size()]);
        LOG4CXX_TRACE(_logger,  funcName << "received next chunk message attId="<<attId
                      <<", stream="<<stream
                      <<", queryID="<<_queryId);
        logMatrix(_messages, "PullSGArray::handleChunkMsg: before _messages");

        PullSGArray::StreamState& streamState = _messages[attId][stream];

        streamState.push(chunkDesc);
        streamState.setLastRemoteId(fetchId);

        if (isDebug()) {
            ScopedMutexLock cLock(_mutexes[attId % _mutexes.size()]);
            ++_numRecvd[attId];
        }
        if (chunkDesc->getBinary()) {
            assert(streamState.getRequested()>0);
            streamState.setRequested(streamState.getRequested()-1);
            if (isDebug()) {
                ScopedMutexLock cLock(_mutexes[attId % _mutexes.size()]);
                assert(_requestedChunks[attId]>0);
                --_requestedChunks[attId];
                ++_cachedChunks[attId];
            }
            assert(streamState.cachedSize()>0);
        }
        assert(streamState.size()>0);

        if (streamState.isPending()) {
            cb = getCallback(attId);
            streamState.setPending(false);
        }
        logMatrix(_messages, "PullSGArray::handleChunkMsg: after _messages");
    }
    if (cb) {
        const Exception* error(NULL);
        cb(error);
    }
}

void
PullSGArray::pruneRedundantPositions(size_t stream, AttributeID attId,
                                     const Coordinates& lastKnownPosition)
{
    static const char* funcName = "PullSGArray::pruneRedundantPositions: ";
    PullSGArray::StreamState& streamState = _messages[attId][stream];

    while (!streamState.isEmpty()) {
        const boost::shared_ptr<MessageDesc>& msg = streamState.head();
        if (msg->getBinary()) {
            break;
        }
        boost::shared_ptr<scidb_msg::Chunk> record = msg->getRecord<scidb_msg::Chunk>();
        if (record->eof()) {
            break;
        }

        size_t n = record->coordinates_size();
        ASSERT_EXCEPTION((n==lastKnownPosition.size()), funcName);

        for (size_t i = 0; i<n; ++i) {
            ASSERT_EXCEPTION((lastKnownPosition[i] == record->coordinates(i)),
                             funcName);
        }
        streamState.pop();
    }
}

bool
PullSGArray::getChunk(size_t stream, AttributeID attId, const Coordinates& position, MemChunk* chunk)
{
    static const char* funcName = "PullSGArray::getChunk: ";
    assert(chunk);

    boost::shared_ptr<MessageDesc> chunkDesc;
    boost::shared_ptr<CompressedBuffer> compressedBuffer;
    {
        ScopedMutexLock lock(_mutexes[stream % _mutexes.size()]);

        logMatrix(_messages, "PullSGArray::getChunk: before _messages");

        pruneRedundantPositions(stream, attId, position);

        PullSGArray::StreamState& streamState = _messages[attId][stream];

        if (!streamState.isEmpty()) {
            chunkDesc = streamState.pop();
            assert(chunkDesc);
            assert(!chunkDesc->getRecord<scidb_msg::Chunk>()->eof());

            compressedBuffer = dynamic_pointer_cast<CompressedBuffer>(chunkDesc->getBinary());
            assert(compressedBuffer);
            {
                ScopedMutexLock cLock(_mutexes[attId % _mutexes.size()]);
                if (isDebug()) { --_cachedChunks[attId]; }
                if ((streamState.cachedSize() +
                     streamState.getRequested()) >= _maxChunksPerStream) {
                    assert(_commonChunks[attId]>0);
                    --_commonChunks[attId];
                    LOG4CXX_TRACE(_logger, funcName << "attId=" << attId
                                  << ", commonChunks=" << _commonChunks[attId]
                                  << ", stream=" << stream);
                }
            }
            boost::shared_ptr<MessageDesc> nextPosMsgDesc;
            if (streamState.isEmpty()) {
                nextPosMsgDesc = toPositionMesg(chunkDesc);
            }
            if (nextPosMsgDesc) {
                streamState.push(nextPosMsgDesc);
            }
        } else {
            assert(streamState.getRequested()>0);
        }
        if (!chunkDesc) {
            streamState.setPending(true);
        }
        LOG4CXX_TRACE(_logger, funcName << "attId=" << attId
                     << ", stream=" << stream
                     << ", message queue size=" << streamState.size());

        logMatrix(_messages, "PullSGArray::getChunk: after _messages");
    }
    if (!chunkDesc) {
        throw RetryException(REL_FILE, __FUNCTION__, __LINE__);
    }

    boost::shared_ptr<scidb_msg::Chunk> chunkMsg = chunkDesc->getRecord<scidb_msg::Chunk>();

    if (!chunkMsg->eof())
    {
        LOG4CXX_TRACE(_logger, funcName << "found next chunk message stream="<<stream<<", attId="<<attId);
        assert(chunk != NULL);
        ASSERT_EXCEPTION(compressedBuffer, funcName);

        const int compMethod = chunkMsg->compression_method();
        const size_t decompressedSize = chunkMsg->decompressed_size();

        Address firstElem;
        firstElem.attId = attId;
        for (int i = 0; i < chunkMsg->coordinates_size(); i++) {
            firstElem.coords.push_back(chunkMsg->coordinates(i));
        }

        chunk->initialize(this, &desc, firstElem, compMethod);
        chunk->setSparse(chunkMsg->sparse());
        chunk->setRLE(chunkMsg->rle());
        chunk->setCount(chunkMsg->count());

        compressedBuffer->setCompressionMethod(compMethod);
        compressedBuffer->setDecompressedSize(decompressedSize);
        chunk->decompress(*compressedBuffer);
        assert(chunkMsg->dest_instance() == getLocalStream());
        assert(checkChunkMagic(*chunk));
        return true;
    }
    else
    {
        LOG4CXX_DEBUG(_logger, funcName << "EOF chunk stream="<<stream<<", attId="<<attId);
        return false;
    }
}

boost::shared_ptr<MessageDesc>
PullSGArray::toPositionMesg(const shared_ptr<MessageDesc>& oldChunkMsg)
{
    if (!oldChunkMsg) {
        return shared_ptr<MessageDesc>();
    }

    shared_ptr<scidb_msg::Chunk> oldChunkRecord = oldChunkMsg->getRecord<scidb_msg::Chunk>();
    if (!oldChunkMsg->getBinary()) {
        // positon mesg should not have the next position
        assert(!oldChunkRecord->has_next());
        // we should not be calling this method
        assert(false);
        return shared_ptr<MessageDesc>();
    }

    if (!oldChunkRecord->has_next()) {
        return shared_ptr<MessageDesc>();
    }
    assert(oldChunkRecord->next_coordinates_size()>0);

    shared_ptr<MessageDesc> chunkMsg = boost::make_shared<MessageDesc>(oldChunkMsg->getMessageType());
    shared_ptr<scidb_msg::Chunk> chunkRecord = chunkMsg->getRecord<scidb_msg::Chunk>();

    // set chunk coordinates
    for (size_t i = 0, n = oldChunkRecord->next_coordinates_size(); i < n; ++i) {
        chunkRecord->add_coordinates(oldChunkRecord->next_coordinates(i));
    }
    chunkRecord->set_dest_instance(oldChunkRecord->next_dest_instance());
    chunkRecord->set_has_next(false);

    assert(!oldChunkRecord->eof());
    chunkRecord->set_eof(oldChunkRecord->eof());
    assert(oldChunkRecord->obj_type() == SG_ARRAY_OBJ_TYPE);

    chunkRecord->set_obj_type     (oldChunkRecord->obj_type());
    chunkRecord->set_attribute_id (oldChunkRecord->attribute_id());

    chunkMsg->setQueryID(oldChunkMsg->getQueryID());
    chunkMsg->setSourceInstanceID(oldChunkMsg->getSourceInstanceID());
    return chunkMsg;
}

bool
PullSGArray::getPosition(size_t stream, AttributeID attId, Coordinates& pos, size_t& destStream)
{
    static const char* funcName = "PullSGArray::getPosition: ";
    boost::shared_ptr<MessageDesc> chunkDesc;
    {
        ScopedMutexLock lock(_mutexes[stream % _mutexes.size()]);

        logMatrix(_messages, "PullSGArray::getPosition: before _messages");

        PullSGArray::StreamState& streamState = _messages[attId][stream];

        if (!streamState.isEmpty()) {
            chunkDesc = streamState.head();
            assert(chunkDesc);
            if (!chunkDesc->getBinary()) {
                streamState.pop();
            }
        }

        if (!chunkDesc) {
            assert(streamState.getLastPositionOnlyId() >
                   streamState.getLastRemoteId());
            streamState.setPending(true);
        }
        LOG4CXX_TRACE(_logger, funcName << "attId=" << attId
                     << ", stream=" << stream
                     << ", stream queue size=" << streamState.size());

        logMatrix(_messages, "PullSGArray::getPosition: after _messages");
    }
    if (!chunkDesc) {
        throw RetryException(REL_FILE, __FUNCTION__, __LINE__);
    }

    boost::shared_ptr<scidb_msg::Chunk> chunkMsg = chunkDesc->getRecord<scidb_msg::Chunk>();

    if (!chunkMsg->eof())
    {
        LOG4CXX_TRACE(_logger, funcName << "checking for position stream="<<stream<<", attId="<<attId);

        for (size_t i = 0, n= chunkMsg->coordinates_size(); i < n;  ++i) {
            pos.push_back(chunkMsg->coordinates(i));
        }
        const InstanceID logicalSGDestination = chunkMsg->dest_instance();
        destStream = logicalSGDestination;

        LOG4CXX_TRACE(_logger, funcName << "found next position stream="<<stream
                      <<", attId="<<attId<<", pos="<<pos);
        return true;
    } else {
        LOG4CXX_DEBUG(_logger, funcName << "EOF chunk stream="<<stream<<", attId="<<attId);
        return false;
    }
}

ConstChunk const*
PullSGArray::nextChunkBody(size_t stream, AttributeID attId, MemChunk& chunk)
{
    assert(stream < getStreamCount());
    assert(attId < _messages.size());

    static const bool positionOnly = true;
    requestNextChunk(stream, attId, !positionOnly, _currMinPos[attId]);

    bool result = getChunk(stream, attId,  _currMinPos[attId], &chunk);

    requestNextChunk(stream, attId, positionOnly, _currMinPos[attId]); // pre-fetching

    return (result ? &chunk : NULL);
}

bool
PullSGArray::nextChunkPos(size_t stream, AttributeID attId, Coordinates& pos, size_t& destStream)
{
    assert(stream < getStreamCount());
    assert(attId < _messages.size());

    static const bool positionOnly = true;
    requestNextChunk(stream, attId, positionOnly, pos);

    bool result = getPosition(stream, attId, pos, destStream);
    return result;
}

PullSGArray::RescheduleCallback
PullSGArray::getCallback(AttributeID attId)
{
   assert(attId<_callbacks.size());
   ScopedMutexLock lock(_mutexes[attId % _mutexes.size()]);
   return _callbacks[attId];
}

PullSGArray::RescheduleCallback
PullSGArray::resetCallback(AttributeID attId)
{
    PullSGArray::RescheduleCallback cb;
    return resetCallback(attId,cb);
}

PullSGArray::RescheduleCallback
PullSGArray::resetCallback(AttributeID attId,
                           const PullSGArray::RescheduleCallback& newCb)
{
    assert(attId<_callbacks.size());
    PullSGArray::RescheduleCallback oldCb;
    {
        ScopedMutexLock lock(_mutexes[attId % _mutexes.size()]);
        _callbacks[attId].swap(oldCb);
        _callbacks[attId] = newCb;
    }
    return oldCb;
}

boost::shared_ptr<MessageDesc>
PullSGArray::StreamState::pop()
{
    boost::shared_ptr<MessageDesc> msg;
    if (_msgs.empty()) {
        return msg;
    }
    msg = _msgs.front();
    _msgs.pop_front();
    if (msg->getBinary()) {
        assert(_cachedSize>0);
        --_cachedSize;
    }
    return msg;
}

boost::shared_ptr<ConstArrayIterator>
PullSGArray::getConstIterator(AttributeID attId) const
{
    assert(attId < _messages.size());

    StreamArray* self = const_cast<StreamArray*>(static_cast<const StreamArray*>(this));
    if (!_iterators[attId]) {
        boost::shared_ptr<ConstArrayIterator> cai(new StreamArrayIterator(*self, attId));
        boost::shared_ptr<ConstArrayIterator>& iter =
           const_cast<boost::shared_ptr<ConstArrayIterator>&>(_iterators[attId]);
        iter = cai;
        LOG4CXX_TRACE(_logger, "PullSGArray::getConstIterator(): new iterator attId="<<attId);
    } else {
        if (!_iterators[attId]->end()) {
            LOG4CXX_TRACE(_logger, "PullSGArray::getConstIterator(): increment attId="<<attId);
            ++(*_iterators[attId]);
        }
    }
    return _iterators[attId];
}

namespace {
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

    void signal(AttributeID attrId, const Exception* error)
    {
        ScopedMutexLock cs(_mutex);
        _cond = true;
        if (error) {
            _error = error->copy();
        }
        _activeAttributes.insert(attrId);
        _ev.signal();
    }

    void waitForActiveAttributes(boost::unordered_set<AttributeID>& activeAttributes)
    {
        ScopedMutexLock cs(_mutex);
        while(!_cond) {
            _ev.wait(_mutex, _ec);
        }
        if (_error) {
            _error->raise();
        }
        _cond = false;
        assert(!_activeAttributes.empty());
        activeAttributes.swap(_activeAttributes);
        assert(!activeAttributes.empty());
    }
    private:
    SyncCtx();
    SyncCtx(const SyncCtx& );
    SyncCtx& operator=(const SyncCtx& );
};
}

shared_ptr<Array> pullRedistribute(shared_ptr<Array>& inputArray,
                                   const shared_ptr<Query>& query,
                                   PartitioningSchema ps,
                                   InstanceID instanceIDMask,
                                   const shared_ptr<DistributionMapper>& distMapper,
                                   size_t shift,
                                   const shared_ptr<PartitioningSchemaData>& psData)
{
    static const char * funcName = "pullRedistribute: ";
    LOG4CXX_DEBUG(PullSGArray::_logger, funcName
                  << "PullSG started with partitioning schema = " << ps
                  << ", instanceIDMask = " << instanceIDMask);

    ASSERT_EXCEPTION((ps!=psReplication), funcName);
    ASSERT_EXCEPTION((inputArray->getSupportedAccess()>=Array::MULTI_PASS), funcName);

    const uint64_t instanceCount = query->getInstancesCount();

    assert(instanceIDMask == COORDINATOR_INSTANCE_MASK ||
           instanceIDMask == ALL_INSTANCES_MASK ||
           (size_t)instanceIDMask < query->getInstancesCount());

    if (instanceIDMask == COORDINATOR_INSTANCE_MASK) {
        instanceIDMask = query->getCoordinatorInstanceID();
    }
    if (instanceCount == 1 ||
        (ps == psLocalInstance &&
         (instanceIDMask == ALL_INSTANCES_MASK)))  {
        return inputArray;
    }

    ArrayDesc const& desc = inputArray->getArrayDesc();
    size_t nAttrs = desc.getAttributes().size();
    assert(nAttrs>0);
    bool isEmptyable = (desc.getEmptyBitmapAttribute() != NULL);
    if (isEmptyable && desc.getEmptyBitmapAttribute()->getId() != nAttrs-1) {
        throw USER_EXCEPTION(SCIDB_SE_MERGE, SCIDB_LE_REDISTRIBUTE_ERROR1);
    }

    ASSERT_EXCEPTION( (!query->getOperatorContext()), funcName);

    syncBarrier(0, query);

    // Creating result array with the same descriptor as the input one
    shared_ptr<PullSGArray> pullArray = boost::make_shared<PullSGArrayBlocking>(desc, query);

    // Assigning result of this operation for current query and signal to concurrent handlers that they
    // can continue to work (after the barrier)
    shared_ptr<PullSGContext> sgCtx = boost::make_shared<PullSGContext>(inputArray,
                                                                        pullArray,
                                                                        instanceCount,
                                                                        ps,
                                                                        distMapper,
                                                                        shift,
                                                                        instanceIDMask,
                                                                        psData);
    query->setOperatorContext(sgCtx);

    return pullArray;
}


PullSGArrayBlocking::PullSGArrayBlocking(const ArrayDesc& arrayDesc,
                                         const boost::shared_ptr<Query>& query,
                                         uint32_t chunkPrefetchPerAttribute)
  : PullSGArray(arrayDesc, query, chunkPrefetchPerAttribute)
{}

ConstChunk const* PullSGArrayBlocking::nextChunk(AttributeID attId, MemChunk& memChunk)
{
    static const char * funcName = "PullSGArrayBlocking::nextChunk: ";
    ConstChunk const* chunk(NULL);
    boost::unordered_set<AttributeID> attributeSet;

    shared_ptr<SyncCtx> ctx = boost::make_shared<SyncCtx>(_query);
    PullSGArray::RescheduleCallback cb = boost::bind(&SyncCtx::signal, ctx, attId, _1);
    resetCallback(attId, cb);

    while (true) {
        try {
            chunk = PullSGArray::nextChunk(attId, memChunk);
            break;
        } catch (const scidb::MultiStreamArray::RetryException& ) {
            LOG4CXX_TRACE(_logger,  funcName
                          << "waiting for attId="<<attId);
            ctx->waitForActiveAttributes(attributeSet);
            assert(attributeSet.size()==1);
            assert((*attributeSet.begin()) == attId);
        }
    }
    resetCallback(attId);

    if (isDebug() && chunk) {
        assert((!chunk->isRLE()) ||
               (getArrayDesc().getEmptyBitmapAttribute() == NULL) ||
               (!chunk->isEmpty()));
        assert(chunk->getSize() > 0);

        assert(chunk->getAttributeDesc().getId() == attId);
        assert(attId < chunk->getArrayDesc().getAttributes().size());
    }
    return chunk;
}

void PullSGArrayBlocking::sync()
{
    static const char * funcName = "PullSGArrayBlocking::sync: ";
    shared_ptr<Query> query = Query::getValidQueryPtr(_query);
    shared_ptr<PullSGContext> sgCtx = dynamic_pointer_cast<PullSGContext>(query->getOperatorContext());

    ASSERT_EXCEPTION((sgCtx && sgCtx->getResultArray().get() == this), funcName);
    ASSERT_EXCEPTION((getConstIterator(0)->end()), funcName);

    syncBarrier(1, query);

    LOG4CXX_DEBUG(_logger, funcName << "SG termination barrier reached.");

    // Reset SG Context to NULL

    query->unsetOperatorContext();

    LOG4CXX_DEBUG(_logger, funcName << "PullSG finished");
}

} // namespace
