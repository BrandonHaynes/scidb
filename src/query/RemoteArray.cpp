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
 * @file RemoteArray.cpp
 *
 * @author roman.simakov@gmail.com
 */

#include <boost/shared_ptr.hpp>
#include <boost/make_shared.hpp>

#include <log4cxx/logger.h>

#include <system/Config.h>
#include <system/SciDBConfigOptions.h>
#include <network/proto/scidb_msg.pb.h>
#include <network/NetworkManager.h>
#include <network/MessageHandleJob.h>
#include <query/QueryProcessor.h>
#include <query/RemoteArray.h>
#include <query/Statistics.h>
#include <system/Exceptions.h>

using namespace std;
using namespace boost;

namespace scidb
{

// to prevent visibility of variable outside of file
static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.qproc.remotearray"));

RemoteArrayContext::RemoteArrayContext(size_t numInstances):
    _inboundArrays(numInstances), _outboundArrays(numInstances)
{}

shared_ptr<RemoteArray> RemoteArrayContext::getInboundArray(InstanceID logicalSrcInstanceID) const
{
    assert(!_inboundArrays.empty());
    assert(logicalSrcInstanceID < _inboundArrays.size());
    return _inboundArrays[logicalSrcInstanceID];
}

void RemoteArrayContext::setInboundArray(InstanceID logicalSrcInstanceID, const shared_ptr<RemoteArray>& array)
{
    assert(!_inboundArrays.empty());
    assert(logicalSrcInstanceID < _inboundArrays.size());
    _inboundArrays[logicalSrcInstanceID] = array;
}

shared_ptr<Array> RemoteArrayContext::getOutboundArray(const InstanceID& logicalDestInstanceID) const
{
    assert(!_outboundArrays.empty());
    assert(logicalDestInstanceID < _outboundArrays.size());
    return _outboundArrays[logicalDestInstanceID];
}

void RemoteArrayContext::setOutboundArray(const InstanceID& logicalDestInstanceID, const shared_ptr<Array>& array)
{
    assert(!_outboundArrays.empty());
    assert(logicalDestInstanceID < _outboundArrays.size());
    _outboundArrays[logicalDestInstanceID] = array;
}

shared_ptr<RemoteArray> RemoteArray::create(
        shared_ptr<RemoteArrayContext>& remoteArrayContext,
        const ArrayDesc& arrayDesc, QueryID queryId, InstanceID logicalSrcInstanceID)
{
    shared_ptr<Query> query = Query::getQueryByID(queryId);

    // Note: if make_shared is used here, you will get a compilation error saying RemoteArray::RemoteArray is private.
    shared_ptr<RemoteArray> array = shared_ptr<RemoteArray>(new RemoteArray(arrayDesc, queryId, logicalSrcInstanceID));
    remoteArrayContext->setInboundArray(logicalSrcInstanceID, array);
    return array;
}

RemoteArray::RemoteArray(const ArrayDesc& arrayDesc, QueryID queryId, InstanceID logicalSrcInstanceID)
: StreamArray(arrayDesc), _queryId(queryId), _instanceID(logicalSrcInstanceID),
  _received(arrayDesc.getAttributes().size()),
  _messages(arrayDesc.getAttributes().size()),
  _requested(arrayDesc.getAttributes().size())
{
}

shared_ptr<RemoteArrayContext> RemoteArray::getContext(boost::shared_ptr<Query>& query)
{
    Query::validateQueryPtr(query);
    shared_ptr<RemoteArrayContext> context = dynamic_pointer_cast<RemoteArrayContext>(query->getOperatorContext());
    ASSERT_EXCEPTION(context, "RemoteArray::getContext failed.");
    return context;
}

void RemoteArray::requestNextChunk(AttributeID attId)
{
    LOG4CXX_TRACE(logger, "RemoteArray fetches next chunk of " << attId << " attribute");
    shared_ptr<MessageDesc> fetchDesc = make_shared<MessageDesc>(mtFetch);
    shared_ptr<scidb_msg::Fetch> fetchRecord = fetchDesc->getRecord<scidb_msg::Fetch>();
    fetchDesc->setQueryID(_queryId);
    fetchRecord->set_attribute_id(attId);
    fetchRecord->set_position_only(false);
    fetchRecord->set_obj_type(0);
    NetworkManager::getInstance()->send(_instanceID, fetchDesc);
}

void RemoteArray::handleChunkMsg(shared_ptr<MessageDesc>& chunkDesc)
{
    assert(chunkDesc->getMessageType() == mtRemoteChunk);
    shared_ptr<scidb_msg::Chunk> chunkMsg = chunkDesc->getRecord<scidb_msg::Chunk>();
    AttributeID attId = chunkMsg->attribute_id();
    assert(attId < _messages.size());
    assert(attId < _received.size());
    _messages[attId] = chunkDesc;
    _received[attId].release();
}

bool RemoteArray::proceedChunkMsg(AttributeID attId, MemChunk& chunk)
{
    shared_ptr<MessageDesc>  chunkDesc = _messages[attId];
    _messages[attId].reset();

    StatisticsScope sScope(_statistics);
    shared_ptr<scidb_msg::Chunk> chunkMsg = chunkDesc->getRecord<scidb_msg::Chunk>();
    currentStatistics->receivedSize += chunkDesc->getMessageSize();
    currentStatistics->receivedMessages++;

    if (!chunkMsg->eof())
    {
        LOG4CXX_TRACE(logger, "RemoteArray received next chunk message");
        assert(chunkDesc->getBinary());

        const int compMethod = chunkMsg->compression_method();
        const size_t decompressedSize = chunkMsg->decompressed_size();

        Address firstElem;
        firstElem.attId = attId;
        for (int i = 0; i < chunkMsg->coordinates_size(); i++) {
            firstElem.coords.push_back(chunkMsg->coordinates(i));
        }

        chunk.initialize(this, &desc, firstElem, compMethod);
        chunk.setCount(chunkMsg->count());

        shared_ptr<CompressedBuffer> compressedBuffer = dynamic_pointer_cast<CompressedBuffer>(chunkDesc->getBinary());
        compressedBuffer->setCompressionMethod(compMethod);
        compressedBuffer->setDecompressedSize(decompressedSize);
        chunk.decompress(*compressedBuffer);
        LOG4CXX_TRACE(logger, "RemoteArray initializes next chunk");

        requestNextChunk(attId);
        return true;
    }
    else
    {
        return false;
    }
}


ConstChunk const* RemoteArray::nextChunk(AttributeID attId, MemChunk& chunk)
{
    if (!_requested[attId]) {
        requestNextChunk(attId);
    }
    shared_ptr<Query> query = Query::getQueryByID(_queryId);
    Semaphore::ErrorChecker errorChecker = bind(&Query::validateQueryPtr, query);
    _received[attId].enter(errorChecker);
    _requested[attId] = true;
    return proceedChunkMsg(attId, chunk) ? &chunk : NULL;
}

#ifndef SCIDB_CLIENT

/* R E M O T E   M E R G E D   A R R A Y */

RemoteMergedArray::RemoteMergedArray(const ArrayDesc& arrayDesc,
                                     const shared_ptr<Query>& query,
                                     Statistics& statistics)
 : MultiStreamArray(query->getInstancesCount(), query->getInstanceID(), arrayDesc, false, query),
  _callbacks(arrayDesc.getAttributes().size()),
  _query(query),
  _messages(arrayDesc.getAttributes().size(), vector< MessageState >(getStreamCount()))
{
    static const size_t MAX_MUTEX_NUM = 100;
    const uint32_t nMutexes = std::min(arrayDesc.getAttributes().size(), MAX_MUTEX_NUM);
    _mutexes.resize(nMutexes);
    _localArray = query->getCurrentResultArray();
}

shared_ptr<RemoteMergedArray>
RemoteMergedArray::create(const ArrayDesc& arrayDesc,
                          QueryID queryId,
                          Statistics& statistics)
{
    shared_ptr<Query> query = Query::getQueryByID(queryId);
    assert(query);

    shared_ptr<RemoteMergedArray> array =
       shared_ptr<RemoteMergedArray>(new RemoteMergedArray(arrayDesc, query, statistics));
    query->setMergedArray(array);
    return array;
}


std::ostream& operator << (std::ostream& out,
                           RemoteMergedArray::MessageState& state)
{
    out << "["<<state._hasPosition<<","<<state._message.get()<<"]";
    return out;
}

namespace {
template<typename T>
void logMatrix(std::vector<std::vector<T> >& matrix, const std::string& prefix)
{
    if (!logger->isTraceEnabled()) {
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
    LOG4CXX_TRACE(logger, prefix << ": " << ss.str());
}
}

void
RemoteMergedArray::requestNextChunk(size_t stream, AttributeID attId, bool positionOnly)
{
    static const char* funcName = "RemoteMergedArray::requestNextChunk: ";
    if (_query->getInstanceID() == stream) {
        return;
    }
    {
        ScopedMutexLock lock(_mutexes[attId % _mutexes.size()]);

        logMatrix(_messages, "RemoteMergedArray::requestNextChunk(): _messages");

        if (positionOnly && _messages[attId][stream]._message) {
            // we must already have the position
            assert(_messages[attId][stream]._hasPosition);
            return;
        }

        if (!_messages[attId][stream]._hasPosition) {
            // already requested
            assert(!_messages[attId][stream]._message);
            return;
        }

        LOG4CXX_TRACE(logger, funcName << " request next chunk attId=" << attId
                     << (positionOnly? ", position only" : ", full")
                     << ", stream #" << stream);

        shared_ptr<MessageDesc>  chunkDesc = _messages[attId][stream]._message;
        if (chunkDesc) {
            shared_ptr<scidb_msg::Chunk> chunkMsg = chunkDesc->getRecord<scidb_msg::Chunk>();
            if (!chunkMsg->has_next() || chunkMsg->eof()) {
                // nothing to request
                return;
            }
        }

        _messages[attId][stream]._hasPosition = false;
        _messages[attId][stream]._message.reset();
    }
    shared_ptr<MessageDesc> fetchDesc = make_shared<MessageDesc>(mtFetch);
    shared_ptr<scidb_msg::Fetch> fetchRecord = fetchDesc->getRecord<scidb_msg::Fetch>();
    fetchDesc->setQueryID(_query->getQueryID());
    fetchRecord->set_attribute_id(attId);
    fetchRecord->set_position_only(positionOnly);
    fetchRecord->set_obj_type(1);
    NetworkManager::getInstance()->send(stream, fetchDesc);
}

void
RemoteMergedArray::handleChunkMsg(shared_ptr< MessageDesc>& chunkDesc)
{
    static const char* funcName = "RemoteMergedArray::handleChunkMsg: ";
    assert(chunkDesc->getMessageType() == mtRemoteChunk);

    shared_ptr<scidb_msg::Chunk> chunkMsg = chunkDesc->getRecord<scidb_msg::Chunk>();
    AttributeID attId = chunkMsg->attribute_id();
    size_t stream = size_t(_query->mapPhysicalToLogical(chunkDesc->getSourceInstanceID()));

    assert(stream < getStreamCount());
    assert(attId < _messages.size());

    if (chunkMsg->warnings_size())
    {
        for (int i = 0; i < chunkMsg->warnings_size(); ++i)
        {
            const ::scidb_msg::Chunk_Warning& w = chunkMsg->warnings(i);
            _query->postWarning(
                        Warning(
                            w.file().c_str(),
                            w.function().c_str(),
                            w.line(),
                            w.strings_namespace().c_str(),
                            w.code(),
                            w.what_str().c_str(),
                            w.stringified_code().c_str())
                        );
        }
    }
    RescheduleCallback cb;
    {
        ScopedMutexLock lock(_mutexes[attId % _mutexes.size()]);
        LOG4CXX_TRACE(logger,  funcName << "received next chunk message attId="<<attId
                     <<", stream="<<stream
                     <<", queryID="<<_query->getQueryID());
        logMatrix(_messages, "RemoteMergedArray::handleChunkMsg: _messages");

        assert(!_messages[attId][stream]._message);
        assert(!_messages[attId][stream]._hasPosition);

        _messages[attId][stream]._message = chunkDesc;
        _messages[attId][stream]._hasPosition = true;

        assert(_messages[attId][stream]._message);
        assert(_messages[attId][stream]._hasPosition);
        cb = _callbacks[attId];
    }
    if (cb) {
        const Exception* error(NULL);
        cb(error);
    } else {
        _query->validate();
        LOG4CXX_TRACE(logger, funcName << "no callback is set attId="<<attId
                      <<", stream="<<stream
                      <<", queryID="<<_query->getQueryID());
    }
}

bool
RemoteMergedArray::getChunk(size_t stream, AttributeID attId, MemChunk* chunk)
{
    static const char* funcName = "RemoteMergedArray::getChunk: ";
    assert(chunk);
    shared_ptr<MessageDesc> chunkDesc;
    {
        ScopedMutexLock lock(_mutexes[attId % _mutexes.size()]);
        chunkDesc = _messages[attId][stream]._message;

        logMatrix(_messages, "RemoteMergedArray::getChunk: _messages");

    }
    if (!chunkDesc) {
        throw RetryException(REL_FILE, __FUNCTION__, __LINE__);
    }

    shared_ptr<scidb_msg::Chunk> chunkMsg = chunkDesc->getRecord<scidb_msg::Chunk>();

    if (!chunkMsg->eof())
    {
        LOG4CXX_TRACE(logger, funcName << "found next chunk message stream="<<stream<<", attId="<<attId);
        assert(chunk != NULL);
        ASSERT_EXCEPTION((chunkDesc->getBinary()), funcName);

        const int compMethod = chunkMsg->compression_method();
        const size_t decompressedSize = chunkMsg->decompressed_size();

        Address firstElem;
        firstElem.attId = attId;
        for (int i = 0; i < chunkMsg->coordinates_size(); i++) {
            firstElem.coords.push_back(chunkMsg->coordinates(i));
        }

        chunk->initialize(this, &desc, firstElem, compMethod);
        chunk->setCount(chunkMsg->count());

        shared_ptr<CompressedBuffer> compressedBuffer =
           dynamic_pointer_cast<CompressedBuffer>(chunkDesc->getBinary());
        compressedBuffer->setCompressionMethod(compMethod);
        compressedBuffer->setDecompressedSize(decompressedSize);
        chunk->decompress(*compressedBuffer);
        checkChunkMagic(*chunk);
        return true;
    }
    else
    {
        LOG4CXX_TRACE(logger, funcName << "EOF chunk stream="<<stream<<", attId="<<attId);
        return false;
    }
}

bool
RemoteMergedArray::getPos(size_t stream, AttributeID attId, Coordinates& pos)
{
    static const char* funcName = "RemoteMergedArray::getPos: ";
    shared_ptr<MessageDesc> chunkDesc;
    {
        ScopedMutexLock lock(_mutexes[attId % _mutexes.size()]);
        chunkDesc = _messages[attId][stream]._message;
        logMatrix(_messages, "RemoteMergedArray::getPos: _messages");

    }
    if (!chunkDesc) {
        throw RetryException(REL_FILE, __FUNCTION__, __LINE__);
    }

    shared_ptr<scidb_msg::Chunk> chunkMsg = chunkDesc->getRecord<scidb_msg::Chunk>();

    if (!chunkMsg->eof())
    {
        LOG4CXX_TRACE(logger, funcName << "checking for position stream="<<stream<<", attId="<<attId);
        if (chunkMsg->has_next())
        {
            for (int i = 0; i < chunkMsg->next_coordinates_size(); i++) {
                pos.push_back(chunkMsg->next_coordinates(i));
            }
            LOG4CXX_TRACE(logger, funcName << "found next position stream="<<stream
                          <<", attId="<<attId<<", pos="<<pos);
            return true;
        }
        return false; //no position => eof
    }
    else
    {
        LOG4CXX_TRACE(logger, funcName << "EOF chunk stream="<<stream<<", attId="<<attId);
        return false;
    }
}

bool
RemoteMergedArray::fetchChunk(size_t stream, AttributeID attId, MemChunk* chunk)
{
    assert(stream < getStreamCount());
    assert(chunk);
    if (_query->getInstanceID() != stream) {

        return getChunk(stream, attId, chunk);

    } else {
        // We get chunk body from the current result array on local instance
        bool result = false;
        if (!_localArray->getConstIterator(attId)->end()) {
            {
                const ConstChunk& srcChunk = _localArray->getConstIterator(attId)->getChunk();
                PinBuffer buf(srcChunk);
                Address firstElem;
                firstElem.attId = attId;
                firstElem.coords = srcChunk.getFirstPosition(false);
                chunk->initialize(this, &desc, firstElem, srcChunk.getCompressionMethod());
                if (!srcChunk.getAttributeDesc().isEmptyIndicator() &&
                    getArrayDesc().getEmptyBitmapAttribute() != NULL &&
                    srcChunk.getBitmapSize() == 0) {
                    checkChunkMagic(srcChunk);
                    srcChunk.makeClosure(*chunk, srcChunk.getEmptyBitmap());
                } else {
                    // MemChunk::allocate
                    chunk->allocate(srcChunk.getSize());
                    // Copy the MemChunk::data field bit-for-bit from one to the other
                    memcpy(chunk->getData(), srcChunk.getData(), srcChunk.getSize());
                }
                // This is a no-op when chunk is a MemChunk
                chunk->write(_query);
            }
            ++(*_localArray->getConstIterator(attId));
            result = true;
        }
        return result;
    }
}

bool
RemoteMergedArray::fetchPosition(size_t stream, AttributeID attId, Coordinates& position)
{
   if (_query->getInstanceID() != stream) {

       return getPos(stream, attId, position);

   } else {
       // We get chunk body from the current result array on local instance
       bool result = false;
       if (!_localArray->getConstIterator(attId)->end()) {
           position = _localArray->getConstIterator(attId)->getPosition();
           result = true;
       } else {
           result = false;
       }
       return result;
   }
}

ConstChunk const*
RemoteMergedArray::nextChunkBody(size_t stream, AttributeID attId, MemChunk& chunk)
{
    assert(stream < getStreamCount());
    assert(attId < _messages.size());

    bool result = fetchChunk(stream, attId, &chunk);
    return (result ? &chunk : NULL);
}

bool
RemoteMergedArray::nextChunkPos(size_t stream, AttributeID attId, Coordinates& pos, size_t& destStream)
{
    assert(stream < getStreamCount());
    assert(attId < _messages.size());
    static const bool positionOnly = true;
    requestNextChunk(stream, attId, positionOnly);

    bool result = fetchPosition(stream, attId, pos);

    requestNextChunk(stream, attId, false);
    destStream = _query->getInstanceID();
    return result;
}

shared_ptr<ConstArrayIterator>
RemoteMergedArray::getConstIterator(AttributeID attId) const
{
    assert(attId < _messages.size());

    const StreamArray* pself = this;
    StreamArray* self = const_cast<StreamArray*>(pself);

    if (!_iterators[attId]) {
        shared_ptr<ConstArrayIterator> cai(new StreamArrayIterator(*self, attId));
        shared_ptr<ConstArrayIterator>& iter =
           const_cast<shared_ptr<ConstArrayIterator>&>(_iterators[attId]);
        iter = cai;
        LOG4CXX_TRACE(logger,
                      "RemoteMergedArray::getConstIterator(): new iterator attId="<<attId);
    } else {
        if (!_iterators[attId]->end()) {
            LOG4CXX_TRACE(logger,
                          "RemoteMergedArray::getConstIterator(): increment attId="<<attId);
            ++(*_iterators[attId]);
        }
    }

    return _iterators[attId];
}

#endif

} // namespace
