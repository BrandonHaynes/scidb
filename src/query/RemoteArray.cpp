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

boost::shared_ptr<RemoteArray> RemoteArray::create(const ArrayDesc& arrayDesc, QueryID queryId, InstanceID instanceID)
{
    boost::shared_ptr<Query> query = Query::getQueryByID(queryId);
    boost::shared_ptr<RemoteArray> array = boost::shared_ptr<RemoteArray>(new RemoteArray(arrayDesc, queryId, instanceID));
    query->setRemoteArray(instanceID, array);
    return array;
}

RemoteArray::RemoteArray(const ArrayDesc& arrayDesc, QueryID queryId, InstanceID instanceID)
: StreamArray(arrayDesc), _queryId(queryId), _instanceID(instanceID),
  _received(arrayDesc.getAttributes().size()),
  _messages(arrayDesc.getAttributes().size()),
  _requested(arrayDesc.getAttributes().size())
{
}

void RemoteArray::requestNextChunk(AttributeID attId)
{
    LOG4CXX_TRACE(logger, "RemoteArray fetches next chunk of " << attId << " attribute");
    boost::shared_ptr<MessageDesc> fetchDesc = boost::make_shared<MessageDesc>(mtFetch);
    boost::shared_ptr<scidb_msg::Fetch> fetchRecord = fetchDesc->getRecord<scidb_msg::Fetch>();
    fetchDesc->setQueryID(_queryId);
    fetchRecord->set_attribute_id(attId);
    fetchRecord->set_position_only(false);
    fetchRecord->set_obj_type(0);
    NetworkManager::getInstance()->send(_instanceID, fetchDesc);
}


void RemoteArray::handleChunkMsg(boost::shared_ptr<MessageDesc>& chunkDesc)
{
    assert(chunkDesc->getMessageType() == mtRemoteChunk);
    boost::shared_ptr<scidb_msg::Chunk> chunkMsg = chunkDesc->getRecord<scidb_msg::Chunk>();
    AttributeID attId = chunkMsg->attribute_id();
    assert(attId < _messages.size());
    assert(attId < _received.size());
    _messages[attId] = chunkDesc;
    _received[attId].release();
}

bool RemoteArray::proceedChunkMsg(AttributeID attId, MemChunk& chunk)
{
    boost::shared_ptr<MessageDesc>  chunkDesc = _messages[attId];
    _messages[attId].reset();

    StatisticsScope sScope(_statistics);
    boost::shared_ptr<scidb_msg::Chunk> chunkMsg = chunkDesc->getRecord<scidb_msg::Chunk>();
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
        chunk.setSparse(chunkMsg->sparse());
        chunk.setCount(chunkMsg->count());

        boost::shared_ptr<CompressedBuffer> compressedBuffer = dynamic_pointer_cast<CompressedBuffer>(chunkDesc->getBinary());
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
    boost::shared_ptr<Query> query = Query::getQueryByID(_queryId);
    Semaphore::ErrorChecker errorChecker = bind(&Query::validateQueryPtr, query);
    _received[attId].enter(errorChecker);
    _requested[attId] = true;
    return proceedChunkMsg(attId, chunk) ? &chunk : NULL;
}

#ifndef SCIDB_CLIENT

/* R E M O T E   M E R G E D   A R R A Y */

RemoteMergedArray::RemoteMergedArray(const ArrayDesc& arrayDesc,
                                     const boost::shared_ptr<Query>& query,
                                     Statistics& statistics)
  : MultiStreamArray(query->getInstancesCount(), query->getInstanceID(), arrayDesc, query),
  _callbacks(arrayDesc.getAttributes().size()),
  _query(query),
  _messages(arrayDesc.getAttributes().size(), vector< MessageState >(getStreamCount()))
{
    static const size_t MAX_MUTEX_NUM = 100;
    const uint32_t nMutexes = std::min(arrayDesc.getAttributes().size(), MAX_MUTEX_NUM);
    _mutexes.resize(nMutexes);
    _localArray = query->getCurrentResultArray();
}

boost::shared_ptr<RemoteMergedArray>
RemoteMergedArray::create(const ArrayDesc& arrayDesc,
                          QueryID queryId,
                          Statistics& statistics)
{
    boost::shared_ptr<Query> query = Query::getQueryByID(queryId);
    assert(query);

    boost::shared_ptr<RemoteMergedArray> array =
       boost::shared_ptr<RemoteMergedArray>(new RemoteMergedArray(arrayDesc, query, statistics));
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

        boost::shared_ptr<MessageDesc>  chunkDesc = _messages[attId][stream]._message;
        if (chunkDesc) {
            boost::shared_ptr<scidb_msg::Chunk> chunkMsg = chunkDesc->getRecord<scidb_msg::Chunk>();
            if (!chunkMsg->has_next() || chunkMsg->eof()) {
                // nothing to request
                return;
            }
        }

        _messages[attId][stream]._hasPosition = false;
        _messages[attId][stream]._message.reset();
    }
    boost::shared_ptr<MessageDesc> fetchDesc = boost::make_shared<MessageDesc>(mtFetch);
    boost::shared_ptr<scidb_msg::Fetch> fetchRecord = fetchDesc->getRecord<scidb_msg::Fetch>();
    fetchDesc->setQueryID(_query->getQueryID());
    fetchRecord->set_attribute_id(attId);
    fetchRecord->set_position_only(positionOnly);
    fetchRecord->set_obj_type(1);
    NetworkManager::getInstance()->send(stream, fetchDesc);
}

void
RemoteMergedArray::handleChunkMsg(boost::shared_ptr< MessageDesc>& chunkDesc)
{
    static const char* funcName = "RemoteMergedArray::handleChunkMsg: ";
    assert(chunkDesc->getMessageType() == mtRemoteChunk);

    boost::shared_ptr<scidb_msg::Chunk> chunkMsg = chunkDesc->getRecord<scidb_msg::Chunk>();
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
    boost::shared_ptr<MessageDesc> chunkDesc;
    {
        ScopedMutexLock lock(_mutexes[attId % _mutexes.size()]);
        chunkDesc = _messages[attId][stream]._message;

        logMatrix(_messages, "RemoteMergedArray::getChunk: _messages");

    }
    if (!chunkDesc) {
        throw RetryException(REL_FILE, __FUNCTION__, __LINE__);
    }

    boost::shared_ptr<scidb_msg::Chunk> chunkMsg = chunkDesc->getRecord<scidb_msg::Chunk>();

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
        chunk->setSparse(chunkMsg->sparse());
        chunk->setRLE(chunkMsg->rle());
        chunk->setCount(chunkMsg->count());

        boost::shared_ptr<CompressedBuffer> compressedBuffer =
           dynamic_pointer_cast<CompressedBuffer>(chunkDesc->getBinary());
        compressedBuffer->setCompressionMethod(compMethod);
        compressedBuffer->setDecompressedSize(decompressedSize);
        chunk->decompress(*compressedBuffer);
        assert(checkChunkMagic(*chunk));
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
    boost::shared_ptr<MessageDesc> chunkDesc;
    {
        ScopedMutexLock lock(_mutexes[attId % _mutexes.size()]);
        chunkDesc = _messages[attId][stream]._message;
        logMatrix(_messages, "RemoteMergedArray::getPos: _messages");

    }
    if (!chunkDesc) {
        throw RetryException(REL_FILE, __FUNCTION__, __LINE__);
    }

    boost::shared_ptr<scidb_msg::Chunk> chunkMsg = chunkDesc->getRecord<scidb_msg::Chunk>();

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
                chunk->setSparse(srcChunk.isSparse());
                chunk->setRLE(srcChunk.isRLE());
                if (srcChunk.isRLE() &&
                    !srcChunk.getAttributeDesc().isEmptyIndicator() &&
                    getArrayDesc().getEmptyBitmapAttribute() != NULL &&
                    srcChunk.getBitmapSize() == 0) {
                    srcChunk.makeClosure(*chunk, srcChunk.getEmptyBitmap());
                } else {
                    chunk->allocate(srcChunk.getSize());
                    assert(checkChunkMagic(srcChunk));
                    memcpy(chunk->getData(), srcChunk.getData(), srcChunk.getSize());
                }
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

boost::shared_ptr<ConstArrayIterator>
RemoteMergedArray::getConstIterator(AttributeID attId) const
{
    assert(attId < _messages.size());

    StreamArray* self = const_cast<StreamArray*>(static_cast<const StreamArray*>(this));
    if (!_iterators[attId]) {
        boost::shared_ptr<ConstArrayIterator> cai(new StreamArrayIterator(*self, attId));
        boost::shared_ptr<ConstArrayIterator>& iter =
           const_cast<boost::shared_ptr<ConstArrayIterator>&>(_iterators[attId]);
        iter = cai;
        LOG4CXX_TRACE(logger, "RemoteMergedArray::getConstIterator(): new iterator attId="<<attId);
    } else {
        if (!_iterators[attId]->end()) {
            LOG4CXX_TRACE(logger, "RemoteMergedArray::getConstIterator(): increment attId="<<attId);
            ++(*_iterators[attId]);
        }
    }
    return _iterators[attId];
}

#endif

} // namespace
