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
 * BaseConnection.cpp
 *
 *  Created on: Jan 15, 2010
 *      Author: roman.simakov@gmail.com
 */

#include <boost/bind.hpp>
#include <boost/make_shared.hpp>
#include "network/proto/scidb_msg.pb.h"
#include "BaseConnection.h"
#include "system/Exceptions.h"

using namespace std;
using namespace boost;

namespace scidb
{
// Logger for network subsystem. static to prevent visibility of variable outside of file
log4cxx::LoggerPtr BaseConnection::logger(log4cxx::Logger::getLogger("scidb.services.network"));

/**
 * Message descriptor
 * @param messageType provides related google protocol buffer message
 * @param binary a pointer to buffer that will be used for reading or writing
 * binary data. Can be ommited when the message has no binary data
 */
MessageDesc::MessageDesc(MessageID messageType, const boost::shared_ptr<SharedBuffer>& binary)
: _binary(binary)
{
   init(messageType);
}
MessageDesc::MessageDesc()
{
  init(mtNone);
}
MessageDesc::MessageDesc(MessageID messageType)
{
   init(messageType);
}
MessageDesc::MessageDesc(const boost::shared_ptr<SharedBuffer>& binary)
: _binary(binary)
{
   init(mtNone);
}
void
MessageDesc::init(MessageID messageType)
{
    setToZeroInDebug(&_messageHeader, sizeof(_messageHeader));

    _messageHeader.netProtocolVersion = NET_PROTOCOL_CURRENT_VER;
    _messageHeader.sourceInstanceID = CLIENT_INSTANCE;
    _messageHeader.recordSize = 0;
    _messageHeader.binarySize = 0;
    _messageHeader.messageType = static_cast<uint16_t>(messageType);
    _messageHeader.queryID = 0;

    if (messageType != mtNone) {
        _record = createRecordByType(messageType);
    }
}
void MessageDesc::writeConstBuffers(std::vector<asio::const_buffer>& constBuffers)
{
    if (_messageHeader.recordSize == 0) {
        ostream out(&_recordStream);
        _record->SerializeToOstream(&out);
        _messageHeader.recordSize = _recordStream.size();
    }
    const bool haveBinary = _binary && _binary->getSize();
    if (haveBinary) {
        _messageHeader.binarySize = _binary->getSize();
    }

    constBuffers.push_back(asio::buffer(&_messageHeader, sizeof(_messageHeader)));
    constBuffers.push_back(asio::buffer(_recordStream.data()));
    if (haveBinary) {
        constBuffers.push_back(asio::buffer(_binary->getData(), _binary->getSize()));
    }

    LOG4CXX_TRACE(BaseConnection::logger, "writeConstBuffers: messageType=" << _messageHeader.messageType <<
                  " ; recordSize=" << _messageHeader.recordSize <<
                  " ; binarySize=" << _messageHeader.binarySize);
}


bool MessageDesc::parseRecord(size_t bufferSize)
{
    _recordStream.commit(bufferSize);

    _record = createRecord(static_cast<MessageID>(_messageHeader.messageType));

    istream inStream(&_recordStream);
    bool rc = _record->ParseFromIstream(&inStream);
    return (rc && _record->IsInitialized());
}


void MessageDesc::prepareBinaryBuffer()
{
    if (_messageHeader.binarySize) {
        if (_binary) {
            _binary->reallocate(_messageHeader.binarySize);
        }
        else {
            // For chunks it's correct but for other data it can required other buffers
            _binary = boost::shared_ptr<SharedBuffer>(new CompressedBuffer());
            _binary->allocate(_messageHeader.binarySize);
        }

    }
}


MessagePtr MessageDesc::createRecordByType(MessageID messageType)
{
    switch (messageType)
    {
    case mtPrepareQuery:
    case mtExecuteQuery:
        return MessagePtr(new scidb_msg::Query());
    case mtPreparePhysicalPlan:
        return MessagePtr(new scidb_msg::PhysicalPlan());
    case mtFetch:
        return MessagePtr(new scidb_msg::Fetch());
    case mtChunk:
    case mtChunkReplica:
    case mtRecoverChunk:
    case mtAggregateChunk:
    case mtRemoteChunk:
        return MessagePtr(new scidb_msg::Chunk());
    case mtQueryResult:
        return MessagePtr(new scidb_msg::QueryResult());
    case mtError:
        return MessagePtr(new scidb_msg::Error());
    case mtSyncRequest:
    case mtSyncResponse:
    case mtCancelQuery:
    case mtNotify:
    case mtWait:
    case mtBarrier:
    case mtBufferSend:
    case mtAlive:
    case mtReplicaSyncRequest:
    case mtReplicaSyncResponse:
    case mtAbort:
    case mtCommit:
    case mtCompleteQuery:
        return MessagePtr(new scidb_msg::DummyQuery());

    //Resources chat messages
    case mtResourcesFileExistsRequest:
        return MessagePtr(new scidb_msg::ResourcesFileExistsRequest());
    case mtResourcesFileExistsResponse:
        return MessagePtr(new scidb_msg::ResourcesFileExistsResponse());
    case mtControl:
        return MessagePtr(new scidb_msg::Control());
    default:
        LOG4CXX_ERROR(BaseConnection::logger, "Unknown message type " << messageType);
        throw SYSTEM_EXCEPTION(SCIDB_SE_NETWORK, SCIDB_LE_UNKNOWN_MESSAGE_TYPE) << messageType;
    }
}

bool MessageDesc::validate()
{
    if (_messageHeader.netProtocolVersion != NET_PROTOCOL_CURRENT_VER) {
        LOG4CXX_ERROR(BaseConnection::logger, "Invalid protocol version: " << _messageHeader.netProtocolVersion);
        return false;
    }
    switch (_messageHeader.messageType)
    {
    case mtPrepareQuery:
    case mtExecuteQuery:
    case mtPreparePhysicalPlan:
    case mtFetch:
    case mtChunk:
    case mtRecoverChunk:
    case mtChunkReplica:
    case mtReplicaSyncRequest:
    case mtReplicaSyncResponse:
    case mtAggregateChunk:
    case mtQueryResult:
    case mtError:
    case mtSyncRequest:
    case mtSyncResponse:
    case mtCancelQuery:
    case mtRemoteChunk:
    case mtNotify:
    case mtWait:
    case mtBarrier:
    case mtBufferSend:
    case mtAlive:
    case mtResourcesFileExistsRequest:
    case mtResourcesFileExistsResponse:
    case mtAbort:
    case mtCommit:
    case mtCompleteQuery:
    case mtControl:
        break;
    default:
        return false;
    }

    return true;
}

/***
 * B a s e C o n n e c t i o n
 */
BaseConnection::BaseConnection(boost::asio::io_service& ioService): _socket(ioService)
{
    assert(mtSystemMax == SYSTEM_MAX_MSG_ID);
}

BaseConnection::~BaseConnection()
{
    disconnect();
}

void BaseConnection::connect(string address, uint16_t port)
{
   LOG4CXX_DEBUG(logger, "Connecting to " << address << ":" << port)

   asio::ip::tcp::resolver resolver(_socket.get_io_service());

   stringstream serviceName;
   serviceName << port;
   asio::ip::tcp::resolver::query query(address, serviceName.str());
   asio::ip::tcp::resolver::iterator endpoint_iterator = resolver.resolve(query);
   asio::ip::tcp::resolver::iterator end;

   boost::system::error_code error = boost::asio::error::host_not_found;
   while (error && endpoint_iterator != end)
   {
      _socket.close();
      _socket.connect(*endpoint_iterator++, error);
   }
   if (error)
   {
      LOG4CXX_FATAL(logger, "Error #" << error << " when connecting to " << address << ":" << port);
      throw SYSTEM_EXCEPTION(SCIDB_SE_NETWORK, SCIDB_LE_CONNECTION_ERROR) << error << address << port;
   }

   configConnectedSocket();
   LOG4CXX_DEBUG(logger, "Connected to " << address << ":" << port);
}

void BaseConnection::configConnectedSocket()
{
   boost::asio::ip::tcp::no_delay no_delay(true);
   _socket.set_option(no_delay);
   boost::asio::socket_base::keep_alive keep_alive(true);
   _socket.set_option(keep_alive);

   int s = _socket.native();
   int optval;
   socklen_t optlen = sizeof(optval);

   /* Set the option active */
   optval = 1;
   if(setsockopt(s, SOL_TCP, TCP_KEEPCNT, &optval, optlen) < 0) {
      perror("setsockopt()");
   }
   optval = 30;
   if(setsockopt(s, SOL_TCP, TCP_KEEPIDLE, &optval, optlen) < 0) {
      perror("setsockopt()");
   }
   optval = 30;
   if(setsockopt(s, SOL_TCP, TCP_KEEPINTVL, &optval, optlen) < 0) {
      perror("setsockopt()");
   }

   if (logger->isTraceEnabled()) {
      boost::asio::socket_base::receive_buffer_size optionRecv;
      _socket.get_option(optionRecv);
      int size = optionRecv.value();
      LOG4CXX_TRACE(logger, "Socket receive buffer size = " << size);
      boost::asio::socket_base::send_buffer_size optionSend;
      _socket.get_option(optionSend);
      size = optionSend.value();
      LOG4CXX_TRACE(logger, "Socket send buffer size = " << size);
   }
}

void BaseConnection::disconnect()
{
    _socket.close();
    LOG4CXX_DEBUG(logger, "Disconnected")
}

void BaseConnection::send(boost::shared_ptr<MessageDesc>& messageDesc)
{
    LOG4CXX_TRACE(BaseConnection::logger, "BaseConnection::send begin");
    try
    {
        std::vector<boost::asio::const_buffer> constBuffers;
        messageDesc->_messageHeader.sourceInstanceID = CLIENT_INSTANCE;
        messageDesc->writeConstBuffers(constBuffers);
        boost::asio::write(_socket, constBuffers);

        LOG4CXX_TRACE(BaseConnection::logger, "BaseConnection::send end");
    }
    catch (const boost::exception &e)
    {
        throw SYSTEM_EXCEPTION(SCIDB_SE_NETWORK, SCIDB_LE_CANT_SEND_RECEIVE);
    }
}

} // namespace
