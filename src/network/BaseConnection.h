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
 * @file BaseConnection.h
 *
 * @author: roman.simakov@gmail.com
 *
 * @brief The BaseConnection class
 *
 * The file includes the main data structures and interfaces used in message exchanging.
 * Also the file contains BaseConnection class for synchronous connection and message exchanging.
 * This class is used in client code. The scidb engine will use a class which is derived from BaseConnection.
 */

#ifndef BASECONNECTION_H_
#define BASECONNECTION_H_

#include <stdint.h>
#include <boost/asio.hpp>
#include <boost/exception/diagnostic_information.hpp>
#include <boost/exception_ptr.hpp>
#include <log4cxx/logger.h>
#include <vector>

#include "array/Metadata.h"
#include "array/Array.h"
#include "util/Semaphore.h"
#include <util/NetworkMessage.h>
#include <util/Network.h>

namespace scidb
{

/**
 * If you are changing the format of the protobuf messages in src/network/protoscidb_msg.proto
 * (especially by adding required message fields), or any structures like MessageType and/or MessagesHeader
 * - you must increment this number. Notice that this will impact all the client tools (by breaking backwards compatibility).
 *
 * Revision history:
 *
 * NET_PROTOCOL_CURRENT_VER = 4:
 *    Author: tigor
 *    Date: 7/17/2014
 *    Ticket: 4138, 3667, ...
 *
 *
 * NET_PROTOCOL_CURRENT_VER = 3:
 *    Author: ??
 *    Date: ??
 *    Ticket: ??
 *    Note: Initial implementation dating back some time
 */
const uint32_t NET_PROTOCOL_CURRENT_VER = 4;

/**
 * Messageg types
 */
enum MessageType
{
    mtNone=SYSTEM_NONE_MSG_ID,
    mtExecuteQuery,
    mtPreparePhysicalPlan,
    mtUnusedPlus3,
    mtFetch,
    mtChunk,
    mtChunkReplica,
    mtRecoverChunk,
    mtReplicaSyncRequest,
    mtReplicaSyncResponse,
    mtAggregateChunk,
    mtQueryResult,
    mtError,
    mtSyncRequest,
    mtSyncResponse,
    mtCancelQuery,
    mtRemoteChunk,
    mtNotify,
    mtWait,
    mtBarrier,
    mtBufferSend,
    mtAlive,
    mtPrepareQuery,
    mtResourcesFileExistsRequest,
    mtResourcesFileExistsResponse,
    mtAbort,
    mtCommit,
    mtCompleteQuery,
    mtControl,
    mtSystemMax // must be last, make sure scidb::SYSTEM_MAX_MSG_ID is set to this value
};


struct MessageHeader
{
    uint16_t netProtocolVersion;         /** < Version of network protocol */
    uint16_t messageType;                /** < Type of message */
    uint32_t recordSize;                 /** < The size of structured part of message to know what buffer size we must allocate */
    uint32_t binarySize;                 /** < The size of unstructured part of message to know what buffer size we must allocate */
    InstanceID sourceInstanceID;         /** < The source instance number */
    uint64_t queryID;                    /** < Query ID */

    MessageHeader()
        : netProtocolVersion(0),
          messageType(0),
          recordSize(0),
          binarySize(0),
          sourceInstanceID(0),
          queryID(0) {}
};


/**
 * Message descriptor with all necessary parts
 */
class MessageDesc
{
public:
   MessageDesc();
   MessageDesc(MessageID messageType);
   MessageDesc(const boost::shared_ptr< SharedBuffer >& binary);
   MessageDesc(MessageID messageType, const boost::shared_ptr< SharedBuffer >& binary);
   virtual ~MessageDesc() {}
   void writeConstBuffers(std::vector<boost::asio::const_buffer>& constBuffers);
   bool parseRecord(size_t bufferSize);
   void prepareBinaryBuffer();

   InstanceID getSourceInstanceID() {
      return _messageHeader.sourceInstanceID;
   }

   /**
    * This method is not part of the public API
    */
   void setSourceInstanceID(const InstanceID& instanceId) {
      _messageHeader.sourceInstanceID = instanceId;
   }

   template <class Derived>
    boost::shared_ptr<Derived> getRecord() {
        return boost::static_pointer_cast<Derived>(_record);
    }

    MessageID getMessageType() {
       return static_cast<MessageID>(_messageHeader.messageType);
    }

    boost::shared_ptr< SharedBuffer > getBinary()
    {
        return _binary;
    }

    virtual bool validate();

    size_t getMessageSize() const
    {
        return _messageHeader.recordSize + _messageHeader.binarySize + sizeof(MessageHeader);
    }

    QueryID getQueryID() const
    {
        return _messageHeader.queryID;
    }

    void setQueryID(QueryID queryID)
    {
        _messageHeader.queryID = queryID;
    }

    void initRecord(MessageID messageType)
    {
       assert( _messageHeader.messageType == mtNone);
       _record = createRecord(messageType);
       _messageHeader.messageType = static_cast<uint16_t>(messageType);
    }

 protected:

    virtual MessagePtr createRecord(MessageID messageType)
    {
       return createRecordByType(static_cast<MessageID>(messageType));
    }

private:

    void init(MessageID messageType);
    MessageHeader _messageHeader;   /** < Message header */
    MessagePtr _record;             /** < Structured part of message */
    boost::shared_ptr< SharedBuffer > _binary;     /** < Buffer for binary data to be transfered */
    boost::asio::streambuf _recordStream; /** < Buffer for serializing Google Protocol Buffers objects */

    static MessagePtr createRecordByType(MessageID messageType);

    friend class BaseConnection;
    friend class Connection;
};


/**
 * Base class for connection to a network manager and send message to it.
 * Class uses sync mode and knows nothing about NetworkManager.
 */
class BaseConnection
{
protected:

        boost::asio::ip::tcp::socket _socket;
        /**
         * Set socket options such as TCP_KEEP_ALIVE
         */
        void configConnectedSocket();

public:
        BaseConnection(boost::asio::io_service& ioService);
        virtual ~BaseConnection();

        /// Connect to remote site
        void connect(std::string address, uint16_t port);

        virtual void disconnect();

        boost::asio::ip::tcp::socket& getSocket() {
            return _socket;
        }

        /**
         * Send message to peer and read message from it.
         * @param inMessageDesc a message descriptor for sending message.
         * @param template MessageDesc_tt must implement MessageDesc APIs
         * @return message descriptor of received message.
         * @throw System::Exception
         */
        template <class MessageDesc_tt>
        boost::shared_ptr<MessageDesc_tt> sendAndReadMessage(boost::shared_ptr<MessageDesc>& inMessageDesc);

         /**
         * Send message to peer
         * @param inMessageDesc a message descriptor for sending message.
         * @throw System::Exception
         */
        void send(boost::shared_ptr<MessageDesc>& messageDesc);

        /**
         * Receive message from peer
         * @param template MessageDesc_tt must implement MessageDesc APIs
         * @return message descriptor of received message
         * @throw System::Exception
         */
        template <class MessageDesc_tt>
        boost::shared_ptr<MessageDesc_tt> receive();

        static log4cxx::LoggerPtr logger;
};

template <class MessageDesc_tt>
boost::shared_ptr<MessageDesc_tt> BaseConnection::sendAndReadMessage(boost::shared_ptr<MessageDesc>& messageDesc)
{
    LOG4CXX_TRACE(BaseConnection::logger, "The sendAndReadMessage: begin");
    send(messageDesc);
    boost::shared_ptr<MessageDesc_tt> resultDesc = receive<MessageDesc_tt>();
    LOG4CXX_TRACE(BaseConnection::logger, "The sendAndReadMessage: end");
    return resultDesc;
}

template <class MessageDesc_tt>
boost::shared_ptr<MessageDesc_tt> BaseConnection::receive()
{
    LOG4CXX_TRACE(BaseConnection::logger, "BaseConnection::receive: begin");
    boost::shared_ptr<MessageDesc_tt> resultDesc(new MessageDesc_tt());
    try
    {
        // Reading message description
        size_t readBytes = read(_socket, boost::asio::buffer(&resultDesc->_messageHeader, sizeof(resultDesc->_messageHeader)));
        assert(readBytes == sizeof(resultDesc->_messageHeader));
        ASSERT_EXCEPTION((resultDesc->validate()), "BaseConnection::receive:");
        // TODO: This must not be an assert but exception of correct handled backward compatibility
        ASSERT_EXCEPTION((resultDesc->_messageHeader.netProtocolVersion == NET_PROTOCOL_CURRENT_VER), "BaseConnection::receive:");

        // Reading serialized structured part
        readBytes = read(_socket, resultDesc->_recordStream.prepare(resultDesc->_messageHeader.recordSize));
        assert(readBytes == resultDesc->_messageHeader.recordSize);
        LOG4CXX_TRACE(BaseConnection::logger, "BaseConnection::receive: recordSize=" << resultDesc->_messageHeader.recordSize);
        bool rc = resultDesc->parseRecord(readBytes);
        ASSERT_EXCEPTION(rc, "BaseConnection::receive:");

        resultDesc->prepareBinaryBuffer();

        if (resultDesc->_messageHeader.binarySize > 0)
        {
            readBytes = read(_socket, boost::asio::buffer(resultDesc->_binary->getData(), resultDesc->_binary->getSize()));
            assert(readBytes == resultDesc->_binary->getSize());
        }

        LOG4CXX_TRACE(BaseConnection::logger, "read message: messageType=" << resultDesc->_messageHeader.messageType <<
                      " ; binarySize=" << resultDesc->_messageHeader.binarySize);

        LOG4CXX_TRACE(BaseConnection::logger, "BaseConnection::receive: end");
    }
    catch (const boost::exception &e)
    {
        LOG4CXX_TRACE(BaseConnection::logger, "BaseConnection::receive: exception: "<< boost::diagnostic_information(e));
        throw SYSTEM_EXCEPTION(SCIDB_SE_NETWORK, SCIDB_LE_CANT_SEND_RECEIVE);
    }
    return resultDesc;
}

}

#endif /* SYNCCONNECTION_H_ */
