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
 * @file Network.h
 * @brief API allowing for sending and receiving network messages
 */

#ifndef NETWORK_H_
#define NETWORK_H_

#include <boost/asio.hpp>
#include <boost/function.hpp>
#include <boost/shared_ptr.hpp>
#include <google/protobuf/message.h>
#include <util/Semaphore.h>
#include <util/WorkQueue.h>
#include <util/NetworkMessage.h>
#include <array/Metadata.h>
#include <array/Array.h>

namespace scidb
{
   /**
    * Network message descriptor
    */
   class MessageDescription
   {
   public:
      virtual InstanceID getSourceInstanceID() const = 0;
      virtual MessagePtr getRecord() = 0;
      virtual MessageID getMessageType() const = 0;
      virtual boost::asio::const_buffer getBinary() = 0;
      virtual ~MessageDescription() {}
      virtual QueryID getQueryId() const = 0;
   };

   /**
    * Abstract client context
    */
   class ClientContext
   {
   public:
       typedef boost::shared_ptr<ClientContext> Ptr;

       /// Client connection disconnect handler
       typedef boost::function<void(const boost::shared_ptr<Query>&)> DisconnectHandler;

       /**
        * Attach a query specific handler for client's disconnect
        * @param queryID query ID
        * @param dh disconnect handler
        */
       virtual void attachQuery(QueryID queryID, DisconnectHandler& dh) = 0;

       /**
        * Detach a query specific handler for client's disconnect
        * @param queryID query ID
        */
       virtual void detachQuery(QueryID queryID) = 0;

       /**
        * Indicate that the context should no longer be used
        */
       virtual void disconnect() = 0;

       virtual ~ClientContext() {}
   };

   /**
    * Client message descriptor
    */
   class ClientMessageDescription : public MessageDescription
   {
   public:
       virtual ClientContext::Ptr getClientContext() = 0;
       virtual ~ClientMessageDescription() {}
   };

   /**
    * Network message factory allows for addition of network message handlers
    * on SciDB server (only).
    */
   class NetworkMessageFactory
   {
   public:
      typedef boost::function< MessagePtr(MessageID) > MessageCreator;
      typedef boost::function< void(const boost::shared_ptr<MessageDescription>& ) > MessageHandler;

      virtual bool isRegistered(const MessageID& msgID) = 0;
      virtual bool addMessageType(const MessageID& msgID,
                                  const MessageCreator& msgCreator,
                                  const MessageHandler& msgHandler) = 0;
      virtual MessagePtr createMessage(const MessageID& msgID) = 0;
      virtual MessageHandler getMessageHandler(const MessageID& msgID) = 0;
      virtual ~NetworkMessageFactory() {}
   };

   /**
    * Network message factory access method
    * @return an instance of the factory
    */
   boost::shared_ptr<NetworkMessageFactory> getNetworkMessageFactory();

   /**
    * Return a reference to the io service object to schedule timers
    * @note XXX:
    * it might be better not to expose the io service directly but rather
    * provide a speicific functionality API (e.g. scheduleTimer())
    */
   boost::asio::io_service& getIOService();

   /**
    * @return a queue suitable for running tasks that are guaranteed to make progress
    * @note No new threads are created as a result of adding work to the queue
    */
   boost::shared_ptr<WorkQueue> getWorkQueue();

   /**
    * A scheduler  that runs *at most* every specified period.
    * It schedules itself to run in max(0, ((lastTime + period) - currentTime)) seconds.
    * It is not recurring - every execution needs to be explicitly scheduled.
    * When scheduled for the first time, it is run immediately.
    */
   class Scheduler
   {
   public:
      typedef boost::function<void()> Work;
      virtual void schedule() = 0;
      virtual ~Scheduler() {}
   };

   /**
    * Get a scheduler for a given time period
    *
    * @param workItem  to execute
    * @param period limiting the max freaquency of execution
    * @throw scidb::UserException if period < 1 or !workItem
    */
   boost::shared_ptr<Scheduler> getScheduler(Scheduler::Work& workItem, time_t period);

   /**
    * Send message to a physical instance asynchronously. The delivery is not guaranteed.
    * @param physicalInstanceID the instance ID of the destination
    * @param msgID a message ID previously registered with NetworkMessageFactory
    * @param record the structured part of the message (derived from ::google::protobuf::Message),
                    the record type must match the one generated by the NetworkMessageFactory
    * @param binary the opaque payload of the message (which can be empty)
    */
   void sendAsyncPhysical(InstanceID physicalInstanceID,
                  MessageID msgID,
                  MessagePtr record,
                  boost::asio::const_buffer& binary);

   /**
    * Send message to a client asynchronously, the delivery is not guaranteed
    * @param clientCtx the client context of the destination
    * @param msgID a message ID previously registered with NetworkMessageFactory
    * @param record the structured part of the message (derived from ::google::protobuf::Message),
                    the record type must match the one generated by the NetworkMessageFactory
    * @param binary the opaque payload of the message (which can be empty)
    */
   void sendAsyncClient(ClientContext::Ptr& clientCtx,
                  MessageID msgID,
                  MessagePtr record,
                  boost::asio::const_buffer& binary);
   /**
    * @return Time in seconds to wait before decalring a network-silent instance dead.
    */
   uint32_t getLivenessTimeout();

   /// Host name resolution handler
   typedef boost::function< void(const boost::system::error_code& ,
                                 boost::asio::ip::tcp::resolver::iterator)> ResolverFunc;
   /**
    * Perform DNS name resolution using the standard means provided by the OS.
    * @param address [in] host DNS name or IP address
    * @param service [in] service name like 'ftp' or numeric port number; "" value does not restrict the resolution to a particular service
    * @param cb [cb] callback functor that is supposed to consume the results of the name resolution;
    *                 it must NOT perform any blocking operations such network IO or long running computation;
    *                 cb is not invoked inside of this function.
    */
   void resolveAsync(const std::string& address, const std::string& service, ResolverFunc& cb);

   /**
    * Receive a block of raw data from another logical instance.
    * @param ctx             a pointer to shared_ptr<Query>.
    * @param logicalInstance the instance to receive data from.
    * @param data            a pre-allocated buffer.
    * @param size            the size of the buffer; must be equal to the size of the to-be-received data.
    */
   void Receive(void* ctx, InstanceID logicalInstance, void* data, size_t size);

   /**
    * Send a block of raw data to another logical instance.
    * @param ctx             a pointer to shared_ptr<Query>.
    * @param logicalInstance the instance to send data to.
    * @param data            the data to send.
    * @param size            the size of data to send.
    */
   void Send(void* ctx, InstanceID logicalInstance, void const* data, size_t size);

   /**
    * Send a SharedBuffer to another logical instance.
    * @param logicalInstance  the instance to send data to.
    * @param data             the data to send.
    * @param query            the query context.
    */
   void BufSend(InstanceID logicalInstance, boost::shared_ptr<SharedBuffer> const& data, boost::shared_ptr<Query>& query);

   /**
    * Receive a SharedBuffer from another logical instance.
    * @param logicalInstance  the instance to receive data from.
    * @param query            the query context.
    * @return received data in the form of shared_ptr<SharedBuffer>.
    */
   boost::shared_ptr<SharedBuffer> BufReceive(InstanceID logicalInstance, boost::shared_ptr<Query>& query);

   /**
    * Send a SharedBuffer to all other logical instances.
    * @param data  the data to send.
    * @param query the query context.
    */
   void BufBroadcast(boost::shared_ptr<SharedBuffer> const& data, boost::shared_ptr<Query>& query);
} // namespace scidb

#endif /* NETWORK_H_ */
