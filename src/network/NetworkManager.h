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
 * NetworkManager.h
 *      Author: roman.simakov@gmail.com
 *      Description: NetworkManager class provides high-level API
 *      for message exchanging and also register instance in system catalog.
 */

#ifndef NETWORKMANAGER_H_
#define NETWORKMANAGER_H_

#include <boost/asio.hpp>
#include <log4cxx/logger.h>

#include <array/Metadata.h>
#include <query/QueryProcessor.h>
#include <network/ThrottledScheduler.h>
#include <system/Cluster.h>
#include <util/Singleton.h>
#include <util/JobQueue.h>
#include <util/WorkQueue.h>
#include <util/Network.h>

namespace scidb
{
 class Connection;

/***
 * The network manager implementation depends on system catalog API
 * and storage manager API. It will register itself online in system catalog and read instance number from
 * storage manager. It's necessary since storage content will be described in system
 * catalog with instance number and its content must be related to instance number.
 * So the best place to store instance number is a storage manager. By reading
 * this value scidb instance can find itself in system catalog and register itself
 * as online. In other words instance number is a local data part number. Registering
 * instance in system catalog online means pointing where data part with given number is.
 *
 * @note As a naming convention, member functions by default interact with logical instances.
 *       A function that interacts with physical instances should have a name that ends with 'Physical'.
 *       E.g. send() takes as input a logical instanceID, and sendPhysical() takes as input a physical instanceID.
 * @note Operator code typically interacts with logical instances.
 *
 */
class NetworkManager: public Singleton<NetworkManager>
{
 public:
    /**
     * Queue types logically partition the send/receive
     * buffer(queue) space for the flow control purposes.
     * The back pressure is performed separately for each type.
     * The purpose for dividing the buffer space into
     * independent pools is to avoid resource starvation when running
     * multi-phase algorithms. For example, replication is the last
     * phase in a query involving a storing operator. The buffer space
     * used for replication must not be used by any other network
     * activity in the query to guarantee progress. Other multi-phase
     * algorithms can also use the same mechanism to divide resources
     * among the phases if desired. mqtNone does not trigger any flow
     * control mechanisms and is the default queue type.
     */
    typedef enum
    {
        mqtNone=0,
        mqtReplication,
        mqtMax //end marker
    } MessageQueueType;

    static const uint64_t MAX_QUEUE_SIZE = ~0;

 private:
    friend class Cluster;
    friend class Connection;

    // Service of boost asio
    boost::asio::io_service _ioService;

    // Acceptor of incoming connections
    boost::asio::ip::tcp::acceptor _acceptor;

    boost::asio::posix::stream_descriptor _input;
    char one_byte_buffer;

    // A timer to check connections for alive
    boost::asio::deadline_timer _aliveTimer;
    time_t _aliveTimeout;

    // InstanceID of this instance of manager
    InstanceID _selfInstanceID;

    /**
     *  A pool of connections to other instances.
     */
    typedef std::map<InstanceID, boost::shared_ptr<Connection> > ConnectionMap;
    ConnectionMap _outConnections;

    // Job queue for processing received messages
    boost::shared_ptr< JobQueue> _jobQueue;

    boost::shared_ptr<Instances> _instances;

    boost::shared_ptr<const InstanceLiveness> _instanceLiveness;

    Mutex _mutex;

    static volatile bool _shutdown;

    // If > 0 handler of reconnect is added
    std::set<InstanceID> _brokenInstances;

    // A timer to handle reconnects
    boost::shared_ptr<ThrottledScheduler> _reconnectScheduler;
    static const time_t DEFAULT_RECONNECT_TIMEOUT = 3; //sec

    // A timer to handle reconnects
    boost::shared_ptr<ThrottledScheduler> _livenessHandleScheduler;
    static const time_t DEFAULT_LIVENESS_HANDLE_TIMEOUT = 60; //sec
    static const time_t DEFAULT_ALIVE_TIMEOUT = 5; //sec

    uint64_t _repMessageCount;
    uint64_t _maxRepSendQSize;
    uint64_t _maxRepReceiveQSize;
    uint64_t _memUsage;

    class DefaultMessageDescription : virtual public ClientMessageDescription
    {
    public:
    DefaultMessageDescription( InstanceID instanceID,
                               MessageID msgID,
                               MessagePtr msgRec,
                               boost::shared_ptr<SharedBuffer> bin,
                               QueryID qId )
    : _instanceID(instanceID), _msgID(msgID), _msgRecord(msgRec), _binary(bin), _queryId(qId)
        {
            assert(msgRec.get());
        }
    DefaultMessageDescription( const ClientContext::Ptr& clientCtx,
                               MessageID msgID,
                               MessagePtr msgRec,
                               boost::shared_ptr<SharedBuffer> bin,
                               QueryID qId )
    : _instanceID(CLIENT_INSTANCE), _clientCtx(clientCtx), _msgID(msgID), _msgRecord(msgRec), _binary(bin), _queryId(qId)
        {
            assert(clientCtx);
            assert(msgRec.get());
        }
        virtual InstanceID getSourceInstanceID() const   {return _instanceID; }
        virtual MessagePtr getRecord()                   {return _msgRecord; }
        virtual MessageID getMessageType() const         {return _msgID; }
        virtual QueryID getQueryId() const             {return _queryId; }
        virtual ClientContext::Ptr getClientContext()    {return _clientCtx; }
        virtual boost::asio::const_buffer getBinary()
        {
            if (_binary) {
                return asio::const_buffer(_binary->getData(), _binary->getSize());
            }
            return asio::const_buffer(NULL, 0);
        }
        virtual ~DefaultMessageDescription() {}
    private:
        DefaultMessageDescription();
        DefaultMessageDescription(const DefaultMessageDescription&);
        DefaultMessageDescription& operator=(const DefaultMessageDescription&);
        InstanceID _instanceID;
        ClientContext::Ptr _clientCtx;
        MessageID _msgID;
        MessagePtr _msgRecord;
        boost::shared_ptr<SharedBuffer> _binary;
        QueryID _queryId;
    };

    class DefaultNetworkMessageFactory : public NetworkMessageFactory
    {
    public:
       DefaultNetworkMessageFactory() {}
       virtual ~DefaultNetworkMessageFactory() {}
       bool isRegistered(const MessageID& msgID);
       bool addMessageType(const MessageID& msgID,
                           const NetworkMessageFactory::MessageCreator& msgCreator,
                           const NetworkMessageFactory::MessageHandler& msgHandler);
       MessagePtr createMessage(const MessageID& msgID);
       NetworkMessageFactory::MessageHandler getMessageHandler(const MessageID& msgType);
    private:
       DefaultNetworkMessageFactory(const DefaultNetworkMessageFactory&);
       DefaultNetworkMessageFactory& operator=(const DefaultNetworkMessageFactory&);

       typedef std::map<MessageID,
                        std::pair<NetworkMessageFactory::MessageCreator,
                                  NetworkMessageFactory::MessageHandler> >  MessageHandlerMap;
       MessageHandlerMap _msgHandlers;
       Mutex _mutex;
    };

    boost::shared_ptr<DefaultNetworkMessageFactory>  _msgHandlerFactory;
    boost::shared_ptr<WorkQueue> _workQueue;
    boost::shared_ptr<WorkQueue> _requestQueue;

    void startAccept();
    void handleAccept(boost::shared_ptr<Connection> newConnection, const boost::system::error_code& error);

    void startInputWatcher();
    void handleInput(const boost::system::error_code& error, size_t bytes_transferr);

    void _handleReconnect();
    static void handleReconnect() {
        getInstance()->_handleReconnect();
    }

    void _handleAlive(const boost::system::error_code& error);
    static void handleAlive(const boost::system::error_code& error) {
        getInstance()->_handleAlive(error);
    }

    /**
     * internal use only
     */
    void handleControlMessage(const boost::shared_ptr<MessageDesc>& msgDesc);

    void _sendPhysical(InstanceID physicalInstanceID,
                      boost::shared_ptr<MessageDesc>& messageDesc,
                      MessageQueueType flowControlType = mqtNone);

    static void handleLivenessNotification(boost::shared_ptr<const InstanceLiveness> liveInfo) {
       getInstance()->_handleLivenessNotification(liveInfo);
    }
    void _handleLivenessNotification(boost::shared_ptr<const InstanceLiveness>& liveInfo);
    static void handleLiveness() {
       getInstance()->_handleLiveness();
    }
    void _handleLiveness();

    static void publishMessage(const boost::shared_ptr<MessageDescription>& msgDesc);

    /// Helper that finds a handler for a given message
    void dispatchMessageToListener(const shared_ptr<Connection>& connection,
                                   const boost::shared_ptr<MessageDesc>& messageDesc,
                                   NetworkMessageFactory::MessageHandler& handler);
    /// Helper that notifies a given query of a connection error
    void handleConnectionError(const QueryID& queryId);

    /**
     * Helper that does the necessary cleanup when a client disconnects
     *  @queryId the query ID of a query associated with a client connection
     * @dh the disconnect handler if any registered by the user
     */
    void handleClientDisconnect(const QueryID& queryId,
                                const ClientContext::DisconnectHandler& dh);

    /**
     * Handle the messages which are not generated by the SciDB engine proper
     * but registered with NetworkMessageFactory
     * @param messageDesc message description
     * @param handler gets assigned if the return value is true
     * @return true if the message is handled, false if it is a system message
     */
    bool handleNonSystemMessage(const boost::shared_ptr<MessageDesc>& messageDesc,
                                NetworkMessageFactory::MessageHandler& handler);

    /**
     * Request information about instances from system catalog.
     * @param force a flag to force usage system catalog. If false and _instances.size() > 0
     * function just return existing version.
     * @return a number of instances registered in the system catalog.
     */
    void getInstances(bool force);

    /**
     * Get currently known instance liveness
     * @return liveness or NULL if not yet known
     * @see scidb::Cluster::getInstanceLiveness()
     */
    boost::shared_ptr<const InstanceLiveness> getInstanceLiveness()
    {
       ScopedMutexLock mutexLock(_mutex);
       getInstances(false);
       return _instanceLiveness;
    }

    size_t getPhysicalInstances(std::vector<InstanceID>& instances);

    InstanceID getPhysicalInstanceID() {
       ScopedMutexLock mutexLock(_mutex);
       return _selfInstanceID;
    }

    void reconnect(InstanceID instanceID);
    void handleShutdown();
    uint64_t _getAvailable(MessageQueueType mqt);
    void _broadcastPhysical(shared_ptr<MessageDesc>& messageDesc);

public:
    NetworkManager();
    ~NetworkManager();

    uint64_t getUsedMemSize() const
    {
        // not synchronized, relying on 8byte atomic load
        return _memUsage;
    }

    /**
     * Request information about instances from system catalog.
     * @return an immutable array of currently registered instance descriptors
     */
    boost::shared_ptr<const Instances> getInstances()
    {
        ScopedMutexLock mutexLock(_mutex);
        getInstances(false);
        return _instances;
    }

    /**
     *  This method send asynchronous message to a physical instance.
     *  @param physicalInstanceID is a instance number for sending message to.
     *  @param MessageDesc contains Google Protocol Buffer message and octet data.
     *  @param mqt the queue to use for this message
     *  @package waitSent waits when until message is sent then return
     */
    void sendPhysical(InstanceID physicalInstanceID, boost::shared_ptr<MessageDesc>& messageDesc,
                     MessageQueueType mqt = mqtNone);

    /**
     *  This method sends out an asynchronous message to every logical instance except this instance (using per-query instance ID maps)
     *  @param MessageDesc contains Google Protocol Buffer message and octet data.
     *  @param waitSent waits when until message is sent then return
     */
    void broadcastLogical(boost::shared_ptr<MessageDesc>& messageDesc);

    /**
     *  This method sends out asynchronous message to every physical instance except this instance
     *  @param MessageDesc contains Google Protocol Buffer message and octet data.
     *
     */
    void broadcastPhysical(boost::shared_ptr<MessageDesc>& messageDesc);

    /// This method handle messages received by connections. Called by Connection class
    void handleMessage(boost::shared_ptr< Connection > connection, const boost::shared_ptr<MessageDesc>& messageDesc);

    /// This method block own thread
    void run(boost::shared_ptr<JobQueue> jobQueue);

    /**
     * @return a queue suitable for running tasks that can always make progress
     * i.e. dont *wait* for any conditions messages from remote instances or anything of that sort
     * @note the progress requirement is to ensure there is no deadlock
     * @note No new threads are created as a result of adding work to the queue
     */
    boost::shared_ptr<WorkQueue> getWorkQueue() {
        return _workQueue;
    }

    /**
     * Intended for internal use only.
     * @return a queue suitable for running tasks that can *wait*
     * for any conditions or messages from remote instances.
     * @note HOWEVER, these conditions must be satisfied by jobs/work items NOT executed on this queue (to avoid deadlock).
     * @note No new threads are created as a result of adding work to the queue
     */
    boost::shared_ptr<WorkQueue> getRequestQueue() {
        return _requestQueue;
    }

    /**
     * Intended for internal use only.
     * @return a queue suitable for running tasks that can always make progress
     * @note the progress requirement is to ensure there is no deadlock
     * @note No new threads are created as a result of adding work to the queue
     *       The items on this queue may not out-live the queue itself, i.e.
     *       once the queue is destroyed an unspecified number of elements may not get to run.
     */
    boost::shared_ptr<WorkQueue> createWorkQueue() {
        return boost::make_shared<WorkQueue>(_jobQueue);
    }

    /**
     * Intended for internal use only.
     * @return a queue suitable for running tasks that can always make progress
     * @note No new threads are created as a result of adding work to the queue
     *       The items on this queue will not out-live the queue itself, i.e.
     *       once the queue is destroyed an unspecified number of elements may not get to run.
     * @param maxOutstanding max number of concurrent work items (the actual number of threads may be fewer)
     * @param maxSize max queue length including maxOutstanding
     */
    boost::shared_ptr<WorkQueue> createWorkQueue(uint32_t maxOutstanding, uint32_t maxSize)
    {
        return boost::make_shared<WorkQueue>(_jobQueue, maxOutstanding, maxSize);
    }

    /// Register incoming message for control flow purposes against a given queue
    void registerMessage(const boost::shared_ptr<MessageDesc>& messageDesc, MessageQueueType mqt);
    /// Unregister incoming message for control flow purposes against a given queue when it is deallocated
    void unregisterMessage(const boost::shared_ptr<MessageDesc>& messageDesc, MessageQueueType mqt);

    /**
     * Get available receive buffer space for a given channel
     * This is the amount advertised to the sender to keep it from
     * overflowing the receiver's buffers
     * (currently the accounting is done in terms of the number of messages rather than bytes
     *  and no memory is pre-allocated for the messages)
     * @param mqt the channel ID
     * @return number of messages the receiver is willing to accept currently (per sender)
     */
    uint64_t getAvailable(MessageQueueType mqt);

    /// internal
    uint64_t getSendQueueLimit(MessageQueueType mqt)
    {
        // mqtRplication is the only supported type for now
        if (mqt == mqtReplication) {
            ScopedMutexLock mutexLock(_mutex);
            getInstances(false);
            assert(_instances->size()>0);
            return (_maxRepSendQSize / _instances->size());
        }
        assert(mqt==mqtNone);
        return MAX_QUEUE_SIZE;
    }

    uint64_t getReceiveQueueHint(MessageQueueType mqt)
    {
        // mqtRplication is the only supported type for now
        if (mqt == mqtReplication) {
            ScopedMutexLock mutexLock(_mutex);
            getInstances(false);
            assert(_instances->size()>0);
            return (_maxRepReceiveQSize / _instances->size());
        }
        assert(mqt==mqtNone);
        return MAX_QUEUE_SIZE;
    }

    boost::asio::io_service& getIOService()
    {
        return _ioService;
    }

    // Network Interface for operators
    void send(InstanceID logicalTargetID, boost::shared_ptr<MessageDesc>& msg);

    /**
     * Send a message to the local instance (from the local instance)
     * @throw NetworkManager::OverflowException currently not thrown, but is present for API completeness
     */
    void sendLocal(const boost::shared_ptr<Query>& query, boost::shared_ptr<MessageDesc>& messageDesc);


    // MPI-like functions
    void send(InstanceID logicalTargetID, boost::shared_ptr< SharedBuffer> const& data, boost::shared_ptr<Query> & query);

    boost::shared_ptr< SharedBuffer> receive(InstanceID source, boost::shared_ptr<Query> & query);

    static void shutdown() {
       _shutdown = true;
    }
    static bool isShutdown() {
       return _shutdown;
    }

    /**
     * Get a factory to register new network messages and
     * to retrieve already registered message handlers.
     * @return the message factory
     */
    boost::shared_ptr<NetworkMessageFactory> getNetworkMessageFactory()
    {
       boost::shared_ptr<NetworkMessageFactory> ptr(_msgHandlerFactory);
       return ptr;
    }

    class OverflowException: public SystemException
    {
    public:
    OverflowException(MessageQueueType mqt, const char* file, const char* function, int32_t line)
    : SystemException(file, function, line, "scidb", SCIDB_SE_NO_MEMORY, SCIDB_LE_NETWORK_QUEUE_FULL,
                      "SCIDB_E_NO_MEMORY", "SCIDB_E_NETWORK_QUEUE_FULL", uint64_t(0)), _mqt(mqt)
        {
        }
        ~OverflowException() throw () {}
        void raise() const { throw *this; }
        MessageQueueType getQueueType() { return _mqt;}
    private:
        MessageQueueType _mqt;
    };

    /**
     * A notification class that reports changes in a connection status
     * Receivers can subscribe to this notification via scidb::Notification<ConnectionStatus>
     */
    class ConnectionStatus
    {
    public:
    ConnectionStatus(InstanceID instanceId, MessageQueueType mqt, uint64_t queueSize)
    : _instanceId(instanceId), _queueType(mqt), _queueSize(queueSize)
        {
            assert(instanceId != INVALID_INSTANCE);
        }
        ~ConnectionStatus() {}
        InstanceID   getPhysicalInstanceId()    const { return _instanceId; }
        uint64_t getAvailabeQueueSize() const { return _queueSize; }
        MessageQueueType getQueueType() const { return _queueType;}
    private:
        ConnectionStatus(const ConnectionStatus&);
        ConnectionStatus& operator=(const ConnectionStatus&);
        InstanceID _instanceId;
        MessageQueueType _queueType;
        uint64_t _queueSize;
    };
};


} //namespace


#endif /* NETWORKMANAGER_H_ */
