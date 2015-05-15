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
 * @file NetworkManager.cpp
 * @author roman.somakov@gmail.com
 *
 * @brief NetworkManager class implementation.
 */
#include <sys/types.h>
#include <signal.h>
#include <boost/bind.hpp>
#include <boost/format.hpp>
#include <boost/make_shared.hpp>
#include <boost/enable_shared_from_this.hpp>
#include <google/protobuf/message.h>
#include <google/protobuf/descriptor.h>

#include "network/NetworkManager.h"
#include "system/SystemCatalog.h"
#include "system/Exceptions.h"
#include "network/MessageHandleJob.h"
#include "network/ClientMessageHandleJob.h"
#include "network/MessageUtils.h"
#include "array/Metadata.h"
#include "system/Config.h"
#include "smgr/io/Storage.h"
#include "util/PluginManager.h"
#include "util/Notification.h"
#include "system/Constants.h"
#include <system/Utils.h>

using namespace std;
using namespace boost;

namespace scidb
{

// Logger for network subsystem. static to prevent visibility of variable outside of file
static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.services.network"));

/***
 * N e t w o r k M a n a g e r
 */

const time_t RECOVER_TIMEOUT = 2;

volatile bool NetworkManager::_shutdown=false;

NetworkManager::NetworkManager():
        _acceptor(_ioService, boost::asio::ip::tcp::endpoint(
                boost::asio::ip::tcp::v4(),
                Config::getInstance()->getOption<int>(CONFIG_PORT))),
        _input(_ioService),
        _aliveTimer(_ioService),
        _aliveTimeout(DEFAULT_ALIVE_TIMEOUT),
        _selfInstanceID(INVALID_INSTANCE),
        _instances(new Instances),
        _repMessageCount(0),
        _maxRepSendQSize(Config::getInstance()->getOption<int>(CONFIG_REPLICATION_SEND_QUEUE_SIZE)),
        _maxRepReceiveQSize(Config::getInstance()->getOption<int>(CONFIG_REPLICATION_RECEIVE_QUEUE_SIZE)),
        _memUsage(0),
        _msgHandlerFactory(new DefaultNetworkMessageFactory)
{
    // Note: that _acceptor is 'fully opened', i.e. bind()'d, listen()'d and polled as needed
    _acceptor.set_option(boost::asio::ip::tcp::acceptor::reuse_address(true));
   int64_t reconnTimeout = Config::getInstance()->getOption<int>(CONFIG_RECONNECT_TIMEOUT);
   Scheduler::Work func = bind(&NetworkManager::handleReconnect);
   _reconnectScheduler =
   shared_ptr<ThrottledScheduler>(new ThrottledScheduler(reconnTimeout,
                                                         func, _ioService));
   func = bind(&NetworkManager::handleLiveness);
   _livenessHandleScheduler =
   shared_ptr<ThrottledScheduler>(new ThrottledScheduler(DEFAULT_LIVENESS_HANDLE_TIMEOUT,
                                                         func, _ioService));
   LOG4CXX_DEBUG(logger, "Network manager is intialized");
}

NetworkManager::~NetworkManager()
{
    LOG4CXX_DEBUG(logger, "Network manager is shutting down");
    _ioService.stop();
}

void NetworkManager::run(shared_ptr<JobQueue> jobQueue)
{
    LOG4CXX_DEBUG(logger, "NetworkManager::run()");

    Config *cfg = Config::getInstance();
    assert(cfg);

    if (cfg->getOption<int>(CONFIG_PORT) == 0) {
        LOG4CXX_WARN(logger, "NetworkManager::run(): Starting to listen on an arbitrary port! (--port=0)");
    }
    boost::asio::ip::tcp::endpoint endPoint = _acceptor.local_endpoint();
    const string address = cfg->getOption<string>(CONFIG_INTERFACE);
    const unsigned short port = endPoint.port();

    const bool registerInstance = cfg->getOption<bool>(CONFIG_REGISTER);

    SystemCatalog* catalog = SystemCatalog::getInstance();
    const string& storageConfigPath = cfg->getOption<string>(CONFIG_STORAGE);

    StorageManager::getInstance().open(storageConfigPath,
                                       cfg->getOption<int>(CONFIG_SMGR_CACHE_SIZE)*MiB);
    _selfInstanceID = StorageManager::getInstance().getInstanceId();

    if (registerInstance) {
        if (_selfInstanceID != INVALID_INSTANCE) {
            throw USER_EXCEPTION(SCIDB_SE_STORAGE, SCIDB_LE_STORAGE_ALREADY_REGISTERED) << _selfInstanceID;
        }
        string storageConfigDir = scidb::getDir(storageConfigPath);
        if (!isFullyQualified(storageConfigDir)) {
            throw (USER_EXCEPTION(SCIDB_SE_STORAGE, SCIDB_LE_NON_FQ_PATH_ERROR) << storageConfigPath);
        }
        _selfInstanceID = catalog->addInstance(InstanceDesc(address, port, storageConfigDir));

        StorageManager::getInstance().setInstanceId(_selfInstanceID);
        LOG4CXX_DEBUG(logger, "Registered instance # " << _selfInstanceID);
        return;
    } else {
        if (_selfInstanceID == INVALID_INSTANCE) {
            throw USER_EXCEPTION(SCIDB_SE_STORAGE, SCIDB_LE_STORAGE_NOT_REGISTERED);
        }
        if (cfg->getOption<int>(CONFIG_REDUNDANCY) >= (int)SystemCatalog::getInstance()->getNumberOfInstances())
            throw USER_EXCEPTION(SCIDB_SE_CONFIG, SCIDB_LE_INVALID_REDUNDANCY);
        catalog->markInstanceOnline(_selfInstanceID, address, port);
    }
    _jobQueue = jobQueue;

    // make sure we have at least one thread in the client request queue
    const uint32_t nJobs = std::max(cfg->getOption<int>(CONFIG_EXECUTION_THREADS),2);
    const uint32_t nRequests = std::max(cfg->getOption<int>(CONFIG_REQUESTS),1);

    _requestQueue = make_shared<WorkQueue>(jobQueue, nJobs-1, nRequests);
    _workQueue = make_shared<WorkQueue>(jobQueue, nJobs-1);

    LOG4CXX_INFO(logger, "Network manager is started on " << address << ":" << port << " instance #" << _selfInstanceID);

    if (!cfg->getOption<bool>(CONFIG_NO_WATCHDOG)) {
       startInputWatcher();
    }

    InstanceLivenessNotification::PublishListener listener = bind(&handleLivenessNotification, _1);
    InstanceLivenessNotification::ListenerID lsnrID =
        InstanceLivenessNotification::addPublishListener(listener);

    startAccept();
    _aliveTimer.expires_from_now(posix_time::seconds(_aliveTimeout));
    _aliveTimer.async_wait(NetworkManager::handleAlive);

    LOG4CXX_DEBUG(logger, "Start connection accepting and async message exchanging");

    // main loop
    _ioService.run();

    try
    {
        SystemCatalog::getInstance()->markInstanceOffline(_selfInstanceID);
    }
    catch(const Exception &e)
    {
        LOG4CXX_ERROR(logger, "Marking instance offline failed:\n" << e.what());
    }
}

void NetworkManager::handleShutdown()
{
   LOG4CXX_INFO(logger, "SciDB is going down ...");
   ScopedMutexLock scope(_mutex);
   assert(_shutdown);

   _acceptor.close();
   _input.close();
   ConnectionMap().swap(_outConnections);
   getIOService().stop();
}

void NetworkManager::startInputWatcher()
{
   _input.assign(STDIN_FILENO);
   _input.async_read_some(boost::asio::buffer((void*)&one_byte_buffer,sizeof(one_byte_buffer)),
                          bind(&NetworkManager::handleInput, this,
                               boost::asio::placeholders::error,
                               boost::asio::placeholders::bytes_transferred));
}

void NetworkManager::handleInput(const boost::system::error_code& error, size_t bytes_transferr)
{
   _input.close();
   if (error == boost::system::errc::operation_canceled) {
      return;
   }
   if (!error) {
      LOG4CXX_INFO(logger, "Got std input event. Terminating myself.");
      // Send SIGTERM to ourselves
      // to initiate the normal shutdown process
      assert(one_byte_buffer == 1);
      kill(getpid(), SIGTERM);
   } else {
      LOG4CXX_INFO(logger, "Got std input error: "
                   << error.value() << " : " << error.message()
                   << ". Killing myself.");
      // let us die
      kill(getpid(), SIGKILL);
   }
}

void NetworkManager::startAccept()
{
   assert(_selfInstanceID != INVALID_INSTANCE);
   shared_ptr<Connection> newConnection(new Connection(*this, _selfInstanceID));
   _acceptor.async_accept(newConnection->getSocket(),
                          bind(&NetworkManager::handleAccept, this,
                               newConnection, boost::asio::placeholders::error));
}

void NetworkManager::handleAccept(shared_ptr<Connection> newConnection, const boost::system::error_code& error)
{
    if (error == boost::system::errc::operation_canceled) {
        return;
    }

    if (false) {
        // XXX TODO: we need to provide bookkeeping to limit the number of client connection
        LOG4CXX_DEBUG(logger, "Connection dropped: too many connections");
        return;
    }
    if (!error)
    {
        LOG4CXX_DEBUG(logger, "Waiting for the first message");
        newConnection->start();
        startAccept();
    }
    else
    {
        stringstream s;
        s << "Error # " << error.value() << " : " << error.message() << " when accepting connection";
        LOG4CXX_ERROR(logger, s.str());
        throw SYSTEM_EXCEPTION(SCIDB_SE_NETWORK, SCIDB_LE_CANT_ACCEPT_CONNECTION) << error.value() << error.message();
    }
}

void NetworkManager::handleMessage(shared_ptr< Connection > connection, const shared_ptr<MessageDesc>& messageDesc)
{
   if (_shutdown) {
      handleShutdown();
      return;
   }
   if (messageDesc->getMessageType() == mtAlive) {
       return;
   }
   try
   {
       if (messageDesc->getMessageType() == mtControl) {
           handleControlMessage(messageDesc);
           return;
       }
      NetworkMessageFactory::MessageHandler handler;

      if (!handleNonSystemMessage(messageDesc, handler)) {
         assert(!handler);

         if (messageDesc->getSourceInstanceID() == CLIENT_INSTANCE)
         {
             shared_ptr<ClientMessageHandleJob> job = make_shared<ClientMessageHandleJob>(connection, messageDesc);
             job->dispatch(_requestQueue,_workQueue);
         }
         else
         {
             shared_ptr<MessageHandleJob> job = make_shared<ServerMessageHandleJob>(messageDesc);
             job->dispatch(_requestQueue,_workQueue);
         }
         handler = bind(&NetworkManager::publishMessage, _1);
      }
      if (handler) {
          dispatchMessageToListener(connection, messageDesc, handler);
      }
   }
   catch (const Exception& e)
   {
      // It's possible to continue of message handling for other queries so we just logging error message.
      InstanceID instanceId = messageDesc->getSourceInstanceID();
      MessageType messageType = static_cast<MessageType>(messageDesc->getMessageType());
      QueryID queryId = messageDesc->getQueryID();

      LOG4CXX_ERROR(logger, "Exception in message handler: messageType = "<< messageType);
      LOG4CXX_ERROR(logger, "Exception in message handler: source instance ID = "
                    << string((instanceId == CLIENT_INSTANCE)
                              ? std::string("CLIENT")
                              : str(format("instance %lld") % instanceId)));
      LOG4CXX_ERROR(logger, "Exception in message handler: " << e.what());

      if (messageType != mtError
          && messageType != mtCancelQuery
          && messageType != mtAbort
          && queryId != 0
          && queryId != INVALID_QUERY_ID
          && instanceId != INVALID_INSTANCE
          && instanceId != _selfInstanceID
          && instanceId != CLIENT_INSTANCE
          && instanceId < _instances->size())
       {
          shared_ptr<MessageDesc> errorMessage = makeErrorMessageFromException(e, queryId);
          _sendPhysical(instanceId, errorMessage);
          LOG4CXX_DEBUG(logger, "Error returned to sender")
       }
   }
}

void NetworkManager::handleControlMessage(const shared_ptr<MessageDesc>& msgDesc)
{
    assert(msgDesc);
    shared_ptr<scidb_msg::Control> record = msgDesc->getRecord<scidb_msg::Control>();
    assert(record);

    InstanceID instanceId = msgDesc->getSourceInstanceID();
    if (instanceId == CLIENT_INSTANCE) {
        return;
    }
    //XXX TODO: change asserts to connection->close()
    if(!record->has_local_gen_id()) {
        assert(false);
        return;
    }
    if(!record->has_remote_gen_id()) {
        assert(false);
        return;
    }
    const google::protobuf::RepeatedPtrField<scidb_msg::Control_Channel>& entries = record->channels();
    for(  google::protobuf::RepeatedPtrField<scidb_msg::Control_Channel>::const_iterator iter = entries.begin();
          iter != entries.end(); ++iter) {

        const scidb_msg::Control_Channel& entry = (*iter);
        if(!entry.has_id()) {
            assert(false);
            return;
        }
        if(!entry.has_available()) {
            assert(false);
            return;
        }
        if(!entry.has_local_sn()) {
            assert(false);
            return;
        }
        if(!entry.has_remote_sn()) {
            assert(false);
            return;
        }
        MessageQueueType mqt = static_cast<MessageQueueType>(entry.id());
        if (mqt < mqtNone || mqt >= mqtMax) {
            assert(false);
            return;
        }
    }

    ScopedMutexLock mutexLock(_mutex);

    ConnectionMap::iterator iter = _outConnections.find(instanceId);
    if (iter == _outConnections.end()) {
        return;
    }
    shared_ptr<Connection>& connection = iter->second;
    if (!connection) {
        return;
    }
    uint64_t peerLocalGenId = record->local_gen_id();
    uint64_t peerRemoteGenId = record->remote_gen_id();
    for(google::protobuf::RepeatedPtrField<scidb_msg::Control_Channel>::const_iterator iter = entries.begin();
        iter != entries.end(); ++iter) {

        const scidb_msg::Control_Channel& entry = (*iter);
        const MessageQueueType mqt  = static_cast<MessageQueueType>(entry.id());
        const uint64_t available    = entry.available();
        const uint64_t peerRemoteSn = entry.remote_sn(); //my last SN seen by peer
        const uint64_t peerLocalSn  = entry.local_sn();  //last SN sent by peer to me

        LOG4CXX_TRACE(logger, "handleControlMessage: Available queue size=" << available
                      << ", instanceID="<<instanceId
                      << ", queue= "<<mqt
                      << ", peerRemoteGenId="<<peerRemoteGenId
                      << ", peerLocalGenId="<<peerLocalGenId
                      << ", peerRemoteSn="<<peerRemoteSn
                      << ", peerLocalSn="<<peerLocalSn);

        connection->setRemoteQueueState(mqt, available,
                                        peerRemoteGenId, peerLocalGenId,
                                        peerRemoteSn, peerLocalSn);
    }
}

uint64_t NetworkManager::getAvailable(MessageQueueType mqt)
{
    // mqtRplication is the only supported type for now
    if (mqt != mqtReplication) {
        assert(mqt==mqtNone);
        return MAX_QUEUE_SIZE;
    }
    ScopedMutexLock mutexLock(_mutex);
    return _getAvailable(mqt);
}

uint64_t NetworkManager::_getAvailable(MessageQueueType mqt)
{ // mutex must be locked
    getInstances(false);
    uint64_t softLimit = 3*_maxRepReceiveQSize/4;
    if (softLimit==0) {
        softLimit=1;
    }
    uint64_t available = 0;
    if (softLimit >_repMessageCount) {
        available = (softLimit -_repMessageCount) / _instances->size();
        if (available==0) {
            available=1;
        }
    }
    LOG4CXX_TRACE(logger, "Available queue size=" << available << " for queue "<<mqt);
    return available;
}

void NetworkManager::registerMessage(const shared_ptr<MessageDesc>& messageDesc,
                                     MessageQueueType mqt)
{
    ScopedMutexLock mutexLock(_mutex);

    _memUsage += messageDesc->getMessageSize();

    LOG4CXX_TRACE(logger, "NetworkManager::registerMessage _memUsage=" << _memUsage);

    // mqtRplication is the only supported type for now
    if (mqt != mqtReplication) {
        assert(mqt == mqtNone);
        return;
    }

    ++_repMessageCount;

    LOG4CXX_TRACE(logger, "Registered message " << _repMessageCount << " for queue "<<mqt);

    _aliveTimeout = 1;//sec
}

void NetworkManager::unregisterMessage(const shared_ptr<MessageDesc>& messageDesc,
                                       MessageQueueType mqt)
{
    ScopedMutexLock mutexLock(_mutex);

    assert(_memUsage>= messageDesc->getMessageSize());

    _memUsage -= messageDesc->getMessageSize();

    LOG4CXX_TRACE(logger, "NetworkManager::unregisterMessage _memUsage=" << _memUsage);

    // mqtRplication is the only supported type for now
    if (mqt != mqtReplication) {
        assert(mqt == mqtNone);
        return;
    }

    --_repMessageCount;
    LOG4CXX_TRACE(logger, "Unregistered message " << _repMessageCount+1 << " for queue "<<mqt);

    _aliveTimeout = 1;//sec
}

bool
NetworkManager::handleNonSystemMessage(const shared_ptr<MessageDesc>& messageDesc,
                                       NetworkMessageFactory::MessageHandler& handler)
{
   assert(messageDesc);
   MessageID msgID = messageDesc->getMessageType();
   if (msgID < mtSystemMax) {
      return false;
   }
   handler = _msgHandlerFactory->getMessageHandler(msgID);
   if (handler.empty()) {
      LOG4CXX_WARN(logger, "Registered message handler (MsgID="<< msgID <<") is empty!");
      return true;
   }
   return true;
}

void NetworkManager::publishMessage(const shared_ptr<MessageDescription>& msgDesc)
{
   shared_ptr<const MessageDescription> msg(msgDesc);
   Notification<MessageDescription> event(msg);
   event.publish();
}

void NetworkManager::dispatchMessageToListener(const shared_ptr<Connection>& connection,
                                               const shared_ptr<MessageDesc>& messageDesc,
                                               NetworkMessageFactory::MessageHandler& handler)
{
    // no locks must be held
    shared_ptr<MessageDescription> msgDesc;

    if (messageDesc->getSourceInstanceID() == CLIENT_INSTANCE) {
        msgDesc = shared_ptr<MessageDescription>(
            new DefaultMessageDescription(connection,
                                          messageDesc->getMessageType(),
                                          messageDesc->getRecord<Message>(),
                                          messageDesc->getBinary(),
                                          messageDesc->getQueryID()
                                          ));
    } else {
        msgDesc = shared_ptr<MessageDescription>(
            new DefaultMessageDescription(messageDesc->getSourceInstanceID(),
                                          messageDesc->getMessageType(),
                                          messageDesc->getRecord<Message>(),
                                          messageDesc->getBinary(),
                                          messageDesc->getQueryID()
                                          ));
    }
    // invoke in-line, the handler is not expected to block
    handler(msgDesc);
}

void
NetworkManager::_sendPhysical(InstanceID targetInstanceID,
                             shared_ptr<MessageDesc>& messageDesc,
                             MessageQueueType mqt)
{
    if (_shutdown) {
        handleShutdown();
        handleConnectionError(messageDesc->getQueryID());
        return;
    }
    ScopedMutexLock mutexLock(_mutex);

    assert(_selfInstanceID != INVALID_INSTANCE);
    assert(targetInstanceID != _selfInstanceID);
    assert(targetInstanceID < _instances->size());

    // Opening connection if it's not opened yet
    shared_ptr<Connection> connection = _outConnections[targetInstanceID];
    if (!connection)
    {
        getInstances(false);
        connection = shared_ptr<Connection>(new Connection(*this, _selfInstanceID, targetInstanceID));
        assert((*_instances)[targetInstanceID].getInstanceId() == targetInstanceID);
        _outConnections[targetInstanceID] = connection;
        connection->connectAsync((*_instances)[targetInstanceID].getHost(), (*_instances)[targetInstanceID].getPort());
    }
    // Sending message through connection
    connection->sendMessage(messageDesc, mqt);
}

void
NetworkManager::sendPhysical(InstanceID targetInstanceID,
                            shared_ptr<MessageDesc>& messageDesc,
                            MessageQueueType mqt)
{
    getInstances(false);
    _sendPhysical(targetInstanceID, messageDesc, mqt);
}

void NetworkManager::broadcastPhysical(shared_ptr<MessageDesc>& messageDesc)
{
   ScopedMutexLock mutexLock(_mutex);
   getInstances(false);
   _broadcastPhysical(messageDesc);
}

void NetworkManager::_broadcastPhysical(shared_ptr<MessageDesc>& messageDesc)
{
   for (Instances::const_iterator i = _instances->begin();
        i != _instances->end(); ++i) {
      InstanceID targetInstanceID = i->getInstanceId();
      if (targetInstanceID != _selfInstanceID) {
        _sendPhysical(targetInstanceID, messageDesc);
      }
   }
}

void NetworkManager::broadcastLogical(shared_ptr<MessageDesc>& messageDesc)
{
    if (!messageDesc->getQueryID()) {
        throw USER_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_MESSAGE_MISSED_QUERY_ID);
    }
   shared_ptr<Query> query = Query::getQueryByID(messageDesc->getQueryID());
   const size_t instancesCount = query->getInstancesCount();
   InstanceID myInstanceID   = query->getInstanceID();
   assert(instancesCount>0);
   {
      ScopedMutexLock mutexLock(_mutex);
      for (size_t targetInstanceID = 0; targetInstanceID < instancesCount; ++targetInstanceID)
      {
          if (targetInstanceID != myInstanceID) {
              send(targetInstanceID, messageDesc);
          }
      }
    }
}

void NetworkManager::getInstances(bool force)
{
    ScopedMutexLock mutexLock(_mutex);

    // The instance membership does not change in RQ
    // when it does we may need to change this logic
    if (force || _instances->size() == 0)
    {
        _instances->clear();
        SystemCatalog::getInstance()->getInstances((*_instances));
    }
}

size_t NetworkManager::getPhysicalInstances(std::vector<InstanceID>& instances)
{
   instances.clear();
   ScopedMutexLock mutexLock(_mutex);
   getInstances(false);
   instances.reserve(_instances->size());
   for (Instances::const_iterator i = _instances->begin();
        i != _instances->end(); ++i) {
      instances.push_back(i->getInstanceId());
   }
   return instances.size();
}

void
NetworkManager::sendLocal(const shared_ptr<Query>& query,
                          shared_ptr<MessageDesc>& messageDesc)
{
    const InstanceID physicalId = query->mapLogicalToPhysical(query->getInstanceID());
    messageDesc->setSourceInstanceID(physicalId);
    shared_ptr<MessageHandleJob> job = boost::make_shared<ServerMessageHandleJob>(messageDesc);
    shared_ptr<WorkQueue> rq = getRequestQueue();
    shared_ptr<WorkQueue> wq = getWorkQueue();
    try {
        job->dispatch(rq, wq);
    } catch (const WorkQueue::OverflowException& e) {
        LOG4CXX_ERROR(logger, "Overflow exception from the work queue: "<<e.what());
        assert(false);
        throw NetworkManager::OverflowException(NetworkManager::mqtNone, REL_FILE, __FUNCTION__, __LINE__);
    }
}

void
NetworkManager::send(InstanceID targetInstanceID,
                     shared_ptr<MessageDesc>& msg)
{
   assert(msg);
   if (!msg->getQueryID()) {
       throw USER_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_MESSAGE_MISSED_QUERY_ID);
   }
   shared_ptr<Query> query = Query::getQueryByID(msg->getQueryID());
   InstanceID target = query->mapLogicalToPhysical(targetInstanceID);
   sendPhysical(target, msg);
}

void NetworkManager::send(InstanceID targetInstanceID, shared_ptr<SharedBuffer> const& data, shared_ptr< Query> & query)
{
    shared_ptr<MessageDesc> msg = make_shared<MessageDesc>(mtBufferSend, data);
    msg->setQueryID(query->getQueryID());
    InstanceID target = query->mapLogicalToPhysical(targetInstanceID);
    sendPhysical(target, msg);
}

shared_ptr<SharedBuffer> NetworkManager::receive(InstanceID sourceInstanceID, shared_ptr< Query> & query)
{
    Semaphore::ErrorChecker ec = bind(&Query::validate, query);
    query->_receiveSemaphores[sourceInstanceID].enter(ec);
    ScopedMutexLock mutexLock(query->_receiveMutex);
    if (query->_receiveMessages[sourceInstanceID].empty()) {
        throw SYSTEM_EXCEPTION(SCIDB_SE_NETWORK, SCIDB_LE_INSTANCE_OFFLINE) << sourceInstanceID;
    }
    shared_ptr<SharedBuffer> res = query->_receiveMessages[sourceInstanceID].front()->getBinary();
    query->_receiveMessages[sourceInstanceID].pop_front();

    return res;
}

void NetworkManager::_handleReconnect()
{
   set<InstanceID> brokenInstances;
   ScopedMutexLock mutexLock(_mutex);
   if (_shutdown) {
      handleShutdown();
      return;
   }
   if(_brokenInstances.size() <= 0 ) {
      return;
   }

   getInstances(false);

   brokenInstances.swap(_brokenInstances);

   for (set<InstanceID>::const_iterator iter = brokenInstances.begin();
        iter != brokenInstances.end(); ++iter) {
      const InstanceID& i = *iter;
      assert(i < _instances->size());
      assert((*_instances)[i].getInstanceId() == i);
      ConnectionMap::iterator connIter = _outConnections.find(i);

      if (connIter == _outConnections.end()) {
         continue;
      }
      shared_ptr<Connection>& connection = (*connIter).second;

      if (!connection) {
         _outConnections.erase(connIter);
         continue;
      }
      connection->connectAsync((*_instances)[i].getHost(), (*_instances)[i].getPort());
   }
}

void NetworkManager::_handleLivenessNotification(shared_ptr<const InstanceLiveness>& liveInfo)
{
    if (logger->isDebugEnabled()) {
        ViewID viewId = liveInfo->getViewId();
        const InstanceLiveness::LiveInstances& liveInstances = liveInfo->getLiveInstances();
        const InstanceLiveness::DeadInstances& deadInstances = liveInfo->getDeadInstances();
        uint64_t ver = liveInfo->getVersion();

        LOG4CXX_DEBUG(logger, "New liveness information, viewID=" << viewId<<", ver="<<ver);
        for ( InstanceLiveness::DeadInstances::const_iterator i = deadInstances.begin();
             i != deadInstances.end(); ++i) {
           LOG4CXX_DEBUG(logger, "Dead instanceID=" << (*i)->getInstanceId());
           LOG4CXX_DEBUG(logger, "Dead genID=" << (*i)->getGenerationId());
        }
        for ( InstanceLiveness::LiveInstances::const_iterator i = liveInstances.begin();
             i != liveInstances.end(); ++i) {
           LOG4CXX_DEBUG(logger, "Live instanceID=" << (*i)->getInstanceId());
           LOG4CXX_DEBUG(logger, "Live genID=" << (*i)->getGenerationId());
        }
    }
    ScopedMutexLock mutexLock(_mutex);
    if (_shutdown) {
       handleShutdown();
       return;
    }
    if (_instanceLiveness &&
        _instanceLiveness->getVersion() == liveInfo->getVersion()) {
       assert(_instanceLiveness->isEqual(*liveInfo));
       return;
    }

    assert(!_instanceLiveness ||
           _instanceLiveness->getVersion() < liveInfo->getVersion());
    _instanceLiveness = liveInfo;

    _livenessHandleScheduler->schedule();
}

void NetworkManager::_handleLiveness()
{
   ScopedMutexLock mutexLock(_mutex);
   assert(_instanceLiveness);
   assert(_instanceLiveness->getNumInstances() == _instances->size());
   const InstanceLiveness::DeadInstances& deadInstances = _instanceLiveness->getDeadInstances();

   for (InstanceLiveness::DeadInstances::const_iterator iter = deadInstances.begin();
        iter != deadInstances.end(); ++iter) {
      InstanceID instanceID = (*iter)->getInstanceId();
      ConnectionMap::iterator connIter = _outConnections.find(instanceID);

      if (connIter == _outConnections.end()) {
         continue;
      }

      shared_ptr<Connection>& connection = (*connIter).second;
      if (connection) {
         connection->disconnect();
         connection.reset();
      }
      _outConnections.erase(connIter);
   }
   if (deadInstances.size() > 0) {
      _livenessHandleScheduler->schedule();
   }
}

void NetworkManager::_handleAlive(const boost::system::error_code& error)
{
    if (error == boost::asio::error::operation_aborted) {
       return;
    }

    shared_ptr<MessageDesc> messageDesc = make_shared<MessageDesc>(mtAlive);

    ScopedMutexLock mutexLock(_mutex);
    if (_shutdown) {
       handleShutdown();
       return;
    }

    _broadcastPhysical(messageDesc);

    _aliveTimer.expires_from_now(posix_time::seconds(_aliveTimeout));
    _aliveTimer.async_wait(NetworkManager::handleAlive);
    _aliveTimeout = DEFAULT_ALIVE_TIMEOUT;
}

void NetworkManager::reconnect(InstanceID instanceID)
{
   {
      ScopedMutexLock mutexLock(_mutex);
      _brokenInstances.insert(instanceID);
      if (_brokenInstances.size() > 1) {
         return;
      }
      _reconnectScheduler->schedule();
   }
}

void NetworkManager::handleClientDisconnect(const QueryID& queryId,
                                            const ClientContext::DisconnectHandler& dh)
{
   if (!queryId) {
      return;
   }

   LOG4CXX_WARN(logger, str(format("Client for query %lld disconnected") % queryId));
   shared_ptr<Query> query = Query::getQueryByID(queryId, false);

   if (!query) {
       return;
   }
   if (!dh) {
       assert(query->isCoordinator());
       shared_ptr<scidb::WorkQueue> errorQ = query->getErrorQueue();

       if (!errorQ) {
           LOG4CXX_TRACE(logger, "Query " << query->getQueryID()
                         << " no longer has the queue for error reporting,"
                         " it must be no longer active");
           return;
       }
       WorkQueue::WorkItem item = boost::bind(&Query::handleCancel, query);
       boost::function<void()> work = boost::bind(&WorkQueue::enqueue, errorQ, item);
       item.clear();
       // XXX TODO: handleCancel() sends messages, and stalling the network thread can theoretically
       // cause a deadlock when throttle control is enabled. So, when it is enabled,
       // we can handle the throttle-control exceptions in handleCancel() to avoid the dealock
       // (independently of this code).
       Query::runRestartableWork<void, WorkQueue::OverflowException>(work);

   } else {
       WorkQueue::WorkItem item = boost::bind(dh, query);
       try {
           _workQueue->enqueue(item);
       } catch (const WorkQueue::OverflowException& e) {
           LOG4CXX_ERROR(logger, "Overflow exception from the work queue: "<<e.what());
           assert(false);
           query->handleError(e.copy());
       }
   }
}

void NetworkManager::handleConnectionError(const QueryID& queryID)
{
   if (!queryID) {
      return;
   }
   LOG4CXX_ERROR(logger, "NetworkManager::handleConnectionError: "
                         "Conection error in query " << queryID);

   shared_ptr<Query> query = Query::getQueryByID(queryID, false);

   if (!query) {
      return;
   }
   query->handleError(SYSTEM_EXCEPTION_SPTR(SCIDB_SE_NETWORK, SCIDB_LE_CONNECTION_ERROR2));
}

void Send(void* ctx, InstanceID instance, void const* data, size_t size)
{
    NetworkManager::getInstance()->send(instance, shared_ptr< SharedBuffer>(new MemoryBuffer(data, size)), *(shared_ptr<Query>*)ctx);
}


void Receive(void* ctx, InstanceID instance, void* data, size_t size)
{
    shared_ptr< SharedBuffer> buf =  NetworkManager::getInstance()->receive(instance, *(shared_ptr<Query>*)ctx);
    assert(buf->getSize() == size);
    memcpy(data, buf->getData(), buf->getSize());
}

void BufSend(InstanceID target, shared_ptr<SharedBuffer> const& data, shared_ptr<Query>& query)
{
    NetworkManager::getInstance()->send(target, data, query);
}

shared_ptr<SharedBuffer> BufReceive(InstanceID source, shared_ptr<Query>& query)
{
    return NetworkManager::getInstance()->receive(source,query);
}

void BufBroadcast(shared_ptr<SharedBuffer> const& data, shared_ptr<Query>& query)
{
    shared_ptr<MessageDesc> msg = make_shared<MessageDesc>(mtBufferSend, data);
    msg->setQueryID(query->getQueryID());
    NetworkManager::getInstance()->broadcastLogical(msg);
}

bool
NetworkManager::DefaultNetworkMessageFactory::isRegistered(const MessageID& msgID)
{
   ScopedMutexLock mutexLock(_mutex);
   return (_msgHandlers.find(msgID) != _msgHandlers.end());
}

bool
NetworkManager::DefaultNetworkMessageFactory::addMessageType(const MessageID& msgID,
                                                             const MessageCreator& msgCreator,
                                                             const MessageHandler& msgHandler)
{
   if (msgID < mtSystemMax) {
      return false;
   }
   ScopedMutexLock mutexLock(_mutex);
   return  _msgHandlers.insert(
              std::make_pair(msgID,
                 std::make_pair(msgCreator, msgHandler))).second;
}

MessagePtr
NetworkManager::DefaultNetworkMessageFactory::createMessage(const MessageID& msgID)
{
   MessagePtr msgPtr;
   NetworkMessageFactory::MessageCreator creator;
   {
      ScopedMutexLock mutexLock(_mutex);
      MessageHandlerMap::const_iterator iter = _msgHandlers.find(msgID);
      if (iter != _msgHandlers.end()) {
         creator = iter->second.first;
      }
   }
   if (!creator.empty()) {
      msgPtr = creator(msgID);
   }
   return msgPtr;
}

NetworkMessageFactory::MessageHandler
NetworkManager::DefaultNetworkMessageFactory::getMessageHandler(const MessageID& msgType)
{
   ScopedMutexLock mutexLock(_mutex);

   MessageHandlerMap::const_iterator iter = _msgHandlers.find(msgType);
   if (iter != _msgHandlers.end()) {
      NetworkMessageFactory::MessageHandler handler = iter->second.second;
      return handler;
   }
   NetworkMessageFactory::MessageHandler emptyHandler;
   return emptyHandler;
}

/**
 * @see Network.h
 */
shared_ptr<NetworkMessageFactory> getNetworkMessageFactory()
{
   return NetworkManager::getInstance()->getNetworkMessageFactory();
}

/**
 * @see Network.h
 */
boost::asio::io_service& getIOService()
{
   return NetworkManager::getInstance()->getIOService();
}

shared_ptr<MessageDesc> prepareMessage(MessageID msgID,
                                       MessagePtr record,
                                       boost::asio::const_buffer& binary)
{
   shared_ptr<SharedBuffer> payload;
   if (boost::asio::buffer_size(binary) > 0) {
      assert(boost::asio::buffer_cast<const void*>(binary));
      payload = shared_ptr<SharedBuffer>(new MemoryBuffer(boost::asio::buffer_cast<const void*>(binary),
                                                          boost::asio::buffer_size(binary)));
   }
   shared_ptr<MessageDesc> msgDesc =
      shared_ptr<Connection::ServerMessageDesc>(new Connection::ServerMessageDesc(payload));

   msgDesc->initRecord(msgID);
   MessagePtr msgRecord = msgDesc->getRecord<Message>();
   const google::protobuf::Descriptor* d1 = msgRecord->GetDescriptor();
   assert(d1);
   const google::protobuf::Descriptor* d2 = record->GetDescriptor();
   assert(d2);
   if (d1->full_name().compare(d2->full_name()) != 0) {
      throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_UNKNOWN_MESSAGE_TYPE);
   }
   msgRecord->CopyFrom(*record.get());

   return msgDesc;
}

/**
 * @see Network.h
 */
void sendAsyncPhysical(InstanceID targetInstanceID,
               MessageID msgID,
               MessagePtr record,
               boost::asio::const_buffer& binary)
{
   shared_ptr<MessageDesc> msgDesc = prepareMessage(msgID,record,binary);
   assert(msgDesc);
   NetworkManager::getInstance()->sendPhysical(targetInstanceID, msgDesc);
}

/**
 * @see Network.h
 */
void sendAsyncClient(ClientContext::Ptr& clientCtx,
               MessageID msgID,
               MessagePtr record,
               boost::asio::const_buffer& binary)
{
    Connection* conn = dynamic_cast<Connection*>(clientCtx.get());
    if (conn == NULL) {
        throw (SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_UNKNOWN_CTX)
               << typeid(*clientCtx).name());
    }
    shared_ptr<MessageDesc> msgDesc = prepareMessage(msgID,record,binary);
    assert(msgDesc);
    conn->sendMessage(msgDesc);
}

boost::shared_ptr<WorkQueue> getWorkQueue()
{
    return NetworkManager::getInstance()->getWorkQueue();
}

uint32_t getLivenessTimeout()
{
   return Config::getInstance()->getOption<int>(CONFIG_LIVENESS_TIMEOUT);
}

shared_ptr<Scheduler> getScheduler(Scheduler::Work& workItem, time_t period)
{
   if (!workItem) {
      throw USER_EXCEPTION(SCIDB_SE_NETWORK, SCIDB_LE_INVALID_SHEDULER_WORK_ITEM);
   }
   if (period < 1) {
      throw USER_EXCEPTION(SCIDB_SE_NETWORK, SCIDB_LE_INVALID_SHEDULER_PERIOD);
   }
   shared_ptr<scidb::Scheduler> scheduler(new ThrottledScheduler(period, workItem,
                                          NetworkManager::getInstance()->getIOService()));
   return scheduler;
}

void resolveComplete(shared_ptr<asio::ip::tcp::resolver>& resolver,
                     shared_ptr<asio::ip::tcp::resolver::query>& query,
                     ResolverFunc& cb,
                     const system::error_code& error,
                     asio::ip::tcp::resolver::iterator endpoint_iterator)
{
    try {
        cb(error, endpoint_iterator);
    } catch (const scidb::Exception& e) {
        LOG4CXX_ERROR(logger, "Name resolution callback failed with: "<<e.what());
        assert(false);
    }
}

void resolveAsync(const string& address, const string& service, ResolverFunc& cb)
{
    shared_ptr<asio::ip::tcp::resolver> resolver(new asio::ip::tcp::resolver(NetworkManager::getInstance()->getIOService()));
    shared_ptr<asio::ip::tcp::resolver::query> query =
       make_shared<asio::ip::tcp::resolver::query>(address, service);
    resolver->async_resolve(*query,
                            boost::bind(&scidb::resolveComplete,
                                        resolver, query, cb,
                                        boost::asio::placeholders::error,
                                        boost::asio::placeholders::iterator));
}

} // namespace scidb
