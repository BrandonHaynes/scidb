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
 * MessageHandleJob.cpp
 *
 *  Created on: Jan 12, 2010
 *      Author: roman.simakov@gmail.com
 */

#include <log4cxx/logger.h>
#include <boost/make_shared.hpp>

#include <array/DBArray.h>
#include <network/MessageUtils.h>
#include <network/NetworkManager.h>
#include <network/MessageHandleJob.h>
#include <query/Operator.h>
#include <query/Query.h>
#include <query/QueryProcessor.h>
#include <smgr/io/Storage.h>
#include <system/Cluster.h>
#include <system/Exceptions.h>
#include <system/Resources.h>
#include <util/RWLock.h>
#include <util/Thread.h>
#include <query/PullSGContext.h>

using namespace std;
using namespace boost;

namespace scidb
{

static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.services.network"));

void MessageHandleJob::reschedule(uint64_t delayMicroSec)
{
    assert(delayMicroSec>0);
    shared_ptr<WorkQueue> toQ(_wq.lock());
    assert(toQ);
    shared_ptr<SerializationCtx> sCtx(_wqSCtx.lock());
    assert(sCtx);
    shared_ptr<Job> thisJob(shared_from_this());

    // try again on the same queue after a delay
    toQ->reserve(toQ);
    try {
        if (!_timer) {
            _timer = shared_ptr<asio::deadline_timer>(new asio::deadline_timer(getIOService()));
        }
        int rc = _timer->expires_from_now(posix_time::microseconds(delayMicroSec));
        if (rc != 0) {
            throw (SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_SYSCALL_ERROR)
                   << "boost::asio::expires_from_now" << rc << rc << delayMicroSec);
        }
        typedef function<void(const system::error_code& error)> TimerCallback;
        TimerCallback func = boost::bind(&handleRescheduleTimeout,
                                         thisJob,
                                         toQ,
                                         sCtx,
                                         _timer,
                                         asio::placeholders::error);
        _timer->async_wait(func);
    } catch (const scidb::Exception& e) {
        toQ->unreserve();
        throw;
    }
}

void
MessageHandleJob::handleRescheduleTimeout(shared_ptr<Job>& job,
                                          shared_ptr<WorkQueue>& toQueue,
                                          shared_ptr<SerializationCtx>& sCtx,
                                          shared_ptr<asio::deadline_timer>& timer,
                                          const boost::system::error_code& error)
{
    static const char *funcName="ClientMessageHandleJob::handleRescheduleTimeout: ";
    if (error == boost::asio::error::operation_aborted) {
        LOG4CXX_ERROR(logger, funcName
                      <<"Lock timer cancelled: "
                      <<" queue=" << toQueue.get()
                      <<", job="<<job.get()
                      <<", queryID="<<job->getQuery()->getQueryID());
        assert(false);
    } else if (error) {
        LOG4CXX_ERROR(logger, funcName
                      <<"Lock timer encountered error: "<<error
                      <<" queue=" << toQueue.get()
                      <<", job="<<job.get()
                      <<", queryID="<<job->getQuery()->getQueryID());
        assert(false);
    }
    // we will try to schedule anyway
    WorkQueue::scheduleReserved(job, toQueue, sCtx);
}

void MessageHandleJob::validateRemoteChunkInfo(const Array* array,
                                               const MessageID msgId,
                                               const uint32_t objType,
                                               const AttributeID attId,
                                               const InstanceID physicalSourceId)
{
    if (!array) {
        // the query must be deallocated, validate() should fail
        _query->validate();
        stringstream ss;
        ss << "Unable to find remote array for remote message:"
           << " messageID="<<msgId
           << " array type="<<objType
           << " attributeID="<<attId
           << " from "
           << string((physicalSourceId == CLIENT_INSTANCE)
                     ? std::string("CLIENT")
                     : str(format("instanceID=%lld") % physicalSourceId))
           <<" for queryID="<<_query->getQueryID();
        ASSERT_EXCEPTION(false, ss.str());
    }
    if (attId >= array->getArrayDesc().getAttributes().size()) {
        stringstream ss;
        ss << "Malformed remote message: "
           << " messageID="<<msgId
           << " invalid attributeID="<<attId
           << " array type="<<objType
           << " from "
           << string((physicalSourceId == CLIENT_INSTANCE)
                     ? std::string("CLIENT")
                     : str(format("instanceID=%lld") % physicalSourceId))
           <<" for queryID="<<_query->getQueryID();
        ASSERT_EXCEPTION(false, ss.str());
    }
}

ServerMessageHandleJob::ServerMessageHandleJob(const boost::shared_ptr<MessageDesc>& messageDesc)
: MessageHandleJob(messageDesc),
  networkManager(*NetworkManager::getInstance()),
  _logicalSourceId(INVALID_INSTANCE),
  _mustValidateQuery(true)
{
    assert(_messageDesc->getSourceInstanceID() != CLIENT_INSTANCE);

    const QueryID queryID = _messageDesc->getQueryID();

    LOG4CXX_TRACE(logger, "Creating a new job for message"
                  << " of type=" << _messageDesc->getMessageType()
                  << " from instance=" << _messageDesc->getSourceInstanceID()
                  << " with message size=" << _messageDesc->getMessageSize()
                  << " for queryID=" << queryID);

    if (queryID != 0) {
        if (_messageDesc->getMessageType() == mtPreparePhysicalPlan) {
            _query = Query::create(queryID,_messageDesc->getSourceInstanceID());
        } else {
            _query = Query::getQueryByID(queryID);
        }
    } else {
        LOG4CXX_TRACE(logger, "Creating fake query: type=" << _messageDesc->getMessageType()
                      << ", for message from instance=" << _messageDesc->getSourceInstanceID());
       // create a fake query for the recovery mode
       boost::shared_ptr<const scidb::InstanceLiveness> myLiveness =
       Cluster::getInstance()->getInstanceLiveness();
       assert(myLiveness);
       _query = Query::createFakeQuery(INVALID_INSTANCE,
                                       Cluster::getInstance()->getLocalInstanceId(),
                                       myLiveness);
    }
    assert(_query);
    if (_messageDesc->getMessageType() == mtChunkReplica) {
        networkManager.registerMessage(_messageDesc, NetworkManager::mqtReplication);
    } else {
        networkManager.registerMessage(_messageDesc, NetworkManager::mqtNone);
    }
}

ServerMessageHandleJob::~ServerMessageHandleJob()
{
    boost::shared_ptr<MessageDesc> msgDesc(_messageDesc);
    _messageDesc.reset();
    assert(msgDesc);

    if (msgDesc->getMessageType() == mtChunkReplica) {
        networkManager.unregisterMessage(msgDesc, NetworkManager::mqtReplication);
    } else {
        networkManager.unregisterMessage(msgDesc, NetworkManager::mqtNone);
    }
    LOG4CXX_TRACE(logger, "Destroying a job for message of"
                  << " type=" << msgDesc->getMessageType()
                  << " from instance=" << msgDesc->getSourceInstanceID()
                  << " with message size=" << msgDesc->getMessageSize()
                  << " for queryID=" << msgDesc->getQueryID());
}

void ServerMessageHandleJob::dispatch(boost::shared_ptr<WorkQueue>& requestQueue,
                                boost::shared_ptr<WorkQueue>& workQueue)
{
    assert(workQueue);
    assert(requestQueue);

    const MessageType messageType = static_cast<MessageType>(_messageDesc->getMessageType());

    if(messageType >= mtSystemMax || messageType <= mtNone) {
        assert(false);
        throw (SYSTEM_EXCEPTION(SCIDB_SE_NETWORK, SCIDB_LE_UNKNOWN_MESSAGE_TYPE)
               << messageType);
    }

    const QueryID queryID = _messageDesc->getQueryID();
    const InstanceID physicalSourceId = _messageDesc->getSourceInstanceID();

    LOG4CXX_TRACE(logger, "Dispatching message of type=" << messageType
                  << ", for queryID=" << queryID
                  << ", from instanceID=" << physicalSourceId);

    // Set the initial message handler
    _currHandler = boost::bind(_msgHandlers[messageType], this);

    switch (messageType)
    {
    case mtChunkReplica:
    {
        _logicalSourceId = _query->mapPhysicalToLogical(physicalSourceId);
        boost::shared_ptr<scidb_msg::Chunk> chunkRecord = _messageDesc->getRecord<scidb_msg::Chunk>();
        ArrayID arrId = chunkRecord->array_id();
        LOG4CXX_TRACE(logger, "ServerMessageHandleJob::dispatch: mtReplicaChunk sourceId="<<_logicalSourceId
                      << ", arrId="<<arrId
                      << ", queryID="<<_query->getQueryID());
        if (arrId <= 0 || _logicalSourceId == _query->getInstanceID()) {
            assert(false);
            stringstream ss;
            ss << "Invalid ArrayID=0 from InstanceID="<<physicalSourceId<<" for QueryID="<<queryID;
            throw (SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_UNKNOWN_ERROR)
                   << ss.str());
        }
        shared_ptr<ReplicationContext> replicationCtx = _query->getReplicationContext();

        // in debug only because ReplicationContext is single-threaded
        assert(replicationCtx->_chunkReplicasReqs[_logicalSourceId].increment() > 0);

        if (logger->isTraceEnabled()) {
            const uint64_t available = networkManager.getAvailable(NetworkManager::mqtReplication);
            LOG4CXX_TRACE(logger, "ServerMessageHandleJob::dispatch: Replication queue available="<<available);
        }
        shared_ptr<Job> thisJob(shared_from_this());
        replicationCtx->enqueueInbound(arrId, thisJob);
        return;
    }
    break;
    case mtChunk:
    case mtAggregateChunk:
    {
        _logicalSourceId = _query->mapPhysicalToLogical(physicalSourceId);
        // in debug only because getOperatorQueue() returns a single-threaded queue
        assert(_query->chunkReqs[_logicalSourceId].increment() > 0);
    }
    // fall through
    case mtSyncRequest:
    {
        boost::shared_ptr<WorkQueue> q = _query->getOperatorQueue();
        assert(q);
        if (logger->isTraceEnabled()) {
            LOG4CXX_TRACE(logger, "ServerMessageHandleJob::dispatch: Operator queue size="<<q->size()
                          << " for query ("<<queryID<<")");
        }
        enqueue(q);
        return;
    }
    break;
    case mtBufferSend:
    {
        boost::shared_ptr<WorkQueue> q = _query->getBufferReceiveQueue();
        assert(q);
        if (logger->isTraceEnabled()) {
            LOG4CXX_TRACE(logger, "ServerMessageHandleJob::dispatch: BufferSend queue size="<<q->size()
                          <<", messageType="<<mtBufferSend
                          << " for query ("<<queryID<<")");
        }
        enqueue(q);
        return;
    }
    case mtRecoverChunk:
    case mtResourcesFileExistsRequest:
    case mtResourcesFileExistsResponse:
    {
        _mustValidateQuery = false;
    }
    break;
    case mtError:
    case mtAbort:
    case mtCommit:
    {
        _mustValidateQuery = false;
        boost::shared_ptr<WorkQueue> q = _query->getErrorQueue();
        if (logger->isTraceEnabled() && q) {
            LOG4CXX_TRACE(logger, "Error queue size="<<q->size()
                          << " for query ("<<queryID<<")");
        }
        // We must not drop query-state-change messages to properly complete a query.
        // Therefore, in a unlikely event of the error queue being full, we will stall
        // the network thread until the queue drains. The queue is expected to drain (without a deadlock)
        // because mtError,mtAbort,mtCommit handlers do not require any network communication.
        const bool handleOverflow = false;
        boost::function<void()> work = boost::bind(&ServerMessageHandleJob::enqueue, this, q, handleOverflow);
        Query::runRestartableWork<void, scidb::WorkQueue::OverflowException>(work);
        return;
    }
    break;
    case mtFetch:
    {
        _logicalSourceId = _query->mapPhysicalToLogical(physicalSourceId);

        uint32_t objType = _messageDesc->getRecord<scidb_msg::Fetch>()->obj_type();
        switch(objType) {
        case RemoteArray::REMOTE_ARRAY_OBJ_TYPE:
        case PullSGArray::SG_ARRAY_OBJ_TYPE:
        {
            // This is in debug-build only because getOperatorQueue() returns a single-threaded queue
            assert(_query->chunkReqs[_logicalSourceId].increment() > 0);

            // Because both RemoteArray and PullSGArray use OperatorContext, they should be using the operator queue.
            boost::shared_ptr<WorkQueue> q = _query->getOperatorQueue();
            assert(q);
            if (logger->isTraceEnabled()) {
                LOG4CXX_TRACE(logger, "ServerMessageHandleJob::dispatch: Operator queue size="<<q->size()
                              << " for query ("<<queryID<<")");
            }
            enqueue(q);
            return;
        }
        // RemoteMergedArray does NOT use operator context; so no need to use the operator queue.
        case RemoteMergedArray::MERGED_ARRAY_OBJ_TYPE:
            enqueue(requestQueue);
            return;
        default:
            ASSERT_EXCEPTION(false, "ServerMessageHandleJob::dispatch need to handle all cases that call mtFetch!");
        } // end switch
    }
    case mtPreparePhysicalPlan:
    {
        enqueue(requestQueue);
        return;
    }
    break;
    case mtRemoteChunk: // reply to mtFetch
    {
        _logicalSourceId = _query->mapPhysicalToLogical(physicalSourceId);

        uint32_t objType = _messageDesc->getRecord<scidb_msg::Chunk>()->obj_type();
        switch(objType) {
        case RemoteArray::REMOTE_ARRAY_OBJ_TYPE:
        case PullSGArray::SG_ARRAY_OBJ_TYPE:
        {
            boost::shared_ptr<WorkQueue> q = _query->getBufferReceiveQueue();
            assert(q);
            if (logger->isTraceEnabled()) {
                LOG4CXX_TRACE(logger, "ServerMessageHandleJob::dispatch: Operator queue size="<<q->size()
                              <<", messageType="<<mtRemoteChunk
                              << " for query ("<<queryID<<")");
            }
            enqueue(q);
            return;
        }
        // RemoteMergedArray does NOT use operator context; so no need to use the BufferReceiveQueue.
        case RemoteMergedArray::MERGED_ARRAY_OBJ_TYPE:
            enqueue(workQueue);
            return;
        default:
            ASSERT_EXCEPTION(false, "ServerMessageHandleJob::dispatch need to handle all cases of mtRemoteChunk!");
        } // end switch
    }
    default:
    break;
    };
    enqueue(workQueue);
    return;
}

void ServerMessageHandleJob::enqueue(boost::shared_ptr<WorkQueue>& q, bool handleOverflow)
{
    static const char *funcName = "ServerMessageHandleJob::enqueue: ";
    LOG4CXX_TRACE(logger, funcName << "message of type="
                  <<  _messageDesc->getMessageType()
                  << ", for queryID=" << _messageDesc->getQueryID()
                  << ", from instanceID=" << _messageDesc->getSourceInstanceID());
    if (!q) {
        LOG4CXX_WARN(logger,  funcName << "Dropping message of type=" <<  _messageDesc->getMessageType()
                      << ", for queryID=" << _messageDesc->getQueryID()
                      << ", from instanceID=" << _messageDesc->getSourceInstanceID()
                      << " because the query appears deallocated");
        return;
    }

    shared_ptr<Job> thisJob(shared_from_this());
    WorkQueue::WorkItem work = boost::bind(&Job::executeOnQueue, thisJob, _1, _2);
    thisJob.reset();
    assert(work);
    try {
        q->enqueue(work);
    } catch (const WorkQueue::OverflowException& e) {
        if (handleOverflow) {
            LOG4CXX_ERROR(logger,  funcName <<
                          "Overflow exception from the message queue ("
                          << q.get()
                          <<"): " << e.what());
            assert(_query);
            _query->handleError(e.copy());
        }
        throw;
    }
}

void ServerMessageHandleJob::run()
{
    static const char *funcName = "ServerMessageHandleJob::run: ";
    assert(_messageDesc);
    assert(_messageDesc->getMessageType() < mtSystemMax);
    boost::function<void()> func = boost::bind(&Query::destroyFakeQuery, _query.get());
    Destructor<boost::function<void()> > fqd(func);

    const MessageType messageType = static_cast<MessageType>(_messageDesc->getMessageType());
    LOG4CXX_TRACE(logger, funcName << "Starting message handling: type=" << messageType
                  << ", queryID=" << _messageDesc->getQueryID());
    try
    {
        Query::setCurrentQueryID(_query->getQueryID());

        if (_mustValidateQuery) {
            Query::validateQueryPtr(_query);
        }

        if (messageType < 0 || messageType >= mtSystemMax) {
            handleInvalidMessage();
            return;
        }

        // Execute the current handler
        assert(_currHandler);

        _currHandler();

        LOG4CXX_TRACE(logger, funcName << "Finishing message handling: type=" << messageType);
    }
    catch ( const Exception& e)
    {
        assert(_messageDesc);
        LOG4CXX_ERROR(logger, funcName << "Error occurred in message handler: "
                      << e.what()
                      << ", messageType = " << messageType
                      << ", sourceInstance = " << _messageDesc->getSourceInstanceID()
                      << ", queryID="<<_messageDesc->getQueryID());
        assert(messageType != mtCancelQuery);

        if (!_query) {
            assert(false);
            LOG4CXX_DEBUG(logger, funcName << "Query " << _messageDesc->getQueryID() << " is already destructed");
        } else {

            if (messageType == mtPreparePhysicalPlan) {
                LOG4CXX_DEBUG(logger, funcName << "Execution of query " << _messageDesc->getQueryID() << " is aborted on worker");
                _query->done(e.copy());
            } else {
                LOG4CXX_DEBUG(logger, funcName << "Handle error for query " << _messageDesc->getQueryID());
                _query->handleError(e.copy());
            }

            if (messageType != mtError && messageType != mtAbort) {

                boost::shared_ptr<MessageDesc> errorMessage = makeErrorMessageFromException(e, _messageDesc->getQueryID());

                InstanceID const physicalCoordinatorID = _query->getPhysicalCoordinatorID();
                if (! _query->isCoordinator()) {
                    networkManager.sendPhysical(physicalCoordinatorID, errorMessage);
                }
                if (physicalCoordinatorID != _messageDesc->getSourceInstanceID() &&
                    _query->getInstanceID() != _query->mapPhysicalToLogical(_messageDesc->getSourceInstanceID())) {
                    networkManager.sendPhysical(_messageDesc->getSourceInstanceID(), errorMessage);
                }
            }
        }
    }
}

/// Map of initial handler routines for each message type
ServerMessageHandleJob::MsgHandler ServerMessageHandleJob::_msgHandlers[scidb::mtSystemMax] = {
    // mtNone,
    &ServerMessageHandleJob::handleInvalidMessage,
    // mtExecuteQuery,
    &ServerMessageHandleJob::handleInvalidMessage,
    // mtPreparePhysicalPlan,
    &ServerMessageHandleJob::handlePreparePhysicalPlan,
    // mtUnusedPlus3
    &ServerMessageHandleJob::handleInvalidMessage,
    // mtFetch,
    &ServerMessageHandleJob::handleFetchChunk,
    // mtChunk,
    &ServerMessageHandleJob::handleChunk,
    // mtChunkReplica,
    &ServerMessageHandleJob::handleReplicaChunk,
    // mtRecoverChunk,
    &ServerMessageHandleJob::handleInvalidMessage,
    // mtReplicaSyncRequest,
    &ServerMessageHandleJob::handleInvalidMessage,
    // mtReplicaSyncResponse,
    &ServerMessageHandleJob::handleReplicaSyncResponse,
    // mtAggregateChunk,
    &ServerMessageHandleJob::handleAggregateChunk,
    // mtQueryResult,
    &ServerMessageHandleJob::handleQueryResult,
    // mtError,
    &ServerMessageHandleJob::handleError,
    // mtSyncRequest,
    &ServerMessageHandleJob::handleSyncRequest,
    // mtSyncResponse,
    &ServerMessageHandleJob::handleSyncResponse,
    // mtCancelQuery,
    &ServerMessageHandleJob::handleInvalidMessage,
    // mtRemoteChunk,
    &ServerMessageHandleJob::handleRemoteChunk,
    // mtNotify,
    &ServerMessageHandleJob::handleNotify,
    // mtWait,
    &ServerMessageHandleJob::handleWait,
    // mtBarrier,
    &ServerMessageHandleJob::handleBarrier,
    // mtBufferSend,
    &ServerMessageHandleJob::handleBufferSend,
    // mtAlive,
    &ServerMessageHandleJob::handleInvalidMessage,
    // mtPrepareQuery,
    &ServerMessageHandleJob::handleInvalidMessage,
    // mtResourcesFileExistsRequest,
    &ServerMessageHandleJob::handleResourcesFileExists,
    // mtResourcesFileExistsResponse,
    &ServerMessageHandleJob::handleResourcesFileExists,
    // mtAbort
    &ServerMessageHandleJob::handleAbortQuery,
    // mtCommit
    &ServerMessageHandleJob::handleCommitQuery,
    // mtCompleteQuery
    &ServerMessageHandleJob::handleInvalidMessage,
    // mtSystemMax // must be last
};

void ServerMessageHandleJob::handleInvalidMessage()
{
    static const char *funcName = "ServerMessageHandleJob::handleInvalidMessage: ";
    const MessageType messageType = static_cast<MessageType>(_messageDesc->getMessageType());
    LOG4CXX_ERROR(logger,  funcName << "Unknown/unexpected message type " << messageType);
    assert(false);
    throw SYSTEM_EXCEPTION(SCIDB_SE_NETWORK, SCIDB_LE_UNKNOWN_MESSAGE_TYPE) << messageType;
}

void ServerMessageHandleJob::handlePreparePhysicalPlan()
{
    static const char *funcName = "ServerMessageHandleJob::handlePreparePhysicalPlan: ";
    shared_ptr<scidb_msg::PhysicalPlan> ppMsg = _messageDesc->getRecord<scidb_msg::PhysicalPlan>();

    const string clusterUuid = ppMsg->cluster_uuid();
    ASSERT_EXCEPTION((clusterUuid==Cluster::getInstance()->getUuid()),
                     (string(funcName)+string("unknown cluster UUID=")+clusterUuid));

    const string physicalPlan = ppMsg->physical_plan();

    LOG4CXX_DEBUG(logger,  funcName << "Preparing physical plan: queryID="
                  << _messageDesc->getQueryID() << ", physicalPlan='" << physicalPlan << "'");

    shared_ptr<InstanceLiveness> coordinatorLiveness;
    bool rc = parseQueryLiveness(coordinatorLiveness, ppMsg);
    if (!rc) {
        throw SYSTEM_EXCEPTION(SCIDB_SE_NETWORK, SCIDB_LE_INVALID_LIVENESS);
    }

    if (!_query->getCoordinatorLiveness()->isEqual(*coordinatorLiveness)) {
        shared_ptr<const InstanceLiveness> coordLiveness =
           const_pointer_cast<const InstanceLiveness>(coordinatorLiveness);
        _query->setCoordinatorLiveness(coordLiveness);
        throw SYSTEM_EXCEPTION(SCIDB_SE_NETWORK, SCIDB_LE_LIVENESS_MISMATCH);
    }

    boost::shared_ptr<QueryProcessor> queryProcessor = QueryProcessor::create();

    queryProcessor->parsePhysical(physicalPlan, _query);
    LOG4CXX_DEBUG(logger,  funcName << "Physical plan was parsed")

    handleExecutePhysicalPlan();
}

void ServerMessageHandleJob::handleExecutePhysicalPlan()
{
   static const char *funcName = "ServerMessageHandleJob::handleExecutePhysicalPlan: ";
   try {

      if  (_query->isCoordinator()) {
          handleInvalidMessage();
      }

      LOG4CXX_DEBUG(logger, funcName << "Running physical plan: queryID=" << _messageDesc->getQueryID())

      boost::shared_ptr<QueryProcessor> queryProcessor = QueryProcessor::create();

      _query->start();

      try {
         queryProcessor->execute(_query);
         LOG4CXX_DEBUG(logger,  funcName << "Query was executed");
      } catch (const std::bad_alloc& e) {
          throw SYSTEM_EXCEPTION(SCIDB_SE_NO_MEMORY, SCIDB_LE_MEMORY_ALLOCATION_ERROR) << e.what();
      }
      _query->done();

      // Creating message with result for sending to client
      boost::shared_ptr<MessageDesc> resultMessage = boost::make_shared<MessageDesc>(mtQueryResult);
      resultMessage->setQueryID(_query->getQueryID());

      networkManager.sendPhysical(_messageDesc->getSourceInstanceID(), resultMessage);
      LOG4CXX_DEBUG(logger, "Result was sent to instance #" << _messageDesc->getSourceInstanceID());
   }
   catch (const scidb::Exception& e)
   {
      LOG4CXX_ERROR(logger,  funcName << "QueryID = " << _query->getQueryID()
                    << " encountered the error: "
                    << e.what());
      e.raise();
   }
}

void ServerMessageHandleJob::handleQueryResult()
{
    static const char *funcName = "ServerMessageHandleJob::handleQueryResult: ";
    if  (!_query->isCoordinator()) {
        handleInvalidMessage();
    }

    const string arrayName = _messageDesc->getRecord<scidb_msg::QueryResult>()->array_name();

    LOG4CXX_DEBUG(logger,  funcName << "Received query result from instance#"
                  << _messageDesc->getSourceInstanceID()
                  << ", queryID=" << _messageDesc->getQueryID()
                  << ", arrayName=" << arrayName)

    // Signaling to query context to defreeze
    _query->results.release();
}

void ServerMessageHandleJob::sgSync()
{
    // in debug only because this executes on a single-threaded queue
    assert(_logicalSourceId!=INVALID_INSTANCE);
    assert(_logicalSourceId<_query->chunkReqs.size());
    assert(!_query->chunkReqs[_logicalSourceId].decrement());
}

void ServerMessageHandleJob::_handleChunkOrAggregateChunk(bool isAggregateChunk)
{
    static const char *funcName = "ServerMessageHandleJob::_handleChunkOrAggregateChunk: ";
    boost::shared_ptr<scidb_msg::Chunk> chunkRecord = _messageDesc->getRecord<scidb_msg::Chunk>();
    assert(!chunkRecord->eof());
    assert(_query);
    try
    {
        LOG4CXX_TRACE(logger, funcName << "Next chunk message was received")
        boost::shared_ptr<SGContext> sgCtx = dynamic_pointer_cast<SGContext>(_query->getOperatorContext());
        if (sgCtx == NULL) {
            shared_ptr<Query::OperatorContext> ctx = _query->getOperatorContext();
            string txt = ctx ? typeid(*ctx).name() : string("NULL") ;
            throw (SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_UNKNOWN_CTX)
                   << txt);
        }

        ScopedMutexLock cs(_query->resultCS);
        shared_ptr<CompressedBuffer> compressedBuffer = dynamic_pointer_cast<CompressedBuffer>(_messageDesc->getBinary());
        shared_ptr<SGChunkReceiver> chunkReceiver = sgCtx->_chunkReceiver;
        assert(chunkReceiver);
        Coordinates coordinates;
        for (int i = 0; i < chunkRecord->coordinates_size(); i++) {
            coordinates.push_back(chunkRecord->coordinates(i));
        }
        chunkReceiver->handleReceivedChunk(sgCtx,
                isAggregateChunk,
                _query->mapPhysicalToLogical(_messageDesc->getSourceInstanceID()),        // _logicalSourceId
                compressedBuffer,
                chunkRecord->compression_method(),
                chunkRecord->decompressed_size(),
                chunkRecord->attribute_id(),
                chunkRecord->count(),
                coordinates
                );

        sgSync();
        LOG4CXX_TRACE(logger, funcName << "Chunk was stored")
    }
    catch(const Exception& e)
    {
        sgSync(); //XXX TODO: this can be removed, because the error message will be sent as a result of throw
        throw;
    }
}

void ServerMessageHandleJob::handleReplicaSyncResponse()
{
    static const char *funcName = "ServerMessageHandleJob::handleReplicaSyncResponse: ";
    shared_ptr<ReplicationContext> replicationCtx(_query->getReplicationContext());
    _logicalSourceId = _query->mapPhysicalToLogical(_messageDesc->getSourceInstanceID());
    boost::shared_ptr<scidb_msg::DummyQuery> responseRecord = _messageDesc->getRecord<scidb_msg::DummyQuery>();
    ArrayID arrId = responseRecord->payload_id();
    if (arrId <= 0 || _logicalSourceId == _query->getInstanceID()) {
        assert(false);
        stringstream ss;
        ss << "Invalid ArrayID=0 from InstanceID="<<_messageDesc->getSourceInstanceID()
           <<" for QueryID="<<_query->getQueryID();
        throw (SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_UNKNOWN_ERROR)
               << ss.str());
    }
    LOG4CXX_TRACE(logger, funcName << "arrId="<<arrId
                  << ", sourceId="<<_logicalSourceId
                  << ", queryID="<<_query->getQueryID());
    replicationCtx->replicationAck(_logicalSourceId, arrId);
}

void ServerMessageHandleJob::handleReplicaChunk()
{
    static const char *funcName = "ServerMessageHandleJob::handleReplicaChunk: ";
    assert(static_cast<MessageType>(_messageDesc->getMessageType()) == mtChunkReplica);
    assert(_logicalSourceId != INVALID_INSTANCE);
    assert(_query);

    boost::shared_ptr<scidb_msg::Chunk> chunkRecord = _messageDesc->getRecord<scidb_msg::Chunk>();
    ArrayID arrId = chunkRecord->array_id();
    assert(arrId>0);

    LOG4CXX_TRACE(logger, funcName << "arrId="<<arrId
                  << ", sourceId="<<_logicalSourceId << ", queryID="<<_query->getQueryID());

    shared_ptr<ReplicationContext> replicationCtx(_query->getReplicationContext());

    assert(_logicalSourceId<replicationCtx->_chunkReplicasReqs.size());
    // in debug only because this executes on a single-threaded queue
    assert(!replicationCtx->_chunkReplicasReqs[_logicalSourceId].decrement());

    if (chunkRecord->eof()) {
        // last replication message for this arrId from _logicalSourceId
        assert(replicationCtx->_chunkReplicasReqs[_logicalSourceId].test());
        // when all eofs are received the work queue for this arrId can be removed

        _query->validate(); // to make sure no previous errors in replication

        LOG4CXX_DEBUG(logger, "handleReplicaChunk: received eof");

        // ack the eof message back to _logicalSourceId
        boost::shared_ptr<MessageDesc> responseMsg = boost::make_shared<MessageDesc>(mtReplicaSyncResponse);
        boost::shared_ptr<scidb_msg::DummyQuery> responseRecord = responseMsg->getRecord<scidb_msg::DummyQuery>();
        responseRecord->set_payload_id(arrId);
        responseMsg->setQueryID(_query->getQueryID());

        networkManager.sendPhysical(_messageDesc->getSourceInstanceID(), responseMsg);
        return;
    }

    const int compMethod = chunkRecord->compression_method();
    const size_t decompressedSize = chunkRecord->decompressed_size();
    const AttributeID attributeID = chunkRecord->attribute_id();
    const size_t count = chunkRecord->count();
    Coordinates coordinates;
    for (int i = 0; i < chunkRecord->coordinates_size(); i++) {
        coordinates.push_back(chunkRecord->coordinates(i));
    }

    boost::shared_ptr<Array> dbArr = replicationCtx->getPersistentArray(arrId);
    assert(dbArr);

    if(chunkRecord->tombstone())
    { // tombstone record
        StorageManager::getInstance().removeLocalChunkVersion(dbArr->getArrayDesc(), coordinates, _query);
    }
    else if (decompressedSize <= 0)
    { // what used to be clone of replica
        assert(false);
        stringstream ss;
        ss << "Invalid chunk decompressedSize=" << decompressedSize
           << " from InstanceID="<<_messageDesc->getSourceInstanceID()
           << " for QueryID="<<_query->getQueryID();
        throw (SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_UNKNOWN_ERROR) << ss.str());
    }
    else
    { // regular chunk
        boost::shared_ptr<ArrayIterator> outputIter = dbArr->getIterator(attributeID);
        boost::shared_ptr<CompressedBuffer> compressedBuffer =
            dynamic_pointer_cast<CompressedBuffer>(_messageDesc->getBinary());
        compressedBuffer->setCompressionMethod(compMethod);
        compressedBuffer->setDecompressedSize(decompressedSize);
        Chunk& outChunk = outputIter->newChunk(coordinates);
        try
        {
            outChunk.decompress(*compressedBuffer);
            outChunk.setCount(count);
            outChunk.write(_query);
        }
        catch (const scidb::Exception& e)
        {
            outputIter->deleteChunk(outChunk);
            throw;
        }
    }
}

void ServerMessageHandleJob::handleRemoteChunk()
{
    boost::shared_ptr<scidb_msg::Chunk> chunkRecord = _messageDesc->getRecord<scidb_msg::Chunk>();
    const uint32_t objType = chunkRecord->obj_type();
    const AttributeID attId = chunkRecord->attribute_id();
    assert(_query);

    // Must have been set in dispatch().
    assert(_logicalSourceId == _query->mapPhysicalToLogical(_messageDesc->getSourceInstanceID()));

    switch(objType)
    {
    case RemoteArray::REMOTE_ARRAY_OBJ_TYPE:
    {
        boost::shared_ptr<RemoteArray> ra = RemoteArray::getContext(_query)->getInboundArray(_logicalSourceId);
        validateRemoteChunkInfo(ra.get(), _messageDesc->getMessageType(), objType,
                            attId, _messageDesc->getSourceInstanceID());
        ra->handleChunkMsg(_messageDesc);
    }
    break;
    case RemoteMergedArray::MERGED_ARRAY_OBJ_TYPE:
    {
        boost::shared_ptr<RemoteMergedArray> rma = _query->getMergedArray();
        validateRemoteChunkInfo(rma.get(), _messageDesc->getMessageType(), objType,
                                attId, _messageDesc->getSourceInstanceID());
        rma->handleChunkMsg(_messageDesc);
    }
    break;
    case PullSGArray::SG_ARRAY_OBJ_TYPE:
    {
        shared_ptr<PullSGContext> sgCtx =
            dynamic_pointer_cast<PullSGContext>(_query->getOperatorContext());
        if (sgCtx == NULL) {
            shared_ptr<Query::OperatorContext> ctx = _query->getOperatorContext();
            string txt = ctx ? typeid(*ctx).name() : string("NULL") ;
            throw (SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_UNKNOWN_CTX)
                   << txt);
        }
        boost::shared_ptr<PullSGArray> arr = sgCtx->getResultArray();
        validateRemoteChunkInfo(arr.get(), _messageDesc->getMessageType(), objType,
                                attId, _messageDesc->getSourceInstanceID());
        arr->handleChunkMsg(_messageDesc,_logicalSourceId);
    }
    break;
    default:
    {
        stringstream ss;
        ss << "Malformed remote message: "
           << " messageID="<< _messageDesc->getMessageType()
           << " attributeID="<<attId
           << " array type="<<objType
           << " from InstanceID="<< _messageDesc->getSourceInstanceID()
           << " for queryID="<<_query->getQueryID();
        ASSERT_EXCEPTION(false, ss.str());
    }
    };
}

void ServerMessageHandleJob::handleFetchChunk()
{
    static const char *funcName = "ServerMessageHandleJob::handleFetchChunk: ";
    boost::shared_ptr<scidb_msg::Fetch> fetchRecord = _messageDesc->getRecord<scidb_msg::Fetch>();
    const QueryID queryID = _messageDesc->getQueryID();
    const uint32_t attributeId = fetchRecord->attribute_id();
    const bool positionOnly = fetchRecord->position_only();
    const uint32_t objType = fetchRecord->obj_type();

    LOG4CXX_TRACE(logger, funcName << "Fetching remote chunk attributeID="
                  << attributeId << " for queryID=" << queryID
                  << " from instanceID="<< _messageDesc->getSourceInstanceID());

    assert(queryID);
    assert(queryID == _query->getQueryID());

    if (objType>PullSGArray::SG_ARRAY_OBJ_TYPE) {
        stringstream ss;
        ss << "Malformed remote message: "
           << " messageID="<< _messageDesc->getMessageType()
           << " attributeID="<<attributeId
           << " invalid array type="<<objType
           << " from InstanceID="<< _messageDesc->getSourceInstanceID()
           << " for queryID="<<queryID;
        ASSERT_EXCEPTION(false, ss.str());
    }

    if (objType==PullSGArray::SG_ARRAY_OBJ_TYPE) {
        handleSGFetchChunk();
        sgSync();
        return;
    }

    // At this point, the puller is either a RemoteArray or a RemoteMergedArray.
    //   - RemoteArray uses Query::_outboundArrays, and allows any instance to pull from any instance.
    //   - RemoteMergedArray uses Query::_currentResultArray, and *only* allows the coordinator pull from a worker instance.
    //
    assert(objType == RemoteArray::REMOTE_ARRAY_OBJ_TYPE || objType == RemoteMergedArray::MERGED_ARRAY_OBJ_TYPE);

    shared_ptr<Array> resultArray;

    if (objType == RemoteArray::REMOTE_ARRAY_OBJ_TYPE) {
        assert(_logicalSourceId == _query->mapPhysicalToLogical(_messageDesc->getSourceInstanceID()));
        resultArray = RemoteArray::getContext(_query)->getOutboundArray(_logicalSourceId);
    }
    else {
        if  (_query->isCoordinator()) {
            handleInvalidMessage();
            return;
        }
        resultArray = _query->getCurrentResultArray();
    }

    validateRemoteChunkInfo(resultArray.get(), _messageDesc->getMessageType(), objType,
                            attributeId, _messageDesc->getSourceInstanceID());

    boost::shared_ptr<ConstArrayIterator> iter = resultArray->getConstIterator(attributeId);

    boost::shared_ptr<MessageDesc> chunkMsg;

    if (!iter->end())
    {
        boost::shared_ptr<scidb_msg::Chunk> chunkRecord;
        if (!positionOnly) {
            const ConstChunk* chunk = &iter->getChunk();
            boost::shared_ptr<CompressedBuffer> buffer = boost::make_shared<CompressedBuffer>();
            boost::shared_ptr<ConstRLEEmptyBitmap> emptyBitmap;
            if (resultArray->getArrayDesc().getEmptyBitmapAttribute() != NULL &&
                !chunk->getAttributeDesc().isEmptyIndicator()) {
                emptyBitmap = chunk->getEmptyBitmap();
            }
            chunk->compress(*buffer, emptyBitmap);
            emptyBitmap.reset(); // the bitmask must be cleared before the iterator is advanced (bug?)
            chunkMsg = boost::make_shared<MessageDesc>(mtRemoteChunk, buffer);
            chunkRecord = chunkMsg->getRecord<scidb_msg::Chunk>();
            chunkRecord->set_compression_method(buffer->getCompressionMethod());
            chunkRecord->set_decompressed_size(buffer->getDecompressedSize());
            chunkRecord->set_count(chunk->isCountKnown() ? chunk->count() : 0);
            const Coordinates& coordinates = chunk->getFirstPosition(false);
            for (size_t i = 0; i < coordinates.size(); i++) {
                chunkRecord->add_coordinates(coordinates[i]);
            }
            ++(*iter);

        } else {

            chunkMsg = boost::make_shared<MessageDesc>(mtRemoteChunk);
            chunkRecord = chunkMsg->getRecord<scidb_msg::Chunk>();
        }
        chunkMsg->setQueryID(queryID);
        chunkRecord->set_eof(false);
        chunkRecord->set_obj_type(objType);
        chunkRecord->set_attribute_id(attributeId);
        if (!iter->end() || positionOnly)
        {
            chunkRecord->set_has_next(true);
            const Coordinates& next_coordinates = iter->getPosition();
            for (size_t i = 0; i < next_coordinates.size(); i++) {
                chunkRecord->add_next_coordinates(next_coordinates[i]);
            }
        }
        else
        {
            chunkRecord->set_has_next(false);
        }

        shared_ptr<Query> query = Query::getQueryByID(queryID);
        if (query->getWarnings().size())
        {
            //Propagate warnings gathered on coordinator to client
            vector<Warning> v = query->getWarnings();
            for (vector<Warning>::const_iterator it = v.begin(); it != v.end(); ++it)
            {
                ::scidb_msg::Chunk_Warning* warn = chunkRecord->add_warnings();
                warn->set_code(it->getCode());
                warn->set_file(it->getFile());
                warn->set_function(it->getFunction());
                warn->set_line(it->getLine());
                warn->set_what_str(it->msg());
                warn->set_strings_namespace(it->getStringsNamespace());
                warn->set_stringified_code(it->getStringifiedCode());
            }
            query->clearWarnings();
        }

        LOG4CXX_TRACE(logger, funcName << "Prepared message with chunk data");
    }
    else
    {
        chunkMsg = boost::make_shared<MessageDesc>(mtRemoteChunk);
        boost::shared_ptr<scidb_msg::Chunk> chunkRecord = chunkMsg->getRecord<scidb_msg::Chunk>();
        chunkMsg->setQueryID(queryID);
        chunkRecord->set_eof(true);
        chunkRecord->set_obj_type(objType);
        chunkRecord->set_attribute_id(attributeId);
        LOG4CXX_TRACE(logger, funcName << "Prepared message with information that there are no unread chunks");
    }

    networkManager.sendPhysical(_messageDesc->getSourceInstanceID(), chunkMsg);

    if (objType==RemoteArray::REMOTE_ARRAY_OBJ_TYPE) {
        sgSync();
        return;
    }

    LOG4CXX_TRACE(logger, funcName << "Remote chunk was sent to client");
}

void ServerMessageHandleJob::handleSGFetchChunk()
{
    static const char *funcName = "ServerMessageHandleJob::handleSGFetchChunk: ";

    boost::shared_ptr<scidb_msg::Fetch> fetchRecord = _messageDesc->getRecord<scidb_msg::Fetch>();
    const QueryID queryID = _messageDesc->getQueryID();

    ASSERT_EXCEPTION((fetchRecord->has_attribute_id()), funcName);
    const uint32_t attributeId = fetchRecord->attribute_id();
    ASSERT_EXCEPTION((fetchRecord->has_position_only()), funcName);
    const bool positionOnlyOK = fetchRecord->position_only();
    ASSERT_EXCEPTION((fetchRecord->has_obj_type()), funcName);
    const uint32_t objType = fetchRecord->obj_type();
    ASSERT_EXCEPTION((objType == PullSGArray::SG_ARRAY_OBJ_TYPE), funcName);
    ASSERT_EXCEPTION((fetchRecord->has_prefetch_size()), funcName);
    const uint32_t prefetchSize = fetchRecord->prefetch_size();
    ASSERT_EXCEPTION((fetchRecord->has_fetch_id()), funcName);
    const uint64_t fetchId = fetchRecord->fetch_id();
    ASSERT_EXCEPTION((fetchId>0 && fetchId<uint64_t(~0)), funcName);

    LOG4CXX_TRACE(logger, funcName << "Fetching remote chunk attributeID="
                  << attributeId << " for queryID=" << queryID
                  << " fetchID=" << fetchId
                  << " from instanceID="<< _messageDesc->getSourceInstanceID());

    boost::shared_ptr<PullSGContext> sgCtx = dynamic_pointer_cast<PullSGContext>(_query->getOperatorContext());
    if (sgCtx == NULL) {
        shared_ptr<Query::OperatorContext> ctx = _query->getOperatorContext();
        string txt = ctx ? typeid(*ctx).name() : string("NULL") ;
        throw (SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_UNKNOWN_CTX)
               << txt);
    }

    _logicalSourceId = _query->mapPhysicalToLogical(_messageDesc->getSourceInstanceID());

    ScopedMutexLock cs(_query->resultCS); //XXX should not be necessary because executing on SG queue

    PullSGContext::ChunksWithDestinations chunksToSend;
    sgCtx->getNextChunks(_query, _logicalSourceId, attributeId,
                         positionOnlyOK, prefetchSize, fetchId,
                         chunksToSend);

    for (PullSGContext::ChunksWithDestinations::iterator iter = chunksToSend.begin();
         iter != chunksToSend.end(); ++iter) {

        const InstanceID instance = iter->first;
        shared_ptr<MessageDesc>& chunkMsg = iter->second;

        LOG4CXX_TRACE(logger, funcName << "Forwarding chunk attributeID="
                      << attributeId << " for queryID=" << queryID
                      << " to (logical) instanceID="<< instance);

        if (instance == _query->getInstanceID() ) {
            networkManager.sendLocal(_query, chunkMsg);
        } else {
            // remote
            networkManager.sendPhysical(_query->mapLogicalToPhysical(instance), chunkMsg);
        }
    }
    LOG4CXX_TRACE(logger, funcName << chunksToSend.size() << " chunks sent");
}

void ServerMessageHandleJob::handleSyncRequest()
{
    static const char *funcName = "ServerMessageHandleJob::handleSyncRequest: ";

    _logicalSourceId = _query->mapPhysicalToLogical(_messageDesc->getSourceInstanceID());
    assert(_logicalSourceId!=INVALID_INSTANCE);

    assert(_logicalSourceId<_query->chunkReqs.size());

    // in debug only because this executes on a single-threaded queue
    assert(_query->chunkReqs[_logicalSourceId].test());

    shared_ptr<MessageDesc> syncMsg(make_shared<MessageDesc>(mtSyncResponse));
    syncMsg->setQueryID(_messageDesc->getQueryID());

    if (_logicalSourceId == _query->getInstanceID()) {
        networkManager.sendLocal(_query, syncMsg);
    } else {
        networkManager.sendPhysical(_messageDesc->getSourceInstanceID(), syncMsg);
    }
    LOG4CXX_TRACE(logger, funcName
                  << "Sync confirmation was sent to instance #"
                  << _messageDesc->getSourceInstanceID());

}

void ServerMessageHandleJob::handleBarrier()
{
    static const char *funcName = "ServerMessageHandleJob::handleBarrier: ";
    boost::shared_ptr<scidb_msg::DummyQuery> barrierRecord = _messageDesc->getRecord<scidb_msg::DummyQuery>();

    LOG4CXX_TRACE(logger, funcName << "handling barrier message in query "
                  << _messageDesc->getQueryID());

    assert(barrierRecord->payload_id()<MAX_BARRIERS);
    _query->semSG[barrierRecord->payload_id()].release();
}

void ServerMessageHandleJob::handleSyncResponse()
{
    static const char *funcName = "ServerMessageHandleJob::handleSyncResponse: ";

    LOG4CXX_TRACE(logger, funcName << "Receiving confirmation for sync message and release syncSG in query"
                  << _messageDesc->getQueryID());

    // Signaling to query to release SG semaphore inside physical operator and continue to work
    // This can run on any queue because the state of SG (or pulling SG) should be such that it is
    // expecting only this single message (so no other messages need to be ordered wrt to this one).
    _query->syncSG.release();
}

// must run on _query->errorQueue
void ServerMessageHandleJob::handleError()
{
    static const char *funcName = "ServerMessageHandleJob::handleError: ";
    boost::shared_ptr<scidb_msg::Error> errorRecord = _messageDesc->getRecord<scidb_msg::Error>();

    const string clusterUuid = errorRecord->cluster_uuid();
    ASSERT_EXCEPTION((clusterUuid==Cluster::getInstance()->getUuid()),
                     (string(funcName)+string("unknown cluster UUID=")+clusterUuid));

    const string errorText = errorRecord->what_str();
    const int32_t errorCode = errorRecord->long_error_code();

    LOG4CXX_ERROR(logger, funcName
                  << " Error on processing query " << _messageDesc->getQueryID()
                  << " on instance " << _messageDesc->getSourceInstanceID()
                  << ". Query coordinator ID: " << _query->getPhysicalCoordinatorID()
                  << ". Message errorCode: " << errorCode
                  << ". Message txt: " << errorText);

    assert(_query->getQueryID() == _messageDesc->getQueryID());

    shared_ptr<Exception> e = makeExceptionFromErrorMessage(_messageDesc);
    bool isAbort = false;
    if (errorCode == SCIDB_LE_QUERY_NOT_FOUND || errorCode == SCIDB_LE_QUERY_NOT_FOUND2)
    {
        if (_query->getPhysicalCoordinatorID() == _messageDesc->getSourceInstanceID()) {
            // The coordinator does not know about this query, we will also abort the query
            isAbort = true;
        }
        else
        {
            // A remote instance did not find the query, it must be out of sync (because of restart?).
            e = SYSTEM_EXCEPTION_SPTR(SCIDB_SE_NETWORK, SCIDB_LE_NO_QUORUM);
        }
    }
    if (isAbort) {
        _query->handleAbort();
    } else {
        _query->handleError(e);
    }
}

// must run on _query->errorQueue
void ServerMessageHandleJob::handleAbortQuery()
{
    static const char *funcName = "ServerMessageHandleJob::handleAbortQuery: ";

    boost::shared_ptr<scidb_msg::DummyQuery> record = _messageDesc->getRecord<scidb_msg::DummyQuery>();
    const string clusterUuid = record->cluster_uuid();
    ASSERT_EXCEPTION((clusterUuid==Cluster::getInstance()->getUuid()),
                     (string(funcName)+string("unknown cluster UUID=")+clusterUuid));

    if (_query->getPhysicalCoordinatorID() != _messageDesc->getSourceInstanceID()
        || _query->isCoordinator()) {
        handleInvalidMessage();
    }
    _query->handleAbort();
}

// must run on _query->errorQueue
void ServerMessageHandleJob::handleCommitQuery()
{
    static const char *funcName = "ServerMessageHandleJob::handleCommitQuery: ";

    boost::shared_ptr<scidb_msg::DummyQuery> record = _messageDesc->getRecord<scidb_msg::DummyQuery>();
    const string clusterUuid = record->cluster_uuid();
    ASSERT_EXCEPTION((clusterUuid==Cluster::getInstance()->getUuid()),
                     (string(funcName)+string("unknown cluster UUID=")+clusterUuid));

    if (_query->getPhysicalCoordinatorID() != _messageDesc->getSourceInstanceID()
        || _query->isCoordinator()) {
        handleInvalidMessage();
    }
    _query->handleCommit();
}

void ServerMessageHandleJob::handleNotify()
{
    static const char *funcName = "ServerMessageHandleJob::handleNotify: ";

    boost::shared_ptr<scidb_msg::DummyQuery> record = _messageDesc->getRecord<scidb_msg::DummyQuery>();
    const string clusterUuid = record->cluster_uuid();
    ASSERT_EXCEPTION((clusterUuid==Cluster::getInstance()->getUuid()),
                     (string(funcName)+string("unknown cluster UUID=")+clusterUuid));

    if  (!_query->isCoordinator()) {
        handleInvalidMessage();
    }
    LOG4CXX_DEBUG(logger, funcName << "Notify on processing query "
                  << _messageDesc->getQueryID() << " from instance "
                  << _messageDesc->getSourceInstanceID());

    _query->results.release();
}


void ServerMessageHandleJob::handleWait()
{
    static const char *funcName = "ServerMessageHandleJob::handleWait: ";

    boost::shared_ptr<scidb_msg::DummyQuery> record = _messageDesc->getRecord<scidb_msg::DummyQuery>();
    const string clusterUuid = record->cluster_uuid();
    ASSERT_EXCEPTION((clusterUuid==Cluster::getInstance()->getUuid()),
                     (string(funcName)+string("unknown cluster UUID=")+clusterUuid));

    if  (_query->isCoordinator()) {
        handleInvalidMessage();
    }
    LOG4CXX_DEBUG(logger, funcName << "Wait on processing query "
                  << _messageDesc->getQueryID())

    _query->results.release();
}


void ServerMessageHandleJob::handleBufferSend()
{
    boost::shared_ptr<scidb_msg::DummyQuery> msgRecord = _messageDesc->getRecord<scidb_msg::DummyQuery>();
    assert(_query);
    _logicalSourceId = _query->mapPhysicalToLogical(_messageDesc->getSourceInstanceID());
    {
        ScopedMutexLock mutexLock(_query->_receiveMutex);
        _query->_receiveMessages[_logicalSourceId].push_back(_messageDesc);
    }
    _query->_receiveSemaphores[_logicalSourceId].release();
}

void ServerMessageHandleJob::handleResourcesFileExists()
{
    static const char *funcName = "ServerMessageHandleJob::handleResourcesFileExists: ";
    LOG4CXX_TRACE(logger, funcName << " called");
    Resources::getInstance()->handleFileExists(_messageDesc);
}

} // namespace
