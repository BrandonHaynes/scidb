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
 * @file ClientMessageHandleJob.h
 *
 * @author roman.simakov@gmail.com
 *
 * @brief The job for handling messages from client.
 *
 * The main difference between handling of messages between instances is that we must send
 * response to this message and keep synchronous client connection to do that.
 */

#ifndef CLIENTMESSAGEHANDLEJOB_H_
#define CLIENTMESSAGEHANDLEJOB_H_

#include <boost/shared_ptr.hpp>
#include <boost/enable_shared_from_this.hpp>
#include <boost/asio.hpp>
#include <stdint.h>

#include <util/Job.h>
#include <network/proto/scidb_msg.pb.h>
#include <array/Metadata.h>
#include <query/QueryProcessor.h>
#include "Connection.h"
#include "MessageHandleJob.h"

namespace scidb
{

class Connection;

/**
 * The class created by network message handler for adding to queue to be processed
 * by thread pool and handle message from client.
 */
class ClientMessageHandleJob : public MessageHandleJob
{
 public:
    ClientMessageHandleJob(boost::shared_ptr< Connection > connection,
                           const boost::shared_ptr<MessageDesc>& messageDesc);
    /**
     * Based on its contents this message is prepared and scheduled to run
     * on an appropriate queue.
     * @param requestQueue a system queue for running jobs that may block waiting for events from other jobs
     * @param workQueue a system queue for running jobs that are guaranteed to make progress
     */
    virtual void dispatch(boost::shared_ptr<WorkQueue>& requestQueue,
                          boost::shared_ptr<WorkQueue>& workQueue);

 protected:

    /// Implementation of Job::run()
    /// @see Job::run()
    virtual void run();

 private:
    boost::shared_ptr<Connection> _connection;

    std::string getProgramOptions(const string &programOptions) const;

    /**
     *  This method processes message mtPrepareQuery containing client query string.
     *  The processing includes parsing and building an execution plan.
     *  When processing is complete a response is sent to client using _connection.
     */
    void prepareClientQuery();

    /**
     * In case of certain exceptions (e.g. LockBusyException),
     * prepareClientQuery() can be re-tried multiple times.
     * This is the method to do so.
     *  @param queryResult is a structure containing the current state of the query
     */
    void retryPrepareQuery(scidb::QueryResult& queryResult);

    /// Helper routine
    void postPrepareQuery(scidb::QueryResult& queryResult);

    /**
     *  This method processes message mtPrepareQuery containing client query string.
     *  The processing includes executing the execution plan.
     *  When processing is complete a response is sent to client using _connection.
     */
    void executeClientQuery();

    /**
     * In case of certain exceptions (e.g. LockBusyException)
     * executeClientQuery() can be re-tried multiple times.
     * This is the method to do so.
     *  @param queryResult is a structure containing the current state of the query
     */
    void retryExecuteQuery(scidb::QueryResult& queryResult);
    /// Helper routine
    void postExecuteQueryInternal(scidb::QueryResult& queryResult);

    /**
     * This method sends next chunk to the client.
     * It may schedule (serially) fetchMergedChunk() to do the actual work.
     */
    void fetchChunk();
    /**
     * Fetches partial chunks from some/all instances to produce a complete chunk
     * to be sent to the client. It never waits, but reschedules and re-executes itself
     * until a complete chunk is ready or the query is aborted.
     */
    void fetchMergedChunk(boost::shared_ptr<RemoteMergedArray>& fetchArray, AttributeID attributeId,
                          Notification<scidb::Exception>::ListenerID queryErrorListenerID);
    /// Helper to construct an mtChunk message for the client
    void populateClientChunk(const std::string& arrayName,
                             AttributeID attributeId,
                             const ConstChunk* chunk,
                             boost::shared_ptr<MessageDesc>& chunkMsg);
    /**
     * Used to re-schedule fetchMergedChunk()
     */
    void executeSerially(boost::shared_ptr<WorkQueue>& serialQueue,
                         boost::weak_ptr<WorkQueue>& initialQueue,
                         const scidb::Exception* error);
    /**
     * Functor used for re-scheduling fetchMergedChunk() in response to various events (e.g. partial chunk arrival)
     */
    typedef boost::function<void(const scidb::Exception* error)> RescheduleCallback;
    /**
     * Generate a RescheduleCallback functor
     * @param serialQueue [out] the serial work queue where fetchMergedChunk() is to be executed
     */
    RescheduleCallback getSerializeCallback(boost::shared_ptr<WorkQueue>& serialQueue);
    /**
     * Query error event handler
     */
    void handleQueryError(RescheduleCallback& cb,
                          Notification<scidb::Exception>::MessageTypePtr errPtr);

    /// Internal exception used to cancel any outstanding attempts to run fetchMergedChunk()
    class CancelChunkFetchException: public SystemException
    {
    public:
      CancelChunkFetchException(const char* file, const char* function, int32_t line)
      : SystemException(file, function, line, "scidb",
                        SCIDB_SE_INTERNAL, SCIDB_LE_UNKNOWN_ERROR,
                        "SCIDB_E_INTERNAL", "SCIDB_E_UNKNOWN_ERROR", uint64_t(0))
      {
          (*this) << "scidb::ClientMessageHandleJob::CancelChunkFetchException";
      }
      ~CancelChunkFetchException() throw () {}
      void raise() const { throw *this; }
      virtual Exception::Pointer copy() const
      {
          Exception::Pointer ep(boost::make_shared<CancelChunkFetchException>(_file.c_str(),
                                                                      _function.c_str(),
                                                                      _line));
          return ep;
      }
   };

    /**
     * This method cancels query execution and free context
     */
    void cancelQuery();

    /**
     * This method completes the query execution, persists the changes, and frees the context
     */
    void completeQuery();

    /// Helper to deal with exceptions in prepare/executeClientQuery()
    void handleExecuteOrPrepareError(const scidb::Exception& e,
                                     const scidb::QueryResult& queryResult,
                                     const scidb::SciDB& scidb);

    /// Helper for scheduling this message on a given queue
    void enqueue(boost::shared_ptr<WorkQueue>& q);

    /// Helper for scheduling this message on the error queue of the query
    void enqueueOnErrorQueue(QueryID queryID);

    /// Helper to send a message to the client on _connection
    void sendMessageToClient(boost::shared_ptr<MessageDesc>& msg);
};

} // namespace

#endif /* CLIENTMESSAGEHANDLEJOB_H_ */
