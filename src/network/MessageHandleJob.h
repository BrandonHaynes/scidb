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
 * MessageHandleJob.h
 *
 *  Created on: Jan 12, 2010
 *      Author: roman.simakov@gmail.com
 */

#ifndef MESSAGEHANDLEJOB_H_
#define MESSAGEHANDLEJOB_H_

#include <boost/shared_ptr.hpp>
#include <boost/enable_shared_from_this.hpp>
#include <stdint.h>
#include <array/Metadata.h>
#include <util/Job.h>
#include <util/JobQueue.h>
#include <util/WorkQueue.h>
#include <query/Query.h>
#include <network/proto/scidb_msg.pb.h>
#include <network/Connection.h>
#include <network/NetworkManager.h>

namespace scidb
{
    class InstanceLiveness;
    /**
     * The class created by network message handler for adding to queue to be processed
     * by thread pool and handle message from client.
     */
    class MessageHandleJob: public Job, public boost::enable_shared_from_this<MessageHandleJob>
    {
    public:
        MessageHandleJob(const boost::shared_ptr<MessageDesc>& messageDesc)
        : Job(boost::shared_ptr<Query>()),
        _messageDesc(messageDesc)
        {
        }
        /**
         * Based on its contents this message is prepared and scheduled to run
         * on an appropriate queue.
         * @param requestQueue a system queue for running jobs that may block waiting for events from other jobs
         * @param workQueue a system queue for running jobs that are guaranteed to make progress
         */
        virtual void dispatch(boost::shared_ptr<WorkQueue>& requestQueue,
                              boost::shared_ptr<WorkQueue>& workQueue) = 0;
    protected:
        boost::shared_ptr<MessageDesc> _messageDesc;
        boost::shared_ptr<boost::asio::deadline_timer> _timer;

        /// Reschedule this job if array locks are not taken
        void reschedule(uint64_t delayMicroSec);

        /**
         * Validate remote message information identifying a chunk
         * @throws scidb::SystemException if the Job::_query is invalid or the arguments are invalid
         * @note in Debug build invalid arguments will cause an abort()
         * @param array to which the chunk corresponds
         * @param msgType message ID
         * @param arrayType 'objType' identifier used in some messages or ~0
         * @param attId the chunk's attribute ID
         * @param physInstanceID physical instance ID of message source
         */
        void validateRemoteChunkInfo(const Array* array,
                                     const MessageID msgID,
                                     const uint32_t arrayType,
                                     const AttributeID attId,
                                     const InstanceID physInstanceID);

    private:
        /// Handler for for the array lock timeout. It reschedules the current job
        static void handleRescheduleTimeout(boost::shared_ptr<Job>& job,
                                            boost::shared_ptr<WorkQueue>& toQueue,
                                            boost::shared_ptr<SerializationCtx>& sCtx,
                                            boost::shared_ptr<boost::asio::deadline_timer>& timer,
                                            const boost::system::error_code& error);
    };

    class ServerMessageHandleJob : public MessageHandleJob
    {
    public:
        ServerMessageHandleJob(const boost::shared_ptr<MessageDesc>& messageDesc);
        virtual ~ServerMessageHandleJob();

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
        NetworkManager& networkManager;

        /**
         * Any message on the wire includes a physical sender ID in it.
         * Users of NetworkManager deal with logical (sender AND receiver) IDs.
         * A typical pattern is: when a ServerMessageHandleJob receives a message, it translates the sender ID from physical to logical.
         */
        size_t _logicalSourceId;

        bool _mustValidateQuery;

        typedef void(ServerMessageHandleJob::*MsgHandler)();
        static MsgHandler _msgHandlers[scidb::mtSystemMax];

        void sgSync();
        void handlePreparePhysicalPlan();
        void handleExecutePhysicalPlan();
        void handleQueryResult();

        /**
         * Helper to avoid duplicate code.
         * When an empty-bitmap chunk is received, if RLE is true, the chunk is materialized, and getEmptyBitmap() is called on it.
         * The returned shared_ptr<ConstRLEEmptyBitmap> will be stored in the SG context and
         * will be used to process future chunks/aggregateChunks from the same sender.
         * Note that an empty-bitmap attribute can never be of aggregate type.
         */
        void _handleChunkOrAggregateChunk(bool isAggregateChunk);

        void handleChunk()
        {
            _handleChunkOrAggregateChunk(false);
        }

        void handleAggregateChunk()
        {
            _handleChunkOrAggregateChunk(true);
        }

        void handleRemoteChunk();
        void handleFetchChunk();
        void handleSGFetchChunk();
        void handleSyncRequest();
        void handleSyncResponse();
        void handleError();
        void handleNotify();
        void handleWait();
        void handleBarrier();
        void handleBufferSend();
        void handleReplicaSyncResponse();
        void handleReplicaChunk();
        void handleInstanceStatus();
        void handleResourcesFileExists();
        void handleInvalidMessage();
        void handleAbortQuery();
        void handleCommitQuery();
        /**
         * Helper method to enqueue this job to a given queue
         * @param queue the WorkQueue for running this job
         * @param handleOverflow indicates whether to handle the queue overflow by setting this message's query to error;
         *        true by default
         * @throws WorkQueue::OverflowException if queue has no space (regardless of the handleOverflow flag)
         *
         * @note After you call enqueue, do NOT read or write this ServerMessageHandleJob (except with extreme care; see below).
         *       The dispatch() function is single threaded before calling enqueue();
         *       but after enqueue() is called, other threads may read or change its content.
         *       In most cases, you do not need to read/write after calling enqueue().
         *       But if you really do, protect ALL accesses to it with a mutex.
         */
        void enqueue(boost::shared_ptr<WorkQueue>& queue, bool handleOverflow=true);
    };
} // namespace

#endif /* MESSAGEHANDLEJOB_H_ */
