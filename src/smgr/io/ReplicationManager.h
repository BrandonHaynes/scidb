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
 * ReplicationManager.h
 *
 *      Description: Simple replication manager that blocks the replicating thread if the network is congested
 */

#ifndef REPLICATION_MANAGER_H_
#define REPLICATION_MANAGER_H_

#include <deque>
#include <map>
#include <network/NetworkManager.h>
#include <util/Event.h>
#include <util/Mutex.h>
#include <util/Thread.h>
#include <util/Notification.h>
#include <query/Query.h>
#include <util/InjectedError.h>

namespace scidb
{
class ReplicationManager;
 class ReplicationManager : public Singleton<ReplicationManager>,
 InjectedErrorListener<ReplicaWaitInjectedError>, InjectedErrorListener<ReplicaSendInjectedError>
{
 public:
    /// Represents a chunk and its replication state
    class Item
    {
        public:
        Item(InstanceID instanceId,
             const boost::shared_ptr<MessageDesc>& chunkMsg,
             const boost::shared_ptr<Query>& query) :
        _instanceId(instanceId), _chunkMsg(chunkMsg), _query(query), _isDone(false)
        {
            assert(instanceId != INVALID_INSTANCE);
            assert(chunkMsg);
            assert(query);
        }
        virtual ~Item() {}
        boost::weak_ptr<Query> getQuery() { return _query; }
        InstanceID getInstanceId() { return _instanceId; }
        boost::shared_ptr<MessageDesc> getChunkMsg() { return _chunkMsg; }
        /**
         * @return true if the chunk has been sent to the network manager or an error has occurred
         */
        bool isDone() { return _isDone; }
        /**
         * @throw if the replication failed
         * @return true if the chunk has been sent
         */
        bool validate(bool raise=true)
        {
            if (_error) {
                assert(_isDone);
                if (raise) {
                    _error->raise();
                }
                return false;
            }
            return true;
        }

        private:
        void setDone()
        {
            _isDone = true;
            _chunkMsg.reset();
        }

        void setDone(const boost::shared_ptr<scidb::Exception>& e)
        {
            setDone();
            _error = e;
        }

        friend class ReplicationManager;
        Item(const Item&);
        Item& operator=(const Item&);

        InstanceID _instanceId;
        boost::shared_ptr<MessageDesc> _chunkMsg;
        boost::weak_ptr<Query> _query;
        bool _isDone;
        boost::shared_ptr<scidb::Exception> _error;
    };

 private:
    typedef std::deque<boost::shared_ptr<Item> > RepItems;
    // XXX TODO: convert this to a set
    typedef std::map<InstanceID, boost::shared_ptr<RepItems> > RepQueue;
 public:
    ReplicationManager() {}
    virtual ~ReplicationManager() {}
    /// start the operations
    void start(const boost::shared_ptr<JobQueue>& jobQueue);
    /// stop the operations and release resources
    void stop();
    /// replicate an item
    void send(const boost::shared_ptr<Item>& item);
    /// wait until the item is sent to network manager
    void wait(const boost::shared_ptr<Item>& item);
    /// discard the item 
    void abort(const boost::shared_ptr<Item>& item)
    {
        assert(item);
        ScopedMutexLock cs(_repMutex);
        if (item->isDone()) {
            return;
        }
        item->setDone(SYSTEM_EXCEPTION_SPTR(SCIDB_SE_INTERNAL,SCIDB_LE_UNKNOWN_ERROR));
    }

    bool isStarted()
    {
        ScopedMutexLock cs(_repMutex);
        return bool(_lsnrId);
    }

    /// Reserve space on the inbound replication queue and get a
    /// work item which will schedule a replication job on the inbound replication queue
    /// @param job replication job
    /// @return WorkQueue::WorkItem for running on any queue
    /// @throws WorkQueue::OverflowException
    WorkQueue::WorkItem getInboundReplicationItem(boost::shared_ptr<Job>& job)
    {
        // no synchronization is needed because _inboundReplicationQ
        // is initialized at start() time and is never changed later
        assert(isStarted());
        assert(_inboundReplicationQ);
        assert(job);
        boost::shared_ptr<Reservation> res = boost::make_shared<Reservation>(_inboundReplicationQ, job);

        WorkQueue::WorkItem item = boost::bind(&Reservation::enqueue, res, _1, _2);
        return item;
    }

 private:

    /// Class to manage space reservation and enqueing on the inbound replication queue
    class Reservation
    {
    public:
        /// Constructor reserves space on a queue
        /// @param queue for reservation
        /// @param job to schedule on the queue
        Reservation(const boost::shared_ptr<WorkQueue>& queue,
                    const boost::shared_ptr<Job>& job)
        : _queue(queue), _job(job)
        {
            assert(queue);
            assert(job);
            queue->reserve();
        }
        /// Destructor unreserves space on the queue if the job was not enqueued
        ~Reservation()
        {
            if ( boost::shared_ptr<WorkQueue> q = _queue.lock()) {
                q->unreserve();
            }
        }
        /// Eqnqueue the job onto the queue
        /// @param fromQueue is the queue invoking this method
        /// @param sCtx serialization context 
        /// @see scidb::WorkQueue
        void enqueue(boost::weak_ptr<scidb::WorkQueue>& fromQueue,
                     boost::shared_ptr<SerializationCtx>& sCtx)
        {
            if ( boost::shared_ptr<WorkQueue> q = _queue.lock()) {
                // sCtx is ignored because _inboundReplicationQ is single-threaded,
                // and there is no need to hold up fromQueue,
                // which just transfers its jobs to _inboundReplicationQ.
                boost::shared_ptr<SerializationCtx> emptySCtx;
                WorkQueue::scheduleReserved(_job, q, emptySCtx);
            }
            _queue.reset();
        }
    private:
        boost::weak_ptr<WorkQueue> _queue;
        boost::shared_ptr<Job> _job;
    };

    void handleConnectionStatus(Notification<NetworkManager::ConnectionStatus>::MessageTypePtr connStatus);
    bool sendItem(RepItems& ri);
    void clear();
    static bool checkItemState(const boost::shared_ptr<Item>& item)
    {
        // _repMutex must be locked
        assert(item);
        if (item->isDone()) {
            return false;
        }
        return Query::getValidQueryPtr(item->getQuery());
    }

    ReplicationManager(const ReplicationManager&);
    ReplicationManager& operator=(const ReplicationManager&);

    RepQueue _repQueue;
    Mutex    _repMutex;
    Event    _repEvent;
    Notification<NetworkManager::ConnectionStatus>::ListenerID _lsnrId;

    boost::shared_ptr<WorkQueue> _inboundReplicationQ;
};
}
#endif
