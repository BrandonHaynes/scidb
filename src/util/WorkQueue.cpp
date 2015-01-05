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
 * @file WorkQueue.cpp
 *
 * @author roman.simakov@gmail.com
 *
 * @brief The WorkQueue class
 */

#include <log4cxx/logger.h>
#include <util/Job.h>
#include <util/JobQueue.h>
#include <query/Query.h>
#include <util/WorkQueue.h>

static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.common.thread"));

namespace scidb
{


SerializationCtx::~SerializationCtx()
{
    // no lock must be held
    for (QList::reverse_iterator ri=_queuesToRelease.rbegin();
         ri != _queuesToRelease.rend(); ++ri) {
        boost::shared_ptr<scidb::WorkQueue> wq = (*ri).lock();
        if (wq) {
            wq->release();
        }
    }
}

void SerializationCtx::record(boost::weak_ptr<scidb::WorkQueue>& wq)
{
    _queuesToRelease.push_back(wq);
    assert(_queuesToRelease.size() < MAX_QUEUES);
}

WorkQueue::WorkQueue(const boost::shared_ptr<JobQueue>& jobQueue)
: _jobQueue(jobQueue),
  _maxOutstanding(DEFAULT_MAX_OUTSTANDING),
  _maxSize(DEFAULT_MAX_SIZE),
  _outstanding(0),
  _reserved(0),
  _isStarted(true)
{
    if (!jobQueue) {
        throw (SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_INVALID_FUNCTION_ARGUMENT) << "NULL job queue");
    }
}

WorkQueue::WorkQueue(const boost::shared_ptr<JobQueue>& jobQueue,
                     uint32_t maxOutstanding)
: _jobQueue(jobQueue),
  _maxOutstanding(maxOutstanding),
  _maxSize(DEFAULT_MAX_SIZE),
  _outstanding(0),
  _reserved(0),
  _isStarted(true)
{
    if (!jobQueue) {
        throw (SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_INVALID_FUNCTION_ARGUMENT) << "NULL job queue");
    }
}

WorkQueue::WorkQueue(const boost::shared_ptr<JobQueue>& jobQueue,
                     uint32_t maxOutstanding,
             uint32_t maxSize)
: _jobQueue(jobQueue),
  _maxOutstanding(maxOutstanding),
  _maxSize(maxSize),
  _outstanding(0),
  _reserved(0),
  _isStarted(true)
{
    if (!jobQueue) {
        throw (SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_INVALID_FUNCTION_ARGUMENT) << "NULL job queue");
    }
}

void WorkQueue::spawn()
{
    std::deque<InternalWorkItem> q;
    std::deque<boost::weak_ptr<WorkQueue> > wq;
    {
        ScopedMutexLock lock(_mutex);

        assert(_outstanding <= _maxOutstanding);

        if (!_isStarted) {
            return;
        }

        // try to transfer the overflow items into _workQueue
        while(_size() < _maxSize && !_overflowQueue.empty()) {
            _workQueue.push_back(InternalWorkItem());
            _workQueue.back().swap(_overflowQueue.front().first);
            wq.push_back(boost::weak_ptr<WorkQueue>());
            wq.back().swap(_overflowQueue.front().second);
            _overflowQueue.pop_front();
        }
        // find items to spawn
        while ((_outstanding < _maxOutstanding) && !_workQueue.empty()) {
            q.push_back(InternalWorkItem());
            q.back().swap(_workQueue.front());
            _workQueue.pop_front();
            ++_outstanding;
        }
    }
    // release any queue waiting
    for (std::deque<boost::weak_ptr<WorkQueue> >::iterator i=wq.begin();
         i!=wq.end(); ++i) {
        boost::shared_ptr<WorkQueue> q = (*i).lock();
        if (q) {
            q->release();
        }
    }
    // spawn more items
    for (std::deque<InternalWorkItem>::iterator i=q.begin();
         i!=q.end(); ++i) {
        InternalWorkItem& item = *i;
        boost::shared_ptr<Job> jobPtr(new WorkQueueJob(item, shared_from_this()));
        _jobQueue->pushJob(jobPtr);
    }
}

void WorkQueue::reEnqueueSerialized(WorkItem& work,
                                    boost::shared_ptr<WorkQueue>& fromQueue,
                                    boost::shared_ptr<SerializationCtx>& sCtx )
{
       assert(work);
       if (!fromQueue) {
           assert(false);
           throw (SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_INVALID_FUNCTION_ARGUMENT) << "NULL fromQueue");
       }
       if (!sCtx) {
           assert(false);
           throw (SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_INVALID_FUNCTION_ARGUMENT) << "NULL serialization ctx");
       }

       // Because the work item is serialized, it can only be released (by fromQueue)
       // when the serialization context is destroyed.
       const bool isSameQueue = (this == fromQueue.get());
       if (!isSameQueue) {
           boost::weak_ptr<WorkQueue> fromQ(fromQueue);
           sCtx->record(fromQ);
       }

       // In case of overflow, we record emptyFromQueue in _overflowQueue
       // because we dont want to let fromQueue release the work item prematurely
       // (the release normally happens when an item is transfered from _overflowQueue to _workQueue).
       boost::shared_ptr<WorkQueue> emptyFromQueue;
       reEnqueueInternal(work, emptyFromQueue, sCtx, isSameQueue);

       if (!isSameQueue) {
           // dont release the item from the current queue
           throw PushBackException();
       }
}

void WorkQueue::reEnqueue(WorkItem& work, boost::shared_ptr<WorkQueue>& fromQueue)
{
    assert(work);

    if (!fromQueue) {
        assert(false);
        throw (SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_INVALID_FUNCTION_ARGUMENT) << "NULL fromQueue");
    }
    boost::shared_ptr<SerializationCtx> sCtx = boost::make_shared<SerializationCtx>();
    const bool isSameQueue = (this == fromQueue.get());

    reEnqueueInternal(work, fromQueue, sCtx, isSameQueue);
}

void WorkQueue::reEnqueueInternal(WorkItem& work,
                                  boost::shared_ptr<WorkQueue>& fromQueue,
                                  boost::shared_ptr<SerializationCtx>& sCtx,
                                  bool isSameQueue)
{
    InternalWorkItem item = boost::bind(&invokeWithContext, work, sCtx, _1);
    {
        ScopedMutexLock lock(_mutex);

        assert(!isSameQueue || _outstanding>0);

        if ((_size()+1)<=_maxSize) {
            _workQueue.push_back(item);

        } else  // overflow
        if (isSameQueue) {
            // we are reinserting into the same WorkQueue, which is full
            _workQueue.push_back(item);
            assert(_outstanding>0);
            assert(_outstanding <= _maxOutstanding);
            --_outstanding;

            assert(_size() <= (_maxSize+_outstanding));
            throw PushBackException();
        } else {
            // Insert the work item into the overflow queue but dont let fromQueue release it right away.
            std::pair<InternalWorkItem,boost::weak_ptr<WorkQueue> > fromQCtx(item, fromQueue);
            _overflowQueue.push_back(fromQCtx);
            throw PushBackException();
        }
    }
    spawn();
}


void WorkQueue::enqueueReserved(WorkItem& work,
                                boost::shared_ptr<SerializationCtx>& sCtx)
{
    static const char *funcName="WorkQueue::enqueueReserved: ";
    {
        ScopedMutexLock lock(_mutex);

        LOG4CXX_TRACE(logger, funcName << "to queue="
                      << this
                      << ", sCtx="<<sCtx.get()
                      << ", size="<<_size());

        assert(_size() <= (_maxSize+_outstanding));
        if (_reserved<=0) {
            assert(false);
            throw InvalidStateException(REL_FILE, __FUNCTION__, __LINE__);
        }
        if (!sCtx) {
            sCtx = boost::make_shared<SerializationCtx>();
        }
        InternalWorkItem item = boost::bind(&invokeWithContext, work, sCtx, _1);
        _workQueue.push_back(item);

        --_reserved;
        assert(_size() <= (_maxSize+_outstanding));
    }
    spawn();
}



void WorkQueue::transfer(boost::shared_ptr<Job>& job,
                         boost::shared_ptr<WorkQueue>& toQueue,
                         boost::weak_ptr<WorkQueue>& fromQueue,
                         boost::shared_ptr<SerializationCtx>& sCtx)
{
    static const char *funcName="WorkQueue::transfer: ";
    assert(job);
    assert(toQueue);
    LOG4CXX_TRACE(logger, funcName << "to queue="
                  << toQueue.get()<< " of size="
                  << toQueue->size());

    boost::shared_ptr<WorkQueue> fromQ = fromQueue.lock();

    WorkQueue::WorkItem item = boost::bind(&Job::executeOnQueue, job, _1, _2);
    if (!fromQ) {
        try {
            toQueue->enqueue(item);
        } catch (const WorkQueue::OverflowException& e) {

            boost::shared_ptr<Query> query(job->getQuery());
            assert(query);
            query->handleError(e.copy());
            LOG4CXX_ERROR(logger, funcName <<
                          "Overflow exception from "
                          <<" queue=" << toQueue.get()
                          <<", job="<<job.get()
                          <<", queryID="<<query->getQueryID()
                          <<" : "<<e.what());
            // XXX TODO: deal with this exception if necessary
            assert(false);
            throw;
        }
    } else if (sCtx) {
        toQueue->reEnqueueSerialized(item, fromQ, sCtx);
    } else {
        toQueue->reEnqueue(item, fromQ);
    }
}


void WorkQueue::scheduleReserved(boost::shared_ptr<Job>& job,
                                 boost::shared_ptr<WorkQueue>& toQueue,
                                 boost::shared_ptr<SerializationCtx>& sCtx)
{
    static const char *funcName="WorkQueue::scheduleReserved: ";
    assert(job);
    assert(toQueue);

    LOG4CXX_TRACE(logger, funcName << "to queue="
                  << toQueue.get()<< " of size="
                  << toQueue->size());
    try {
        WorkQueue::WorkItem item = boost::bind(&Job::executeOnQueue, job, _1, _2);
        toQueue->enqueueReserved(item, sCtx);
    } catch (const Exception& e) {
        toQueue->unreserve();

        boost::shared_ptr<Query> query(job->getQuery());
        assert(query);
        query->handleError(e.copy());
        LOG4CXX_ERROR(logger, funcName <<
                      "Exception from "
                      <<" queue=" << toQueue.get()
                      <<", job="<<job.get()
                      <<", queryID="<<query->getQueryID()
                      <<" : "<<e.what());
        assert(false);
    }
}

} //namespace
