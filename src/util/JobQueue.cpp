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
 * @file JobQueue.cpp
 *
 * @author roman.simakov@gmail.com
 *
 * @brief The queue of jobs for execution in thread pool
 */

#include "util/JobQueue.h"
#include "util/Mutex.h"
#include <log4cxx/logger.h>

namespace scidb
{

static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.common.thread"));

JobQueue::JobQueue()
{

}

// Add new job to the end of queue
void JobQueue::pushJob(boost::shared_ptr<Job> job)
{
    { // scope
        ScopedMutexLock scopedMutexLock(_queueMutex);
        _queue.push_back(job);
        LOG4CXX_TRACE(logger, "JobQueue::pushJob: Q ("<<this<<") size = "<<getSize());
    }
    // We are releasing semaphore after unlocking mutex to
    // prevent unwanted _queueMutex sleeping in popJob.
    _queueSemaphore.release();
}

// Add new job to the end of queue
void JobQueue::pushHighPriorityJob(boost::shared_ptr<Job> job)
{
    { // scope
        ScopedMutexLock scopedMutexLock(_queueMutex);
        _queue.push_front(job);
        LOG4CXX_TRACE(logger, "JobQueue::pushHighPriorityJob: Q ("<<this<<") size = "<<getSize());
    }
    // We are releasing semaphore after unlocking mutex to
    // prevent unwanted _queueMutex sleeping in popJob.
    _queueSemaphore.release();
}


// Get next job from the beginning of the queue
// If there is next element the method waits
boost::shared_ptr<Job> JobQueue::popJob()
{
    _queueSemaphore.enter();
    { // scope
        ScopedMutexLock scopedMutexLock(_queueMutex);
        assert(!_queue.empty());

        boost::shared_ptr<Job> job = _queue.front();
        _queue.pop_front();
        LOG4CXX_TRACE(logger, "JobQueue::popJob: Q ("<<this<<") size = "<<getSize());
        return job;
    }
}


} // namespace
