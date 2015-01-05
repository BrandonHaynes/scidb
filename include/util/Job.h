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
 * @file Job.h
 *
 * @author roman.simakov@gmail.com
 *
 * @brief The base class for jobs of thread pool
 */

#ifndef JOB_H_
#define JOB_H_

#include <string>
#include <vector>

#include <boost/shared_ptr.hpp>
#include <boost/weak_ptr.hpp>
#include <util/Atomic.h>
#include <util/Semaphore.h>

namespace scidb
{

/**
 * Base virtual class for job executed by ThreadPool.
 */
class Query;
class WorkQueue;
class SerializationCtx;

class Job
{
private:
    Semaphore _done;
    Atomic<bool> _removed;

 protected:
    typedef boost::function<void()> Handler;
    boost::shared_ptr<Exception> _error;
    boost::shared_ptr<Query> _query; //XXX TODO: make it a weak_ptr ?

    // When a job is executed multiple times using executeOnQueue(),
    // _wq, _wqSCtx, _currHandler need to be set accordingly
    // _wq, _wqSCtx are set by the WorkQueue invoking executeOnQueue()
    // _currHandler must be set by the job algorithm prior
    // to scheduling the next invocation of executeOnQueue()
   
    boost::weak_ptr<WorkQueue> _wq;
    boost::weak_ptr<SerializationCtx> _wqSCtx;
    Handler _currHandler;

    /// This method must be implemented in child classes
    /// It gets invoked by Job::execute() when this job is executed directly on a JobQueue or
    /// by Job::executeOnQueue() when this job is executed on a WorkQueue
    virtual void run() = 0;

public:
    Job(boost::shared_ptr<Query> query)
    : _removed(false),
      _query(query)
    {
    }
    virtual ~Job()
    {
    }
    boost::shared_ptr<Query> getQuery()
    {
        return _query;
    }

    /**
     * The (pool) threads servicing this Job's JobQueue call this method
     */
    void execute();

    /**
     * If this job is enqueued onto a WorkQueue in a form of a WorkItem,
     * this method is called. A given job can be executed multiple times
     * (presumable to execute different steps of an algorithm) using this method.
     * @param wq the WorkQueue executing this job
     * @throw WorkQueue::PushBackException if this job is re-enqueued onto another under the overflow condition
     * @see WorkQueue::PushBackException
     */
    void executeOnQueue(boost::weak_ptr<WorkQueue>& wq,
                        boost::shared_ptr<SerializationCtx>& sCtx);

    /// Waits until job is done
    bool wait(bool propagateException = false, bool allowMultipleWaits = true);

    /// Force to skip job execution
    void skip()
    {
        _removed = true;
    }

    void rethrow();
};


} //namespace

#endif /* JOB_H_ */
