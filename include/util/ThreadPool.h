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
 * @file ThreadPool.h
 *
 * @author roman.simakov@gmail.com
 *
 * @brief The ThreadPool class
 */

#ifndef THREADPOOL_H_
#define THREADPOOL_H_

#include <vector>

#include "Semaphore.h"
#include "JobQueue.h"
#include "system/Sysinfo.h"
#include <util/InjectedError.h>

namespace scidb
{

class Thread;

/**
 * Pool of threads. Process jobs from queue.
 *
 */
class ThreadPool : public InjectedErrorListener<ThreadStartInjectedError>
{
friend class Thread;
private:
    std::vector<boost::shared_ptr<Thread> > _threads;
    boost::shared_ptr<JobQueue> _queue;
    Mutex _mutex;
    std::vector< boost::shared_ptr<Job> > _currentJobs;
    bool _shutdown;
    size_t _threadCount;
    boost::shared_ptr<Semaphore> _terminatedThreads;

public:

    class InvalidArgumentException: public SystemException
    {
    public:
      InvalidArgumentException(const char* file, const char* function, int32_t line)
      : SystemException(file, function, line, "scidb", SCIDB_SE_INTERNAL, SCIDB_LE_INVALID_FUNCTION_ARGUMENT,
                        "SCIDB_SE_INTERNAL", "SCIDB_LE_INVALID_FUNCTION_ARGUMENT", uint64_t(0))
      {
      }
       ~InvalidArgumentException() throw () {}
       void raise() const { throw *this; }
    };

    class AlreadyStoppedException: public SystemException
    {
    public:
      AlreadyStoppedException(const char* file, const char* function, int32_t line)
      : SystemException(file, function, line, "scidb", SCIDB_SE_INTERNAL, SCIDB_LE_UNKNOWN_ERROR,
                        "SCIDB_SE_INTERNAL", "SCIDB_LE_UNKNOWN_ERROR", uint64_t(0))
      {
      }
       ~AlreadyStoppedException() throw () {}
       void raise() const { throw *this; }
    };

    class AlreadyStartedException: public SystemException
    {
    public:
      AlreadyStartedException(const char* file, const char* function, int32_t line)
      : SystemException(file, function, line, "scidb", SCIDB_SE_INTERNAL, SCIDB_LE_UNKNOWN_ERROR,
                        "SCIDB_SE_INTERNAL", "SCIDB_LE_UNKNOWN_ERROR", uint64_t(0))
      {
      }
       ~AlreadyStartedException() throw () {}
       void raise() const { throw *this; }
    };
	/**
	 * Constructor of ThreadPool.
	 *
	 * @param threadCount a number of threads that will be
	 * process jobs from queue.
	 *
	 * @param queue an object of queue from which pool
	 * will be process jobs.
         * @throws scidb::ThreadPool::InvalidArgumentException if the threadCount <= 0
	 */
	ThreadPool(size_t threadCount, boost::shared_ptr<JobQueue> queue);

	/**
	 * Start the threads in the pool. It can be called only once.
     * @throws scidb::ThreadPool::AlreadyStoppedException if it has been stopped
     * @throws scidb::ThreadPool::AlreadyStartedException if it has been started
     */
    void start();

    /**
     * Try to force the threads to exit and wait for all of them to exit.
     */
	void stop();

    ~ThreadPool() { 
        stop();
    }

    boost::shared_ptr<JobQueue> getQueue() const
    {
        return _queue;
    }

    /**
     * @return true if start() was called in the lifetime of the object. False otherwise.
     */
    bool isStarted();

    static void startInjectedErrorListener()
    {
        s_injectedErrorListener.start();
    }

    static InjectedErrorListener<ThreadStartInjectedError>&
    getInjectedErrorListener()
    {
        return s_injectedErrorListener;
    }

 private:
    static InjectedErrorListener<ThreadStartInjectedError> s_injectedErrorListener;

};

} //namespace

#endif /* THREADPOOL_H_ */
