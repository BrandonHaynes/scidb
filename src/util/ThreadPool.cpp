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
 * @file ThreadPool.cpp
 *
 * @author roman.simakov@gmail.com
 *
 * @brief The ThreadPool class
 */
#include "util/Thread.h"
#include "system/Config.h"
#include "system/SciDBConfigOptions.h"

namespace scidb
{

ThreadPool::ThreadPool(size_t threadCount, boost::shared_ptr<JobQueue> queue)
: _queue(queue),
  _currentJobs(threadCount),
  _threadCount(threadCount),
  _terminatedThreads(boost::make_shared<Semaphore>())
{
    _shutdown = false;
    if (_threadCount <= 0) {
        throw InvalidArgumentException(REL_FILE, __FUNCTION__, __LINE__)
        << "thread count";
    }
}

void ThreadPool::start()
{
    ScopedMutexLock lock(_mutex);

    if (_shutdown) {
        throw AlreadyStoppedException(REL_FILE, __FUNCTION__, __LINE__)
        << "thread pool cannot be started after being stopped";
    }
    if (_threads.size() > 0) {
        throw AlreadyStartedException(REL_FILE, __FUNCTION__, __LINE__)
        << "thread pool can be started only once";
    }
    assert(_threadCount>0);

    _threads.reserve(_threadCount);
    for (size_t i = 0; i < _threadCount; i++)
    {
        boost::shared_ptr<Thread> thread(new Thread(*this, i));
        _threads.push_back(thread);
        thread->start();
        getInjectedErrorListener().check();
    }
}

bool ThreadPool::isStarted()
{
    ScopedMutexLock lock(_mutex);
    return _threads.size() > 0;
}

class FakeJob : public Job
{
public:
    FakeJob(): Job(boost::shared_ptr<Query>()) {
    }

    virtual void run()
    {
    }
};

void ThreadPool::stop()
{
    std::vector<boost::shared_ptr<Thread> > threads;
    { // scope
        ScopedMutexLock lock(_mutex);
        if (_shutdown) {
            return;
        }
        threads.swap(_threads);
        _shutdown = true;
    }
    size_t nThreads = threads.size();
    for (size_t i = 0; i < threads.size(); ++i) {
        if (threads[i]->isStarted()) {
            _queue->pushJob(boost::shared_ptr<Job>(new FakeJob()));
        } else {
            --nThreads;
        }
    }
    _terminatedThreads->enter(nThreads);
}
InjectedErrorListener<ThreadStartInjectedError> ThreadPool::s_injectedErrorListener;

} //namespace
