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
 * @file Thread.h
 *
 * @author roman.simakov@gmail.com
 *
 * @brief The Thread class for executing jobs from queue
 */

#ifndef THREAD_H_
#define THREAD_H_

#include <assert.h>
#include <pthread.h>
#include <signal.h>

#include "Job.h"
#include "JobQueue.h"
#include "ThreadPool.h"


namespace scidb
{

extern "C" typedef void *(*pthread_callback)(void *);


class Thread
{
private:
	pthread_t _handle;
        ThreadPool& _threadPool;
        size_t _index;
        boost::shared_ptr<Job> _currentJob;
        bool _isStarted;

	Thread(const Thread&);
	void operator= (const Thread&);
        static void* threadFunction(void* arg);
	void _threadFunction();

public:

        Thread(ThreadPool& threadPool, size_t index);
        void start();
        bool isStarted();
        virtual ~Thread();
        /**
         * Put this thread to sleep
         * @param nanoSec number of nanoseconds to sleep
         */
        static void nanoSleep(uint64_t nanoSec);
};


/// @return current time in nanoseconds
uint64_t getTimeInNanoSecs();
/// @return return (getTimeInNanoSecs() - startTimeNanoSec >= timeoutNanoSec && timeoutNanoSec > 0)
bool hasExpired(uint64_t startTimeNanoSec, uint64_t timeoutNanoSec);

/**
 * class Functor_tt
 * {
 *  operator() ();
 *  clear();
 * }
 */
template<class Functor_tt>
class Destructor
{
 public:
    Destructor(Functor_tt& w) : _work(w)
    {
    }
    ~Destructor()
    {
        if (_work) {
            _work();
        }
    }
    void disarm()
    {
        _work.clear();
   }
 private:

    Functor_tt _work;
};

} //namespace

#endif /* THREAD_H_ */
