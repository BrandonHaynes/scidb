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
 * @file Event.h
 *
 * @author knizhnik@garret.ru, roman.simakov@gmail.com
 *
 * @brief POSIX conditional variable
 */

#ifndef EVENT_H_
#define EVENT_H_

#include <stdio.h>
#include <time.h>
#include <sys/time.h>
#include <errno.h>

#include "util/Mutex.h"
#include "system/Exceptions.h"
#include "boost/function.hpp"

namespace scidb
{

class Event
{
private:
    pthread_cond_t  _cond;
    bool signaled;

public:
    /**
     * @throws a scidb::Exception if necessary
     */
    typedef boost::function<bool()> ErrorChecker;

    Event()
    {
        if (pthread_cond_init(&_cond, NULL)) {
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_OPERATION_FAILED) << "pthread_cond_init";
        }
        signaled = false;
    }

    ~Event()
    {
        if (pthread_cond_destroy(&_cond)) {
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_OPERATION_FAILED) << "pthread_cond_destroy";
        }
    }

    /**
     * Wait for the Event to become signalled (based on the POSIX (timed) conditional variable+mutex).
     * If the event is signalled before the call to wait,
     * the wait will never return. The signal will cause the wait()'s of *all* threads to return.
     * @param cs associated with this event, the same mutex has to be used the corresponding wait/signal
     * @param errorChecker if set, it will be invoked periodically and
     *                     the false return code will force this wait() to return regardless of whether the signal is received.
     * @note The errorChecker must also check for the condition predicate for which this Event is used because of the unavoidable
     *       race condition between the timer expiring (in pthread_cond_timedwait) and another thread signalling.
     */

    bool wait(Mutex& cs, ErrorChecker& errorChecker)
    {
        cs.checkForDeadlock();
        if (errorChecker) {
            if (!errorChecker()) {
               return false;
            }
           
            signaled = false;
            do {
                struct timespec ts;
                if (clock_gettime(CLOCK_REALTIME, &ts) == -1) {
                    assert(false);
                    throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_CANT_GET_SYSTEM_TIME);
                }
                ts.tv_sec = ts.tv_sec + 10;
                const int e = pthread_cond_timedwait(&_cond, &cs._mutex, &ts);
                if (e == 0) {
                    return true;
                }
                if (e != ETIMEDOUT) 
                {
                    assert(false);
                    throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_THREAD_EVENT_ERROR) << e;
                }
                if (!errorChecker()) {
                   return false;
                }
            } while (!signaled);
        }
        else
        {
            if (pthread_cond_wait(&_cond, &cs._mutex)) {
                assert(false);
                throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_OPERATION_FAILED) << "pthread_cond_wait";
            }
        }
        return true;
    }

    void signal()
    { 
        signaled = true;
        if (pthread_cond_broadcast(&_cond)) {
            assert(false);
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_OPERATION_FAILED) << "pthread_cond_broadcast";
        }
    }
};

}

#endif
