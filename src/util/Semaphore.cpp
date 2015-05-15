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
 * @file Semaphore.cpp
 *
 * @author roman.simakov@gmail.com
 *
 * @brief The Semaphore class
 */

#include "assert.h"
#include <stdio.h>
#include <string>
#include <sstream>
#include <system/Exceptions.h>
#include <util/Semaphore.h>


namespace scidb
{

#ifdef POSIX_SEMAPHORES

Semaphore::Semaphore()
{
	if (sem_init(_sem, 0, 0) == -1)
	{
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_SEMAPHORE_ERROR)
                << "initialize" << ::strerror(errno) << errno;
	}
}

Semaphore::~Semaphore()
{
	if (sem_destroy(_sem) == -1)
	{
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_SEMAPHORE_ERROR)
                << "destroy" << ::strerror(errno) << errno;
	}
}

void Semaphore::enter()
{
    do
    {
        if (sem_wait(_sem) == 0)
            return;
    } while (errno == EINTR);

    throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_SEMAPHORE_ERROR)
        << "wait" << ::strerror(errno) << errno;
}

bool Semaphore::enter(ErrorChecker& errorChecker)
{
    if (errorChecker && !errorChecker()) {
        return false;
    }
    const time_t TIMEOUT_SEC = 10;

    timespec ts;
    if (clock_gettime(CLOCK_REALTIME, &ts) == -1) {
        assert(false);
        throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_CANT_GET_SYSTEM_TIME);
    }
    ts.tv_sec += TIMEOUT_SEC;

    while (true)
    {
        if (sem_timedwait(_sem, &ts) == 0) {
            return true;
        }
        if (errno == EINTR) {
            continue;
        }
        if (errno != ETIMEDOUT) {
            assert(false);
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_SEMAPHORE_ERROR)
                << "timedwait" << ::strerror(errno) << errno;
        }
        if (errorChecker && !errorChecker()) {
           return false;
        }
        if (clock_gettime(CLOCK_REALTIME, &ts) == -1) {
            assert(false);
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_CANT_GET_SYSTEM_TIME);
        }
        ts.tv_sec += TIMEOUT_SEC;
    }
}

bool Semaphore::tryEnter()
{
	// Instant try
	do
	{
		if (sem_trywait(_sem) != -1)
			return true;
	} while (errno == EINTR);
	
	if (errno == EAGAIN)
		return false;

        throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_SEMAPHORE_ERROR)
            << "trywait" << ::strerror(errno) << errno;

	return false;	// Shutdown warning
}

#else

Semaphore::Semaphore(): _count(0)
{
}

Semaphore::~Semaphore()
{ 
}


bool Semaphore::enter(int n, ErrorChecker& errorChecker)
{
    ScopedMutexLock mutexLock(_cs);
    while (_count < n) {
        // take what is possible currently to prevent infinity waiting
        n -= _count;
        _count = 0;
        // wait for new releases
        if (!_cond.wait(_cs, errorChecker)) { 
            return false;
        }
    }
    _count -= n;
    return true;
}

void Semaphore::enter(int n)
{
    ScopedMutexLock mutexLock(_cs);
    while (_count < n) {
        ErrorChecker errorChecker;
        // take what is possible currently to prevent infinity waiting
        n -= _count;
        _count = 0;
        // wait for new releases
        _cond.wait(_cs, errorChecker);
    }
    _count -= n;
}

void Semaphore::release(int n)
{
    ScopedMutexLock mutexLock(_cs);
    _count += n;
    _cond.signal();
}

bool Semaphore::tryEnter()
{
    ScopedMutexLock mutexLock(_cs);
    if (_count > 0) {
        _count--;
        return true;
    }
    return false;
}

#endif

} //namespace
