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
 * @file Mutex.h
 *
 * @author roman.simakov@gmail.com
 *
 * @brief The Mutex class for synchronization
 */

#ifndef MUTEX_H_
#define MUTEX_H_

#include "assert.h"
#include <stdlib.h>
#include <pthread.h>

#include "system/Exceptions.h"


namespace scidb
{

class Event;

class Mutex
{
friend class Event;
friend class Semaphore;
private:
 class PAttrEraser
 {
 public:
 PAttrEraser(pthread_mutexattr_t *attrPtr) : _attrPtr(attrPtr)
     {
         assert(_attrPtr!=NULL);
     }
     ~PAttrEraser()
     {
         pthread_mutexattr_destroy(_attrPtr);
     }
 private:
     pthread_mutexattr_t *_attrPtr;
 };

    pthread_mutex_t _mutex;

  public:
    void checkForDeadlock() { 
        assert(_mutex.__data.__count == 1);
    }
    
    Mutex()
    {
        pthread_mutexattr_t __attr;
        if (pthread_mutexattr_init(&__attr)) {
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_OPERATION_FAILED) << "pthread_mutexattr_init";
        }
        PAttrEraser onStack(&__attr);

        if (pthread_mutexattr_settype(&__attr, PTHREAD_MUTEX_RECURSIVE)) {
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_OPERATION_FAILED) << "pthread_mutexattr_settype";
        }
        if (pthread_mutex_init(&_mutex, &__attr)) {
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_OPERATION_FAILED) << "pthread_mutex_init";
        }
    }

    ~Mutex()
    {
        if (pthread_mutex_destroy(&_mutex)) {
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_OPERATION_FAILED) << "pthread_mutex_destroy";
        }
    }

    void lock()
    {
        if (pthread_mutex_lock(&_mutex)) {
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_OPERATION_FAILED) << "pthread_mutex_lock";
        }
    }

    void unlock()
    {
        if (pthread_mutex_unlock(&_mutex)) {
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_OPERATION_FAILED) << "pthread_mutex_unlock";
        }
    }
};


/***
 * RAII class for holding Mutex in object visible scope.
 */
class ScopedMutexLock
{
private:
	Mutex& _mutex;

public:
	ScopedMutexLock(Mutex& mutex): _mutex(mutex)
	{
		_mutex.lock();
	}
	
	~ScopedMutexLock()
	{
		_mutex.unlock();
	}
};


} //namespace

#endif /* MUTEX_H_ */
