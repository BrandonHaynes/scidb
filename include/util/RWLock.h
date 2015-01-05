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
 * @file RWLock.h
 *
 * @author roman.simakov@gmail.com
 *
 * @brief Read-Write lock implementation.
 *
 * Allow to lock in one thread and unlock in another.
 *
 */

#ifndef RWLOCK_H_
#define RWLOCK_H_

#include <pthread.h>
#include "system/Exceptions.h"
#include "Event.h"

namespace scidb
{

class RWLock
{
private:
    Mutex _mutex;

    int _nested;
    int _readers;
    Event _noReaders;

    int _pendingWriters;
    pthread_t _currentWriter;
    Event _noWriter;

    RWLock(const RWLock&);

public:
    /**
     * @throws a scidb::Exception if necessary
     * @return false if an error is detected
     */
    typedef boost::function<bool()> ErrorChecker;
    
    RWLock(): _nested(0), _readers(0), _pendingWriters(0), _currentWriter(0)
    {
    }

    bool lockRead(ErrorChecker& errorChecker)
    {
        ScopedMutexLock mutexLock(_mutex);

        if (_currentWriter == pthread_self()) { 
            _nested += 1;
        } else { 
            while (_pendingWriters || _currentWriter) {
                if (!_noWriter.wait(_mutex, errorChecker)) { 
                    return false;
                }
            }
            assert(!_currentWriter);
            ++_readers;
        }
        return true;
    }

    void unLockRead()
    {
        ScopedMutexLock mutexLock(_mutex);

        if (_nested != 0) { 
            _nested -= 1;
        } else {
            --_readers;
            assert(_readers >= 0);
            
            if (!_readers) {
                _noReaders.signal();
            }
        }
    }

    struct PendingWriter 
    {
        RWLock& lock;
        PendingWriter(RWLock& rwlock) : lock(rwlock) 
        {
            lock._pendingWriters += 1;
        }
        ~PendingWriter() {
            lock._pendingWriters -= 1;
            if (lock._currentWriter == 0) { 
                lock._noWriter.signal();
            }
        }
    };

    bool lockWrite(ErrorChecker& errorChecker)
    {
        ScopedMutexLock mutexLock(_mutex);

        if (_currentWriter == pthread_self()) { 
            _nested += 1;
        } else { 
            PendingWriter writer(*this);
            
            while (_readers > 0) {
                if (!_noReaders.wait(_mutex, errorChecker)) { 
                    return false;
                }
            }
            
            while (_currentWriter) {
                if (!_noWriter.wait(_mutex, errorChecker)) { 
                    return false;
                }
            }
            
            assert(_pendingWriters > 0);
            assert(!_readers);
            _currentWriter = pthread_self();
        }
        return true;
    }

    void unLockWrite()
    {
        ScopedMutexLock mutexLock(_mutex);

        if (_nested != 0) { 
            _nested -= 1;
        } else {
            _currentWriter = (pthread_t)0;
            _noWriter.signal();
        }
    }
   
    int getNumberOfReaders() const
    {
        return _readers;
    }

    void unLock()
    {
        ScopedMutexLock mutexLock(_mutex);

        if (_nested != 0) { 
            _nested -= 1;
        } else {
            if (_readers > 0) { 
                if (--_readers == 0) { 
                    _noReaders.signal();
                } 
            } else { 
                _currentWriter = (pthread_t)0;
                _noWriter.signal();
            }
        }
    }
};

class ScopedRWLockRead
{
private:
    RWLock& _rwLock;
    bool locked;
public:
    ScopedRWLockRead(RWLock& rwLock, RWLock::ErrorChecker& errorChecker)
    : _rwLock(rwLock), locked(false)
    {
        locked = _rwLock.lockRead(errorChecker);
    }
    bool isLocked()
    {
       return locked;
    }
    ~ScopedRWLockRead()
    {
        if (locked) { 
            _rwLock.unLockRead();
        }
    }
};

class ScopedRWLockWrite
{
private:
    RWLock& _rwLock;
    bool locked;

public:
    ScopedRWLockWrite(RWLock& rwLock, RWLock::ErrorChecker& errorChecker)
    : _rwLock(rwLock), locked(false)
    {
        locked = _rwLock.lockWrite(errorChecker);
    }
    bool isLocked()
    {
       return locked;
    }
    ~ScopedRWLockWrite()
    {
        if (locked) { 
            _rwLock.unLockWrite();
        }
    }
};

class ScopedRWLock
{
private:
    RWLock& _rwLock;
    bool _writeAccess;
    bool locked;
public:
    ScopedRWLock(RWLock& rwLock, bool writeAccess, RWLock::ErrorChecker& errorChecker)
    : _rwLock(rwLock), _writeAccess(writeAccess), locked(false)
    {
        locked = writeAccess ? _rwLock.lockWrite(errorChecker) : _rwLock.lockRead(errorChecker);
    }
    bool isLocked()
    {
       return locked;
    }
    void disarm()
    {
       locked = false;
    }
    ~ScopedRWLock()
    {
        if (locked) { 
            if (_writeAccess) { 
                _rwLock.unLockWrite();
            } else { 
                _rwLock.unLockRead();
            }
        }
    }
};

} // namespace

#endif
