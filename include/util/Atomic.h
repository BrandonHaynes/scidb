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
 * @file Atomic.h
 *
 * @author roman.simakov@gmail.com
 *
 * @brief The Atomic class for changing values in thread safe mode
 */

#ifndef ATOMIC_H
#define ATOMIC_H

#include "Mutex.h"

namespace scidb
{

template<typename T> class Atomic
{
private:
    T _value;
    mutable Mutex _mutex;

public:
    Atomic() {
    }

    Atomic(const T& value)
    {
        ScopedMutexLock lock(_mutex);
        _value = value;
    }

    void operator=(const T& value)
    {
        ScopedMutexLock lock(_mutex);
        _value = value;
    }

    operator T() const
    {
        ScopedMutexLock lock(_mutex);
        return _value;
    }

    bool testAndSet (const T& before, const T& after)
    {
        ScopedMutexLock lock(_mutex);
        if (_value == before) {
           _value = after;
           return true;
        }
        return false;
    }
};


} // namespace

#endif // ATOMIC_H
