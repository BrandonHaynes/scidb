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
 * @author knizhnik@garret.ru
 *
 * @brief Read-Write lock implementation.
 *
 * Allow to lock in one thread and unlock in another.
 *
 */

#ifndef LOCK_MANAGER_H_
#define LOCK_MANAGER_H_

#include <pthread.h>
#include "system/Exceptions.h"
#include <map>
#include <string>
#include "RWLock.h"
#include "Singleton.h"


namespace scidb
{

    using namespace std;
    
    class LockManager : public Singleton<LockManager>
    {
      private:
        Mutex mutex;
        map< string, shared_ptr<RWLock> > locks;

      public:
        shared_ptr<RWLock> getLock(string const& arrayName) 
        {
            ScopedMutexLock cs(mutex);
            shared_ptr<RWLock> lock = locks[arrayName];
            if (!lock) { 
                locks[arrayName] = lock = shared_ptr<RWLock>(new RWLock());
            }
            return lock;
        }
    };

} // namespace

#endif
