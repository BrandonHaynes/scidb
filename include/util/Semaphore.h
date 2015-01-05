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
 * @file Semaphore.h
 *
 * @author roman.simakov@gmail.com
 *
 * @brief The Semaphore class for synchronization
 */

#ifndef SEMAPHORE_H_
#define SEMAPHORE_H_

#include <errno.h>
#include <boost/function.hpp>
#include <string>

// TODO: Implement auto detection of this directive
#ifndef __APPLE__
#define POSIX_SEMAPHORES
#endif


#ifdef POSIX_SEMAPHORES

#include <semaphore.h>
#include <time.h>
#include <fcntl.h>

namespace scidb
{

class Semaphore
{
private:
    sem_t _sem[1];

public:
    /**
     * @throws a scidb::Exception if necessary
     */
    typedef boost::function<bool()> ErrorChecker;
    Semaphore();

    ~Semaphore();

    void enter();

    void enter(int count) {
        for (int i = 0; i < count; i++)
            enter();
    }

    /**
     * Try to enter semaphore with time out and check error state.
     * If error state is set it returns.
     */

    bool enter(ErrorChecker& errorChecker);

    bool enter(int count, ErrorChecker& errorChecker) {
        for (int i = 0; i < count; i++) {
            if (!enter(errorChecker)) {
                return false;
            }
        }
        return true;
    }

    void release(int count = 1)
    {
        for (int i = 0; i < count; i++)
        {
            if (sem_post(_sem) == -1)
            {
                // XXX TODO: this must be an error
                assert(false);
            }
        }
    }

    bool tryEnter();
};

#else

#include "Mutex.h"
#include "Event.h"

namespace scidb
{

class Semaphore
{
private:
    Event _cond;
    Mutex _cs;
    int _count;

public:
     /**
      * @throws a scidb::Exception if necessary
      */
    typedef boost::function<bool()> ErrorChecker;
    Semaphore();
    ~Semaphore();

    bool enter(ErrorChecker& errorChecker) { 
       return enter(1, errorChecker);
    }

    void enter(int count = 1);
    bool enter(int count, ErrorChecker& errorChecker);

    void release(int n = 1);
    bool tryEnter();
};

#endif

class ReleaseOnExit
{
    Semaphore& sem;

  public:
    ReleaseOnExit(Semaphore& s) : sem(s) {}

    ~ReleaseOnExit() {
        sem.release();
    }    
};

} //namespace



#endif /* SEMAPHORE_H_ */
