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

#ifndef UTIL_ARENA_LOCKING_ARENA_H_
#define UTIL_ARENA_LOCKING_ARENA_H_

/****************************************************************************/

#include <util/Arena.h>                                  // For Arena
#include <util/Mutex.h>                                  // For Mutex

/****************************************************************************/
namespace scidb { namespace arena {
/****************************************************************************/
/**
 *  @brief      Specializes an Arena to enable synchronized access from across
 *              multiple concurrent threads.
 *
 *  @details    Class Locking provides support for accessing an Arena from two
 *              or more threads simultaneously by embedding a mutex within the
 *              Arena and overriding every virtual function to first aquire it
 *              before continuing as before.
 *
 *  @author     jbell@paradigm4.com.
 */
template<class Arena>
class Locking : public Arena
{
 public:                   // Construction
                              Locking(const Options& o) : Arena(o)             {}
                             ~Locking()                                        {ScopedMutexLock x(_mutex);       Arena::reset();}

 public:                   // Attributes
    virtual name_t            name()                     const                 {ScopedMutexLock x(_mutex);return Arena::name();}
    virtual ArenaPtr          parent()                   const                 {ScopedMutexLock x(_mutex);return Arena::parent();}
    virtual size_t            available()                const                 {ScopedMutexLock x(_mutex);return Arena::available();}
    virtual size_t            allocated()                const                 {ScopedMutexLock x(_mutex);return Arena::allocated();}
    virtual size_t            peakUsage()                const                 {ScopedMutexLock x(_mutex);return Arena::peakUsage();}
    virtual size_t            allocations()              const                 {ScopedMutexLock x(_mutex);return Arena::allocations();}
    virtual bool              supports(features_t  f)    const                 {ScopedMutexLock x(_mutex);return Arena::supports(f & ~locking);}
    virtual void              checkpoint(name_t    l)    const                 {ScopedMutexLock x(_mutex);return Arena::checkpoint(l);}
    virtual void              insert(std::ostream& o)    const                 {ScopedMutexLock x(_mutex);return Arena::insert(o);}

 public:                   // Operations
    virtual void*             allocate(size_t n)                               {ScopedMutexLock x(_mutex);return Arena::allocate(n);}
    virtual void*             allocate(size_t n,finalizer_t f)                 {ScopedMutexLock x(_mutex);return Arena::allocate(n,f);}
    virtual void*             allocate(size_t n,finalizer_t f,count_t c)       {ScopedMutexLock x(_mutex);return Arena::allocate(n,f,c);}
    virtual void              recycle(void* p)                                 {ScopedMutexLock x(_mutex);return Arena::recycle (p);}
    virtual void              destroy(void* p,count_t n)                       {ScopedMutexLock x(_mutex);return Arena::destroy (p,n);}
    virtual void              reset()                                          {ScopedMutexLock x(_mutex);return Arena::reset();}

 public:                   // Implementation
    virtual void*             doMalloc(size_t n)                               {ScopedMutexLock x(_mutex);return Arena::doMalloc(n);}
    virtual size_t            doFree  (void*  p,size_t n)                      {ScopedMutexLock x(_mutex);return Arena::doFree(p,n);}

 protected:                // Representation
            Mutex     mutable _mutex;                     // The arena mutex
};

/****************************************************************************/
}}
/****************************************************************************/
#endif
/****************************************************************************/
