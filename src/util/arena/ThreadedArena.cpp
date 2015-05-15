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

/****************************************************************************/

#include <util/arena/ArenaDecorator.h>                   // For ArenaDecorator
#include <util/Mutex.h>                                  // For Mutex
#include "ArenaDetails.h"                                // For implementation

/****************************************************************************/
namespace scidb { namespace arena {
/****************************************************************************/

/**
 *  @brief      Decorates an %arena with support for synchronizing access from
 *              across multiple concurrent threads of execution.
 *
 *  @details    Class ThreadedArena adds support for calling an arena from two
 *              or more threads simultaneously by embedding a mutex within the
 *              %arena and overriding each virtual function to first aquire it
 *              before delegating the call to the original %arena as before.
 *
 *  @author     jbell@paradigm4.com.
 */
class ThreadedArena : public ArenaDecorator
{
 public:                   // Construction
                              ThreadedArena(const ArenaPtr& p)
                               : ArenaDecorator(p)                             {}

 public:                   // Attributes
    virtual name_t            name()                     const                 {ScopedMutexLock x(_mutex);return _arena->name();}
    virtual ArenaPtr          parent()                   const                 {ScopedMutexLock x(_mutex);return _arena->parent();}
    virtual size_t            available()                const                 {ScopedMutexLock x(_mutex);return _arena->available();}
    virtual size_t            allocated()                const                 {ScopedMutexLock x(_mutex);return _arena->allocated();}
    virtual size_t            peakusage()                const                 {ScopedMutexLock x(_mutex);return _arena->peakusage();}
    virtual size_t            allocations()              const                 {ScopedMutexLock x(_mutex);return _arena->allocations();}
    virtual features_t        features()                 const                 {ScopedMutexLock x(_mutex);return _arena->features() | threading;}
    virtual void              checkpoint(name_t l)       const                 {ScopedMutexLock x(_mutex);return _arena->checkpoint(l);}
    virtual void              insert(std::ostream& o)    const                 {ScopedMutexLock x(_mutex);return _arena->insert(o);}

 public:                   // Operations
    virtual void*             allocate(size_t n)                               {ScopedMutexLock x(_mutex);return _arena->allocate(n);}
    virtual void*             allocate(size_t n,finalizer_t f)                 {ScopedMutexLock x(_mutex);return _arena->allocate(n,f);}
    virtual void*             allocate(size_t n,finalizer_t f,count_t c)       {ScopedMutexLock x(_mutex);return _arena->allocate(n,f,c);}
    virtual void              recycle(void* p)                                 {ScopedMutexLock x(_mutex);return _arena->recycle(p);}
    virtual void              destroy(void* p,count_t c)                       {ScopedMutexLock x(_mutex);return _arena->destroy(p,c);}
    virtual void              reset()                                          {ScopedMutexLock x(_mutex);return _arena->reset();}

 public:                   // Implementation
    virtual void*             doMalloc(size_t n)                               {ScopedMutexLock x(_mutex);return _arena->doMalloc(n);}
    virtual void              doFree  (void*  p,size_t n)                      {ScopedMutexLock x(_mutex);return _arena->doFree(p,n);}

 protected:                // Representation
            Mutex     mutable _mutex;                    // The arena mutex
};

/****************************************************************************/

/**
 *  Add support for thread locking to the %arena o.parent() if it does not yet
 *  support this feature.
 *
 *  Notice that it is an error to try to add thread locking to an %arena whose
 *  parent %arena does not also support this feature.
 */
ArenaPtr addThreading(const Options& o)
{
    ArenaPtr p(o.parent());                              // The delegate arena

    assert(p->parent()->supports(threading));            // So parent must too

    if (p->supports(threading))                          // Already supported?
    {
        return p;                                        // ...no need to add
    }

    return boost::make_shared<ThreadedArena>(p);         // Attach decoration
}

/****************************************************************************/
}}
/****************************************************************************/
