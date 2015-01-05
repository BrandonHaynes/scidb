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

#include <util/Singleton.h>                              // For Singleton
#include <util/Mutex.h>                                  // For Mutex
#include "ArenaDetails.h"                                // For implementation

/****************************************************************************/
namespace scidb { namespace arena {
/****************************************************************************/

/**
 *  @brief      Implements the 'root' Arena, a locking, recycling, Arena from
 *              which all other arenas ultimately obtain their memory.
 *
 *  @details    Class RootArena represents what is  in some sense the simplest
 *              possible implementation of the Arena interface,  by forwarding
 *              its calls directly on through to the system free store, namely
 *              'malloc' and 'free'.  It forms the root of a parent-child tree
 *              that runs from all other arenas up to this singleton instance,
 *              and is automatically supplied as the default when constructing
 *              an Arena for which no other parent has been specified.
 *
 *  @todo       Add support for routing allocation requests made using global
 *              operator new and friends though this Arena too.
 *
 *  @author     jbell@paradigm4.com.
 */
class RootArena : public Arena
{
 public:                   // Construction
                              RootArena();

 public:                   // Attributes
    virtual name_t            name()               const {return "root";}
    virtual size_t            allocated()          const {return _allocated;}
    virtual size_t            peakUsage()          const {return _peakUsage;}
    virtual size_t            allocations()        const {return _allocations;}
    virtual bool              supports(features_t) const;

 public:                   // Operations
    virtual void              reset();                   // Reset statistics

 public:                   // Allocation
            void*             doMalloc(size_t);          // Calls ::malloc()
            size_t            doFree  (void*,size_t);    // Calls ::free()

 protected:                // Implementation
    static  void*             alloc(size_t);
    static  void              dealloc(void*);

            bool              consistent()         const;

 protected:                // Representation
            size_t            _allocated;                // Bytes allocated
            size_t            _peakUsage;                // High water mark
            size_t            _allocations;              // Total allocations
            Mutex             _mutex;                    // Guards our members
};

/**
 *  Initialize the root Arena's allocation statistics.
 */
    RootArena::RootArena()
             : _allocated(0),
               _peakUsage(0),
               _allocations(0)
{
    assert(consistent());                                // Check consistency
}

/**
 *  Overrides Arena::supports() to indicate that we also support recycling and
 *  locking.
 */
bool RootArena::supports(features_t f) const
{
    return Arena::supports(f & ~(recycling|locking));    // We are So green...
}

/**
 *  The root Arena does not support resetting - at least not in the sense that
 *  it tracks allocations and frees those  that were never explictly recycled,
 *  as does class ScopedArena; nevertheless, it can still reset its allocation
 *  statistics to their original default values.
 */
void RootArena::reset()
{
    ScopedMutexLock x(_mutex);                           // Acquire our mutex

    _allocated = _peakUsage = _allocations = 0;          // Reset statistics

    assert(consistent());                                // Check consistency
}

/**
 *  Allocate 'size' bytes of raw storage from the system free store.
 */
void* RootArena::doMalloc(size_t const size)
{
    assert(size!=0 && isAligned(size));                  // Is already aligned

    if (void* p = ::malloc(size))                        // Allocate raw memory
    {
        ScopedMutexLock x(_mutex);                       // ...grab our mutex

        _allocated   += size;                            // ...allocated size
        _allocations += 1;                               // ...allocated one
        _peakUsage    = std::max(_allocated,_peakUsage); // ...update the peak

        assert(consistent());                            // ...check consistency
        return p;                                        // ...the allocation
    }

    this->exhausted(size);                               // Signal exhaustion
}

/**
 *  Return the 'size' bytes of raw storage pointed to by 'payload' to the free
 *  store for recycling.
 */
size_t RootArena::doFree(void* payload,size_t const size)
{
    assert(payload!=0 && size!=0 && isAligned(size));    // Validate arguments
    assert(size<=_allocated && _allocated!=0);           // Check counts match

    ::free(payload);                                     // Return the storage
    {
        ScopedMutexLock x(_mutex);                       // ...grab our mutex

        _allocated   -= size;                            // ...update stats
        _allocations -= 1;                               // ...update stats
    }

    return size;                                         // Return bytes freed
}

/**
 *  Return true if the object looks to be in good shape.  Centralizes a number
 *  of consistency checks that would otherwise clutter up the code, and, since
 *  only ever called from within assertions, can be eliminated entirely by the
 *  compiler from the release build.
 */
bool RootArena::consistent() const
{
    return true;                                         // Seems to be kosher
}

/**
 *  Return a reference to the one and only root Arena. All other arenas end up
 *  attaching to, and allocating from, this root object, one way or the other.
 */
ArenaPtr getRootArena()
{
 /* Class Singleton - perhaps erroneously? - takes  over the  deletion of its
    instance,  which does not sit all that well with passing arenas around by
    shared_ptr. We can work around this, however, by supplying our own custom
    deleter function...*/

    return ArenaPtr(Singleton<RootArena>::getInstance(),null_deleter());
}

/****************************************************************************/
}}
/****************************************************************************/
