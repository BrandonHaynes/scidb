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

#include <util/arena/LimitedArena.h>                     // For LimitedArena
#include <util/arena/LockingArena.h>                     // For Locking<>
#include "ArenaDetails.h"                                // For implementation

/****************************************************************************/
namespace scidb { namespace arena {
/****************************************************************************/

/**
 *  Construct an Arena that can allocate at most o.limit() bytes of memory off
 *  the parent Arena o.parent().
 */
    LimitedArena::LimitedArena(const Options& o)
                : _name       (o.name()),
                  _limit      (o.limit()),
                  _parent     (o.parent()),
                  _available  (o.limit()),
                  _allocated  (0),
                  _peakUsage  (0),
                  _allocations(0)
{
    assert(consistent());                                // Check consistency
}

/**
 *  A LimitedArena supports the same features as the parent to which it passes
 *  the actual requests for memory, but does not support resetting: to forward
 *  the call to reset() on to the parent would invalidate any allocations made
 *  by sibling arenas that happen to be allocating from the same parent.
 */
bool LimitedArena::supports(features_t features) const
{
    if (features & (resetting|locking))                  // Resetting|locking?
    {
        return false;                                    // ...not supported
    }

    return _parent->supports(features);                  // Delegate to parent
}

/**
 *  Reset the allocation statistics to their original default values. We can't
 *  reset our parent, however, because in general it will be handling requests
 *  from other arenas too.
 */
void LimitedArena::reset()
{
    _available  = _limit;                                // Reclaim allocated
    _allocated  = _peakUsage = _allocations = 0;         // Reset all counters

    assert(consistent());                                // Check consistency
}

/**
 *  Allocate 'size' bytes of raw storage from our parent Arena, first checking
 *  to see if this would exceed our own internal limit, in which case we throw
 *  an Exhausted exception.
 */
void* LimitedArena::doMalloc(size_t const size)
{
    assert(size!=0 && isAligned(size));                  // Is already aligned

    if (size > _available)                               // Exceeds our limit?
    {
        this->exhausted(size);                           // ...sorry,  but no
    }

    void*       p = _parent->doMalloc(size);             // Delegate to parent
    _available   -= size;                                // ...less available
    _allocated   += size;                                // ...more allocated
    _allocations += 1;                                   // ...update counter
    _peakUsage    = std::max(_allocated,_peakUsage);     // ...update peak

    assert(consistent());                                // Check consistency
    return p;                                            // The new allocation
}

/**
 *  Return the 'size' bytes of memory at address 'p' to our parent Arena.
 */
size_t LimitedArena::doFree(void* p,size_t const size)
{
    assert(p!=0 && size!=0 && isAligned(size));          // Validate arguments
    assert(size<=_allocated && _allocated!=0);           // Check counts match

    size_t n = _parent->doFree(p,size);                  // Delegate to parent

    _available   += n;                                   // ...more available
    _allocated   -= n;                                   // ...less allocated
    _allocations -= 1;                                   // ...update counter

    assert(consistent());                                // Check consistency
    return n;                                            // Actually reclaimed
}

/**
 *  Return true if the object looks to be in good shape.  Centralizes a number
 *  of consistency checks that would otherwise clutter up the code, and, since
 *  only ever called from within assertions, can be eliminated entirely by the
 *  compiler from the release build.
 */
bool LimitedArena::consistent() const
{
    assert(_parent      != 0);                           // Validate the parent
    assert(_limit       <= unlimited);                   // Now check that our
    assert(_available   <= _limit);                      //   various counters
    assert(_allocated   <= _peakUsage);                  //   are all within
    assert(_peakUsage   <= _limit);                      //   legal ranges
    assert(_allocations <= unlimited);                   //
    assert(_allocated + _available == _limit);           // Check book keeping
    assert((_allocated==0) == (_allocations==0));        // Live=0 <=> Total=0

    return true;                                         // Seems to be kosher
}

/**
 *  Construct and return a LimitedArena that constrains the Arena 'o.parent()'
 *  to allocating at most 'o.limit()' bytes of memory before throwing an Arena
 *  Exhausted exception.
 */
ArenaPtr newLimitedArena(const Options& o)
{
    using boost::make_shared;                            // For make_shared()

    if (o.locking())                                     // Needs locking too?
    {
        return make_shared< Locking<LimitedArena> >(o);  // ...locking arena
    }
    else                                                 // No locking needed
    {
        return make_shared<LimitedArena>(o);             // ...vanilla arena
    }
}

/****************************************************************************/
}}
/****************************************************************************/
