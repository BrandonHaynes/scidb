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
#include "ArenaDetails.h"                                // For implementation

/****************************************************************************/
namespace scidb { namespace arena {
/****************************************************************************/

/**
 *  Construct an %arena that allocates at most o.limit() bytes of memory from
 *  the parent %arena o.parent().
 */
    LimitedArena::LimitedArena(const Options& o)
                : _name       (o.name()),
                  _limit      (o.limit()),
                  _parent     (o.parent()),
                  _available  (o.limit()),
                  _allocated  (0),
                  _peakusage  (0),
                  _allocations(0)
{
    assert(consistent());                                // Check consistency
}

/**
 *  Return a bitfield indicating the set of features this %arena supports.
 */
features_t LimitedArena::features() const
{
    if (_parent->supports(recycling))                    // Parent recycles?
    {
        return finalizing | recycling;                   // ...then so do we
    }
    else                                                 // Nope, they do not
    {
        return finalizing;                               // ...then nor do we
    }
}

/**
 *  Reset the allocation statistics to their original default values. We can't
 *  reset our parent, however, because in general it will be handling requests
 *  from other arenas too.
 */
void LimitedArena::reset()
{
    _available  = _limit;                                // Reclaim allocated
    _allocated  = _peakusage = _allocations = 0;         // Reset all counters

    assert(consistent());                                // Check consistency
}

/**
 *  Allocate 'size' bytes of raw memory from our parent %arena, first checking
 *  to see if this would exceed our own internal limit, in which case we throw
 *  an Exhausted exception.
 */
void* LimitedArena::doMalloc(size_t const size)
{
    assert(size != 0);                                   // Validate arguments

    if (size > _available)                               // Exceeds our limit?
    {
        this->exhausted(size);                           // ...sorry,  but no
    }

    void* const p = _parent->doMalloc(size);             // Delegate to parent

    _allocations += 1;                                   // ...update counter
    _allocated   += size;                                // ...more allocated
    _peakusage    = std::max(_allocated,_peakusage);     // ...update peak

    if (_available < unlimited)                          // Enforcing a limit?
    {
        _available -= size;                              // ...less available
    }

    assert(consistent());                                // Check consistency
    return p;                                            // The new allocation
}

/**
 *  Return the 'size' bytes of memory at address 'p' to our parent %arena.
 */
void LimitedArena::doFree(void* p,size_t const size)
{
    assert(aligned(p) && size!=0);                       // Validate arguments
    assert(size<=_allocated && _allocated!=0);           // Check counts match

    _parent->doFree(p,size);                             // Delegate to parent

    _allocations -= 1;                                   // ...update counter
    _allocated   -= size;                                // ...less allocated

    if (_available < unlimited)                          // Enforcing a limit?
    {
        _available += size;                              // ...more available
    }

    assert(consistent());                                // Check consistency
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
    assert(_available   <= _limit);                      //  various counters
    assert(_allocated   <= _peakusage);                  //  are within their
    assert(_peakusage   <= _limit);                      //  legal ranges...
    assert(_allocations <= unlimited);                   //
    assert(iff(_allocated==0,_allocations==0));          // Live=0 <=> Total=0

    if (_available < unlimited)                          // Enforcing a limit?
    {
        assert(_allocated + _available == _limit);       // ...check accounts
    }

    return true;                                         // Seems to be kosher
}

/**
 *  Construct and return a LimitedArena that constrains the %arena  o.parent()
 *  to allocating at most o.limit() bytes of memory before throwing an arena::
 *  Exhausted exception.
 */
ArenaPtr newLimitedArena(const Options& o)
{
    return boost::make_shared<LimitedArena>(o);          // Allocate new arena
}

/****************************************************************************/
}}
/****************************************************************************/
