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
#include <list>                                          // For list<>
#include "ArenaDetails.h"                                // For implementation

/****************************************************************************/
namespace scidb { namespace arena {
/****************************************************************************/

/**
 *  @brief      Decorates an %arena with support for memory painting and other
 *              diagnostic checks.
 *
 *  @details    Details to follow...
 *
 *  @todo       - malloc() places blocks on intrusive doubly linked list.
 *
 *              - free() takes them off again
 *
 *              - walk the list to dump leaked blocks to the log.
 *
 *              - design issue: when to check this list? obviously from dtor,
 *              but when else?
 *
 *              - possible to also use system malloc debugging features?
 *
 *  @author     jbell@paradigm4.com.
 */
class DebugArena : public ArenaDecorator
{
 public:                   // Construction
                              DebugArena(const ArenaPtr&);

 public:                   // Attributes
    virtual features_t        features()           const;

 public:                   // Implementation
    virtual void*             doMalloc(size_t);
    virtual void              doFree  (void*,size_t);
};

/****************************************************************************/

const size_t head = 0xAaaaAaaaAaaaAaaa;                  // Head of a block
const size_t tail = 0xFfffFfffFfffFfff;                  // Tail of a block
const size_t born = 0xBabeFaceBabeFace;                  // Color of new block
const size_t dead = 0xDeadBeefDeadBeef;                  // Color of dead block
const size_t over = sizeof(head) + sizeof(tail);         // Additional overhead

/****************************************************************************/

BOOST_STATIC_ASSERT(sizeof(size_t)==sizeof(alignment_t));// Guards are aligned

/****************************************************************************/

/**
 *  Construct a decorator that adds support for memory painting and diagnostic
 *  checking to the given arena.
 */
    DebugArena::DebugArena(const ArenaPtr& p)
              : ArenaDecorator(p)
{}

/**
 *  Return a bitfield indicating the set of features this %arena supports.
 */
features_t DebugArena::features() const
{
    return _arena->features() | debugging;               // Adds debug support
}

/**
 *  Allocate 'size' bytes of raw storage.
 *
 *  'size' may not be zero.
 *
 *  The result is correctly aligned to hold one or more 'alignment_t's.
 *
 *  The resulting allocation must eventually be returned to the same a%arena by
 *  calling doFree(), and with the same value for 'size'.
 */
void* DebugArena::doMalloc(size_t size)
{
    assert(size != 0);                                   // Validate arguments

    size = align(size);                                  // Round up the size

    size_t  const m = size / sizeof(size_t);             // Words in the body
    void*   const p = _arena->doMalloc(size + over);     // Allocate the block
    size_t* const q = static_cast<size_t*>(p) + 1;       // Point at payload

    q[-1] = head;                                        // Paint the head
    std::fill(q,q+m,born);                               // Paint the body
    q[+m] = tail;                                        // Paint the tail

    assert(aligned(q));                                  // Check it's aligned
    return q;                                            // The new allocation
}

/**
 *  Free the memory that was allocated earlier from the same %arena by calling
 *  malloc() and attempt to recycle it for future reuse to reclaim up to 'size'
 *  bytes of raw storage.  No promise is made as to *when* this memory will be
 *  made available again however: the %arena may, for example, prefer to defer
 *  recycling until a subsequent call to reset() is made.
 */
void DebugArena::doFree(void* payload,size_t size)
{
    assert(aligned(payload) && size!=0);                 // Validate arguments

    size = align(size);                                  // Round up the size

    size_t  const m = size / sizeof(size_t);             // Words in the body
    size_t* const q = static_cast<size_t*>(payload);     // Retreive the block

    assert(q[-1] == head);                               // Check head intact
    assert(q[+m] == tail);                               // Check tail intact
    std::fill(q,q+m,dead);                               // Paint the body

    _arena->doFree(q-1,size + over);                     // And free the block
}

/**
 *  Add support for memory painting and other diagnostic checks to the %arena
 *  o.parent() if it does not already support these features.
 */
ArenaPtr addDebugging(const Options& o)
{
    ArenaPtr p(o.parent());                              // The delegate arena

    if (p->supports(debugging))                          // Already supported?
    {
        return p;                                        // ...no need to add
    }

    return boost::make_shared<DebugArena>(p);            // Attach decoration
}

/****************************************************************************/
}}
/****************************************************************************/
