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

#include <util/arena/LockingArena.h>                     // For Locking
#include <list>                                          // For list
#include "ArenaDetails.h"                                // For implementation

/****************************************************************************/
namespace scidb { namespace arena {
/****************************************************************************/

/**
 *  @brief      Adds diagnostic checking to an existing Arena.
 *
 *  @details    Details to follow...
 *
 *  @todo       malloc() to  place blocks on an intrusive doubly linked list
 *              free() to take them off again
 *              walk the list to dump leaked blocks to the log.
 *              design issue: when to check this list? obviously from dtor,
 *              but when else?
 *              possible to also make use of system malloc debugging features?
 *
 *  @author     jbell@paradigm4.com.
 */
struct DebugArena : public Arena
{
 public:                   // Construction
                              DebugArena(const Options&);

 public:                   // Attributes
    virtual name_t            name()               const {return "debug";}
    virtual ArenaPtr          parent()             const {return _parent;}

 public:                   // Implementation
    virtual void*             doMalloc(size_t);
    virtual size_t            doFree  (void*,size_t);

 protected:                // Implementation
            bool              consistent()  const;

 protected:                // Representation
            ArenaPtr    const _parent;                   // Our parent arena
};

/****************************************************************************/

const size_t head = 0xaaaaaaaaaaaaaaaa;
const size_t tail = 0xffffffffffffffff;
const size_t born = 0xbabefacebabeface;
const size_t dead = 0xdeadbeefdeadbeef;
const size_t overhead = sizeof(head) + sizeof(tail);

/****************************************************************************/

    DebugArena::DebugArena(const Options& o)
              : _parent(o.parent())
{
    assert(consistent());                                // Check consistency
}

void* DebugArena::doMalloc(size_t const size)
{
    assert(size!=0 && isAligned(size));                  // Is already aligned

    size_t  const m = size / sizeof(size_t);
    void*   const p = _parent->doMalloc(size + overhead);
    size_t*       q = static_cast<size_t*>(p) + 1;


    q[-1] = head;
    std::fill(q,q+m,born);
    q[+m] = tail;

    return q;
}

size_t DebugArena::doFree(void* payload,size_t const size)
{
    assert(payload!=0 && size!=0 && isAligned(size));    // Validate arguments

    size_t  const m = size / sizeof(size_t);
    size_t* const q = static_cast<size_t*>(payload);

    assert(q[-1] == head);
    assert(q[+m] == tail);
    std::fill(q,q+m,dead);

    _parent->doFree(q-1,size + overhead);

    return size;
}

/**
 *  Return true if the object looks to be in good shape.  Centralizes a number
 *  of consistency checks that would otherwise clutter up the code, and, since
 *  only ever called from within assertions, can be eliminated entirely by the
 *  compiler from the release build.
 */
bool DebugArena::consistent() const
{
    assert(_parent != 0);

    return true;
}

ArenaPtr newDebugArena(const Options& o)
{
    using boost::make_shared;                            // For make_shared()

    if (o.locking())                                     // Needs locking too?
    {
        return make_shared< Locking<DebugArena> >(o);    // ...locking arena
    }
    else                                                 // No locking needed
    {
        return make_shared<DebugArena>(o);               // ...vanilla arena
    }
}

/****************************************************************************/
}}
/****************************************************************************/
