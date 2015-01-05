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

#include <util/arena/LockingArena.h>                     // For LockingArena
#include <util/arena/ScopedArena.h>                      // For ScopedArena
#include <boost/foreach.hpp>                             // For BOOST_FOREACH
#include "ArenaHeader.h"                                 // For ArenaHeader
#include "ArenaDetails.h"                                // For implementation

/****************************************************************************/
namespace scidb { namespace arena {
/****************************************************************************/

/**
 *  Class Page extends the CustomScalar allocation header to save a pointer to
 *  the allocating Arena, and supplies as the custom finalizer a function that
 *  returns the underlying memory for the page to this Arena.
 */
class Page : Header::CS
{
 public:                   // Construction
                              Page(size_t n,Arena* p)
                               : Header::CS(n-sizeof(Header::CS),finalizer_t(free)),
                                 _arena(p)               {assert(p != 0);}

 public:                   // Operations
            byte_t*           getPayload()               {return _h.getPayload();}

 private:                  // Implementation
    static  void              free(Arena*&);             // Releases the page

 private:                  // Representation
            Arena*      const _arena;                    // The parent arena
};

/**
 *  Return the page in which this header lives to the Arena that allocated it.
 */
void Page::free(Arena*& arena)
{
    Header& h = Header::retrieve(&arena);                // Retrieve header

    arena->free(h.getAllocation(),h.getOverallSize());   // Release the page
}

/****************************************************************************/

/**
 *  Construct a resetting Arena that allocates storage o.pagesize() bytes at a
 *  time from the Arena o.parent().  Actually, we increment the page size just
 *  a bit to make room for a header - class Page - in which we put a finalizer
 *  and pointer back to this Arena; thus we're reusing the finalizer machinery
 *  to arrange for our pages to, in a sense, 'destroy themselves'.
 */
    ScopedArena::ScopedArena(const Options& o)
               : LimitedArena(o),
                 _size(align(sizeof(Page) + o.pagesize())),
                 _next(0),
                 _last(0)
{
    assert(consistent());                                // Check consistency
}

/**
 *  Reset the Arena, so returning any remaining pages to our parent Arena.
 */
    ScopedArena::~ScopedArena()
{
    this->reset();                                       // Free all our pages
}

/**
 *  Overrides Arena::supports() to indicate that we also support resetting and
 *  finalizing.
 */
bool ScopedArena::supports(features_t f) const
{
    return Arena::supports(f & ~(finalizing|resetting)); // Skip LimitedArena
}

/**
 *  Overrides Arena::insert() to emit a few interesting members of our own.
 */
void ScopedArena::insert(std::ostream& o) const
{
    LimitedArena::insert(o);                             // First insert base

    o <<",pagesize=" << _size  << ','                    // Emit page size
      << "pending="  << _list.size();                    // Emit list size
}

/**
 *  Overrides Arena::allocate() to omit the header that is normally written to
 *  the front of an allocation. ScopedArenas do not need to write a header for
 *  simple allocations because they do not support recycling.
 */
void* ScopedArena::allocate(size_t n)
{
    if (isDebug())                                       // In a debug build?
    {
        return LimitedArena::allocate(n);                // ...accept a header
    }

    if (n == 0)                                          // A trivial request?
    {
        n = 1;                                           // ...take care of it
    }
    else
    if (n > unlimited)                                   // Too big to handle?
    {
        this->overflowed();                              // ...signal overflow
    }

    return this->malloc(n);                              // Align and allocate
}

/**
 *  Overrides Arena::allocate() to add the allocation to the finalization list
 *  where reset() can find it later.
 */
void* ScopedArena::allocate(size_t n,finalizer_t f)
{
    void* p = LimitedArena::allocate(n,f);               // Pass down to base

    if (f != 0)                                          // Needs finalizing?
    {
        _list.push_front(p);                             // ...push onto list
    }

    return p;                                            // The new allocation
}

/**
 *  Overrides Arena::allocate() to add the allocation to the finalization list
 *  where reset() can find it later.
 */
void* ScopedArena::allocate(size_t n,finalizer_t f,count_t c)
{
    void* p = LimitedArena::allocate(n,f,c);             // Pass down to base

    if (f!=0 && c!=0)                                    // Needs finalizing?
    {
        _list.push_front(p);                             // ...push onto list
    }

    return p;                                            // The new allocation
}

/**
 *  Overrides Arena::recycle() to ignore the request to recycle the allocation.
 */
void ScopedArena::recycle(void* payload)
{
    if (isDebug())                                       // In a debug build?
    {
        LimitedArena::recycle(payload);                  // ...validate header
    }
}

/**
 *  Allocate 'size' bytes of raw memory from the current page, or set up a new
 *  page by allocating from our parent Arena if there is insufficient space in
 *  the current page to satisfy the request.
 *
 *  We write a custom finalizing header (class Page) at the front of each page
 *  and push this onto the front of the finalizer list where reset() can later
 *  find it. This neatly ensures that the pages are deallocated only after the
 *  objects in them have been finalized.
 */
void* ScopedArena::doMalloc(size_t size)
{
    assert(size!=0 && isAligned(size));                  // Is already aligned

    if (_next + size < _next)                            // Pointer overflows?
    {
        this->overflowed();                              // ...signal overflow
    }

    if (_next + size > _last)                            // Page out of space?
    {
        size_t n = std::max(_size,sizeof(Page) + size);  // ...at least a page
        void*  m = LimitedArena::doMalloc(n);            // ...allocate memory
        Page*  p = (new(m) Page(n,_parent.get()));       // ...init the header

        _list.push_front(p->getPayload());               // ...add to the list

        _next = static_cast<byte_t*>(m) + sizeof(Page);  // ...aim past header
        _last = static_cast<byte_t*>(m) + n;             // ...end of the page
    }

    assert(_next + size <= _last);                       // Now there is room!

    byte_t* p = _next;                                   // Copy next pointer
    _next    += size;                                    // Then step over it
    return  p;                                           // Our new allocation
}

/**
 *  Ignore a request to free the given allocation: the memory will instead be
 *  recovered when reset() is next called.
 */
size_t ScopedArena::doFree(void*,size_t)
{
    return 0;                                            // Wait until reset()
}

/**
 *  Reset the Arena,  finalizing allocations that have not yet been explicitly
 *  destroyed, and returning the underlying pages in which the allocations sit
 *  to our parent Arena.
 *
 *  We iterate forward over the finalizer list, firing each finalizer in turn;
 *  this has the effect of destroying both objects and the pages in which they
 *  sit in the opposite order to that in which they were allocated.
 */
void ScopedArena::reset()
{
    BOOST_FOREACH (void* p,_list)                        // For each payload
    {
        Header::retrieve(p).finalize();                  // ...finalize it
    }

    _list.clear();                                       // Discard finalizers
    _next = _last = 0;                                   // Reset current page
    LimitedArena::reset();                               // Now reset our base
    assert(consistent());                                // Check we're kosher
}

/**
 *  Return true if the object looks to be in good shape.  Centralizes a number
 *  of consistency checks that would otherwise clutter up the code, and, since
 *  only ever called from within assertions, can be eliminated entirely by the
 *  compiler from the release build.
 */
bool ScopedArena::consistent() const
{
    assert(LimitedArena::consistent());                  // Check base is good

    assert(isAligned(_size));                            // Validate page size
    assert(isAligned(sizeof(Page)));                     // And header size
    assert(_list.size() <= _allocations);                // Check page list
    assert(_next<=_last && _last<=_next+_size);          // Check page extent
    assert(!_next  == !_last);                           // Both, or neither

    return true;                                         // Appears to be good
}

/**
 *  Construct and return a ScopedArena that constrains the Arena o.parent() to
 *  allocating at most o.limit() bytes of memory before it throws an Exhausted
 *  exception, that allocates memory in pages of o.pagesize() bytes at a time,
 *  and that defers the recycling of memory until reset() is eventually called.
 */
ArenaPtr newScopedArena(const Options& o)
{
    using boost::make_shared;                            // For make_shared()

    if (o.locking())                                     // Needs locking too?
    {
        return make_shared< Locking<ScopedArena> >(o);   // ...locking arena
    }
    else                                                 // No locking needed
    {
        return make_shared<ScopedArena>(o);              // ...vanilla arena
    }
}

/****************************************************************************/
}}
/****************************************************************************/
