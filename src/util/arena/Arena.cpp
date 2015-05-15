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

#include <util/arena/ArenaMonitor.h>                     // For Monitor
#include <system/Exceptions.h>                           // For Exception
#include "ArenaHeader.h"                                 // For Header
#include "ArenaDetails.h"                                // For implementation

/****************************************************************************/
namespace scidb { namespace arena {
/****************************************************************************/

/**
 *  Return the name of the %arena, an optional label that can help to identify
 *  the object when its statistics appear in reports prepared by the monitor.
 */
name_t Arena::name() const
{
    return "";                                           // The optional name
}

/**
 *  Return a shared pointer to the parent,  an optional %arena to which we may
 *  choose to delegate some or all of our allocation requests.
 */
ArenaPtr Arena::parent() const
{
    return ArenaPtr();                                   // The parent arena
}

/**
 *  Return the number of bytes still available for allocation from within this
 *  %arena before it throws an Exhausted exception.
 */
size_t Arena::available() const
{
    return unlimited;                                    // No allocation limit
}

/**
 *  Return the number of bytes that have been allocated through the %arena and
 *  that are still in use, or 'live'.  An allocation is live if it has not yet
 *  been recycled by the %arena from which it was allocated nor has the %arena
 *  since been reset.
 */
size_t Arena::allocated() const
{
    return 0;                                            // Nothing allocated
}

/**
 *  Return the maximum or 'peak' number of bytes that have been allocated from
 *  this %arena since either it was first constructed or last reset.
 */
size_t Arena::peakusage() const
{
    return 0;                                            // Nothing allocated
}

/**
 *  Return the number of individual allocations that are still in use,  or are
 *  'live'. An allocation is live if it hasn't yet been recycled by the %arena
 *  from which it was allocated nor has the %arena since been reset.
 */
size_t Arena::allocations() const
{
    return 0;                                            // Nothing allocated
}

/**
 *  Return a bitfield indicating the set of features this %arena supports.
 */
features_t Arena::features() const
{
    return finalizing;                                   // Finalizers handled
}

/**
 *  Return true if this %arena supports every one of the features appearing in
 *  the given bitfield.
 */
bool Arena::supports(features_t features) const
{
    return (features & ~this->features()) == 0;          // Are all supported?
}

/**
 *  Update the %arena monitor with a snapshot of our allocation statistics and
 *  associate this snapshot with the given label.
 */
void Arena::checkpoint(name_t label) const
{
    Monitor::getInstance().update(*this,label);          // Update the monitor
}

/**
 *  Insert a formatted representation of the %arena onto the output stream 'o'.
 */
void Arena::insert(std::ostream& o) const
{
 /* Recurse through the list of arenas that runs through the parent attribute,
    building up a name of the form 'root/.../parent-arena/arena' in the string
    from which we inherit. Note that this list is short - no more than a dozen
    links, at most - and that each  recursive call pushes just two pointers on
    the stack, so the consumption of stack space is minimal...*/

    struct pathname : std::string
    {
        void traverse(const Arena& arena)                // Build up the name
        {
            if (ArenaPtr p = arena.parent())             // Has a parent arena?
            {
                traverse(*p);                            // ...traverse parent
                *this += '/';                            // ...add a delimiter
            }

            *this += arena.name();                       // Append arena name
        }

        pathname(const Arena& arena) {traverse(arena);}  // Traverse ancestors
    };

    o <<  "name=\""      << pathname(*this) << '"'       // Emit path name
      << ",available="   << bytes_t (available())        // Emit available()
      << ",allocated="   << bytes_t (allocated())        // Emit allocated()
      << ",peakusage="   << bytes_t (peakusage())        // Emit peakusage()
      << ",allocations=" << size_t  (allocations());     // Emit allocations()
}

/**
 *  Allocate 'n' bytes of raw storage from the %arena.
 *
 *  A value of 0 for the size 'n' is acceptable, and returns a unique pointer,
 *  as required by operator new().
 *
 *  Throws an exception if 'n' exceeds 'unlimited'.
 *
 *  The result is correctly aligned to hold one or more 'alignment_t's.
 *
 *  @remarks    Be sure never to pass the resulting allocation directly to the
 *  function Arena::destroy(), which would attempt to retrieve a finalizer for
 *  the object that is not there - and most likely crash.  Instead, you should
 *  call either Arena::recycle() or better still, the associated free function
 *  destroy(Arena&,void*).
 */
void* Arena::allocate(size_t n)
{
    if (n > unlimited)                                   // Too big to handle?
    {
        this->overflowed();                              // ...signal overflow
    }

 /* By default we *do* put a header on the block, despite our earlier warning;
    but subclasses like the ScopedArena, which ignores the request to recycle,
    may not...*/

    return carve<Header::POD>(*this,n);                  // Carve a new block
}

/**
 *  Allocate 'n' bytes  of raw storage from the %arena, and save the finalizer
 *  function 'f', to be applied to the allocation later when the new object is
 *  eventually destroyed.
 *
 *  A value of 0 for the size 'n' is acceptable, and returns a unique pointer,
 *  as required by operator new().
 *
 *  Throws an exception if 'n' exceeds 'unlimited'.
 *
 *  The result is correctly aligned to hold one or more 'alignment_t's.
 *
 *  @remarks    Be sure never to pass the resulting allocation directly to the
 *  function Arena::recycle(),  which will ignore any registered finalizer for
 *  the object and most likely cause a leak.  Instead,  you should call either
 *  Arena::destroy() or better still, the free function destroy(Arena&,void*).
 */
void* Arena::allocate(size_t n,finalizer_t f)
{
    if (f == 0)                                          // Trivial finalizer?
    {
        return this->allocate(n);                        // ...handled earlier
    }

    if (n > unlimited)                                   // Too big to handle?
    {
        this->overflowed();                              // ...signal overflow
    }

    if (f == arena::allocated)                           // Allocated Scalar?
    {
        return carve<Header::AS>(*this,n);               // ...carve the block
    }
    else                                                 // No, Custom Scalar
    {
        return carve<Header::CS>(*this,n,f);             // ...carve the block
    }
}

/**
 *  Allocate a vector of 'c' elements each of size 'n', and save the finalizer
 *  function 'f', to be applied to every vector element in turn when the array
 *  is eventually destroyed.
 *
 *  A value of 0 for the size 'n' is acceptable, and returns a unique pointer,
 *  as required by operator new().  The function 'f' will be invoked with this
 *  pointer 'c' times when the allocation is later destroyed.
 *
 *  Throws an exception if the product 'n' * 'c' exceeds 'unlimited'.
 *
 *  The result is correctly aligned to hold one or more 'alignment_t's.
 *
 *  @remarks    Be sure never to pass the resulting allocation directly to the
 *  function Arena::recycle(),  which will ignore any registered finalizer for
 *  the object and most likely cause a leak.  Instead,  you should call either
 *  Arena::destroy() or better still, the free function destroy(Arena&,void*).
 */
void* Arena::allocate(size_t n,finalizer_t f,count_t c)
{
    if (c == 0)                                          // No array elements?
    {
        struct local {static void nop(void*){}};         // ...no-op finalizer

        return this->allocate(0,f!=0 ? &local::nop : 0); // ...needs a header?
    }

    if (c == 1)                                          // Scalar allocation?
    {
        return this->allocate(n,f);                      // ...handled earlier
    }

    if (n > unlimited/c)                                 // Too big to handle?
    {
        this->overflowed();                              // ...signal overflow
    }

    if (f == 0)                                          // Trivial finalizer?
    {
        return this->allocate(c * n);                    // ...handled earlier
    }

    if (f == arena::allocated)                           // Allocated Vector?
    {
        return carve<Header::AV>(*this,n,0,c);           // ...carve the block
    }
    else                                                 // No, Custom Vector
    {
        return carve<Header::CV>(*this,n,f,c);           // ...carve the block
    }
}

/**
 *  Return the given allocation to the %arena to be recycled - that is, reused
 *  in a subsequent allocation. Needless to say, it is an error for the caller
 *  to make any use of the argument beyond this point. If the allocation needs
 *  to be finalized then use destroy() instead: recycle() doesn't finalize its
 *  argument. Notice also that arenas are not obliged to honor this request at
 *  all: class ScopedArena, for example, ignores the request to recycle memory
 *  in favor of freeing the allocations en masse when reset() is later called.
 */
void Arena::recycle(void* payload)
{
    assert(payload==0 || aligned(payload));              // Validate arguments

    if (payload != 0)                                    // Something to do?
    {
        Header& h(Header::retrieve(payload));            // ...retrieve header

        assert(h.getFinalizer() == 0);                   // ...no! use destroy

        byte_t* p = h.getAllocation();                   // ...get allocation
        size_t  n = h.getOverallSize();                  // ...and its length

        this->doFree(p,n);                               // ...free the memory
    }
}

/**
 *  Finalize the given allocation and return it to the %arena to be recycled -
 *  that is, reused in a subsequent allocation. Needless to say, it's an error
 *  for the caller to make any use of the allocation beyond this point. If the
 *  allocation does not need to be finalized then call recycle() instead. Note
 *  also that although arenas are obliged to implement this function to invoke
 *  the finalizer, they're not required to recycle the allocation immediately:
 *  they may instead prefer to wait until reset() is later called.
 */
void Arena::destroy(void* payload,count_t count)
{
    assert(payload==0 || aligned(payload));              // Validate arguments

    if (payload != 0)                                    // Something to do?
    {
        Header& h(Header::retrieve(payload));            // ...retrieve header

        assert(h.getFinalizer() != 0);                   // ...no! use recycle

        h.finalize(count);                               // ...finalize vector

        assert(h.getFinalizer() == 0);                   // ...check finalized

        byte_t* p = h.getAllocation();                   // ...get allocation
        size_t  n = h.getOverallSize();                  // ...and its length

        this->doFree(p,n);                               // ...free the memory
    }
}

/**
 *  Reset the %arena to its originally constructed state, so destroying extant
 *  objects, recycling their underlying storage for use in future allocations,
 *  and resetting the allocation statistics to their default values. An %arena
 *  must implement at least one of the members reset() or destroy()/ recycle()
 *  or it will leak memory; it may also wish to implement both, however.
 */
void Arena::reset()
{}

/**
 *  Allocate 'size' bytes of raw storage.
 *
 *  'size' may not be zero.
 *
 *  Throws an exception if 'size' exceeds 'unlimited'.
 *
 *  The result is correctly aligned to hold one or more 'alignment_t's.
 */
void* Arena::malloc(size_t size)
{
    assert(size != 0);                                   // Validate arguments

    if (size > unlimited)                                // Too big to handle?
    {
        this->overflowed();                              // ...signal overflow
    }

    return this->doMalloc(size);                         // Pass to doMalloc()
}

/**
 *  Allocate 'size' * 'count' bytes of raw storage.
 *
 *  Neither 'size' nor 'count' may be zero.
 *
 *  Throws an exception if the product 'size' * 'count' exceeds 'unlimited'.
 *
 *  The result is correctly aligned to hold one or more 'alignment_t's.
 */
void* Arena::malloc(size_t size,count_t count)
{
    assert(size!=0 && count!=0);                         // Validate arguments

    if (size > unlimited/count)                          // Too big to handle?
    {
        this->overflowed();                              // ...signal overflow
    }

    return this->doMalloc(size * count);                 // Pass to doMalloc()
}

/**
 *  Allocate 'size' bytes of zero-initialized raw storage.
 *
 *  'size' may not be zero.
 *
 *  Throws an exception if 'size' exceeds 'unlimited'.
 *
 *  The result is correctly aligned to hold one or more 'alignment_t's.
 */
void* Arena::calloc(size_t size)
{
    return memset(this->malloc(size),0,size);            // Allocate and clear
}

/**
 *  Allocate 'size' * 'count' bytes of zero-initialized raw storage.
 *
 *  Neither 'size' nor 'count' may be zero.
 *
 *  Throws an exception if the product 'size' * 'count' exceeds 'unlimited'.
 *
 *  The result is correctly aligned to hold one or more 'alignment_t's.
 */
void* Arena::calloc(size_t size,count_t count)
{
    return memset(this->malloc(size,count),0,size);      // Allocate and clear
}

/**
 *  Copy the string 's', including its terminating null, into memory allocated
 *  from within this arena.
 *
 *  When returning the allocation to a recycling arena, use a call such as:
 *  @code
 *      arena.free(s,strlen(s) + 1);                     // Remember the '\0'
 *  @endcode
 */
char* Arena::strdup(const char* s)
{
    assert(s != 0);                                      // Validate arguments

    size_t n = strlen(s) + 1;                            // Size of duplicate

    return (char*)memcpy(this->malloc(n),s,n);           // Allocate and copy
}

/**
 *  Copy the string 's', including its terminating null, into memory allocated
 *  from within this arena.
 *
 *  When returning the allocation to a recycling arena, use a call such as:
 *  @code
 *      arena.free(s,s.length() + 1);                    // Remember the '\0'
 *  @endcode
 */
char* Arena::strdup(const std::string& s)
{
    size_t n = s.size() + 1;                             // Size of duplicate

    return (char*)memcpy(this->malloc(n),s.c_str(),n);   // Allocate and copy
}

/**
 *  Free the memory that was allocated earlier from the same %arena by calling
 *  malloc() and attempt to recycle it for future reuse to reclaim up to 'size'
 *  bytes of raw storage.  No promise is made as to *when* this memory will be
 *  made available again however: the %arena may, for example, prefer to defer
 *  recycling until a subsequent call to reset() is made.
 */
void Arena::free(void* payload,size_t size)
{
    assert(aligned(payload) && size!=0);                 // Validate arguments

    this->doFree(payload,size);                          // Pass to doFree()
}

/**
 *  @fn void* Arena::doMalloc(size_t size) = 0
 *
 *  Allocate 'size' bytes of raw storage.
 *
 *  'size' may not be zero.
 *
 *  The result is correctly aligned to hold one or more 'alignment_t's.
 *
 *  The resulting allocation must eventually be returned to the same %arena by
 *  calling doFree(), and with the same value for 'size'.
 */

/**
 *  @fn void Arena::doFree(void* p,size_t size) = 0
 *
 *  Free the memory that was allocated earlier from the same %arena by calling
 *  malloc() and attempt to recycle it for future reuse to reclaim up to 'size'
 *  bytes of raw storage.  No promise is made as to *when* this memory will be
 *  made available again however: the %arena may, for example, prefer to defer
 *  recycling until a subsequent call to reset() is made.
 */

/**
 *  Throw an exception to indicate that an arithmetic operation has overflowed
 *  while computing the size of an allocation.  This can occur when either the
 *  element size, the element count, or their product,  is too big to be saved
 *  within a header block -  the subsequent call to 'malloc()' would certainly
 *  have failed in this event were we to have proceeded. This almost certainly
 *  indicates some kind of error in the caller's arithmetic.
 *
 *  The exception we throw can be caught either as a std::bad_alloc or else as
 *  a SystemException.
 */
void Arena::overflowed() const
{
    struct overflowed : std::bad_alloc, SystemException
    {
        overflowed(const Arena& a)
         : SystemException(SYSTEM_EXCEPTION(SCIDB_SE_NO_MEMORY,SCIDB_LE_ARENA_OVERFLOWED) << a)
        {}

        const char* what() const throw()
        {
            return SystemException::what();              // Use scidb message
        }
    };

    throw overflowed(*this);                             // Throw an exception
}

/**
 *  Throw an Exhausted exception to indicate that the %arena has been asked to
 *  allocate 'size' bytes of memory and that this exceeds the arena's internal
 *  limit.
 *
 *  The exception we throw can be caught either as an arena::Exhausted or else
 *  as a SystemException.
 */
void Arena::exhausted(size_t size) const
{
    struct exhausted : Exhausted, SystemException
    {
        exhausted(const Arena& a,size_t n)
         : SystemException(SYSTEM_EXCEPTION(SCIDB_SE_NO_MEMORY,SCIDB_LE_ARENA_EXHAUSTED) << a << bytes_t(n))
        {}

        const char* what() const throw()
        {
            return SystemException::what();              // Use scidb message
        }
    };

    throw exhausted(*this,size);                         // Throw an exception
}

/**
 *  Construct and return an %arena that supports the features specified in the
 *  given options structure.
 *
 *  This is the preferred way to construct an arena (as opposed to including a
 *  header file and invoking the constructor directly) because it isolates the
 *  caller from having to know the exact type of the %arena that they receive.
 */
ArenaPtr newArena(Options o)
{
    ArenaPtr p;                                          // The result arena

    if (o.resetting() && o.recycling())                  // Reset And recycle?
    {
        p = newLeaArena(o);                              // ...the only choice
    }
    else
    if (o.resetting())                                   // Support resetting?
    {
        p = newScopedArena(o);                           // ...the best choice
    }
    else                                                 // Support recycling?
    {
        p = newLimitedArena(o);                          // ...the best choice
    }

    if (o.debugging())                                   // Support debugging?
    {
        p = addDebugging(o.parent(p));                   // ...then add it now
    }

    if (o.threading())                                   // Support threading?
    {
        p = addThreading(o.parent(p));                   // ...then add it now
    }

    return p;                                            // Your arena, chief!
}

/****************************************************************************/
}}
/****************************************************************************/
