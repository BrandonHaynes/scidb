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
 *  Return the name of the Arena, an optional string that can help to identify
 *  the Arena when its statistics appear in reports prepared by the monitor.
 */
name_t Arena::name() const
{
    return "";                                           // The optional name
}

/**
 *  Return a shared pointer to the parent, an optional Arena to which this one
 *  may choose to delegate some or all of its allocation requests.
 */
ArenaPtr Arena::parent() const
{
    return ArenaPtr();                                   // The parent arena
}

/**
 *  Return the number of bytes still available for allocation from within this
 *  Arena before it throws an 'Exhausted' exception.
 */
size_t Arena::available() const
{
    return unlimited;                                    // No allocation limit
}

/**
 *  Return the number of bytes that have been allocated through this Arena and
 *  that are still in use, or 'live'.  An allocation is live if it has not yet
 *  been recycled by the Arena from which it was allocated, nor has that Arena
 *  since been reset.
 */
size_t Arena::allocated() const
{
    return 0;                                            // Nothing allocated
}

/**
 *  Return the maximum or 'peak' number of bytes that have been allocated from
 *  this Arena since either it was first constructed or last reset.
 */
size_t Arena::peakUsage() const
{
    return 0;                                            // Nothing allocated
}

/**
 *  Return the number of individual allocations that are still in use,  or are
 *  'live'. An allocation is live if it has not yet been recycled by the Arena
 *  from which it was allocated, nor has that Arena since been reset.
 */
size_t Arena::allocations() const
{
    return 0;                                            // Nothing allocated
}

/**
 *  Return true if this Arena supports every one of the features appearing in
 *  the given bitfield.
 */
bool Arena::supports(features_t features) const
{
    return (features & ~finalizing) == 0;                // Finalizers handled
}

/**
 *  Update the Arena monitor with a snapshot of our allocation statistics, and
 *  associate this snapshot with the given label.
 */
void Arena::checkpoint(name_t label) const
{
    Monitor::getInstance().update(*this,label);          // Update the monitor
}

/**
 *  Insert a formatted representation of the Arena onto the output stream 'o'.
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
            if (ArenaPtr p = arena.parent())             // Has parent arena?
            {
                traverse(*p);                            // ...traverse parent
                *this += '/';                            // ...append a slash
            }

            *this += arena.name();                       // Append arena name
        }

        pathname(const Arena& arena) {traverse(arena);}  // Traverse ancestors
    };

    o << "name=\""      << pathname(*this)      << "\"," // Emit the name
      << "available="   << bytes_t(available()) << ","   // Emit available()
      << "allocated="   << bytes_t(allocated()) << ","   // Emit allocated()
      << "peakUsage="   << bytes_t(peakUsage()) << ","   // Emit peakUsage()
      << "allocations=" << allocations();                // Emit allocations()
}

/**
 *  Allocate 'n' bytes of raw storage from the Arena.
 *
 *  The resulting address is correctly aligned to hold one or more doubles.
 *
 *  A value of 0 for the size 'n' is acceptable, and returns a unique pointer,
 *  as required by operator new().
 *
 *  @remarks    Be sure never to pass the resulting allocation directly to the
 *  function Arena::destroy(), which would attempt to retrieve a finalizer for
 *  the object that is not there - and most likely crash.  Instead, you should
 *  call either Arena::recycle() or better still, the associated free function
 *  arena::destroy(Arena&,void*).
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

    void* p = this->malloc(sizeof(Header::POD) + n);     // Allocate raw bytes

    return (new(p) Header::POD(n)) + 1;                  // Initialize header
}

/**
 *  Allocate 'n' bytes of raw memory from off the Arena and save the finalizer
 *  function 'f', to be applied to the allocation later when the new object is
 *  eventually destroyed.
 *
 *  The resulting address is correctly aligned to hold one or more doubles.
 *
 *  A value of 0 for the size 'n' is acceptable, and returns a unique pointer,
 *  as required by operator new().
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
        void* p = this->malloc(sizeof(Header::AS) + n);  // ...allocate memory

        return (new(p) Header::AS(n)) + 1;               // ...init the header
    }
    else                                                 // No, Custom Scalar
    {
        void* p = this->malloc(sizeof(Header::CS) + n);  // ...allocate memory

        return (new(p) Header::CS(n,f)) + 1;             // ...init the header
    }
}

/**
 *  Allocate an array of 'c' elements each of size 'n', and save the finalizer
 *  function 'f',  to be applied to every array element in turn when the array
 *  is eventually destroyed.
 *
 *  The resulting address is correctly aligned to hold one or more doubles.
 *
 *  A value of 0 for the size 'n' is acceptable, and returns a unique pointer,
 *  as required by operator new(). The function 'f' will be invoked with this
 *  pointer 'c' times when the allocation is later destroyed.
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
        struct local{static void nop(void*){}};          // ...no-op finalizer

        return this->allocate(0,f ? &local::nop: 0);     // ...needs a header?
    }

    if (c == 1)                                          // Scalar allocation?
    {
        return this->allocate(n,f);                      // ...handled earlier
    }

    if (n > unlimited/c)                                 // Too big to handle?
    {
        this->overflowed();                              // ...signal overflow
    }

    if (f == 0)                                          // Trivial destructor?
    {
        return this->allocate(c * n);                    // ...handled earlier
    }

    if (f == arena::allocated)                           // Allocated Vector?
    {
        void* p = this->malloc(sizeof(Header::AV) + c*n);// ...allocate memory

        return (new(p) Header::AV(n,c)) + 1;             // ...init the header
    }
    else                                                 // No, Custom Vector
    {
        void* p = this->malloc(sizeof(Header::CV) + c*n);// ...allocate memory

        return (new(p) Header::CV(n,f,c)) + 1;           // ...init the header
    }
}

/**
 *  Return the given allocation to the Arena to be recycled -  that is, reused
 *  in a subsequent allocation. Needless to say, it is an error for the caller
 *  to make any use of the argument beyond this point. If the allocation needs
 *  to be finalized then use destroy() instead: recycle() doesn't finalize its
 *  argument. Notice also that arenas are not obliged to honor this request at
 *  all: class ScopedArena, for example, ignores the request to recycle memory
 *  in favor of freeing the allocations en masse when reset() is later called.
 */
void Arena::recycle(void* payload)
{
    if (payload != 0)                                    // Something to do?
    {
        Header& h(Header::retrieve(payload));            // ...retrieve header

        assert(h.getFinalizer() == 0);                   // ...no! use destroy

        byte_t* p = h.getAllocation();                   // ...get allocation
        size_t  n = h.getOverallSize();                  // ...and its length

        this->free(p,n);                                 // ...free the memory
    }
}

/**
 *  Finalize the given  allocation and return it to the Arena to be recycled -
 *  that is, reused in a subsequent allocation. Needless to say, it's an error
 *  for the caller to make any use of the allocation beyond this point. If the
 *  allocation does not need to be finalized then call recycle() instead. Note
 *  also that although arenas are obliged to implement this function to invoke
 *  the finalizer, they're not required to recycle the allocation immediately:
 *  they may instead prefer to wait until reset() is later called.
 */
void Arena::destroy(void* payload,count_t count)
{
    if (payload != 0)                                    // Something to do?
    {
        Header& h(Header::retrieve(payload));            // ...retrieve header

        assert(h.getFinalizer() != 0);                   // ...no! use recycle

        byte_t* p = h.getAllocation();                   // ...get allocation
        size_t  n = h.getOverallSize();                  // ...and its length

        h.finalize(count);                               // ...finalize array

        this->free(p,n);                                 // ...free the memory
    }
}

/**
 *  Reset the Arena to its originally constructed state, destroying any extant
 *  objects, recycling their underlying storage for use in future allocations,
 *  and resetting the allocation statistics to their default values.  An Arena
 *  must implement at least one of the members reset() or destroy() / recycle()
 *  or it will leak memory; it may also wish to implement both, however.
 */
void Arena::reset()
{}

/**
 *  Allocate 'size' bytes of raw storage.
 *
 *  'size' may not be zero.
 *
 *  The resulting address is correctly aligned to hold one or more doubles.
 */
void* Arena::malloc(size_t size)
{
    assert(size != 0);                                   // Validate argument

    return this->doMalloc(align(size));                  // Align and allocate
}

/**
 *  Allocate 'size' * 'count' bytes of raw storage.
 *
 *  Neither 'size' nor 'count' may be zero.
 *
 *  Throws an exception if the product 'size' * 'count' exceeds 'unlimited'.
 *
 *  The resulting address is correctly aligned to hold one or more doubles.
 */
void* Arena::malloc(size_t size,count_t count)
{
    assert(size!=0 && count!=0);                         // Validate arguments

    if (size > unlimited/count)                          // Too big to handle?
    {
        this->overflowed();                              // ...signal overflow
    }

    return this->doMalloc(align(size * count));          // Align and allocate
}

/**
 *  Allocate 'size' bytes of zero-initialized storage.
 *
 *  'size' may not be zero.
 *
 *  The resulting address is correctly aligned to hold one or more doubles.
 */
void* Arena::calloc(size_t size)
{
    assert(size != 0);                                   // Validate argument

    return memset(this->malloc(size),0,size);            // Allocate and clear
}

/**
 *  Allocate 'size' * 'count' bytes of zero-initialized storage.
 *
 *  Neither 'size' nor 'count' may be zero.
 *
 *  Throws an exception if the product 'size' * 'count' exceeds 'unlimited'.
 *
 *  The resulting address is correctly aligned to hold one or more doubles.
 */
void* Arena::calloc(size_t size,count_t count)
{
    assert(size!=0 && count!=0);                         // Validate arguments

    if (size > unlimited/count)                          // Too big to handle?
    {
        this->overflowed();                              // ...signal overflow
    }

    return memset(this->malloc(size * count),0,size);    // Allocate and clear
}

/**
 *  Copy the string 's', including its terminating null, into memory allocated
 *  from within this arena.
 *
 *  When returning the allocation to a recycling arena, use a call such as:
 *  @code
 *      arena.free(s,strlen(s) + 1);                     // Don't forget null
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
 *      arena.free(s,strlen(s) + 1);                     // Don't forget null
 *  @endcode
 */
char* Arena::strdup(const std::string& s)
{
    size_t n = s.size() + 1;                             // Size of duplicate

    return (char*)memcpy(this->malloc(n),s.c_str(),n);   // Allocate and copy
}

/**
 *  Free the memory that was allocated earlier from this same Arena by calling
 *  malloc() and attempt to recycle it for future reuse to reclaim up to 'size'
 *  bytes of raw storage.  No promise is made however as to *when* this memory
 *  will be made available again:  the Arena may, for example, prefer to defer
 *  recycling until a subsequent call to reset() is made.
 */
void Arena::free(void* payload,size_t size)
{
    assert(payload!=0 && size!=0);                       // Validate arguments

 /* Adjust 'size' to reflect the actual size we allocated earlier; in general
    this may be slightly larger than the size orginally requested of malloc()
    because we rounded it up to align the storage to hold doubles...*/

    this->doFree(payload,align(size));                   // Align and free
}

/**
 *  @fn void* Arena::doMalloc(size_t size) = 0
 *
 *  Allocate 'size' bytes of raw storage. The given size must be a multiple of
 *  `sizeof(double)`, and cannot be zero.
 *
 *  The resulting allocation must eventually be returned to this same Arena by
 *  calling doFree(), and with the same value for 'size'.
 */

/**
 *  @fn void Arena::doFree(void* p,size_t size) = 0
 *
 *  Free the memory that was allocated earlier from this same Arena by calling
 *  malloc() and attempt to recycle it for future reuse to reclaim up to 'size'
 *  bytes of raw storage.  No promise is made however as to *when* this memory
 *  will be made available again:  the Arena may, for example, prefer to defer
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
 *  Throw an Exhausted exception to indicate that this Arena has been asked to
 *  allocate 'size' bytes of memory and that this exceeds the Arena's internal
 *  limit.
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
 *  Construct and return a concrete Arena that supports the features specified
 *  in the options structure 'o'.
 *
 *  This is the preferred way to construct an Arena, as opposed to including a
 *  header file and invoking the constructor directly, because it isolates the
 *  caller from needing to know the exact type of the Arena that they receive.
 */
ArenaPtr newArena(Options o)
{
    if (o.debugging()&& !o.parent()->supports(debugging))// Debugging support?
    {
        o.parent(newDebugArena(o));                      // ...add debug arena
    }

    if (o.resetting()&& !o.parent()->supports(resetting))// Resetting support?
    {
        return newScopedArena(o);                        // ...the only choice
    }

    return newLimitedArena(o);                           // The default choice
}

/****************************************************************************/
}}
/****************************************************************************/
