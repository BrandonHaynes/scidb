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

#include <system/Constants.h>                        // For KiB, MiB, and GiB
#include "ArenaDetails.h"                            // For implementation

/****************************************************************************/
namespace scidb { namespace arena {
/****************************************************************************/

/**
 *  Insert a formatted representation of this %arena allocated object onto the
 *  given output stream.  Subclasses overide this virtual function to simplify
 *  debugging memory leaks; if the DebugArena detects a leak, it will walk its
 *  list of extant allocations and, for Allocated objects at least, write some
 *  sort of string representation to the console, debugger, log, or whatever,
 */
void Allocated::insert(std::ostream&) const
{}

/**
 *  Insert a formatted representation of the %arena 'a' onto the ostream 'o'.
 *
 *  Notice that we handle the formatting of the feature flags here rather than
 *  in the arena itself; our use of the decorator pattern to implement some of
 *  the features means that the arena being decorated does not actually 'know'
 *  the full set of features it is supporting.
 */
std::ostream& operator<<(std::ostream& o,const Arena& a)
{
    o << '{';                                            // Add opening brace
    a.insert(o);                                         // Add arena  itself
    o << ",features=\"";                                 // Add a field label

    if (a.supports(finalizing)) o << 'F';                // Add finalizer flag
    if (a.supports(recycling))  o << 'C';                // Add recycling flag
    if (a.supports(resetting))  o << 'S';                // Add resetting flag
    if (a.supports(debugging))  o << 'D';                // Add debugging flag
    if (a.supports(threading))  o << 'T';                // Add threading flag

    return o << "\"}";                                   // Add trailing brace
}

/**
 *  Insert a formatted representation of the %arena allocated object 'a' onto
 *  the ostream 'o'.
 */
std::ostream& operator<<(std::ostream& o,const Allocated& a)
{
    return o << '{', a.insert(o), o << '}';              // Insert and return
}

/**
 *  Insert a formatted representation of the Exhausted exception 'e' onto the
 *  ostream 'o'.
 */
std::ostream& operator<<(std::ostream& o,const Exhausted& e)
{
    return o << e.what();                                // Insert and return
}

/**
 *  Format the integer 'bytes' as a number of bytes of raw memory and print it
 *  to the output stream 'o'.
 */
std::ostream& operator<<(std::ostream& o,bytes_t bytes)
{
    o.precision(2);                                      // Set decimal places

    if (bytes >= unlimited)                              // Is it gigUndous?
    {
        return o << "unlimited";                         // ...as 'unlimited'
    }
    else
    if (bytes >= GiB)                                    // In gibibyte range?
    {
        return o << float(bytes)/GiB << "GiB";           // ...as gibibytes
    }
    else
    if (bytes >= MiB)                                    // In mebibyte range?
    {
        return o << float(bytes)/MiB << "MiB";           // ...as mebibytes
    }
    else
    if (bytes >= KiB)                                    // In kibibyte range?
    {
        return o << float(bytes)/KiB << "KiB";           // ...as kibibytes
    }
    else                                                 // No special prefix
    {
        return o << size_t(bytes)    << "B";             // ...as plain bytes
    }
}

/**
 *  Format the integer 'words' as a number of bytes of raw memory and print it
 *  to the output stream 'o'.
 */
std::ostream& operator<<(std::ostream& o,words_t words)
{
    return o << bytes_t(asBytes(words));                 // Convert to bytes_t
}

/**
 *  Return the current %arena associated with the current thread of execution.
 *
 *  An Allocator, like the managed containers that use it,  can be constructed
 *  by explicitly supplying the %arena from which it's to allocate from. There
 *  are a number of situations that arise, however, in which it is awkward, or
 *  even impossible, to do so. Consider, for example, an array of vectors:
 *
 *  @code
 *      ArenaPtr              a = ...                    // Abstract arena
 *      managed::vector<int>  v[...];                    // Array of vectors
 *  @endcode
 *
 *  For any type to serve as the element type of an array, it must be possible
 *  to construct its objects *without arguments*; the language simply does not
 *  provide the necessary syntax to specify that the %arena 'a' should be used
 *  to construct each element of 'v'.  What we appear to be bumping up against
 *  is the fact that the standard library was designed with incomplete support
 *  for what are now known as 'stateful allocators'.
 *
 *  The C++11 language improves this situation by adding the necessary support
 *  for stateful allocators,  introducing the concept of a 'scoped' allocator,
 *  providing the 'scoped_allocator_adaptor' template class, and by revisiting
 *  the design of each and every container class that carries an allocator. In
 *  particular, in C++11, containers can be made to propagate their allocators
 *  on down to their elements recursively.
 *
 *  In lieu of such complete language support, however, the current version of
 *  the %arena library adopts a somewhat simpler approach to the problem: each
 *  thread maintains a stack of %arena pointers and the default constructor of
 *  class Allocator is implemented to grab the top %arena from the stack via a
 *  call to getArena().  The stack itself is manipulated by instances of class
 *  Scope, which push and pop the current thread's stack as they come into and
 *  go out of existence and thus synchronize the thread local arena stack with
 *  the lexical scope in which they are instantiated - hence the name.
 *
 *  Interestingly,  the Boost library appears to have an implementation of the
 *  standard container classes - the Boost Container library - that works with
 *  both stateful and scoped allocators: one that even compiles under C++98. A
 *  future version of the %arena library may wish to adopt this implementation
 *  instead as a step towards supporting compilation under C++11.
 *
 *  @see    http://www.stroustrup.com/C++11FAQ.html for a good introduction to
 *  scoped allocators.
 *
 *  @see    http://www.boost.org/doc/libs/1_54_0/doc/html/container.html for
 *  more on the Boost Container library.
 *
 *  Update: In 13.9 we upgraded to version 1.54 of the Boost library, and also
 *  reimplemented the managed  containers to use the  Boost Container library.
 *  In order to facilitate conversion of exisiting code to use the new managed
 *  containers, however, there's still a need to support the idea of a default
 *  or 'current' %arena, though it's not yet clear whether the model of a per-
 *  thread arena stack is the right one to adopt - it may well be more trouble
 *  than it is worth; in the mean time, and until we gain more experience with
 *  using the library, we prefer to simply return the root arena instead.
 */
ArenaPtr getArena()
{
    return getRootArena();                               // Return root arena
}

/****************************************************************************/
}}
/****************************************************************************/
