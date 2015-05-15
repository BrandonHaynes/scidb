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

#ifndef UTIL_ARENA_SCOPED_ARENA_H_
#define UTIL_ARENA_SCOPED_ARENA_H_

/****************************************************************************/

#include <util/arena/LimitedArena.h>                     // For LimitedArena
#include <deque>                                         // For deque

/****************************************************************************/
namespace scidb { namespace arena {
/****************************************************************************/
/**
 *  @brief      A resetting %arena that defers recycling of memory until it is
 *              either reset or destroyed.
 *
 *  @details    Class ScopedArena implements an efficient resetting arena that
 *              allocates memory from within fixed size pages that are held on
 *              a list; when the %arena is reset, the pages are recycled. This
 *              can result in very fast allocations - a ScopedArena frequently
 *              outperforms the default allocator - at the expense of possibly
 *              holding memory alive for longer than is strictly necessary.
 *
 *              The page size is specified at construction with the 'pagesize'
 *              field of the options structure, while the parent %arena - from
 *              which the actual pages of storage are allocated - is specified
 *              with the 'parent' field of the structure. For example:
 *  @code
 *                  b = newArena(Options("B").pagesize(1024).parent(a));
 *  @endcode
 *              creates a new arena 'b' that allocates memory 1024 bytes at a
 *              time from the arena 'a', then sub-allocates from within these
 *              fixed size pages.
 *
 *              While it is perfectly fine to return an allocation to an arena
 *              by calling recycle(), you should be aware that the ScopedArena
 *              will silently ignore this request. Similarly, although you may
 *              return an object requiring finalization to an arena by calling
 *              destroy(), the ScopedArena will directly invoke the finalizer,
 *              but will continue to hold the underlying allocation live until
 *              either reset() is called or the %arena itself is destroyed.
 *
 *              The ScopedArena also supports the same monitoring and limiting
 *              capabilities as class LimitedArena, from which it inherits.
 *
 *  @remarks    Also known as a Region, Zone, Area, and Stack Allocator.
 *
 *  @see        http://en.wikipedia.org/wiki/Region_allocation.
 *
 *  @author     jbell@paradigm4.com.
 */
class ScopedArena : public LimitedArena
{
 public:                   // Construction
                              ScopedArena(const Options&);
    virtual                  ~ScopedArena();

 public:                   // Attributes
    virtual features_t        features()           const;
    virtual void              insert(std::ostream&)const;

 public:                   // Operations
    virtual void*             allocate(size_t);
    virtual void*             allocate(size_t,finalizer_t);
    virtual void*             allocate(size_t,finalizer_t,count_t);
    virtual void              recycle(void*);
    virtual void              reset();

 public:                   // Implementation
    virtual void*             doMalloc(size_t);
    virtual void              doFree  (void*,size_t);

 protected:                // Implementation
            bool              consistent()         const;

 protected:                // Representation
       std::deque<void*>       _list;                    // The list of blocks
            size_t      const _size;                     // The size of a page
            byte_t*           _next;                     // The next available
            byte_t*           _last;                     // The last in page
};

/****************************************************************************/
}}
/****************************************************************************/
#endif
/****************************************************************************/
