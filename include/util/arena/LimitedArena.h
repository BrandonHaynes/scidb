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

#ifndef UTIL_ARENA_LIMITED_ARENA_H_
#define UTIL_ARENA_LIMITED_ARENA_H_

/****************************************************************************/

#include <util/Arena.h>                                  // For Arena

/****************************************************************************/
namespace scidb { namespace arena {
/****************************************************************************/
/**
 *  @brief      Limits the memory that can be allocated from another %arena.
 *
 *  @details    Class LimitedArena constrains the amount of memory that can be
 *              requested of its parent by maintaining a count of the bytes it
 *              has allocated so far and by throwing an exception if the count
 *              ever exceeds a certain preset limit.
 *
 *              The limit is specified at construction using the 'limit' field
 *              of the options structure. For example:
 *  @code
 *                  b = newArena(Options("B").limit(1*GB).parent(a));
 *  @endcode
 *              creates a new %arena 'b' that is permitted to allocate as much
 *              as a gibibyte of memory from the %arena 'a' before throwing an
 *              arena::Exhausted exception.  Clients can track the memory that
 *              is still available by calling available(),  and can also catch
 *              the exception if they attempt to allocate beyond this limit.
 *
 *              Bear in mind that other arenas may also be allocating from the
 *              the same parent too:  the limit affects only those allocations
 *              made through *this* %arena, while others,  perhaps with larger
 *              internal limits, may satisfy requests where this one fails to.
 *              The parent, of course, may also carry a limit of its own too.
 *
 *              Thus the arenas form a sort of tree,  with requests for memory
 *              flowing in at the leaves, propagating their way up towards the
 *              root, and being checked and monitored all along their journey.
 *
 *  @see        Class Exhausted for more on recovering from an out-of-memory
 *              exception.
 *
 *  @author     jbell@paradigm4.com.
 */
class LimitedArena : public Arena
{
 public:                   // Construction
                              LimitedArena(const Options&);

 public:                   // Attributes
    virtual name_t            name()               const {return _name.c_str();}
    virtual ArenaPtr          parent()             const {return _parent;}
    virtual size_t            available()          const {return _available;}
    virtual size_t            allocated()          const {return _allocated;}
    virtual size_t            peakusage()          const {return _peakusage;}
    virtual size_t            allocations()        const {return _allocations;}
    virtual features_t        features()           const;

 public:                   // Operations
    virtual void              reset();

 public:                   // Implementation
    virtual void*             doMalloc(size_t);
    virtual void              doFree  (void*,size_t);

 protected:                // Implementation
            bool              consistent()         const;

 protected:                // Representation
       std::string      const _name;                     // The arena name
            size_t      const _limit;                    // The preset limit
            ArenaPtr    const _parent;                   // The parent arena
            size_t            _available;                // Bytes available
            size_t            _allocated;                // Bytes allocated
            size_t            _peakusage;                // High water mark
            size_t            _allocations;              // Total allocations
};

/****************************************************************************/
}}
/****************************************************************************/
#endif
/****************************************************************************/
