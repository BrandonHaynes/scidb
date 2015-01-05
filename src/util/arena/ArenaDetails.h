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

#ifndef UTIL_ARENA_ARENA_DETAILS_H_
#define UTIL_ARENA_ARENA_DETAILS_H_

/****************************************************************************/

#include <stdint.h>                                      // For uintptr_t
#include <endian.h>                                      // For __BYTE_ORDER
#include <boost/strong_typedef.hpp>                      // For STRONG_TYPEDEF
#include <util/Arena.h>                                  // For Arena

/****************************************************************************/
namespace scidb { namespace arena {
/****************************************************************************/

ArenaPtr        getRootArena    ();
ArenaPtr        newDebugArena   (const Options&);
ArenaPtr        newScopedArena  (const Options&);
ArenaPtr        newLimitedArena (const Options&);

/****************************************************************************/

BOOST_STRONG_TYPEDEF(size_t,bytes_t)                     // Bytes of storage

std::ostream&   operator<<      (std::ostream&,bytes_t); // Customize printing

/****************************************************************************/

size_t          align           (size_t);                // Align the request
bool            isAligned       (size_t);                // No need to align?
bool            isAligned       (const void*);           // Is memory aligned?

/****************************************************************************/

/**
 *  Round the given allocation request  up to the nearest multiple of the size
 *  of a double in order to ensure that the subsequent allocation is correctly
 *  aligned.
 */
inline size_t align(size_t size)
{
    size_t const a = alignment;                          // Payload alignment

    return (size + a - 1) / a * a;                       // Round up by 'a'
}

/**
 *  Return true if the given size is a multiple of the alignment size; that is
 *  it has already been run through align() and hence is a suitable allocation
 *  request for doMalloc(). Our goal here is to arrange for the alignment math
 *  to be computed just once, before calling doMalloc(), instead of repeatedly
 *  with each and every call to doMalloc() as the request makes its way up the
 *  parent Arena chain.
 */
inline bool isAligned(size_t size)
{
    return size % sizeof(double) == 0;                   // Already aligned?
}

/**
 *  Return true if the allocation pointed to by 'p' is correctly aligned.
 *
 *  @todo   Add alternate implementations for other endian architectures.
 *
 *  @see    http://stackoverflow.com/questions/1898153/how-to-determine-if-memory-is-aligned-testing-for-alignment-not-aligning
 */
inline bool isAligned(const void* p)
{
    assert(__BYTE_ORDER == __LITTLE_ENDIAN);             // Little endian only

    return (reinterpret_cast<uintptr_t>(p)%alignment)==0;// Alignment divides?
}

/****************************************************************************/
}}
/****************************************************************************/
#endif
/****************************************************************************/
