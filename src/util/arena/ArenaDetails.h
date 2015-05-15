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
#include <boost/strong_typedef.hpp>                      // For STRONG_TYPEDEF
#include <util/Arena.h>                                  // For Arena

/****************************************************************************/
namespace scidb { namespace arena {
/****************************************************************************/

ArenaPtr        getRootArena    ();                      // The singleton root
ArenaPtr        newLimitedArena (const Options&);        // Supports limiting
ArenaPtr        newScopedArena  (const Options&);        // Supports resetting
ArenaPtr        newLeaArena     (const Options&);        // Supports resetting+recycling
ArenaPtr        addDebugging    (const Options&);        // Supports debugging
ArenaPtr        addThreading    (const Options&);        // Supports threading

/****************************************************************************/

BOOST_STRONG_TYPEDEF(size_t,bytes_t)                     // Bytes of storage
BOOST_STRONG_TYPEDEF(size_t,words_t)                     // Words of storage

std::ostream&   operator<<      (std::ostream&,bytes_t); // Customize printing
std::ostream&   operator<<      (std::ostream&,words_t); // Customize printing

/****************************************************************************/
namespace alignment {
/****************************************************************************/

const size_t size = sizeof(alignment_t);                 // Alignment boundary
const size_t mask = size - 1;                            // Low order bit mask
const size_t bits = 3;                                   // Bits known to be 0

/**
 *  Confirm that 'bits' is indeed the base 2 logarithm of our chosen alignment
 *  size, and thus in particular that this size is a power of 2.
 *
 *  This fact justifies our use of the '&' operator to quickly round up to the
 *  nearest multiple of the alignment size, as well as to test that an address
 *  is correctly aligned.
 *
 *  It follows that the 'bits' low order bits of each properly aligned address
 *  must always be clear, and are therefore available for use in storing other
 *  information, such as allocation status flags or whatnot.
 */
BOOST_STATIC_ASSERT(size == 1<<bits);                    // Check size==2^bits
/****************************************************************************/
}
/****************************************************************************/

size_t          asBytes         (size_t);                // Map words to bytes
size_t          asWords         (size_t);                // Map bytes to words
size_t          align           (size_t);                // Adjust the request
bool            aligned         (uintptr_t);             // Is proper multiple?
bool            aligned         (const void*);           // Is memory aligned?

/****************************************************************************/

/**
 *  Return the number of bytes needed to represent the given number of words,
 *  each of which has size 'alignment::size' bytes.
 */
inline size_t asBytes(size_t words)
{
    return words << alignment::bits;                     // Multiply by 'size'
}

/**
 *  Return the number of 'alignment::size' words needed to represent an object
 *  of the given size.
 *
 *  In particular, notice that 'words(n) == words(align(n))' for positive 'n'.
 */
inline size_t asWords(size_t bytes)
{
    return (bytes + alignment::mask) >> alignment::bits; // Round, then divide
}

/**
 *  Round 'size' up to the nearest multiple of the alignment size.
 *
 *  'size' may not be zero.
 *
 *  @see http://stackoverflow.com/questions/1766535/bit-hack-round-off-to-multiple-of-8
 */
inline size_t align(size_t size)
{
    assert(size != 0);                                   // Validate argument

    return (size + alignment::mask) & ~alignment::mask;  // Round the size up
}

/**
 *  Return true if 'size' is a proper multiple of the alignment size.
 */
inline bool aligned(uintptr_t size)
{
    return size!=0 && (size & alignment::mask)==0;       // Are low bits zero?
}

/**
 *  Return true if the allocation pointed to by 'pv' is non-null and correctly
 *  aligned.
 */
inline bool aligned(const void* pv)
{
    return aligned(reinterpret_cast<uintptr_t>(pv));     // Are low bits zero?
}

/****************************************************************************/
}}
/****************************************************************************/
#endif
/****************************************************************************/
