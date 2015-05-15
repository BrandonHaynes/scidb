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

#ifndef UTIL_ARENA_ARENA_HEADER_H_
#define UTIL_ARENA_ARENA_HEADER_H_

/****************************************************************************/

#include <limits>                                        // For numeric_limits
#include "ArenaDetails.h"                                // For implementation

/****************************************************************************/
namespace scidb { namespace arena {
/****************************************************************************/
/**
 *  @brief      Describes the layout of an individual memory allocation.
 *
 *  @details    Class Header describes the  layout and  finalization semantics
 *              of the allocation in which it sits. Its actual size depends on
 *              the manner in which the allocation is to be finalized and this
 *              in turn is described by a bit field of flags:
 *
 *              - @b finalizer: the allocation (still) requires finalization.
 *
 *              - @b customFinalizer:  a pointer to the finalizer is stored in
 *              the allocation header itself.
 *
 *              - @b vectorFinalizer: the finalizer should be applied to every
 *              element of a vector whose size is stored within the allocation
 *              header itself.
 *
 *              The goal here is to ensure that only allocations that actually
 *              require custom and/or vector finalization need pay the cost of
 *              supporting these features.
 *
 *              An allocation is laid out as follows:
 *  @code
 *              |- - - - - - - - - - H e a d e r - - - - - - - - -|
 *              [ElementCount] [Finalizer] [ElementSize:61;Flags:3] Payload...
 *  @endcode
 *              where:
 *
 *              - @b Flags is a bit field of flags describing the finalization
 *              semantics of the allocation as above.
 *
 *              - @b ElementSize   (size_t) is the size of each vector element
 *              in bytes.
 *
 *              - @b ElementCount (count_t) is the length of the vector; it is
 *              present only if the 'vectorFinalizer' flag is set; if not, the
 *              value '1' is implied.
 *
 *              - @b Finalizer (finalizer_t) carries a pointer to the function
 *              with which each element is to be finalized; it is present only
 *              if the 'customFinalizer' flag is set: if not,the special value
 *              'allocated' is implied if the 'finalizer' flag is set, and the
 *              value '0' is implied otherwise.
 *
 *              The header constructor is protected; rather than construct the
 *              header directly, an %arena constructs (using the placement new
 *              operator) one of 5 nested types that in turn embed the Header;
 *              for example:
 *  @code
 *                  void* p = doMalloc(size + ...);      // Perform allocation
 *
 *                  new(p) Header::CS(size,finalizer);   // Fill in the header
 *  @endcode
 *              because this gives us a type whose size we can take at compile
 *              time (with the sizeof operator) and a constructor that accepts
 *              the proper arguments and that initializes the (dynamic) fields
 *              for us correctly, while all the while ensuring that the header
 *              flags remain at the *back* of the structure, directly in front
 *              of the actual allocation payload, thus allowing us to retrieve
 *              the header later when given only the payload pointer (say in a
 *              call to recycle(), for example).
 *
 *  @author     jbell@paradigm4.com.
 */
class Header
{
 public:                   // Header Types
    struct  POD;                                         // Plain Old Data
    struct  AS;                                          // Allocated Scalar
    struct  AV;                                          // Allocated Vector
    struct  CS;                                          // Custom    Scalar
    struct  CV;                                          // Custom    Vector

 public:                   // Attributes
            size_t            getHeaderSize()     const; // h
            size_t            getPayloadSize()    const; // n * c
            size_t            getOverallSize()    const; // h + n * c
            size_t            getElementSize()    const; // n
            size_t            getElementCount()   const; // c
            finalizer_t       getFinalizer()      const; // f
      const byte_t*           getPayload()        const; // this + 1
            byte_t*           getPayload();              // this + 1
            byte_t*           getAllocation();           // this - h

 public:                   // Operations
    static  Header&           retrieve(void*);
            void              finalize(count_t = unlimited);

 protected:                // Attribute Flags
    enum                      {
                                    finalizer = 1,       // Has a finalizer?
                              customFinalizer = 2,       // Has custom finalizer?
                              vectorFinalizer = 4        // Has element count?
                              };

    typedef byte_t            flags_t;                   // Bitfield of flags

 protected:                // Implementation
                              Header(size_t,flags_t);
            bool              has(flags_t)        const; // Tests for flags
            bool              consistent()        const; // Check consistency

 protected:                // Implementation
   template<class field>
    static        field*      rewind(void*);
   template<class field>
    static  const field*      rewind(const void*);

 protected:                // Representation
            size_t            _size  : std::numeric_limits<size_t>::digits - 3;
            size_t            _flags : 3;                // Attribute flags
};

/**
 * @class Header::POD
 *
 * Class POD (Plain Old Data) describes the layout of an allocation that needs
 * no finalization at all,  so neither a custom finalizer pointer nor a vector
 * length need be stored. This is the simplest and most compact header type.
 *
 * @class Header::AS
 *
 * Class AS (Allocated Scalar) describes the layout of a scalar object that is
 * derived from class Allocated,  hence we *know* the address of the finalizer
 * and need not store it at runtime.
 *
 * @class Header::AV
 *
 * Class AV (Allocated Vector) describes the layout of a vector of objects all
 * derived from class Allocated,  hence we *know* the address of the finalizer
 * and need not store it at runtime. We must, however, store the length of the
 * vector so that we know how many times to apply the ~Allocated() function.
 *
 * @class Header::CS
 *
 * Class CS (Custom Scalar) describes the layout of scalar object that must be
 * finalized by applying a user defined function whose address is saved within
 * the header itself.
 *
 * @class Header::CV
 *
 * Class CV  (Custom Vector)  describes the layout of a vector of objects that
 * are to  be finalized by applying a user supplied function whose  address is
 * stored within the header itself. In addition, the header records the length
 * of this vector. This is the largest and most complex type of header.
 */
struct Header::POD{                          Header _h;POD(size_t n,finalizer_t=0,count_t=1):            _h(n,0)                                        {}};
struct Header::AS {                          Header _h;AS (size_t n,finalizer_t=0,count_t=1):            _h(n,finalizer)                                {}};
struct Header::AV {count_t _c;               Header _h;AV (size_t n,finalizer_t  ,count_t c):_c(c),      _h(n,finalizer|vectorFinalizer)                {}};
struct Header::CS {           finalizer_t _f;Header _h;CS (size_t n,finalizer_t f,count_t=1):      _f(f),_h(n,finalizer|customFinalizer)                {}};
struct Header::CV {count_t _c;finalizer_t _f;Header _h;CV (size_t n,finalizer_t f,count_t c):_c(c),_f(f),_h(n,finalizer|vectorFinalizer|customFinalizer){}};

/**
 *  Construct the header for an allocation with element size 'n' and finalizer
 *  flags 'f'.
 */
inline Header::Header(size_t n,flags_t f)
             : _size (n),
               _flags(f)
{
    assert(_size==n && _flags==f);                       // Check they fit ok
    assert(consistent());                                // Check consistency
}

/**
 *  Return the actual size of the allocation header,  taking into account the
 *  dynamic 'finalizer' and 'element_count' attributes that may or may not be
 *  present, according to which bits in the '_flags' bitfield are set.
 */
inline size_t Header::getHeaderSize() const
{
    switch (_flags & (customFinalizer|vectorFinalizer))  // Which attributes?
    {
        default:                              SCIDB_UNREACHABLE();
        case 0:                               return sizeof(AS);
        case vectorFinalizer:                 return sizeof(AV);
        case customFinalizer:                 return sizeof(CS);
        case customFinalizer|vectorFinalizer: return sizeof(CV);
    }
}

/**
 *  Return the size of the user visible area of the allocation (the 'payload')
 *  which starts immediately beyond the header.
 */
inline size_t Header::getPayloadSize() const
{
    return getElementCount() * getElementSize();         // Size of user area
}

/**
 *  Return the overall size of the allocation, which includes both the header
 *  and its payload.
 */
inline size_t Header::getOverallSize() const
{
    return getHeaderSize() + getPayloadSize();           // Size of allocation
}

/**
 *  Return the size of each individual element of the vector; this is also the
 *  size of the entire payload if the 'vectorFinalizer' flag is cleared.
 */
inline size_t Header::getElementSize() const
{
    return _size;                                        // Size of an element
}

/**
 *  Return a pointer to the start of the user visible area of the allocation.
 */
inline const byte_t* Header::getPayload() const
{
    return reinterpret_cast<const byte_t*>(this + 1);    // Start of user area
}

/**
 *  Return a pointer to the start of the user visible area of the allocation.
 */
inline byte_t* Header::getPayload()
{
    return reinterpret_cast<byte_t*>(this + 1);          // Start of user area
}

/**
 *  Return a pointer to the start of the original allocation.
 */
inline byte_t* Header::getAllocation()
{
    byte_t* p = reinterpret_cast<byte_t*>(this);         // Cast to raw bytes

    return p - (getHeaderSize() - sizeof(Header));       // Rewind past header
}

/**
 *  Return a reference to the allocation header from a pointer to its payload.
 */
inline Header& Header::retrieve(void* payload)
{
    assert(rewind<Header>(payload)->consistent());       // Check it looks ok

    return *rewind<Header>(payload);                     // Rewind past header
}

/**
 *  Return a pointer to the dynamic field that lies immediately in front of the
 *  location currently pointed to by 'p'.
 */
template<class field_t>
inline field_t* Header::rewind(void* p)
{
    assert(aligned(p));                                  // Validate argument

    return static_cast<field_t*>(p) - 1;                 // Rewind past field
}

/**
 *  Return a pointer to the dynamic field that lies immediately in front of the
 *  location currently pointed to by 'p'.
 */
template<class field_t>
inline const field_t* Header::rewind(const void* p)
{
    assert(aligned(p));                                  // Validate argument

    return static_cast<const field_t*>(p) - 1;           // Rewind past field
}

/**
 *  Return true if all of the given flags are set.
 */
inline bool Header::has(flags_t flags) const
{
    return (_flags & flags) == flags;                    // Are flags all set?
}

/**
 *  Carve a block of memory from the given %arena that is large enough to hold
 *  a payload of 'c' objects of size 'n' bytes,  each being finalized with the
 *  function 'f', establish a header of the given type at the front of the new
 *  block, and return this new payload.
 */
template<class header>
inline void* carve(Arena& arena,size_t n,finalizer_t f = 0,count_t c = 1)
{
    assert(n <= (unlimited - sizeof(header)) / c);       // Validate arguments
    void* a = arena.doMalloc(sizeof(header) + n * c);    // Perform allocation
    void* p = (new(a) header(n,f,c)) + 1;                // Fill in the header

    assert(aligned(p));                                  // Check it's aligned
    return p;                                            // Return the payload
}

/****************************************************************************/
}}
/****************************************************************************/
#endif
/****************************************************************************/
