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

#ifndef UTIL_POINTER_RANGE_H_
#define UTIL_POINTER_RANGE_H_

/****************************************************************************/

#include <boost/range/iterator_range.hpp>                // For iterator_range
#include <boost/operators.hpp>                           // For totally_ordered
#include <string>                                        // For std::string
#include <util/arena/Vector.h>                           // For mgd::vector

/****************************************************************************/
namespace scidb {
/****************************************************************************/
/**
 *  @brief      Represents a sub-sequence within some other native array.
 *
 *  @details    Class PointerRange<value>  models the boost 'range' concept as
 *              a pair of raw pointers into a native array of 'value's -  that
 *              is,  a contiguous block of storage owned by some other entity,
 *              such as a C style array or an std::vector<value>, for example.
 *              It extends boost::iterator_range<value> to enable construction
 *              from various containers that  themselves manage the underlying
 *              storage - it posesses reference semantics -  and so represents
 *              a sort of 'view' into some interesting part of their sequence.
 *
 *              It's principle utility is as:
 *
 *              -  an argument type for a function that requires a sequence of
 *              values and that would otherwise be expressed as a template, or
 *              else require specifying its argument type more concretely than
 *              is really necessary, perhaps as a std::vector<>, for example.
 *
 *              -  the return type for a function that computes an interesting
 *              subrange of an array that outlives its call,such as a function
 *              that computes a substring, for example.
 *
 *              Consider, for example, the function foo():
 *  @code
 *                  typedef PointerRange<Coordinate const> coords;
 *
 *                  coords foo(coords);
 *  @endcode
 *              which is callable with:
 *  @code
 *                  Coordinate              a   = ..
 *                  Coordinate              b[] = {13, 78, ...};
 *                  std::vector<Coordinate> c   = boost::list_of(..)(..)
 *
 *                  foo(coords());          // An empty range
 *                  foo(a)                  // A single value
 *                  foo(b)                  // A stack allocated array
 *                  foo(c)                  // A vector of values
 *                  foo(foo(b))             // A portion of b's values
 *                  foo(drop(b,3,5))        // A portion of b's values
 *                  foo(subrange(c,0,2))    // A portion of c's values
 *  @endcode
 *              Notice that *none* of these calls  require either constructing
 *              or copying an std::vector - they are fast and cheap - and that
 *              foo() need not be a template - it could, in fact, be a virtual
 *              function, for example.
 *
 *  @see        http://www.boost.org/doc/libs/1_53_0/libs/range/doc/html/index.html
 *              for details of the range concept.
 *
 *  @author     jbell@paradigm4.com
 */
template<class value>
class PointerRange : public boost::iterator_range<value*>,
                            boost::totally_ordered<PointerRange<value> >
{
 private:                  // Infer constness of value
    template<class v>
    struct t               // ...for mutable ranges
    {
        typedef std::vector<v>                  std_vector_type;
        typedef mgd::vector<v>                  mgd_vector_type;
        typedef std::basic_string<v>            std_string_type;
    };
    template<class v>
    struct t<const v>      // ...for constant ranges
    {
        typedef std::vector<v>            const std_vector_type;
        typedef mgd::vector<v>            const mgd_vector_type;
        typedef std::basic_string<v>      const std_string_type;
    };

 public:                   // Supporting types
    typedef boost::iterator_range<value*>       base_type;
    typedef typename t<value>::std_vector_type  std_vector_type;
    typedef typename t<value>::mgd_vector_type  mgd_vector_type;
    typedef typename t<value>::std_string_type  std_string_type;

 public:                   // Construction
                              PointerRange();
                              PointerRange(value&);
                              PointerRange(value*,value*);
                              PointerRange(size_t,value*);
                              PointerRange(std_vector_type&);
                              PointerRange(mgd_vector_type&);
                              PointerRange(std_string_type&);
    template<size_t size>     PointerRange(value(&)[size]);

 public:                   // Operations
                              operator PointerRange<value const>() const;

 public:                   // Operations
            size_t            size()            const;
            bool              consistent()      const;
};

/**
 *  Construct an empty range.
 */
template<class v>
inline PointerRange<v>::PointerRange()
                      : base_type(static_cast<v*>(0),static_cast<v*>(0))
{
    assert(consistent());                                // Check consistency
}

/**
 *  Construct a range from the single value 'r'.
 */
template<class v>
inline PointerRange<v>::PointerRange(v& r)
                      : base_type(&r,&r + 1)
{
    assert(consistent());                                // Check consistency
}

/**
 *  Construct a range from the sequence of values [i,e).
 */
template<class v>
inline PointerRange<v>::PointerRange(v* i,v* e)
                      : base_type(i,e)
{
    assert(consistent());                                // Check consistency
}

/**
 *  Construct a range from the sequence of 'n' values beginning at 'i'.
 */
template<class v>
inline PointerRange<v>::PointerRange(size_t n,v* i)
                      : base_type(i,i+n)
{
    assert(consistent());                                // Check consistency
}

/**
 *  Construct a range from the elements of the vector 'r'.
 */
template<class v>
inline PointerRange<v>::PointerRange(std_vector_type& r)
                      : base_type(&*r.begin(),&*r.end())
{
    assert(consistent());                                // Check consistency
}

/**
 *  Construct a range from the elements of the vector 'r'.
 */
template<class v>
inline PointerRange<v>::PointerRange(mgd_vector_type& r)
                      : base_type(&*r.begin(),&*r.end())
{
    assert(consistent());                                // Check consistency
}

/**
 *  Construct a range from the characters of the string 'r'.
 */
template<class v>
inline PointerRange<v>::PointerRange(std_string_type& r)
                      : base_type(&*r.begin(),&*r.end())
{
    assert(consistent());                                // Check consistency
}

/**
 *  Construct a range from the 'n' elements of the array 'a'.
 */
template<class v>
template<size_t n>
inline PointerRange<v>::PointerRange(v(&a)[n])
                      : base_type(a,a+n)
{
    assert(consistent());                                // Check consistency
}

/**
 *  A range of mutable values can safely be supplied where a range of constant
 *  values is required, but not, of course, the other way around.
 */
template<class v>
inline PointerRange<v>::operator PointerRange<const v>() const
{
    return PointerRange<const v>(this->begin(),this->end());
}

/**
 *  Return the size of the range as an unsigned integral type, rather than the
 *  ptrdiff_t returned by our base class, which tends to generate a great deal
 *  of 'signed / unsigned comparison mismatch' type compiler warnings.
 */
template<class v>
inline size_t PointerRange<v>::size() const
{
    return static_cast<size_t>(base_type::size());       // Safe, since i <= e
}

/**
 *  Return true if the object looks to be in good shape. Centralizes a number
 *  of consistency checks that would otherwise clutter up the code, and since
 *  only ever called from within assertions can be eliminated entirely by the
 *  compiler in an optimized build.
 */
template<class v>
inline bool PointerRange<v>::consistent() const
{
    assert(this->begin() <= this->end());                // Validate pointers
    assert((this->begin()==0) == (this->end()==0));      // Null <=> empty()

    return true;                                         // Appears to be good
}

/**
 *  Return true if the ranges 'a' and 'b' have the same elements.
 *
 *  Individual range elements are compared with the operator== appropriate for
 *  the element types 'u' and 'v'.
 *
 *  @see http://www.cplusplus.com/reference/algorithm/equal.
 */
template<class u,class v>
inline bool operator==(PointerRange<u> a,PointerRange<v> b)
{
    return a.size()==b.size() && std::equal(a.begin(),a.end(),b.begin());
}

/**
 *  Return true if range 'a' compares lexicographically less than range 'b'.
 *
 *  Individual range elements are compared with the operator< appropriate for
 *  the element types 'u' and 'v'.
 *
 *  @see http://www.cplusplus.com/reference/algorithm/lexicographical_compare
 */
template<class u,class v>
inline bool operator<(PointerRange<u> a,PointerRange<v> b)
{
    return std::lexicographical_compare(a.begin(),a.end(),b.begin(),b.end());
}

/**
 *  Construct a range from the sequence of values [i,e).
 */
template<class v>
inline PointerRange<v> pointerRange(v* i,v* e)
{
    return PointerRange<v>(i,e);                         // Create from pointers
}

/**
 *  Construct a range from the sequence of 'n' values beginning at 'i'.
 */
template<class v>
inline PointerRange<v> pointerRange(size_t n,v* i)
{
    return PointerRange<v>(n,i);                         // Create from array
}

/**
 *  Construct a range from the elements of the vector 'r'.
 */
template<class v>
inline PointerRange<v> pointerRange(std::vector<v>& r)
{
    return PointerRange<v>(r);                           // Create from vector
}

/**
 *  Construct a range from the elements of the vector 'r'.
 */
template<class v>
inline PointerRange<const v> pointerRange(const std::vector<v>& r)
{
    return PointerRange<const v>(r);                     // Create from vector
}

/**
 *  Construct a range from the elements of the vector 'r'.
 */
template<class v>
inline PointerRange<v> pointerRange(mgd::vector<v>& r)
{
    return PointerRange<v>(r);                           // Create from vector
}

/**
 *  Construct a range from the elements of the vector 'r'.
 */
template<class v>
inline PointerRange<const v> pointerRange(const mgd::vector<v>& r)
{
    return PointerRange<const v>(r);                     // Create from vector
}

/**
 *  Construct a range from the characters of the string 'r'.
 */
template<class v>
inline PointerRange<v> pointerRange(std::basic_string<v>& r)
{
    return PointerRange<v>(r);                           // Create from string
}

/**
 *  Construct a range from the characters of the string 'r'.
 */
template<class v>
inline PointerRange<const v> pointerRange(const std::basic_string<v>& r)
{
    return PointerRange<const v>(r);                     // Create from string
}

/**
 *  Construct a range from the 'n' elements of the array 'a'.
 */
template<class v,size_t n>
inline PointerRange<v> pointerRange(v (&a)[n])
{
    return PointerRange<v>(a,a+n);                       // Create from array
}

/**
 *  Construct a pointer range from the null-terminated sequence of values that
 *  begins at address 'i'.
 */
template<class v>
inline PointerRange<v> nullTerminated(v* i)
{
/*  Compute the length of the null terminated sequence using whichever method
    is most efficient for the type 'v': calls strlen() when 'v' is the 'char',
    for example, which is usually implemented to count characters a word at a
    time...*/

    size_t n = std::char_traits<v>::length(i);           // Defer to traits

    return PointerRange<v>(i,i+n);                       // Return [i,i+n)
}

/**
 *  Take the initial 'i' elements of the range 'r'.
 */
template<class v>
inline PointerRange<v> take(PointerRange<v> r,size_t i = 0)
{
    assert(i <= r.size());                               // Validate arguments

    return PointerRange<v>(r.begin(),r.begin()+i);       // Return [b,b+i)
}

/**
 *  Drop the initial 'i' and trailing 'j' elements of the range 'r'.
 */
template<class v>
inline PointerRange<v> drop(PointerRange<v> r,size_t i = 0,size_t j = 0)
{
    assert(i+j <= r.size());                             // Validate arguments

    return PointerRange<v>(r.begin()+i,r.end()-j);       // Return [b+i,e-j)
}

/**
 *  Take the 'n' elements beginning at element 'i' of the range 'r'.
 */
template<class v>
inline PointerRange<v> subrange(PointerRange<v> r,size_t i = 0,size_t n = 0)
{
    assert(i+n <= r.size());                             // Validate arguments

    return PointerRange<v>(r.begin()+i,r.begin()+i+n);   // Return [b+i,b+i+n)
}

/**
 *  Shift (or translate) the range 'r' by 'i' elements forward (i > 0) or back
 *  (i < 0).
 */
template<class v>
inline PointerRange<v> shift(PointerRange<v> r,ptrdiff_t i = 0)
{
    return PointerRange<v>(r.begin()+i,r.end()+i);       // Return [b+i,e+i)
}

/**
 *  Grow (or expand) the range 'r' by 'i' elements at the front and 'j' at the
 *  back.
 */
template<class v>
inline PointerRange<v> grow(PointerRange<v> r,ptrdiff_t i = 0,ptrdiff_t j = 0)
{
    return PointerRange<v>(r.begin()-i,r.end()+j);       // Return [b-i,e+j)
}

/**
 *  Insert the sequence of values -  not necessarily a PointerRange - that lie
 *  within the half open interval [i,e) onto the output stream 'io'.
 */
template<class forward>
std::ostream& insertRange(std::ostream& io,forward i,forward e)
{
    for ( ; i!=e; ++i)                                   // For each element
    {
        io << *i;                                        // ...drop on stream
    }

    return io;                                           // Return the stream
}

/**
 *  Insert the sequence of values -  not necessarily a PointerRange - that lie
 *  within the half open interval [i,e) onto the output stream 'io',separating
 *  each with the the delimiter 'd', which can be of any streamable type.
 */
template<class iterator,class delimiter>
std::ostream& insertRange(std::ostream& io,iterator i,iterator e,delimiter d)
{
    if (i != e)                                          // Non-empty range?
    {
        io << *i;                                        // ...format first

        for ( ++i; i != e; ++i)                          // ...for each next
        {
            io << d << *i;                               // ....delimit it
        }
    }

    return io;                                           // Return the stream
}

/**
 *  Insert the sequence of values -  not necessarily a PointerRange - onto the
 *  output stream 'io'.
 */
template<class range>
inline std::ostream& insertRange(std::ostream& io,const range& r)
{
    return insertRange(io,r.begin(),r.end());            // Insert the range
}

/**
 *  Insert the sequence of values -  not necessarily a PointerRange - onto the
 *  output stream 'io',  separating each with the the delimiter 'd', which can
 *  be of a streamable type.
 */
template<class range,class delimiter>
inline std::ostream& insertRange(std::ostream& io,const range& r,delimiter d)
{
    return insertRange(io,r.begin(),r.end(),d);          // Insert the range
}

/****************************************************************************/
}
/****************************************************************************/
#endif
/* @file ********************************************************************/
