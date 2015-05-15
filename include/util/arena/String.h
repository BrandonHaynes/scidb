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

#ifndef UTIL_ARENA_STRING_H_
#define UTIL_ARENA_STRING_H_

/****************************************************************************/

#include <util/arena/Managed.h>                          // For macros
#include <util/PointerRange.h>                           // For PointerRange
#include <boost/container/string.hpp>                    // For basic_string

/****************************************************************************/
namespace scidb { namespace arena { namespace managed {
/****************************************************************************/
/**
 *  @brief      Specializes the standard container to allocate its memory from
 *              an %arena.
 *
 *  @details    This class specializes the standard library container class of
 *              the same name to take an arena::Allocator at construction, and
 *              subsequently  allocates all of  its internal storage from this
 *              allocator. It reorders the constructor parameters to take this
 *              required object first, but in all other respects is completely
 *              identical to the standard container from which it derives.
 *
 *  @see        http://www.cplusplus.com/reference/string/basic_string
 *
 *  @see        Class string.
 *
 *  @author     jbell@paradigm4.com.
 */
template<class C,class T  = std::char_traits<C> >
class basic_string : public SCIDB_MANAGED_BASE(boost::container,basic_string,C,T)
{
 private:            // Implementation
                        SCIDB_MANAGED_ORDERED(boost::container,basic_string,C,T)

 public:             // Constants
    using               base_type::npos;

 public:             // Construction
                        basic_string(                                                                            ) : base_type()                   {}
    template<class it>  basic_string(                        it i,it e                                           ) : base_type(i,e)                {}
                        basic_string(                        const C* s                                          ) : base_type(s)                  {}
                        basic_string(                        const C* s,size_type n                              ) : base_type(s,n)                {}
                        basic_string(                        size_type n,C c                                     ) : base_type(n,c)                {}
                        basic_string(                        const basic_string& s,size_type p,size_type n = npos) : base_type(s,p,n)              {}
                        basic_string(                        PointerRange<const C> r                             ) : base_type(r.begin(),r.end())  {}

 public:             // Construction
    explicit            basic_string(const allocator_type& a                                                     ) : base_type(a)                  {}
    template<class it>  basic_string(const allocator_type& a,it i,it e                                           ) : base_type(i,e,a)              {}
                        basic_string(const allocator_type& a,const C* s                                          ) : base_type(s,a)                {}
                        basic_string(const allocator_type& a,const C* s,size_type n                              ) : base_type(s,n,a)              {}
                        basic_string(const allocator_type& a,size_type n,C c                                     ) : base_type(n,c,a)              {}
                        basic_string(const allocator_type& a,const basic_string& s,size_type p,size_type n = npos) : base_type(s,p,n,a)            {}
                        basic_string(const allocator_type& a,PointerRange<const C> r                             ) : base_type(r.begin(),r.end(),a){}
};

/**
 *  @brief      Specializes std::swap() to take advantage of the swap() member
 *              function provided by the container itself.
 */
template<class C,class T>
inline void swap(basic_string<C,T>& a,basic_string<C,T>& b) {a.swap(b);}

/**
 *  @brief      Represents a string of characters that are allocated within an
 *              %arena.
 *
 *  @see        http://www.cplusplus.com/reference/string/string
 */
typedef basic_string<char> string;                       // The managed string

/****************************************************************************/
}}}
/****************************************************************************/
#endif
/****************************************************************************/
