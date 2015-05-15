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

#ifndef UTIL_ARENA_UNORDERED_SET_H_
#define UTIL_ARENA_UNORDERED_SET_H_

/****************************************************************************/

#include <util/arena/Managed.h>                          // For macros
#include <boost/unordered_set.hpp>                       // For unordered_set

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
 *  @see        http://www.boost.org/doc/libs/1_48_0/doc/html/boost/unordered_set.html
 *
 *  @see        Class unordered_multiset.
 *
 *  @author     jbell@paradigm4.com.
 */
template<class V,class H = boost::hash<V>,class P = std::equal_to<V> >
class unordered_set : public SCIDB_MANAGED_BASE(boost,unordered_set,V,H,P)
{
 private:            // Implementation
                        SCIDB_MANAGED_UNORDERED(boost,unordered_set,V,H,P)

 public:             // Implementation
    static const size_t _n = boost::unordered::detail::default_bucket_count;

 public:             // Construction
    explicit            unordered_set(                                  size_type n = _n,const H& h = H(),const P& p = P()) : base_type(n,h,p)      {}
    template<class it>  unordered_set(                        it i,it e,size_type n = _n,const H& h = H(),const P& p = P()) : base_type(i,e,n,h,p)  {}

 public:             // Construction
    explicit            unordered_set(const allocator_type& a,          size_type n = _n,const H& h = H(),const P& p = P()) : base_type(n,h,p,a)    {}
    template<class it>  unordered_set(const allocator_type& a,it i,it e,size_type n = _n,const H& h = H(),const P& p = P()) : base_type(i,e,n,h,p,a){}
};

/**
 *  @brief      Specializes std::swap() to take advantage of the swap() member
 *              function provided by the container itself.
 */
template<class V,class H,class P>
inline void swap(unordered_set<V,H,P>& a,unordered_set<V,H,P>& b) {a.swap(b);}

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
 *  @see        http://www.boost.org/doc/libs/1_48_0/doc/html/boost/unordered_multiset.html
 *
 *  @see        Class unordered_set.
 *
 *  @author     jbell@paradigm4.com.
 */
template<class V,class H = boost::hash<V>,class P = std::equal_to<V> >
class unordered_multiset : public SCIDB_MANAGED_BASE(boost,unordered_multiset,V,H,P)
{
 private:            // Implementation
                        SCIDB_MANAGED_UNORDERED(boost,unordered_multiset,V,H,P)

 public:             // Implementation
    static const size_t _n = boost::unordered::detail::default_bucket_count;

 public:             // Construction
    explicit            unordered_multiset(                                  size_type n = _n,const H& h = H(),const P& p = P()) : base_type(n,h,p)      {}
    template<class it>  unordered_multiset(                        it i,it e,size_type n = _n,const H& h = H(),const P& p = P()) : base_type(i,e,n,h,p)  {}

 public:             // Construction
    explicit            unordered_multiset(const allocator_type& a,          size_type n = _n,const H& h = H(),const P& p = P()) : base_type(n,h,p,a)    {}
    template<class it>  unordered_multiset(const allocator_type& a,it i,it e,size_type n = _n,const H& h = H(),const P& p = P()) : base_type(i,e,n,h,p,a){}
};

/**
 *  @brief      Specializes std::swap() to take advantage of the swap() member
 *              function provided by the container itself.
 */
template<class V,class H,class P>
inline void swap(unordered_multiset<V,H,P>& a,unordered_multiset<V,H,P>& b) {a.swap(b);}

/****************************************************************************/
}}}
/****************************************************************************/
#endif
/****************************************************************************/
