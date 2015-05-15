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

#ifndef UTIL_ARENA_SET_H_
#define UTIL_ARENA_SET_H_

/****************************************************************************/

#include <util/arena/Managed.h>                          // For macros
#include <boost/container/set.hpp>                       // For set

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
 *  @see        http://www.cplusplus.com/reference/set/set
 *
 *  @see        Class multiset.
 *
 *  @author     jbell@paradigm4.com.
 */
template<class V,class P = std::less<V> >
class set : public SCIDB_MANAGED_BASE(boost::container,set,V,P)
{
 private:            // Implementation
                        SCIDB_MANAGED_ORDERED(boost::container,set,V,P)

 public:             // Construction
    explicit            set(                                  const P& p = P()) : base_type(p)      {}
    template<class it>  set(                        it i,it e,const P& p = P()) : base_type(i,e,p)  {}

 public:             // Construction
    explicit            set(const allocator_type& a,          const P& p = P()) : base_type(p,a)    {}
    template<class it>  set(const allocator_type& a,it i,it e,const P& p = P()) : base_type(i,e,p,a){}
};

/**
 *  @brief      Specializes std::swap() to take advantage of the swap() member
 *              function provided by the container itself.
 */
template<class V,class P>
inline void swap(set<V,P>& a,set<V,P>& b) {a.swap(b);}

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
 *  @see        http://www.cplusplus.com/reference/set/multiset
 *
 *  @see        Class set.
 *
 *  @author     jbell@paradigm4.com.
 */
template<class V,class P = std::less<V> >
class multiset : public SCIDB_MANAGED_BASE(boost::container,multiset,V,P)
{
 private:            // Implementation
                        SCIDB_MANAGED_ORDERED(boost::container,multiset,V,P)

 public:             // Construction
    explicit            multiset(                                  const P& p = P()) : base_type(p)      {}
    template<class it>  multiset(                        it i,it e,const P& p = P()) : base_type(i,e,p)  {}

 public:             // Construction
    explicit            multiset(const allocator_type& a,          const P& p = P()) : base_type(p,a)    {}
    template<class it>  multiset(const allocator_type& a,it i,it e,const P& p = P()) : base_type(i,e,p,a){}
};

/**
 *  @brief      Specializes std::swap() to take advantage of the swap() member
 *              function provided by the container itself.
 */
template<class V,class P>
inline void swap(multiset<V,P>& a,multiset<V,P>& b) {a.swap(b);}

/****************************************************************************/
}}}
/****************************************************************************/
#endif
/****************************************************************************/
