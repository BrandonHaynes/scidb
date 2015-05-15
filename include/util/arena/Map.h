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

#ifndef UTIL_ARENA_MAP_H_
#define UTIL_ARENA_MAP_H_

/****************************************************************************/

#include <util/arena/Managed.h>                          // For macros
#include <boost/container/map.hpp>                       // For map

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
 *  @see        http://www.cplusplus.com/reference/map/map
 *
 *  @see        Class multimap.
 *
 *  @author     jbell@paradigm4.com.
 */
template<class K,class V,class P = std::less<K> >
class map : public SCIDB_MANAGED_BASE(boost::container,map,K,V,P)
{
 private:            // Implementation
                        SCIDB_MANAGED_ORDERED(boost::container,map,K,V,P)

 public:             // Construction
    explicit            map(                                  const P& p = P()) : base_type(p)      {}
    template<class it>  map(                        it i,it e,const P& p = P()) : base_type(i,e,p)  {}

 public:             // Construction
    explicit            map(const allocator_type& a,          const P& p = P()) : base_type(p,a)    {}
    template<class it>  map(const allocator_type& a,it i,it e,const P& p = P()) : base_type(i,e,p,a){}
};

/**
 *  @brief      Specializes std::swap() to take advantage of the swap() member
 *              function provided by the container itself.
 */
template<class K,class V,class P>
inline void swap(map<K,V,P>& a,map<K,V,P>& b) {a.swap(b);}

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
 *  @see        http://www.cplusplus.com/reference/map/multimap
 *
 *  @see        Class map.
 *
 *  @author     jbell@paradigm4.com.
 */
template<class K,class V,class P = std::less<K> >
class multimap : public SCIDB_MANAGED_BASE(boost::container,multimap,K,V,P)
{
 private:            // Implementation
                        SCIDB_MANAGED_ORDERED(boost::container,multimap,K,V,P)

 public:             // Construction
    explicit            multimap(                                  const P& p = P()) : base_type(p)      {}
    template<class it>  multimap(                        it i,it e,const P& p = P()) : base_type(i,e,p)  {}

 public:             // Construction
    explicit            multimap(const allocator_type& a,          const P& p = P()) : base_type(p,a)    {}
    template<class it>  multimap(const allocator_type& a,it i,it e,const P& p = P()) : base_type(i,e,p,a){}
};

/**
 *  @brief      Specializes std::swap() to take advantage of the swap() member
 *              function provided by the container itself.
 */
template<class K,class V,class P>
inline void swap(multimap<K,V,P>& a,multimap<K,V,P>& b) {a.swap(b);}

/****************************************************************************/
}}}
/****************************************************************************/
#endif
/****************************************************************************/
