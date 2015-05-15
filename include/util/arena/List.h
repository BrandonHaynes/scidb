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

#ifndef UTIL_ARENA_LIST_H_
#define UTIL_ARENA_LIST_H_

/****************************************************************************/

#include <util/arena/Managed.h>                          // For macros
#include <boost/container/list.hpp>                      // For list

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
 *  @see        http://www.cplusplus.com/reference/list/list
 *
 *  @author     jbell@paradigm4.com.
 */
template<class V>
class list   : public SCIDB_MANAGED_BASE(boost::container,list,V)
{
 private:            // Implementation
                        SCIDB_MANAGED_ORDERED(boost::container,list,V)

 public:             // Construction
    explicit            list  (                                                    ) : base_type()     {}
    explicit            list  (                        size_type n,const V& v = V()) : base_type(n,v)  {}
    template<class it>  list  (                        it i,it e                   ) : base_type(i,e)  {}

 public:             // Construction
    explicit            list  (const allocator_type& a                             ) : base_type(a)    {}
    explicit            list  (const allocator_type& a,size_type n,const V& v = V()) : base_type(n,v,a){}
    template<class it>  list  (const allocator_type& a,it i,it e                   ) : base_type(i,e,a){}
};

/**
 *  @brief      Specializes std::swap() to take advantage of the swap() member
 *              function provided by the container itself.
 */
template<class V>
inline void swap(list<V>& a,list<V>& b) {a.swap(b);}

/****************************************************************************/
}}}
/****************************************************************************/
#endif
/****************************************************************************/
