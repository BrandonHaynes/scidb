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

#ifndef UTIL_ARENA_VECTOR_H_
#define UTIL_ARENA_VECTOR_H_

/****************************************************************************/

#include <util/arena/Managed.h>                          // For macros
#include <boost/container/vector.hpp>                    // For vector

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
 *  @see        http://www.cplusplus.com/reference/vector/vector
 *
 *  @author     jbell@paradigm4.com.
 */
template<class V>
class vector : public SCIDB_MANAGED_BASE(boost::container,vector,V)
{
 private:            // Implementation
                        SCIDB_MANAGED_ORDERED(boost::container,vector,V)

 public:             // Construction
    explicit            vector(                                                    ) : base_type()     {}
    explicit            vector(                        size_type n,const V& v = V()) : base_type(n,v)  {}
    template<class it>  vector(                        it i,it e                   ) : base_type(i,e)  {}

 public:             // Construction
    explicit            vector(const allocator_type& a                             ) : base_type(a)    {}
    explicit            vector(const allocator_type& a,size_type n,const V& v = V()) : base_type(n,v,a){}
    template<class it>  vector(const allocator_type& a,it i,it e                   ) : base_type(i,e,a){}

 public:             // Operations
    V const&            operator[](size_type i)    const {return isDebug() ? base_type::at(i) : base_type::operator[](i);}
    V&                  operator[](size_type i)          {return isDebug() ? base_type::at(i) : base_type::operator[](i);}
};

/**
 *  @brief      Specializes std::swap() to take advantage of the swap() member
 *              function provided by the container itself.
 */
template<class V>
inline void swap(vector<V>& a,vector<V>& b) {a.swap(b);}

/****************************************************************************/
}}}
/****************************************************************************/
#endif
/****************************************************************************/
