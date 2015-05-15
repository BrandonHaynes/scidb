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

#ifndef UTIL_ARENA_MANAGED_H_
#define UTIL_ARENA_MANAGED_H_

/****************************************************************************/

#include <util/Arena.h>                                  // For Allocator
#include <boost/container/scoped_allocator.hpp>          // For scoped adaptor

/****************************************************************************/
namespace scidb
{
    namespace arena{namespace managed{}}                 // Defined elsewhere
    namespace mgd = arena::managed;                      // A convenient alias
}
/****************************************************************************/

/**
 *  Expands to the standard container class S::C<P1,..,Pn,A> whose allocator A
 *  allocates memory from an %arena.
 */
#define SCIDB_MANAGED_BASE(S,C,P...)                                                         \
    S::C<P,::boost::container::scoped_allocator_adaptor<                                     \
           ::scidb::arena::Allocator<                                                        \
           typename S::C<P>::allocator_type::value_type  > > >

/**
 *  Expands to the standard container boilerplate needed to implement an arena-
 *  aware version of the standard unordered container S::C<P1,..,Pn>.
 */
#define SCIDB_MANAGED_UNORDERED(S,C,P...)                                                    \
 public:                                                                                     \
    typedef SCIDB_MANAGED_BASE(S,C,P)                         base_type;                     \
    typedef                           C                       this_type;                     \
                                                                                             \
 public:                                                                                     \
    typedef typename base_type::      allocator_type          allocator_type;                \
    typedef typename base_type::      difference_type         difference_type;               \
    typedef typename base_type::      value_type              value_type;                    \
    typedef typename base_type::      size_type               size_type;                     \
    typedef typename base_type::      reference               reference;                     \
    typedef typename base_type::const_reference         const_reference;                     \
    typedef typename base_type::      pointer                 pointer;                       \
    typedef typename base_type::const_pointer           const_pointer;                       \
    typedef typename base_type::      iterator                iterator;                      \
    typedef typename base_type::const_iterator          const_iterator;                      \
                                                                                             \
 public:                                                                                     \
    using            base_type::operator=;                                                   \
                                                                                             \
 public:                                                                                     \
     C(const        base_type& c)                         : base_type(c,c.get_allocator()){} \
     C(const        base_type& c,const allocator_type& a) : base_type(c,a)                {} \
     C(BOOST_RV_REF(base_type) c)                         : base_type(c,c.get_allocator()){} \
     C(BOOST_RV_REF(base_type) c,const allocator_type& a) : base_type(c,a)                {}

/**
 *  Expands to the standard container boilerplate needed to implement an arena-
 *  aware version of the standard ordered container S::C<P1,..,Pn>.
 */
#define SCIDB_MANAGED_ORDERED(S,C,P...)                                                      \
        SCIDB_MANAGED_UNORDERED(S,C,P)                                                       \
                                                                                             \
 public:                                                                                     \
    typedef typename base_type::const_reverse_iterator  const_reverse_iterator;              \
    typedef typename base_type::      reverse_iterator        reverse_iterator;

/****************************************************************************/
#endif
/** @file *******************************************************************/
