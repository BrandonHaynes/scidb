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

#ifndef QUERY_TRANSIENT_CACHE_H_
#define QUERY_TRANSIENT_CACHE_H_

/****************************************************************************/

#include <array/Metadata.h>                              // For ArrayDesc

/****************************************************************************/
namespace scidb {
/****************************************************************************/

typedef boost::shared_ptr<Query>    QueryPtr;            // A shared query ptr
typedef boost::shared_ptr<Array>    MemArrayPtr;         // A shared array ptr

/****************************************************************************/
namespace transient {
/****************************************************************************/

/**
 *  Return the currently cached transient array that is described by the given
 *  schema,  or throw an error if the cache for this instance contains no such
 *  array.
 *
 *  @throws scidb::SystemException if the cache for this instance contains no
 *  such array.
 */
MemArrayPtr lookup(const ArrayDesc&,const QueryPtr&);

/**
 *  Record the given array in this instance's portion of the transient array
 *  cache.
 */
MemArrayPtr record(const MemArrayPtr&);

/**
 *  Remove the given array from this instance's portion of the transient array
 *  cache.
 */
void remove(const ArrayDesc&);

/**
 *  Remove all arrays from this instance's portion of the transient array cache.
 */
void clear();

/****************************************************************************/
}}
/****************************************************************************/
#endif
/****************************************************************************/
