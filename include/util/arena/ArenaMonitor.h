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

#ifndef UTIL_ARENA_MONITOR_H_
#define UTIL_ARENA_MONITOR_H_

/****************************************************************************/

#include <util/Arena.h>                                  // For Arena

/****************************************************************************/
namespace scidb { namespace arena {
/****************************************************************************/
/**
 *  @brief      Collects and records statistics from participating arenas.
 *
 *  @details    Details to follow...
 *
 *  @see        http://en.wikipedia.org/wiki/Singleton_pattern.
 *
 *  @author     jbell@paradigm4.com.
 */
class Monitor : boost::noncopyable
{
 public:                   // Singleton Access
    static  Monitor&          getInstance();

 public:                   // Operations
    virtual void              update(const Arena&,name_t label) = 0;
};

/****************************************************************************/
}}
/****************************************************************************/
#endif
/****************************************************************************/
