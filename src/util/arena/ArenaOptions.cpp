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

/****************************************************************************/

#include <system/Constants.h>                        // For KiB, MiB, and GiB
#include "ArenaDetails.h"                            // For implementation

/****************************************************************************/
namespace scidb { namespace arena {
/****************************************************************************/

/**
 *  Construct a set of %arena options with sensible default values.
 */
    Options::Options(name_t name)
           : _name      (name),
             _limit     (unlimited),
             _psize     (4*KiB),
             _parent    (getArena()),
             _finalizing(true ),
             _recycling (false),
             _resetting (false),
             _debugging (false),
             _threading (false)
{
    assert(consistent());                                // Check consistency
}

/**
 *  Insert a formatted representation of the %arena options 'o' on the output
 *  stream 's'.
 */
std::ostream& operator<<(std::ostream& s,const Options& o)
{
    s << "Options{"                                      // Emit the header
      << "name=\""     << o.name()              << "\"," // Emit the name
      << "limit="      << bytes_t(o.limit())    << ','   // Emit the limit
      << "pagesize="   << bytes_t(o.pagesize()) << ','   // Emit the page size
      << "parent=\""   << o.parent()->name()    << "\"," // Emit parent name
      << "finalizing=" << o.finalizing()        << ','   // Emit finalizing
      << "recycling="  << o.recycling()         << ','   // Emit recycling
      << "resetting="  << o.resetting()         << ','   // Emit resetting
      << "debugging="  << o.debugging()         << ','   // Emit debugging
      << "threading="  << o.threading()         << '}';  // Emit threading

    return s;
}

/**
 *  Return true if the object looks to be in good shape.  Centralizes a number
 *  of consistency checks that would otherwise clutter up the code, and, since
 *  only ever called from within assertions, can be eliminated entirely by the
 *  compiler from the release build.
 */
bool Options::consistent() const
{
    assert(_name   != 0);                                // Validate name
    assert(_parent != 0);                                // Validate parent
    assert(_limit  <= unlimited);                        // Validate limit
    assert(_psize  <= unlimited);                        // Validate page size

    return true;                                         // Appears to be good
}

/****************************************************************************/
}}
/****************************************************************************/
