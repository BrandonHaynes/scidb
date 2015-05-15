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

#include "ArenaDetails.h"                                // For implementation

/****************************************************************************/
namespace scidb { namespace arena {
/****************************************************************************/

/**
 *  Update the monitor with a snapshot of the given arena's statistics and tag
 *  the snapshot with the given label. Furthermore, save the arguments so that
 *  a similar snapshot can also be taken when the Checkpoint later goes out of
 *  scope and its destructor is called.
 */
    Checkpoint::Checkpoint(const Arena& arena,name_t label)
              : _arena(arena),
                _label(label)
{
    assert(_label != 0);                                 // Validate arguments

    _arena.checkpoint(_label);                           // Update the monitor
}

/**
 *  Update the monitor with a snapshot of the given arena's statistics and tag
 *  the snapshot with the given label.
 */
    Checkpoint::~Checkpoint()
{
    _arena.checkpoint(_label);                           // Update the monitor
}

/****************************************************************************/
}}
/****************************************************************************/
