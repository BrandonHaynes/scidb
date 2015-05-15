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

#include <util/arena/ArenaMonitor.h>                     // For Monitor
#include <util/Singleton.h>                              // For Singleton

/****************************************************************************/
namespace scidb { namespace arena {
/****************************************************************************/

struct TheMonitor : Monitor
{
    virtual void update(const Arena&,const char*);
};

void TheMonitor::update(const Arena& arena,name_t label)
{
    std::cout << std::endl;
    std::cout << "update(\"" << label << "\"," << arena << ")\n";
}

/****************************************************************************/

/**
 *  Save a snapshot of the current allocation statistics for the given %arena,
 *  and associate this snapshot with the given label,  which can be any string
 *  you like.
 */
void Monitor::update(const Arena& arena,name_t label)
{}

/**
 *  Return a reference to the one and only %arena monitor.
 */
Monitor& Monitor::getInstance()
{
    return *Singleton<TheMonitor>::getInstance();        // Return singleton
}

/****************************************************************************/
}}
/****************************************************************************/
