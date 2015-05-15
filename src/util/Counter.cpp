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

/**
 * @file Counter.cpp
 * @brief Implementation of counter utility
 * @author sfridella@paradigm4.com
 */

#include <util/Counter.h>
#include <query/ops/list/ListArrayBuilder.h>

namespace scidb
{

using namespace boost;
using namespace std;

/* List information about all counters using the builder
 */
void
CounterState::listCounters(ListCounterArrayBuilder& builder)
{
    ScopedMutexLock sm(_stateMutex);

    for (int i = 0; i < LastCounter; i++)
    {
        _entries[i]._id = static_cast<CounterId>(i);
        builder.listElement(_entries[i]);
    }
}

}
