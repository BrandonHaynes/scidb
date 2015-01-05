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
 * @file Sysinfo.h
 *
 * @brief Information about host system and hardware
 *
 * @author knizhnik@garret.ru
 */

#ifndef SYSINFO_H
#define SYSINFO_H

#include "system/Constants.h"

namespace scidb
{

class Sysinfo
{
public:
    enum CacheLevel
    {
        CPU_CACHE_L1 = 1,
        CPU_CACHE_L2 = 2,
        CPU_CACHE_L3 = 4
    };

    enum {
        INTEL_L1_DATA_CACHE_BYTES = 32*KiB      // This hasn't changed for Intel for years and years.
                                                // (We don't have access from Linux to the L1 size,
                                                // only the last-level-cache size.)
    };

    static int getNumberOfCPUs();
    static int getCPUCacheSize(int level);
};    

} //namespace

#endif
