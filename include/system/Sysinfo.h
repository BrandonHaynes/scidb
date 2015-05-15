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

/**
 * Obtain various information about the host system and its hardware.
 *
 * @note
 * Code that formerly used the constant Sysinfo::INTEL_L1_DATA_CACHE_BYTES
 * should instead call Sysinfo::getCPUCacheSize(Sysinfo::CPU_CACHE_L1).
 */
class Sysinfo
{
public:
    enum CacheLevel
    {
        CPU_CACHE_L1 = 1,
        CPU_CACHE_L2 = 2,
        CPU_CACHE_L3 = 4
    };

    static int getNumberOfCPUs();

    /**
     * Get hardware cache sizes from the operating system.
     *
     * @param level a mask of @c Sysinfo::CacheLevel flags
     * @return sum of bytes in all caches in the level mask
     *
     * @note Returned cache size values are themselves cached, so that
     * only the initial calls to this method will actually perform
     * system calls.
     */
    static int getCPUCacheSize(int level);
};    

} //namespace

#endif
