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
 * @file Sysinfo.cpp
 *
 * @brief Information about host system and hardware
 *
 * @author knizhnik@garret.ru
 */

#include <unistd.h>
#include <sys/types.h>
#include <sys/sysctl.h>

#include "system/Sysinfo.h"
#include "system/SciDBConfigOptions.h"
#include "system/Constants.h"
#ifndef SCIDB_CLIENT
#include "system/Config.h"
#endif

namespace scidb
{

// James says these defaults should be reasonable for all x86_64 CPUs.
enum {
    DEFAULT_L1_CACHE_BYTES = 32*KiB,    // This hasn't changed for Intel for years and years.
    DEFAULT_L2_CACHE_BYTES = 64*KiB, 
    DEFAULT_L3_CACHE_BYTES = 2*MiB
};

int Sysinfo::getNumberOfCPUs()
{
    int nCores = sysconf(_SC_NPROCESSORS_ONLN);
    int usedCpuLimit =
#ifndef SCIDB_CLIENT
        Config::getInstance()->getOption<int>(CONFIG_OPERATOR_THREADS);
#else
        0;
#endif
    return usedCpuLimit != 0 && nCores > usedCpuLimit ? usedCpuLimit : nCores;
}

int Sysinfo::getCPUCacheSize(int level)
{
    // Cache values from OS so we needn't always make system calls.
    static int l1_size = -1;
    static int l2_size = -1;
    static int l3_size = -1;

    // Add up the requested cache sizes.
    //
    // Frankly, I do not understand why the sum of two cache sizes is
    // a useful number, but I won't mess with it now.
    //
    int cache_size = 0;
    if (level & CPU_CACHE_L1) {
        if (l1_size < 0) {
#ifdef _SC_LEVEL1_DCACHE_SIZE
            l1_size = sysconf(_SC_LEVEL1_DCACHE_SIZE);
#else
            l1_size = DEFAULT_L1_CACHE_BYTES;
#endif
        }
        cache_size += l1_size;
    }
    if (level & CPU_CACHE_L2) {
        if (l2_size < 0) {
#ifdef _SC_LEVEL2_CACHE_SIZE
            l2_size = sysconf(_SC_LEVEL2_CACHE_SIZE);
#else
            l2_size = DEFAULT_L2_CACHE_BYTES;
#endif
        }
        cache_size += l2_size;
    }
    if (level & CPU_CACHE_L3) {
        if (l3_size < 0) {
#ifdef _SC_LEVEL3_CACHE_SIZE
            l3_size = sysconf(_SC_LEVEL3_CACHE_SIZE);
#else
            l3_size = DEFAULT_L3_CACHE_BYTES;
#endif
        }
        cache_size += l3_size;
    }
    return cache_size;
}


} //namespace
