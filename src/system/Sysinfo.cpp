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

enum { DEFAULT_CACHE_SIZE = 64*KiB };

int Sysinfo::getNumberOfCPUs()
{
    int nCores = sysconf(_SC_NPROCESSORS_ONLN);
    int usedCpuLimit =
#ifndef SCIDB_CLIENT
        Config::getInstance()->getOption<int>(CONFIG_USED_CPU_LIMIT);
#else
        0;
#endif
    return usedCpuLimit != 0 && nCores > usedCpuLimit ? usedCpuLimit : nCores;
}

int Sysinfo::getCPUCacheSize(int level)
{
    int cache_size = 0;
    if (level & CPU_CACHE_L1) { 
#ifdef _SC_LEVEL1_DCACHE_SIZE
        cache_size += sysconf(_SC_LEVEL1_DCACHE_SIZE); 
#elif defined(HW_L1DCACHESIZE)
        int mib[2] = {CTL_HW, HW_L1DCACHESIZE};
        int val = 0;
        size_t len = sizeof(val);
        sysctl(mib, 2, &val, &len, NULL, 0);
        cache_size += val;
#endif       
    }    
    if (level & CPU_CACHE_L2) { 
#ifdef _SC_LEVEL2_CACHE_SIZE
        cache_size += sysconf(_SC_LEVEL2_CACHE_SIZE); 
#elif defined(HW_L2CACHESIZE)
        int mib[2] = {CTL_HW, HW_L2CACHESIZE};
        int val = 0;
        size_t len = sizeof(val);
        sysctl(mib, 2, &val, &len, NULL, 0);
        cache_size += val;
#endif       
    }    
    if (level & CPU_CACHE_L3) { 
#ifdef _SC_LEVEL3_CACHE_SIZE
        cache_size += sysconf(_SC_LEVEL3_CACHE_SIZE); 
#elif defined(HW_L3CACHESIZE)
        int mib[2] = {CTL_HW, HW_L3CACHESIZE};
        int val = 0;
        size_t len = sizeof(val);
        sysctl(mib, 2, &val, &len, NULL, 0);
        cache_size += val;
#endif       
    }    
    return cache_size ? cache_size : DEFAULT_CACHE_SIZE;
}
    

} //namespace
