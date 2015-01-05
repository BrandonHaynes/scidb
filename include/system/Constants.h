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

#ifndef CONSTANTS_H_
#define CONSTANTS_H_
/**
 *  @file    Constants.h
 *
 *  @author  Artyom Smirnov <smirnoffjr@gmail.com>
 *
 *  @brief   Introduces a number of system-wide build-time constants.
 */
#include <stdint.h>
#include <string>

/****************************************************************************/
namespace scidb {
/****************************************************************************/

const char* SCIDB_VERSION();                        ///< The full version string      : maj.min.pat.bld
const char* SCIDB_VERSION_PUBLIC();                 ///< The public version string    : maj.min.bld
const char* SCIDB_BUILD_TYPE();                     ///< The build type               : release|debug|...
const char* SCIDB_COPYRIGHT();                      ///< The official copyright string: Copyright (C) 2008-...
const char* SCIDB_INSTALL_PREFIX();                 ///< The target installation path prefix

uint32_t    SCIDB_VERSION_MAJOR();                  ///< The major revision number
uint32_t    SCIDB_VERSION_MINOR();                  ///< The minor revision number
uint32_t    SCIDB_VERSION_PATCH();                  ///< The patch revision number
uint32_t    SCIDB_VERSION_BUILD();                  ///< The build revision number

std::string SCIDB_BUILD_INFO_STRING(const char* separator = "\n");
std::string DEFAULT_MPI_DIR();
std::string DEFAULT_MPI_TYPE();

/**
 * @{
 * @name Power-of-1024 IEC multiplier constants.
 * @see http://en.wikipedia.org/wiki/KiB
 */
const size_t KiB = 1024;                            ///< kibibyte
const size_t MiB = 1024*KiB;                        ///< mebibyte
const size_t GiB = 1024*MiB;                        ///< gibibyte
const size_t TiB = 1024*GiB;                        ///< tebibyte
const size_t PiB = 1024*TiB;                        ///< pebibyte
const size_t EiB = 1024*PiB;                        ///< exbibyte
/**@}*/

const size_t MAX_NUM_DIMS_SUPPORTED         = 100;  ///< The maximum number of array dimensions supported.
const size_t DEFAULT_MEM_THRESHOLD          = 1*KiB;
const double DEFAULT_DENSE_CHUNK_THRESHOLD  = 1.0;
const double DEFAULT_SPARSE_CHUNK_INIT_SIZE = 0.01;
const int    DEFAULT_STRING_SIZE_ESTIMATION = 10;

/**
 * This version corresponds to the number in the postgres table "cluster"
 * as defined in meta.sql. If we start and find that
 * cluster.metadata_version is less than this number, we will perform an
 * upgrade. The upgrade files are provided as sql scripts in
 * src/system/catalog/data/[NUMBER].sql. They are converted to string
 * constants in a C++ header file  (variable METADATA_UPGRADES_LIST[])
 * and then linked in at build time. Note: there is no downgrade path at
 * the moment.
 * @see SystemCatalog::connect(const string&, bool)
 */
const int    METADATA_VERSION               = 2;

/****************************************************************************/
}
/****************************************************************************/

#endif /* CONSTANTS_H_ */
