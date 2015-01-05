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
 * @file errdb.h
 *
 * @author girish_hilage@persistent.co.in
 *
 * @brief file containing error database definitions for
 * configuration and system errors
 */

# ifndef ERRDB_H
# define ERRDB_H

# include <string>

namespace scidbtestharness
{
namespace Exceptions
{

/* core Error Database definition */
struct coreErrorDB
{
	int code;
	std::string msg;
};

/**
 ***** Configuration Errors *****
 */
enum config_errcodes
{
	ERR_CONFIG_UNKNOWN = -1,
	ERR_CONFIG_SCIDBCONNECTIONSTRING_EMPTY,
	ERR_CONFIG_SCIDBPORT_INVALID,
	ERR_CONFIG_TESTCASEFILENAME_EMPTY,
	ERR_CONFIG_SCIDBROOTDIR_EMPTY,
	ERR_CONFIG_AMBIGUOUS_SUITEID,
	ERR_CONFIG_INVALID_LOGDESTINATION,
	ERR_CONFIG_REGEX_MUTUALLY_EXCLUSIVE,
	ERR_CONFIG_REGEX_EXPR_SPECIFIED_BUT_FLAG_NOT_SET,
	ERR_CONFIG_INVALID_SLEEPVALUE,
	ERR_CONFIG_INVALID_EXECUTOR_TYPE,
	ERR_CONFIG_MAX
};

/* Configuration errors database with min,max error code range */
struct ConfigErrorDB
{
	int errorcodeMin;
	struct coreErrorDB core[ERR_CONFIG_MAX];
	int errorcodeMax;
};

extern struct ConfigErrorDB config_errdb;

/* __________________________________________________________________________________ */
/**
 ******** System Errors *********
 */
enum system_errcodes
{
	ERR_SYSTEM_UNKNOWN = -1,
	ERR_SYSTEM_FILE_CREATION,
	ERR_SYSTEM_INVALID_DEBUGLEVEL,
	ERR_SYSTEM_EMPTY_JOBLIST,
	ERR_SYSTEM_MAX
};

/* System errors database with min,max error code range */
struct SystemErrorDB
{
	int errorcodeMin;
	struct coreErrorDB core[ERR_SYSTEM_MAX];
	int errorcodeMax;
};

extern struct SystemErrorDB system_errdb;

/* __________________________________________________________________________________ */
/**
 ******** Executor Errors *********
 */

enum executor_errcodes
{
	ERR_EXECUTOR_UNKNOWN = -1,
	ERR_EXECUTOR_SHELLCOMMAND_NOT_SPECIFIED,
	ERR_EXECUTOR_MAX
};

struct ExecutorErrorDB
{
	int errorcodeMin;
	struct coreErrorDB core[ERR_EXECUTOR_MAX];
	int errorcodeMax;
};

extern struct ExecutorErrorDB executor_errdb;
} //END namespace Exceptions
} //END namespace scidbtestharness
# endif
