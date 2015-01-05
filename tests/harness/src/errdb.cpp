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

/*
 * @file errdb.cpp
 * @author girish_hilage@persistent.co.in
 */

# include "errdb.h"

namespace scidbtestharness
{
namespace Exceptions
{

struct ConfigErrorDB config_errdb = 
{
	ERR_CONFIG_UNKNOWN,
	{
		{ERR_CONFIG_SCIDBCONNECTIONSTRING_EMPTY, "SciDB connection string must be specified"},
		{ERR_CONFIG_SCIDBPORT_INVALID, "SciDB Port must be > 0"},
		{ERR_CONFIG_TESTCASEFILENAME_EMPTY, "Test case file either does not exists or is not specified"},
		{ERR_CONFIG_SCIDBROOTDIR_EMPTY, "SciDB Root directory must be specified"},
		{ERR_CONFIG_AMBIGUOUS_SUITEID, "Suite directory name and suite file name can not be the same"},
		{ERR_CONFIG_INVALID_LOGDESTINATION, "Invalid log destination specified"},
		{ERR_CONFIG_REGEX_MUTUALLY_EXCLUSIVE, "Only one of --include-regex-id, --exclude-regex-id, --include-regex-name, --exclude-regex-name can be specified"},
		{ERR_CONFIG_REGEX_EXPR_SPECIFIED_BUT_FLAG_NOT_SET, "Regex expression is specified but regex flag is not internally set to appropriate value"},
		{ERR_CONFIG_INVALID_SLEEPVALUE, "Invalid sleep value"},
		{ERR_CONFIG_INVALID_EXECUTOR_TYPE, "Invalid executor type supplied"},
	},
	ERR_CONFIG_MAX
};

struct SystemErrorDB system_errdb = 
{
	ERR_SYSTEM_UNKNOWN,
	{
		{ERR_SYSTEM_FILE_CREATION, "Failed to create a file"},
		{ERR_SYSTEM_INVALID_DEBUGLEVEL, "Invalid debug level"},
		{ERR_SYSTEM_EMPTY_JOBLIST, "empty job list"},
	},
	ERR_SYSTEM_MAX
};

struct ExecutorErrorDB executor_errdb = 
{
	ERR_EXECUTOR_UNKNOWN,
	{
		{ERR_EXECUTOR_SHELLCOMMAND_NOT_SPECIFIED, "Please specify the shell command to be executed."},
	},
	ERR_EXECUTOR_MAX
};

} //END namespace Exceptions
} //END namespace scidbtestharness
