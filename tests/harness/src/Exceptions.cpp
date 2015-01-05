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
 * @file Exceptions.cpp
 * @author girish_hilage@persistent.co.in
 */

# include <string>
# include "Exceptions.h"

using namespace std;

namespace scidbtestharness
{
namespace Exceptions
{

string ERROR :: what (void) throw()
{
	string tag = "ERROR: ";
	return string (tag+_msg);
}

string ConfigError :: what (void) throw()
{
	string tag = "ConfigError: ";
	if (_code == NO_CODE)
		return string(tag+_msg);

	return string (tag + _msg + config_errdb.core[_code].msg);
}

string SystemError :: what (void) throw()
{
	string tag = "SystemError: ";
	if (_code == NO_CODE)
		return string(tag+_msg);

	return string (tag + _msg + system_errdb.core[_code].msg);
}

string ExecutorError :: what (void) throw()
{
	string tag = "ExecutorError: ";
	if (_code == NO_CODE)
		return string(tag+_msg);

	return string (tag + _msg + executor_errdb.core[_code].msg);
}


} //END namespace Exceptions
} //END namespace scidbtestharness
