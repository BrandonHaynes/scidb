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
 * @file Exceptions.h
 * @author girish_hilage@persistent.co.in
 * @brief file containing concrete classes for accessing corresponding error databases
 */

# ifndef EXCEPTIONS_H
# define EXCEPTIONS_H

# include <string>
# include <exception>
# include <stdexcept>

# include "helper.h"
# include "errdb.h"

# define NO_CODE -1
# define FILE_LINE_FUNCTION __FILE__,__LINE__,__FUNCTION__
# define PRINT_ERROR(msg) \
{\
	if (_loggerEnabled)\
	{\
		LOG4CXX_ERROR (_logger, msg);\
	}\
	else\
		cerr << msg << endl;\
}

namespace scidbtestharness
{
namespace Exceptions
{

/**
 * basic class only displaying the message that was set in its member variable '_msg'
 */
class ERROR
{
	public :
		ERROR (void) {}
		ERROR (std::string m) : _msg(m) {}

		virtual std::string what (void) throw();
		virtual ~ERROR () throw () {}

	protected :
		std::string _msg;
};

/**
 * accesses configuration error messages
 */
class ConfigError : public ERROR
{
	public :
		ConfigError (const std::string &filename, int linenum, const std::string &functionname, int c) : ERROR ()
		{
			_msg = "";
			if (c<=config_errdb.errorcodeMin || c>=config_errdb.errorcodeMax)
			{
				_msg = _msg + filename + ":" + iTos (linenum) + ":" + functionname + "(): ";
				_msg = _msg + "Out of range. Invalid Error Code mentioned for ConfigError.";
				throw std::range_error (_msg);
			}

			_msg = _msg + filename + ":" + iTos (linenum) + ":" + functionname + "(): ";
			_code = c;
		}

		ConfigError (const std::string &filename, int linenum, const std::string &functionname, const std::string &m) : ERROR ()
		{
			_msg = "";
			_msg = _msg + filename + ":" + iTos (linenum) + ":" + functionname + "(): "  + m;
			_code = NO_CODE;
		}

		~ConfigError () throw () {}
		std::string what (void) throw();

	private :
		int _code;
};

/**
 * accesses system error messages
 */
class SystemError : public ERROR
{
	public :
		SystemError (const std::string &filename, int linenum, const std::string &functionname, int c) : ERROR ()
		{
			_msg = "";
			if (c<=system_errdb.errorcodeMin || c>=system_errdb.errorcodeMax)
			{
				_msg = _msg + filename + ":" + iTos (linenum) + ":" + functionname + "(): " ;
				_msg = _msg + "Out of range. Invalid Error Code mentioned for ConfigError.";
				throw std::range_error (_msg);
			}

			_msg = _msg + filename + ":" + iTos (linenum) + ":" + functionname + "(): ";
			_code = c;
		}

		SystemError (const std::string &filename, int linenum, const std::string &functionname, const std::string &m) : ERROR ()
		{
			_msg = "";
			_msg = _msg + filename + ":" + iTos (linenum) + ":" + functionname + "(): " + m;
			_code = NO_CODE;
		}

		~SystemError () throw () {}
		std::string what (void) throw();

	private :
		int _code;
};

/**
 * accesses executor error messages
 */
class ExecutorError : public ERROR
{
	public :
		ExecutorError (const std::string &filename, int linenum, const std::string &functionname, int c) : ERROR ()
		{
			_msg = "";
			if (c<=executor_errdb.errorcodeMin || c>=executor_errdb.errorcodeMax)
			{
				_msg = _msg + filename + ":" + iTos (linenum) + ":" + functionname + "(): " ;
				_msg = _msg + "Out of range. Invalid Error Code mentioned for ConfigError.";
				throw std::range_error (_msg);
			}

			_msg = _msg + filename + ":" + iTos (linenum) + ":" + functionname + "(): ";
			_code = c;
		}

		ExecutorError (const std::string &filename, int linenum, const std::string &functionname, const std::string &m) : ERROR ()
		{
			_msg = "";
			_msg = _msg + filename + ":" + iTos (linenum) + ":" + functionname + "(): " + m;
			_code = NO_CODE;
		}

		~ExecutorError () throw () {}
		std::string what (void) throw();

	private :
		int _code;
};

} //END namespace Exceptions
} //END namespace scidbtestharness

# endif
