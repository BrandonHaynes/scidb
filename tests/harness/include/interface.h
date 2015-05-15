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
 * @file interface.h
 * @author girish_hilage@persistent.co.in
 * @brief file containing pure interfaces (i.e. abstract base classes)
 */

# ifndef INTERFACE_H
# define INTERFACE_H

# include <log4cxx/logger.h>
# include <log4cxx/ndc.h>

# include "global.h"

# define DAEMON        1
# define COMMANDLINE   2

namespace scidbtestharness
{

namespace executors
{
/**
 * Interface for scidb test harness's actual test case executors
 */
class Executor
{
	public :
		Executor (void)
		{ }

		virtual ~Executor (void)
		{
			if (_loggerEnabled)
			{
				LOGGER_POP_NDCTAG;
				_logger->removeAllAppenders ();
			}
		}

		virtual long getTotalCaseExecutionTime (void) = 0;
		virtual int validateParameters (void) = 0;
		virtual int execute (InfoForExecutor &ie) = 0;

	protected :
		struct CaseExecutionTime
		{
			long setupTime;
			long testTime;
			long cleanupTime;
			long totalTime;
		};

		CaseExecutionTime _caseexecTime;
		log4cxx::LogString saved_context;
		log4cxx::LoggerPtr _logger;          /* logger */
		bool _loggerEnabled;
};

} //END namespace executors
} //END namespace scidbtestharness

/* ______________________________________________________________________________ */
namespace interface
{
/**
 * Interface for general applications
 */
class Application
{
	public :
		Application (void)
		{ }

		virtual ~Application (void)
		{ }

		int run (unsigned int argc, char **argv, int mode)
		{
			if (parseCommandLine (argc, argv) == FAILURE ||
				execute (mode) == FAILURE)
			{
				return FAILURE;
			}

			return SUCCESS;
		}

	private :
		virtual int parseCommandLine (unsigned int argc, char **argv) = 0;
		virtual int execute (int) = 0;
};
} //END namespace interface

# endif
