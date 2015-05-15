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
 * @file harnesstestexecutor.h
 * @author girish_hilage@persistent.co.in
 * @brief file containing test case executor for harness selftesting
 */

# ifndef HARNESSTESTEXECUTOR_H
# define HARNESSTESTEXECUTOR_H

# include <vector>
# include <list>
# include <fstream>
# include <boost/filesystem/fstream.hpp>
# include <log4cxx/logger.h>
# include <log4cxx/ndc.h>

# include "global.h"
# include "interface.h"

namespace scidbtestharness
{
namespace executors
{

/**
 * An executor to be used for the purpose of testing of the test haness (i.e. harness selftesting).
 * It will work similar to Default executor.
 */
class HarnessTestExecutor : public Executor
{
	public :
		HarnessTestExecutor (void) : Executor ()
		{
			_caseexecTime.setupTime   = -1;
			_caseexecTime.testTime    = -1;
			_caseexecTime.cleanupTime = -1;
			_caseexecTime.totalTime   = -1;
			_loggerEnabled = false;
		}

		~HarnessTestExecutor () throw()
		{ }

	private :
		InfoForExecutor _ie;
		std::string _executorTag;
		std::string _shellscript;

		long getTotalCaseExecutionTime (void)
		{
			return _caseexecTime.totalTime;
		}

		int executeTestCase (void);
		int prepareShellscript (void);
		void printExecutorEnvironment (void);
		int createLogger (void);
		int validateParameters (void);
		void copyToLocal (const InfoForExecutor &ie);
		int execute (InfoForExecutor &ie);
};

} //END namespace executors
} //END namespace scidbtestharness
# endif
