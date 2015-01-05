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
 * @file harness.h
 * @author girish_hilage@persistent.co.in
 * @brief file containing a concrete class for actual harness executable
 */

# ifndef HARNESS_H
# define HARNESS_H

# include <string>
# include <vector>
# include <log4cxx/logger.h>
# include <log4cxx/ndc.h>

# include "global.h"
# include "interface.h"
# include "manager.h"
# include "reporter.h"

# define DEFAULT_SUITE_ID               "t"
# define DEFAULT_LOG_DIR                "log"
# define DEFAULT_REPORT_FILENAME        "Report.xml"
# define DEFAULT_PARALLEL_TESTCASES     1
# define DEFAULT_HARNESSLOGFILE         "harness.log"

class HarnessTest;
namespace scidbtestharness
{
/**
 * a concrete class for actual harness executable
 */
class SciDBTestHarness : public interface::Application
{
	public :
		SciDBTestHarness (ExecutorType executor_type=DEFAULT_TC_EXECUTOR) : _executorType(executor_type)
		{
			initConfDefault ();
			_rptr = 0;
			_loggerEnabled = false;
		}

		~SciDBTestHarness () throw()
		{
			log4cxx::NDC :: pop ();
			log4cxx::NDC :: remove ();
			if (_rptr)
				delete _rptr;
		}

		friend class HarnessTest;
	private :
		std::string _cwd;
		struct HarnessCommandLineOptions _c;
		std::vector <std::string> _tcList;
		ExecutorType _executorType;        /* executor to be used (i.e. either DefaultExecutor or HarnessTestExecutor) */
		MANAGER _M;                          /* job manager */
		REPORTER* _rptr;                     /* reporter */
		log4cxx::LoggerPtr _logger;          /* logger */
		struct ExecutionStats _harnessES;
		bool _loggerEnabled;

	private :
		int runSuites (const std::vector<std::string> &skip_tclist);
		int runTests (const std::vector<std::string> &skip_tclist);
		int execute (int mode);
		void printConf (void);

		int createLogger (void);
		void createReporter (void)
		{
			_rptr = new REPORTER (_c.reportFilename, REPORT_WRITE);
		}
		void resultDirCleanup (const std::string resultdir);
		void cleanUpLog (const std::string rootdir, const std::string logdir, const std::string reportfile);
		int validateParameters (void);
		int parseCommandLine (unsigned int argc, char** argv);
		void initConfDefault (void);
};
} //END namespace scidbtestharness

# endif
