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
 * @file manager.h
 * @author girish_hilage@persistent.co.in
 * @brief file containing thread manager class
 */

# ifndef MANAGER_H
# define MANAGER_H

# include <vector>
# include <string>
# include <boost/thread/thread.hpp>
# include <log4cxx/logger.h>

# include "global.h"
# include "reporter.h"

# define DEFAULT_nWORKERS 1

namespace scidbtestharness
{

/**
 * class responsible for creating the worker threads,
 * allocating the jobs to worker threads etc.
 * Allocates each worker thread a single test case file for execution as a job.
 */
class MANAGER
{
	private :
		boost::thread_group    _G;
		int                    _nWorkers;
		int                    _terminateOnFailure;
		ExecutorType           _executorType;
		struct InfoForExecutor _ie;
		log4cxx::LoggerPtr     _logger;

	public :
		MANAGER ()
		{
			_nWorkers = -1;
			_terminateOnFailure = 0;
		}

		void join_all (void)
		{
			_G.join_all ();
		}
		void cleanup (void);
		struct ExecutionStats getExecutionStats (void);
		int runJob (std::vector <std::string> &joblist, REPORTER *rptr);
		void createWorkgroup (int number_of_workers = DEFAULT_nWORKERS);
		void getInfoForExecutorFromharness (const HarnessCommandLineOptions &c, ExecutorType executor_type);
		void useLogger (const std::string logger_name)
		{
			_logger = log4cxx::Logger::getLogger (logger_name);
		}
		
		~MANAGER ()
		{
			//cleanup ();
		}
};
} //END namespace scidbtestharness

# endif
