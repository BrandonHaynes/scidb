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
 * @file suite.h
 * @author girish_hilage@persistent.co.in
 * @brief file containing pure interfaces (i.e. abstract base classes)
 */

# ifndef SUITE_H
# define SUITE_H

# include <string>
# include <vector>
# include <log4cxx/logger.h>

# include "manager.h"
# include "reporter.h"
# include "global.h"

namespace scidbtestharness
{

/**
 * collects all the suites under the given suite id,
 * collects all the tests cases under those subsuites,
 * gives those subsuites one by one to the MANAGER to run them.
 */
class Suite
{
	private :
		std::string _suiteId;
		std::vector <std::string> _subSuites;
		std::vector <std::string> _tcList;
		std::vector <std::string> _skiptcList;
		log4cxx :: LoggerPtr _logger;

	public :
		Suite (const std::string sid) : _suiteId(sid)
		{
			_logger = log4cxx :: Logger :: getLogger (HARNESS_LOGGER_NAME);
		}

		int run (const std::string root_dir, std::string skiptestfname, const std::vector<std::string> &skip_tclist, const std::string regex_expr,
                 RegexType regex_flag, MANAGER &M, int no_parallel_testcases, int *testcases_total, int *nSkipped, REPORTER *rptr, int *suitesSkipped);
		int collectSubSuites (std::string parentdir, std::string sid);
};
} //END namespace scidbtestharness

# endif
