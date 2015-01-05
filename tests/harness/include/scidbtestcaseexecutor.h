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
 * @file scidbtestcaseexecutor.h
 * @author girish_hilage@persistent.co.in
 * @brief file containing a concrete class for scidbtest executable which executes
 * a single test case at a time
 */

# ifndef SCIDBTCEXECUTOR_H
# define SCIDBTCEXECUTOR_H

# include "global.h"
# include "interface.h"
# include "executorfactory.h"

namespace scidbtestharness
{

/**
 * a concrete class for scidbtest executable which is able to execute only a single test case at a time
 */
class SciDBTCExecutor : public interface::Application
{
	public :
		SciDBTCExecutor (ExecutorType executor_type = DEFAULT_TC_EXECUTOR)
		{
			initConfDefault ();
			_actualExecutor = _f.getExecutor (executor_type);
		}

		~SciDBTCExecutor () throw()
		{
			if (_actualExecutor)
				delete _actualExecutor;
		}

	private :
		executors::ExecutorFactory _f;
		executors::Executor *      _actualExecutor;
		InfoForExecutor            _ie;
		ExecutorCommandLineOptions _rc;

	private :
		void fillupExecutorInfo (bool internally_called=false);
		int execute (int mode);
		int validateParameters (void);
		int parseCommandLine (unsigned int argc, char** argv);
		void initConfDefault (void);
};
} //END namespace scidbtestharness

# endif
