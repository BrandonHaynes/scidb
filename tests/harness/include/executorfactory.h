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
 * @file executorfactory.h
 * @author girish_hilage@persistent.co.in
 * @brief file containing factory class for supplying different types of executors
 * (e.g. default executor, harnesstest executor for harness selftesting)
 */

# ifndef EXECUTORFACTORY_H
# define EXECUTORFACTORY_H

# include "global.h"
# include "interface.h"

namespace scidbtestharness
{
namespace executors
{

/**
 * a factory class for supplying different types of executors
 * (e.g. default executor, harnesstest executor for harness selftesting)
 */
class ExecutorFactory
{
	public :
		Executor *getExecutor (ExecutorType executor_type=DEFAULT_TC_EXECUTOR);
};

} //END namespace executors
} //END namespace scidbtestharness
# endif
