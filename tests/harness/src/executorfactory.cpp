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
 * @file executorfactory.cpp
 * @author girish_hilage@persistent.co.in
 */

# include "interface.h"
# include "executorfactory.h"
# include "defaultexecutor.h"
# include "harnesstestexecutor.h"

using namespace scidbtestharness::executors;

namespace scidbtestharness
{
namespace executors
{

Executor * ExecutorFactory :: getExecutor (ExecutorType executor_type)
{
	if (executor_type == DEFAULT_TC_EXECUTOR)
		return new DefaultExecutor ();
	if (executor_type == HARNESSTEST_EXECUTOR)
		return new HarnessTestExecutor ();

	return NULL;
}

} //END namespace scidbtestharness
} //END namespace executors
