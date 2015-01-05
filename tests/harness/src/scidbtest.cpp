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
 * @file scidbtest.cpp
 * @author girish_hilage@persistent.co.in
 */

# include <cstdlib>

# include "global.h"
# include "scidbtestcaseexecutor.h"
# include "interface.h"
# include "Exceptions.h"
# include "executorfactory.h"

using namespace std;
using namespace scidbtestharness;
namespace harnessexceptions = scidbtestharness::Exceptions;

int main (int argc, char** argv)
{
	interface::Application *a = new scidbtestharness::SciDBTCExecutor(DEFAULT_TC_EXECUTOR);
	int rv;

	try
	{
		if ((rv = a->run (argc, argv, COMMANDLINE)) == FAILURE)
		{
			delete a;
			return EXIT_FAILURE;
		}
	}

	catch (harnessexceptions :: ERROR &e)
	{
		cout << e.what () << endl;
		delete a;
		return EXIT_FAILURE;
	}

    catch (const std::exception& e)
	{
		cout << e.what () << endl;
		delete a;
		return EXIT_FAILURE;
	}

	catch (...)
	{
		cout << "Unhandled Exception caught...\n";
		delete a;
		return EXIT_FAILURE;
	}

	delete a;
	return EXIT_SUCCESS;
}
