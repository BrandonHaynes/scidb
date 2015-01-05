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
 * @file reporter.cpp
 * @author girish_hilage@persistent.co.in
 */

# include <fstream>
# include <boost/archive/xml_oarchive.hpp>
# include <log4cxx/ndc.h>

# include "global.h"
# include "reporter.h"
# include "xmlarchive.h"

# define LOGGER_TAG_REPORTER   "[REPORTER]"

using namespace log4cxx;

namespace scidbtestharness
{

int REPORTER :: readHarnessTestReport (struct CDASH_Report &SciDBTestReport)
{
	_xi->load (SciDBTestReport);
	return SUCCESS;
}

/* ________________________________________________________________ */
int REPORTER :: writeFinalInfo (const struct ExecutionStats &harness_execution_stats)
{
    LogString saved_context;
	LOGGER_PUSH_NDCTAG (LOGGER_TAG_REPORTER);

	LOG4CXX_INFO (_logger, "Writing Final Info to report file.");
	_xa->seekp (_prevStoredPosition);
	_xa->putEndTagNOIndent ("TestResults");
	_xa->putStartTag ("FinalStats");
    _xa->save (harness_execution_stats);
	_xa->putEndTag ("FinalStats");
	_xa->putEndTagNOIndent ("SciDBTestReport");
	//_xa->putEndTagNOIndent ("boost_serialization"); /* not required because it's internally put when _xa is deleted */
	_xa->flush ();

	LOGGER_POP_NDCTAG;
	return SUCCESS;
}

int REPORTER :: writeIntermediateRunStat (const int testcases_passed, const int testcases_failed)
{
	_xa->seekp (_prevStoredPosition);
	_xa->putEndTagNOIndent ("TestResults");
	_xa->putStartTag ("IntermediateStats");

	struct IntermediateStats is(testcases_passed, testcases_failed);
    _xa->save (is);

	_xa->putEndTag ("IntermediateStats");
	_xa->putEndTagNOIndent ("SciDBTestReport");
	_xa->putEndTagNOIndent ("boost_serialization");

	_xa->flush ();
	//sleep (2);

	return SUCCESS;
}

int REPORTER :: writeTestcaseExecutionInfo (const IndividualTestInfo &individualtest_info)
{
	_xa->seekp (_prevStoredPosition);
	_xa->putStartTagNOIndent ("IndividualTestResult");
    _xa->save (individualtest_info);
	_xa->putEndTagNOIndent ("IndividualTestResult");
	_xa->flush ();
	if ((_prevStoredPosition = _xa->tellp ()) == FAILURE)
		return FAILURE;

	return SUCCESS;
}

int REPORTER :: writeInitialInfo (const struct HarnessCommandLineOptions &SciDBHarnessEnv)
{
    LogString saved_context;
	LOGGER_PUSH_NDCTAG (LOGGER_TAG_REPORTER);

	LOG4CXX_INFO (_logger, "Writing Initial Info to report file.");
	_xa->putStartTag ("SciDBHarnessEnv");
	_xa->save (SciDBHarnessEnv);
	_xa->putEndTag ("SciDBHarnessEnv");

	_xa->putStartTagNOIndent ("TestResults");
	_xa->flush ();
	if ((_prevStoredPosition = _xa->tellp ()) == FAILURE)
	{
		LOGGER_POP_NDCTAG;
		return FAILURE;
	}

	LOGGER_POP_NDCTAG;
	return SUCCESS;
}
} //END namespace scidbtestharness
