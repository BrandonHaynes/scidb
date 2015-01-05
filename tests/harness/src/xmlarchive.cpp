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
 * @file xmlarchive.cpp
 * @author girish_hilage@persistent.co.in
 */

# include <string>
# include <boost/config.hpp>
# include <boost/archive/archive_exception.hpp>
# include <boost/lexical_cast.hpp>

# include "global.h"
# include "helper.h"
# include "xmlarchive.h"

# define XML_CHAR_TAB         '\t'

using namespace std;

namespace scidbtestharness
{

void XMLiArchive :: load (struct CDASH_Report &SciDBTestReport)
{
	try
	{
		(*this) & BOOST_SERIALIZATION_NVP (SciDBTestReport);
	}

	catch (boost::archive::archive_exception &e)
	{
		//std::cout << "Caught XML serialization exception\n";
		//std::cout << e.what() << std::endl;
		return;
	}
}

/* ________________________________________________________________ */
void XMLArchive :: save (const struct ExecutionStats &harness_es)
{
	unsigned int TotalTestCases = harness_es.testcasesTotal;
	(*this) << BOOST_SERIALIZATION_NVP(TotalTestCases);

	unsigned int TotalTestsPassed = harness_es.testcasesPassed;
	(*this) << BOOST_SERIALIZATION_NVP(TotalTestsPassed);

	unsigned int TotalTestsFailed = harness_es.testcasesFailed;
	(*this) << BOOST_SERIALIZATION_NVP(TotalTestsFailed);

	unsigned int TotalTestsSkipped = harness_es.testcasesSkipped;
	(*this) << BOOST_SERIALIZATION_NVP(TotalTestsSkipped);

	unsigned int TotalSuitesSkipped = harness_es.testsuitesSkipped;
	(*this) << BOOST_SERIALIZATION_NVP(TotalSuitesSkipped);
}

void XMLArchive :: save (const struct IntermediateStats &is)
{
	unsigned int TotalTestsPassed = is.testcasesPassed;
	(*this) << BOOST_SERIALIZATION_NVP(TotalTestsPassed);

	unsigned int TotalTestsFailed = is.testcasesFailed;
	(*this) << BOOST_SERIALIZATION_NVP(TotalTestsFailed);
}

void XMLArchive :: save (const struct IndividualTestInfo &iti)
{
	string TestID = iti.testEi.testID;
	putCHAR (XML_CHAR_TAB);
	(*this) << BOOST_SERIALIZATION_NVP(TestID);

	string TestDescription = iti.testEi.description;
	putCHAR (XML_CHAR_TAB);
	(*this) << BOOST_SERIALIZATION_NVP(TestDescription);

	time_t startTime = iti.testEi.sTime/1000;
	time_t endTime = iti.testEi.eTime/1000;
	string TestStartTime = ctime (&startTime);
	string TestEndTime = ctime (&endTime);

	putCHAR (XML_CHAR_TAB);
	(*this) << BOOST_SERIALIZATION_NVP(TestStartTime);
	putCHAR (XML_CHAR_TAB);
	(*this) << BOOST_SERIALIZATION_NVP(TestEndTime);

//	string TestTotalExeTime = iTos (endTime - startTime);
	string TestTotalExeTime = boost::lexical_cast<std::string> ((double(time_t(iti.testEi.eTime) - time_t(iti.testEi.sTime)))/1000);
	putCHAR (XML_CHAR_TAB);
	(*this) << BOOST_SERIALIZATION_NVP(TestTotalExeTime);

	string TestcaseFile = iti.testEnvInfo.tcfile;
	putCHAR (XML_CHAR_TAB);
	(*this) << BOOST_SERIALIZATION_NVP(TestcaseFile);

	string TestcaseExpectedResultFile = iti.testEnvInfo.expected_rfile;
	putCHAR (XML_CHAR_TAB);
	(*this) << BOOST_SERIALIZATION_NVP(TestcaseExpectedResultFile);

	string TestcaseActualResultFile = iti.testEnvInfo.actual_rfile;
	putCHAR (XML_CHAR_TAB);
	(*this) << BOOST_SERIALIZATION_NVP(TestcaseActualResultFile);

	string TestcaseTimerFile = iti.testEnvInfo.timerfile;
	putCHAR (XML_CHAR_TAB);
	(*this) << BOOST_SERIALIZATION_NVP(TestcaseTimerFile);

	string TestcaseDiffFile = iti.testEnvInfo.diff_file;
	putCHAR (XML_CHAR_TAB);
	(*this) << BOOST_SERIALIZATION_NVP(TestcaseDiffFile);

	string TestcaseResult;
	switch (iti.testEi.result)
	{
		case RESULT_PASS               : TestcaseResult = "PASS";                break;
		case RESULT_ERROR_CODES_DIFFER : TestcaseResult = "ERROR_CODES_DIFFER";  break;
		case RESULT_SYSTEM_EXCEPTION   : /* fallthrough */
		case RESULT_CONFIG_EXCEPTION   : /* fallthrough */
		case RESULT_FILES_DIFFER       : TestcaseResult = "FAIL";                break;
		case RESULT_SKIPPED            : TestcaseResult = "SKIPPED";             break;
		case RESULT_RECORDED           : TestcaseResult = "RECORDED";            break;
	}
	putCHAR (XML_CHAR_TAB);
	(*this) << BOOST_SERIALIZATION_NVP(TestcaseResult);

	string TestcaseFailureReason = iti.testEi.failureReason;
	putCHAR (XML_CHAR_TAB);
	(*this) << BOOST_SERIALIZATION_NVP(TestcaseFailureReason);

	string TestcaseLogFile = iti.testEnvInfo.log_file;
	putCHAR (XML_CHAR_TAB);
	(*this) << BOOST_SERIALIZATION_NVP(TestcaseLogFile);
}

void XMLArchive :: save (const struct HarnessCommandLineOptions &SciDBHarnessEnv)
{
    string scidbServer = SciDBHarnessEnv.scidbServer;
	(*this) << BOOST_SERIALIZATION_NVP(scidbServer);

    int scidbPort = SciDBHarnessEnv.scidbPort;
	(*this) << BOOST_SERIALIZATION_NVP(scidbPort);

	string rootDir = SciDBHarnessEnv.rootDir;
	(*this) << BOOST_SERIALIZATION_NVP(rootDir);

	string skipTestfname = SciDBHarnessEnv.skipTestfname;
	(*this) << BOOST_SERIALIZATION_NVP(skipTestfname);

	int regexFlag = SciDBHarnessEnv.regexFlag;
	(*this) << BOOST_SERIALIZATION_NVP(regexFlag);
	string regexExpr = SciDBHarnessEnv.regexExpr;
	(*this) << BOOST_SERIALIZATION_NVP(regexExpr);

	string logDir = SciDBHarnessEnv.logDir;
	(*this) << BOOST_SERIALIZATION_NVP(logDir);

	string reportFilename = SciDBHarnessEnv.reportFilename;
	(*this) << BOOST_SERIALIZATION_NVP(reportFilename);

	int parallelTestCases = SciDBHarnessEnv.parallelTestCases;
	(*this) << BOOST_SERIALIZATION_NVP(parallelTestCases);

	int debugLevel = SciDBHarnessEnv.debugLevel;
	(*this) << BOOST_SERIALIZATION_NVP(debugLevel);

	if (!SciDBHarnessEnv.selfTesting)
	{
		bool record = SciDBHarnessEnv.record;
		(*this) << BOOST_SERIALIZATION_NVP(record);
	}

	bool keepPreviousRun = SciDBHarnessEnv.keepPreviousRun;
	(*this) << BOOST_SERIALIZATION_NVP(keepPreviousRun);

	bool terminateOnFailure = SciDBHarnessEnv.terminateOnFailure;
	(*this) << BOOST_SERIALIZATION_NVP(terminateOnFailure);
}
} //END namespace scidbtestharness
