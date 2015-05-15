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
 * @file global.h
 * @author girish_hilage@persistent.co.in
 * @brief file containing global macros, structure, variable definitions
 */

# ifndef GLOBAL_H
# define GLOBAL_H

# include <iostream>
# include <string>
# include <vector>

# define LOGGER_PROPERTIES_FILE        "log4j.properties" 
# define SCIDBCAPI_LOGGER_NAME         "scidb.services.network" 

# define DEFAULT_SCIDB_CONNECTION      "localhost" 
# define DEFAULT_SCIDB_PORT             1239
# define DEFAULT_DEBUGLEVEL             3
# define MIN_DEBUG_LEVEL                0
# define MAX_DEBUG_LEVEL                5

# define HARNESS_LOGGER_NAME            "SciDBTestHarness"
# define DEFAULT_XML_OUTPUT_FILE        "XmlResult.xml"
# define DEFAULT_TEST_CASE_DIR          "t/"
# define DEFAULT_RESULT_DIR             "r/"
# define DEFAULT_SKIP_TEST_FILE_NAME    "disable.tests"
# define DEFAULT_SKIP_TEST_OPTION       "yes"
# define DEFAULT_TESTCASE_FILE_EXTENSION ".test"

# define LOGDESTINATION_FILE      "file"
# define LOGDESTINATION_CONSOLE   "console"

enum RegexType
{
REGEX_FLAG_NO_REGEX_EXPR = 0,
REGEX_FLAG_INCLUDE_ID,
REGEX_FLAG_EXCLUDE_ID,
REGEX_FLAG_INCLUDE_NAME,
REGEX_FLAG_EXCLUDE_NAME
};

# define DEBUGLEVEL_FATAL    0
# define DEBUGLEVEL_ERROR    1
# define DEBUGLEVEL_WARN     2
# define DEBUGLEVEL_INFO     3
# define DEBUGLEVEL_DEBUG    4
# define DEBUGLEVEL_TRACE    5

# define LINE_FEED  '\n'

# define FAILURE   -1
# define SUCCESS    0
# define EXIT       1
# define ERROR_CODES_DIFFER 2

# define LOGGER_PUSH_NDCTAG(tag) \
{\
    log4cxx::NDC :: get(saved_context);\
    log4cxx::NDC :: clear();\
    log4cxx::NDC :: push(tag);\
}

# define LOGGER_POP_NDCTAG \
{\
    log4cxx::NDC :: pop();\
    log4cxx::NDC :: remove();\
    log4cxx::NDC :: push(saved_context);\
}

namespace scidbtestharness
{

enum Result
{
	RESULT_PASS,
	RESULT_SYSTEM_EXCEPTION,
	RESULT_CONFIG_EXCEPTION,
	RESULT_FILES_DIFFER,
	RESULT_SKIPPED,
	RESULT_RECORDED,
	RESULT_ERROR_CODES_DIFFER
};

enum ExecutorType
{
	DEFAULT_TC_EXECUTOR,
	HARNESSTEST_EXECUTOR
};

struct ExecutionStats
{
	ExecutionStats ()
	{
		testcasesTotal = testcasesPassed = testcasesFailed = testcasesSkipped = testsuitesSkipped = 0;
	}
	unsigned int testcasesTotal;
    unsigned int testcasesPassed;
	unsigned int testcasesFailed;
	unsigned int testcasesSkipped;
	unsigned int testsuitesSkipped;
};

struct IntermediateStats
{
	IntermediateStats ()
	{
		testcasesPassed = testcasesFailed = 0;
	}

	IntermediateStats (int passed, int failed)
	{
		testcasesPassed = passed;
		testcasesFailed = failed;
	}

    unsigned int testcasesPassed;
	unsigned int testcasesFailed;
};

struct TestcaseExecutionInfo
{
    std::string testID;
    std::string description;
    long int sTime;
    long int eTime;
    Result result;
    std::string failureReason;
};

struct InfoForExecutor
{
	std::string        tcfile;
    std::string        connectionString;
	int                scidbPort;
    std::string        rootDir;
    int                sleepTime;
    std::string        logDir;
    std::string        scratchDir;
    std::string        logDestination;
    std::string        log_prop_file;
    int                debugLevel;
    bool               record;
    bool               keepPreviousRun;
    bool               selftesting;
    bool               log_queries;
    bool               save_failures;

	std::string        expected_rfile;
	std::string        actual_rfile;
	std::string        timerfile;
	std::string        diff_file;
	std::string        log_file;

	std::string        logger_name;

	/* to be printed by each worker thread */
	int                test_sequence_number;
	pthread_t          tid;
	std::string        testID;

	/* Test Section Time */
	long int startTestSectionMillisec;
	long int endTestSectionMillisec;
};

struct IndividualTestInfo
{
	IndividualTestInfo (InfoForExecutor &ir, TestcaseExecutionInfo &ei)
	{
		testEnvInfo = ir;
		testEi = ei;
	}
	struct InfoForExecutor testEnvInfo;
	struct TestcaseExecutionInfo testEi;
};

/* commandline options for Executor */
struct ExecutorCommandLineOptions
{
    std::string    connectionString;
	int            scidbPort;
    std::string    testFile;
    int            sleepTime;
    int            debugLevel;
    bool           record;
    bool           log_queries;
    bool           save_failures;
};

/* commandline options for harness */
struct HarnessCommandLineOptions
{
    std::string             scidbServer;
    int                     scidbPort;
    std::string             rootDir;
    std::vector<std::string>     testId;
    std::vector<std::string>     testName;
    std::vector<std::string>     suiteId;
    std::string             skipTestfname;
    std::string             regexExpr;
    RegexType               regexFlag;
    int                     sleepTime;
    std::string             logDir;
    std::string             scratchDir;
    std::string             logDestination;
    std::string             log_prop_file;
    std::string             reportFilename;
    int                     parallelTestCases;
    int                     debugLevel;
    std::string             harnessLogFile;
    bool                    record;
    bool                    keepPreviousRun;
    bool                    terminateOnFailure;
    bool                    cleanupLog;
    bool                    selfTesting;
    bool                    log_queries;
    bool                    save_failures;
};

} //END namespace scidbtestharness
# endif
