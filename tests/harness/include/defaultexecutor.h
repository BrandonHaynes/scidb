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
 * @file defaultexecutor.h
 * @author girish_hilage@persistent.co.in
 * @brief file containing test case executor to be used by default
 */

# ifndef DEFAULTEXECUTOR_h
# define DEFAULTEXECUTOR_h

#include <vector>
#include <set>
#include <map>
#include <list>
#include <fstream>
#include <sstream>
#include <boost/filesystem/fstream.hpp>
#include <boost/date_time/posix_time/posix_time_types.hpp>
#include <log4cxx/logger.h>
#include <log4cxx/ndc.h>

#include "global.h"
#include "interface.h"

#include "SciDBAPI.h"

# define MAX_TESTCASE_SECTIONS   3

namespace scidbtestharness
{
namespace executors
{

/* struct to store arguments for options(e.g. --out) of --shell command */
struct ShellCommandOptions
{
	ShellCommandOptions (void)
	{
		_store = false;
		_storeAll = false;
		_cwd = "";
	}

	std::string _command;
	std::string _outputFile;
	bool _store, _storeAll;
	std::string _cwd;
};

/* struct to store arguments for options of --igdata command */
struct IgnoreDataOptions
{
	IgnoreDataOptions (void)
	{
		_afl = true;
	}

	bool _afl;
	std::string _query;
};

/* struct to store arguments for options of --justrun command */
struct JustRunCommandOptions
{
	JustRunCommandOptions (void)
	{
		_afl = true;
		_igdata = false;
	}

	bool _afl;
	bool _igdata;
	std::string _query;
};

/* struct to store arguments for options of --error command */
struct ErrorCommandOptions
{
	ErrorCommandOptions (void):
	    _expected_errorcode(""),
        _expected_errorcode2(""),
	    _expected_errns(""),
	    _expected_errshort(~0)
	{
		_afl = true;
		_igdata = false;
		_hideQueryString = false;
	}

	std::string _expected_errorcode; // stringified code id
	std::string _expected_errorcode2;// non-stringified more compact code id

	std::string _expected_errns; //error namespace
    int32_t _expected_errshort;  //short error code
    std::set<int32_t> _expected_errlong;   //long error code

	bool _afl;
	bool _igdata;
	bool _hideQueryString;
	std::string _query;
};

/* struct for all commands (e.g. --echo, --start-query-logging, --stop-query-logging) except (--setup, --test, --cleanup) */
struct Command
{
	std::string cmd;
	std::string args;
	void *extraInfo;
	std::vector<Command> subCommands;
};

/**
 * An executor to be used by default.
 * It will read the test case file, parse it and execute all the commands inside it.
 * It will send actual SciDB queries to the database for execution.
 */
class DefaultExecutor : public Executor
{
	public :
		DefaultExecutor (void) : Executor (), _scidb(scidb::getSciDB())
		{
			_dbconnection = 0;
			_justrun_flag = false;
			_ignoredata_flag = false;

			_caseexecTime.setupTime   = -1;
			_caseexecTime.testTime    = -1;
			_caseexecTime.cleanupTime = -1;
			_caseexecTime.totalTime   = -1;
			_loggerEnabled = false;
			_queryLogging = false;
                        _ignoreWarnings = false;
			_timerfileOpened = false;
			_timerEnabled = false;
			_outputFormat = "dcsv";
			_errorCodesDiffer = false;
			_precisionSet = false;
			_precisionDefaultValue = 6;
		}

		~DefaultExecutor () throw()
		{
			_resultfileStream.close();
			_timerfileStream.close();
			if (_dbconnection)
			{
				_scidb.disconnect (_dbconnection);
				_dbconnection = 0;
			}
		}

	private :
		const scidb::SciDB& _scidb;
		void *_dbconnection;

		/* flag to specify if the query is AQL/AFL */
		bool _afl;

		/* Flag to indicate that the command should just run with no regard to whether it will succeed or fail.
         * Executor will proceed with the next command. */
		bool _justrun_flag;

		/* Flag to indicate if the output of the shell command should be stored inside the .expected/.out file along with the output of SciDB queries. */
		bool _ignoredata_flag;

		std::stringstream _errStream;
		/**
		 * <test_name>.out file
		 */
		boost::filesystem::ofstream _resultfileStream;

		/**
		 * vector storing the commands in the test case file after the parsing is complete
		 */
		std::vector<Command> *_currentSection;
		std::vector<Command> _preSetupCommands;
		std::vector<Command> _setupCommands;
		std::vector<Command> _testCommands;
		std::vector<Command> _cleanupCommands;
		
		std::map<std::string,std::string> _testEnvVars;

		/**
		 * information gathered from the harness that is required during executors execution
		 */
		InfoForExecutor _ie;

		/**
		 * tag in the form "EXECUTOR[<thread_id>]" to be placed in the corresponding .log file
		 */
		std::string _executorTag;

		/**
		 * flag specifying whether to log the actual SciDB query
		 */
		bool _queryLogging;

                /**
                 * flag specifying whether to ignore warnings
                 */
                bool _ignoreWarnings;

		/**
		 * file for storing timer values
		 */
		boost::filesystem::ofstream _timerfileStream;
		bool _timerfileOpened;
		std::vector<std::string> _timerTags;
		bool _timerEnabled;
		std::string _outputFormat;
		bool _errorCodesDiffer;
		bool _precisionSet;
		int _precisionDefaultValue;
		boost::posix_time::ptime _timerStarttime;

		/**
		 * @return Returns total time taken by the test case for execution
		 */
		long getTotalCaseExecutionTime (void)
		{
			return _caseexecTime.totalTime;
		}

		/**
		 * Cause test case to exit in the middle
		 * @return Returns exit code EXIT
		 */
		int exit (void);

		std::string getErrorCodeFromException (const std::string &errstr);

		/**
		 * Sends actual SciDB query to the database for execution.
		 * Reads the result of the query and stores it into the .out file
		 *
		 * @param query Specifies the actual SciDB query string
		 * @param errorInfo Specifies the expected error code if the query is expected to fail
		 * @return FAILURE if errorInfo is given but exception does not occur
		 * @return SUCCESS otherwise
		 */
		int runSciDBquery (const std::string &query, const ErrorCommandOptions* errorInfo = NULL);

		/**
 		 * stop measuring the time and write it into the .timer file for the corresponding tag
 		 * @param args represents the tag name for the current timer
 		 *
 		 * @return SUCCESS
 		 */
		int stopTimer (const std::string &args);

		/**
 		 * opens the .timer file if not already opened and start measuring the time
 		 * @param args represents the tag name for the current timer
 		 *
 		 * @return SUCCESS
 		 */
		int startTimer (const std::string &args); 
		int setOutputFormat (const std::string &args);
		int endOutputFormat (void);
		int setPrecision (const std::string &args);

		int Shell (ShellCommandOptions *sco);
		void initializeCommand (std::string &command);
                /** 
                 * Disconnects any established connection to SciDB.
                 */
                void disconnectFromSciDB(void);
                
		/**
 		 * writes the string passed as an argument into the actual output file .out as is
 		 * @param args represents the string to be written to the .out file
 		 *
 		 * @return SUCCESS
 		 */
		int Echo (const std::string &args);

		/**
 		 * calls a routine corresponding to each command in the test case file
 		 * @param cmd string representing the test file command
 		 * @param args string representing the arguments required for the corresponding command
 		 *
 		 * @return SUCCESS, FAILURE, EXIT
 		 */
		int execCommand (const std::string &cmd, const std::string &args, void *extraInfo=0);

		/**
 		 * calls a routine execCommand() for each command and it's subcommands in the test case file
 		 * @param section contains each test file command and its corresponding arguments
 		 *
 		 * @return SUCCESS, FAILURE, EXIT
 		 */
		int execCommandsection (const Command &section);
		int execCommandVector (const std::vector<Command> &commandvector);

		/**
 		 * calls a routine execCommandsection() for each command section in the test case file
 		 *
 		 * @return SUCCESS, FAILURE
 		 */
		int executeTestCase (void);

		/**
 		 * prints all the test file commands stored in the vector in the indentated form
 		 * @param vc vector of commands and subcommands in the test case file
 		 * @param indentation indentation required at each level
 		 */
		void printParsedVector (std::vector <Command> vc, const std::string &indentation="");
		void printParsedSections (void);
		void parseEnvironmentVariables (std::string &line, int line_number);

		int remove_option (std::string &line, const std::string option, std::string &parametervalue, bool mandatory=true);
		int parseShellCommandOptions (std::string line, struct ShellCommandOptions *sco);
		int parseIgnoreDataOptions (std::string line, struct IgnoreDataOptions *ido);
		int parseJustRunCommandOptions (std::string line, struct JustRunCommandOptions *jco);
		int parseErrorCommandOptions (std::string line, struct ErrorCommandOptions *eco);
		/**
 		 * parses the test case file and store the test file commands and its subcommands
 		 * into the vector of commands
 		 *
 		 * @return SUCCESS, FAILURE
 		 */
		int parseTestCaseFile (void);

		/**
 		 * prints all the values of the variables that contain executor environment
 		 */
		void printExecutorEnvironment (void);

		/**
 		 * create a logger for the corresponding test case
 		 * @return SUCCESS, FAILURE
 		 */
		int createLogger (void);

		/**
 		 * validate parameter values received from the harness
 		 * @return SUCCESS, FAILURE
 		 */
		int validateParameters (void);

		/**
 		 * get the variable values from the harness required for its execution
 		 * @param ie contains the information provided by the harness for executors execution
 		 */
		void copyToLocal (const InfoForExecutor &ie);

		/**
		 * club together copying of information from harness to executor,
		 * validating of those parameters,
		 * logger creation,
		 * parsing of a test case file,
		 * executing the test case
		 *
		 * @param ie contains the information provided by the harness for executors execution
		 *
		 * @return SUCCESS, FAILURE
		 */
		int execute (InfoForExecutor &ie);
		
		/**
		 * Sets up test-specific environment variables for use in shell command (e.g. TESTDIR
		 * variable will point to the path where the .test file is located).
		 */
		void addTestSpecificVariables(void);
		
		/**
		 * Inserts the test-specific environment variables into the current process which makes
		 * them available in the child processes also.
		 */
		void setTestSpecificVariables(void);
};

} //END namespace executors
} //END namespace scidbtestharness
# endif
