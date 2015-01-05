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
 * @file scidbtestcaseexecutor.cpp
 * @author girish_hilage@persistent.co.in
 */

# include <iostream>
# include <string>
# include <sstream>
# include <boost/filesystem/operations.hpp>
# include <boost/program_options/options_description.hpp>
# include <boost/program_options/variables_map.hpp>
# include <boost/program_options/parsers.hpp>

# include "global.h"
# include "helper.h"
# include "errdb.h"
# include "Exceptions.h"
# include "scidbtestcaseexecutor.h"

using namespace std;
using namespace scidbtestharness :: Exceptions;
namespace bfs = boost::filesystem;
namespace po = boost::program_options;
namespace harnessexceptions = scidbtestharness::Exceptions;

namespace scidbtestharness
{

void SciDBTCExecutor :: fillupExecutorInfo (bool internally_called)
{
	_ie.connectionString  = _rc.connectionString;
	_ie.scidbPort         = _rc.scidbPort;
	_ie.tcfile	          = _rc.testFile;
	_ie.sleepTime	      = _rc.sleepTime;
	_ie.debugLevel	      = _rc.debugLevel;
	_ie.record	          = _rc.record;

	prepare_filepaths (_ie, internally_called);
}

int SciDBTCExecutor :: execute (int mode)
{
	Result result = RESULT_SYSTEM_EXCEPTION;

	try
	{
		fillupExecutorInfo ();

		int retValue;
		string result_str = "FAILED";
		string failureReason("Test case execution failed, Check log file.");

		retValue = _actualExecutor->execute (_ie);
		cout << "Executor returned : " << (retValue == SUCCESS ? "SUCCESS" : "FAILURE") << endl;

		if (retValue == SUCCESS)
		{
			if (_rc.record)                 // PASS
			{
				bfs::remove (_ie.expected_rfile);	
				bfs::rename (_ie.actual_rfile, _ie.expected_rfile);	
				result = RESULT_RECORDED;
				result_str = "RECORDED";
				failureReason = "";
			}

			else
			{
				cout << "Going to compare the files now.\n";
				if (diff (_ie.expected_rfile, _ie.actual_rfile, _ie.diff_file) == DIFF_FILES_MATCH)   // PASS
				{
					cout << "Files Match\n";
					result = RESULT_PASS;
					result_str = "PASS";
					failureReason = "";
				}
				else                           // FAIL
				{
					cout << "Files Differ\n";
					result = RESULT_FILES_DIFFER;
					result_str = "FILES_DIFFER";
					failureReason = "Expected output and Actual Output differ. Check .diff file.";
				}
			}
		}

		cout << "Result : .............................................................. " << result_str << endl;
	}

	catch (harnessexceptions :: ERROR &e)
	{
		throw;
	}
	return result;
}

int SciDBTCExecutor :: validateParameters (void)
{
	if (_rc.connectionString.empty ())
		throw ConfigError (FILE_LINE_FUNCTION, ERR_CONFIG_SCIDBCONNECTIONSTRING_EMPTY);

	if (_rc.scidbPort < 1)
		throw ConfigError (FILE_LINE_FUNCTION, ERR_CONFIG_SCIDBPORT_INVALID);

	_rc.testFile = getAbsolutePath (_rc.testFile, false);
	if (_rc.testFile.empty ())
		throw ConfigError (FILE_LINE_FUNCTION, ERR_CONFIG_TESTCASEFILENAME_EMPTY);

	if (!bfs::is_regular (_rc.testFile))
	{
		stringstream ss;
		ss << "Test case file " << _rc.testFile << " either does not exist or is not a regular file.";
		throw SystemError (FILE_LINE_FUNCTION, ss.str());
	}

#if (BOOST_FS_VER==2)
	string file_extension = (bfs::path (_rc.testFile)).extension();
#else
	string file_extension = (bfs::path (_rc.testFile)).extension().string();
#endif
	if (file_extension != DEFAULT_TESTCASE_FILE_EXTENSION)
	{
		stringstream ss;
		ss << "Test name " << _rc.testFile << " must have a " << DEFAULT_TESTCASE_FILE_EXTENSION << " extension.";
		throw SystemError (FILE_LINE_FUNCTION, ss.str());
	}

	if (_rc.sleepTime < 0)
		throw ConfigError (FILE_LINE_FUNCTION, ERR_CONFIG_INVALID_SLEEPVALUE);

	if (_rc.debugLevel < MIN_DEBUG_LEVEL || _rc.debugLevel > MAX_DEBUG_LEVEL)
	{
		stringstream ss;
		ss << "Invalid value specified for option --debug. Valid range is [" << MIN_DEBUG_LEVEL << "-" << MAX_DEBUG_LEVEL << "]";
		throw ConfigError (FILE_LINE_FUNCTION, ss.str());
	}

	return SUCCESS;
}

int SciDBTCExecutor :: parseCommandLine (unsigned int argc, char** argv)
{
	try
	{
		po::options_description desc(
				"Usage: scidbtest [--connect <value>] [--port <value>] [--test-file <value>] "
                "[--sleep <value>] [--debug <value>] [--record] \n"
				);

		desc.add_options()
			("connect",              po::value<string>(), "Host of one of the cluster instances. Default is 'localhost'.")
			("port",                 po::value<int>(),    "Port for connection. Default is 1239.")
			("test-file",            po::value<string>(), "Test Case file path.")
			("sleep",                po::value<int>(),    "Execution is paused after each statement in the test case.")
			("debug",                po::value<int>(),    "Log level can be in the range [0-5]. Level 0 only logs fatal errors while level 5 is most verbose. Default is 3.")
			("record",                                    "Record test case output.")
			("help,h", "View this text.");

		po::variables_map vm;
		po::store (po::parse_command_line (argc, argv, desc), vm);
		po::notify (vm);

		if (!vm.empty ())
		{
			if (vm.count ("help"))
			{
				cout << desc << endl;
				exit (0);
			}

			if (vm.count ("connect"))
				_rc.connectionString = vm["connect"].as<string>();

			if (vm.count ("port"))
				_rc.scidbPort = vm["port"].as<int>();

			if (vm.count ("test-file"))
				_rc.testFile = vm["test-file"].as<string>();

            if (vm.count ("sleep"))
				_rc.sleepTime = vm["sleep"].as<int>();

			if (vm.count ("debug"))
				_rc.debugLevel = vm["debug"].as<int>();

			if (vm.count ("record"))
				_rc.record = true;
		}

		validateParameters ();
	}

	catch (harnessexceptions :: ERROR &e)
	{
		//PRINT_ERROR (e.what ());
		cout << e.what () << endl;
		return FAILURE;
	}

	catch (std::exception &e)
	{
		//PRINT_ERROR (e.what ());
		cout << e.what () << endl;
		return FAILURE;
	}

	return SUCCESS;
}

void SciDBTCExecutor :: initConfDefault (void)
{
	_rc.connectionString   = DEFAULT_SCIDB_CONNECTION;
	_rc.scidbPort          = DEFAULT_SCIDB_PORT;
	_rc.testFile           = "";
	_rc.sleepTime          = 0;
	_rc.debugLevel         = DEFAULT_DEBUGLEVEL;
	_rc.record             = false;
}

} //END namespace scidbtestharness

