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
 * @file defaultexecutor.cpp
 * @author girish_hilage@persistent.co.in
 */

# include <stdio.h>
# include <unistd.h>
# include <sys/types.h>
# include <sys/stat.h>
# include <fcntl.h>
# include <string.h>
# include <errno.h>
# include <iostream>
# include <string>
# include <set>
# include <fstream>
# include <sstream>
# include <log4cxx/patternlayout.h>
# include <log4cxx/consoleappender.h>
# include <log4cxx/fileappender.h>
# include <log4cxx/logger.h>
# include <log4cxx/helpers/exception.h>
# include <log4cxx/ndc.h>
# include <boost/lexical_cast.hpp>
# include <boost/algorithm/string.hpp>
# include <boost/filesystem.hpp>
# include <boost/filesystem/fstream.hpp>
# include <boost/date_time/posix_time/posix_time.hpp>
# include <boost/date_time/posix_time/posix_time_types.hpp>
# include <boost/thread/pthread/condition_variable.hpp>
# include <boost/program_options/options_description.hpp>
# include <boost/program_options/variables_map.hpp>
# include <boost/program_options/parsers.hpp>
# include <boost/scoped_array.hpp>

# include "global.h"
# include "helper.h"
# include "errdb.h"
# include "Exceptions.h"
# include "defaultexecutor.h"

# include "SciDBAPI.h"
# include "array/Array.h"
# include "smgr/io/ArrayWriter.h"

# define TESTCASE_COMMAND_SETUP             "--setup"
# define TESTCASE_COMMAND_TEST              "--test"
# define TESTCASE_COMMAND_CLEANUP           "--cleanup"
# define TESTCASE_COMMAND_ECHO              "--echo"
# define TESTCASE_COMMAND_SHELL             "--shell"
# define TESTCASE_COMMAND_DISABLEQUERY      "--disable-query"
# define TESTCASE_COMMAND_SLEEP             "--sleep"
# define TESTCASE_COMMAND_RECONNECT         "--reconnect" /* for reconnecting to scidb */
# define TESTCASE_COMMAND_STARTQUERYLOGGING "--start-query-logging"
# define TESTCASE_COMMAND_STOPQUERYLOGGING  "--stop-query-logging"
# define TESTCASE_COMMAND_STARTIGNOREWARNINGS "--start-ignore-warnings"
# define TESTCASE_COMMAND_STOPIGNOREWARNINGS "--stop-ignore-warnings"
# define TESTCASE_COMMAND_STARTIGDATA       "--start-igdata"
# define TESTCASE_COMMAND_STOPIGDATA        "--stop-igdata"
# define TESTCASE_COMMAND_STARTTIMER        "--start-timer"
# define TESTCASE_COMMAND_STOPTIMER         "--stop-timer"
# define TESTCASE_COMMAND_SETFORMAT	    "--set-format"
# define TESTCASE_COMMAND_ENDFORMAT	    "--reset-format"
# define TESTCASE_COMMAND_SETPRECISION      "--set-precision"
# define TESTCASE_COMMAND_ERROR             "--error"
# define TESTCASE_COMMAND_JUSTRUN           "--justrun"
# define TESTCASE_COMMAND_IGDATA            "--igdata"  /* ignore data */
# define TESTCASE_COMMAND_AQL               "--aql"
# define TESTCASE_COMMAND_AFL               "--afl"
# define TESTCASE_COMMAND_EXIT              "--exit"

# define LOGGER_TAG_EXECUTOR "EXECUTOR"

# define throw_PARSEERROR(err_msg) \
{\
	stringstream ss1;\
	ss1 << "Error at Line " << line_number << ": " << err_msg;\
	throw ExecutorError (FILE_LINE_FUNCTION, ss1.str ());\
}

enum TESTCASEFILE_SECTIONS
{
SECTION_PRESETUP = 0,
SECTION_SETUP,
SECTION_TEST,
SECTION_CLEANUP
};
using namespace std;
using namespace log4cxx;
using namespace scidbtestharness::Exceptions;
namespace po = boost::program_options;
namespace bfs = boost :: filesystem;
namespace harnessexceptions = scidbtestharness::Exceptions;

namespace scidbtestharness
{
namespace executors
{

/* Prepare command for shell redirection */
void DefaultExecutor :: initializeCommand (string& command)
{
	boost::replace_all(command,"\\","\\\\"); /* prepend \ with \    */
	boost::replace_all(command,"'","\\'");   /* prepend ' with \    */
}

int DefaultExecutor :: exit (void)
{
	return EXIT;
}

string DefaultExecutor :: getErrorCodeFromException (const string &errstr)
{
	size_t index = errstr.find ("Error code:");

	if (index == string::npos)
		throw ExecutorError (FILE_LINE_FUNCTION, "Could not find \"Error code:\" inside the error message. Format of the error message is not valid.");

	index += strlen ("Error code:");

	size_t space = errstr.find (" ", index);

	if (space == string::npos)
		throw ExecutorError (FILE_LINE_FUNCTION, "Format of the error message is not valid.");

	int bytes = space - index;
	char *errcode = new char [bytes+1];

	errstr.copy (errcode, bytes, index);
	errcode[bytes] = 0;

	string codestr (errcode);

	delete errcode;
	return codestr;
}

int DefaultExecutor :: runSciDBquery (const string &queryString, const ErrorCommandOptions *errorInfo)
{
	scidb::QueryResult queryResult;

    string qString = queryString;
    if ((errorInfo) && (errorInfo->_hideQueryString))
    {
        // Igor L.: changed to hide the query string in --error directive to prevent expanded paths from shoing up in .out files.
        qString = "(... omitted ...)";
	}
	try
	{
		assert (!queryString.empty ());
		LOG4CXX_INFO (_logger, "Executing query : <" << queryString << ">");

		if (_queryLogging == true || _ie.log_queries == true)
		{
			_resultfileStream << "SCIDB QUERY : <" << qString << ">" << endl;
		}

		string queryoutput_file = _ie.actual_rfile + ".queryoutput";

		/* connect to database only once and use the same connection
		 * for subsequent queries from this test case (i.e. a .test file) */
		if (_dbconnection == 0)
		{
			_dbconnection = _scidb.connect (_ie.connectionString, _ie.scidbPort);
		}


		_scidb.executeQuery (queryString, _afl, queryResult, _dbconnection);

		/**
		 ** Fetching result array
		 **/
		if (queryResult.selective && _ignoredata_flag == false)
		{
            scidb::ArrayWriter::save (*queryResult.array, queryoutput_file, boost::shared_ptr<scidb::Query>(), _outputFormat);
		}
		else if (queryResult.selective && _ignoredata_flag == true)
		{
			scidb::AttributeID nAttrs = (scidb::AttributeID)queryResult.array->getArrayDesc().getAttributes().size();
			std::vector< boost::shared_ptr<scidb::ConstArrayIterator> >iterators(nAttrs);
			for (scidb::AttributeID i = 0; i < nAttrs; i++)
			{
				iterators[i] = queryResult.array->getConstIterator(i);
			}
			uint64_t totalSize = 0;
			while (!iterators[0]->end())
			{
				for (scidb::AttributeID i = 0; i < nAttrs; i++)
				{
					totalSize += iterators[i]->getChunk().getSize();
					++(*iterators[i]);
				}
			}

			boost::filesystem::ofstream outFileStream;
			outFileStream.open (queryoutput_file);

			if (!outFileStream.good ())
			{
				bfs::remove (queryoutput_file);
				_errStream << "Error while opening of the file " << queryoutput_file;
				if(queryResult.queryID && _dbconnection) {
					_scidb.cancelQuery(queryResult.queryID, _dbconnection);
                                }
				throw SystemError (FILE_LINE_FUNCTION, _errStream.str ());
			}

			outFileStream << "[Query was executed successfully, ignoring data output by this query.]\n";
			outFileStream.close ();
		}
		else
		{
			/* for commands which do not have any data to fetch just write this line in the .out.queryoutput file */
			boost::filesystem::ofstream outFileStream;
			outFileStream.open (queryoutput_file);

			if (!outFileStream.good ())
			{
				bfs::remove (queryoutput_file);
				_errStream << "Error while opening of the file " << queryoutput_file;
				if(queryResult.queryID && _dbconnection) {
					_scidb.cancelQuery(queryResult.queryID, _dbconnection);
                               }
				throw SystemError (FILE_LINE_FUNCTION, _errStream.str ());
			}

			if (_justrun_flag == true && _ignoredata_flag == true)
				outFileStream << "[Query was executed successfully, ignoring data output by this query. It was only intended to just run.]";
			else if (_ignoredata_flag == true)
				outFileStream << "[Query was executed successfully, ignoring data output by this query.]" << endl;
			else if (_justrun_flag == true)
				outFileStream << "[Query was executed successfully. It was only intended to just run.]";
			else
				outFileStream << "Query was executed successfully" << endl;
			outFileStream.close ();
		}

        if (_ignoreWarnings == false && queryResult.hasWarnings())
        {
            _resultfileStream << "Warning(s) detected during query execution: " << endl;
            while (queryResult.hasWarnings())
            {
                _resultfileStream << queryResult.nextWarning().msg() << endl;
            }
            _resultfileStream << endl;
        }

        if(queryResult.queryID && _dbconnection) {
            _scidb.completeQuery(queryResult.queryID, _dbconnection);
	}
		/* appending the output stored in the file ".out.queryoutput" to the ".out" file */
		int fd = Open (queryoutput_file.c_str(), O_RDONLY);

		char buf[BUFSIZ];
		int rbytes=-1;
		int size = sizeof (buf) - 1;
		while ((rbytes = read (fd, buf, size)) > 0)
		{
			buf[rbytes] = 0;
			_resultfileStream << buf;
		}

		if (rbytes == -1)
		{
			char errbuf[BUFSIZ];
			strerror_r (errno, errbuf, sizeof (errbuf));
			_errStream << "Got Error [" << errbuf << "] while reading from the file " << queryoutput_file;
			throw SystemError (FILE_LINE_FUNCTION, _errStream.str ());
		}

		Close (fd);
		bfs::remove (queryoutput_file);

		/* as the output for the query saved by CAPIs do not have and <eol> at the end of the file */
		_resultfileStream << endl;
	}

    catch (harnessexceptions::SystemError &e)
	{
        if (queryResult.queryID && _dbconnection) {
            try {
                _scidb.cancelQuery(queryResult.queryID, _dbconnection);
            } catch (const std::exception& err) {
                LOG4CXX_ERROR(_logger, "Exception: " << err.what()
                              <<" CAUGHT on query cancellation while handling: " << e.what ());
            }
        }
		return FAILURE;
	}

    catch (const scidb::Exception &e)
	{
		LOG4CXX_INFO (_logger, "Exception CAUGHT for SCIDB query...:" << e.what ());

		/*TODO: expected_error_code should be compared with actual error code returned
                 * i.e. expected exception and actual exception received should match.
		 * This case refers to --error <scidb query>
		 * which failed as was expected.
		 */
		if (errorInfo)
		{

			_resultfileStream << "[An error expected at this place for the query \"" << qString << "\".";
			_resultfileStream << " And it failed";

			if (errorInfo->_expected_errorcode != "" ||
                            errorInfo->_expected_errorcode2 != "" ||
                            errorInfo->_expected_errns != "" ||
                            errorInfo->_expected_errshort != int32_t(~0) ||
                            !errorInfo->_expected_errlong.empty())
			{
                                // In case of multiple allowed error codes we will not print the actual value
                                // until we check it. However, not to break the old tests (with a single allowed value)
                                // we will still print it.
                                if (errorInfo->_expected_errlong.size()<2) {

                                    _resultfileStream << " with error code = " << e.getStringifiedErrorId();
                                    if (errorInfo->_expected_errorcode2 != "") {
                                        _resultfileStream << " error code2 = " << e.getErrorId();
                                    }
                                    if (errorInfo->_expected_errns != "") {
                                        _resultfileStream << " error namespace = " << e.getErrorsNamespace();
                                    }
                                    if (errorInfo->_expected_errshort != int32_t(~0)) {
                                        _resultfileStream << " short code = " << e.getShortErrorCode();
                                    }
                                    if (!errorInfo->_expected_errlong.empty()) {
                                        _resultfileStream << " long code = " << e.getLongErrorCode();
                                    }
                                }
				_resultfileStream << ". Expected";

				stringstream expectedTmp;
				if (errorInfo->_expected_errorcode != "")
			    		expectedTmp << " error code = " << errorInfo->_expected_errorcode;
            			if (errorInfo->_expected_errorcode2 != "")
                			expectedTmp << " error code2 = " << errorInfo->_expected_errorcode2;
            			if (errorInfo->_expected_errns != "")
                			expectedTmp << " error namespace = " << errorInfo->_expected_errns;
            			if (errorInfo->_expected_errshort != int32_t(~0))
                			expectedTmp << " short code = " <<  errorInfo->_expected_errshort;
                                if (!errorInfo->_expected_errlong.empty()) {
                                    expectedTmp << " long code = " ;
                                    for (set<int32_t>::const_iterator i=errorInfo->_expected_errlong.begin();
                                         i != errorInfo->_expected_errlong.end(); ) {
                                        expectedTmp << (*i);

                                        ++i;
                                        if (i != errorInfo->_expected_errlong.end()) {
                                            expectedTmp << ",";
                                        }
                                    }
                                }
            			_resultfileStream << expectedTmp.str() << ".]" << endl << endl;

			/* compare actual and expected error codes */
			if (
			    (errorInfo->_expected_errorcode != "" && errorInfo->_expected_errorcode != e.getStringifiedErrorId())
			    || (errorInfo->_expected_errorcode2 != "" && errorInfo->_expected_errorcode2 != e.getErrorId())
			    || (errorInfo->_expected_errns != "" && errorInfo->_expected_errns != e.getErrorsNamespace())
			    || (errorInfo->_expected_errshort != int32_t(~0) && errorInfo->_expected_errshort != e.getShortErrorCode())
			    || (!errorInfo->_expected_errlong.empty() && errorInfo->_expected_errlong.count(e.getLongErrorCode()) < 1)
			    )
			  {
				_errorCodesDiffer = true;
				LOG4CXX_INFO (_logger, "Actual Error code (" << e.getErrorId() << " aka "
				    << e.getStringifiedErrorId() << ") does not match with the Expected Error code ("
				    << expectedTmp.str() << "). Hence this is a test case failure...");
				if(queryResult.queryID && _dbconnection) {
				    _scidb.cancelQuery(queryResult.queryID, _dbconnection);
                                }
//				throw ExecutorError (FILE_LINE_FUNCTION, "SciDB query execution failed");
				return SUCCESS;
// We should now return SUCCESS because we dont want the test to throw EXECUTOR_ERROR and jump to cleanup section if the actual error code does not match with the expected error code.
			  }
			}
			else {
                            _resultfileStream << ".]" << endl << endl;
                        }
			LOG4CXX_INFO (_logger, "This was an expected Exception. Hence continuing...");
			if(queryResult.queryID && _dbconnection) {
                            try {
                                _scidb.cancelQuery(queryResult.queryID, _dbconnection);
                            } catch (const std::exception& e) {
                                LOG4CXX_ERROR(_logger, "Exception CAUGHT for cancel query...:" << e.what ());
                            }
                        }
			return SUCCESS;
		}

		if (_justrun_flag == true)
		{
			_resultfileStream << "[SciDB query execution failed. But continuing, as it was intended to just run.]";
			_resultfileStream << endl << endl;
			if(queryResult.queryID && _dbconnection) {
                            _scidb.cancelQuery(queryResult.queryID, _dbconnection);
                        }
			return SUCCESS;
		}

		if(queryResult.queryID && _dbconnection) {
                    _scidb.cancelQuery(queryResult.queryID, _dbconnection);
                }
		/* for all other queries not mentioned under --error */
		throw ExecutorError (FILE_LINE_FUNCTION, "SciDB query execution failed");
	}

    catch (const std::exception &e)
    {
        LOG4CXX_INFO (_logger, "Unhandled exception CAUGHT for SCIDB query...:" << e.what());
        _resultfileStream << "[SciDB query execution failed with unhandled exception:" << e.what() << "]" << endl << endl;
        if(queryResult.queryID && _dbconnection) {
            try {
                _scidb.cancelQuery(queryResult.queryID, _dbconnection);
            } catch (const std::exception& err) {
                 LOG4CXX_ERROR(_logger, "Exception: " << err.what()
                              <<" CAUGHT on query cancellation while handling: " << e.what ());
            }
        }
        throw ExecutorError(FILE_LINE_FUNCTION, "SciDB query execution failed with unhandled exception");
    }
	/* This case refers to --error <scidb query>
	 * but which executed successfully without throwing any exception where as it was supposed to fail
	 */
	if (errorInfo)
	{
		_resultfileStream << "[An error was expected at this place for the query \"" << qString
                          << "\". But actually query got executed successfully.]";
		_resultfileStream << LINE_FEED;
		_resultfileStream << LINE_FEED;
		LOG4CXX_INFO (_logger, "An Exception was expected, but actually query got executed successfully...Hence this is a test case failure...");

		return FAILURE;
	}

	return SUCCESS;
}

void DefaultExecutor :: disconnectFromSciDB (void) {
    //check if a connection has already been established .. if so
    //close it. Connection is established if not connected in runSciDBQuery
    if (_dbconnection)
    {
            _scidb.disconnect (_dbconnection);
            _dbconnection = 0;
    }
}

int DefaultExecutor :: stopTimer (const string &args)
{
	LOG4CXX_INFO (_logger, "Stoping the timer...");

	/* stop counting the time and write to timer file */
	long totalTime = (boost::posix_time::microsec_clock::local_time() - _timerStarttime).total_milliseconds();
	_timerfileStream << args << " : " << totalTime << " ms" << endl;
	_timerEnabled = false;

	return SUCCESS;
}

int DefaultExecutor :: startTimer (const string &args)
{
	LOG4CXX_INFO (_logger, "Starting a timer..." << args);

	_timerEnabled = true;
	if (!_timerfileOpened)
	{
		_timerfileStream.open (_ie.timerfile.c_str(), std::ios::trunc | std::ios::binary);
		if (!_timerfileStream.good ())
		{
			_errStream << "Error while opening of the file " << _ie.timerfile;
			throw SystemError (FILE_LINE_FUNCTION, _errStream.str ());
		}
		_timerfileOpened = true;
	}

	/* start counting the time */
	_timerStarttime = boost::posix_time::microsec_clock::local_time();

	return SUCCESS;
}

int DefaultExecutor :: endOutputFormat (void)
{
	_outputFormat = "dcsv";
	LOG4CXX_INFO (_logger, "Reset Query Output Format to 'dcsv'");
	return SUCCESS;
}

int DefaultExecutor :: setOutputFormat(const string &args)
{
	LOG4CXX_INFO (_logger, "Setting Query Output Format to " << args);
        if (scidb::ArrayWriter::isSupportedFormat(args)
            || strcasecmp("auto", args.c_str()) == 0)
        {
                _outputFormat = args;
        }
        else
        {
                LOG4CXX_INFO (_logger, "Invalid Format: " << args
                              << ". Switching back to dcsv format.");
                _outputFormat = "dcsv";
        }
	return SUCCESS;
}

int DefaultExecutor :: setPrecision(const string &args)
{
	LOG4CXX_INFO (_logger, "Setting Precision to " << args);
	try
	{
		if(_precisionSet == false)
                    _precisionDefaultValue = scidb::ArrayWriter::getPrecision();
		int temp = boost::lexical_cast<int>(args);
		scidb::ArrayWriter::setPrecision(temp);
		_precisionSet = true;
	}
	catch(...)
	{
		LOG4CXX_INFO (_logger, "Invalid Precision Value: " << args << ". Re-setting back to default.");
		_precisionSet = false;
		scidb::ArrayWriter::setPrecision(_precisionDefaultValue);
	}
	return SUCCESS;
}

int DefaultExecutor :: Shell (ShellCommandOptions *sco)
{
	LOG4CXX_INFO (_logger, "Executing the shell command : " << sco->_command);
	if (strcasecmp(sco->_cwd.c_str(),"") != 0)
	{
		if (sco->_cwd.find("$BASE_PATH") != string::npos)
		{
			string new_command;
			new_command = "iquery -p ";
			new_command.append(boost::lexical_cast<string>(_ie.scidbPort));
			new_command.append(" -aq \"project(list('instances'),instance_path)\"");
			stringstream bstream;
			int rbytes, exit_code=FAILURE;
		        char buf[BUFSIZ+1];
		        FILE *mystream = 0;
		        while ((rbytes = ReadOutputOf (new_command, &mystream, buf, BUFSIZ, &exit_code)) > 0)
		        {
		                buf[rbytes] = 0;
	                        bstream << buf;
			}
			string server_base_path;
			server_base_path = bstream.str();
			boost::trim(server_base_path);
			server_base_path = server_base_path.substr(3,server_base_path.find("\")")-8);
			LOG4CXX_INFO (_logger, "Server-base-path detected: '" << server_base_path << "'.");
			boost::replace_first(sco->_cwd,"$BASE_PATH",server_base_path);
		}

		LOG4CXX_INFO (_logger, "Working directory specified: '"
<< sco->_cwd << "'.");
		boost::replace_all(sco->_cwd,"\"","");
	}

	boost::filesystem::ofstream _outputfileStream;
	if (!sco->_outputFile.empty ())
	{
		LOG4CXX_INFO (_logger, "Storing the output in the file : " << sco->_outputFile);

		_outputfileStream.open (sco->_outputFile, std::ios::trunc | std::ios::binary);
		if (!_outputfileStream.good ())
		{
			_errStream << "Error while opening of the file " << sco->_outputFile;
			throw SystemError (FILE_LINE_FUNCTION, _errStream.str ());
		}
	}

	if (sco->_store == true)
		LOG4CXX_INFO (_logger, "Storing the output in the .out file.");

	if (_queryLogging == true || _ie.log_queries == true)
	{
		/* store in the file specified in the --out option */
		if (!sco->_outputFile.empty ())
			_outputfileStream << "SCIDB QUERY : <" <<sco->_command << ">" << "\n";

		/* store in the .out file along with the results of different SciDB queries */
		if (sco->_store == true)
			_resultfileStream << "SCIDB QUERY : <" <<sco->_command << ">" << "\n";
	}

	/* Change Current Working Directory if it is specified */
	if (strcasecmp(sco->_cwd.c_str(),"") != 0)
	{
		sco->_cwd = getAbsolutePath(sco->_cwd.c_str());
		if (boost::filesystem::is_directory(sco->_cwd))
		{
                        sco->_command = "cd " + sco->_cwd + " && " + sco->_command;
                        LOG4CXX_INFO (_logger, "Changing working directory to : '" << sco->_cwd << "'.");
                }
                else
                        LOG4CXX_INFO (_logger, "Invalid working directory specified : '" << sco->_cwd << "'. Reverting to current working directory.");
	}

	/* execute the shell command */
	int rbytes, exit_code=FAILURE, exit_code_err=FAILURE;
	char buf[BUFSIZ+1];
	FILE *mystream = 0, *mystream2 = 0;
	stringstream execShellCommand, getShellErr;

	char tmpFileName[] = "/tmp/harness_XXXXXX"; // temporary filename
	int tfd = mkstemp(tmpFileName);            // create temporary file
	while (tfd == -1)
	{
		if (errno == EEXIST)        // try another name if failed to open unique temporary file
		{
			strncpy (tmpFileName, "/tmp/Harness_XXXXXX", 20);
			tfd = mkstemp(tmpFileName);
		}
		else
		{
			close (tfd);
			bfs::remove (tmpFileName);
			LOG4CXX_ERROR (_logger, "Error creating temporary file: " << tmpFileName);
			return FAILURE;
		}
	}
	close (tfd);                               // close file so that it can be used below

	initializeCommand (sco->_command);
	execShellCommand << "/bin/sh -c $'" << sco->_command << "' 2> " << tmpFileName;  // wrap shell command to redirect stderr into the temporary file
	getShellErr << "cat " << tmpFileName;
	LOG4CXX_DEBUG (_logger, "Actual shell command executed: \"" << execShellCommand.str() << "\"");

	while ((rbytes = ReadOutputOf (execShellCommand.str(), &mystream, buf, BUFSIZ, &exit_code)) > 0)
	{
		buf[rbytes] = 0;

		/* store in the file specified in the --out option */
		if (!sco->_outputFile.empty ())
			_outputfileStream << buf;

		/* store in the .out file along with the results of different SciDB queries */
		if (sco->_store == true)
			_resultfileStream << buf;

		/* if none of --out, --store is given then log the output to .log file */
		if (!sco->_outputFile.empty() == 0 && sco->_store == false)
			LOG4CXX_INFO (_logger, buf);
	}

	/* display stderr of the above executed shell command from the temporary file */
	if ( bfs::file_size (tmpFileName) > 0 )
	{
		LOG4CXX_INFO (_logger, "Getting stderr from " << getShellErr.str());
		while ((rbytes = ReadOutputOf (getShellErr.str(), &mystream2, buf, BUFSIZ, &exit_code_err)) > 0)
		{
			buf[rbytes] = 0;

			/* display on stderr */
			cerr << buf;

			/* save the output to .log file */
			LOG4CXX_INFO (_logger, buf);

	                /* store in the file specified in the --out option */
			if (!sco->_outputFile.empty ())
				_outputfileStream << buf;

			/* store in the .out file along with the results of different SciDB queries */
			if (sco->_storeAll == true)
				_resultfileStream << buf;
		}
	}
	bfs::remove (tmpFileName); // cleanup temporary file


    /* store in the file specified in the --out option */
    if (!sco->_outputFile.empty ())
        _outputfileStream << "\n";

    /* store in the .out file along with the results of different SciDB queries */
    if (sco->_store == true)
        _resultfileStream << "\n";

	if (!sco->_outputFile.empty ())
	{
        _outputfileStream.close ();
    }
	LOG4CXX_INFO (_logger, "Shell command exited with Exit code = " << exit_code << ".");
	if (exit_code == 0)
	{
	return SUCCESS;
    }
	LOG4CXX_INFO (_logger, "Hence this is a test case failure...");
	return FAILURE;
}

int DefaultExecutor :: Echo (const string &args)
{
	_resultfileStream << args << endl;
	return SUCCESS;
}

int DefaultExecutor :: execCommand (const string &cmd, const string &args, void *extraInfo)
{
	if (_ie.sleepTime > 0)
		sleep (_ie.sleepTime);

	if (cmd == TESTCASE_COMMAND_ECHO)
	{
		Echo (args);
	} else if (cmd == TESTCASE_COMMAND_SHELL)
	{
		int status;
		if ((status = Shell ((ShellCommandOptions *) extraInfo)) == FAILURE)
			return FAILURE;
	} else if (cmd == TESTCASE_COMMAND_AQL)
	{
		_afl = 0;
		if (runSciDBquery (args) == FAILURE)
			return FAILURE;
	} else if (cmd == TESTCASE_COMMAND_SLEEP)
	{
		long int seconds = sToi (args);
		sleep (seconds);
	} else if (cmd ==  TESTCASE_COMMAND_RECONNECT)
        {
                LOG4CXX_INFO (_logger, "Reconnecting to SciDB");
                //We only disconnect on --reconnect command
                //connection is established in runSciDBQuery if connection object
                //does not exist
                disconnectFromSciDB();
        }
        else if (cmd == TESTCASE_COMMAND_STARTQUERYLOGGING)
	{
		LOG4CXX_INFO (_logger, "Starting query logging...");
		_queryLogging = true;
	} else if (cmd == TESTCASE_COMMAND_STOPQUERYLOGGING)
	{
		LOG4CXX_INFO (_logger, "Stoping query logging...");
		_queryLogging = false;

        } else if (cmd == TESTCASE_COMMAND_STARTIGNOREWARNINGS)
        {
                LOG4CXX_INFO (_logger, "Starting to ignore warnings...");
                _ignoreWarnings = true;
        } else if (cmd == TESTCASE_COMMAND_STOPIGNOREWARNINGS)
        {
                LOG4CXX_INFO (_logger, "Stopping to ignore warnings...");
                _ignoreWarnings = false;

	} else if (cmd == TESTCASE_COMMAND_STARTIGDATA)
	{
		LOG4CXX_INFO (_logger, "Starting Ignoring the data output by query ...");
		_ignoredata_flag = true;
	} else if (cmd == TESTCASE_COMMAND_STOPIGDATA)
	{
		LOG4CXX_INFO (_logger, "Stoping Ignoring the data output by query ...");
		_ignoredata_flag = false;
	} else if (cmd == TESTCASE_COMMAND_STARTTIMER)
	{
		startTimer (args);
	} else if (cmd == TESTCASE_COMMAND_SETFORMAT)
	{
		setOutputFormat (args);
	} else if (cmd == TESTCASE_COMMAND_ENDFORMAT)
	{
		endOutputFormat ();
	} else if (cmd == TESTCASE_COMMAND_SETPRECISION)
	{
		setPrecision (args);
	} else if (cmd == TESTCASE_COMMAND_STOPTIMER)
	{
		stopTimer (args);
	} else if (cmd == TESTCASE_COMMAND_ERROR)
	{
		_afl = ((ErrorCommandOptions *)extraInfo)->_afl;

		bool inside_start_igdata = false;

		/* respect its own --igdata flag only if it is not inside --start-igdata */
		if (_ignoredata_flag == false)
			_ignoredata_flag = ((ErrorCommandOptions *)extraInfo)->_igdata;
		else
			inside_start_igdata = true;

		if (runSciDBquery (((ErrorCommandOptions *)extraInfo)->_query,
						   ((ErrorCommandOptions *)extraInfo)) == FAILURE)
		{
			if (inside_start_igdata == false)
				_ignoredata_flag = false;

			return FAILURE;
		}
		if (inside_start_igdata == false)
			_ignoredata_flag = false;

	} else if (cmd == TESTCASE_COMMAND_JUSTRUN)
	{
		_justrun_flag = true;
		_afl = ((JustRunCommandOptions *)extraInfo)->_afl;

		bool inside_start_igdata = false;

		/* respect its own --igdata flag only if it is not inside --start-igdata */
		if (_ignoredata_flag == false)
			_ignoredata_flag = ((JustRunCommandOptions *)extraInfo)->_igdata;
		else
			inside_start_igdata = true;

		if (runSciDBquery (((JustRunCommandOptions *)extraInfo)->_query) == FAILURE)
		{
			if (inside_start_igdata == false)
				_ignoredata_flag = false;

			_justrun_flag = false;
			return FAILURE;
		}
		if (inside_start_igdata == false)
			_ignoredata_flag = false;

		_justrun_flag = false;

	} else if (cmd == TESTCASE_COMMAND_IGDATA)
	{
		_afl = ((IgnoreDataOptions *)extraInfo)->_afl;

		bool inside_start_igdata = false;

		/* respect this command only if it is not inside --start-igdata */
		if (_ignoredata_flag == false)
			_ignoredata_flag = true;
		else
			inside_start_igdata = true;

		if (runSciDBquery (((IgnoreDataOptions *)extraInfo)->_query) == FAILURE)
		{
			if (inside_start_igdata == false)
				_ignoredata_flag = false;

			return FAILURE;
		}
		if (inside_start_igdata == false)
			_ignoredata_flag = false;

	} else if (cmd == TESTCASE_COMMAND_AFL)
	{
		_afl = 1;
		if (runSciDBquery (args) == FAILURE)
			return FAILURE;
	} else if (cmd == TESTCASE_COMMAND_EXIT)
	{
		return exit ();
	}

	return SUCCESS;
}

/* returns SUCCESS, FAILURE, EXIT */
int DefaultExecutor :: execCommandsection (const Command &section)
{
	LOG4CXX_INFO (_logger, "Executing Command " << section.cmd); /* << " with no. of subcommands = " << section.subcommands.size ()); */

	/* sections --setup, --test, --cleanup */
	if ((section.cmd == TESTCASE_COMMAND_SETUP) ||
	    (section.cmd == TESTCASE_COMMAND_TEST)  ||
	    (section.cmd == TESTCASE_COMMAND_CLEANUP))
	{
		boost::posix_time::ptime section_starttime = boost::posix_time::microsec_clock::local_time();

		/* if these sections have any subcommands then execute them */
		if (section.subCommands.size () > 0)
		{
			bool cleanup_failure=false;
			for (vector<Command> :: const_iterator it = section.subCommands.begin (); it != section.subCommands.end (); ++it)
			{
				try
				{
					int rv = execCommandsection (*it);
					if (section.cmd == TESTCASE_COMMAND_CLEANUP && rv == FAILURE)
						cleanup_failure = true;
					else if (rv == EXIT || rv == FAILURE)
						return rv;
				}
				catch (harnessexceptions :: ERROR &e)
				{
				// Catch Exceptions from CLEANUP section so that it does not exit after a command under CLEANUP section fails
					if (section.cmd != TESTCASE_COMMAND_CLEANUP)
						throw;
					PRINT_ERROR (e.what ());
					cleanup_failure = true;
				}
			}
			if (cleanup_failure)
				return FAILURE;
		}

		if (section.cmd == TESTCASE_COMMAND_SETUP)
			_caseexecTime.setupTime = (boost::posix_time::microsec_clock::local_time() - section_starttime).total_milliseconds();
		if (section.cmd == TESTCASE_COMMAND_TEST)
			_caseexecTime.testTime = (boost::posix_time::microsec_clock::local_time() - section_starttime).total_milliseconds();
		if (section.cmd == TESTCASE_COMMAND_CLEANUP)
			_caseexecTime.cleanupTime = (boost::posix_time::microsec_clock::local_time() - section_starttime).total_milliseconds();

	/* section --echo */
	} else if (section.cmd == TESTCASE_COMMAND_ECHO)
	{
		assert (section.subCommands.size () == 0);
		if (execCommand (section.cmd, section.args) == FAILURE)
			return FAILURE;

	/* section --shell */
	} else if (section.cmd == TESTCASE_COMMAND_SHELL)
	{
		assert (section.subCommands.size () == 0);
		if (execCommand (section.cmd, section.args, section.extraInfo) == FAILURE)
			return FAILURE;

	/* section --aql */
	} else if (section.cmd == TESTCASE_COMMAND_AQL)
	{
		assert (section.subCommands.size () == 0);
		if (execCommand (section.cmd, section.args) == FAILURE)
			return FAILURE;

	/* section --sleep */
	} else if (section.cmd == TESTCASE_COMMAND_SLEEP)
	{
		assert (section.subCommands.size () == 0);
		if (execCommand (section.cmd, section.args) == FAILURE)
			return FAILURE;
        /* section --reconnect */
        } else if(section.cmd == TESTCASE_COMMAND_RECONNECT)
        {
        	assert (section.subCommands.size () == 0);
		if (execCommand (section.cmd, section.args) == FAILURE)
			return FAILURE;
	/* section --start-query-logging */
	} else if (section.cmd == TESTCASE_COMMAND_STARTQUERYLOGGING)
	{
		assert (section.subCommands.size () == 0);
		if (execCommand (section.cmd, section.args) == FAILURE)
			return FAILURE;

	/* section --stop-query-logging */
	} else if (section.cmd == TESTCASE_COMMAND_STOPQUERYLOGGING)
	{
		assert (section.subCommands.size () == 0);
		if (execCommand (section.cmd, section.args) == FAILURE)
			return FAILURE;

	/* section --start-igdata */
	} else if (section.cmd == TESTCASE_COMMAND_STARTIGDATA)
	{
		assert (section.subCommands.size () == 0);
		if (execCommand (section.cmd, section.args) == FAILURE)
			return FAILURE;

	/* section --stop-igdata */
	} else if (section.cmd == TESTCASE_COMMAND_STOPIGDATA)
	{
		assert (section.subCommands.size () == 0);
		if (execCommand (section.cmd, section.args) == FAILURE)
			return FAILURE;

        /* section --start-ignore-warnings */
        } else if (section.cmd == TESTCASE_COMMAND_STARTIGNOREWARNINGS)
        {
                assert (section.subCommands.size () == 0);
                if (execCommand (section.cmd, section.args) == FAILURE)
                        return FAILURE;

        /* section --stop-ignore-warnings */
        } else if (section.cmd == TESTCASE_COMMAND_STOPIGNOREWARNINGS)
        {
                assert (section.subCommands.size () == 0);
                if (execCommand (section.cmd, section.args) == FAILURE)
                        return FAILURE;

	/* section --start-timer */
	} else if (section.cmd == TESTCASE_COMMAND_STARTTIMER)
	{
		assert (section.subCommands.size () == 0);
		if (execCommand (section.cmd, section.args) == FAILURE)
			return FAILURE;

	/* section --stop-timer */
	} else if (section.cmd == TESTCASE_COMMAND_STOPTIMER)
	{
		assert (section.subCommands.size () == 0);
		if (execCommand (section.cmd, section.args) == FAILURE)
			return FAILURE;

	/* section --set-format */
	} else if (section.cmd == TESTCASE_COMMAND_SETFORMAT)
	{
		assert (section.subCommands.size () == 0);
		if (execCommand (section.cmd, section.args) == FAILURE)
			return FAILURE;

	/* section --reset-format */
	} else if (section.cmd == TESTCASE_COMMAND_ENDFORMAT)
	{
		assert (section.subCommands.size () == 0);
		if (execCommand (section.cmd, section.args) == FAILURE)
			return FAILURE;

	/* section --set-precision */
	} else if (section.cmd == TESTCASE_COMMAND_SETPRECISION)
	{
		assert (section.subCommands.size () == 0);
		if (execCommand (section.cmd, section.args) == FAILURE)
			return FAILURE;

	/* section --error */
	} else if (section.cmd == TESTCASE_COMMAND_ERROR)
	{
		assert (section.subCommands.size () == 0);
		if (execCommand (section.cmd, section.args, section.extraInfo) == FAILURE)
			return FAILURE;

	/* section --justrun */
	} else if (section.cmd == TESTCASE_COMMAND_JUSTRUN)
	{
		assert (section.subCommands.size () == 0);
		if (execCommand (section.cmd, section.args, section.extraInfo) == FAILURE)
			return FAILURE;

	/* section --igdata */
	} else if (section.cmd == TESTCASE_COMMAND_IGDATA)
	{
		assert (section.subCommands.size () == 0);
		if (execCommand (section.cmd, section.args, section.extraInfo) == FAILURE)
			return FAILURE;

	/* section --scidbquery */
	} else if (section.cmd == TESTCASE_COMMAND_AFL)
	{
		assert (section.subCommands.size () == 0);
		if (execCommand (section.cmd, section.args) == FAILURE)
			return FAILURE;

	/* section --exit */
	} else if (section.cmd == TESTCASE_COMMAND_EXIT)
	{
		assert (section.subCommands.size () == 0);
		return execCommand (section.cmd, section.args);
	} else
	{
		assert (0);
	}

	return SUCCESS;
}

int DefaultExecutor :: execCommandVector (const vector<Command> &commandvector)
{
	assert (commandvector.size ());

	int rv=FAILURE;
	vector<Command> :: const_iterator it;

	for (it = commandvector.begin (); it != commandvector.end (); ++it)
	{
		rv = execCommandsection (*it);
		if (rv == EXIT || rv == FAILURE)
			return rv;
	}

	return rv;
}

/* test case in the sense; whole test case file
 * i.e. all the commands in the test case file.
 */
int DefaultExecutor :: executeTestCase (void)
{
	LOG4CXX_INFO (_logger, "Starting executing the test case ...");
	int i=-1, rv=FAILURE;
	_ie.startTestSectionMillisec=0;
	_ie.endTestSectionMillisec=0;
	boost::posix_time::ptime time_of_epoch(boost::gregorian::date(1970,1,1));

	try
	{
		/* open .out file */
		_resultfileStream.open (_ie.actual_rfile.c_str(), std::ios::trunc | std::ios::binary);
		if (!_resultfileStream.good ())
		{
			_errStream << "Error while opening of the file " << _ie.actual_rfile;
			throw SystemError (FILE_LINE_FUNCTION, _errStream.str ());
		}

		int stored_rv=-2;
		string section_name;
		vector<Command> *current_section=0;
		for (i=SECTION_PRESETUP; i<=SECTION_CLEANUP; )
		{
			switch (i)
			{
				case SECTION_PRESETUP  : current_section = &_preSetupCommands; section_name="[preSetup] commands"; break;
				case SECTION_SETUP     : current_section = &_setupCommands; section_name="[SETUP] section"; break;
				case SECTION_TEST      : current_section = &_testCommands; section_name="[TEST] section"; break;
				case SECTION_CLEANUP   : current_section = &_cleanupCommands; section_name="[CLEANUP] section"; break;
			}

			if (current_section->size () == 0)
			{
				LOG4CXX_INFO (_logger, "No " << section_name << " found.");
			}
			else
			{
				LOG4CXX_INFO (_logger, "Executing " << section_name << ".");
				if (i == SECTION_TEST)
				{
					_ie.startTestSectionMillisec = (boost::posix_time::microsec_clock::local_time()-time_of_epoch).total_milliseconds();
				}

				rv = execCommandVector (*current_section);

				if (i == SECTION_TEST)
				{
					_ie.endTestSectionMillisec = (boost::posix_time::microsec_clock::local_time()-time_of_epoch).total_milliseconds();
				}

				if (rv == EXIT || rv == FAILURE)
				{
					if (rv == EXIT)
					{
						rv = SUCCESS;
					}
					else
					{
						rv = FAILURE;

						/* skip to --cleanup only if it is already not a --cleanup */
						if (i<SECTION_CLEANUP)
						{
							i = SECTION_CLEANUP;
							stored_rv = rv;
							continue;
						}
					}

					_resultfileStream.close();
					_timerfileStream.close();
					if (stored_rv != -2)
						rv = stored_rv;

					return rv;
				}

				if (stored_rv != -2)
					rv = stored_rv;
			}
			i++;
		}
	}

	catch (harnessexceptions :: ERROR &e)
	{
		/* in case of any exception forget previous context */
		_ignoredata_flag = false;
		_justrun_flag = false;

		PRINT_ERROR (e.what ());

		if (i==SECTION_CLEANUP)
			LOG4CXX_WARN (_logger, "An exception occurred inside the [CLEANUP] section.");

		try
		{
			/* do not again execute --cleanup as the exception is thrown from --cleanup section itself */
			if (i>=SECTION_PRESETUP && i<SECTION_CLEANUP)
			{
				if (_cleanupCommands.size())
				{
					LOG4CXX_WARN (_logger, "Executing [CLEANUP] section.");
					execCommandVector (_cleanupCommands);
				}
				else
					LOG4CXX_WARN (_logger, "No [CLEANUP] section found.");
			}
			else
				LOG4CXX_WARN (_logger, "Not Executing [CLEANUP] section again.");
		}

		catch (harnessexceptions :: ERROR &e)
		{
			LOG4CXX_WARN (_logger, "An exception occurred inside the [CLEANUP] section.");
		}

		_resultfileStream.close();
		_timerfileStream.close();
		return FAILURE;
	}

	_resultfileStream.close();
	_timerfileStream.close();

	LOG4CXX_INFO (_logger, "Done executing the test case ...");
	return rv;
}

void DefaultExecutor :: printParsedVector (vector<Command> vc, const string &indentation)
{
	vector<Command> :: iterator it;
	for (it = vc.begin (); it != vc.end (); ++it)
	{
		LOG4CXX_INFO (_logger, "" << indentation << it->cmd << " |" << it->args << "|");
		if (it->subCommands.size () > 0)
		{
			printParsedVector (it->subCommands, indentation + "\t");
		}
	}
}

void DefaultExecutor :: printParsedSections (void)
{
	printParsedVector (_preSetupCommands);
	printParsedVector (_setupCommands);
	printParsedVector (_testCommands);
	printParsedVector (_cleanupCommands);
}

int DefaultExecutor :: parseShellCommandOptions (string line, struct ShellCommandOptions *sco)
{
	int argc=0;
	char **argv=0;

	try
	{
		/* first parse and remove --command option */
		size_t index = line.find (" --command ");
		size_t index1 = line.find (" --command=");

		if (index != string::npos || index1 != string::npos)
		{
			size_t starting_index=-1;
			if (index != string::npos)
			{
				starting_index = index;
				index += strlen (" --command ");
			}

			if (index1 != string::npos)
			{
				starting_index = index1;
				index = index1 + strlen (" --command=");
			}

			if (index >= line.length ())
				throw (string ("Please specify the command within unescaped double quotes (\"\")."));

            //---------------------------------------------------------
            string actualShellCommand;
            string lineRemainder;

            size_t first_quote_pos = line.find_first_of("\"",index);

            size_t last_quote_pos = line.find_last_of("\"");

            if  (last_quote_pos == first_quote_pos)
            {
                throw (string ("Please specify the command within unescaped double quotes (\"\")."));
            }

            actualShellCommand = line.substr( // Hack off the shell command.
                first_quote_pos+1,
                last_quote_pos-first_quote_pos-1
                );
            boost::trim(actualShellCommand);

            lineRemainder = line.substr( // Check if there is anything else on the line after --command " " option.
                last_quote_pos+1
                );
            boost::trim(lineRemainder);


            if (!lineRemainder.empty())
            {
                throw (string ("No other options are allowed after --command\"...\" option!"));
            }

            sco->_command = actualShellCommand;

            line = line.substr(0,starting_index); // Remove the command option from the line.
            //---------------------------------------------------------

			index = line.find ("--command");
			if (index != string::npos)
				throw (string ("Please specify --command only once."));
		}
		else
			throw (string ("Please specify the shell command to be executed."));

		{
			vector<string> tokens;
			tokenize (line, tokens, " ");

			argc = tokens.size ();
			argv = new char * [argc];
			for (int i=0; i<argc; i++)
			{
				argv[i] = new char [tokens[i].length() + 1];
				snprintf (argv[i], tokens[i].length() + 1, "%s", tokens[i].c_str());
			}
		}

		po::options_description desc("Usage: --shell [--command <shell command to be executed>] [--out <file name>] [--store]");
		desc.add_options() ("command", po::value<string>(), "shell command to be executed.");
		desc.add_options() ("out", po::value<string>(), "File name to redirect the output of the shell command.");
		desc.add_options() ("store",                    "Flag to indicate if the output of the shell command should be stored inside the .expected/.out file along with the output of SciDB queries.");
		desc.add_options() ("store-all",                "Flag to indicate if the stderr of the shell command should be stored inside the .expected/.out file along with the output of SciDB queries.");
		desc.add_options() ("cwd", po::value<string>(), "Working directory for the shell command.");
		po::variables_map vm;
		po::store (po::parse_command_line (argc, argv, desc), vm);
		po::notify (vm);

		if (!vm.empty ())
		{
			if (vm.count ("out"))
				sco->_outputFile = vm["out"].as<string>();

			if (vm.count ("store"))
				sco->_store = true;

			if (vm.count ("store-all"))
			{
				sco->_storeAll = true;
				sco->_store = true;   // 'store-all' option includes 'store' option implicitly
			}

			if (vm.count ("cwd"))
				sco->_cwd = vm["cwd"].as<string>();
		}

		for (int i=0; i<argc; i++)
			delete argv[i];
		delete []argv;
	}

    catch (const std::exception& e)
	{
		for (int i=0; i<argc; i++)
			delete argv[i];
		delete []argv;

		throw (string (e.what ()));
	}

	return SUCCESS;
}

int DefaultExecutor :: parseIgnoreDataOptions (string line, struct IgnoreDataOptions *ido)
{
	int argc=0;
	char **argv=0;

	try
	{
		size_t starting_dq = line.find ("\"");
		if (starting_dq == string::npos || line[starting_dq-1] == '\\')
			throw (string ("Please specify the command within unescaped double quotes (\"\")."));

		/* if none of -aql, -afl is found then insert " --afl=" */
		size_t aql = line.find ("-aql");
		if (aql == string::npos)
		{
			aql = line.find ("-aql");
			if (aql == string::npos)
			{
				size_t afl = line.find ("-afl");
				if (afl == string::npos)
				{
					afl = line.find ("-afl");
					if (afl == string::npos)
					{
						/* it must be an AFL query. So insert " --afl=" */
						char *part1 = new char [starting_dq+1];
						line.copy (part1, starting_dq);
						part1[starting_dq]=0;

						int bytes = line.size() - starting_dq;
						char *part2 = new char [bytes + 1];
						bytes = line.copy (part2, bytes, starting_dq);
						part2[bytes]=0;

						string newLine = part1;
						newLine = newLine + " --afl=" + part2;
						line = newLine;

						delete part1;
						delete part2;
					}
				}
			}
		}

		/*
         * parse remaining command line
         */
		{
			vector<string> tokens;
			if (tokenize_commandline (line, tokens) == FAILURE)
			{
				string error="Problem while separating the tokens of the commandline [";
				error = error + line + "]";
				throw (error);
			}
			else
			{
				LOG4CXX_INFO (_logger, "Arguments for command [" << line << "] successfully separated.");
			}

			argc = tokens.size ();
			argv = new char * [argc];
			for (int i=0; i<argc; i++)
			{
				argv[i] = new char [tokens[i].length() + 1];
				snprintf (argv[i], tokens[i].length() + 1, "%s", tokens[i].c_str());
			}
		}

		po::options_description desc(
				"Usage: --igdata [--aql \"<AQL query to be executed>\"]|[--afl \"<AFL query to be executed>\"]\n"
				);

		desc.add_options()
		    ("aql",  po::value<string>(), "AQL query to be executed.")
			("afl",  po::value<string>(), "AFL query to be executed.");

		po::variables_map vm;
		po::store (po::parse_command_line (argc, argv, desc), vm);
		po::notify (vm);

		int aql_given = 0;
		int afl_given = 0;
		if (!vm.empty ())
		{
			if (vm.count ("aql"))
			{
				if (afl_given)
					throw (string ("Only one of --aql or --afl can be given."));
				ido->_query = vm["aql"].as<string>();
				aql_given = 1;
				ido->_afl = false;
			}

			if (vm.count ("afl"))
			{
				if (aql_given)
					throw (string ("Only one of --aql or --afl can be given."));
				ido->_query = vm["afl"].as<string>();
				afl_given = 1;
				ido->_afl = true;
			}
		}

		if (ido->_query.empty())
			throw (string ("Please specify scidb query to be executed."));

		for (int i=0; i<argc; i++)
			delete argv[i];
		delete []argv;
	}

    catch (const std::exception& e)
	{
		for (int i=0; i<argc; i++)
			delete argv[i];
		delete []argv;

		throw (string (e.what ()));
	}

	return SUCCESS;
}

int DefaultExecutor :: parseJustRunCommandOptions (string line, struct JustRunCommandOptions *jco)
{
	int argc=0;
	char **argv=0;

	try
	{
		size_t starting_dq = line.find ("\"");
		if (starting_dq == string::npos || line[starting_dq-1] == '\\')
			throw (string ("Please specify the command within unescaped double quotes (\"\")."));

		/* if none of -aql, -afl is found then insert " --afl=" */
		size_t aql = line.find ("-aql");
		if (aql == string::npos)
		{
			aql = line.find ("-aql");
			if (aql == string::npos)
			{
				size_t afl = line.find ("-afl");
				if (afl == string::npos)
				{
					afl = line.find ("-afl");
					if (afl == string::npos)
					{
						/* it must be an AFL query. So insert " --afl=" */
						char *part1 = new char [starting_dq+1];
						line.copy (part1, starting_dq);
						part1[starting_dq]=0;

						int bytes = line.size() - starting_dq;
						char *part2 = new char [bytes + 1];
						bytes = line.copy (part2, bytes, starting_dq);
						part2[bytes]=0;

						string newLine = part1;
						newLine = newLine + " --afl=" + part2;
						line = newLine;

						delete part1;
						delete part2;
					}
				}
			}
		}

		/*
         * parse remaining command line
         */
		{
			vector<string> tokens;
			if (tokenize_commandline (line, tokens) == FAILURE)
			{
				string error="Problem while separating the tokens of the commandline [";
				error = error + line + "]";
				throw (error);
			}
			else
			{
				LOG4CXX_INFO (_logger, "Arguments for command [" << line << "] successfully separated.");
			}

			argc = tokens.size ();
			argv = new char * [argc];
			for (int i=0; i<argc; i++)
			{
				argv[i] = new char [tokens[i].length() + 1];
				snprintf (argv[i], tokens[i].length() + 1, "%s", tokens[i].c_str());
			}
		}

		po::options_description desc(
				"Usage: --justrun "
				"[--igdata] "
                "[--aql \"<AQL query to be executed>\"] "
                "[--afl \"<AFL query to be executed>\"]\n"
				);

		desc.add_options()
			("igdata",                    "Whether data output by the query should be ignored or stored in a .expected/.out file.")
		    ("aql",  po::value<string>(), "AQL query to be executed.")
			("afl",  po::value<string>(), "AFL query to be executed.");

		po::variables_map vm;
		po::store (po::parse_command_line (argc, argv, desc), vm);
		po::notify (vm);

		int aql_given = 0;
		int afl_given = 0;
		if (!vm.empty ())
		{
			if (vm.count ("igdata"))
				jco->_igdata = true;

			if (vm.count ("aql"))
			{
				if (afl_given)
					throw (string ("Only one of --aql or --afl can be given."));
				jco->_query = vm["aql"].as<string>();
				aql_given = 1;
				jco->_afl = false;
			}

			if (vm.count ("afl"))
			{
				if (aql_given)
					throw (string ("Only one of --aql or --afl can be given."));
				jco->_query = vm["afl"].as<string>();
				afl_given = 1;
				jco->_afl = true;
			}
		}

		if (jco->_query.empty())
			throw (string ("Please specify scidb query to be executed."));

		for (int i=0; i<argc; i++)
			delete argv[i];
		delete []argv;
	}

    catch (const std::exception& e)
	{
		for (int i=0; i<argc; i++)
			delete argv[i];
		delete []argv;

		throw (string (e.what ()));
	}

	return SUCCESS;
}

int DefaultExecutor :: parseErrorCommandOptions (string line, struct ErrorCommandOptions *eco)
{
	int argc=0;
	char **argv=0;

	try
	{
		size_t starting_dq = line.find ("\"");
		if (starting_dq == string::npos || line[starting_dq-1] == '\\')
			throw (string ("Please specify the command within unescaped double quotes (\"\")."));

		/* if none of -aql, -afl is found then insert " --afl=" */
		size_t aql = line.find ("-aql");
		if (aql == string::npos)
		{
			aql = line.find ("-aql");
			if (aql == string::npos)
			{
				size_t afl = line.find ("-afl");
				if (afl == string::npos)
				{
					afl = line.find ("-afl");
					if (afl == string::npos)
					{
						/* it must be an AFL query. So insert " --afl=" */
						char *part1 = new char [starting_dq+1];
						line.copy (part1, starting_dq);
						part1[starting_dq]=0;

						int bytes = line.size() - starting_dq;
						char *part2 = new char [bytes + 1];
						bytes = line.copy (part2, bytes, starting_dq);
						part2[bytes]=0;

						string newLine = part1;
						newLine = newLine + " --afl=" + part2;
						line = newLine;

						delete part1;
						delete part2;
					}
				}
			}
		}

		/*
         * parse remaining command line
         */
		{
			vector<string> tokens;
			if (tokenize_commandline (line, tokens) == FAILURE)
			{
				string error="Problem while separating the tokens of the commandline [";
				error = error + line + "]";
				throw (error);
			}
			else
			{
				LOG4CXX_INFO (_logger, "Arguments for command [" << line << "] successfully separated.");
			}

			argc = tokens.size ();
			argv = new char * [argc];
			for (int i=0; i<argc; i++)
			{
				argv[i] = new char [tokens[i].length() + 1];
				snprintf (argv[i], tokens[i].length() + 1, "%s", tokens[i].c_str());
			}
		}

		po::options_description desc(
				"Usage: --error [--code <value>] "
				"[--igdata] "
                "[--aql \"<AQL query to be executed>\"] "
                "[--afl \"<AFL query to be executed>\"]\n"
				);

		desc.add_options()
                ("code", po::value<string>(), "Expected stringified error id.")
                ("shortid", po::value<string>(), "Expected non-stringified error id.")
                ("namespace", po::value<string>(), "Expected error code namespace.")
                ("short", po::value<int32_t>(), "Expected short error code.")
                ("long", po::value< vector<int32_t> > (), "Expected long error code.")
                ("igdata",                    "Whether data output by the query should be ignored or stored in a .expected/.out file.")
                ("hideQueryString",           "Exclude query string from the output.")
                ("aql",  po::value<string>(), "AQL query to be executed.")
                ("afl",  po::value<string>(), "AFL query to be executed.");

		po::variables_map vm;
		po::store (po::parse_command_line (argc, argv, desc), vm);
		po::notify (vm);

		int aql_given = 0;
		int afl_given = 0;
                if (!vm.empty ())
		{
		    if (vm.count("code")) {
				eco->_expected_errorcode = vm["code"].as<string>();
                    }

                    if (vm.count("shortid")) {
                        eco->_expected_errorcode2 = vm["shortid"].as<string>();
                    }
                    if (vm.count("namespace")) {
                        eco->_expected_errns = vm["namespace"].as<string>();
                    }
                    if (vm.count("short")) {
                        eco->_expected_errshort = vm["short"].as<int32_t>();
                    }
                    if (vm.count("long")) {
                        vector<int32_t> v = vm["long"].as<vector<int32_t> >();
                        eco->_expected_errlong.insert(v.begin(), v.end());
                    }
                    if (vm.count ("igdata")) {
                        eco->_igdata = true;
                    }
                    if (vm.count ("hideQueryString")) {
                        eco->_hideQueryString = true;
                    }
                    if (vm.count ("aql"))
                    {
                        if (afl_given) {
                            throw (string ("Only one of --aql or --afl can be given."));
                        }
                        eco->_query = vm["aql"].as<string>();
                        aql_given = 1;
                        eco->_afl = false;
                    }

                    if (vm.count ("afl"))
                    {
                        if (aql_given) {
                            throw (string ("Only one of --aql or --afl can be given."));
                        }
                        eco->_query = vm["afl"].as<string>();
                        afl_given = 1;
                        eco->_afl = true;
                    }
		}

//		if (!code_given)
//			throw (string ("Please specify expected error code for --error command."));
// The above two lines are commented so that it is no longer necessary to specify the expected error code.

		if (eco->_query.empty())
			throw (string ("Please specify scidb query to be executed."));

		for (int i=0; i<argc; i++)
			delete argv[i];
		delete []argv;
	}

    catch (const std::exception& e)
	{
		for (int i=0; i<argc; i++)
			delete argv[i];
		delete []argv;

		throw (string (e.what ()));
	}

	return SUCCESS;
}

void DefaultExecutor :: parseEnvironmentVariables (string &line, int line_number)
{
        /* Code for substituting environment variables */
        if (boost::starts_with (line, "--shell"))
        {
                return;
        }

        char *env_value = NULL;
        size_t env_end = string::npos;
        size_t env_start = line.find ("${");
        if (env_start != string::npos)
        {
                LOG4CXX_DEBUG (_logger, "Checking and substituting environment variables for line: " << line);
        }
        while (env_start != string::npos)
        {
                env_end = line.find ("}", env_start + 2);
                if (env_end == string::npos)
                {
                        throw_PARSEERROR ("Missing '}' in environment variable.");
                }
                if (env_end == env_start+2)
                {
                        throw_PARSEERROR ("No variable name specified \"${}\".");
                }

                boost::scoped_array<char> env_var_ptr (new char [env_end - env_start - 1]);
                line.copy (env_var_ptr.get(), env_end - env_start - 2, env_start + 2);
                env_var_ptr.get() [env_end - env_start - 2] = '\0';
                if (line.find ("${", env_start + 2) <= env_end)
                {
                        throw_PARSEERROR ("Illegal variable name: '" << env_var_ptr.get() << "'.");
                }

                env_value = getenv (env_var_ptr.get());
 		if (env_value)
		{
                        line.replace (env_start, env_end - env_start + 1, env_value);
                        LOG4CXX_DEBUG (_logger, "Environment variable '" << env_var_ptr.get() << "' substituted with '" << env_value << "'.");
                        if (strcmp (env_value,"") == 0)
                        {
                                 env_start --; // required when the next environment variable is immediately after this (value=empty) one
                        }
                }
                else
                {
                        throw_PARSEERROR ("Environment variable '" << env_var_ptr.get() << "' not found.");
                }
                env_start = line.find ("${", env_start+1);
        }
}

void DefaultExecutor :: addTestSpecificVariables (void)
{
    assert (!_ie.tcfile.empty ());

    //----------------------------------------------------------------
    // Add environment variable that specifies test directory for
    // shell option.
    size_t lastDirSep = _ie.tcfile.find_last_of("/");

    if (lastDirSep != std::string::npos)
    {
      std::string testFolder = _ie.tcfile.substr(0,lastDirSep);

      _testEnvVars["TESTDIR"] = testFolder;
    }
    else
    {
      LOG4CXX_INFO (_logger, "Setting $TESTDIR environment variable failed: failed to locate dir separator in .test path!");
    }

    //----------------------------------------------------------------
    // Set all of the above variables in the environment now.
    setTestSpecificVariables();
}

void DefaultExecutor :: setTestSpecificVariables (void)
{
  for (
       std::map<std::string,std::string>::iterator it=_testEnvVars.begin();
       it!=_testEnvVars.end();
       it++
       )
    {
      std::string variableName;
      std::string variableValue;

      variableName = it->first;
      variableValue = it->second;

      if (setenv(variableName.c_str(),variableValue.c_str(),1) != 0)
     {
       LOG4CXX_INFO (_logger, "Setting environment variable $" << variableName << " failed: call to setenv returned non-zero!");
     }
    }
}

int DefaultExecutor :: parseTestCaseFile (void)
{
	LOG4CXX_INFO (_logger, "Parsing test case file : " << _ie.tcfile);
	assert (!_ie.tcfile.empty ());

	ifstream f;

	try
	{
		bfs::path p (_ie.tcfile);
		if (bfs :: is_empty (p))
		{
			_errStream << "Test case File [" << _ie.tcfile << "] is empty";
			throw SystemError (FILE_LINE_FUNCTION, _errStream.str());
		}

		f.open (_ie.tcfile.c_str());
		if (!f.is_open ())
		{
			_errStream << "Could not open file [" << _ie.tcfile << "]";
			throw SystemError (FILE_LINE_FUNCTION, _errStream.str());
		}

		string line;
		int sFound = 0;
		int tFound = 0;
		int cFound = 0;
		int in_setup = 0;
		int in_test = 0;
		int in_cleanup = 0;
		Command tmp;
		int setup_pushed_to_vector = 0;
		int test_pushed_to_vector = 0;
		int cleanup_pushed_to_vector = 0;

		_currentSection = &_preSetupCommands;
		int line_number=0;
		while (!f.eof ())
		{
			std::getline (f, line, LINE_FEED);
			line_number++;

			/* remove left and right hand side spaces.
			 * a line containing only the spaces will be reduced to a blank line */
			boost::trim (line);

			/* if blank line or a comment */
			if (line.empty() || boost::starts_with (line, "#"))
				continue;

			parseEnvironmentVariables(line, line_number);

			/* TODO : first join multiple lines if it is a multiline command */
			vector<string> tokens;
			tokenize (line, tokens, " ");

			assert (tokens.size () > 0);

			/* case incensitive comparison : command --setup */
			if (boost::iequals (tokens[0],TESTCASE_COMMAND_SETUP))
			{
				if (sFound)
					throw_PARSEERROR ("SETUP command should not appear more than once")
				else if (tFound || cFound)
					throw_PARSEERROR ("SETUP command should appear before TEST and CLEANUP commands")

				LOG4CXX_INFO (_logger, "found SETUP section.");
				setup_pushed_to_vector = 0;
				sFound = 1;
				in_setup = 1;
				in_test = 0;
				in_cleanup = 0;
				tmp.cmd = TESTCASE_COMMAND_SETUP;
				_currentSection = &_setupCommands;

			/* command --test */
			} else if (boost::iequals (tokens[0],TESTCASE_COMMAND_TEST))
			{
				if (tFound)
					throw_PARSEERROR ("TEST command should not appear more than once")
				else if (cFound)
					throw_PARSEERROR ("TEST command should appear before CLEANUP command")

				if (!sFound)
				{
					LOG4CXX_INFO (_logger, "WARN :: TEST command found without SETUP command found first");
				}
				else
					LOG4CXX_INFO (_logger, "found TEST section.");

				tFound = 1;
				test_pushed_to_vector = 0;
				in_setup = 0;
				in_test = 1;
				in_cleanup = 0;

				/* store --setup section to a vector of commands */
				if (sFound && !setup_pushed_to_vector)
				{
					_currentSection->push_back (tmp);
					tmp.cmd = "";
					tmp.args = "";
					tmp.subCommands.clear ();
					setup_pushed_to_vector = 1;
				}
				tmp.cmd = TESTCASE_COMMAND_TEST;
				_currentSection = &_testCommands;

			/* command --cleanup */
			} else if (boost::iequals (tokens[0],TESTCASE_COMMAND_CLEANUP))
			{
				if (cFound)
					throw_PARSEERROR ("CLEANUP command should not appear more than once")

				if (!tFound)
				{
					LOG4CXX_INFO (_logger, "WARN :: CLEANUP command found without TEST command found first");
				}
				else
					LOG4CXX_INFO (_logger, "found CLEANUP section.");

				cFound = 1;
				cleanup_pushed_to_vector = 0;
				in_setup = 0;
				in_test = 0;
				in_cleanup = 1;

				/* store --setup section to a vector of commands */
				if (sFound && !setup_pushed_to_vector)
				{
					_currentSection->push_back (tmp);
					tmp.cmd = "";
					tmp.args = "";
					tmp.subCommands.clear ();
					setup_pushed_to_vector = 1;
				}
				/* store --test section to a vector of commands */
				else if (tFound && !test_pushed_to_vector)
				{
					_currentSection->push_back (tmp);
					tmp.cmd = "";
					tmp.args = "";
					tmp.subCommands.clear ();
					test_pushed_to_vector = 1;
				}
				tmp.cmd = TESTCASE_COMMAND_CLEANUP;
				_currentSection = &_cleanupCommands;

			/* command --echo */
			} else if (boost::iequals (tokens[0],TESTCASE_COMMAND_ECHO))
			{
				string echo = TESTCASE_COMMAND_ECHO;
				int command_length = echo.length() + 1; /* for first space */
				line.replace (0, command_length, "");   /* remove the command and rest will be the args */

				if (!in_setup && !in_test && !in_cleanup)
				{
					/* this means --echo is out of any section i.e. at the beginning of the file before --setup */
					tmp.cmd = TESTCASE_COMMAND_ECHO;
					tmp.args = line;
					_currentSection->push_back (tmp);
					tmp.cmd = "";
					tmp.args = "";
					tmp.subCommands.clear ();
				}
				else
				{
					/* this means --echo is appearing under some section */
					Command subtmp;
					subtmp.cmd = TESTCASE_COMMAND_ECHO;
					subtmp.args = line;
					tmp.subCommands.push_back (subtmp);
				}

			/* command --shell */
			} else if (boost::iequals (tokens[0],TESTCASE_COMMAND_SHELL))
			{
				/* --shell commandline parsing */
				ShellCommandOptions *sco = new ShellCommandOptions;
				try
				{
					parseShellCommandOptions (line, sco);
				}

				catch (string s)
				{
					delete sco;
					throw_PARSEERROR (s);
				}

				string shell = TESTCASE_COMMAND_SHELL;
				int command_length = shell.length() + 1; /* for first space */
				line.replace (0, command_length, "");    /* remove the command and rest will be the args */

				if (!in_setup && !in_test && !in_cleanup)
				{
					/* this means --shell is out of any section i.e. at the beginning of the file before --setup */
					tmp.cmd = TESTCASE_COMMAND_SHELL;
					tmp.args = line;
					tmp.extraInfo = sco;
					_currentSection->push_back (tmp);
					tmp.cmd = "";
					tmp.args = "";
					tmp.extraInfo = 0;
					tmp.subCommands.clear ();
				}
				else
				{
					/* this means --shell is appearing under some section */
					Command subtmp;
					subtmp.cmd = TESTCASE_COMMAND_SHELL;
					subtmp.args = line;
					subtmp.extraInfo = sco;
					tmp.subCommands.push_back (subtmp);
				}

			/* command --aql */
			} else if (boost::iequals (tokens[0],TESTCASE_COMMAND_AQL))
			{
				if (!in_setup && !in_test && !in_cleanup)
				{
					/* this means --aql is out of any section i.e. at the beginning of the file before --setup */
					_errStream << "Command " << TESTCASE_COMMAND_AQL << " must appear inside some section (i.e. --setup, --test, --cleanup)";
					throw_PARSEERROR (_errStream.str())
				}
				else
				{
					/* this means --aql is appearing under some section */
					string aql = TESTCASE_COMMAND_AQL;
					int command_length = aql.length() + 1;
					line.replace (0, command_length, "");

					if (line.empty ())
					{
						_errStream << "Command " << TESTCASE_COMMAND_AQL << "  must be followed by some scidb query on the same line";
						throw_PARSEERROR (_errStream.str())
					}

					Command subtmp;
					subtmp.cmd = TESTCASE_COMMAND_AQL;
					subtmp.args = line;
					tmp.subCommands.push_back (subtmp);
				}

			/* command --disable-query */
			} else if (boost::iequals (tokens[0],TESTCASE_COMMAND_DISABLEQUERY))
			{
				if (!in_setup && !in_test && !in_cleanup)
				{
					/* this means --disable-query is out of any section i.e. at the beginning of the file before --setup */
					_errStream << "Command " << TESTCASE_COMMAND_DISABLEQUERY << " must appear inside some section (i.e. --setup, --test, --cleanup)";
					throw_PARSEERROR (_errStream.str())
				}
				else
				{
					/* this means --echo is appearing under some section */
					string echo = TESTCASE_COMMAND_DISABLEQUERY;
					int command_length = echo.length() + 1;
					line.replace (0, command_length, "");

					if (line.empty ())
					{
						_errStream << TESTCASE_COMMAND_DISABLEQUERY << " must be followed by some scidb query on the same line";
						throw_PARSEERROR (_errStream.str())
					}

					/* ignore the line as it is treated as a comment line that starts with a '#' */
				}

			/* command --sleep */
			} else if (boost::iequals (tokens[0],TESTCASE_COMMAND_SLEEP))
			{
				string sleep = TESTCASE_COMMAND_SLEEP;
				int command_length = sleep.length() + 1;
				line.replace (0, command_length, "");

				if (line.empty ())
				{
					_errStream << "Command " << TESTCASE_COMMAND_SLEEP << " must be given number of seconds to sleep";
					throw_PARSEERROR (_errStream.str())
				}

				int seconds = sToi (line);
				if (seconds <= 0)
					throw_PARSEERROR ("Invalid interger string provided to --sleep")

				if (!in_setup && !in_test && !in_cleanup)
				{
					/* this means --sleep is out of any section i.e. at the beginning of the file before --setup */
					tmp.cmd = TESTCASE_COMMAND_SLEEP;
					tmp.args = line;
					_currentSection->push_back (tmp);
					tmp.cmd = "";
					tmp.args = "";
					tmp.subCommands.clear ();
				}
				else
				{
					/* this means --sleep is appearing under some section */
					Command subtmp;
					subtmp.cmd = TESTCASE_COMMAND_SLEEP;
					subtmp.args = line;
					tmp.subCommands.push_back (subtmp);
				}
			/* command --reconnect */
			} else if (boost::iequals (tokens[0],TESTCASE_COMMAND_RECONNECT))
			{
				if (!in_setup && !in_test && !in_cleanup)
				{
					/* this means --reconnect is out of any section i.e. at the beginning of the file before --setup */
					_errStream << "Command " << TESTCASE_COMMAND_RECONNECT << " must appear inside some section (i.e. --setup, --test, --cleanup)";
					throw_PARSEERROR (_errStream.str())
				}
				else
				{
					/* this means --connect is appearing under some section */
					Command subtmp;
					subtmp.cmd = TESTCASE_COMMAND_RECONNECT;
					subtmp.args = "";
					tmp.subCommands.push_back (subtmp);
				}

			/* command --start-query-logging */
			} else if (boost::iequals (tokens[0],TESTCASE_COMMAND_STARTQUERYLOGGING))
			{
				if (!in_setup && !in_test && !in_cleanup)
				{
					/* this means --start-query-logging is out of any section i.e. at the beginning of the file before --setup */
					_errStream << "Command " << TESTCASE_COMMAND_STARTQUERYLOGGING << " must appear inside some section (i.e. --setup, --test, --cleanup)";
					throw_PARSEERROR (_errStream.str())
				}
				else
				{
					/* this means --start-query-logging is appearing under some section */
					Command subtmp;
					subtmp.cmd = TESTCASE_COMMAND_STARTQUERYLOGGING;
					subtmp.args = "";
					tmp.subCommands.push_back (subtmp);
					_queryLogging = true;
				}

			/* command --stop-query-logging */
			} else if (boost::iequals (tokens[0],TESTCASE_COMMAND_STOPQUERYLOGGING))
			{
				if (_queryLogging == false)
				{
					_errStream << "Command " << TESTCASE_COMMAND_STOPQUERYLOGGING << " does not match with the corresponding " << TESTCASE_COMMAND_STARTQUERYLOGGING;
					throw_PARSEERROR (_errStream.str())
				}

				if (!in_setup && !in_test && !in_cleanup)
				{
					/* this means --stop-query-logging is out of any section i.e. at the beginning of the file before --setup */
					_errStream << "Command " << TESTCASE_COMMAND_STOPQUERYLOGGING << " must appear inside some section (i.e. --setup, --test, --cleanup)";
					throw_PARSEERROR (_errStream.str())
				}
				else
				{
					/* this means --stop-query-logging is appearing under some section */
					Command subtmp;
					subtmp.cmd = TESTCASE_COMMAND_STOPQUERYLOGGING;
					subtmp.args = "";
					tmp.subCommands.push_back (subtmp);
					_queryLogging = false;
				}

                        /* command --start-ignore-warnings */
                        } else if (boost::iequals (tokens[0],TESTCASE_COMMAND_STARTIGNOREWARNINGS))
                        {
                                if (!in_setup && !in_test && !in_cleanup)
                                {
                                        /* this means --start-ignore-warnings is out of any section i.e. at the beginning of the file before --setup */
                                        _errStream << "Command " << TESTCASE_COMMAND_STARTIGNOREWARNINGS << " must appear inside some section (i.e. --setup, --test, --cleanup)";
                                        throw_PARSEERROR (_errStream.str())
                                }
                                else
                                {
                                        /* this means --start-ignore-warnings is appearing under some section */
                                        Command subtmp;
                                        subtmp.cmd = TESTCASE_COMMAND_STARTIGNOREWARNINGS;
                                        subtmp.args = "";
                                        tmp.subCommands.push_back (subtmp);
                                        _ignoreWarnings = true;
                                }

                        /* command --stop-ignore-warnings */
                        } else if (boost::iequals (tokens[0],TESTCASE_COMMAND_STOPIGNOREWARNINGS))
                        {
                                if (_ignoreWarnings == false)
                                {
                                        _errStream << "Command " << TESTCASE_COMMAND_STOPIGNOREWARNINGS << " does not match with the corresponding " << TESTCASE_COMMAND_STARTIGNOREWARNINGS;
                                        throw_PARSEERROR (_errStream.str())
                                }

                                if (!in_setup && !in_test && !in_cleanup)
                                {
                                        /* this means --stop-ignore-warnings is out of any section i.e. at the beginning of the file before --setup */
                                        _errStream << "Command " << TESTCASE_COMMAND_STOPIGNOREWARNINGS << " must appear inside some section (i.e. --setup, --test, --cleanup)";
                                        throw_PARSEERROR (_errStream.str())
                                }
                                else
                                {
                                        /* this means --stop-ignore-warnings is appearing under some section */
                                        Command subtmp;
                                        subtmp.cmd = TESTCASE_COMMAND_STOPIGNOREWARNINGS;
                                        subtmp.args = "";
                                        tmp.subCommands.push_back (subtmp);
                                        _ignoreWarnings = false;
                                }

			/* command --start-igdata */
			} else if (boost::iequals (tokens[0],TESTCASE_COMMAND_STARTIGDATA))
			{
				if (!in_setup && !in_test && !in_cleanup)
				{
					/* this means --start-igdata is out of any section i.e. at the beginning of the file before --setup */
					_errStream << "Command " << TESTCASE_COMMAND_STARTIGDATA << " must appear inside some section (i.e. --setup, --test, --cleanup)";
					throw_PARSEERROR (_errStream.str())
				}
				else
				{
					/* this means --start-igdata is appearing under some section */
					Command subtmp;
					subtmp.cmd = TESTCASE_COMMAND_STARTIGDATA;
					subtmp.args = "";
					tmp.subCommands.push_back (subtmp);
					_ignoredata_flag = true;
				}

			/* command --stop-igdata*/
			} else if (boost::iequals (tokens[0],TESTCASE_COMMAND_STOPIGDATA))
			{
				if (_ignoredata_flag == false)
				{
					_errStream << "Command " << TESTCASE_COMMAND_STOPIGDATA << " does not match with the corresponding " << TESTCASE_COMMAND_STARTIGDATA;
					throw_PARSEERROR (_errStream.str())
				}

				if (!in_setup && !in_test && !in_cleanup)
				{
					/* this means --stop-igdata is out of any section i.e. at the beginning of the file before --setup */
					_errStream << "Command " << TESTCASE_COMMAND_STOPIGDATA << " must appear inside some section (i.e. --setup, --test, --cleanup)";
					throw_PARSEERROR (_errStream.str())
				}
				else
				{
					/* this means --stop-igdata is appearing under some section */
					Command subtmp;
					subtmp.cmd = TESTCASE_COMMAND_STOPIGDATA;
					subtmp.args = "";
					tmp.subCommands.push_back (subtmp);
					_ignoredata_flag = false;
				}

			/* command --start-timer */
			} else if (boost::iequals (tokens[0],TESTCASE_COMMAND_STARTTIMER))
			{
				if (!in_setup && !in_test && !in_cleanup)
				{
					/* this means --start-timer is out of any section i.e. at the beginning of the file before --setup */
					_errStream << "Command " << TESTCASE_COMMAND_STARTTIMER << " must appear inside some section (i.e. --setup, --test, --cleanup)";
					throw_PARSEERROR (_errStream.str())
				}
				else
				{
					/* consecutive --start-timer appear without --stop-timer in between */
					if (_timerEnabled == true)
						throw_PARSEERROR ("Previous timer must be stopped before starting the new one")

					/* this means --start-timer is appearing under some section */
					if (tokens.size() == 1)
					{
						_errStream << "Command " << TESTCASE_COMMAND_STARTTIMER << " must be followed by some timer tag";
						throw_PARSEERROR (_errStream.str())
					}

					vector<string>::iterator it;
					string timer_tag = tokens[1];
					it = find (_timerTags.begin(), _timerTags.end(), timer_tag);

					if (it != _timerTags.end ())
					{
						_errStream << "Timer with the name [" << timer_tag << "] already exist. Give some different timer name.";
						throw_PARSEERROR (_errStream.str())
					}
					_timerTags.push_back (timer_tag);

					Command subtmp;
					subtmp.cmd = TESTCASE_COMMAND_STARTTIMER;
					subtmp.args = timer_tag;    /* only considering first token as a tag and ignoring rest of the tokens */
					tmp.subCommands.push_back (subtmp);
					_timerEnabled = true;
				}

			/* command --set-format */
			} else if (boost::iequals (tokens[0],TESTCASE_COMMAND_SETFORMAT))
			{
				if (!in_setup && !in_test && !in_cleanup)
				{
					/* this means --set-format is out of any section i.e. at the beginning of the file before --setup */
					_errStream << "Command " << TESTCASE_COMMAND_SETFORMAT << " must appear inside some section (i.e. --setup, --test, --cleanup)";
					throw_PARSEERROR (_errStream.str())
				}
				else
				{
					/* this means --set-format is appearing under some section */
					if (tokens.size() == 1)
					{
						_errStream << "Command " << TESTCASE_COMMAND_SETFORMAT << " must be followed by a Query Output Format";
						throw_PARSEERROR (_errStream.str())
					}
					string format_tag = tokens[1];

					Command subtmp;
					subtmp.cmd = TESTCASE_COMMAND_SETFORMAT;
					subtmp.args = format_tag; /* only considering first token as a tag and ignoring rest of the tokens */
					tmp.subCommands.push_back (subtmp);
				}

			/* command --stop-timer */
			} else if (boost::iequals (tokens[0],TESTCASE_COMMAND_STOPTIMER))
			{
				if (_timerEnabled == false)
				{
					_errStream << "Command " << TESTCASE_COMMAND_STOPTIMER << " does not match with the corresponding " << TESTCASE_COMMAND_STARTTIMER;
					throw_PARSEERROR (_errStream.str())
				}

				if (!in_setup && !in_test && !in_cleanup)
				{
					/* this means --stop-timer is out of any section i.e. at the beginning of the file before --setup */
					_errStream << "Command " << TESTCASE_COMMAND_STOPTIMER << " must appear inside some section (i.e. --setup, --test, --cleanup)";
					throw_PARSEERROR (_errStream.str())
				}
				else
				{
					/* this means --stop-timer is appearing under some section */
					Command subtmp;
					subtmp.cmd = TESTCASE_COMMAND_STOPTIMER;
					subtmp.args = _timerTags[_timerTags.size() - 1];
					tmp.subCommands.push_back (subtmp);
					_timerEnabled = false;
				}

			/* command --reset-format */
			} else if (boost::iequals (tokens[0],TESTCASE_COMMAND_ENDFORMAT))
			{
				if (!in_setup && !in_test && !in_cleanup)
				{
					/* this means --reset-format is out of any section i.e. at the beginning of the file before --setup */
					_errStream << "Command " << TESTCASE_COMMAND_ENDFORMAT << " must appear inside some section (i.e. --setup, --test, --cleanup)";
					throw_PARSEERROR (_errStream.str())
				}
				else
				{
					/* this means --reset-format is appearing under some section */
					Command subtmp;
					subtmp.cmd = TESTCASE_COMMAND_ENDFORMAT;
					tmp.subCommands.push_back (subtmp);
				}

			/* command --set-precision */
			} else if (boost::iequals (tokens[0],TESTCASE_COMMAND_SETPRECISION))
			{
				if (!in_setup && !in_test && !in_cleanup)
				{
					/* this means --set-precision is out of any section i.e. at the beginning of the file before --setup */
					_errStream << "Command " << TESTCASE_COMMAND_SETPRECISION << " must appear inside some section (i.e. --setup, --test, --cleanup)";
					throw_PARSEERROR (_errStream.str())
				}
				else
				{
					/* this means --set-precision is appearing under some section */
					if (tokens.size() == 1)
					{
						_errStream << "Command " << TESTCASE_COMMAND_SETPRECISION << " must be followed by a Precision Value";
						throw_PARSEERROR (_errStream.str())
					}
					string precision_tag = tokens[1];

					Command subtmp;
					subtmp.cmd = TESTCASE_COMMAND_SETPRECISION;
					subtmp.args = precision_tag; /* only considering first token as a tag and ignoring rest of the tokens */
					tmp.subCommands.push_back (subtmp);
				}

			/* command --error */
			} else if (boost::iequals (tokens[0],TESTCASE_COMMAND_ERROR))
			{
				/* commandline parsing */
				ErrorCommandOptions *eco = new ErrorCommandOptions;
				try
				{
					parseErrorCommandOptions (line, eco);
				}

				catch (string s)
				{
					delete eco;
					throw_PARSEERROR (s);
				}

				if (!in_setup && !in_test && !in_cleanup)
				{
					/* this means --error is out of any section i.e. at the beginning of the file before --setup */
					_errStream << "Command " << TESTCASE_COMMAND_ERROR << " must appear inside some section (i.e. --setup, --test, --cleanup)";
					throw_PARSEERROR (_errStream.str())
				}
				else
				{
					/* this means --error is appearing under some section */
					string error = TESTCASE_COMMAND_ERROR;
					int command_length = error.length() + 1;
					line.replace (0, command_length, "");

					if (line.empty ())
					{
						_errStream << "Invalid syntax for command " << TESTCASE_COMMAND_ERROR;
						throw_PARSEERROR (_errStream.str())
					}

					Command subtmp;
					subtmp.cmd = TESTCASE_COMMAND_ERROR;
					subtmp.args = line;
					subtmp.extraInfo = eco;
					tmp.subCommands.push_back (subtmp);
				}

			/* command --justrun */
			} else if (boost::iequals (tokens[0],TESTCASE_COMMAND_JUSTRUN))
			{
				/* commandline parsing */
				JustRunCommandOptions *jco = new JustRunCommandOptions;
				try
				{
					parseJustRunCommandOptions (line, jco);
				}

				catch (string s)
				{
					delete jco;
					throw_PARSEERROR (s);
				}

				if (!in_setup && !in_test && !in_cleanup)
				{
					/* this means --error is out of any section i.e. at the beginning of the file before --setup */
					_errStream << "Command " << TESTCASE_COMMAND_JUSTRUN << " must appear inside some section (i.e. --setup, --test, --cleanup)";
					throw_PARSEERROR (_errStream.str())
				}
				else
				{
					/* this means --justrun is appearing under some section */
					string error = TESTCASE_COMMAND_JUSTRUN;
					int command_length = error.length() + 1;
					line.replace (0, command_length, "");

					if (line.empty ())
					{
						_errStream << "Invalid syntax for command " << TESTCASE_COMMAND_JUSTRUN;
						throw_PARSEERROR (_errStream.str())
					}

					Command subtmp;
					subtmp.cmd = TESTCASE_COMMAND_JUSTRUN;
					subtmp.args = line;
					subtmp.extraInfo = jco;
					tmp.subCommands.push_back (subtmp);
				}

			/* command --igdata */
			} else if (boost::iequals (tokens[0],TESTCASE_COMMAND_IGDATA))
			{
				/* commandline parsing */
				IgnoreDataOptions *ido = new IgnoreDataOptions;
				try
				{
					parseIgnoreDataOptions (line, ido);
				}

				catch (string s)
				{
					delete ido;
					throw_PARSEERROR (s);
				}

				if (!in_setup && !in_test && !in_cleanup)
				{
					/* this means --igdata is out of any section i.e. at the beginning of the file before --setup */
					_errStream << "Command " << TESTCASE_COMMAND_IGDATA << " must appear inside some section (i.e. --setup, --test, --cleanup)";
					throw_PARSEERROR (_errStream.str())
				}
				else
				{
					/* this means --igdata is appearing under some section */
					string error = TESTCASE_COMMAND_IGDATA;
					int command_length = error.length() + 1;
					line.replace (0, command_length, "");

					if (line.empty ())
					{
						_errStream << "Invalid syntax for command " << TESTCASE_COMMAND_IGDATA;
						throw_PARSEERROR (_errStream.str())
					}

					Command subtmp;
					subtmp.cmd = TESTCASE_COMMAND_IGDATA;
					subtmp.args = line;
					subtmp.extraInfo = ido;
					tmp.subCommands.push_back (subtmp);
				}

			/* command --exit */
			} else if (boost::iequals (tokens[0],TESTCASE_COMMAND_EXIT))
			{
				string exit = TESTCASE_COMMAND_EXIT;
				int command_length = exit.length() + 1;
				line.replace (0, command_length, "");

				if (!in_setup && !in_test && !in_cleanup)
				{
					/* this means --echo is out of any section i.e. at the beginning of the file before --setup */
					tmp.cmd = TESTCASE_COMMAND_EXIT;
					tmp.args = line;
					_currentSection->push_back (tmp);
					tmp.cmd = "";
					tmp.args = "";
					tmp.subCommands.clear ();
				}
				else
				{
					/* this means --echo is appearing under some section */
					Command subtmp;
					subtmp.cmd = TESTCASE_COMMAND_EXIT;
					subtmp.args = line;
					tmp.subCommands.push_back (subtmp);
				}

			/* everything else */
			} else
			{
				if (boost::starts_with (tokens[0], "--"))
				{
					_errStream << "Invalid test-file command [" << tokens[0] << "].";
					throw_PARSEERROR (_errStream.str())
				}

				/* all other lines will be treated as SciDB Queries(i.e. AFL queries) appearing under some section */
				if (!in_setup && !in_test && !in_cleanup)
				{
					_errStream << "SciDBQuery [" << line << "] must appear inside some section (i.e. --setup, --test, --cleanup)";
					throw_PARSEERROR (_errStream.str())
				}
				else
				{
					/* considered an AFL query */
					Command subtmp;
					subtmp.cmd = TESTCASE_COMMAND_AFL;
					subtmp.args = line;
					tmp.subCommands.push_back (subtmp);
				}
			}
		} // END while(!EOF)

		/* --start-timer is not closed with --stop-timer */
		if (_timerEnabled == true)
		{
			_errStream << "Timer [" << _timerTags[_timerTags.size() - 1] << "] is not stopped. Please stop the timer using " << TESTCASE_COMMAND_STOPTIMER << " command.";
			throw_PARSEERROR (_errStream.str());
		}

		/* store the last section (i.e. either of --setup, --test or --cleanup) */
		if ((sFound && !setup_pushed_to_vector) ||
			(tFound && !test_pushed_to_vector)  ||
			(cFound && !cleanup_pushed_to_vector))
		{
			_currentSection->push_back (tmp);
		}
	}

	catch (harnessexceptions :: ERROR &e)
	{
		PRINT_ERROR (e.what ());
		f.close ();
		return FAILURE;
	}

	f.close ();
	LOG4CXX_INFO (_logger, "Done Parsing test case file...");
	return SUCCESS;
}

void DefaultExecutor :: printExecutorEnvironment (void)
{
	LOG4CXX_INFO (_logger, "Printing executor Environment : ");

	LOG4CXX_INFO (_logger, "_ie.tcfile : "           << _ie.tcfile);
	LOG4CXX_INFO (_logger, "_ie.connectionString : " << _ie.connectionString);
	LOG4CXX_INFO (_logger, "_ie.scidbPort : "        << _ie.scidbPort);
	LOG4CXX_INFO (_logger, "_ie.rootDir : "          << _ie.rootDir);
	LOG4CXX_INFO (_logger, "_ie.sleepTime : "        << _ie.sleepTime);
	LOG4CXX_INFO (_logger, "_ie.log_queries : "      << _ie.log_queries);
	LOG4CXX_INFO (_logger, "_ie.save_failures : "    << _ie.save_failures);
	LOG4CXX_INFO (_logger, "_ie.logDir : "           << _ie.logDir);
	LOG4CXX_INFO (_logger, "_ie.debugLevel : "       << _ie.debugLevel);

	/* print this only in normal cases and not during selftesting */
	if (_ie.selftesting == false)
		LOG4CXX_INFO (_logger, "_ie.record : "           << _ie.record);

	LOG4CXX_INFO (_logger, "_ie.keepPreviousRun : "  << _ie.keepPreviousRun);

	/* these parameters will be filled up by the Executor to be used for reporting */
	LOG4CXX_INFO (_logger, "_ie.expected_rfile : "   << _ie.expected_rfile);
	LOG4CXX_INFO (_logger, "_ie.actual_rfile : "     << _ie.actual_rfile);
	LOG4CXX_INFO (_logger, "_ie.diff_file : "        << _ie.diff_file);
	LOG4CXX_INFO (_logger, "_ie.log_file : "         << _ie.log_file);

	LOG4CXX_INFO (_logger, "_ie.logger_name : "     << _ie.logger_name);

	LOG4CXX_INFO (_logger, "Done Printing executor Environment...");
}

int DefaultExecutor :: createLogger (void)
{
	assert (_ie.log_file.length () > 0);
	bfs::remove (_ie.log_file);

	_logger = log4cxx :: Logger :: getLogger (_ie.logger_name);
    _logger->setAdditivity (0);

	string pattern_string;

	/* if not a selftesting then print date */
	if (_ie.selftesting == false)
		pattern_string = "%d %p %x - %m%n";
	else
		pattern_string = "%p %x - %m%n";

	log4cxx :: LayoutPtr layout (new log4cxx :: PatternLayout (pattern_string));

	if (strcasecmp (_ie.logDestination.c_str (), LOGDESTINATION_CONSOLE) == 0)
	{
		log4cxx :: ConsoleAppenderPtr appender (new log4cxx :: ConsoleAppender(layout));
		_logger->addAppender (appender);
	}
	else
	{
		log4cxx :: FileAppenderPtr appender (new log4cxx :: FileAppender (layout, _ie.log_file, true));
		_logger->addAppender (appender);
	}

    _executorTag = LOGGER_TAG_EXECUTOR;

	/* if not a selftesting then print thread ID */
	if (_ie.selftesting == false)
		_executorTag += '[' + _ie.logger_name + ']';
	else
		_executorTag += "[]";

    LOGGER_PUSH_NDCTAG (_executorTag);
	_loggerEnabled = true;
	LOG4CXX_INFO (_logger, "logger SYSTEM ENABLED");

	switch (_ie.debugLevel)
	{
		case DEBUGLEVEL_FATAL : _logger->setLevel (log4cxx :: Level :: getFatal ()); break;
		case DEBUGLEVEL_ERROR : _logger->setLevel (log4cxx :: Level :: getError ()); break;
		case DEBUGLEVEL_WARN  : _logger->setLevel (log4cxx :: Level :: getWarn ()); break;
		case DEBUGLEVEL_INFO  : _logger->setLevel (log4cxx :: Level :: getInfo ()); break;
		case DEBUGLEVEL_DEBUG : _logger->setLevel (log4cxx :: Level :: getDebug ()); break;
		case DEBUGLEVEL_TRACE : _logger->setLevel (log4cxx :: Level :: getTrace ()); break;
		default               : return FAILURE;
	}

	return SUCCESS;
}

int DefaultExecutor :: validateParameters (void)
{
	try
	{
		if (_ie.connectionString.empty ())
			throw ConfigError (FILE_LINE_FUNCTION, ERR_CONFIG_SCIDBCONNECTIONSTRING_EMPTY);

		if (_ie.scidbPort < 1)
			throw ConfigError (FILE_LINE_FUNCTION, ERR_CONFIG_SCIDBPORT_INVALID);

		_ie.tcfile = getAbsolutePath (_ie.tcfile, false);
		if (_ie.tcfile.empty ())
			throw ConfigError (FILE_LINE_FUNCTION, ERR_CONFIG_TESTCASEFILENAME_EMPTY);
		if (!bfs::is_regular (_ie.tcfile))
		{
			_errStream << "Test case file " << _ie.tcfile << " either does not exist or is not a regular file.";
			throw SystemError (FILE_LINE_FUNCTION, _errStream.str());
		}

		if (_ie.sleepTime < 0)
			throw ConfigError (FILE_LINE_FUNCTION, ERR_CONFIG_INVALID_SLEEPVALUE);

		if (_ie.debugLevel < MIN_DEBUG_LEVEL || _ie.debugLevel > MAX_DEBUG_LEVEL)
		{
			_errStream << "Invalid value specified for option --debug. Valid range is [" << MIN_DEBUG_LEVEL << "-" << MAX_DEBUG_LEVEL << "]";
			throw ConfigError (FILE_LINE_FUNCTION, _errStream.str());
		}
	}

	catch (harnessexceptions :: ConfigError &e)
	{
		PRINT_ERROR (e.what ());
		return FAILURE;
	}

	return SUCCESS;
}

void DefaultExecutor :: copyToLocal (const InfoForExecutor &ie)
{
	_ie.tcfile             = ie.tcfile;
	_ie.connectionString   = ie.connectionString;
	_ie.scidbPort          = ie.scidbPort;
	_ie.rootDir            = ie.rootDir;
	_ie.sleepTime          = ie.sleepTime;
	_ie.log_queries        = ie.log_queries;
	_ie.save_failures      = ie.save_failures;
	_ie.logDir             = ie.logDir;
	_ie.scratchDir         = ie.scratchDir;
	_ie.logDestination     = ie.logDestination;
	_ie.debugLevel         = ie.debugLevel;
	_ie.record             = ie.record;
	_ie.keepPreviousRun    = ie.keepPreviousRun;
	_ie.selftesting        = ie.selftesting;
	_ie.startTestSectionMillisec = ie.startTestSectionMillisec;
	_ie.endTestSectionMillisec   = ie.endTestSectionMillisec;

	_ie.expected_rfile     = ie.expected_rfile;
	_ie.actual_rfile       = ie.actual_rfile;
	_ie.timerfile          = ie.timerfile;
	_ie.log_file           = ie.log_file;

	_ie.logger_name       = ie.logger_name;
}

int DefaultExecutor :: execute (InfoForExecutor &ie)
{
	time_t rawtime;
	time ( &rawtime );
	std::string nowStr(ctime (&rawtime));
	nowStr = nowStr.substr(0,nowStr.size()-1); // remove newline

	if (strcasecmp (ie.logDestination.c_str (), LOGDESTINATION_CONSOLE) != 0)
	{
		cout << "[" << ie.test_sequence_number << "][" << nowStr << "]: "
		     << "[start] " << ie.testID << std::endl;
	}

	copyToLocal (ie);

	if (validateParameters () == FAILURE)
		return FAILURE;

	createLogger ();

    //------------------------------------------------------------------------
    // Introduce test-specific environment variables (see function definition
    // for details).
    //------------------------------------------------------------------------
    addTestSpecificVariables();

	//printExecutorEnvironment ();

	if (parseTestCaseFile () == FAILURE)
	{
		LOG4CXX_INFO (_logger, "EXECUTOR returning FAILURE to the caller.");
		return FAILURE;
	}

	LOG4CXX_INFO (_logger, "Printing Parsed Vector : ");
	printParsedSections ();
	LOG4CXX_INFO (_logger, "Done Printing Parsed Vector... ");

	if (executeTestCase () == FAILURE)
	{
		LOG4CXX_INFO (_logger, "EXECUTOR returning FAILURE to the caller.");
		if (_precisionSet == true)
		{
			scidb::ArrayWriter::setPrecision(_precisionDefaultValue);
			_precisionSet = false;
		}
		return FAILURE;
	}

	// for success: update the caller's ie to publish the time taken
	ie.startTestSectionMillisec = _ie.startTestSectionMillisec;
	ie.endTestSectionMillisec   = _ie.endTestSectionMillisec;

	if (_errorCodesDiffer == true)
	{
		LOG4CXX_INFO (_logger, "EXECUTOR returning FAILURE to the caller.");
		if (_precisionSet == true)
		{
			scidb::ArrayWriter::setPrecision(_precisionDefaultValue);
			_precisionSet = false;
		}
		return ERROR_CODES_DIFFER;
	}

	LOG4CXX_INFO (_logger, "EXECUTOR returning SUCCESS to the caller.");
	if (_precisionSet == true)
	{
		scidb::ArrayWriter::setPrecision(_precisionDefaultValue);
		_precisionSet = false;
	}
	return SUCCESS;
}

} //END namespace scidbtestharness
} //END namespace executors
