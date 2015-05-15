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
 * @file harnesstestexecutor.cpp
 * @author girish_hilage@persistent.co.in
 */

# include <stdlib.h>
# include <unistd.h>
# include <iostream>
# include <string>
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
# include <boost/date_time/posix_time/posix_time_types.hpp>
# include <boost/thread/pthread/condition_variable.hpp>

# include "global.h"
# include "helper.h"
# include "errdb.h"
# include "Exceptions.h"
# include "harnesstestexecutor.h"

# define LOGGER_TAG_HARNESSEXECUTOR "HARNESSEXECUTOR"

using namespace std;
using namespace log4cxx;
using namespace scidbtestharness::Exceptions;
namespace bfs = boost :: filesystem;
namespace harnessexceptions = scidbtestharness::Exceptions;

namespace scidbtestharness
{
namespace executors
{

int HarnessTestExecutor :: executeTestCase (void)
{
	LOG4CXX_INFO (_logger, "Starting executing the test case ...");

	try
	{
		string command = "sh " + _shellscript;

		cout << "Executing " << command << endl;
		if (system (command.c_str ()) == -1)
			cout << "System command failed to execute\n";
	}

	catch (harnessexceptions :: ERROR &e)
	{
		throw;
	}

	LOG4CXX_INFO (_logger, "Done executing the test case ...");
	return SUCCESS;
}

int HarnessTestExecutor :: prepareShellscript (void)
{
	LOG4CXX_INFO (_logger, "Preparing test executable file : " << _ie.tcfile);
	assert (!_ie.tcfile.empty ());

	bfs::path p (_ie.tcfile);
	if (bfs :: is_empty (p))
	{
		stringstream ss;
		ss << "Test case File [" << _ie.tcfile << "] is empty";
		throw SystemError (FILE_LINE_FUNCTION, ss.str());
	}

	ifstream f(_ie.tcfile.c_str());
	if (!f.is_open ())
	{
		stringstream ss;
		ss << "Could not open file [" << _ie.tcfile << "]";
		throw SystemError (FILE_LINE_FUNCTION, ss.str());
	}

	_shellscript = _ie.tcfile + ".sh";
	ofstream fe(_shellscript.c_str());
	if (!fe.is_open ())
	{
		stringstream ss;
		ss << "Could not open file [" << _shellscript << "]";
		throw SystemError (FILE_LINE_FUNCTION, ss.str());
	}

	string line;
	try
	{
		while (!f.eof ())
		{
			std::getline (f, line, LINE_FEED);

			/* remove left and right hand side spaces.
			 * a line containing only the spaces will be reduced to a blank line */ 
			boost::trim (line);

			/* if blank line or a comment */
			if (line.empty() || boost::starts_with (line, "#"))
				continue;

			//cout << "line = " << line << endl;

			vector<string> tokens;
			tokenize (line, tokens, " ");

			assert (tokens.size () > 0);

			if (boost::iequals (tokens[0],"scidbtestharness"))
			{
				char buf[BUFSIZ];
				char* shouldBeBuf = getcwd (buf, BUFSIZ);
				if (shouldBeBuf!=buf) assert(false);
				string NewLine = buf;
				int option_record_found=0, option_logdest_found=0, option_selftesting_found=0;
				NewLine = NewLine + "/" + line;

				for (unsigned int i=0; i<tokens.size(); i++)
				{
					if (strcasecmp (tokens[i].c_str (), "--record") == 0)
						option_record_found = 1;
					else if (strcasecmp (tokens[i].c_str (), "--log-destination") == 0)
						option_logdest_found = 1;
					else if (strcasecmp (tokens[i].c_str (), "--selftesting") == 0)
						option_selftesting_found = 1;
				}

				/* add --record only if it is already not there as same option is not accepted twice.
                 * Need to give --record so that worker will convert .out to .expected */
				if (_ie.record && !option_record_found)
					NewLine = NewLine + " --record";
				if (!option_logdest_found)
					NewLine = NewLine + " --log-destination=console";

				/* it's always a selftesting */
				if (!option_selftesting_found)
					NewLine = NewLine + " --selftesting";

				NewLine = NewLine + " >& " + _ie.actual_rfile + "\n";
				fe.write (NewLine.c_str (), NewLine.size());
			}
			else
				fe.write (line.c_str (), line.size());
		} // END while(!EOF)
	}

	catch (harnessexceptions :: ERROR &e)
	{
		PRINT_ERROR (e.what ());
		f.close ();
		fe.close ();
		bfs::remove (_shellscript);
		return FAILURE;
	}

	f.close ();
	fe.close ();
	LOG4CXX_INFO (_logger, "Done Preparing test case executable...");
	return SUCCESS;
}

void HarnessTestExecutor :: printExecutorEnvironment (void)
{
	LOG4CXX_INFO (_logger, "Printing executor Environment : ");
	
	LOG4CXX_INFO (_logger, "_ie.tcfile : "           << _ie.tcfile);
    LOG4CXX_INFO (_logger, "_ie.connectionString : " << _ie.connectionString);
	LOG4CXX_INFO (_logger, "_ie.scidbPort : "        << _ie.scidbPort);
    LOG4CXX_INFO (_logger, "_ie.rootDir : "          << _ie.rootDir);
    LOG4CXX_INFO (_logger, "_ie.sleepTime : "        << _ie.sleepTime);
    LOG4CXX_INFO (_logger, "_ie.logDir : "           << _ie.logDir);
    LOG4CXX_INFO (_logger, "_ie.debugLevel : "       << _ie.debugLevel);
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

int HarnessTestExecutor :: createLogger (void)
{
	assert (_ie.log_file.length () > 0);
	bfs::remove (_ie.log_file);	

	log4cxx :: LayoutPtr layout (new log4cxx :: PatternLayout ("%d %p %x - %m%n"));
	log4cxx :: FileAppenderPtr appender (new log4cxx :: FileAppender (layout, _ie.log_file, true));
	_logger = log4cxx :: Logger :: getLogger (_ie.logger_name);
	_logger->addAppender (appender);

    _executorTag = LOGGER_TAG_HARNESSEXECUTOR;
    _executorTag += '[' + _ie.logger_name + ']';
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

int HarnessTestExecutor :: validateParameters (void)
{
	try
	{ 
		_ie.tcfile = getAbsolutePath (_ie.tcfile, false);
		if (_ie.tcfile.empty ())
			throw ConfigError (FILE_LINE_FUNCTION, ERR_CONFIG_TESTCASEFILENAME_EMPTY);
		if (!bfs::is_regular (_ie.tcfile))
		{
			stringstream ss;
			ss << "Test case file " << _ie.tcfile << " either does not exist or is not a regular file.";
			throw SystemError (FILE_LINE_FUNCTION, ss.str());
		}

		if (_ie.debugLevel < MIN_DEBUG_LEVEL || _ie.debugLevel > MAX_DEBUG_LEVEL)
		{
			stringstream ss;
			ss << "Invalid value specified for option --debug. Valid range is [" << MIN_DEBUG_LEVEL << "-" << MAX_DEBUG_LEVEL << "]";
			throw ConfigError (FILE_LINE_FUNCTION, ss.str());
		}
	}

	catch (harnessexceptions :: ConfigError &e)
	{
		PRINT_ERROR (e.what ());
		return FAILURE;
	}

	return SUCCESS;
}

void HarnessTestExecutor :: copyToLocal (const InfoForExecutor &ir)
{
	_ie.tcfile             = ir.tcfile;
	_ie.sleepTime          = ir.sleepTime;
	_ie.debugLevel         = ir.debugLevel;
	_ie.record             = ir.record;
	_ie.actual_rfile       = ir.actual_rfile;
	_ie.timerfile          = ir.timerfile;
	_ie.log_file           = ir.log_file;
	_ie.logger_name        = ir.logger_name;
}

int HarnessTestExecutor :: execute (InfoForExecutor &ir)
{
	time_t rawtime;
	time ( &rawtime );
	std::string nowStr(ctime (&rawtime));
	nowStr = nowStr.substr(0,nowStr.size()-1); // remove newline

	if (strcasecmp (ir.logDestination.c_str (), LOGDESTINATION_CONSOLE) != 0)
	{
		cout << "[" << ir.test_sequence_number << "][" << nowStr << "]: "
		     << "[start] " << ir.testID << std::endl;
	}

	copyToLocal (ir);

	if (validateParameters () == FAILURE)
		return FAILURE;

	createLogger ();
	//print_executor_environment ();

	if (prepareShellscript () == FAILURE)
	{
		LOG4CXX_INFO (_logger, "EXECUTOR returning FAILURE to the caller.");
		return FAILURE;
	}

	if (executeTestCase () == FAILURE)
	{
		LOG4CXX_INFO (_logger, "EXECUTOR returning FAILURE to the caller.");
		return FAILURE;
	}

	LOG4CXX_INFO (_logger, "EXECUTOR returning SUCCESS to the caller.");
	return SUCCESS;
}

} //END namespace scidbtestharness
} //END namespace executors
