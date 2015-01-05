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
 * @file harness.cpp
 * @author girish_hilage@persistent.co.in
 */

# include <string>
# include <sstream>
# include <iostream>
# include <fstream>
# include <fcntl.h>
# include <log4cxx/patternlayout.h>
# include <log4cxx/consoleappender.h>
# include <log4cxx/fileappender.h>
# include <log4cxx/propertyconfigurator.h>
# include <log4cxx/logger.h>
# include <log4cxx/helpers/exception.h>
# include <log4cxx/ndc.h>
# include <boost/filesystem/operations.hpp>
# include <boost/program_options/options_description.hpp>
# include <boost/program_options/variables_map.hpp>
# include <boost/program_options/parsers.hpp>
# include <boost/iostreams/detail/current_directory.hpp>
# include <boost/version.hpp>

# include "global.h"
# include "helper.h"
# include "harness.h"
# include "suite.h"
# include "manager.h"
# include "Exceptions.h"
# include "system/Constants.h"
# include "util/PluginManager.h"

# define LOGGER_TAG_HARNESS  "[HARNESS]"

# define DEFAULT_STYLE_FILENAME         "XSLTFile.xsl"
# define DELIMITERS                     ", "
# define MIN_PARALLEL_TESTCASES         1
# define MAX_PARALLEL_TESTCASES         50

# define CHECK_REGEX_ARG \
{ \
	if (!_c.regexExpr.empty ()) \
	{ \
		throw ConfigError (FILE_LINE_FUNCTION, ERR_CONFIG_REGEX_MUTUALLY_EXCLUSIVE);\
	} \
}

using namespace std;
using namespace log4cxx;
using namespace scidbtestharness;
using namespace scidbtestharness::Exceptions;
namespace harnessexceptions = scidbtestharness::Exceptions;
namespace po = boost::program_options;
namespace bfs = boost :: filesystem;
namespace bid = boost :: iostreams :: detail;

namespace scidbtestharness
{

int SciDBTestHarness :: runSuites (const vector<string> &skip_tclist)
{
	int rv=SUCCESS;
	remove_duplicates (_c.suiteId);

	LOG4CXX_INFO (_logger, "There are " << _c.suiteId.size () << " suite(s) to be run.");

	/* for all suite ids specified on the commandline */
	for (unsigned int i=0; i<_c.suiteId.size (); i++)
	{
		int local_testcases_total=0, local_nSkipped=0, suitesSkipped=0;
		Suite S(_c.suiteId[i]);

		/* collect sub-suites under suiteId[i] recursively in the form of fully qualified path names */
		if (S.collectSubSuites (_c.rootDir, _c.suiteId[i]) == FAILURE)
		{
			LOG4CXX_DEBUG (_logger, "Continuing to next suite...");
			continue;
		}

		/* Suite START */
		rv = S.run (_c.rootDir, _c.skipTestfname, skip_tclist, _c.regexExpr, _c.regexFlag, _M,
                    _c.parallelTestCases, &local_testcases_total, &local_nSkipped, _rptr, &suitesSkipped);

		_harnessES.testcasesTotal += local_testcases_total;
		_harnessES.testcasesSkipped += local_nSkipped;
		_harnessES.testsuitesSkipped += suitesSkipped;
		if (rv == FAILURE)
			break;
	}

	return rv;
}

int SciDBTestHarness :: runTests (const vector<string> &skip_tclist)
{
	int rv=SUCCESS;

	if (remove_duplicates (_c.testId) > 0)
	{
		if (collectTestCases (_c.rootDir, _c.testId, _c.regexExpr, _c.regexFlag, _tcList) == FAILURE)
			return FAILURE;
	}

	if (remove_duplicates (_c.testName) > 0)
	{
		if (collectTestCases (_c.rootDir, _c.testName, _c.regexExpr, _c.regexFlag, _tcList, DEFAULT_TEST_CASE_DIR, TESTCASE_NAMES) == FAILURE)
			return FAILURE;
	}

	if (remove_duplicates (_tcList) > 0)
	{
		_harnessES.testcasesTotal += _tcList.size ();
		_harnessES.testcasesSkipped += filterSkippedTestCases (_tcList, skip_tclist);

		if (_tcList.size () > 0)
		{
			LOG4CXX_INFO (_logger, "Running (" << _tcList.size () << ") Individual Test(s) ...:");
			_M.createWorkgroup (_c.parallelTestCases);
			rv = _M.runJob (_tcList, _rptr);
		}
		else
			LOG4CXX_INFO (_logger, "After filtering there remain no test cases to run...");
	}
	else
		LOG4CXX_INFO (_logger, "There are no Individual Tests to run.");

	return rv;
}

int SciDBTestHarness :: execute (int mode)
{
	int rv=SUCCESS;
	int rv_skip=0;
	try
	{
		vector<string> skip_tclist;
		_rptr->writeInitialInfo (_c);

		/* if --skip-tests=<some_file_name> then collectSkippedTestCases() only once */
		if (strcasecmp(_c.skipTestfname.c_str(), "yes") == 0)
			_c.skipTestfname = DEFAULT_SKIP_TEST_FILE_NAME;
		string skiptestfname = _c.skipTestfname;

		if ((skiptestfname.length() > 0)                                       &&
            (strcasecmp(skiptestfname.c_str(), DEFAULT_SKIP_TEST_OPTION) != 0) &&
		    (strcasecmp(skiptestfname.c_str(), "no") != 0))
		{
			LOG4CXX_INFO (_logger, "Picking up test cases/suites to be skipped from the file [" << skiptestfname << "]");
			bfs::path p (skiptestfname);

			string under_dir = DEFAULT_TEST_CASE_DIR;
			if (!bfs :: is_regular (p))
			{
			under_dir = under_dir + p.parent_path ().string ();
#if (BOOST_FS_VER==2)
			skiptestfname = p.filename();
#else
			skiptestfname = p.filename().string();
#endif
            }
            else // Full path to disable tests file was specified.
            {
                under_dir = _c.rootDir + "/" + DEFAULT_TEST_CASE_DIR;
            }

			/* collect skipped testcases */
			rv_skip = collectSkippedTestCases (_c.rootDir, under_dir, skiptestfname, skip_tclist); 
			if ( (rv_skip != -2) && (rv_skip <= -1) )
//			if (collectSkippedTestCases (_c.rootDir, under_dir, skiptestfname, skip_tclist) <= -1)
			{
				_M.cleanup ();
				LOG4CXX_INFO (_logger, "Returning from execute()");
				log4cxx::NDC :: pop();
				log4cxx::NDC :: remove();
				_rptr->writeFinalInfo (_harnessES);
				return FAILURE;
			}

			LOG4CXX_INFO (_logger, "Picked up below " << skip_tclist.size() << " test cases/suites to be skipped from the file [" << skiptestfname << "]");
			print_vector (skip_tclist);
		}

		if ((rv = runTests (skip_tclist)) == SUCCESS)
		{
			rv = runSuites (skip_tclist);
		}

		_M.cleanup ();

		struct ExecutionStats tmp_es = _M.getExecutionStats ();
		_harnessES.testcasesPassed = tmp_es.testcasesPassed;
		_harnessES.testcasesFailed = tmp_es.testcasesFailed;
		print_execution_stats (_harnessES);
		_rptr->writeFinalInfo (_harnessES);

		LOG4CXX_INFO (_logger, "Returning from execute()");
	}

	catch (harnessexceptions :: ERROR &e)
	{
		PRINT_ERROR (e.what ());
		_M.cleanup ();
		_rptr->writeFinalInfo (_harnessES);
		LOG4CXX_INFO (_logger, "Returning from execute()");
		log4cxx::NDC :: pop();
		log4cxx::NDC :: remove();
		return FAILURE;
	}

	catch (exception &e)
	{
		PRINT_ERROR (e.what ());
		_M.cleanup ();
		_rptr->writeFinalInfo (_harnessES);
		LOG4CXX_INFO (_logger, "Returning from execute()");
		log4cxx::NDC :: pop();
		log4cxx::NDC :: remove();
		return FAILURE;
	}

	log4cxx::NDC :: pop();
	log4cxx::NDC :: remove();
	return rv;
}

void SciDBTestHarness :: printConf (void)
{
	LOG4CXX_INFO (_logger, "Printing Harness CommandLine options :");
    LOG4CXX_INFO (_logger, "SciDB Server =                                " << _c.scidbServer);
    LOG4CXX_INFO (_logger, "SciDB Port =                                  " << _c.scidbPort);
    LOG4CXX_INFO (_logger, "SciDB Root Dir =                              " << _c.rootDir);
	for (unsigned int i=0; i<_c.testId.size (); i++)
	{
		LOG4CXX_INFO (_logger, "Test-Id =                                 " << _c.testId[i]);
	}
	for (unsigned int i=0; i<_c.testName.size (); i++)
	{
		LOG4CXX_INFO (_logger, "Test-Name =                               " << _c.testName[i]);
	}
	for (unsigned int i=0; i<_c.suiteId.size (); i++)
	{
		LOG4CXX_INFO (_logger, "Suite-Id =                                " << _c.suiteId[i]);
	}
    LOG4CXX_INFO (_logger, "Name of the file containing disabled test ids = " << _c.skipTestfname);
    LOG4CXX_INFO (_logger, "Sleep Time =                                  " << _c.sleepTime);
    LOG4CXX_INFO (_logger, "Log queries =                                 " << _c.log_queries);
    LOG4CXX_INFO (_logger, "Save Failures =                               " << _c.save_failures);
    LOG4CXX_INFO (_logger, "Log Directory =                               " << _c.logDir);
    LOG4CXX_INFO (_logger, "Log Destination =                             " << _c.logDestination);
    LOG4CXX_INFO (_logger, "Report File Name  =                           " << _c.reportFilename);
    LOG4CXX_INFO (_logger, "Number of test cases to be run in Parallel =  " << _c.parallelTestCases);
    LOG4CXX_INFO (_logger, "DebugLevel  =                                 " << _c.debugLevel);

	/* print this only in normal cases and not during selftesting */
	if (_c.selfTesting == false)
		LOG4CXX_INFO (_logger, "Record =                                  " << _c.record);

    LOG4CXX_INFO (_logger, "KeepPreviousRun =                             " << _c.keepPreviousRun);
    LOG4CXX_INFO (_logger, "TerminateOnFailure =                          " << _c.terminateOnFailure);
}

int SciDBTestHarness :: createLogger (void)
{
	_logger = log4cxx :: Logger :: getLogger (HARNESS_LOGGER_NAME);
	_logger->setAdditivity (0);

	string pattern_string;

	/* if not a selftesting then print date */
	if (_c.selfTesting == false)
		pattern_string = "%d %p %x - %m%n";
	else
		pattern_string = "%p %x - %m%n";

	log4cxx :: LayoutPtr layout (new log4cxx :: PatternLayout (pattern_string));

	if (strcasecmp (_c.logDestination.c_str (), LOGDESTINATION_CONSOLE) == 0)
	{
		log4cxx :: ConsoleAppenderPtr appender (new log4cxx :: ConsoleAppender(layout));
		_logger->addAppender (appender);
	}
	else
	{
		log4cxx :: FileAppenderPtr appender (new log4cxx :: FileAppender (layout, _c.harnessLogFile, true));
		_logger->addAppender (appender);
	}

	NDC::push (LOGGER_TAG_HARNESS);
	_loggerEnabled = true;
	LOG4CXX_INFO (_logger, "logger SYSTEM ENABLED");

	switch (_c.debugLevel)
	{
		case DEBUGLEVEL_FATAL : _logger->setLevel (log4cxx :: Level :: getFatal ()); break;
		case DEBUGLEVEL_ERROR : _logger->setLevel (log4cxx :: Level :: getError ()); break;
		case DEBUGLEVEL_WARN  : _logger->setLevel (log4cxx :: Level :: getWarn ()); break;
		case DEBUGLEVEL_INFO  : _logger->setLevel (log4cxx :: Level :: getInfo ()); break;
		case DEBUGLEVEL_DEBUG : _logger->setLevel (log4cxx :: Level :: getDebug ()); break;
		case DEBUGLEVEL_TRACE : _logger->setLevel (log4cxx :: Level :: getTrace ()); break;
		default               : return FAILURE;
	}

	/* reading log4j.properties file */
	PropertyConfigurator::configure (_c.log_prop_file);

	log4cxx :: LoggerPtr scidbcapi_logger = log4cxx :: Logger :: getLogger ("root");
	log4cxx :: FileAppenderPtr scidbcapi_currentappender = scidbcapi_logger->getRootLogger ()->getAppender (SCIDBCAPI_LOGGER_NAME);

	if (scidbcapi_currentappender)
	{
		LOG4CXX_INFO (_logger, "Found Appender \"" << scidbcapi_currentappender->getName() << "\" under root logger.");

		string scidblogfile = scidbcapi_currentappender->getFile ();

		/* PropertyConfigurator::configure() internally creates scidb.log file under the current directory.
         * Hence remove it.
         * We are creating scidb.log under <root-dir>/log/
         */
		bfs::remove (scidblogfile.c_str ());
		scidblogfile = _c.logDir + "/" + "harness_connection.log";

		/* also remove log/scidb.log. log4cxx will internally create it */
		bfs::remove (scidblogfile.c_str ());

		LOG4CXX_INFO (_logger, "Setting SciDB log file to " << scidblogfile);
		scidbcapi_currentappender->setFile (scidblogfile);
		log4cxx::helpers::Pool pool;
		scidbcapi_currentappender->activateOptions (pool);
	}
	else
	{
		LOG4CXX_INFO (_logger, "Could not find Appender \"" << SCIDBCAPI_LOGGER_NAME << "\" under root logger.");
		LOG4CXX_INFO (_logger, "Exiting...");
		return FAILURE;
	}

	return SUCCESS;
}

void SciDBTestHarness :: resultDirCleanup (const string resultdir)
{
	if (bfs :: is_directory (resultdir.c_str ()))
	{
		bfs::directory_iterator end_iter;
		for (bfs::directory_iterator dir_iter(resultdir); dir_iter != end_iter; dir_iter++)
		{
			bfs::path p (dir_iter->path ());

			/* if it is a ".expected" file */
			if (bfs :: is_regular (p) && p.extension () == ".expected")
				continue;
			else if (bfs :: is_directory (p))
#if (BOOST_FS_VER==2)
				resultDirCleanup (p.directory_string ());
#else
				resultDirCleanup (p.string ());
#endif
			else
			{
#if (BOOST_FS_VER==2)
				string abs_fname = getAbsolutePath (p.directory_string ());
#else
				string abs_fname = getAbsolutePath (p.string ());
#endif
				bfs::remove (abs_fname);
			}
		}
	}
}

/*
 * Deletes all files except .expected under r/, and everything under log/, directory
 * and files Report.xml, .serverout which were created in last run
 */
void SciDBTestHarness :: cleanUpLog (const string rootdir, const string logdir, const string reportfile)
{
	/* remove all files except .expected files from under "r/" recursively */
	string result_dir = rootdir + "/" + DEFAULT_RESULT_DIR;
	resultDirCleanup (result_dir);

	/* remove "log/" and create it again */
	{
		bfs::path p (logdir);
		if (bfs :: is_directory (p))
			remove_all (p);

		create_directory (p);
	}

	/* remove Report.xml */
	{
		bfs::path p (reportfile);
		if (bfs :: is_regular (p))
			remove (p);
	}
}

int SciDBTestHarness :: validateParameters (void)
{
	if (_c.scidbServer.empty ())
		throw ConfigError (FILE_LINE_FUNCTION, ERR_CONFIG_SCIDBCONNECTIONSTRING_EMPTY);

	if (_c.scidbPort < 1)
		throw ConfigError (FILE_LINE_FUNCTION, ERR_CONFIG_SCIDBPORT_INVALID);

	_c.rootDir = getAbsolutePath (_c.rootDir, false);
	_c.scratchDir = getAbsolutePath (_c.scratchDir, false);
	
	if (_c.rootDir.empty ())
		throw ConfigError (FILE_LINE_FUNCTION, ERR_CONFIG_SCIDBROOTDIR_EMPTY);
		
	if (_c.scratchDir.empty ())
	{
	  _c.scratchDir = _c.rootDir;
	}

	for (unsigned int i=0; i<_c.testName.size (); i++)
	{
#if (BOOST_FS_VER==2)
		string file_extension = (bfs::path (_c.testName[i])).extension();
#else
		string file_extension = (bfs::path (_c.testName[i])).extension().string();
#endif
		if (file_extension != DEFAULT_TESTCASE_FILE_EXTENSION)
		{
			stringstream ss;
			ss << "Test name " << _c.testName[i] << " must have a " << DEFAULT_TESTCASE_FILE_EXTENSION << " extension.";
			throw SystemError (FILE_LINE_FUNCTION, ss.str());
		}
	}

	/* test case directory "<root-dir>/t/" must exist */
	string default_suiteId = _c.rootDir + "/" + DEFAULT_SUITE_ID;
	if (!bfs::is_directory (default_suiteId))
	{
		stringstream ss;
		ss << "Test case directory " << default_suiteId << " either does not exist or is not a directory.";
		throw SystemError (FILE_LINE_FUNCTION, ss.str());
	}

	if (strcasecmp(_c.skipTestfname.c_str(), "no") == 0)
		_c.skipTestfname = "";

	if (_c.sleepTime < 0)
		throw ConfigError (FILE_LINE_FUNCTION, ERR_CONFIG_INVALID_SLEEPVALUE);

	/* if user has mentioned some different log directory then consider it
     * otherwise it will be <ROOT_DIR/log>
     */
	if (_c.logDir != DEFAULT_LOG_DIR)
		_c.logDir = getAbsolutePath (_c.logDir, false);
	else
		_c.logDir = _c.rootDir + "/" + _c.logDir;

	if (!bfs::is_directory (_c.logDir))
	{
		stringstream ss;
		ss << "Log directory " << _c.logDir << " either does not exist or is not a directory.";
		throw SystemError (FILE_LINE_FUNCTION, ss.str());
	}

	_c.harnessLogFile = _c.logDir + "/" + DEFAULT_HARNESSLOGFILE;
	if (creat (_c.harnessLogFile.c_str (), S_IRUSR|S_IWUSR|S_IRGRP|S_IROTH) == -1)
	{
		stringstream ss;
		ss << "Failed to create a file " << _c.harnessLogFile;
		throw SystemError (FILE_LINE_FUNCTION, ss.str());
	}

	if ((strcasecmp (_c.logDestination.c_str (), LOGDESTINATION_CONSOLE) != 0) &&
	    (strcasecmp (_c.logDestination.c_str (), LOGDESTINATION_FILE) != 0))
	{
		throw ConfigError (FILE_LINE_FUNCTION, ERR_CONFIG_INVALID_LOGDESTINATION);
	}

	/* check if log4j.properties file exists */
	string log4j_properties_file = LOGGER_PROPERTIES_FILE;
	if (strcasecmp(_c.log_prop_file.c_str(),"none") != 0)
		log4j_properties_file = _c.log_prop_file.c_str();
	if (!bfs :: exists (log4j_properties_file))
	{
		stringstream ss;
		ss << "log4j.properties file '" << log4j_properties_file << "'does not exist.";
                ss << " Please check --log-properties-file option of scidbtestharness for a valid path to a log4j.properties file.";
		throw SystemError (FILE_LINE_FUNCTION, ss.str());
	}
	_c.log_prop_file = log4j_properties_file;

	/* check if style file exists */
	string stylefile = _c.rootDir + "/" + DEFAULT_STYLE_FILENAME;
	if (!bfs :: exists (stylefile))
	{
		stringstream ss;
		ss << "Style sheet file " << stylefile << " must exist.";
		throw SystemError (FILE_LINE_FUNCTION, ss.str());
	}

	_c.reportFilename = _c.scratchDir + "/" + _c.reportFilename;
	if (creat (_c.reportFilename.c_str (), S_IRUSR|S_IWUSR|S_IRGRP|S_IROTH) == -1)
	{
		stringstream ss;
		ss << "Failed to create report file " << _c.reportFilename;
		throw SystemError (FILE_LINE_FUNCTION, ss.str());
	}

	if (_c.parallelTestCases < MIN_PARALLEL_TESTCASES || _c.parallelTestCases > MAX_PARALLEL_TESTCASES)
	{
		stringstream ss;
		ss << "Invalid value specified for option --parallel. Valid range is [" << MIN_PARALLEL_TESTCASES << "-" << MAX_PARALLEL_TESTCASES << "]";
		throw ConfigError (FILE_LINE_FUNCTION, ss.str());
	}

	if (_c.debugLevel < MIN_DEBUG_LEVEL || _c.debugLevel > MAX_DEBUG_LEVEL)
	{
		stringstream ss;
		ss << "Invalid value specified for option --debug. Valid range is [" << MIN_DEBUG_LEVEL << "-" << MAX_DEBUG_LEVEL << "]";
		throw ConfigError (FILE_LINE_FUNCTION, ss.str());
	}

	return SUCCESS;
}

int SciDBTestHarness :: parseCommandLine (unsigned int argc, char** argv)
{
	po::options_description desc(
			"Usage: scidbtestharness [--connect <value>] [--port <value>] [--root-dir <value>] "
			"[--test-id <value>] [--test-list <value>] [--test-name <value>] [--suite-id <value>] [--skip-tests <yes/no/value>] "
			"[--include-regex-id <regex_expression>] [--exclude-regex-id <regex_expression>] "
			"[--include-regex-name <regex_expression>] [--exclude-regex-name <regex_expression>] "
			"[--sleep <value>] [--log-queries] [--log-dir <value>] [--log-destination <value>] [--log-properties-file <value>] [--report-file <value>] [--parallel <value>] [--scratch-dir <value>]"
			"[--debug <value>] [--record] [--keep-previous-run] [--save-failures] [--terminate-on-failure] [--cleanup] [--version]\n"
			);

	desc.add_options()
		("connect",              po::value<string>(), "Host of one of the cluster instances. Default is 'localhost'.")
		("port",                 po::value<int>(),    "Port for connection. Default is 1239.")
		("root-dir",             po::value<string>(), "Root directory in which test cases are kept. Default is Current directory.")
		("test-id",              po::value<string>(), "Test Id.")
		("test-name",            po::value<string>(), "Test Case name mentioned with .test extension.")
		("test-list",            po::value<string>(), "File with list of test ids.")
		("suite-id",             po::value<string>(), "Suite Id. If neither of test-id, test-name, suite-id is mentioned then Default suite-id is \"t\" under the root-dir. Suite-id could be either the directory path specified in the dot form or path of the .suite file specified in the dot form without .suite extension.")
		("skip-tests",           po::value<string>(), "yes/no/file-name. yes: skip tests specified in all the disable.tests files, no: do not skip any test, file-name: skip tests/suites mentioned in this file. Default is \"yes\".")
		("include-regex-id",     po::value<string>(), "regex expression specifying test ids to be included that match the given expression.")
		("exclude-regex-id",     po::value<string>(), "regex expression specifying test ids to be excluded that match the given expression.")
		("include-regex-name",   po::value<string>(), "regex expression specifying test names to be included that match the given expression.")
		("exclude-regex-name",   po::value<string>(), "regex expression specifying test names to be excluded that match the given expression.")
		("sleep",                po::value<int>(),    "Execution is paused after each statement in the test case.")
		("log-queries",                               "Log queries in the test case output.")
		("log-dir",              po::value<string>(), "Path to the directory where log files are kept.")
		("scratch-dir",          po::value<string>(), "Path to the scratch directory where .out, .diff, .log, and other temporaries will be stored.")
		("log-destination",      po::value<string>(), "Indicates where to log the messages. Valid values are \"console\" or \"file\". Default is \"file\".")
		("log-properties-file",	 po::value<string>(), "Path of log4j.properties file.")
		("report-file",          po::value<string>(), "Name of the file in which output report will be stored in an XML format under the root-dir. Default is \"Report.xml\".")
		("parallel",             po::value<int>(),    "Number of test cases to be executed in parallel.")
		("debug",                po::value<int>(),    "Log level can be in the range [0-5]. Level 0 only logs fatal errors while level 5 is most verbose. Default is 3.")
		("record",                                    "Record test case output.")
		("keep-previous-run",                         "Keeps the backup of output files produced by a previous run with the extension .bak. By default harness will clear all the previous log files, result files, output files etc.")
		("save-failures",                             "Save output file, log file and diff file with timestamp")
		("terminate-on-failure",                      "Stop running the harness when a test case fails. By default it will continue to run.")
		("cleanup",                                   "Does a cleanup and exit. Removes Report.xml and also everything under r/ and log/ directories generated in previous run.")
		("help,h", "View this text.")
                ("plugins,p",            po::value<string>(), "Plugins folder.")
                ("version", "version");

    po::variables_map vm;
    try
    {
        po::store (po::parse_command_line (argc, argv, desc), vm);
    }
    catch (const boost::program_options::multiple_occurrences& e)
    {
#if BOOST_VERSION - BOOST_VERSION % 100 > 1 * 100000 + 41 * (100 % 1000)
        //get_option_name() appeared in boost 1.42
        cerr << "Error during command line parsing: " << e.what() << " of option " << e.get_option_name() << endl;
#else
        cerr << "Error during command line parsing: " << e.what() << endl;
#endif
        return FAILURE;
    }
    catch (const boost::program_options::error &e)
    {
        cerr << "Error during command line parsing: " << e.what() << endl;
        return FAILURE;
    }
    po::notify (vm);

	scidb::PluginManager::getInstance()->setPluginsDirectory(
	    string(scidb::SCIDB_INSTALL_PREFIX()) + string("/lib/scidb/plugins"));

# if 0
	if ((vm.size () + 1) != argc)
	{
		cerr << "Invalid parameter on the command line\n";
		cerr << "Use --help for help\n";
		exit (0);
	}
# endif

	_c.log_prop_file = "none";
	if (!vm.empty ())
	{
		if (vm.count ("help"))
		{
			cerr << desc << endl;
			exit (0);
		}

		if (vm.count ("connect"))
			_c.scidbServer = vm["connect"].as<string>();

		if (vm.count ("port"))
			_c.scidbPort = vm["port"].as<int>();

		if (vm.count ("root-dir"))
			_c.rootDir = vm["root-dir"].as<string>();
			
		if (vm.count ("scratch-dir"))
		{
		  _c.scratchDir = vm["scratch-dir"].as<string>();
		}

        if (vm.count("plugins")) {
            scidb::PluginManager::getInstance()->setPluginsDirectory(vm["plugins"].as<string>());
        }

		if (vm.count ("test-id"))
		{
			_c.suiteId.clear ();
			tokenize (vm["test-id"].as<string>(), _c.testId, DELIMITERS);
		}

		if (vm.count ("test-list"))
		{
			_c.suiteId.clear ();
            vector<string> lists;
			tokenize (vm["test-list"].as<string>(), lists, DELIMITERS);
            for (size_t i = 0; i < lists.size(); i++) { 
            	ifstream list(lists[i].c_str());
                string testId;
                while (!list.eof()) { 
                    getline(list, testId);
                    _c.testId.push_back(testId);
                }
            }
		}

		if (vm.count ("test-name"))
		{
			_c.suiteId.clear ();
			tokenize (vm["test-name"].as<string>(), _c.testName, DELIMITERS);
		}

		if (vm.count ("suite-id"))
		{
			_c.suiteId.clear ();
			tokenize (vm["suite-id"].as<string>(), _c.suiteId, DELIMITERS);

			for (unsigned int i=0; i<_c.suiteId.size (); i++)
			{
				string tmp = vm["suite-id"].as<string>();

				/* if suite_id is not "t" and also
				 * does not start with "t." then prepend it with "t." */
				if (_c.suiteId[i] != "t" && _c.suiteId[i].find ("t.") != 0)
					_c.suiteId[i] = "t." + _c.suiteId[i];
			}
		}

		if (vm.count ("skip-tests"))
			_c.skipTestfname = vm["skip-tests"].as<string>();

		if (vm.count ("include-regex-id"))
		{
			CHECK_REGEX_ARG;
			_c.regexExpr = vm["include-regex-id"].as<string>();
			_c.regexFlag = REGEX_FLAG_INCLUDE_ID;
		}

		if (vm.count ("exclude-regex-id"))
		{
			CHECK_REGEX_ARG;
			_c.regexExpr = vm["exclude-regex-id"].as<string>();
			_c.regexFlag = REGEX_FLAG_EXCLUDE_ID;
		}

		if (vm.count ("include-regex-name"))
		{
			CHECK_REGEX_ARG;
			_c.regexExpr = vm["include-regex-name"].as<string>();
			_c.regexFlag = REGEX_FLAG_INCLUDE_NAME;
		}

		if (vm.count ("exclude-regex-name"))
		{
			CHECK_REGEX_ARG;
			_c.regexExpr = vm["exclude-regex-name"].as<string>();
			_c.regexFlag = REGEX_FLAG_EXCLUDE_NAME;
		}

		if (vm.count ("sleep"))
			_c.sleepTime = vm["sleep"].as<int>();

		if (vm.count ("log-queries"))
					_c.log_queries = true;

		if (vm.count ("save-failures"))
					_c.save_failures = true;

		if (vm.count ("log-dir"))
			_c.logDir = vm["log-dir"].as<string>();

		if (vm.count ("log-destination"))
			_c.logDestination = vm["log-destination"].as<string>();

		if (vm.count ("log-properties-file"))
			_c.log_prop_file = vm["log-properties-file"].as<string>();

		if (vm.count ("report-file"))
			_c.reportFilename = vm["report-file"].as<string>();

		if (vm.count ("parallel"))
			_c.parallelTestCases = vm["parallel"].as<int>();

		if (vm.count ("debug"))
			_c.debugLevel = vm["debug"].as<int>();

		if (vm.count ("record"))
			_c.record = true;

		if (vm.count ("keep-previous-run"))
			_c.keepPreviousRun = true;

		if (vm.count ("terminate-on-failure"))
			_c.terminateOnFailure = true;

		if (vm.count ("cleanup"))
			_c.cleanupLog = true;

		if (vm.count ("selftesting"))
			_c.selfTesting = true;

		if (vm.count("version")) {
            cout << "SciDB Test Harness Version: " << scidb::SCIDB_VERSION_PUBLIC() << endl
                 << "Build Type: "                 << scidb::SCIDB_BUILD_TYPE()     << endl
                 << scidb::SCIDB_COPYRIGHT()                                        << endl;
			exit (0);
    }
	}

	validateParameters ();

	if (_c.cleanupLog)
	{
		cleanUpLog (_c.rootDir, _c.logDir, _c.reportFilename);
		exit (0);
	}

	if (createLogger () == FAILURE)
		return FAILURE;

	printConf ();
	_M.useLogger (HARNESS_LOGGER_NAME);
	_M.getInfoForExecutorFromharness (_c, _executorType);
//	_M.create_workgroup (_c.parallelTestCases);
	createReporter ();

	return SUCCESS;
}

void SciDBTestHarness :: initConfDefault (void)
{
	_cwd = bid::current_directory ();
	_c.scidbServer        = DEFAULT_SCIDB_CONNECTION;
	_c.scidbPort          = DEFAULT_SCIDB_PORT;
	_c.rootDir            = _cwd;
	_c.suiteId.push_back (DEFAULT_SUITE_ID);
	_c.skipTestfname      = DEFAULT_SKIP_TEST_OPTION;
	_c.regexFlag         = REGEX_FLAG_NO_REGEX_EXPR;
	_c.sleepTime          = 0;
	_c.logDir             = DEFAULT_LOG_DIR;
	_c.logDestination     = LOGDESTINATION_FILE;
	_c.reportFilename     = DEFAULT_REPORT_FILENAME;
	_c.parallelTestCases  = DEFAULT_PARALLEL_TESTCASES;
	_c.debugLevel         = DEFAULT_DEBUGLEVEL;
	_c.harnessLogFile     = DEFAULT_HARNESSLOGFILE;
	_c.record             = false;
	_c.keepPreviousRun    = false;
	_c.terminateOnFailure = false;
	_c.cleanupLog         = false;
	_c.selfTesting        = false;
	_c.log_queries        = false;
	_c.save_failures      = false;
}

} //END namespace scidbtestharness
