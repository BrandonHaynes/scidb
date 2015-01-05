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
 * @file suite.cpp
 * @author girish_hilage@persistent.co.in
 */

# include <stdio.h>
# include <iostream>
# include <vector>
# include <log4cxx/logger.h>
# include <log4cxx/ndc.h>
# include <boost/filesystem/operations.hpp>
# include <boost/iostreams/detail/current_directory.hpp>

# include "Exceptions.h"
# include "helper.h"
# include "suite.h"
# include "manager.h"
# include "reporter.h"
# include "global.h"

# define LOGGER_TAG_SUITE  "[SUITE]"

using namespace std;
using namespace log4cxx;
using namespace scidbtestharness::Exceptions;
namespace harnessexceptions = scidbtestharness::Exceptions;
namespace bfs = boost :: filesystem;
namespace bid = boost :: iostreams :: detail;

namespace scidbtestharness
{
/* runs all subsuites including suiteid */
int Suite :: run (const string root_dir, string skiptestfname, const vector<string> &skip_tclist, const string regex_expr, RegexType regex_flag,
                  MANAGER &M, int no_parallel_testcases, int *testcases_total, int *testcases_skipped, REPORTER *rptr, int *suitesSkipped)
{
	int rv=SUCCESS;
	LogString saved_context;
	LOGGER_PUSH_NDCTAG (LOGGER_TAG_SUITE);

	try
	{
		LOG4CXX_INFO (_logger, "Running a suite [" << _suiteId << "] with [" << _subSuites.size () << "] subsuite(s) ... (including itself)");
		print_vector (_subSuites);

		/* if --skip-tests=<some_file_name> then collectSkippedTestCases() only once */
		if (skip_tclist.size() > 0 && strcasecmp(skiptestfname.c_str(), DEFAULT_SKIP_TEST_OPTION) != 0)
		{
# if 0
			bfs::path p (skiptestfname);

			string under_dir = DEFAULT_TEST_CASE_DIR;
			under_dir = under_dir + p.parent_path ().string ();
			skiptestfname = p.filename();
			_skiptcList.clear ();

			/* collect skipped testcases */
			if (collectSkippedTestCases (root_dir, under_dir, skiptestfname, _skiptcList) <= -1)
			{
				LOGGER_POP_NDCTAG;
				return FAILURE;
			}
# endif

			for (unsigned int i=0; i<skip_tclist.size(); i++)
			{
				_skiptcList.push_back (skip_tclist[i]);
			}

			int filteredoutsuites = filterSkippedTestSuites (_subSuites, _skiptcList);
			if (filteredoutsuites > 0)
			{
				if (_subSuites.size() == 0)
				{
					LOG4CXX_INFO (_logger, "After filtering there remains no suite to run...");
				}
				else
				{
					LOG4CXX_INFO (_logger, "After filtering, Running a suite [" << _suiteId << "] with [" << _subSuites.size ()
                                                                                                          << "] subsuite(s) ... (including itself)");
					print_vector (_subSuites);
				}
			}
			*suitesSkipped += filteredoutsuites;
		}

		sort (_subSuites.begin (), _subSuites.end ());
		for (unsigned int i=0; i<_subSuites.size (); i++)
		{
			int thissuite_total_tc=0, thissuite_runable_tc=0, thissuite_skipped_tc=0;

			_tcList.clear ();

			/* collect all the testcases for this subsuite */
			if (collectTestCases (root_dir, _subSuites[i], regex_expr, regex_flag, _tcList) == -1)
			{
				LOGGER_POP_NDCTAG;
				return FAILURE;
			}

			thissuite_total_tc = _tcList.size ();
			*testcases_total += thissuite_total_tc;

			/* if it is "yes" */
			if (strcasecmp (skiptestfname.c_str(), DEFAULT_SKIP_TEST_OPTION) == 0)
			{
				/* then use disable.tests as a file name */
				skiptestfname = DEFAULT_SKIP_TEST_FILE_NAME;

				_skiptcList.clear ();
				/* collect skipped testcases, ignore "file does not exist" error i.e. -2 */
				if (collectSkippedTestCases (root_dir, _subSuites[i], skiptestfname, _skiptcList) == -1)
				{
					LOGGER_POP_NDCTAG;
					return FAILURE;
				}
				skiptestfname = DEFAULT_SKIP_TEST_OPTION;
			}

			/* filter out skipped testcases */
			thissuite_skipped_tc = filterSkippedTestCases (_tcList, _skiptcList);
			*testcases_skipped += thissuite_skipped_tc;

			thissuite_runable_tc = _tcList.size ();
			LOG4CXX_INFO (_logger, "Running a suite [" << _subSuites[i] << "] : with total[" << thissuite_total_tc << "], runable[" << thissuite_runable_tc << "], skipped[" << thissuite_skipped_tc << "] test case(s)...");

			if (thissuite_runable_tc > 0)
			{
				M.createWorkgroup (no_parallel_testcases);
				sort (_tcList.begin (), _tcList.end ());
				if ((rv = M.runJob (_tcList, rptr)) == FAILURE)
					break;
			}
		}
	}

	catch (harnessexceptions :: ERROR &e)
	{
		LOGGER_POP_NDCTAG;
		throw;
	}
	LOGGER_POP_NDCTAG;
	return rv;
}

/* recursively collects only the names of the subdirectories (i.e. sub suites)
 * and also .suite file names mentioned on the commandline
 * and any directories (i.e. suite names) and .suite file names mentioned in this file
 * and puts it into the vector '_subSuites'
 */
int Suite :: collectSubSuites (string parentdir, string sid)
{
	LogString saved_context;
	LOGGER_PUSH_NDCTAG (LOGGER_TAG_SUITE);

	string converted_sid = converttopath (sid);
	string suite_dir_fullpath = parentdir + "/" + converted_sid;
	string suite_file_fullpath = parentdir + "/" + converted_sid + ".suite";

	/* check directory after 'sid' conversion
         * TODO:
         * this entire routine is confusing because some checks are done up here, and
         * some later, when they could have been handled in a more streamlined
         * way.
         * NOTE:
         * an important case this code is trying to get right is when a directory
         * contains a subdirectory and also a .suite file of matching name.
         * So it wants to check when the directory form doesn't exist or
         * should be ignored because its a directory and not a plain file.
         * In reality, I think both checks are superfluous and we just want to
         * see whether the suite form of the name is present.
         * EXAMPLE: the following files/directories exist:
         * foo/bar/
         * foo/bar.suite
         */
	if (!bfs::exists (suite_dir_fullpath) || !bfs::is_directory (suite_dir_fullpath))
	{
		/* check .suite file after 'sid' conversion */
		if (!bfs::exists (suite_file_fullpath))
		{
			LOG4CXX_WARN (_logger, "suiteid [" << sid << "] is not a .suite file path after conversion.");
			LOGGER_POP_NDCTAG;
			return FAILURE;
                }
                else if (!bfs::is_regular (suite_file_fullpath))
		{
			LOG4CXX_WARN (_logger, "suiteid [" << sid << "] A '.' is not allowed in a file/directory name under test case directory t/");
			LOGGER_POP_NDCTAG;
			return FAILURE;
		}
		else
			LOG4CXX_DEBUG (_logger, "suiteid [" << sid << "] is a valid .suite file path after conversion.");
	}
	else
		LOG4CXX_DEBUG (_logger, "suiteid [" << sid << "] is a valid directory path after conversion.");

	/* check if it is a directory */
	if (bfs::exists (suite_dir_fullpath) && bfs::is_directory (suite_dir_fullpath))
	{
		/* if there exists a directory t/abc/ and also the file t/abc.suite
         * and we specify --suite-id=abc then that will be ambiguous
         */
		if (bfs::exists (suite_file_fullpath) && bfs::is_regular (suite_file_fullpath))
		{
			LOGGER_POP_NDCTAG;
			throw ConfigError (FILE_LINE_FUNCTION, ERR_CONFIG_AMBIGUOUS_SUITEID);
		}

		_subSuites.push_back (suite_dir_fullpath);

                /* its a directory, examine each entry and decide whether to recurse */
		bfs::directory_iterator end_iter;
		for (bfs::directory_iterator dir_iter(suite_dir_fullpath); dir_iter != end_iter; dir_iter++)
		{
			bfs::path p (dir_iter->path ());

			/* ignore hidden files and directories (e.g. ".svn") */
			if (p.stem().string() == "")
			{
				continue;
			}

			/* if it is a non-empty directory then traverse recursively */
			if (bfs :: is_directory (p))
			{
				if (!bfs :: is_empty (p))
#if (BOOST_FS_VER==2)
					collectSubSuites (p.directory_string (), "");
#else
                                        // p.directory_string() does not exist for
                                        // boost 1.46 aka BOOST_FS_VER==3
                                        // using p.string() as the directory part
                                        // avoids collectSubSuites from prepending another '/'
					collectSubSuites (p.string (), "");
#endif
			}
		}
	}
	/* check if it is a .suite file */
	else
	{
		if (!bfs::exists (suite_file_fullpath) || !bfs::is_regular (suite_file_fullpath))
		{
			LOGGER_POP_NDCTAG;
			stringstream ss;
			ss << "Suite [" << suite_dir_fullpath << "] does not exist and also Suite [" << suite_file_fullpath << "] either does not exist or is not a regular file";
			throw SystemError (FILE_LINE_FUNCTION, ss.str());
		}

		ifstream f(suite_file_fullpath.c_str());
		if (!f.is_open ())
		{
			LOGGER_POP_NDCTAG;
			stringstream ss;
			ss << "Could not open suite file [" << suite_file_fullpath << "]";
			throw SystemError (FILE_LINE_FUNCTION, ss.str());
		}

		_subSuites.push_back (suite_file_fullpath);

		string line;
		/* read the file */
		while (!f.eof ())
		{
			getline (f, line);

			/* if blank line */
			if (line.empty())
				continue;

			{
				bfs::path p (suite_file_fullpath);
				string parent_dir = p.parent_path().string();
				string pathstring = converttopath (line);
				string linefullpath = parent_dir + "/" + pathstring;
				string suitefile = linefullpath + ".suite";
				string testfile = linefullpath + ".test";

				/* if it is a directory then it must be a subsuite-id */
				if (bfs::is_directory (linefullpath))
				{
					/* if there is a line "abc" in the .suite file and
                     * there exists a directory abc/ as well as file abc.suite
                     * then it will be ambiguous
                     */
					if (bfs::exists (suitefile) && bfs::is_regular (suitefile))
					{
						LOGGER_POP_NDCTAG;
						stringstream ss;
						ss << "Ambiguous mention of suite id [" << line << "] in the file " << suite_file_fullpath;
						throw SystemError (FILE_LINE_FUNCTION, ss.str());
					}
					/* if there is a line "abc" in the .suite file and
                     * there exists a directory "abc" as well as file abc.test
                     * then it will be ambiguous
                     */
					else if (bfs::exists (testfile) && bfs::is_regular (testfile))
					{
						LOGGER_POP_NDCTAG;
						stringstream ss;
						ss << "Ambiguous mention of test/suite id [" << line << "] in the file " << suite_file_fullpath;
						throw SystemError (FILE_LINE_FUNCTION, ss.str());
					}

					if (!bfs::is_empty (linefullpath))
						collectSubSuites ("", linefullpath);
				}
				else
				{
					/* if it is a regular file with ".suite" extension */
					if (bfs::exists (suitefile) && bfs :: is_regular (suitefile))
					{
						/* if there is a line "abc" in the .suite file and
						 * there exists a file abc.suite as well as file abc.test
						 * then it will be ambiguous
						 */
						if (bfs::exists (testfile) && bfs::is_regular (testfile))
						{
							LOGGER_POP_NDCTAG;
							stringstream ss;
							ss << "Ambiguous mention of test/suite id [" << line << "] in the file " << suite_file_fullpath;
							throw SystemError (FILE_LINE_FUNCTION, ss.str());
						}

						collectSubSuites ("", linefullpath); /* .suite extension is being attached at the beginning of this function */
					}

					/* if it is a regular file with ".test" extension
					 * then ignore it as it will be collected in collectTestCases()
					 */
				}
			}
		} //END while(!eof)
	} //END else .suite file
	
	LOGGER_POP_NDCTAG;
	return _subSuites.size ();
}
} //END namespace scidbtestharness
