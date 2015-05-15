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
 * @file helper.cpp
 * @author girish_hilage@persistent.co.in
 */

# include <sys/types.h>
# include <sys/stat.h>
# include <fcntl.h>
# include <stdlib.h>
# include <limits.h>
# include <stdio.h>
# include <errno.h>
# include <string.h>
# include <errno.h>
# include <iostream>
# include <vector>
# include <string>
# include <fstream>
# include <sstream>
# include <log4cxx/logger.h>
# include <log4cxx/ndc.h>
# include <boost/tokenizer.hpp>
# include <boost/filesystem/operations.hpp>
# include <boost/iostreams/detail/current_directory.hpp>
# include <boost/algorithm/string.hpp>
# include <boost/regex.hpp>
# include <sys/ioctl.h>
# include <sys/socket.h>

# include "errdb.h"
# include "Exceptions.h"
# include "helper.h"
# include "global.h"

# define LOGGER_TAG_HELPER  "[HELPER]"
# define DIFF_COMMAND       "/usr/bin/diff"

using namespace std;
using namespace log4cxx;
using namespace scidbtestharness::Exceptions;
namespace bfs = boost :: filesystem;
namespace bid = boost :: iostreams :: detail;
namespace harnessexceptions = scidbtestharness::Exceptions;

namespace scidbtestharness
{

int if_addr_fetch (struct ifconf *ifc, int fd)
{
	int numreqs = 5;
	int allocated_len = sizeof(struct ifreq) * numreqs;

	ifc->ifc_buf = NULL;
	for (;;)
	{
		ifc->ifc_len = allocated_len;
		ifc->ifc_buf = (char *)realloc(ifc->ifc_buf, ifc->ifc_len);

		if (ioctl (fd, SIOCGIFCONF, ifc) < 0)
		{
			char errbuf[BUFSIZ];
			strerror_r (errno, errbuf, sizeof (errbuf));

			stringstream ss;
			ss << "Got ioctl() Error [" << errbuf << "].";
			throw SystemError (FILE_LINE_FUNCTION, ss.str ());
		}

		if (ifc->ifc_len == allocated_len)
		{
			/* assume it overflowed and try again */
			numreqs += 1;
			continue;
		}
		break;
	}

	return ifc->ifc_len / (sizeof (struct ifreq));
}

int ReadOutputOf (const string &command, FILE **pipe, char *buf, int len, int *exit_code)
{
	assert (command.length () > 0);

	LogString saved_context;
	LOGGER_PUSH_NDCTAG (LOGGER_TAG_HELPER);
	log4cxx :: LoggerPtr logger = log4cxx :: Logger :: getLogger (HARNESS_LOGGER_NAME);

	if (pipe && *pipe == 0)
		LOG4CXX_INFO (logger, "Executing the shell command : " << command);

	int rbytes=FAILURE;
	char errbuf[BUFSIZ];

	try
	{
		if (pipe && *pipe == 0)
		{
			if (!(*pipe = popen (command.c_str(), "r")))
			{
				strerror_r (errno, errbuf, sizeof (errbuf));
				throw SystemError (FILE_LINE_FUNCTION, errbuf);
			}
		}

		if (pipe && *pipe && buf && len > 0)
		{
			int fd;

			if ((fd = fileno (*pipe)) == -1)
			{
				strerror_r (errno, errbuf, sizeof (errbuf));
				throw SystemError (FILE_LINE_FUNCTION, errbuf);
			}

			while ((rbytes = read (fd, buf, len)) > 0)
			{
				LOGGER_POP_NDCTAG;
				return rbytes;
			}
		}

		int status;
		if ((status = pclose (*pipe)) == -1)
		{
			strerror_r (errno, errbuf, sizeof (errbuf));
            throw SystemError (FILE_LINE_FUNCTION, errbuf);
		}

		if (WIFEXITED(status))
		{
			*exit_code = WEXITSTATUS(status);
			if (*exit_code == 0)
			{
				LOG4CXX_INFO (logger, "Shell command executed successfully.");
			}
			else
				LOG4CXX_INFO (logger, "Shell command failed to execute successfully. Exit code = " << *exit_code << ".");
		}
		else
			LOG4CXX_INFO (logger, "Shell command could not exit normally.");

		/* read() failure */
		if (buf && len > 0 && rbytes == -1)
			throw SystemError (FILE_LINE_FUNCTION, "Failure while reading the output from popen()");
	}

	catch (harnessexceptions :: ERROR &e)
    {
		LOG4CXX_ERROR (logger, e.what ());
		LOGGER_POP_NDCTAG;
        return FAILURE;
    }

	LOGGER_POP_NDCTAG;
    return rbytes;
}

int runShellCommand (const string &command)
{
	LogString saved_context;
	LOGGER_PUSH_NDCTAG (LOGGER_TAG_HELPER);
	log4cxx :: LoggerPtr logger = log4cxx :: Logger :: getLogger (HARNESS_LOGGER_NAME);

	int exit_code=-1;
	FILE *pipe=0;
	ReadOutputOf (command, &pipe, 0, 0, &exit_code);

	LOGGER_POP_NDCTAG;
	return exit_code;
}

/* will only tell if the files differ. It does not store the difference into a .diff file */
int manual_diff (const string &file1, const string &file2, const string &diff_file)
{
    FILE* file1_ptr=NULL;
    FILE* file2_ptr=NULL;

	LogString saved_context;
	LOGGER_PUSH_NDCTAG (LOGGER_TAG_HELPER);
	log4cxx :: LoggerPtr logger = log4cxx :: Logger :: getLogger (HARNESS_LOGGER_NAME);
	LOG4CXX_TRACE (logger, "Doing manual diff...");

	try
	{
		//bfs::remove (diff_file);	
		if ((file1_ptr = fopen (file1.c_str(), "rb")) == NULL)
		{
			throw SystemError (FILE_LINE_FUNCTION, string("error opening file " + file1).c_str());
		}

		if ((file2_ptr = fopen (file2.c_str(), "rb")) == NULL)
		{
			throw SystemError (FILE_LINE_FUNCTION, string("error opening file " + file2).c_str());
		}

		char char1, char2;
		do
		{
			char1 = fgetc (file1_ptr);
			char2 = fgetc (file2_ptr);
		}
		while ((char1 == char2) && (char1 != EOF) && ((char2 != EOF)));

		LOGGER_POP_NDCTAG;
		fclose (file1_ptr);
		fclose (file2_ptr);
		return (char1 == char2) ? DIFF_FILES_MATCH : DIFF_FILES_DIFFER;
	}

	catch (harnessexceptions :: ERROR &e)
	{
		fclose (file1_ptr);
		fclose (file2_ptr);
		LOGGER_POP_NDCTAG;
		throw;
	}

	LOGGER_POP_NDCTAG;
	return FAILURE;
}

int diff (const string &file1, const string &file2, const string &diff_file)
{
	LogString saved_context;
	LOGGER_PUSH_NDCTAG (LOGGER_TAG_HELPER);
	log4cxx :: LoggerPtr logger = log4cxx :: Logger :: getLogger (HARNESS_LOGGER_NAME);

	LOG4CXX_TRACE (logger, "Comparing files [" << file1 << "] & [" << file2 << "]");
	assert (file1.length() > 0 && file2.length() > 0 && diff_file.length() > 0);

	try
	{
		if (!bfs::is_regular_file (file1)) // Case when expected file does not exist
		{
			if (bfs::file_size(file2)) // Case when actual output file is Not empty
			{
				stringstream ss;
				ss << "File [" << file1 << "] either does not exist or is not a regular file.";
				throw SystemError (FILE_LINE_FUNCTION, ss.str());
			}
			else // Case when actual file is empty
			{    // Remove empty actual file and return PASS
				bfs::remove (file2);
				return DIFF_FILES_MATCH;
			}
		}

		if (!bfs::is_regular_file (file2))
		{
			stringstream ss;
			ss << "File [" << file2 << "] either does not exist or is not a regular file.";
			throw SystemError (FILE_LINE_FUNCTION, ss.str());
		}

		string command = DIFF_COMMAND;
		//command = command + " -au " + file1 + " " + file2 + " >& " + diff_file;
		command = command + " -au " + file1 + " " + file2 + " 1> " + diff_file + " 2>&1 ";
		//cout << "Executing command ((" << command << "))" << endl;

		switch (runShellCommand (command))
		{
			case 0 :
				LOGGER_POP_NDCTAG;
				bfs::remove (diff_file);	
				return DIFF_FILES_MATCH;

			/* if exit code is 1 and there is something written in .diff file then files differ */
			case 1 :
				{
					bfs::path p (diff_file);
					if (!bfs :: is_empty (p))
					{
						LOGGER_POP_NDCTAG;
						return DIFF_FILES_DIFFER;
					}
					/* should not reach here */
					assert (0);
				}

			/* fallthrough */
			case -1 :
			default :
				LOG4CXX_ERROR (logger, "diff() : runShellCommand() failed. Hence doing manual_diff() now. Check .diff file for error.\n");
				LOGGER_POP_NDCTAG;
				return manual_diff (file1, file2, diff_file);
		}
	}

	catch (harnessexceptions :: ERROR &e)
	{
		LOGGER_POP_NDCTAG;
		throw;
	}

	LOGGER_POP_NDCTAG;
	return FAILURE;
}

void print_execution_stats (const struct ExecutionStats &es)
{
	cout << "testcases_total = " << es.testcasesTotal << endl;
	cout << "testcases_passed = " << es.testcasesPassed << endl;
	cout << "testcases_failed = " << es.testcasesFailed << endl;
	cout << "testcases_skipped = " << es.testcasesSkipped << endl;
	cout << "testsuites_skipped = " << es.testsuitesSkipped << endl;
}

bool check_regex_match (const string fmt, const string str)
{
	if (str.empty ())
		return false;

    static const boost::regex e(fmt);
	return boost::regex_search(str, e);
}

int remove_duplicates (vector <string> &strcollection)
{
	if (strcollection.empty ())
		return 0;

	/* first sort all ids */
	sort (strcollection.begin (), strcollection.end ());

	/* remove duplicates */
	vector <string> :: iterator it = unique (strcollection.begin (), strcollection.end ());  
	strcollection.resize (it - strcollection.begin());

	return strcollection.size ();
}

string converttoid (string rootdir, string filename)
{
    if (filename.empty ())
        return "";

    if (!rootdir.empty () && filename.find ('/') == 0)
    {
        size_t found = filename.find (rootdir);
        if (found != string :: npos)
            filename.replace (0, rootdir.length (), "");
    }

    string :: size_type slash;
    while ((slash = filename.find ('/')) != string::npos)
        filename.replace (slash, 1, ".");

    // remove '.test' from the end
    filename = filename.substr (0, filename.find_last_of ("."));
    filename.replace (0, 1, "");

    return filename;
}

string converttopath (string suiteid)
{
	string :: size_type dot;
	boost::algorithm::trim(suiteid);
	while ((dot = suiteid.find ('.')) != string::npos)
		suiteid.replace (dot, 1, "/");

	return suiteid;
}

void print_vector (const vector<string> v)
{
	LogString saved_context;
	LOGGER_PUSH_NDCTAG (LOGGER_TAG_HELPER);
	log4cxx :: LoggerPtr logger = log4cxx :: Logger :: getLogger (HARNESS_LOGGER_NAME);

	for (unsigned int i=0; i<v.size(); i++)
	{
		LOG4CXX_INFO (logger, "v[" << i << "] : " << v[i]);
    }
	LOGGER_POP_NDCTAG;
}

/* remove testsuites present in both skip_tclist and suite_list from suite_list */
int filterSkippedTestSuites (vector <string> &suite_list, vector <string> &skip_tclist)
{
	int num_suites_filtered = 0;

	if (suite_list.size () == 0 || skip_tclist.size () == 0)
		return num_suites_filtered;

	LogString saved_context;
	LOGGER_PUSH_NDCTAG (LOGGER_TAG_HELPER);
	log4cxx :: LoggerPtr logger = log4cxx :: Logger :: getLogger (HARNESS_LOGGER_NAME);

	normalizePath (suite_list);
	normalizePath (skip_tclist);

	string tmp_value = "";
	for (unsigned int i=0; i<skip_tclist.size (); i++)
	{
		for (unsigned int j=0; j<suite_list.size (); )
		{
			tmp_value = suite_list[j] + "/";
			if (tmp_value.find (skip_tclist[i]) != 0)
			{
				j++;
				continue;
			}

			LOG4CXX_TRACE (logger, "Filtering suite : " << suite_list[j]);
			suite_list.erase (suite_list.begin() + j);
			num_suites_filtered++;
		}
	}

	LOGGER_POP_NDCTAG;
	return num_suites_filtered;
}

/* remove testcases present in both skip_tclist and tclist from tclist */
int filterSkippedTestCases (vector <string> &tclist, const vector <string> &skip_tclist)
{
	int num_tc_filtered = 0;

	if (tclist.size () == 0 || skip_tclist.size () == 0)
		return num_tc_filtered;

	LogString saved_context;
	LOGGER_PUSH_NDCTAG (LOGGER_TAG_HELPER);
	log4cxx :: LoggerPtr logger = log4cxx :: Logger :: getLogger (HARNESS_LOGGER_NAME);

	vector <string> :: iterator it;
	for (unsigned int i=0; i<skip_tclist.size (); )
	{
		it = find (tclist.begin (), tclist.end (), skip_tclist[i]);

		if (it != tclist.end ())
		{
			LOG4CXX_TRACE (logger, "Filtering test case : " << *it);
			tclist.erase (it);
			/* TODO : also erase skip_tclist[i] as it has already found and there is no use of it here onward */
			num_tc_filtered++;
		}
		else
			i++;
	}

	LOGGER_POP_NDCTAG;
	return num_tc_filtered;
}

/* collects skipped test cases and suites from file 'skiptestfname' or from 'under_directory/disable.tests'
 * and fills up the 'skip_tclist' with the fully qualified file paths */
int collectSkippedTestCases (string root_dir, string under_directory, string skiptestfname, vector <string> &skip_tclist)
{
	LogString saved_context;
	LOGGER_PUSH_NDCTAG (LOGGER_TAG_HELPER);
	log4cxx :: LoggerPtr logger = log4cxx :: Logger :: getLogger (HARNESS_LOGGER_NAME);

	if (root_dir.empty ())
		root_dir = bid::current_directory ();

	/* if it is a relative path then prepend it with 'root_dir' */
	if (under_directory.find ('/') != 0 && under_directory.find ('~') != 0)
		under_directory = root_dir + "/" + under_directory;

	{
	  // First, test if the file already exists (full absolute path passed in via --disable-tests switch)
		bfs::path p (skiptestfname);

		/* if exist, it should be a regular file */
		if (!bfs :: is_regular (p))
		{
		  if (under_directory[under_directory.length()-1] != '/')
		  {
		      skiptestfname = under_directory + "/" + skiptestfname;
		  }
		  else
		  {
		      skiptestfname = under_directory +  skiptestfname;
		  }
		  p = skiptestfname;
		  if (!bfs :: is_regular (p))
		    {
			LOG4CXX_ERROR (logger, "Skip Test file [" << skiptestfname << "] either does not exist or is not a regular file.");
			LOGGER_POP_NDCTAG;
			return -2;
		    }
		}
	}
         
	/* read file containing disabled tests */
	ifstream f(skiptestfname.c_str());
	string line;

	/* file exists but we could not open it */
	if (!f.is_open ())
	{
		LOGGER_POP_NDCTAG;
		stringstream ss;
		ss << "Could not open Skip Test file [" << skiptestfname << "]";
		throw SystemError (FILE_LINE_FUNCTION, ss.str());
	}

	LOG4CXX_INFO (logger, "Reading skiplist from file " << skiptestfname);
	
	while (!f.eof ())
	{
		getline (f, line);

		/* if blank line */
		if (line.empty() || boost::starts_with (line, "#"))
			continue;

		/* convert test id to test case file path */
		string converted_path = converttopath (line);
		string full_path = under_directory + "/" + converted_path;
		full_path = getAbsolutePath (full_path);

		/* check if it is a directory */
		if (full_path.length() > 0 && bfs::exists (full_path) && bfs::is_directory (full_path))
		{
			full_path = full_path + "/";
			skip_tclist.push_back (full_path);
		}
		else
		{
			/* check if it is a .test file */
			string testcasefile_fullpath = under_directory + "/" + converted_path + ".test";

			/* it should be a regular file */
			if (bfs :: exists (testcasefile_fullpath) && bfs :: is_regular (testcasefile_fullpath))
			{
				testcasefile_fullpath = getAbsolutePath (testcasefile_fullpath);

				/* ignore non existing files */
				if (!testcasefile_fullpath.empty ())
					skip_tclist.push_back (testcasefile_fullpath);
			}
		}
	}
	f.close ();

	if (skip_tclist.size ())
	{
		vector <string> :: iterator it;

		/* remove duplicates */
		sort (skip_tclist.begin (), skip_tclist.end ());
		it = unique (skip_tclist.begin (), skip_tclist.end ());  
		skip_tclist.resize (it - skip_tclist.begin());
	}

	LOGGER_POP_NDCTAG;
    return skip_tclist.size ();
}

/* if "dirORfile" is a directory then collects all .test files from under this directory 
 * if it is a .suite file name then collects all test ids mentioned in it
 * and fills up the 'tclist' with the fully qualified file paths
 */
int collectTestCases (string root_dir, string dirORfile, const string regex_expr, RegexType regex_flag, vector <string> &tclist)
{
	LogString saved_context;
	LOGGER_PUSH_NDCTAG (LOGGER_TAG_HELPER);
	log4cxx :: LoggerPtr logger = log4cxx :: Logger :: getLogger (HARNESS_LOGGER_NAME);

	if (root_dir.empty ())
		root_dir = bid::current_directory ();
	
	/* if it is a relative path then prepend it with 'root_dir' */
	if (dirORfile.find ('/') != 0 && dirORfile.find ('~') != 0)
		dirORfile = root_dir + "/" + dirORfile;

	dirORfile = getAbsolutePath (dirORfile);
	if (dirORfile.empty ())
	{
		LOGGER_POP_NDCTAG;
		stringstream ss;
		ss << "File or Directory [" << dirORfile << "] does not exist.";
		throw SystemError (FILE_LINE_FUNCTION, ss.str());
	}


	/* if it is a directory */
	if (bfs::exists (dirORfile) && bfs::is_directory (dirORfile))
	{
		/* now collected all testcases from under all the subdirectories */
		bfs::directory_iterator end_iter;
		for (bfs::directory_iterator dir_iter(dirORfile); dir_iter != end_iter; dir_iter++)
		{
			bfs::path p (dir_iter->path ());

			/* it should be a regular file with ".test" as an extension */
			if (bfs :: is_regular (p) && p.extension () == ".test")
			{
#if (BOOST_FS_VER==2)
				string abs_fname = getAbsolutePath (p.directory_string ());
#else
				string abs_fname = getAbsolutePath (p.string ());
#endif
				if (abs_fname.empty ())
				{
					LOGGER_POP_NDCTAG;
					LOG4CXX_ERROR (logger, "File [" << abs_fname << "] either does not exist or is not a regular file.");
					return FAILURE;
				}

				if (bfs::is_empty (abs_fname.c_str ()))
				{
					LOG4CXX_WARN (logger, "File [" << abs_fname << "] is an empty file. Hence ignoring it.");
					continue;
				}

				if (!regex_expr.empty ())
				{
					switch (regex_flag)
					{
						case REGEX_FLAG_INCLUDE_ID :
							if (check_regex_match (regex_expr, converttoid (root_dir, abs_fname)) == true)
								tclist.push_back (abs_fname);
							break;
						case REGEX_FLAG_EXCLUDE_ID :
							if (check_regex_match (regex_expr, converttoid (root_dir, abs_fname)) == true)
							{
								/* do nothing */
							}
							else
								tclist.push_back (abs_fname);
							break;
						case REGEX_FLAG_INCLUDE_NAME :
							if (check_regex_match (regex_expr, abs_fname) == true)
								tclist.push_back (abs_fname);
							break;
						case REGEX_FLAG_EXCLUDE_NAME :
							if (check_regex_match (regex_expr, abs_fname) == true)
							{
								/* do nothing */
							}
							else
								tclist.push_back (abs_fname);
							break;
						case REGEX_FLAG_NO_REGEX_EXPR :
							throw ConfigError (FILE_LINE_FUNCTION, ERR_CONFIG_REGEX_EXPR_SPECIFIED_BUT_FLAG_NOT_SET);
							break;
					}
				}
				else
					tclist.push_back (abs_fname);
			}
		}
	}
	/* if it is a .suite file then convert all ids (test id, suite id, suite file id) to path name
     * and only collect test case file names (i.e. files with .test extension)
     */
	else
	{
		if (!bfs::is_regular (dirORfile))
		{
			LOGGER_POP_NDCTAG;
			stringstream ss;
			ss << "Suite file [" << dirORfile << "] does not exist or is not a regular file";
			throw SystemError (FILE_LINE_FUNCTION, ss.str());
		}

		ifstream f(dirORfile.c_str());
		if (!f.is_open ())
		{
			LOGGER_POP_NDCTAG;
			stringstream ss;
			ss << "Could not open suite file [" << dirORfile << "]";
			throw SystemError (FILE_LINE_FUNCTION, ss.str());
		}

		string line;
		/* read the file */
		while (!f.eof ())
		{
			std::getline (f, line, LINE_FEED);
			boost::trim (line);

			/* if blank line or a line containing only the spaces */
			if (line.empty())
				continue;

			{
				bfs::path p (dirORfile);
				string pathstring = converttopath (line);
				string testfile = p.parent_path ().string () + "/" + pathstring + ".test";

				if (bfs::is_regular (testfile))
				{
					string abs_fname = testfile;

					if (!regex_expr.empty ())
					{
						switch (regex_flag)
						{
							case REGEX_FLAG_INCLUDE_ID :
								if (check_regex_match (regex_expr, converttoid (root_dir, abs_fname)) == true)
									tclist.push_back (abs_fname);
								break;
							case REGEX_FLAG_EXCLUDE_ID :
								if (check_regex_match (regex_expr, converttoid (root_dir, abs_fname)) == true)
								{
									/* do nothing */
								}
								else
									tclist.push_back (abs_fname);
								break;
							case REGEX_FLAG_INCLUDE_NAME :
								if (check_regex_match (regex_expr, abs_fname) == true)
									tclist.push_back (abs_fname);
								break;
							case REGEX_FLAG_EXCLUDE_NAME :
								if (check_regex_match (regex_expr, abs_fname) == true)
								{
									/* do nothing */
								}
								else
									tclist.push_back (abs_fname);
								break;
							case REGEX_FLAG_NO_REGEX_EXPR :
								throw ConfigError (FILE_LINE_FUNCTION, ERR_CONFIG_REGEX_EXPR_SPECIFIED_BUT_FLAG_NOT_SET);
								break;
						}
					}
					else
						tclist.push_back (abs_fname);
				}
				else
				{
					/* ignore it, as it might be a suite-id or suite file id */
				}
			}
		} //END while(!eof)
		f.close ();
	} //END else .suite file

	LOGGER_POP_NDCTAG;
    return tclist.size ();
}

/* converts the test case ids mentioned on the commandline into test case file names.
 * In future we can add "--under_directory <directory path>" commandline option to harness.
 */
int collectTestCases (string root_dir, const vector <string> &testcase_idsORnames, const string regex_expr, RegexType regex_flag,
                      vector <string> &tclist, string under_directory, int flag)
{
	LogString saved_context;
	LOGGER_PUSH_NDCTAG (LOGGER_TAG_HELPER);
	log4cxx :: LoggerPtr logger = log4cxx :: Logger :: getLogger (HARNESS_LOGGER_NAME);

	if (root_dir.empty ())
		root_dir = bid::current_directory ();
	
	/* if it is a relative path then prepend it with 'root_dir' */
	if (under_directory.find ('/') != 0 && under_directory.find ('~') != 0)
		under_directory = root_dir + "/" + under_directory;

	under_directory = getAbsolutePath (under_directory);
	if (under_directory.empty ())
	{
		LOGGER_POP_NDCTAG;
		stringstream ss;
		ss << "Directory [" << under_directory << "] Does not Exists.";
		throw SystemError (FILE_LINE_FUNCTION, ss.str());
	}

	/* first check if all testcases exists under this directory */
	for (unsigned int i=0; i<testcase_idsORnames.size (); i++)
	{
		string file_fullpath;

		if (flag == TESTCASE_IDS)
			file_fullpath = under_directory + "/" + converttopath (testcase_idsORnames[i]) + ".test";
		else
			file_fullpath = under_directory + "/" + testcase_idsORnames[i];

        bfs::path p (file_fullpath);

        /* it should be a regular file */
        if (bfs :: is_regular (p))
        {
			file_fullpath = getAbsolutePath (file_fullpath);
			if (file_fullpath.empty ())
			{
				LOGGER_POP_NDCTAG;
				LOG4CXX_ERROR (logger, "File [" << file_fullpath << "] either does not exist or is not a regular file.");
				return FAILURE;
			}

			if (bfs::is_empty (file_fullpath.c_str ()))
			{
				LOG4CXX_WARN (logger, "File [" << file_fullpath << "] is an empty file. Hence ignoring it.");
				continue;
			}

			if (!regex_expr.empty ())
			{
				switch (regex_flag)
				{
					case REGEX_FLAG_INCLUDE_ID :
						if (check_regex_match (regex_expr, converttoid (root_dir, file_fullpath)) == true)
							tclist.push_back (file_fullpath);
						break;
					case REGEX_FLAG_EXCLUDE_ID :
						if (check_regex_match (regex_expr, converttoid (root_dir, file_fullpath)) == true)
						{
							/* do nothing */
						}
						else
							tclist.push_back (file_fullpath);
						break;
					case REGEX_FLAG_INCLUDE_NAME :
						if (check_regex_match (regex_expr, file_fullpath) == true)
							tclist.push_back (file_fullpath);
						break;
					case REGEX_FLAG_EXCLUDE_NAME :
						if (check_regex_match (regex_expr, file_fullpath) == true)
						{
							/* do nothing */
						}
						else
							tclist.push_back (file_fullpath);
						break;
					case REGEX_FLAG_NO_REGEX_EXPR :
						throw ConfigError (FILE_LINE_FUNCTION, ERR_CONFIG_REGEX_EXPR_SPECIFIED_BUT_FLAG_NOT_SET);
						break;
				}
			}
			else
				tclist.push_back (file_fullpath);
        }
		else
			LOG4CXX_ERROR (logger, "File [" << file_fullpath << "] either does not exist or is not a regular file.");
	}

	if (flag == TESTCASE_NAMES)
	{
		/* now collected all testcases from under all the subdirectories */
		bfs::directory_iterator end_iter;
		for (bfs::directory_iterator dir_iter(under_directory); dir_iter != end_iter; dir_iter++)
		{
			bfs::path p (dir_iter->path ());

			/* if it is a non-empty directory then traverse recursively */
			if (bfs :: is_directory (p))
			{
				if (!bfs :: is_empty (p))
#if (BOOST_FS_VER==2)
					collectTestCases (root_dir, testcase_idsORnames, regex_expr, regex_flag, tclist, p.directory_string (), TESTCASE_NAMES);
#else
					collectTestCases (root_dir, testcase_idsORnames, regex_expr, regex_flag, tclist, p.string (), TESTCASE_NAMES);
#endif
			}
		}
	}

	LOGGER_POP_NDCTAG;
    return tclist.size ();
}

int tokenize_commandline (const string str, vector<string> &token_list)
{
	if (str.empty())
		return FAILURE;

	LogString saved_context;
	LOGGER_PUSH_NDCTAG (LOGGER_TAG_HELPER);
	log4cxx :: LoggerPtr logger = log4cxx :: Logger :: getLogger (HARNESS_LOGGER_NAME);

	string command = "arg_separator ";
	command = command + str;

	LOG4CXX_INFO (logger, "Tokenizing : [" << command << "]");

	int rbytes, exit_code=FAILURE;
	char buf[BUFSIZ+1];
	FILE *pipe = 0;
	string stored_buf;
	while ((rbytes = ReadOutputOf (command, &pipe, buf, BUFSIZ, &exit_code)) > 0)
	{
		buf[rbytes] = 0;

		string tmp = stored_buf + buf;
		while (1)
		{
			size_t nl_index = tmp.find ("\n");

			if (nl_index != string::npos)
			{
				char *token = new char [nl_index + 1];
				tmp.copy (token, nl_index, 0);
				token[nl_index] = 0;
				token_list.push_back (token);
				delete token;

				tmp.replace (0, nl_index+1, "");	
				stored_buf = tmp;
			}
			else
				break;
		}
	}

	if (exit_code == 0)
	{
		/* args successfully separated */
		/* do nothing */
	}
	else
	{
		LOGGER_POP_NDCTAG;
		std::stringstream ss;
		ss << "tokenize_commandline: arg_separator failed when processing '" << str << "' Please check quoting." ;
		throw SystemError (FILE_LINE_FUNCTION, ss.str());
	}
	
	LOGGER_POP_NDCTAG;
	return token_list.size ();
}

int tokenize (const string str, vector<string> &token_list, const string separators)
{
	typedef boost::tokenizer<boost::char_separator<char> > tokenizer;
	boost::char_separator<char> sep(separators.c_str ());
	tokenizer tokens(str, sep);

	for (tokenizer::iterator tok_iter = tokens.begin(); tok_iter != tokens.end(); ++tok_iter)
	{
		token_list.push_back (*tok_iter);
	}

	return token_list.size ();
}

string getAbsolutePath(std::string path, bool logger_enabled)
{
	if (path.empty())
		return "";

	char* abs_path = realpath (path.c_str(), NULL);
	if (abs_path == NULL)
	{
		cerr << "No such file or directory : " << path << endl;
		return "";
	}

	string absolute_path = abs_path;
	free (abs_path);            // realpath() call allocates memory using malloc, which needs to be freed
	return absolute_path;
}

string getAbsolutePath(std::string path)
{
	if (path.empty())
		return "";

	LogString saved_context;
	LOGGER_PUSH_NDCTAG (LOGGER_TAG_HELPER);
	log4cxx :: LoggerPtr logger = log4cxx :: Logger :: getLogger (HARNESS_LOGGER_NAME);

	char* abs_path = realpath (path.c_str(), NULL);
	if (abs_path == NULL)
	{
		LOG4CXX_ERROR (logger, "No such file or directory : " << path);
		LOGGER_POP_NDCTAG;
		return "";
	}

	string absolute_path = abs_path;
	free (abs_path);            // realpath() call allocates memory using malloc, which needs to be freed
	LOGGER_POP_NDCTAG;

	return absolute_path;
}

bool Is_regular (const string fname)
{
	bfs::path p(fname);
	return bfs :: is_regular (p);
}

int Creat(const char *pathname, mode_t mode)
{
	int rv;

	if ((rv = ::creat(pathname, mode)) == -1)
	{
		char errbuf[BUFSIZ];
		strerror_r (errno, errbuf, sizeof (errbuf));
		cerr << "Creat() : " << errbuf << endl;
		return -1;
	}
	return rv;
}

long int sToi (const string str)
{
	char *endptr;
	long int val;

    errno = 0;    /* To distinguish success/failure after call */
    val = strtol (str.c_str (), &endptr, 10);

    /* Check for various possible errors */
    if ((errno == ERANGE && (val == LONG_MAX || val == LONG_MIN)) ||
        (errno != 0 && val == 0) ||
    	(endptr == str)          ||
		(*endptr != '\0'))
    {
        return -1;
    }

    return val;
}

string iTos (long long int i)
{
    string str;
    stringstream ss(stringstream::in | stringstream::out);
    ss << i;
    ss >> str;

	return str;
}

void prepare_filepaths (InfoForExecutor &ie, int internally_called)
{
	if (internally_called)
	{
		string tmp_testdir, tmp_resultdir;
		tmp_testdir = tmp_testdir + "/" + DEFAULT_TEST_CASE_DIR;
		tmp_resultdir = tmp_resultdir + "/" + DEFAULT_RESULT_DIR;

		/* find "/t/" in ie.tcfile, replace it with "/r/" */
		string tmp = ie.tcfile;
		size_t found = tmp.find (tmp_testdir);
		int len = tmp_testdir.length ();
		tmp.replace (found, len, tmp_resultdir); /* here it will be ...../r/1.test */

		ie.expected_rfile = ie.tcfile; // Expected files are now in the same location as test files.
		ie.actual_rfile = tmp;
		ie.timerfile = tmp;
		ie.diff_file = tmp;
		ie.log_file = tmp;
	}
	else
	{
		ie.expected_rfile = ie.tcfile;
		ie.actual_rfile = ie.tcfile;
		ie.timerfile = ie.tcfile;
		ie.diff_file = ie.tcfile;
		ie.log_file = ie.tcfile;
	}

#if (BOOST_FS_VER==2)
	string file_extension = (bfs::path (ie.tcfile)).extension();
#else
	string file_extension = (bfs::path (ie.tcfile)).extension().string();
#endif
	int file_extensionLength = file_extension.length();

	assert (file_extensionLength > 0);

	ie.expected_rfile.replace (ie.expected_rfile.find (file_extension), file_extensionLength, ".expected");
	ie.actual_rfile.replace (ie.actual_rfile.find (file_extension), file_extensionLength, ".out");
	ie.timerfile.replace (ie.timerfile.find (file_extension), file_extensionLength, ".timer");
	ie.diff_file.replace (ie.diff_file.find (file_extension), file_extensionLength, ".diff");
	ie.log_file.replace (ie.log_file.find (file_extension), file_extensionLength, ".log");
	
	//------------------------------------------------------------------------
	// Change: during the desktop/development workflow the harness will be run
	// directly against the tests located in the checked out SVN tree (rootDir
	// switch value will point there).  The logs, outs, diffs, etc. will be 
	// dumped into scratchDir in order not to pollute the SVN tree diffs.
	ie.actual_rfile = ie.actual_rfile.replace (0,ie.rootDir.length(),ie.scratchDir,0,ie.scratchDir.length());
	ie.timerfile = ie.timerfile.replace(0,ie.rootDir.length(),ie.scratchDir,0,ie.scratchDir.length());
	ie.diff_file = ie.diff_file.replace(0,ie.rootDir.length(),ie.scratchDir,0,ie.scratchDir.length());
	ie.log_file = ie.log_file.replace(0,ie.rootDir.length(),ie.scratchDir,0,ie.scratchDir.length());
	// End change.
	//------------------------------------------------------------------------
}

int Socket (int domain, int type, int protocol)
{
	int rv;

	if ((rv = socket (domain, type, protocol)) == -1)
	{
		char errbuf[BUFSIZ];
		strerror_r (errno, errbuf, sizeof (errbuf));

		stringstream ss;
		ss << "Got Error [" << errbuf << "] while creating a socket.";
		throw SystemError (FILE_LINE_FUNCTION, ss.str ());
	}

	return rv;
}

int Close (int fd)
{
	int rv;

	if ((rv = close (fd)) == -1)
	{
		char errbuf[BUFSIZ];
		strerror_r (errno, errbuf, sizeof (errbuf));

		stringstream ss;
		ss << "Got Error [" << errbuf << "] while closing of the fd " << fd;
		throw SystemError (FILE_LINE_FUNCTION, ss.str ());
	}

	return rv;
}

int Open (const char *pathname, int flags)
{
	int fd = open (pathname, flags);
	if (fd == -1)
	{
		char errbuf[BUFSIZ];
		strerror_r (errno, errbuf, sizeof (errbuf));

		stringstream ss;
		ss << "Got Error [" << errbuf << "] while opening of the file " << pathname;
		throw SystemError (FILE_LINE_FUNCTION, ss.str ());
	}

	return fd;
}

/* Remove additional slashes "/" from paths
 */
void normalizePath (std::vector <std::string> &pathvec)
{
	string :: size_type slash;
	std::vector <std::string> tmp;
	tmp = pathvec;
	pathvec.clear();
	std::string tmp_value="";
	for (unsigned int i=0; i<tmp.size();i++)
	{
		tmp_value = tmp[i];
		while ((slash = tmp_value.find ("//")) != string::npos)
			tmp_value.replace (slash, 2, "/");
		pathvec.push_back(tmp_value);
	}

}

} //END namespace scidbtestharness
