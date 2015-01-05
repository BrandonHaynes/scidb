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
 * @file cdashreport.cpp
 * @author girish_hilage@persistent.co.in
 */

# include <string>
# include <sstream>
# include <fstream>
# include <boost/filesystem/operations.hpp>
# include <boost/program_options/options_description.hpp>
# include <boost/program_options/variables_map.hpp>
# include <boost/program_options/parsers.hpp>
# include <boost/algorithm/string.hpp>

# include "Exceptions.h"
# include "cdashreportapp.h"

# define CDASH_COMPATIBLE_REPORTNAME "CDASH_SciDBFinalReport.xml"

using namespace std;
using namespace scidbtestharness;
using namespace scidbtestharness::Exceptions;
namespace po = boost::program_options;
namespace bfs = boost :: filesystem;

namespace scidbtestharness
{

void CdashReportApp :: prepare_cdash_compatible_report (void)
{
	bfs::path p (_harnessreport);
	string cdashcompatiblereport = p.parent_path().string() + "/" + CDASH_COMPATIBLE_REPORTNAME;

	ofstream cdashfile(cdashcompatiblereport.c_str());
	if (!cdashfile.good())
	{
        stringstream ss;
        ss << "Error while opening of the File [" << cdashcompatiblereport << "].";
        throw SystemError (FILE_LINE_FUNCTION, ss.str());
	}

	ifstream ctr(_ctestreport.c_str());
	if (!ctr.good())
	{
        stringstream ss;
        ss << "Error while opening of the File [" << _ctestreport << "].";
        throw SystemError (FILE_LINE_FUNCTION, ss.str());
	}

	string line, linecopy;
	bool testlist_started=false;
	while (!ctr.eof())
	{
		getline (ctr, line);

		if (ctr.eof ())
			break;

		linecopy = line;
		boost::trim (linecopy);

		if (testlist_started == true)
		{
			if (!boost::starts_with (linecopy, "<EndDateTime>"))
				continue;

			testlist_started = false;
		}

		if (boost::starts_with (linecopy, "<TestList>"))
		{
			testlist_started = true;
		}

		cdashfile << line << endl;

		if (testlist_started == true)
		{
			print_HarnessTestResults (cdashfile, _SciDBTestReport);
		}
	}

	cdashfile.close ();
	ctr.close ();
}

void CdashReportApp :: read_edited_harness_report (void)
{
	createReporter ();

	_rptr->readHarnessTestReport (_SciDBTestReport);
}

void CdashReportApp :: remove_tag_stylesheet (void)
{
	ifstream f1(_harnessreport.c_str());
	if (!f1.good())
	{
        stringstream ss;
        ss << "Error while opening of the File [" << _harnessreport << "].";
        throw SystemError (FILE_LINE_FUNCTION, ss.str());
	}

	ofstream f2(_edited_harnessreport.c_str());
	if (!f2.good())
	{
        stringstream ss;
        ss << "Error while opening of the File [" << _edited_harnessreport << "].";
        throw SystemError (FILE_LINE_FUNCTION, ss.str());
	}

	string line, linecopy;
	while (!f1.eof())
	{
		getline (f1, line);

		if (f1.eof ())
			break;

		linecopy = line;
		boost::trim (linecopy);

		/* if line starts with "xml-stylesheet" */
		if (boost::starts_with (linecopy, "<?xml-stylesheet "))
			continue;

		f2 << line << endl;
	}

	f1.close ();
	f2.close ();
}

int CdashReportApp :: execute (int mode)
{
	/* remove line containing a tag "<?xml-stylesheet " as it is not
     * recognized by boost XML library */
	remove_tag_stylesheet ();

	read_edited_harness_report ();
	prepare_cdash_compatible_report ();

	return SUCCESS;
}

int CdashReportApp :: validateParameters (void)
{
	if (_ctestreport.empty())
        throw ConfigError (FILE_LINE_FUNCTION, "Missing mandatory parameter --ctestreport");

	string full_path1 = getAbsolutePath (_ctestreport);
    if (!bfs :: is_regular (full_path1))
    {
        stringstream ss;
        ss << "File [" << _ctestreport << "] either does not exist or is not a regular file.";
        throw SystemError (FILE_LINE_FUNCTION, ss.str());
    }

	if (_harnessreport.empty())
        throw ConfigError (FILE_LINE_FUNCTION, "Missing mandatory parameter --harnessreport");

	string full_path2 = getAbsolutePath (_harnessreport);

	if (full_path1 == full_path2)
        throw ConfigError (FILE_LINE_FUNCTION, "Files specified by --ctestreport and --harnessreport can not be the same");
	
    if (!bfs :: is_regular (full_path2))
    {
        stringstream ss;
        ss << "File [" << _harnessreport << "] either does not exist or is not a regular file.";
        throw SystemError (FILE_LINE_FUNCTION, ss.str());
    }

	_edited_harnessreport = _harnessreport + ".edited";
	string full_path3 = getAbsolutePath (_edited_harnessreport);
    if (bfs::exists (full_path3) && !bfs::is_regular (full_path3))
    {
        stringstream ss;
        ss << "File [" << _edited_harnessreport << "] is not a regular file.";
        throw SystemError (FILE_LINE_FUNCTION, ss.str());
    }

	return SUCCESS;
}

int CdashReportApp :: parseCommandLine (unsigned int argc, char** argv)
{
	po::options_description desc(
			"Usage: cdashreport [--ctestreport <ctestreportfile_path>] [--harnessreport <harnessreportfile_path>]\n"
			);

	desc.add_options()
		("ctestreport",          po::value<string>(), "Path to a Test.xml file generated by 'ctest'")
		("harnessreport",        po::value<string>(), "Path to a Report.xml file generated by test harness")
		("help,h", "View this text.");

	po::variables_map vm;
	po::store (po::parse_command_line (argc, argv, desc), vm);
	po::notify (vm);

	if (!vm.empty ())
	{
		if (vm.count ("help"))
		{
			cerr << desc << endl;
			exit (0);
		}

		if (vm.count ("ctestreport"))
			_ctestreport = vm["ctestreport"].as<string>();

		if (vm.count ("harnessreport"))
			_harnessreport = vm["harnessreport"].as<string>();
	} else
	{
		cerr << "Invalid no. of parameters on the command line\n";
		cerr << "Use --help for help\n";
		exit (0);
	}

	validateParameters ();
	//createReporter ();
	return SUCCESS;
}

} //END namespace scidbtestharness
