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

# include <iostream>
# include <strings.h>
# include <vector>
# include <sstream>
# include <sys/socket.h>
# include <net/if.h>
# include <arpa/inet.h>

# include "Exceptions.h"
# include "cdashreportstructs.h"
using namespace std;
using namespace scidbtestharness;
using namespace scidbtestharness::Exceptions;

namespace scidbtestharness
{

ostream & print_IndividualTestResults (ostream &os, const struct CDASH_HarnessTestResults &tr)
{
	string cdashclntip;
	char *envvalue = getenv ("cdashclientip");
	if (!envvalue || *envvalue == 0)
	{
		/* Warning : value for "cdashclientip" is not set in the environment. Will use IP of this machine.
		 * Find ethernet address for this machine. */
		int unneeded_sockfd=0;
		unneeded_sockfd = Socket (AF_INET, SOCK_STREAM, 0);

		struct ifconf ifc;
		int num_iface = if_addr_fetch (&ifc, unneeded_sockfd);
		Close (unneeded_sockfd);

		if (num_iface == 0)
			throw ConfigError (FILE_LINE_FUNCTION, "Network interfaces are not properly configured.");

		struct ifreq *ip = ifc.ifc_req; ;
		for ( ; num_iface > 0; num_iface--, ip++)
		{
			cdashclntip = inet_ntoa (((struct sockaddr_in *) &ip->ifr_addr)->sin_addr);

			cout << "IFACE: " << ip->ifr_name << " - " << cdashclntip << endl;
			if (num_iface == 1 && cdashclntip == "127.0.0.1")
				throw ConfigError (FILE_LINE_FUNCTION, "Network interfaces are not properly configured.");

			if (cdashclntip != "127.0.0.1")
				break;
		}
	}
	else
		cdashclntip = envvalue;

	string scidbtestcasesURL = "scidbtestcases";
	string scidbtestresultsURL = "scidbtestresults";
	string revisionURL = "?rev=";

	envvalue = getenv ("scidbtestcasesURL");
	if (envvalue && *envvalue)
	{
		scidbtestcasesURL = envvalue;
	}

	envvalue = getenv ("scidbtestresultsURL");
	if (envvalue && *envvalue)
	{
		scidbtestresultsURL = envvalue;
	}

	envvalue = getenv ("revisionURL");
	if (envvalue && *envvalue)
	{
		revisionURL += envvalue;
	}

	cout << "CdashReportApp :: Using cdashclientip=" << cdashclntip << endl;
	cout << "CdashReportApp :: Using scidbtestcasesURL=" << scidbtestcasesURL << endl;
	cout << "CdashReportApp :: Using scidbtestresultsURL=" << scidbtestresultsURL << endl;
	cout << "CdashReportApp :: Using revisionURL=" << revisionURL << endl;
	cout << "INFO : Total TestList Size = [" << tr.v_IndividualTestResult.size() << "]" << endl;

	for (unsigned int i=0; i<tr.v_IndividualTestResult.size(); i++)
	{
//		cout << "INFO : putting in test number [" << i+1 << "]" << endl;

		string status_str="failed";
		if ((strcasecmp (tr.v_IndividualTestResult[i].TestcaseResult.c_str(), "PASS") == 0) ||
		    (strcasecmp (tr.v_IndividualTestResult[i].TestcaseResult.c_str(), "RECORDED") == 0))
			status_str = "passed";

		os << "<Test Status=\"" << status_str << "\">" << endl;
		os << "<Name>" << tr.v_IndividualTestResult[i].TestID << "</Name>" << endl;

		//<< "<Path>.</Path>" << endl
		//<< "<FullName>" << tr.v_IndividualTestResult[i].TestID << "</FullName>" << endl
		//<< "<FullCommandLine></FullCommandLine>" << endl

		os << "<Results>" << endl;
		os << "<NamedMeasurement type=\"numeric/double\" name=\"Execution Time\"><Value>" << tr.v_IndividualTestResult[i].TestTotalExeTime << "</Value></NamedMeasurement>" << endl;
		string cstatus_str="Completed";
		if (strcasecmp (tr.v_IndividualTestResult[i].TestcaseResult.c_str(), "ERROR_CODES_DIFFER") == 0)
			cstatus_str="ERROR_CODES_DIFFER";
		os << "<NamedMeasurement type=\"text/string\" name=\"Completion Status\"><Value>" << cstatus_str << "</Value></NamedMeasurement>" << endl;

		/* test case file name */
		string tmp = tr.v_IndividualTestResult[i].TestcaseFile;
		string testdir = "/t/";
	        size_t found = tmp.find (testdir);
		bool run_tests_basic=false;
		if (found == string :: npos)
			run_tests_basic=true;

/*       if (found == string :: npos)
		{
			stringstream ss;
			ss << "Invalid test case file name [" << tmp << "]. Check if file exists.";
			throw ConfigError (FILE_LINE_FUNCTION, ss.str());
		}
*/

	string trac_url = "http://trac.scidb.net/browser/trunk/tests/harness/testcases/";
	char *env_trac = getenv ("trac_url");
	if (env_trac && *env_trac)
	  {
	    trac_url = env_trac;
	  }
	else
	  {
	    trac_url = "http://trac.scidb.net/browser/trunk/tests/harness/testcases/";
	  }
	
	string url = "";
	if (!run_tests_basic)
		{
		found += 2;
		tmp.replace (0, found, "");

//		url = "http://" + cdashclntip + "/" + scidbtestcasesURL + "/" + tmp;
		url = trac_url + "t/" + tmp + revisionURL;
		}
	else
//		url = "http://trac.scidb.net/browser/trunk/tests/basic/" + tmp + revisionURL;
//		url = trac_url + tmp + revisionURL;
//		url = "http://" + cdashclntip + "/" + scidbtestresultsURL + "/" + "/tests_basic/" + tmp;

	        url = scidbtestcasesURL;
	
		os << "<NamedMeasurement type=\"text/string\" name=\"Testcase File\"><Value>&lt;a href=\"" 
		   << url << "\">" << tr.v_IndividualTestResult[i].TestcaseFile << " &lt;/a></Value></NamedMeasurement>" << endl;

		/* .expected file name */
		tmp = tr.v_IndividualTestResult[i].TestcaseExpectedResultFile;
		if (tmp.length())
		{
			if (!run_tests_basic)
			{
				tmp.replace (0, found, "");
//				url = "http://" + cdashclntip + "/" + scidbtestresultsURL + "/" + tmp;
				url = trac_url + "t/" + tmp + revisionURL; // Changed the location of the expected files from r/ to t/ (ticket #3663).
			}
			else
				url = trac_url + tmp + revisionURL;
//				url = "http://" + cdashclntip + "/" + scidbtestresultsURL + "/tests_basic/" + tmp;

			os << "<NamedMeasurement type=\"text/string\" name=\"Expected Result File\"><Value>&lt;a href=\""
				<< url << "\">" << tr.v_IndividualTestResult[i].TestcaseExpectedResultFile << " &lt;/a></Value></NamedMeasurement>" << endl;
		}

		/* .out file name */
		tmp = tr.v_IndividualTestResult[i].TestcaseActualResultFile;
		if (tmp.length())
		{
			if (!run_tests_basic)
			{
				tmp.replace (0, found, "");
				url = "http://" + cdashclntip + "/" + scidbtestresultsURL + "/" + tmp;
			}
			else
				url = "http://" + cdashclntip + "/" + scidbtestresultsURL + "/tests_basic/" + tmp;

			os << "<NamedMeasurement type=\"text/string\" name=\"Actual Result File\"><Value>&lt;a href=\""
				<< url << "\">" << tr.v_IndividualTestResult[i].TestcaseActualResultFile << " &lt;/a></Value></NamedMeasurement>" << endl;
		}

		/* .diff file name */
		tmp = tr.v_IndividualTestResult[i].TestcaseDiffFile;
		if (tmp.length())
		{
			if (!run_tests_basic)
			{
				tmp.replace (0, found, "");
				url = "http://" + cdashclntip + "/" + scidbtestresultsURL + "/" + tmp;
			}
			else
				url = "http://" + cdashclntip + "/" + scidbtestresultsURL + "/tests_basic/" + tmp;

			os << "<NamedMeasurement type=\"text/string\" name=\"Diff File\"><Value>&lt;a href=\""
				<< url << "\">" << tr.v_IndividualTestResult[i].TestcaseDiffFile << " &lt;/a></Value></NamedMeasurement>" << endl;
		}

		/* .timer file name */
		tmp = tr.v_IndividualTestResult[i].TestcaseTimerFile;
		if (tmp.length())
		{
			if (!run_tests_basic)
			{
				tmp.replace (0, found, "");
				url = "http://" + cdashclntip + "/" + scidbtestresultsURL + "/" + tmp;
			}
			else
				url = "http://" + cdashclntip + "/" + scidbtestresultsURL + "/tests_basic/" + tmp;

			os << "<NamedMeasurement type=\"text/string\" name=\"Timer File\"><Value>&lt;a href=\""
				<< url << "\">" << tr.v_IndividualTestResult[i].TestcaseTimerFile << " &lt;/a></Value></NamedMeasurement>" << endl;
		}

		/* test case result (PASS/FAIL) */
		os << "<NamedMeasurement type=\"text/string\" name=\"TestcaseResult\"><Value>"
		   << tr.v_IndividualTestResult[i].TestcaseResult << "</Value></NamedMeasurement>" << endl;

		if (tr.v_IndividualTestResult[i].TestcaseFailureReason != "")
		{
			os << "<NamedMeasurement type=\"text/string\" name=\"Testcase Failure Reason\"><Value>"
				<< tr.v_IndividualTestResult[i].TestcaseFailureReason << "</Value></NamedMeasurement>" << endl;
		}

		/* .log file name */
		tmp = tr.v_IndividualTestResult[i].TestcaseLogFile;
		if (tmp != "")
		{
			if (!run_tests_basic)
			{
				tmp.replace (0, found, "");
				url = "http://" + cdashclntip + "/" + scidbtestresultsURL + "/" + tmp;
			}
			else
				url = "http://" + cdashclntip + "/" + scidbtestresultsURL + "/tests_basic/" + tmp;

			os << "<NamedMeasurement type=\"text/string\" name=\"Testcase Log File\"><Value>&lt;a href=\""
		   	<< url << "\">" << tr.v_IndividualTestResult[i].TestcaseLogFile << " &lt;/a></Value></NamedMeasurement>" << endl;
		}

		os << "<Measurement><Value>Not Applicable</Value></Measurement>" << endl;
		os << "</Results>" << endl;
		os << "</Test>" << endl;
	}

	return os;
}

ostream & print_TestList (ostream &os, const struct CDASH_HarnessTestResults &tr)
{
	for (unsigned int i=0; i<tr.v_IndividualTestResult.size(); i++)
		os << "<Test>" << tr.v_IndividualTestResult[i].TestID << "</Test>" << endl;

	return os;
}

ostream & print_HarnessTestResults (ostream &os, const struct CDASH_Report &cr)
{
	print_TestList (os, cr.TestResults);
	os << "</TestList>" << endl;
	print_IndividualTestResults (os, cr.TestResults);

	return os;
}

} //END namespace scidbtestharness
