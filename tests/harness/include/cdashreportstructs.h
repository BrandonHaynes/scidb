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
 * @file cdashreportstructs.h
 * @author girish_hilage@persistent.co.in
 * @brief file contains structs required for CDASH report preparation
 */

# ifndef CDASHREPORTSTRUCTS_H
# define CDASHREPORTSTRUCTS_H

# include <vector>
# include <string>

# include <boost/archive/xml_oarchive.hpp>
# include <boost/archive/xml_iarchive.hpp>
# include <boost/archive/archive_exception.hpp>
# include <boost/serialization/nvp.hpp>
# include <boost/serialization/version.hpp>
# include <boost/serialization/string.hpp>

# include "global.h"

namespace scidbtestharness
{

struct CDASH_FinalStats
{
	private :
		std::string TotalTestCases;
		std::string TotalTestsPassed;
		std::string TotalTestsFailed;
		std::string TotalTestsSkipped;

	public :
		template<class Archive> void serialize(Archive &ar, const unsigned int version)
		{
			if (version >= 2)
			{
				ar & BOOST_SERIALIZATION_NVP(TotalTestCases);
				ar & BOOST_SERIALIZATION_NVP(TotalTestsPassed);
				ar & BOOST_SERIALIZATION_NVP(TotalTestsFailed);
				ar & BOOST_SERIALIZATION_NVP(TotalTestsSkipped);
			}
		}
};

struct CDASH_IndividualTestResult
{
	private :
		std::string TestID;
		std::string TestDescription;
		std::string TestStartTime;
		std::string TestEndTime;
		std::string TestTotalExeTime;
		std::string TestcaseFile;
		std::string TestcaseExpectedResultFile;
		std::string TestcaseActualResultFile;
		std::string TestcaseTimerFile;
		std::string TestcaseDiffFile;
		std::string TestcaseResult;
		std::string TestcaseFailureReason;
		std::string TestcaseLogFile;

	public :
		friend std::ostream & print_IndividualTestResults (std::ostream &os, const struct CDASH_HarnessTestResults &tr);
		friend std::ostream & print_TestList (std::ostream &os, const struct CDASH_HarnessTestResults &tr);
		template<class Archive> void serialize(Archive &ar, const unsigned int version)
		{
			if (version >= 2)
			{
				ar & BOOST_SERIALIZATION_NVP(TestID);
				ar & BOOST_SERIALIZATION_NVP(TestDescription);
				ar & BOOST_SERIALIZATION_NVP(TestStartTime);
				ar & BOOST_SERIALIZATION_NVP(TestEndTime);
				ar & BOOST_SERIALIZATION_NVP(TestTotalExeTime);
				ar & BOOST_SERIALIZATION_NVP(TestcaseFile);
				ar & BOOST_SERIALIZATION_NVP(TestcaseExpectedResultFile);
				ar & BOOST_SERIALIZATION_NVP(TestcaseActualResultFile);
				ar & BOOST_SERIALIZATION_NVP(TestcaseTimerFile);
				ar & BOOST_SERIALIZATION_NVP(TestcaseDiffFile);
				ar & BOOST_SERIALIZATION_NVP(TestcaseResult);
				ar & BOOST_SERIALIZATION_NVP(TestcaseFailureReason);
				ar & BOOST_SERIALIZATION_NVP(TestcaseLogFile);
			}
		}
};

struct CDASH_HarnessTestResults
{
	private :
		std::vector<CDASH_IndividualTestResult> v_IndividualTestResult;
		CDASH_IndividualTestResult IndividualTestResult;

	public :
		friend std::ostream & print_IndividualTestResults (std::ostream &os, const struct CDASH_HarnessTestResults &tr);
		friend std::ostream & print_TestList (std::ostream &os, const struct CDASH_HarnessTestResults &tr);
		template<class Archive> void serialize(Archive &ar, const unsigned int version)
		{
			if (version >= 2)
			{
				//for (int i=0; i<10; i++)
				for (;;)
				{
					ar & BOOST_SERIALIZATION_NVP(IndividualTestResult);
					v_IndividualTestResult.push_back (IndividualTestResult);
				}
			}
		}
};

/* commandline options for harness */
struct CDASH_HarnessEnvironment
{
	private :
		std::string             scidbServer;
		std::string                     scidbPort;
		std::string             rootDir;
		std::string             skipTestfname;
		std::string             regexExpr;
		std::string               regexFlag;
		std::string             logDir;
		std::string             reportFilename;
		std::string                     parallelTestCases;
		std::string                     debugLevel;
		std::string                    record;
		std::string                    keepPreviousRun;
		std::string                    terminateOnFailure;

	public :
		template<class Archive> void serialize(Archive &ar, const unsigned int version)
		{
			if (version >= 2)
			{
				ar & BOOST_SERIALIZATION_NVP(scidbServer);
				ar & BOOST_SERIALIZATION_NVP(scidbPort);
				ar & BOOST_SERIALIZATION_NVP(rootDir);
				ar & BOOST_SERIALIZATION_NVP(skipTestfname);
				ar & BOOST_SERIALIZATION_NVP(regexFlag);
				ar & BOOST_SERIALIZATION_NVP(regexExpr);
				ar & BOOST_SERIALIZATION_NVP(logDir);
				ar & BOOST_SERIALIZATION_NVP(reportFilename);
				ar & BOOST_SERIALIZATION_NVP(parallelTestCases);
				ar & BOOST_SERIALIZATION_NVP(debugLevel);
				ar & BOOST_SERIALIZATION_NVP(record);
				ar & BOOST_SERIALIZATION_NVP(keepPreviousRun);
				ar & BOOST_SERIALIZATION_NVP(terminateOnFailure);
			}
		}
};

struct CDASH_Report
{
	private :
		struct CDASH_HarnessEnvironment SciDBHarnessEnv;
		struct CDASH_HarnessTestResults TestResults;
		struct CDASH_FinalStats FinalStats;

	public :
		friend std::ostream & print_HarnessTestResults (std::ostream &os, const struct CDASH_Report &cr);
		template<class Archive> void serialize(Archive &ar, const unsigned int version)
		{
			if(version >= 2)
			{
				try
				{
					ar & BOOST_SERIALIZATION_NVP(SciDBHarnessEnv);
					ar & BOOST_SERIALIZATION_NVP(TestResults);
					//ar & BOOST_SERIALIZATION_NVP(FinalStats);
				}

				BOOST_CATCH (boost::archive::archive_exception &ae)
				{
					/* Do not throw this exception. Just return. */
					return;
				}
			}
		}
};

} //END namespace scidbtestharness

BOOST_CLASS_VERSION(scidbtestharness::CDASH_FinalStats, 2)
BOOST_CLASS_VERSION(scidbtestharness::CDASH_IndividualTestResult, 2)
BOOST_CLASS_VERSION(scidbtestharness::CDASH_HarnessTestResults, 2)
BOOST_CLASS_VERSION(scidbtestharness::CDASH_HarnessEnvironment, 2)
BOOST_CLASS_VERSION(scidbtestharness::CDASH_Report, 2)

# endif
