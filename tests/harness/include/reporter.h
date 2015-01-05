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
 * @file reporter.h
 * @author girish_hilage@persistent.co.in
 * @brief file containing reporter class
 */

# ifndef REPORTER_H
# define REPORTER_H

# include <fstream>
# include <string>
# include <log4cxx/logger.h>
# include <boost/archive/archive_exception.hpp>

# include "xmlarchive.h"
# include "Exceptions.h"

namespace scidbtestharness
{

enum ReportType
{
	REPORT_READ,
	REPORT_WRITE
};

/**
 * this class writes the report into the report file in the XML format
 * which when accessed through a browser using an XSLTFile.xsl file shows the report in
 * the table format.
 */
class REPORTER
{
    private :
		std::ifstream      _ifs;
		XMLiArchive *      _xi;

		std::ofstream      _ofs;
		XMLArchive *       _xa;
		long               _prevStoredPosition;
		log4cxx::LoggerPtr _logger;

    public :

	REPORTER (std::string fname)
	{
		_xi = 0;
		_xa = 0;

		_ofs.open (fname.c_str ());
		assert (_ofs.good());

		_xa = new XMLArchive(_ofs);
		_prevStoredPosition = 0;

		_logger = log4cxx::Logger::getLogger (HARNESS_LOGGER_NAME);
	}

	REPORTER (std::string fname, ReportType rtype)
	{
		_xi = 0;
		_xa = 0;

		try
		{
			if (rtype == REPORT_READ)
			{
				_ifs.open (fname.c_str ());
				assert (_ifs.good());

				_xi = new XMLiArchive(_ifs);
			}
			else if (rtype == REPORT_WRITE)
			{
				_ofs.open (fname.c_str ());
				assert (_ofs.good());

				_xa = new XMLArchive(_ofs);
			}
			else
				throw scidbtestharness::Exceptions::ConfigError (FILE_LINE_FUNCTION, "Invalid REPORT type given.");

			_prevStoredPosition = 0;
			_logger = log4cxx::Logger::getLogger (HARNESS_LOGGER_NAME);
		}

		BOOST_CATCH (boost::archive::archive_exception ae)
		{
			throw scidbtestharness::Exceptions::SystemError (FILE_LINE_FUNCTION, ae.what());
		}
	}

	int readHarnessTestReport (struct CDASH_Report &SciDBTestReport);

	int writeFinalInfo (const struct ExecutionStats &harness_es);
	int writeIntermediateRunStat (const int testcases_passed, const int testcases_failed);
	int writeTestcaseExecutionInfo (const IndividualTestInfo &individualtest_info);
	int writeInitialInfo (const struct HarnessCommandLineOptions &SciDBHarnessEnv);

	~REPORTER ()
	{
		if (_xi)
		{
			delete _xi;
		}

		_ofs.flush ();
		if (_xa)
		{
			delete _xa;
		}
	}
};

} //END namespace scidbtestharness

# endif
