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
 * @file cdashreport.h
 * @author girish_hilage@persistent.co.in
 * @brief file containing a concrete class for generation of CDASH compatible SciDB test report.
 */

# ifndef CDASHREPORT_H
# define CDASHREPORT_H

# include <string>
# include "interface.h"
# include "cdashreportstructs.h"
# include "reporter.h"

namespace scidbtestharness
{

class CdashReportApp : public interface::Application
{
	public :
		CdashReportApp (void)
		{
			_rptr = 0;
		}

		~CdashReportApp () throw()
		{
			if (_rptr)
				delete _rptr;
		}

	private :
		void prepare_cdash_compatible_report (void);
		void read_edited_harness_report (void);
		void remove_tag_stylesheet (void);
		int execute (int mode);

		void createReporter (void)
		{
			_rptr = new REPORTER (_edited_harnessreport, REPORT_READ);
		}
		int validateParameters (void);
		int parseCommandLine (unsigned int argc, char** argv);

	private :
		REPORTER* _rptr;
		CDASH_Report _SciDBTestReport;
		std::string _ctestreport;
		std::string _harnessreport;
		std::string _edited_harnessreport;
};

} //END namespace scidbtestharness
# endif
