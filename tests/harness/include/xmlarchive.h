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
 * @file xmlarchive.h
 * @author girish_hilage@persistent.co.in
 * @brief file containing a concrete class which writes a report in the XML format to the report file.
 */

# ifndef XMLARCHIVE_H
# define XMLARCHIVE_H

# include <string>
# include <fstream>
# include <boost/archive/xml_oarchive.hpp>
# include <boost/archive/xml_iarchive.hpp>

# include "global.h"
# include "cdashreportstructs.h"

# define XML_OPEN_ANGLE1  "<"
# define XML_OPEN_ANGLE2  "</"
# define XML_CLOSE_ANGLE  ">\n"
# define XML_MAKE_START_TAG(tagname) (std::string("") + XML_OPEN_ANGLE1 + tagname + XML_CLOSE_ANGLE);
# define XML_MAKE_END_TAG(tagname)   (std::string("") + XML_OPEN_ANGLE2 + tagname + XML_CLOSE_ANGLE);

namespace scidbtestharness
{

class XMLiArchive : public boost::archive::xml_iarchive
{
	public :
	XMLiArchive (std::istream &is) : boost::archive::xml_iarchive(is)
	{ }

	void load (struct CDASH_Report &SciDBTestReport);
	~XMLiArchive () {}
};

/**
 * This class writes a report in the XML format to the report file.
 * The report file when accessed from a browser it will make use of the .xsl file
 * and will show the report in a browser in a tabular format
 */
class XMLArchive : public boost::archive::xml_oarchive_impl<boost::archive::xml_oarchive>
{
	public :
	XMLArchive (std::ostream &os, unsigned int flags = 0) : boost::archive::xml_oarchive_impl<boost::archive::xml_oarchive>(os, flags)
	{
		std::string line1 = "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\" ?>\n";
		std::string styletag = "<?xml-stylesheet type=\"text/xsl\" href=\"XSLTFile.xsl\"?>\n";
		std::string line2 = "<!DOCTYPE boost_serialization>\n";
		std::string line3 = "<boost_serialization signature=\"serialization::archive\" version=\"5\">\n";
		std::string line4 = "<SciDBTestReport class_id=\"0\" tracking_level=\"0\" version=\"2\">\n";
		os.seekp (0);
		this->This()->put(line1.c_str ());
		this->This()->put(styletag.c_str ());
		this->This()->put(line2.c_str ());
		this->This()->put(line3.c_str ());
		this->This()->put(line4.c_str ());
	}

	void save (const struct ExecutionStats &harness_es);
	void save (const struct IntermediateStats &is);
	void save (const struct IndividualTestInfo &iti);
	void save (const struct HarnessCommandLineOptions &SciDBHarnessEnv);

	void putEndTagNOIndent (const char *tagname)
	{
		os << XML_MAKE_END_TAG (tagname);
	}

	void putStartTagNOIndent (const char *tagname)
	{
		os << XML_MAKE_START_TAG (tagname);
	}

	void putEndTag (const char *tagname)
	{
		save_end (tagname);
	}

	void putStartTag (const char *tagname)
	{
		save_start (tagname);
	}

	void putCHAR (const char c)
	{
		this->This()->put(c);
	}

	void flush (void)
	{
		os.flush ();
	}

	void seekp (const int pos)
	{
		os.seekp (pos);
	}

	std::streampos tellp (void)
	{
		long pos;

		if ((pos = os.tellp ()) == -1)
		{
			std::cerr << "Error in tellp()\n";
		}

		return pos;
	}

	~XMLArchive () {}
};
} //END namespace scidbtestharness

# endif
