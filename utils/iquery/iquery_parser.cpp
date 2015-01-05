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
 * @file
 *
 * @brief Glue class between lexer and parser for iquery commands
 *
 * @author Artyom Smirnov <smirnoffjr@gmail.com>
 */

#include <boost/format.hpp>
#include <boost/make_shared.hpp>

#include <sstream>

#include "system/Exceptions.h"
#include "system/ErrorCodes.h"

#include "iquery/iquery_parser.h"
#include "iquery/scanner.h"
#include "iquery/commands.h"

#include "iquery/location.hh"

using namespace boost;

namespace yy
{

IqueryParser::IqueryParser(bool trace) :
	_trace(trace),
	_scanner(NULL),
	_cmd(NULL),
	_iqueryCommand(false)
{}


int IqueryParser::parse(const std::string& input)
{
	std::istringstream iss(input);
	Scanner scanner(*this, &iss);
	scanner.set_debug(_trace);
	_scanner = &scanner;
	
	Parser parser(*this);
	parser.set_debug_level(_trace);

	int result = parser.parse();
    _scanner = NULL;
	return result; 
}

const IqueryCmd* IqueryParser::getResult() const
{
    return _cmd;
}

void IqueryParser::error(const class location& loc, const std::string& msg)
{
	_errorString = msg;
}

void IqueryParser::error2(const class location& loc, const std::string& msg)
{
	throw USER_EXCEPTION(scidb::SCIDB_SE_SYNTAX, scidb::SCIDB_LE_IQUERY_PARSER_ERROR) << msg;
}

bool IqueryParser::isIqueryCommand() const
{
    return _iqueryCommand;
}

}
