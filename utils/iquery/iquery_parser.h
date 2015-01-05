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

#ifndef IQUERYPARSER_H_
#define IQUERYPARSER_H_

#include <sstream>
#include <boost/shared_ptr.hpp>

class IqueryCmd;

namespace yy
{

class IqueryParser
{
public:
	IqueryParser(bool trace = false);

	int parse(const std::string& input);

	void error2(const class location& l, const std::string& m);

    const class IqueryCmd* getResult() const;

    bool isIqueryCommand() const;

private:
	void error(const class location& l, const std::string& m);

	bool _trace;
	class Scanner *_scanner;
    class IqueryCmd* _cmd;
	std::string _errorString;
	bool _iqueryCommand;

	friend class Parser;
};

}

#endif /* IQUERYPARSER_H_ */
