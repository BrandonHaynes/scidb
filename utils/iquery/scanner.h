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
 * @brief Derived scanner from base FlexLexer
 *
 * @author Artyom Smirnov <smirnoffjr@gmail.com>
 */

#ifndef IQUERY_SCANNER_H_
#define IQUERY_SCANNER_H_

#undef yyFlexLexer
#define yyFlexLexer BaseFlexLexer
#include "FlexLexer.h"

#include "iquery/parser.hpp"

namespace yy
{

class Scanner : public BaseFlexLexer
{
public:
	Scanner(IqueryParser& glue, std::istream* arg_yyin = 0,	std::ostream* arg_yyout = 0);

	virtual ~Scanner();

	virtual Parser::token_type lex(Parser::semantic_type* yylval, Parser::location_type* yylloc);

	void set_debug(bool b);
	
	void error(const std::string &msg, const  Parser::location_type* location);

private:
	class IqueryParser &_glue;
};

}

#endif /* IQUERY_SCANNER_H_ */
