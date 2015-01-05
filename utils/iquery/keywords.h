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
 * @brief Routines for manipulating registered keywords of iquery parser
 *
 * @author Artyom Smirnov <smirnoffjr@gmail.com>
 */

#ifndef IQUERY_KEYWORDS_H_
#define IQUERY_KEYWORDS_H_

#include <stdint.h>
#include <string>

#include "iquery/parser.hpp"

/**
 * Macroses for easy defining keywords and collating them to tokens
 */
#define KW(name, tok) {name, yy::Parser::token::tok}

/**
 * Structure for defining keywords.
 */
struct Keyword
{
	const char* name;
    yy::Parser::token::yytokentype tok;
};

/**
 * Seaching keyword in array of possible keywords
 * @param keyword Keyword for searching
 * @return If found - Keyword structure, else - NULL.
 */
const Keyword* FindKeyword(const char *keyword);

#endif /* IQUERY_KEYWORDS_H_ */
