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
#include <string.h>

#include "keywords.h"

using namespace std;

const Keyword Keywords[] =
{
    KW("set",       token::SET),
    KW("lang",      token::LANG),
    KW("afl",       token::AFL),
    KW("aql",       token::AQL),
    KW("no",        token::NO),
    KW("fetch",     token::FETCH),
    KW("verbose",   token::VERBOSE),
    KW("timer",     token::TIMER),
    KW("quit",      token::QUIT),
    KW("exit",      token::QUIT),
    KW("help",      token::HELP),
    KW("format",    token::FORMAT),
    KW(NULL,    token::EOQ) //end of tokens
};

const Keyword* FindKeyword(const char *keyword)
{
	const Keyword *kw = &Keywords[0];

	while(kw->name)
	{
		if (!strcasecmp(kw->name, keyword))
			return kw;
		++kw;
	}

	return NULL;
}
