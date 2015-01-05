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

/****************************************************************************/

#include <query/ParsingContext.h>                        // For ParsingContext
#include "location.hh"                                   // For location

/****************************************************************************/
namespace scidb {
/****************************************************************************/

    ParsingContext::ParsingContext()
                  : _text(boost::make_shared<std::string>()),
                    _slin(0),
                    _scol(0),
                    _elin(0),
                    _ecol(0)
{}

    ParsingContext::ParsingContext(const string_ptr& s)
                  : _text(s),
                    _slin(0),
                    _scol(0),
                    _elin(0),
                    _ecol(0)
{}

    ParsingContext::ParsingContext(const string_ptr& s,const parser::location& l)
                  : _text(s),
                    _slin(l.begin.line),
                    _scol(l.begin.column),
                    _elin(l.end.line),
                    _ecol(l.end.column)
{}

/****************************************************************************/
}
/****************************************************************************/
