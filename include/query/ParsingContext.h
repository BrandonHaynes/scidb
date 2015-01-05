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

#ifndef QUERY_PARSING_CONTEXT_H_
#define QUERY_PARSING_CONTEXT_H_

/****************************************************************************/

#include <string>                                        // For string
#include <stdint.h>                                      // For uint_32
#include <boost/make_shared.hpp>                         // For shared_ptr

/****************************************************************************/
namespace scidb {
/****************************************************************************/

namespace parser {class location;}                       // A source location

/****************************************************************************/

class ParsingContext
{
 public:                   // Supporting types
    typedef boost::shared_ptr<std::string> string_ptr;   // A tracking pointer

 public:                   // Construction
                              ParsingContext();
                              ParsingContext(const string_ptr&);
                              ParsingContext(const string_ptr&,const parser::location&);
                              ParsingContext(const std::string&,uint32_t,uint32_t,uint32_t,uint32_t);

 public:                   // Operations
 const std::string&           getQueryString()     const {return*_text;}
            uint32_t          getLineStart()       const {return _slin;}
            uint32_t          getLineEnd()         const {return _elin;}
            uint32_t          getColStart()        const {return _scol;}
            uint32_t          getColEnd()          const {return _ecol;}

 private:                  // Representation
            string_ptr  const _text;                     // The original text
            uint32_t    const _slin;                     // The starting line
            uint32_t    const _scol;                     // The starting column
            uint32_t    const _elin;                     // The ending   line
            uint32_t    const _ecol;                     // The ending   column
};

/****************************************************************************/

inline ParsingContext::ParsingContext(const std::string& qs,uint32_t sl,uint32_t sc,uint32_t el,uint32_t ec)
                     : _text(boost::make_shared<std::string>(qs)),
                       _slin(sl),
                       _scol(sc),
                       _elin(el),
                       _ecol(ec)
{}

/****************************************************************************/
}
/****************************************************************************/
#endif
/****************************************************************************/
