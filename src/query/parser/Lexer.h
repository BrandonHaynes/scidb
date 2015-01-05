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

#ifndef  QUERY_PARSER_LEXER_H_
#define  QUERY_PARSER_LEXER_H_

/****************************************************************************/

#undef   yyFlexLexer
#define  yyFlexLexer BaseLexer
#include <FlexLexer.h>                                   // For BaseLexer
#include "Parser.hpp"                                    // For token numbers

/****************************************************************************/
namespace scidb { namespace parser {
/****************************************************************************/
/**
 *  @brief      Implements a lexical analyzer for the array query languages.
 *
 *  @details    Class Lexer implements a simple lexical analyzer based upon a
 *              function, Lexer::operator(), that is generated for us by FLEX
 *              from a standard lexer specification (given elsewhere).
 *
 *              A Lexer is constructed with:
 *
 *              - a resetting arena with which to allocate memory for variable
 *              length token attributes such as the values of string literals;
 *              in this sense the caller is responsible for garbage collecting
 *              tokens and their atrributes;
 *
 *              - an abstract log to which lexical errors can be appended;
 *
 *              - an input stream from which the source text is to be read;
 *
 *              - a flag that indicates the kind of syntactic construct we are
 *              initially parsing for:  this flag determines the first pseudo-
 *              token that will be returned to the parser from operator().
 *
 *              The lexemes of AFL and AQL are almost identical, the only real
 *              difference being that the latter adds a few more keywords. The
 *              implementation is therefore  parameterized on the lexicon that
 *              is used to resolve keywords, and the result is a lexer that is
 *              suitable for scanning either language. In fact, it can even be
 *              switched between the two on the fly by calling setLexicon() at
 *              any time while parsing.
 *
 *  @see        http://flex.sourceforge.net for the FLEX reference manual.
 *
 *  @author     jbell@paradigm4.com.
 */
class Lexer : protected BaseLexer
{
 public:                   // Construction
                              Lexer(Arena&,Log&,istream&,syntax);

 public:                   // Operations
                lexicon       getLexicon()         const;

 public:                   // Operations
                int           operator()(semantic_type*,location_type*);
                lexicon       setLexicon(lexicon);
                void          setTracing(bool);

 private:                  // Implementation
                int           onReal();
                int           onString();
                int           onBoolean();
                int           onInteger();
                int           onIdentifier();
                int           onQuotedIdentifier();
                void          onLineComment();
                void          onBlockComment();
                int           onError(error);

 private:                  // Implementation
                void          copy(size_t,chars);        // Copy to arena
                int           input();                   // Read a character

 private:                  // Representation
            syntax      const _start;                    // The initial syntax
            Arena&            _arena;                    // The memory arena
            Log&              _log;                      // The error log
            semantic_type*    _yylval;                   // The token value
            location_type*    _yylloc;                   // The token location
            bool            (*_isKeyword)(chars,chars&,int&);
};

/****************************************************************************/
}}
/****************************************************************************/
#endif
/****************************************************************************/
