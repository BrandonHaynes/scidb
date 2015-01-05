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

%option c++
%option batch
%option debug
%option stack
%option noyywrap
%option nounput

/****************************************************************************/

%{
#include <boost/format.hpp>                              // For format()
#include <boost/lexical_cast.hpp>                        // For lexical_cast
#include <boost/algorithm/string/replace.hpp>            // For replace()
#include "query/parser/Lexer.h"                          // For Lexer

#define YY_USER_ACTION  {yylloc->columns(yyleng);}
#define YY_DECL         int scidb::parser::Lexer::operator()(semantic_type* yylval,location_type* yylloc)
%}

/****************************************************************************/

Space                   [ \f\r\t\v]
String                  (L?'(\\.|[^'])*')
Boolean                 (?i:"true")|(?i:"false")
Integer                 ([0-9]+)
Decimal                 (([0-9]*\.[0-9]+)|([0-9]+\.[0-9]*))
Scientific              ({Integer}|{Decimal})[Ee][-+]?[0-9]+
Special                 (inf|nan|NaN)
Real                    ({Decimal}|{Scientific}|{Special})
Identifier              ([A-Za-z_\$][A-Za-z0-9_\$]*)

%% /*************************************************************************/

%{
 /* The constructor initialized the keyword lookup function pointer to 0 so if
    this is the first call to scan a token we install the appropriate function
    now and return a pseudo-token that indicates  which grammar production the
    parser should begin gathering from; see the topic 'multiple start-symbols'
    of the Bison reference manual for more on this technique...*/

    if (_isKeyword == 0)                                 // First time called?
    {
        switch (_start)                                  // ...initial syntax?
        {
            default:            SCIDB_UNREACHABLE();     // ....exhaustive
            case aqlStatement:  setLexicon(AQL);return Token::AQL_STATEMENT;
            case aflStatement:  setLexicon(AFL);return Token::AFL_STATEMENT;
            case aflExpression: setLexicon(AFL);return Token::AFL_EXPRESSION;
            case aflModule:     setLexicon(AFL);return Token::AFL_MODULE;
        }
    }

 /* We save the semantic value and location references in data members of our
    enclosing class so that our member functions will have implicit access to
    them too... */

   _yylloc = yylloc;                                     // Save token value
   _yylval = yylval;                                     // Save location

 /* And advance the starting the position to match the current ending...*/

    yylloc->step();                                      // Advance location
%}

{Space}{1,8}        {}
[\n]                {yylloc->lines();}
"--"                {onLineComment();}
"//"                {onLineComment();}
"/*"                {onBlockComment();}
"<>"                {return Token::NEQ;}
"!="                {return Token::NEQ;}
"<="                {return Token::LSEQ;}
">="                {return Token::GTEQ;}
{Real}              {return onReal();}
{String}            {return onString();}
{Boolean}           {return onBoolean();}
{Integer}           {return onInteger();}
{Identifier}        {return onIdentifier();}
\"{Identifier}\"    {return onQuotedIdentifier();}
.                   {return yytext[0];}

%% /*************************************************************************/

/****************************************************************************/
namespace scidb { namespace parser {
/****************************************************************************/

/**
 *  We construct a lexer by saving the arena, error log, input stream, and the
 *  initial syntax we will be scanning for in data members where we can easily
 *  access them all later.
 */
    Lexer::Lexer(Arena& arena,Log& log,istream& in,syntax start)
         : BaseLexer(&in),                               // The input stream
          _start    (start),                             // The initial syntax
          _arena    (arena),                             // The parser's arena
          _log      (log),                               // The error log
          _yylval   (0),                                 // The semantic value
          _yylloc   (0),                                 // The token location
          _isKeyword(0)                                  // The active lexicon
{
    assert(arena.supports(resetting));                   // Garbage collecting
}

/**
 *  Enable or disable the lexical analyzer's built in support for tracing its
 *  progress as it attempts to scan each token of the language.
 */
void Lexer::setTracing(bool enable)
{
    yy_flex_debug = enable;                              // Enable tracing?
}

/**
 *  Read and discard characters from the input stream until we find a newline.
 *  This function is invoked by the Lex rule for a line comment, and completes
 *  the job of stripping the comment from the input stream.
 */
inline void Lexer::onLineComment()
{
   for (int c=input(); c!='\n' && c!=EOF; c=input())     // For each character
   {}                                                    // ...just discard it
}

/**
 *  Read and discard characters from the input stream until we see a * / pair.
 *  This function is called by the Lex rule for a block comment, and completes
 *  the job of stripping the comment from the input stream. If no such pair is
 *  found, then the comment was not properly terminated and we log an error.We
 *  save the stream offset in case of an error, then set up a tiny specialized
 *  DFA to recognize the terminating markers. We use an index to keep track of
 *  the number of comment levels we are nested, and ignore block comments that
 *  occur within line comments.
 */
inline void Lexer::onBlockComment()
{
   size_t n = 1;                                         // The nesting depth

 A:                                                      // The initial state
   switch (input())                                      // Which character?
   {
      case EOF: goto F;                                  // ...end of file
      case '*': goto B;                                  // ...seen the *
      case '/': goto C;                                  // ...seen the /
      default : goto A;                                  // ...back to top
   }
 B:                                                      // Seen a single *
   switch (input())                                      // Which character?
   {
      case EOF: goto F;                                  // ...end of file
      case '*': goto B;                                  // ...keep looking
      case '/': if (--n == 0) return;                    // ...one less pair
      default : goto A;                                  // ...back to top
   }
 C:                                                      // Seen a single /
   switch (input())                                      // Which character?
   {
      case EOF: goto F;                                  // ...end of file
      case '/': goto D;                                  // ...seen // pair
      case '*': ++n;                                     // ...seen /* pair
      default : goto A;                                  // ...back to top
   }
 D:                                                      // Seen a // pair
   switch (input())                                      // Which character?
   {
      case EOF: goto F;                                  // ...end of file
      case'\n': goto A;                                  // ...end of line
      default : goto D;                                  // ...discard input
   }
 F:
   onError(SCIDB_LE_BAD_BLOCK_COMMENT);                  // No terminating */
}

/**
 *  Parse the contents of the lexeme buffer for a real number.
 */
inline int Lexer::onReal()
try                                                      // In case of error
{
    _yylval->real = boost::lexical_cast<double>(yytext); // ...let boost parse

    return Token::REAL;                                  // ...parsed a real
}
catch (boost::bad_lexical_cast&)                         // Failed to parse?
{
    return onError(SCIDB_LE_BAD_LITERAL_REAL);           // ...log the error
}

/**
 *  Parse the contents of the lexeme buffer for a string literal.
 */
inline int Lexer::onString()
{
    string  s(yytext,1,yyleng-2);                        // Copy into a string

    boost::replace_all(s,"\\'","'");                     // FIXME: Ugly unescaping

    copy(s.size(),&*s.begin());                          // Assign token value
    return Token::STRING;                                // Parsed a string
}

/**
 *  Parse the contents of the lexeme buffer for a boolean constant.
 */
inline int Lexer::onBoolean()
{
   _yylval->boolean = tolower(*yytext) == 't';           // Assign token value

    return Token::BOOLEAN;                               // Parsed a boolean
}

/**
 *  Parse the contents of the lexeme buffer for an integer.
 */
inline int Lexer::onInteger()
try                                                      // In case of error
{
    _yylval->integer = boost::lexical_cast<int64_t>(yytext);

    return Token::INTEGER;                               // ...parsed integer
}
catch(boost::bad_lexical_cast&)                          // Failed to parse?
{
    return onError(SCIDB_LE_BAD_LITERAL_INTEGER);        // ...log the error
}

/**
 *  Parse the contents of the lexeme buffer for a keyword or identifier.
 *
 *  We check the current lexicon to see if the matching text is a keyword:  if
 *  so, then there is no need to copy anything because the lexicon entries are
 *  all statically allocated. If, on the other hand, the text isn't a keyword,
 *  then we must copy the identifier we have just found off into the arena.
 */
inline int Lexer::onIdentifier()
{
    int token;                                           // The keyword token

    if (_isKeyword(yytext,_yylval->keyword,token))       // Jump thru pointer
    {
        return token;                                    // ...read a keyword
    }
    else                                                 // No, not a keyword
    {
        copy(yyleng,yytext);                             // ...set token value

        return Token::IDENTIFIER;                        // ...read identifier
    }
}

/**
 *  Parse the contents of the lexeme buffer for an identifier.
 *
 *  No need to check the lexicon this time because the text is surrounded with
 *  quotes, so can only be an identifier.
 */
inline int Lexer::onQuotedIdentifier()
{
    assert(yytext[0]=='"' && yytext[yyleng-1]=='"');     // Surrounding quotes

    copy(yyleng-2,yytext+1);                             // Assign token value

    return Token::IDENTIFIER;                            // Read an identifier
}

/**
 *  Report an error to the log. The error names a printf-style template string
 *  that can refer to the current contents of the lexeme buffer 'yytext'. Note
 *  that in principle the fail() method may return control back to us; thus we
 *  must recover and continue lexing the input.
 */
int Lexer::onError(error error)
{
    _log.fail(error,*_yylloc,yytext);                    // Report the error

    return Token::LEXICAL_ERROR;                         // Complain to parser
}

/**
 *  Copy the first 'n' characters of the string 's' into memory allocated from
 *  the arena that was supplied us at construction and write the copy into the
 *  token value.
 */
void Lexer::copy(size_t n,const char* s)
{
    assert(s!=0 && n<=strlen(s));                        // Validate arguments

    char* t = static_cast<char*>(_arena.malloc(n + 1));  // Allocate off arena

    memcpy(t,s,n);                                       // Copy the 'n' chars
    t[n] = '\0';                                         // And terminate them

    _yylval->string = t;                                 // Assign token value
}

/**
 *  Return the next character waiting in the input buffer. Replaces the normal
 *  yyinput() in order to keep the token location object synchronized with our
 *  input stream.
 */
int Lexer::input()
{
    int c = yyinput();                                   // Get next character

    if (c == '\n')                                       // ...read a newline?
    {
        _yylloc->lines();                                // ....advance lines
    }
    else                                                 // No, something else
    {
        _yylloc->columns();                              // ....advance columns
    }

    return c;                                            // The character read
}

/****************************************************************************/
}}
/****************************************************************************/

int BaseLexer::yylex()  {return 0;}                      // Keep linker happy
int BaseLexer::yywrap() {return 1;}                      // Keep linker happy

/****************************************************************************/
