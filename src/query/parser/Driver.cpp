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

#include <system/Exceptions.h>                           // For USER_EXCEPTION
#include <system/Config.h>                               // For config file
#include <util/arena/ScopedArena.h>                      // For ScopedArena
#include <query/ParsingContext.h>                        // For ParsingContext
#include <log4cxx/logger.h>                              // For logging stuff
#include <fstream>                                       // For ifstream
#include "Module.h"                                      // For ModuleLock
#include "Lexer.h"                                       // For Lexer
#include "AST.h"                                         // For Node etc.

/****************************************************************************/
namespace scidb { namespace parser { namespace {
/****************************************************************************/

log4cxx::LoggerPtr log(log4cxx::Logger::getLogger("scidb.qproc.driver"));

typedef shared_ptr<LogicalExpression>    LEPtr;
typedef shared_ptr<LogicalQueryPlanNode> LQPtr;

/****************************************************************************/

class Driver : public Log, boost::noncopyable
{
 public:                   // Construction
                              Driver(const string&);

 public:                   // Operations
            Node*             process(syntax);
            LEPtr             translate(Node*);
            LQPtr             translate(Node*,const QueryPtr&);

 private:                  // From class Log
    virtual void              fail(const UserException&);
    virtual void              fail(error,const Node&    ,const char*);
    virtual void              fail(error,const location&,const char*);

 private:                  // Representation
            StringPtr   const _text;
            ScopedArena       _arena;
            Factory           _fact;
};

    Driver::Driver(const string& text)
          : _text (make_shared<string>(text)),
            _arena("parser::Driver"),
            _fact (_arena)
{}

Node* Driver::process(syntax syntax)
{
    Node*          tree  (0);
    istringstream  source(*_text);
    Lexer          lexer (_arena,*this,source,syntax);
    Parser         parser(_fact,*this,tree,lexer);

    if (false)
    {
        lexer.setTracing(true);
        parser.set_debug_level(true);
    }

    parser.parse();

    LOG4CXX_DEBUG(log,"Driver::process(1)\n" << tree);

    desugar (_fact,*this,tree);

    LOG4CXX_DEBUG(log,"Driver::process(2)\n" << tree);

//  stratify(_fact,*this,tree);
    inliner (_fact,*this,tree);

    LOG4CXX_DEBUG(log,"Driver::process(3)\n" << tree);

    return tree;
}

LEPtr Driver::translate(Node* n)
{
    return parser::translate(_fact,*this,_text,n);
}

LQPtr Driver::translate(Node* n,const QueryPtr& q)
{
    return parser::translate(_fact,*this,_text,n,q);
}

/****************************************************************************/

void Driver::fail(const UserException& what)
{
    throw what;
}

void Driver::fail(error e,const Node& n,const char* s)
{
    const Node* p = &n;                                  // The location node

    if (p->is(variable))                                 // Is it a variable?
    {
        p = p->get(variableArgName);                     // ...then use name
    }

    if (p->is(cstring))                                  // Is a string node?
    {
        s = p->getString();                              // ...then use chars
    }

    this->fail(e,p->getWhere(),s);                       // Reformat and throw
}

void Driver::fail(error e,const location& w,const char* s)
{
    shared_ptr<ParsingContext> c(make_shared<ParsingContext>(_text,w));

/* Translate error code 'e' into an exception object: unfortunately these can
   only be created via macros at the moment, hence the ugly 'switch'. hoping
   to change this soon...*/

    switch (e)
    {
        case SCIDB_LE_QUERY_PARSING_ERROR:  fail(USER_QUERY_EXCEPTION(SCIDB_SE_PARSER,SCIDB_LE_QUERY_PARSING_ERROR, c) << s);break;
        case SCIDB_LE_BAD_BLOCK_COMMENT:    fail(USER_QUERY_EXCEPTION(SCIDB_SE_SYNTAX,SCIDB_LE_BAD_BLOCK_COMMENT,   c) << s);break;
        case SCIDB_LE_BAD_LITERAL_REAL:     fail(USER_QUERY_EXCEPTION(SCIDB_SE_SYNTAX,SCIDB_LE_BAD_LITERAL_REAL,    c) << s);break;
        case SCIDB_LE_BAD_LITERAL_INTEGER:  fail(USER_QUERY_EXCEPTION(SCIDB_SE_SYNTAX,SCIDB_LE_BAD_LITERAL_INTEGER, c) << s);break;
        case SCIDB_LE_NAME_REDEFINED:       fail(USER_QUERY_EXCEPTION(SCIDB_SE_SYNTAX,SCIDB_LE_NAME_REDEFINED,      c) << s);break;
        case SCIDB_LE_NAME_NOT_APPLICABLE:  fail(USER_QUERY_EXCEPTION(SCIDB_SE_SYNTAX,SCIDB_LE_NAME_NOT_APPLICABLE, c) << s);break;
        case SCIDB_LE_NAME_IS_RECURSIVE:    fail(USER_QUERY_EXCEPTION(SCIDB_SE_SYNTAX,SCIDB_LE_NAME_IS_RECURSIVE,   c) << s);break;
        case SCIDB_LE_NAME_ARITY_MISMATCH:  fail(USER_QUERY_EXCEPTION(SCIDB_SE_SYNTAX,SCIDB_LE_NAME_ARITY_MISMATCH, c) << s);break;
        default                          :  fail(USER_QUERY_EXCEPTION(SCIDB_SE_SYNTAX,e,                            c) << s);break;
    }
}

/****************************************************************************/

/**
 *  Return the path to the AFL 'prelude', a special module of macros that ship
 *  with, and that the user percevies as being built into, the SciDB system.
 */
string getPreludePath()
{
    return Config::getInstance()->getOption<string>(CONFIG_INSTALL_ROOT) + "/lib/scidb/modules/prelude.txt";
}

/**
 *  Read the contents of the given text file into a string and return it.
 */
string read(const string& path)
{
    ifstream f(path.c_str());                            // Open for reading
    string   s((istreambuf_iterator<char>(f)),           // Copy contents to
                istreambuf_iterator<char>());            //  the string 's'

    if (f.fail())                                        // Failed to read?
    {
        throw SYSTEM_EXCEPTION(SCIDB_SE_PLUGIN_MGR,SCIDB_LE_FILE_READ_ERROR) << path;
    }

    return s;                                            // The file contents
}

/**
 *  Parse and translate the module statement 'text', and install the resulting
 *  bindings in the currently loaded module, where other queries can then find
 *  them.
 */
void load(const string& text)
{
    Module m(Module::write);                             // Lock for writing
    Driver d(text);                                      // Create the driver
    Node*  n(d.process(aflModule));                      // Parse and desugar

    m.load(d,n);                                         // Install the module
}

/****************************************************************************/
}}}
/****************************************************************************/

#include <query/Query.h>

/****************************************************************************/
namespace scidb {
/****************************************************************************/

using namespace parser;

/**
 *  Parse and translate the expression 'text'.
 */
LEPtr parseExpression(const string& text)
{
    Module m(Module::read);                              // Lock for reading
    Driver d(text);                                      // Create the driver
    Node*  n(d.process(aflExpression));                  // Parse and desugar

    return d.translate(n);                               // Return translation
}

/**
 *  Parse and translate the given query, which is specified in either 'AFL' or
 *  'AQL' syntax.
 */
LQPtr parseStatement(const QueryPtr& query,bool afl)
{
    Module m(Module::read);                              // Lock for reading
    Driver d(query->queryString);                        // Create the driver
    Node*  n(d.process(afl ? aflStatement:aqlStatement));// Parse and desugar

    return d.translate(n,query);                         // Return translation
}

/**
 *  Parse and translate the prelude module.
 */
void loadPrelude()
{
    load(read(getPreludePath()));                        // Load the prelude
}

/**
 *  Parse and translate the given user module, after concatenating it onto the
 *  prelude module.
 */
void loadModule(const string& module)
{
    string p(read(getPreludePath()));                    // Read the prelude
    string m(read(module));                              // Read user module

    try                                                  // May fail to load
    {
        load(p + m);                                     // ...concat and load
    }
    catch (UserException&)
    {
        load(p);                                         // ...load prelude
        throw;                                           // ...rethrow error
    }
}

/****************************************************************************/
}
/****************************************************************************/
