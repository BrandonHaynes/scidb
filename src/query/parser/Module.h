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

#ifndef QUERY_PARSER_MODULE_H_
#define QUERY_PARSER_MODULE_H_

/****************************************************************************/

#include "ParserDetails.h"                               // For implementation

/****************************************************************************/
namespace scidb { namespace parser {
/****************************************************************************/

/**
 *  @brief      Represents the currently loaded module.
 *
 *  @details    Class Module provides a  simplified interface for manipulating
 *              the currently loaded module, a container for the various named
 *              entities that the user may define from within the language and
 *              then refer to by name from susbequent queries.
 *
 *              The module may be read from by many queries simultaneously but
 *              only loaded by a single query that first locks it for writing,
 *              and only then when no other query holds a read lock on it.
 *
 *              An instance of class Module represents a read or write lock on
 *              the real singleton module implementation, and, when created in
 *              'write' mode, provides the caller with the necessary interface
 *              to update the contents of the master module with a new list of
 *              variable bindings.
 *  @code
 *                  Module m(Module::write);             // Acquire write lock
 *
 *                  m.load(...);                         // Good, and now load
 *  @endcode
 *
 *  @author     jbell@paradigm4.com.
 */
class Module : noncopyable, stackonly
{
 public:                   // Types
    enum    mode              {read,write};

 public:                   // Construction
                              Module(mode = read);
                             ~Module();

 public:                   // Operations
            void              load(Log&,const Node*);    // Needs a write lock

 private:                  // Representation
            mode       const _mode;                      // The locking mode
};

/****************************************************************************/
}}
/****************************************************************************/
#endif
/****************************************************************************/
