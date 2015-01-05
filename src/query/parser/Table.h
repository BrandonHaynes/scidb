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

#ifndef QUERY_PARSER_TABLE_H_
#define QUERY_PARSER_TABLE_H_

/****************************************************************************/

#include "ParserDetails.h"                               // For implementation

/****************************************************************************/
namespace scidb { namespace parser {
/****************************************************************************/

/**
 *  @brief     Represents an abstract symbol table.
 *
 *  @details   Class Table represents a collection of definition bindings that
 *             is efficiently searchable by identifier. The abstract interface
 *             allows us to substitute implementations optimized for different
 *             performance requirements easily, and allows concrete subclasses
 *             to use whatever data structure they find most appropriate.
 *
 *             Tables also hold an optional pointer to a parent table to which
 *             they delegate requests for bindings they do not contain. We use
 *             the pointers to build a tree of tables that mirrors the lexical
 *             structure of the program we are compiling. Every table inherits
 *             the bindings of its ancestors so to find the binding occurrence
 *             of a variable in the source text we need only consult the table
 *             that corresponds to the scope in which the reference occurs.
 *
 *  @author    jbell@paradigm4.com.
 */
class Table
{
 public:                   // Operations
    virtual size_t            size()           const = 0;// Number of bindings
    virtual Table*            getParent()      const = 0;// Return the parent
    virtual Node const*       get(const Name*) const = 0;// Search the table
    virtual void              accept(Visitor&) const = 0;// Visit each binding
};

/****************************************************************************/
}}
/****************************************************************************/
#endif
/****************************************************************************/
