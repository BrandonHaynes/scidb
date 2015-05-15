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

#ifndef QUERY_PARSER_H_
#define QUERY_PARSER_H_

/****************************************************************************/

#include <boost/shared_ptr.hpp>                           // For shared_ptr

/****************************************************************************/
namespace scidb {
/****************************************************************************/

class Query;
class Array;
class ArrayDesc;
class LogicalExpression;
class LogicalQueryPlanNode;

/****************************************************************************/
namespace arena {typedef boost::shared_ptr<class Arena> ArenaPtr;}
/****************************************************************************/

boost::shared_ptr<LogicalQueryPlanNode> parseStatement (const boost::shared_ptr<Query>&,bool afl);
boost::shared_ptr<LogicalExpression>    parseExpression(const std::string&);

/****************************************************************************/

void                                    loadPrelude();
void                                    loadModule(const std::string&);

/****************************************************************************/

ArrayDesc                               logicalListMacros ();
boost::shared_ptr<Array>                physicalListMacros(const arena::ArenaPtr&);

/****************************************************************************/
}
/****************************************************************************/
#endif
/****************************************************************************/
