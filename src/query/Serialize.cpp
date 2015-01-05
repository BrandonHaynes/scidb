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
 * @brief Routines for serializing physical plans to strings.
 *
 * @author Artyom Smirnov <smirnoffjr@gmail.com>
 */

#include <boost/format.hpp>
#include <boost/foreach.hpp>
#include <boost/archive/text_oarchive.hpp>
#include <boost/archive/text_iarchive.hpp>

#include <sstream>

#include "query/Serialize.h"
#include "query/QueryPlan.h"
#include "query/LogicalExpression.h"
#include "query/Operator.h"

using namespace std;
using namespace boost;
using namespace boost::archive;

namespace scidb
{

string serializePhysicalPlan(const shared_ptr<PhysicalPlan> &plan)
{
    stringstream ss;
    text_oarchive oa(ss);

    const shared_ptr<PhysicalQueryPlanNode> &queryRoot = plan->getRoot();

    oa.register_type(static_cast<OperatorParam*>(NULL));
    oa.register_type(static_cast<OperatorParamReference*>(NULL));
    oa.register_type(static_cast<OperatorParamArrayReference*>(NULL));
    oa.register_type(static_cast<OperatorParamAttributeReference*>(NULL));
    oa.register_type(static_cast<OperatorParamDimensionReference*>(NULL));
    oa.register_type(static_cast<OperatorParamLogicalExpression*>(NULL));
    oa.register_type(static_cast<OperatorParamPhysicalExpression*>(NULL));
    oa.register_type(static_cast<OperatorParamSchema*>(NULL));
    oa.register_type(static_cast<OperatorParamAggregateCall*>(NULL));
    oa.register_type(static_cast<OperatorParamAsterisk*>(NULL));
    oa & queryRoot;

    return ss.str();
}

string serializePhysicalExpression(const Expression &expr)
{
    stringstream ss;
    text_oarchive oa(ss);

    oa & expr;

    return ss.str();
}

Expression deserializePhysicalExpression(const string &str)
{
    Expression expr;
    stringstream ss;
    ss << str;
    text_iarchive ia(ss);
    ia & expr;

    return expr;
}


} //namespace scidb
