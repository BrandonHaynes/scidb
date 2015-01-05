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
 * @file QueryPlanUtilites.cpp
 *
 * @brief Plan helper functions
 *
 * @author Oleg Tsarev <oleg@oleg.sh>
 */


#include "query/QueryPlanUtilites.h"
#include "query/QueryPlan.h"

namespace scidb
{

const PhysNodePtr getRoot(PhysNodePtr node)
{
    if (!node) {
        return node;
    }

    while(node->hasParent()) {
        node = node->getParent();
    }

    return node;
}

void printPlan(std::ostream& out, PhysNodePtr node, int indent, bool children)
{
    PhysPlanPtr plan(new PhysicalPlan(node));
    plan->toString(out, indent, children);
}

void logPlan(log4cxx::LoggerPtr logger,
             log4cxx::LevelPtr level,
             PhysNodePtr node,
             int indent,
             bool children)
{
    if(level->isGreaterOrEqual(logger->getEffectiveLevel())) {
        std::ostringstream out;
        PhysPlanPtr plan(new PhysicalPlan(node));
        plan->toString(out, 0, children);
        LOG4CXX_LOG(logger, level, out.str().c_str());
    }
}

void printPlan(PhysNodePtr node, int indent, bool children)
{
    printPlan(std::cout, node, indent, children);
}

void logPlanDebug(log4cxx::LoggerPtr logger, PhysNodePtr node, int indent, bool children)
{
    if (logger->isDebugEnabled()) {
        logPlan(logger, ::log4cxx::Level::getDebug(), node, indent, children);
    }
}

void logPlanTrace(log4cxx::LoggerPtr logger, PhysNodePtr node, int indent, bool children)
{
    if (logger->isTraceEnabled()) {
        logPlan(logger, ::log4cxx::Level::getTrace(), node, indent, children);
    }
}


} /* namespace scidb */
