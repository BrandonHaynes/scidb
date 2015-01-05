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
 * @file QueryProcessor.h
 *
 * @author pavel.velikhov@gmail.com, roman.simakov@gmail.com
 *
 * @brief The interface to the Query Processor in SciDB
 *
 * The QueryProcessor provides the interface to create and execute queries in SciDB.
 * The class that handles all major query processing tasks is QueryProcessor, which
 * is a stateless, reentrant class. The client of the QueryProcessor however uses the
 * Query and QueryResult interfaces instead of the QueryProcessor interface.
 */

#ifndef QUERY_PROCESSOR_H_
#define QUERY_PROCESSOR_H_

#include <boost/shared_ptr.hpp>
#include <string>
#include <queue>
#include <stdio.h>

#include "array/Metadata.h"
#include "array/Array.h"
#include "query/Operator.h"
#include "query/QueryPlan.h"
#include "query/optimizer/Optimizer.h"
#include "util/Semaphore.h"
#include "util/Event.h"
#include "array/StreamArray.h"
#include "util/RWLock.h"
#include "SciDBAPI.h"
#include "query/RemoteArray.h"

namespace scidb
{

/**
 * The stub for data type
 */
struct QueryParamMap
{

};

/**
 * The query processor is the interface to all major query processing tasks in SciDB.
 * Methods are sorted by calling stage.
 * Implementation of QueryProcessor should not save any state of execution. To do that it must use
 * Query object.
 */
class QueryProcessor
{
public:
    virtual ~QueryProcessor() {}
    /**
     * Creates query from query received from the user.
     */
   virtual boost::shared_ptr<Query> createQuery(std::string queryString, QueryID queryId) = 0;

    //TODO: [ap] want to combine parseLogical, semanticCheck and inferTypes into single function called prepareLogical(). Objections?

    /**
     * Parse the query string into logical plan
     */
    virtual void parseLogical(boost::shared_ptr<Query> query, bool afl) = 0;

    /**
     * Parse the query string into physical plan
     */
    virtual void parsePhysical(const std::string& plan, boost::shared_ptr<Query> query) = 0;

    /**
     * Infers types through logical tree
     */
    virtual const ArrayDesc& inferTypes(boost::shared_ptr<Query> query) = 0;

    /**
     * Examine the logical tree and let the operators request array locks
     */
    virtual void inferArrayAccess(boost::shared_ptr<Query> query) = 0;

    /**
     * Optimizes current logical tree. It must leave the rest of logical plan in query and assign to physical plan
     * new one for sending out and execution.
     * @return true if there is physical plan for execution, false - if there is nothing to execute.
     */
    virtual bool optimize(boost::shared_ptr< Optimizer> optimizer, boost::shared_ptr<Query> query) = 0;

    /**
     * Set parameters of query before execution
     */
    virtual void setParameters(boost::shared_ptr<Query> query, QueryParamMap queryParams) = 0;

    /**
     * Execute the physical plan in query only for coordinator instance.
     * It's useful for some preparations before execution.
     */
    virtual void preSingleExecute(boost::shared_ptr<Query> query) = 0;

    /**
     * Execute the physical plan in query only for coordinator instance after execute part on all instances.
     */
    virtual void postSingleExecute(boost::shared_ptr<Query> query) = 0;

    /**
     * Execute the physical plan in query. It doesn't perform any additional check. Just perform operators.
     * All operators must presents and system consistency must be checked before.
     */
    virtual void execute(boost::shared_ptr<Query> query) = 0;

    /**
     * Creates an object implementing QueryProcessor interface.
     */
    static boost::shared_ptr<QueryProcessor> create();
};

}

#endif /* QUERY_PROCESSOR_H_ */
