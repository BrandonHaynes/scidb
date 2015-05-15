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
 * @file QueryProcessor.cpp
 *
 * @author pavel.velikhov@gmail.com, roman.simakov@gmail.com
 *
 * @brief The interface to the Query Processor in SciDB
 *
 * The QueryProcessor provides the interface to create and execute queries
 * in SciDB.
 * The class that handles all major query processing tasks is QueryProcessor, which
 * is a stateless, reentrant class. The client of the QueryProcessor however uses the
 * Query and QueryResult interfaces instead of the QueryProcessor interface.
 */

#include <time.h>
#include <boost/make_shared.hpp>
#include <boost/serialization/string.hpp>
#include <boost/archive/text_iarchive.hpp>
#include <log4cxx/logger.h>

#include <query/QueryProcessor.h>
#include <query/Parser.h>
#include <smgr/io/Storage.h>
#include <network/MessageUtils.h>
#include <network/NetworkManager.h>
#include <system/SciDBConfigOptions.h>
#include <system/SystemCatalog.h>
#include <system/Cluster.h>
#include <array/ParallelAccumulatorArray.h>
#include <util/Thread.h>

using namespace std;
using namespace boost;
using namespace boost::archive;

namespace scidb
{

// Logger for query processor. static to prevent visibility of variable outside of file
static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.qproc.processor"));

// Basic QueryProcessor implementation
class QueryProcessorImpl: public QueryProcessor
{
private:
    // Recursive method for executing physical plan
    boost::shared_ptr<Array> execute(boost::shared_ptr<PhysicalQueryPlanNode> node, boost::shared_ptr<Query> query, int depth);
    void preSingleExecute(boost::shared_ptr<PhysicalQueryPlanNode> node, boost::shared_ptr<Query> query);
    void postSingleExecute(boost::shared_ptr<PhysicalQueryPlanNode> node, boost::shared_ptr<Query> query);
    // Synchronization methods
    /**
     * Worker notifies coordinator about its state.
     * Coordinator waits for worker notifications.
     */
    void notify(boost::shared_ptr<Query>& query, uint64_t timeoutNanoSec=0);

    /**
     * Worker waits for a notification from coordinator.
     * Coordinator sends out notifications to all workers.
     */
    void wait(boost::shared_ptr<Query>& query);

public:
    boost::shared_ptr<Query> createQuery(string queryString, QueryID queryId);
    void parseLogical(boost::shared_ptr<Query> query, bool afl);
    void parsePhysical(const string& plan, boost::shared_ptr<Query> query);
    const ArrayDesc& inferTypes(boost::shared_ptr<Query> query);
    void setParameters(boost::shared_ptr<Query> query, QueryParamMap queryParams);
    bool optimize(boost::shared_ptr<Optimizer> optimizer, boost::shared_ptr<Query> query);
    void preSingleExecute(boost::shared_ptr<Query> query);
    void execute(boost::shared_ptr<Query> query);
    void postSingleExecute(boost::shared_ptr<Query> query);
    void inferArrayAccess(boost::shared_ptr<Query> query);
};


boost::shared_ptr<Query> QueryProcessorImpl::createQuery(string queryString, QueryID queryID)
{
    assert(queryID > 0);
    boost::shared_ptr<Query> query = Query::create(queryID);
    query->queryString = queryString;

    return query;
}


void QueryProcessorImpl::parseLogical(boost::shared_ptr<Query> query, bool afl)
{
    query->logicalPlan = boost::make_shared<LogicalPlan>(parseStatement(query,afl));
}


void QueryProcessorImpl::parsePhysical(const std::string& plan, boost::shared_ptr<Query> query)
{
    assert(!plan.empty());

    boost::shared_ptr<PhysicalQueryPlanNode> node;

    stringstream ss;
    ss << plan;
    text_iarchive ia(ss);
    ia.register_type(static_cast<OperatorParam*>(NULL));
    ia.register_type(static_cast<OperatorParamReference*>(NULL));
    ia.register_type(static_cast<OperatorParamArrayReference*>(NULL));
    ia.register_type(static_cast<OperatorParamAttributeReference*>(NULL));
    ia.register_type(static_cast<OperatorParamDimensionReference*>(NULL));
    ia.register_type(static_cast<OperatorParamLogicalExpression*>(NULL));
    ia.register_type(static_cast<OperatorParamPhysicalExpression*>(NULL));
    ia.register_type(static_cast<OperatorParamSchema*>(NULL));
    ia.register_type(static_cast<OperatorParamAggregateCall*>(NULL));
    ia.register_type(static_cast<OperatorParamAsterisk*>(NULL));
    ia & node;

    query->addPhysicalPlan(boost::make_shared<PhysicalPlan>(node));
}


const ArrayDesc& QueryProcessorImpl::inferTypes(boost::shared_ptr<Query> query)
{
    return query->logicalPlan->inferTypes(query);
}

void QueryProcessorImpl::inferArrayAccess(boost::shared_ptr<Query> query)
{
    return query->logicalPlan->inferArrayAccess(query);
}


bool QueryProcessorImpl::optimize(boost::shared_ptr<Optimizer> optimizer, boost::shared_ptr<Query> query)
{
   query->addPhysicalPlan(optimizer->optimize(query, query->logicalPlan));

    return !query->getCurrentPhysicalPlan()->empty();
}


void QueryProcessorImpl::setParameters(boost::shared_ptr<Query> query, QueryParamMap queryParams)
{
}


// Recursive method for single executing physical plan
void QueryProcessorImpl::preSingleExecute(boost::shared_ptr<PhysicalQueryPlanNode> node, boost::shared_ptr<Query> query)
{
    Query::validateQueryPtr(query);

    boost::shared_ptr<PhysicalOperator> physicalOperator = node->getPhysicalOperator();

    vector<boost::shared_ptr<PhysicalQueryPlanNode> >& childs = node->getChildren();
    for (size_t i = 0; i < childs.size(); i++) {
        preSingleExecute(childs[i], query);
    }

    StatisticsScope sScope(&physicalOperator->getStatistics());
    physicalOperator->preSingleExecute(query);
}


void QueryProcessorImpl::preSingleExecute(boost::shared_ptr<Query> query)
{
    LOG4CXX_DEBUG(logger, "(Pre)Single executing queryID: " << query->getQueryID())

    preSingleExecute(query->getCurrentPhysicalPlan()->getRoot(), query);
}

void QueryProcessorImpl::postSingleExecute(boost::shared_ptr<PhysicalQueryPlanNode> node, boost::shared_ptr<Query> query)
{
   Query::validateQueryPtr(query);

    boost::shared_ptr<PhysicalOperator> physicalOperator = node->getPhysicalOperator();

    vector<boost::shared_ptr<PhysicalQueryPlanNode> >& childs = node->getChildren();
    for (size_t i = 0; i < childs.size(); i++) {
        postSingleExecute(childs[i], query);
    }

    StatisticsScope sScope(&physicalOperator->getStatistics());
    physicalOperator->postSingleExecute(query);
}


void QueryProcessorImpl::postSingleExecute(boost::shared_ptr<Query> query)
{
    LOG4CXX_DEBUG(logger, "(Post)Single executing queryID: " << query->getQueryID())

    postSingleExecute(query->getCurrentPhysicalPlan()->getRoot(), query);
}

// Recursive method for executing physical plan
boost::shared_ptr<Array> QueryProcessorImpl::execute(boost::shared_ptr<PhysicalQueryPlanNode> node, boost::shared_ptr<Query> query, int depth)
{
    Query::validateQueryPtr(query);

    boost::shared_ptr<PhysicalOperator> physicalOperator = node->getPhysicalOperator();
    physicalOperator->setQuery(query);

    vector<boost::shared_ptr<Array> > operatorArguments;
    vector<boost::shared_ptr<PhysicalQueryPlanNode> >& childs = node->getChildren();

    StatisticsScope sScope(&physicalOperator->getStatistics());
    if (node->isAgg())
    {
        const size_t numInstances = query->getInstancesCount();

        // This assert should be provided by optimizer
        assert(childs.size() == 1);

        boost::shared_ptr<Array> currentResultArray = execute(childs[0], query, depth+1);
        assert(currentResultArray);

        // Prepare RemoteArrayContext.
        //   - worker instance: store the local result in RemoteArrayContext::_outboundArrays[0].
        //   - coordinator instance: create a vector of RemoteArray objects, and store in RemoteArrayContext::_inboundArrays.
        shared_ptr<RemoteArrayContext> remoteArrayContext = make_shared<RemoteArrayContext>(numInstances);

        if (! query->isCoordinator()) {
            if (Config::getInstance()->getOption<int>(CONFIG_RESULT_PREFETCH_QUEUE_SIZE) > 1 && currentResultArray->getSupportedAccess() == Array::RANDOM) {
                boost::shared_ptr<ParallelAccumulatorArray> paa = boost::make_shared<ParallelAccumulatorArray>(currentResultArray);
                currentResultArray = paa;
                paa->start(query);
            } else {
                currentResultArray = boost::make_shared<AccumulatorArray>(currentResultArray,query);
            }
            remoteArrayContext->setOutboundArray(query->getCoordinatorID(), currentResultArray);
        }
        else {
            for (size_t i = 0; i < numInstances; i++)
            {
                boost::shared_ptr<Array> arg;
                if (i != query->getInstanceID()) {
                    arg = RemoteArray::create(remoteArrayContext, currentResultArray->getArrayDesc(), query->getQueryID(), i);
                } else {
                    arg = currentResultArray;
                }
                if (!arg)
                    throw SYSTEM_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_NO_OPERATOR_RESULT);
                operatorArguments.push_back(arg);
            }
        }

        // Record RemoteArrayContext in the query context.
        ASSERT_EXCEPTION(!query->getOperatorContext(), "In QueryProcessorImpl, operator context is supposed to be empty.");
        query->setOperatorContext(remoteArrayContext);
        notify(query);

        if (query->isCoordinator())
        {
            /**
             * TODO: we need to get whole result on this instance before we call wait(query)
             * because we hope on currentResultArray of remote instances but it lives until we send wait notification
             */
            boost::shared_ptr<Array> res = physicalOperator->executeWrapper(operatorArguments, query);
            wait(query);

            // Unset remote array context.
            query->unsetOperatorContext();

            return res;
        }
        else
        {
            wait(query);

            // Unset remote array context.
            query->unsetOperatorContext();

            /**
             * TODO: This is temporary. 2-nd phase is performed only on coordinator but
             * other instances can continue execution with empty array with the same schema.
             * All data should be on coordinator.
             * But we also can't do it if coordinator will request data as pipeline.
             * For example count2 uses MergeArray.
             */
            return depth != 0
                ? boost::shared_ptr<Array>(new MemArray(physicalOperator->getSchema(), query))
                : currentResultArray;
        }
    }
    else if (node->isDdl())
    {
        physicalOperator->executeWrapper(operatorArguments, query);
        return boost::shared_ptr<Array>();
    }
    else
    {
        for (size_t i = 0; i < childs.size(); i++)
        {
            boost::shared_ptr<Array> arg = execute(childs[i], query, depth+1);
            if (!arg)
                throw SYSTEM_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_NO_OPERATOR_RESULT);
            operatorArguments.push_back(arg);
        }
        return physicalOperator->executeWrapper(operatorArguments, query);
    }
}

void QueryProcessorImpl::execute(boost::shared_ptr<Query> query)
{
    LOG4CXX_INFO(logger, "Executing query(" << query->getQueryID() << "): " << query->queryString <<
                 "; from program: " << query->programOptions << ";")

    // Make sure ALL instance are ready to run. If the coordinator does not hear
    // from the workers within a timeout, the query is aborted. This is done to prevent a deadlock
    // caused by thread starvation. XXX TODO: In a long term we should solve the problem of thread starvation
    // using for example asynchronous execution techniques.
    int deadlockTimeoutSec = Config::getInstance()->getOption<int>(CONFIG_DEADLOCK_TIMEOUT);
    if (deadlockTimeoutSec <= 0) {
        deadlockTimeoutSec = 10;
    }
    static const uint64_t NANOSEC_PER_SEC = 1000 * 1000 * 1000;
    notify(query, static_cast<uint64_t>(deadlockTimeoutSec)*NANOSEC_PER_SEC);
    wait(query);

    Query::validateQueryPtr(query);

    boost::shared_ptr<PhysicalQueryPlanNode> rootNode = query->getCurrentPhysicalPlan()->getRoot();
    boost::shared_ptr<Array> currentResultArray = execute(rootNode, query, 0);

    Query::validateQueryPtr(query);

    if (currentResultArray)
    {
        if (Config::getInstance()->getOption<int>(CONFIG_RESULT_PREFETCH_QUEUE_SIZE) > 1 && currentResultArray->getSupportedAccess() == Array::RANDOM) {
            if (typeid(*currentResultArray) != typeid(ParallelAccumulatorArray)) {
               boost::shared_ptr<ParallelAccumulatorArray> paa = boost::make_shared<ParallelAccumulatorArray>(currentResultArray);
               currentResultArray = paa;
               paa->start(query);
            }
        } else {
            if (typeid(*currentResultArray) != typeid(AccumulatorArray)) {
                currentResultArray = boost::make_shared<AccumulatorArray>(currentResultArray,query);
            }
        }
        if (query->getInstancesCount() > 1 &&
            query->isCoordinator() &&
            !rootNode->isAgg() && !rootNode->isDdl())
        {
            // RemoteMergedArray uses the Query::_currentResultArray as its local (stream) array
            // so make sure to set it in advance
            query->setCurrentResultArray(currentResultArray);
            currentResultArray = RemoteMergedArray::create(currentResultArray->getArrayDesc(),
                    query->getQueryID(), query->statistics);
        }
    }
    query->setCurrentResultArray(currentResultArray);
}

namespace {
bool validateQueryWithTimeout(uint64_t startTime,
                              uint64_t timeout,
                              boost::shared_ptr<Query>& query)
{
    bool rc = query->validate();
    assert(rc);
    if (hasExpired(startTime, timeout)) {
        throw (SYSTEM_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_RESOURCE_BUSY)
               << "not enough resources to start a query");
    }
    return rc;
}
}

void QueryProcessorImpl::notify(boost::shared_ptr<Query>& query, uint64_t timeoutNanoSec)
{
   if (! query->isCoordinator())
    {
        QueryID queryID = query->getQueryID();
        LOG4CXX_DEBUG(logger, "Sending notification in queryID: " << queryID << " to coord instance #" << query->getCoordinatorID())
        boost::shared_ptr<MessageDesc> messageDesc = makeNotifyMessage(queryID);

        NetworkManager::getInstance()->send(query->getCoordinatorID(), messageDesc);
    }
    else
    {
        const size_t instancesCount = query->getInstancesCount() - 1;
        LOG4CXX_DEBUG(logger, "Waiting notification in queryID from " << instancesCount << " instances")
        Semaphore::ErrorChecker errorChecker;
        if (timeoutNanoSec > 0) {
            errorChecker = bind(&validateQueryWithTimeout, getTimeInNanoSecs(), timeoutNanoSec, query);
        } else {
            errorChecker = bind(&Query::validate, query);
        }
        query->results.enter(instancesCount, errorChecker);
    }
}

void QueryProcessorImpl::wait(boost::shared_ptr<Query>& query)
{
   if (query->isCoordinator())
    {
        QueryID queryID = query->getQueryID();
        LOG4CXX_DEBUG(logger, "Send message from coordinator for waiting instances in queryID: " << query->getQueryID())
        boost::shared_ptr<MessageDesc> messageDesc = makeWaitMessage(queryID);

        NetworkManager::getInstance()->broadcastLogical(messageDesc);
    }
    else
    {
        LOG4CXX_DEBUG(logger, "Waiting notification in queryID from coordinator")
        Semaphore::ErrorChecker errorChecker = bind(&Query::validate, query);
        query->results.enter(errorChecker);
    }
}

/**
 * QueryProcessor static method implementation
 */

boost::shared_ptr<QueryProcessor> QueryProcessor::create()
{
    return boost::shared_ptr<QueryProcessor>(new QueryProcessorImpl());
}


} // namespace
