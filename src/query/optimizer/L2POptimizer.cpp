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
 * @brief Dummy optimizer which producing physical plan from logical without any
 *                complicated transformations.
 *
 * @author Artyom Smirnov <smirnoffjr@gmail.com>
 */

#include <boost/foreach.hpp>
#include <boost/make_shared.hpp>
#include <log4cxx/logger.h>

#include "query/optimizer/Optimizer.h"
#include "query/OperatorLibrary.h"
#include "query/ParsingContext.h"
#include "system/SystemCatalog.h"
#include "network/NetworkManager.h"
#include "array/Metadata.h"

using namespace boost;
using namespace std;

namespace scidb
{

static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.qproc.optimizer"));

class L2POptimizer : public Optimizer
{
public:
    boost::shared_ptr<PhysicalPlan> optimize(const boost::shared_ptr<Query>& query,
                                             boost::shared_ptr<LogicalPlan>& logicalPlan);

private:
    boost::shared_ptr<PhysicalQueryPlanNode> traverse(const boost::shared_ptr<Query>& query,
                                                      boost::shared_ptr<LogicalQueryPlanNode> node);
};

boost::shared_ptr<PhysicalPlan> L2POptimizer::optimize(const boost::shared_ptr<Query>& query,
                                                       boost::shared_ptr<LogicalPlan>& logicalPlan)
{
    boost::shared_ptr<LogicalQueryPlanNode> root = logicalPlan->getRoot();

    if (!root) {
        return boost::make_shared<PhysicalPlan>(boost::shared_ptr<PhysicalQueryPlanNode>());
    }

    boost::shared_ptr<PhysicalPlan> physicalPlan = boost::make_shared<PhysicalPlan>(traverse(query, root));
    // null out the root
    logicalPlan->setRoot( boost::shared_ptr<LogicalQueryPlanNode>());

    return physicalPlan;
}

boost::shared_ptr<PhysicalQueryPlanNode> L2POptimizer::traverse(const boost::shared_ptr<Query>& query,
                                                                boost::shared_ptr<LogicalQueryPlanNode> node)
{
    node = logicalRewriteIfNeeded(query, node);
    boost::shared_ptr<LogicalOperator> logicalOp = node->getLogicalOperator();

    vector<string> physicalOperatorsNames;
    OperatorLibrary::getInstance()->getPhysicalNames(logicalOp->getLogicalName(), physicalOperatorsNames);
    const string &physicalName = physicalOperatorsNames[0];

    // Collection of input schemas of operator for resolving references
    vector< ArrayDesc> inputSchemas;

    // Adding children schemas
    const vector<boost::shared_ptr<LogicalQueryPlanNode> >& childs = node->getChildren();
    for (size_t ch = 0; ch < childs.size(); ch++)
    {
        inputSchemas.push_back(childs[ch]->getLogicalOperator()->getSchema());
    }

    const LogicalOperator::Parameters& lParams = logicalOp->getParameters();
    PhysicalOperator::Parameters phParams;
    for (size_t i = 0; i < lParams.size(); i++)
    {
        if (lParams[i]->getParamType() == PARAM_LOGICAL_EXPRESSION)
        {
            boost::shared_ptr<Expression> phParam = boost::make_shared<Expression>();

            boost::shared_ptr<OperatorParamLogicalExpression>& lParam = (boost::shared_ptr<OperatorParamLogicalExpression>&)lParams[i];

            try
            {
                if (lParam->isConstant())
                {
                   phParam->compile(lParam->getExpression(), query, false, lParam->getExpectedType().typeId());
                }
                else
                {
                   phParam->compile(lParam->getExpression(), query, false, lParam->getExpectedType().typeId(), inputSchemas, logicalOp->getSchema());
                }
            }
            catch (Exception &e)
            {
                if (e.getLongErrorCode() == SCIDB_LE_TYPE_CONVERSION_ERROR || e.getLongErrorCode() == SCIDB_LE_TYPE_CONVERSION_ERROR2)
                {
                    throw USER_QUERY_EXCEPTION(SCIDB_SE_SYNTAX, SCIDB_LE_PARAMETER_TYPE_ERROR, lParam->getParsingContext())
                        << lParam->getExpectedType().name() << TypeLibrary::getType(phParam->getType()).name();
                }
                else
                {
                    throw;
                }
            }

            phParams.push_back(boost::shared_ptr<OperatorParam>(
                    new OperatorParamPhysicalExpression(lParams[i]->getParsingContext(), phParam, lParam->isConstant())));
        }
        else
        {
            phParams.push_back(lParams[i]);
        }
    }

    boost::shared_ptr<PhysicalOperator> physicalOp = OperatorLibrary::getInstance()->
            createPhysicalOperator(logicalOp->getLogicalName(), physicalName, phParams, logicalOp->getSchema());
    physicalOp->setQuery(query);

    boost::shared_ptr<PhysicalQueryPlanNode> result(new PhysicalQueryPlanNode(physicalOp, false, node->isDdl(), node->supportsTileMode()));

    BOOST_FOREACH(boost::shared_ptr<LogicalQueryPlanNode> node_child, node->getChildren())
    {
       boost::shared_ptr <PhysicalQueryPlanNode> pChild = traverse(query, node_child);
       result->addChild(pChild);
    }

    /*
     * Adding global operator of two-phase operator considering by name of local operator
     */
    if (logicalOp->getGlobalOperatorName().first != "" && logicalOp->getGlobalOperatorName().second != "")
    {
        boost::shared_ptr<PhysicalOperator> globalOp = OperatorLibrary::getInstance()->
            createPhysicalOperator(logicalOp->getGlobalOperatorName().first,
                logicalOp->getGlobalOperatorName().second, PhysicalOperator::Parameters(), logicalOp->getSchema());
        globalOp->setQuery(query);

        boost::shared_ptr<PhysicalQueryPlanNode> globalNode(new PhysicalQueryPlanNode(globalOp, true, false, false));

        if ( query->getInstancesCount() > 1) {
            // Insert SG to scatter aggregated result
            ArrayDesc sgSchema =  logicalOp->getSchema();
            sgSchema.setName("");
            //  TODO[apoliakov]: sgSchema.setPartitioningSchema(psHashPartitioned); ??

            //Prepare SG operator and single parameter - array name equal original name
            //which used in LOAD statement
            PhysicalOperator::Parameters sgParams;

            boost::shared_ptr<Expression> psConst = boost::make_shared<Expression>();
            Value ps(TypeLibrary::getType(TID_INT32));
            ps.setInt32(psHashPartitioned);
            psConst->compile(false, TID_INT32, ps);
            sgParams.push_back(boost::shared_ptr<OperatorParam>(
                                   new OperatorParamPhysicalExpression(boost::make_shared<ParsingContext>(), psConst, true)));

            boost::shared_ptr<PhysicalOperator> sgOp =
                OperatorLibrary::getInstance()->createPhysicalOperator("sg", "impl_sg", sgParams, sgSchema);
            sgOp->setQuery(query);

            boost::shared_ptr<PhysicalQueryPlanNode> sgNode(new PhysicalQueryPlanNode(sgOp, false, false, false));

            sgNode->addChild(globalNode);
            globalNode->addChild(result);

            result = sgNode;
        } else {
            globalNode->addChild(result);
            result = globalNode;
        }
    }
    else if (result->changesDistribution() || !result->outputFullChunks())
    {
        // Dumbest thing we can do: if we're not ABSOLUTELY SURE the operator is distribution-preserving, insert an SG,
        // and redistribute data exactly as in the output schema of the operator

        ArrayDesc sgSchema = result->getPhysicalOperator()->getSchema();

        PhysicalOperator::Parameters sgParams;
        boost::shared_ptr<Expression> distributeExpression = boost::make_shared<Expression>();
        Value distributeType(TypeLibrary::getType(TID_INT32));
        distributeType.setInt32(psHashPartitioned);
        distributeExpression->compile(false, TID_INT32, distributeType);
        sgParams.push_back(boost::shared_ptr<OperatorParam>( new OperatorParamPhysicalExpression(boost::make_shared<ParsingContext>(),
                                                                                                 distributeExpression,
                                                                                                 true)));

        boost::shared_ptr<PhysicalOperator> sgOperator = OperatorLibrary::getInstance()->createPhysicalOperator("sg",
                                                                                                                "impl_sg",
                                                                                                                sgParams,
                                                                                                                sgSchema);
        sgOperator->setQuery(query);

        boost::shared_ptr<PhysicalQueryPlanNode> sgNode(new PhysicalQueryPlanNode(sgOperator,
                                                                                  false,
                                                                                  false,
                                                                                  false));
        sgNode->addChild(result);
        result = sgNode;
    }

    return result;
}

boost::shared_ptr<Optimizer> Optimizer::createL2POptimizer()
{
    LOG4CXX_DEBUG(logger, "Creating L2P optimizer instance")
    return boost::shared_ptr<Optimizer>(new L2POptimizer());
}

} // namespace
