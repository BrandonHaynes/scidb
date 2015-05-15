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
 * @file HabilisOptimizer.cpp
 *
 * @brief Our first attempt at a halfway intelligent optimizer.
 * habilis (adj.) Latin: fit, easy, adaptable, apt, handy, well-adapted, inventive,..
 *
 * @author poliocough@gmail.com
 */

#include <boost/foreach.hpp>
#include <boost/make_shared.hpp>
#include <log4cxx/logger.h>

#include <fstream>
#include <iostream>
#include <iomanip>

#include "query/QueryPlanUtilites.h"
#include "query/optimizer/Optimizer.h"
#include "query/optimizer/HabilisOptimizer.h"
#include "query/ParsingContext.h"
#include "system/SystemCatalog.h"
#include "network/NetworkManager.h"
#include "array/Metadata.h"
#include "array/DelegateArray.h"

#include <iostream>

using namespace boost;
using namespace std;

namespace scidb
{

static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.qproc.optimizer"));

HabilisOptimizer::HabilisOptimizer(): _root(),
        _featureMask(
                     CONDENSE_SG
                   | INSERT_REPART
                   | REWRITE_STORING_SG
         )
{
    _featureMask |= INSERT_MATERIALIZATION;

    const char* path = "/tmp/scidb_optimizer_override";
    std::ifstream inFile (path);
    if (inFile && !inFile.eof())
    {
        inFile >> _featureMask;
        LOG4CXX_DEBUG(logger, "Feature mask overridden to "<<_featureMask);
    }
    inFile.close();
}


PhysPlanPtr HabilisOptimizer::optimize(const boost::shared_ptr<Query>& query,
                                       boost::shared_ptr<LogicalPlan>& logicalPlan)
{
    assert(_root.get() == NULL);
    assert(_query.get() == NULL);

    Eraser onStack(*this);

    _query = query;
    assert(_query);

    boost::shared_ptr<LogicalQueryPlanNode> logicalRoot = logicalPlan->getRoot();
    if (!logicalRoot)
    {   return PhysPlanPtr(new PhysicalPlan(_root)); }

    bool tileMode = Config::getInstance()->getOption<int>(CONFIG_TILE_SIZE) > 1;
    _root = tw_createPhysicalTree(logicalRoot, tileMode);

    if (!logicalPlan->getRoot()->isDdl())
    {
        if (isFeatureEnabled(INSERT_REPART))
        {
            tw_insertRepartNodes(_root);
        }

        tw_insertSgNodes(_root);

        if (isFeatureEnabled(CONDENSE_SG))
        {
            LOG4CXX_TRACE(logger, "CONDENSE_SG: begin");

            tw_collapseSgNodes(_root);

            while (tw_pushupJoinSgs(_root))
            {
                tw_collapseSgNodes(_root);
            }

            LOG4CXX_TRACE(logger, "CONDENSE_SG: end");
        }

        if (isFeatureEnabled(INSERT_MATERIALIZATION))
        {
            tw_insertChunkMaterializers(_root);
        }

        if (isFeatureEnabled(REWRITE_STORING_SG) && query->getInstancesCount()>1)
        {
            tw_rewriteStoringSG(_root);
        }
    }

    PhysPlanPtr result(new PhysicalPlan(_root));
    // null out the root
    logicalPlan->setRoot(boost::shared_ptr<LogicalQueryPlanNode>());

    return result;
}

void HabilisOptimizer::printPlan(PhysNodePtr node, bool children)
{
    if (!node) {
        node = _root;
    }
    scidb::printPlan(node, 0, children);
}

void HabilisOptimizer::logPlanDebug(PhysNodePtr node, bool children)
{
    if (!node) {
        node = _root;
    }
    scidb::logPlanDebug(logger, node, 0, children);
}

void HabilisOptimizer::logPlanTrace(PhysNodePtr node, bool children)
{
    if (!node) {
        node = _root;
    }
    scidb::logPlanTrace(logger, node, 0, children);
}

void HabilisOptimizer::n_addParentNode(PhysNodePtr target, PhysNodePtr nodeToInsert)
{
    LOG4CXX_TRACE(logger, "[n_addParentNode] begin");
    LOG4CXX_TRACE(logger, "[n_addParentNode] node to insert:");
    logPlanTrace(nodeToInsert, false);
    LOG4CXX_TRACE(logger, "[n_addParentNode] target tree:");
    logPlanTrace(target);

    if (target->hasParent())
    {
        PhysNodePtr parent = target->getParent();
        parent->replaceChild(target, nodeToInsert);
    }
    else
    {
        assert(_root == target);
        _root = nodeToInsert;
        _root->resetParent();   // paranoid
    }

    nodeToInsert->addChild(target);

    LOG4CXX_TRACE(logger, "[n_addParentNode] done");
    logPlanTrace();
    LOG4CXX_TRACE(logger, "[n_addParentNode] end");
}

void HabilisOptimizer::n_cutOutNode(PhysNodePtr nodeToRemove)
{
    LOG4CXX_TRACE(logger, "[n_cutOutNode] begin");
    logPlanTrace(nodeToRemove, false);
    vector<PhysNodePtr> children = nodeToRemove->getChildren();
    assert(children.size()<=1);

    if (nodeToRemove->hasParent())
    {
        PhysNodePtr parent = nodeToRemove->getParent();
        if (children.size() == 1)
        {
            PhysNodePtr child = children[0];
            parent->replaceChild(nodeToRemove, child);
        }
        else
        {
            parent->removeChild(nodeToRemove);
        }
    }

    else
    {
        assert(_root == nodeToRemove);
        if (children.size() == 1)
        {
            PhysNodePtr child = children[0];
            _root = child;
            _root->resetParent();
        }
        else
        {
            _root.reset();
        }
    }
    LOG4CXX_TRACE(logger, "[n_cutOutNode] done");
    logPlanTrace();
    LOG4CXX_TRACE(logger, "[n_cutOutNode] end");
}

boost::shared_ptr<OperatorParam> HabilisOptimizer::n_createPhysicalParameter(const boost::shared_ptr<OperatorParam> & logicalParameter,
                                                                      const vector<ArrayDesc>& logicalInputSchemas,
                                                                      const ArrayDesc& logicalOutputSchema,
                                                                      bool tile)
{
    if (logicalParameter->getParamType() == PARAM_LOGICAL_EXPRESSION)
    {
        boost::shared_ptr<Expression> physicalExpression = boost::make_shared<Expression> ();
        boost::shared_ptr<OperatorParamLogicalExpression>& logicalExpression = (boost::shared_ptr<OperatorParamLogicalExpression>&) logicalParameter;
        try
        {
            if (logicalExpression->isConstant())
            {
               physicalExpression->compile(logicalExpression->getExpression(), _query, tile, logicalExpression->getExpectedType().typeId());
            }
            else
            {
               physicalExpression->compile(logicalExpression->getExpression(), _query, tile, logicalExpression->getExpectedType().typeId(), logicalInputSchemas,
                                            logicalOutputSchema);
            }
            if (tile && !physicalExpression->supportsTileMode()) {
                return boost::shared_ptr<OperatorParam>();
            }
            return boost::shared_ptr<OperatorParam> (new OperatorParamPhysicalExpression(logicalParameter->getParsingContext(), physicalExpression,
                                                                                  logicalExpression->isConstant()));
        } catch (Exception &e)
        {
            if (e.getLongErrorCode() == SCIDB_LE_TYPE_CONVERSION_ERROR || e.getLongErrorCode() == SCIDB_LE_TYPE_CONVERSION_ERROR2)
            {
                throw USER_QUERY_EXCEPTION(SCIDB_SE_SYNTAX, SCIDB_LE_PARAMETER_TYPE_ERROR, logicalExpression->getParsingContext())
                    << logicalExpression->getExpectedType().name() << TypeLibrary::getType(physicalExpression->getType()).name();
            }
            else
            {
                throw;
            }
        }
    }
    else
    {
        return logicalParameter;
    }
}

PhysNodePtr HabilisOptimizer::n_createPhysicalNode(boost::shared_ptr<LogicalQueryPlanNode> logicalNode,
                                                   bool tileMode)
{
    const boost::shared_ptr<LogicalOperator>& logicalOp = logicalNode->getLogicalOperator();
    const string& logicalName = logicalOp->getLogicalName();

    OperatorLibrary* opLibrary = OperatorLibrary::getInstance();
    vector<string> physicalOperatorsNames;
    opLibrary->getPhysicalNames(logicalName, physicalOperatorsNames);
    const string &physicalName = physicalOperatorsNames[0];
    const vector<boost::shared_ptr<LogicalQueryPlanNode> >& children = logicalNode->getChildren();

    // Collection of input schemas of operator for resolving references
    vector<ArrayDesc> inputSchemas;
    tileMode &= logicalOp->getProperties().tile;
    for (size_t ch = 0; ch < children.size(); ch++)
    {
        inputSchemas.push_back(children[ch]->getLogicalOperator()->getSchema());
    }
    const ArrayDesc& outputSchema = logicalOp->getSchema();

    const LogicalOperator::Parameters& logicalParams = logicalOp->getParameters();
    size_t nParams = logicalParams.size();
    PhysicalOperator::Parameters physicalParams(nParams);

  Retry:
    for (size_t i = 0; i < nParams; i++)
    {
        bool paramTileMode = tileMode && logicalOp->compileParamInTileMode(i);
        boost::shared_ptr<OperatorParam> param = n_createPhysicalParameter(logicalParams[i], inputSchemas, outputSchema, paramTileMode);

        if (!param) {
            assert(paramTileMode);
            tileMode = false;
            goto Retry;
        }
        physicalParams[i] = param;
    }

    PhysOpPtr physicalOp = opLibrary->createPhysicalOperator(logicalName, physicalName, physicalParams, outputSchema);
    physicalOp->setQuery(_query);
    physicalOp->setTileMode(tileMode);
    return PhysNodePtr(new PhysicalQueryPlanNode(physicalOp, false, logicalNode->isDdl(), tileMode));
}

PhysNodePtr HabilisOptimizer::n_buildSgNode(const ArrayDesc & outputSchema,
                                            PartitioningSchema partSchema, bool storeArray)
{
    PhysicalOperator::Parameters sgParams;

    boost::shared_ptr<Expression> psConst = boost::make_shared<Expression> ();
    Value ps(TypeLibrary::getType(TID_INT32));

    ps.setInt32(partSchema);

    psConst->compile(false, TID_INT32, ps);
    sgParams.push_back(boost::shared_ptr<OperatorParam> (new OperatorParamPhysicalExpression(boost::make_shared<ParsingContext>(), psConst, true)));
    LOG4CXX_TRACE(logger, "Building SG node, output schema = "<<outputSchema);
    if (storeArray)
    {
       boost::shared_ptr<Expression> instanceConst = boost::make_shared<Expression> ();
        Value instance(TypeLibrary::getType(TID_INT64));
        instance.setInt64(-1);
        instanceConst->compile(false, TID_INT64, instance);
        sgParams.push_back(boost::shared_ptr<OperatorParam> (new OperatorParamPhysicalExpression(boost::make_shared<ParsingContext>(),
                                                                                          instanceConst,
                                                                                          true)));
        LOG4CXX_TRACE(logger, "Building storing SG node, output schema name = "<<outputSchema.getName());
        sgParams.push_back(boost::shared_ptr<OperatorParam> (new OperatorParamArrayReference(boost::make_shared<ParsingContext>(),
                                                                                      "",
                                                                                      outputSchema.getName(),
                                                                                      true)));
    }

    PhysOpPtr sgOp = OperatorLibrary::getInstance()->createPhysicalOperator("sg", "impl_sg", sgParams, outputSchema);
    sgOp->setQuery(_query);

    PhysNodePtr sgNode(new PhysicalQueryPlanNode(sgOp, false, false, false));
    return sgNode;
}

PhysNodePtr HabilisOptimizer::n_buildReducerNode(PhysNodePtr const& child, PartitioningSchema partSchema)
{
    //insert a distro reducer node. In this branch sgNeeded is always false.
    PhysicalOperator::Parameters reducerParams;
    boost::shared_ptr<Expression> psConst = boost::make_shared<Expression> ();
    Value ps(TypeLibrary::getType(TID_INT32));
    ps.setInt32(partSchema);
    psConst->compile(false, TID_INT32, ps);
    reducerParams.push_back(boost::shared_ptr<OperatorParam> (new OperatorParamPhysicalExpression(boost::make_shared<ParsingContext>(), psConst, true)));
    PhysOpPtr reducerOp = OperatorLibrary::getInstance()->createPhysicalOperator("reduce_distro",
                                                                                 "physicalReduceDistro",
                                                                                 reducerParams,
                                                                                 child->getPhysicalOperator()->getSchema());
    reducerOp->setQuery(_query);
    bool useTileMode = child->getPhysicalOperator()->getTileMode();
    PhysNodePtr reducerNode(new PhysicalQueryPlanNode(reducerOp, false, false, useTileMode));
    reducerNode->getPhysicalOperator()->setTileMode(useTileMode );
    return reducerNode;
}

PhysNodePtr HabilisOptimizer::tw_createPhysicalTree(boost::shared_ptr<LogicalQueryPlanNode> logicalRoot, bool tileMode)
{
   logicalRoot = logicalRewriteIfNeeded(_query, logicalRoot);

    vector<boost::shared_ptr<LogicalQueryPlanNode> > logicalChildren = logicalRoot->getChildren();
    vector<PhysNodePtr> physicalChildren(logicalChildren.size());
    bool rootTileMode = tileMode;
    for (size_t i = 0; i < logicalChildren.size(); i++)
    {
        boost::shared_ptr<LogicalQueryPlanNode> logicalChild = logicalChildren[i];
        PhysNodePtr physicalChild = tw_createPhysicalTree(logicalChild, tileMode);
        rootTileMode &= physicalChild->getPhysicalOperator()->getTileMode();
        physicalChildren[i] = physicalChild;
    }
    PhysNodePtr physicalRoot = n_createPhysicalNode(logicalRoot, rootTileMode);

    if (physicalRoot->isSgNode())
    {
        //this is a user-inserted explicit SG. So we don't mess with it.
        physicalRoot->setSgMovable(false);
        physicalRoot->setSgOffsetable(false);
    }
    for (size_t i = 0; i < physicalChildren.size(); i++)
    {
        PhysNodePtr physicalChild = physicalChildren[i];
        physicalRoot->addChild(physicalChild);
    }
    boost::shared_ptr<LogicalOperator> logicalOp = logicalRoot->getLogicalOperator();
    if (logicalOp->getGlobalOperatorName().first != "" && logicalOp->getGlobalOperatorName().second != "")
    {
        PhysOpPtr globalOp =
                OperatorLibrary::getInstance()->createPhysicalOperator(logicalOp->getGlobalOperatorName().first,
                                                                       logicalOp->getGlobalOperatorName().second,
                                                                       PhysicalOperator::Parameters(),
                                                                       logicalOp->getSchema());
        globalOp->setQuery(_query);
        PhysNodePtr globalNode(new PhysicalQueryPlanNode(globalOp, true, false, false));
        physicalRoot->inferBoundaries();
        globalNode->addChild(physicalRoot);
        physicalRoot = globalNode;
    }

    physicalRoot->inferBoundaries();
    return physicalRoot;
}

static void s_setSgDistribution(PhysNodePtr sgNode,
                                ArrayDistribution const& dist)
{
    if (dist.isUndefined())
        throw SYSTEM_EXCEPTION(SCIDB_SE_OPTIMIZER, SCIDB_LE_CANT_CREATE_SG_WITH_UNDEFINED_DISTRIBUTION);

    PhysicalOperator::Parameters _parameters = sgNode->getPhysicalOperator()->getParameters();
    PhysicalOperator::Parameters newParameters;

    boost::shared_ptr<Expression> psConst = boost::make_shared<Expression> ();
    Value ps(TypeLibrary::getType(TID_INT32));
    ps.setInt32(dist.getPartitioningSchema());
    psConst->compile(false, TID_INT32, ps);
    newParameters.push_back(boost::shared_ptr<OperatorParam> (new OperatorParamPhysicalExpression(boost::make_shared<ParsingContext>(),
                                                                                                  psConst, true)));

    LOG4CXX_TRACE(logger, "Adding new param to SG node, ps="<<ps.get<int32_t>());

    size_t nParams = 1;
    if( dist.getPartitioningSchema() == psLocalInstance)
    {   //add instance number for local node distribution
        Value instanceId(TypeLibrary::getType(TID_INT64));
        instanceId.setInt64(dist.getInstanceId());

        boost::shared_ptr<Expression> instanceIdExpr = boost::make_shared<Expression> ();
        instanceIdExpr->compile(false, TID_INT64, instanceId);
        newParameters.push_back(boost::shared_ptr<OperatorParam> (new OperatorParamPhysicalExpression(boost::make_shared<ParsingContext>(),
                                                                                                      instanceIdExpr, true)));
        LOG4CXX_TRACE(logger, "Adding new param to SG node, instanceId="<<instanceId.get<int64_t>());

        nParams = 2;
    }

    for (size_t i = nParams; i< _parameters.size() && i<4; i++)
    {   //add other params from input
        newParameters.push_back(_parameters[i]);
    }

    ArrayDesc sgSchema = sgNode->getPhysicalOperator()->getSchema();

    if (newParameters.size() < 2)
    {   //if we don't have an instance parameter - add a fake instance
        boost::shared_ptr<Expression> instanceConst(new Expression());;
        Value instance(TypeLibrary::getType(TID_INT64));
        instance.setInt64(-1);
        instanceConst->compile(false, TID_INT64, instance);
        newParameters.push_back(boost::shared_ptr<OperatorParam> (new OperatorParamPhysicalExpression(boost::make_shared<ParsingContext>(),
                                                                                                      instanceConst, true)));
        LOG4CXX_TRACE(logger, "Adding new param to SG node, instanceId="<<instance.get<int64_t>());
    }

    if (newParameters.size() < 3)
    {   //if not already there - add fake schema name and fake strict flag set to "false"
        newParameters.push_back(boost::shared_ptr<OperatorParam> (new OperatorParamArrayReference(boost::make_shared<ParsingContext>(),
                                                                                      "",
                                                                                      "",
                                                                                      true)));
        LOG4CXX_TRACE(logger, "Adding new param to SG node, array name=");
        boost::shared_ptr<Expression> strictFlagExpr(new Expression());
        Value strictFlag(TypeLibrary::getType(TID_BOOL));
        strictFlag.setBool(false);
        strictFlagExpr->compile(false, TID_BOOL, strictFlag);
        newParameters.push_back(boost::shared_ptr<OperatorParam> (new OperatorParamPhysicalExpression(boost::make_shared<ParsingContext>(),
                                                                                                      strictFlagExpr, true)));
        LOG4CXX_TRACE(logger, "Adding new param to SG node, isStrict="<<false);
    }

    DimensionVector offset;
    if (dist.hasMapper())
    {
        offset = dist.getMapper()->getOffsetVector();
    }

    for ( size_t i = 0;  i < offset.numDimensions(); i++)
    {
        boost::shared_ptr<Expression> vectorValueExpr(new Expression());
        Value vectorValue(TypeLibrary::getType(TID_INT64));
        vectorValue.setInt64(offset[i]);
        vectorValueExpr->compile(false, TID_INT64, vectorValue);
        newParameters.push_back(boost::shared_ptr<OperatorParam> (new OperatorParamPhysicalExpression(boost::make_shared<ParsingContext>(),
                                                                                                      vectorValueExpr, true)));
        LOG4CXX_TRACE(logger, "Adding new param to SG node, <offset vector> ");
    }

    LOG4CXX_TRACE(logger, "Setting params to SG node, size = "<<newParameters.size());

    sgNode->getPhysicalOperator()->setParameters(newParameters);
}

static PhysNodePtr s_findThinPoint(PhysNodePtr root)
{
    double dataWidth = root->getDataWidth();
    PhysNodePtr candidate = root;

    while (root->isSgNode() == false &&
           root->needsSpecificDistribution() == false &&
           root->changesDistribution() == false &&
           root->outputFullChunks() &&
           root->getChildren().size() == 1 )
    {
        root = root->getChildren()[0];
        if (root->getDataWidth() < dataWidth)
        {
            dataWidth = root->getDataWidth();
            candidate = root;
        }
    }
    return candidate;
}

static ArrayDistribution s_propagateDistribution(PhysNodePtr node,
                                                 PhysNodePtr end)
{
    SCIDB_ASSERT(node);
    SCIDB_ASSERT(end);
    LOG4CXX_TRACE(logger, "[s_propagateDistribution] begin");
    logPlanTrace(logger, node, 0 , false);
    LOG4CXX_TRACE(logger, "[s_propagateDistribution] propogation: begin");
    ArrayDistribution dist;
    do
    {
        dist = node->inferDistribution();
        if (node == end) {
            break;
        } else {
            node = node->getParent();
        }
    } while (node->getChildren().size() <= 1);

    LOG4CXX_TRACE(logger, "[s_propagateDistribution] propogation: end");
    logPlanTrace(logger, node, 0 , false);
    LOG4CXX_TRACE(logger, "[s_propagateDistribution] end");

    return dist;
}

void HabilisOptimizer::tw_insertSgNodes(PhysNodePtr root)
{
    LOG4CXX_TRACE(logger, "[tw_insertSgNodes]");
    assert(_root.get() != NULL);

    for (size_t i = 0; i < root->getChildren().size(); i ++)
    {
        tw_insertSgNodes(root->getChildren()[i]);
    }

    if (root->isSgNode() == false)
    {
        if (root->getChildren().size() == 1)
        {
            PhysNodePtr child = root->getChildren()[0];
            ArrayDistribution cDist = child->getDistribution();
            PhysNodePtr sgCandidate = child;

            bool sgNeeded = false;
            ArrayDistribution newDist;
            bool sgMovable = true, sgOffsetable = true;

            if (child -> outputFullChunks() == false || cDist.getPartitioningSchema() == psLocalInstance)
            {
                if(root->needsSpecificDistribution())
                {
                    ArrayDistribution reqDistro = root->getDistributionRequirement().getSpecificRequirements()[0];
                    if (reqDistro.isViolated())
                        throw SYSTEM_EXCEPTION(SCIDB_SE_OPTIMIZER, SCIDB_LE_NOT_IMPLEMENTED) << "requiring violated distributions";

                    if (reqDistro == cDist && child->outputFullChunks())
                    {} //op1 returns data on local node and op2 REQUIRES all data on local node
                    else
                    {
                        sgNeeded = true;
                        newDist = reqDistro;
                        sgOffsetable = false;
                    }
                }
                else if (child -> isSgNode() && child->outputFullChunks())
                {} //user inserted sg to local node because they felt like it
                else
                {
                    sgNeeded = true;
                    newDist = ArrayDistribution(psHashPartitioned);
                }
                sgMovable = false;
            }
            else if( cDist == ArrayDistribution(psReplication) )
            {
                //replication distributions can be reduced instead of sg-ed so they are handled as a special case
                ArrayDistribution reqDistro(psHashPartitioned);
                //does root want a particular distribution? if so - use that
                //if not - force round robin! Otherwise we may get incorrect results
                if( root->needsSpecificDistribution())
                {
                    reqDistro = root->getDistributionRequirement().getSpecificRequirements()[0];
                }
                if (reqDistro.isViolated())
                    throw SYSTEM_EXCEPTION(SCIDB_SE_OPTIMIZER, SCIDB_LE_NOT_IMPLEMENTED) << "requiring violated distributions";

                if (reqDistro != cDist)
                {
                    //insert a distro reducer node. In this branch sgNeeded is always false.
                    PhysNodePtr reducerNode = n_buildReducerNode(child, reqDistro.getPartitioningSchema());
                    n_addParentNode(child, reducerNode);
                    reducerNode->inferBoundaries();
                    s_propagateDistribution(reducerNode, root);
                }
            }
            else if (root -> needsSpecificDistribution())
            {
                ArrayDistribution reqDistro = root->getDistributionRequirement().getSpecificRequirements()[0];
                if (reqDistro.isViolated())
                    throw SYSTEM_EXCEPTION(SCIDB_SE_OPTIMIZER, SCIDB_LE_NOT_IMPLEMENTED) << "requiring violated distributions";

                if (reqDistro != cDist)
                {
                    sgNeeded = true;
                    newDist = reqDistro;
                    sgOffsetable = false;
                    sgCandidate = s_findThinPoint(child);
                }
            }

            if (sgNeeded)
            {
                PhysNodePtr sgNode = n_buildSgNode(sgCandidate->getPhysicalOperator()->getSchema(), newDist.getPartitioningSchema());
                n_addParentNode(sgCandidate,sgNode);
                s_setSgDistribution(sgNode, newDist);
                sgNode->inferBoundaries();
                sgNode->setSgMovable(sgMovable);
                sgNode->setSgOffsetable(sgOffsetable);
                s_propagateDistribution(sgNode, root);
            }
        }
        else if (root->getChildren().size() == 2)
        {
            ArrayDistribution lhs = root->getChildren()[0]->getDistribution();
            if (root->getChildren()[0]->outputFullChunks() == false || lhs == ArrayDistribution(psLocalInstance))
            {
                PhysNodePtr sgNode = n_buildSgNode(root->getChildren()[0]->getPhysicalOperator()->getSchema(), psHashPartitioned);
                n_addParentNode(root->getChildren()[0],sgNode);
                sgNode->inferBoundaries();
                sgNode->setSgMovable(false);
                lhs = s_propagateDistribution(sgNode, root);
            }

            ArrayDistribution rhs = root->getChildren()[1]->getDistribution();
            if (root->getChildren()[1]->outputFullChunks() == false || rhs == ArrayDistribution(psLocalInstance))
            {
                PhysNodePtr sgNode = n_buildSgNode(root->getChildren()[1]->getPhysicalOperator()->getSchema(), psHashPartitioned);
                n_addParentNode(root->getChildren()[1],sgNode);
                sgNode->inferBoundaries();
                sgNode->setSgMovable(false);
                rhs = s_propagateDistribution(sgNode, root);
            }

            if(root->getDistributionRequirement().getReqType() == DistributionRequirement::Collocated)
            {
                if (lhs != rhs || lhs.getPartitioningSchema() != psHashPartitioned)
                {
                    bool canMoveLeftToRight = (rhs.isViolated() == false && rhs.getPartitioningSchema() == psHashPartitioned);
                    bool canMoveRightToLeft = (lhs.isViolated() == false && lhs.getPartitioningSchema() == psHashPartitioned);

                    PhysNodePtr leftCandidate = s_findThinPoint(root->getChildren()[0]);
                    PhysNodePtr rightCandidate = s_findThinPoint(root->getChildren()[1]);

                    double leftDataWidth = leftCandidate->getDataWidth();
                    double rightDataWidth = rightCandidate->getDataWidth();

                    if (leftDataWidth < rightDataWidth && canMoveLeftToRight)
                    {   //move left to right
                        if(lhs.getPartitioningSchema() == psReplication)
                        {   //left is replicated - reduce it
                            PhysNodePtr reducerNode = n_buildReducerNode(root->getChildren()[0], rhs.getPartitioningSchema());
                            n_addParentNode(root->getChildren()[0], reducerNode);
                            reducerNode->inferBoundaries();
                            s_propagateDistribution(reducerNode, root);
                        }
                        else
                        {   //left is not replicated - sg it
                            PhysNodePtr sgNode = n_buildSgNode(leftCandidate->getPhysicalOperator()->getSchema(), rhs.getPartitioningSchema());
                            n_addParentNode(leftCandidate, sgNode);
                            sgNode->inferBoundaries();
                            s_propagateDistribution(sgNode, root);
                        }
                    }
                    else if (canMoveRightToLeft)
                    {   //move right to left
                        if(rhs.getPartitioningSchema() == psReplication)
                        {   //right is replicated - reduce it
                            PhysNodePtr reducerNode = n_buildReducerNode(root->getChildren()[1], lhs.getPartitioningSchema());
                            n_addParentNode(root->getChildren()[1], reducerNode);
                            reducerNode->inferBoundaries();
                            s_propagateDistribution(reducerNode, root);
                        }
                        else
                        {   //right is not replicated - sg it
                            PhysNodePtr sgNode = n_buildSgNode(rightCandidate->getPhysicalOperator()->getSchema(), lhs.getPartitioningSchema());
                            n_addParentNode(rightCandidate, sgNode);
                            sgNode->inferBoundaries();
                            s_propagateDistribution(sgNode, root);
                        }
                    }
                    else
                    {   //move both left and right to roundRobin
                        if(lhs.getPartitioningSchema() == psReplication)
                        {   //left is replicated - reduce it
                            PhysNodePtr reducerNode = n_buildReducerNode(root->getChildren()[0], psHashPartitioned);
                            n_addParentNode(root->getChildren()[0], reducerNode);
                            reducerNode->inferBoundaries();
                            s_propagateDistribution(reducerNode, root);
                        }
                        else
                        {   //left is not replicated - sg it
                            PhysNodePtr leftSg = n_buildSgNode(leftCandidate->getPhysicalOperator()->getSchema(), psHashPartitioned);
                            n_addParentNode(leftCandidate, leftSg);
                            leftSg->inferBoundaries();
                            s_propagateDistribution(leftSg, root);
                        }

                        if(rhs.getPartitioningSchema() == psReplication)
                        {   //right is replicated - reduce it
                            PhysNodePtr reducerNode = n_buildReducerNode(root->getChildren()[1], psHashPartitioned);
                            n_addParentNode(root->getChildren()[1], reducerNode);
                            reducerNode->inferBoundaries();
                            s_propagateDistribution(reducerNode, root);
                        }
                        else
                        {   //right is not replicated - sg it
                            PhysNodePtr rightSg = n_buildSgNode(rightCandidate->getPhysicalOperator()->getSchema(), psHashPartitioned);
                            n_addParentNode(rightCandidate, rightSg);
                            rightSg->inferBoundaries();
                            s_propagateDistribution(rightSg, root);
                        }
                    }
                }
            }
            else if (root->needsSpecificDistribution())
            {
                throw SYSTEM_EXCEPTION(SCIDB_SE_OPTIMIZER, SCIDB_LE_DISTRIBUTION_SPECIFICATION_ERROR);
            }
        }
        else if (root->getChildren().size() > 2)
        {
            bool needCollocation = false;
            if(root->getDistributionRequirement().getReqType() != DistributionRequirement::Any)
            {
                if (root->getDistributionRequirement().getReqType() != DistributionRequirement::Collocated)
                {
                    throw SYSTEM_EXCEPTION(SCIDB_SE_OPTIMIZER, SCIDB_LE_DISTRIBUTION_SPECIFICATION_ERROR2);
                }
                needCollocation = true;
            }

            for(size_t i=0; i<root->getChildren().size(); i++)
            {
                PhysNodePtr child = root->getChildren()[i];
                ArrayDistribution distro = child->getDistribution();

                if (child->outputFullChunks()==false || (needCollocation && distro!= ArrayDistribution(psHashPartitioned)))
                {   //If needCollocation is true, then we have more than two children who must be collocated. This is a hard problem.
                    //Let's move everyone to roundRobin for now.
                    PhysNodePtr sgCandidate = s_findThinPoint(child);
                    PhysNodePtr sgNode = n_buildSgNode(sgCandidate->getPhysicalOperator()->getSchema(), psHashPartitioned);
                    sgNode->setSgMovable(false);
                    sgNode->setSgOffsetable(false);
                    n_addParentNode(sgCandidate, sgNode);
                    sgNode->inferBoundaries();
                    s_propagateDistribution(sgNode, root);
                }
                else if(distro.getPartitioningSchema() == psReplication)
                {   //this child is replicated - reduce it to roundRobin no matter what
                    PhysNodePtr reducerNode = n_buildReducerNode(child, psHashPartitioned);
                    n_addParentNode(child, reducerNode);
                    reducerNode->inferBoundaries();
                    s_propagateDistribution(reducerNode, root);
                }
            }
        }
    }

    root->inferDistribution();
}

static PhysNodePtr s_getChainBottom(PhysNodePtr chainRoot)
{
    PhysNodePtr chainTop = chainRoot;
    while (chainTop->getChildren().size() == 1)
    {
        chainTop = chainTop->getChildren()[0];
    }
    assert(chainTop->isSgNode() == false);
    return chainTop;
}

static PhysNodePtr s_getFirstOffsetableSg(PhysNodePtr chainRoot)
{
    if (chainRoot->isSgNode() && chainRoot->isSgOffsetable())
    {
        return chainRoot;
    }

    if (chainRoot->getChildren().size() != 1 ||
        chainRoot->changesDistribution() ||
        chainRoot->outputFullChunks() == false ||
        chainRoot->needsSpecificDistribution())
    {
        return PhysNodePtr();
    }

    return s_getFirstOffsetableSg(chainRoot->getChildren()[0]);
}



void HabilisOptimizer::cw_rectifyChainDistro(PhysNodePtr root,
                                             PhysNodePtr sgCandidate,
                                             const ArrayDistribution & requiredDistribution)
{
    ArrayDistribution currentDistribution = root->getDistribution();
    PhysNodePtr chainParent = root->getParent();

    if (requiredDistribution != currentDistribution)
    {
        PhysNodePtr sgNode = s_getFirstOffsetableSg(root);
        if (sgNode.get() == NULL)
        {
            sgNode = n_buildSgNode(sgCandidate->getPhysicalOperator()->getSchema(), requiredDistribution.getPartitioningSchema());
            n_addParentNode(sgCandidate,sgNode);
            sgNode->inferBoundaries();
            if (sgCandidate == root)
            {
                root = sgNode;
            }
        }
        if (requiredDistribution.isViolated())
        {
            boost::shared_ptr<DistributionMapper> requiredMapper = requiredDistribution.getMapper();
            assert(requiredMapper.get()!=NULL);
        }
        s_setSgDistribution(sgNode, requiredDistribution);

        ArrayDistribution newRdStats = s_propagateDistribution(sgNode, chainParent);
    }

    assert(root->getDistribution() == requiredDistribution);
}

void HabilisOptimizer::tw_collapseSgNodes(PhysNodePtr root)
{
    LOG4CXX_TRACE(logger, "[tw_collapseSgNodes] begin");

    bool topChain = (root == _root);

    PhysNodePtr chainBottom = s_getChainBottom(root);
    PhysNodePtr curNode = chainBottom;
    PhysNodePtr sgCandidate = chainBottom;

    ArrayDistribution runningDistribution = curNode->getDistribution();
    ArrayDistribution chainOutputDistribution = root->getDistribution();

    LOG4CXX_TRACE(logger, "[tw_collapseSgNodes] cycle: begin");
    do
    {
        LOG4CXX_TRACE(logger, "[tw_collapseSgNodes] cycle iteration: begin");
        logPlanTrace(root);
        LOG4CXX_TRACE(logger, "[tw_collapseSgNodes] chainBottom:");
        logPlanTrace(chainBottom, false);
        LOG4CXX_TRACE(logger, "[tw_collapseSgNodes] curNode:");
        logPlanTrace(curNode, false);
        LOG4CXX_TRACE(logger, "[tw_collapseSgNodes] sgCandidate:");
        logPlanTrace(sgCandidate, false);

        runningDistribution = curNode->inferDistribution();

        if (curNode->isSgNode() == false &&
             (curNode->changesDistribution() ||
              curNode->outputFullChunks() == false ||
              curNode->getDataWidth() < sgCandidate->getDataWidth()))
        {
            LOG4CXX_TRACE(logger, "[tw_collapseSgNodes] sgCandidate switched to curNode");
            sgCandidate = curNode;
        }
        if (curNode->hasParent() &&
            curNode->getParent()->getChildren().size() == 1 &&
            curNode->getParent()->needsSpecificDistribution())
        {
            LOG4CXX_TRACE(logger, "[tw_collapseSgNodes] curNode has parent and single child; need specific distribution");
            ArrayDesc curSchema =  curNode->getPhysicalOperator()->getSchema();
            ArrayDistribution neededDistribution = curNode->getParent()->getDistributionRequirement().getSpecificRequirements()[0];
            if (runningDistribution != neededDistribution)
            {
                LOG4CXX_TRACE(logger, "[tw_collapseSgNodes] curNode and required for parent distributions are different");
                if (curNode->isSgNode() && runningDistribution.getPartitioningSchema() == neededDistribution.getPartitioningSchema())
                {
                    LOG4CXX_TRACE(logger, "[tw_collapseSgNodes] curNode is SG, update distribution: begin");
                    logPlanTrace(curNode, false);
                    curNode->getPhysicalOperator()->setSchema(curSchema);
                    s_setSgDistribution(curNode, neededDistribution);
                    curNode->setSgMovable(false);
                    curNode->setSgOffsetable(false);
                    runningDistribution = curNode->inferDistribution();
                    LOG4CXX_TRACE(logger, "[tw_collapseSgNodes] curNode is SG, update distribution: end");
                    logPlanTrace(curNode, false);
                }
                else
                {
                    LOG4CXX_TRACE(logger, "[tw_collapseSgNodes] curNod is not SG, inserting one: begin");
                    PhysNodePtr newSg = n_buildSgNode(curSchema, neededDistribution.getPartitioningSchema());
                    n_addParentNode(sgCandidate,newSg);
                    s_setSgDistribution(newSg, neededDistribution);
                    newSg->inferBoundaries();
                    runningDistribution = s_propagateDistribution(newSg, curNode->getParent());
                    newSg->setSgMovable(false);
                    newSg->setSgOffsetable(false);

                    if (curNode == sgCandidate)
                    {
                        LOG4CXX_TRACE(logger, "[tw_collapseSgNodes] curNode switched to sgCandidate");
                        curNode = newSg;
                    }
                    LOG4CXX_TRACE(logger, "[tw_collapseSgNodes] curNod is not SG, inserting one: end");
                }
            }
        }
        else if (curNode->isSgNode() && curNode->isSgMovable())
        {
            LOG4CXX_TRACE(logger, "[tw_collapseSgNodes] curNode is movable SG node, remove it: begin");
            PhysNodePtr newCur = curNode->getChildren()[0];
            n_cutOutNode(curNode);
            if (curNode == sgCandidate)
            {
                LOG4CXX_TRACE(logger, "[tw_collapseSgNodes] sgCandidate switched to curNode");
                sgCandidate = newCur;
            }
            curNode = newCur;
            runningDistribution = curNode->getDistribution();
            LOG4CXX_TRACE(logger, "[tw_collapseSgNodes] curNode is movable SG node, remove it: end");
        }

        root = curNode;
        curNode = curNode->getParent();

        LOG4CXX_TRACE(logger, "[tw_collapseSgNodes] cycle iteration: end");
    } while (curNode.get() != NULL && curNode->getChildren().size()<=1);
    LOG4CXX_TRACE(logger, "[tw_collapseSgNodes] cycle: end");

    assert(root);

    LOG4CXX_TRACE(logger, "[tw_collapseSgNodes] chainBottom:");
    logPlanTrace(chainBottom, false);
    LOG4CXX_TRACE(logger, "[tw_collapseSgNodes] curNode:");
    logPlanTrace(curNode, false);
    LOG4CXX_TRACE(logger, "[tw_collapseSgNodes] sgCandidate:");
    logPlanTrace(sgCandidate, false);

    if (!topChain)
    {
        LOG4CXX_TRACE(logger, "[tw_collapseSgNodes] is not top chain: begin");
        PhysNodePtr parent = root->getParent();
        if (parent->getDistributionRequirement().getReqType() != DistributionRequirement::Any)
        {
            LOG4CXX_TRACE(logger, "[tw_collapseSgNodes] required distribution is not Any");
            //we have a parent instance that has multiple children and needs a specific distribution
            //so we must correct the distribution back to the way it was before we started messing with the chain
            cw_rectifyChainDistro(root, sgCandidate, chainOutputDistribution);
        }
        LOG4CXX_TRACE(logger, "[tw_collapseSgNodes] is not top chain: end");
    }

    LOG4CXX_TRACE(logger, "[tw_collapseSgNodes] process children chains");
    for (size_t i = 0; i< chainBottom->getChildren().size(); i++)
    {
        tw_collapseSgNodes(chainBottom->getChildren()[i]);
    }

    LOG4CXX_TRACE(logger, "[tw_collapseSgNodes] end");
}

static PhysNodePtr s_getTopSgFromChain(PhysNodePtr chainRoot)
{
    PhysNodePtr chainTop = chainRoot;

    while (chainTop->getChildren().size() == 1)
    {
        if(chainTop->isSgNode())
        {
            return chainTop;
        }
        else if (chainTop->changesDistribution() ||
                 chainTop->outputFullChunks() == false)
        {
            //TODO: this case can be opened up.. but it requires subtraction of offset vectors
            return PhysNodePtr();
        }

        chainTop = chainTop->getChildren()[0];
    }
    return PhysNodePtr();
}

void HabilisOptimizer::cw_pushupSg (PhysNodePtr root, PhysNodePtr sgToRemove, PhysNodePtr sgToOffset)
{
    PhysNodePtr sgrChild = sgToRemove->getChildren()[0];
    n_cutOutNode(sgToRemove);

    ArrayDistribution newSgrDistro = sgrChild->getDistribution();

    for (PhysNodePtr n = sgrChild->getParent(); n != root; n = n->getParent())
    {
        newSgrDistro = n->inferDistribution();
    }

    assert(newSgrDistro.hasMapper());

    ArrayDistribution newDist (newSgrDistro.getPartitioningSchema(), newSgrDistro.getMapper());

    s_setSgDistribution(sgToOffset, newDist);
    ArrayDistribution newSgoDistro = sgToOffset->inferDistribution();
    for (PhysNodePtr n = sgToOffset->getParent(); n != root; n = n->getParent())
    {
        newSgoDistro = n->inferDistribution();
    }

    assert(newSgrDistro == newSgoDistro);
    root->inferDistribution();

    PhysNodePtr newSg = n_buildSgNode(root->getPhysicalOperator()->getSchema(), psHashPartitioned);
    newSg->setSgMovable(true);
    newSg->setSgOffsetable(true);
    n_addParentNode(root,newSg);
    newSg->inferDistribution();
    newSg->inferBoundaries();
}

void HabilisOptimizer::cw_swapSg (PhysNodePtr root, PhysNodePtr sgToRemove, PhysNodePtr oppositeThinPoint)
{
    PhysNodePtr sgrChild = sgToRemove->getChildren()[0];
    n_cutOutNode(sgToRemove);

    ArrayDistribution newSgrDistro = sgrChild->getDistribution();

    for (PhysNodePtr n = sgrChild->getParent(); n != root; n = n->getParent())
    {
        newSgrDistro = n->inferDistribution();
    }

    assert(newSgrDistro.hasMapper());

    ArrayDistribution newDist (newSgrDistro.getPartitioningSchema(), newSgrDistro.getMapper());

    PhysNodePtr newOppositeSg = n_buildSgNode(oppositeThinPoint->getPhysicalOperator()->getSchema(), psHashPartitioned);
    n_addParentNode(oppositeThinPoint, newOppositeSg);
    s_setSgDistribution(newOppositeSg, newDist);
    newOppositeSg->inferBoundaries();
    ArrayDistribution newOppositeDistro = newOppositeSg->inferDistribution();
    for (PhysNodePtr n = newOppositeSg->getParent(); n != root; n = n->getParent())
    {
        newOppositeDistro = n->inferDistribution();
    }

    assert(newSgrDistro == newOppositeDistro);
    root->inferDistribution();

    PhysNodePtr newRootSg = n_buildSgNode(root->getPhysicalOperator()->getSchema(), psHashPartitioned);
    newRootSg->setSgMovable(true);
    newRootSg->setSgOffsetable(true);
    n_addParentNode(root,newRootSg);
    newRootSg->inferDistribution();

    logPlanDebug();

    newRootSg->inferBoundaries();

    logPlanDebug();
}

bool HabilisOptimizer::tw_pushupJoinSgs(PhysNodePtr root)
{
    //"pushup" is a transformation from root(...join(sg(A),sg(B))) into root(...sg(join(sg(A),B)))
    //Note this is advantageous if placing sg on top results in less data movement

    //True if top chain SG will be "collapsed" by subsequent collapse()
    bool parentChainWillCollapse = root==_root ||
                                   root->getDistribution().hasMapper();

    //Thinnest available data point in top chain
    double parentChainThinPoint = root->getDataWidth();

    while (root->getChildren().size() == 1)
    {
        double currentThickness = root->getChildren()[0]->getDataWidth();
        if (currentThickness < parentChainThinPoint)
        {
            parentChainThinPoint = currentThickness;
        }

        //If the closest node above the join is an SG, then we can place another
        //SG onto top chain and the two SGs will collapse.

        //Otherwise, if the closest node above join needs correct distribution,
        //new SG will have to stay on top chain and get run

        if (root->isSgNode())
        {
            parentChainWillCollapse = true;
        }
        else if (root->needsSpecificDistribution())
        {
            parentChainWillCollapse = false;
            parentChainThinPoint = currentThickness;
        }

        root = root->getChildren()[0];
    }

    bool transformPerformed = false;

    if (root->getChildren().size() == 2)
    {
        if (root->getDistributionRequirement().getReqType() == DistributionRequirement::Collocated &&
            root->getChildren()[0]->getPhysicalOperator()->getSchema().getDimensions().size() ==
            root->getChildren()[1]->getPhysicalOperator()->getSchema().getDimensions().size())
        {
            PhysNodePtr leftChainRoot = root->getChildren()[0];
            PhysNodePtr rightChainRoot = root->getChildren()[1];

            PhysNodePtr leftSg = s_getTopSgFromChain(leftChainRoot);
            PhysNodePtr rightSg = s_getTopSgFromChain(rightChainRoot);

            if (leftSg.get()!=NULL && rightSg.get()!=NULL)
            {
                double leftAttributes = leftSg->getDataWidth();
                double rightAttributes = rightSg->getDataWidth();

                //the cost of not doing anything - run left SG and right SG
                double currentCost = leftAttributes + rightAttributes;

                //the cost of removing either SG
                double moveLeftCost = rightAttributes;
                double moveRightCost = leftAttributes;

                if (parentChainWillCollapse == false)
                {
                    //we will put sg on top and it will not collapse - add to the cost
                    moveLeftCost += parentChainThinPoint;
                    moveRightCost += parentChainThinPoint;
                }

                bool canMoveLeft = leftSg->isSgMovable() &&
                                   leftSg->getChildren()[0]->getDistribution().hasMapper() &&
                                   rightSg->isSgOffsetable();

                bool canMoveRight = rightSg->isSgMovable() &&
                                    rightSg->getChildren()[0]->getDistribution().hasMapper() &&
                                    leftSg->isSgOffsetable();

                if (canMoveLeft && moveLeftCost <= moveRightCost && moveLeftCost <= currentCost)
                {
                    cw_pushupSg(root,leftSg,rightSg);
                    transformPerformed = true;
                }
                else if (canMoveRight && moveRightCost <= currentCost)
                {
                    cw_pushupSg(root,rightSg,leftSg);
                    transformPerformed = true;
                }
            }
            else if ( leftSg.get() != NULL || rightSg.get() != NULL )
            {
                PhysNodePtr sg = leftSg.get() != NULL ? leftSg : rightSg;
                PhysNodePtr oppositeChain = leftSg.get() != NULL ? rightChainRoot : leftChainRoot;
                oppositeChain = s_findThinPoint(oppositeChain);

                bool canMoveSg = sg->isSgMovable() &&
                                 sg->getChildren()[0]->getDistribution().hasMapper();

                double currentCost = sg->getDataWidth();
                double moveCost = oppositeChain->getDataWidth();

                if (parentChainWillCollapse == false)
                {
                    //we will put sg on top and it will not collapse - add to the cost
                    moveCost += parentChainThinPoint;
                }

                if ( canMoveSg && moveCost < currentCost )
                {
                    cw_swapSg(root, sg, oppositeChain);
                    transformPerformed = true;
                }
            }
        }
    }

    bool result = transformPerformed;
    for (size_t i = 0; i< root->getChildren().size(); i++)
    {
        bool transformPerformedAtChild = tw_pushupJoinSgs(root->getChildren()[i]);
        result = transformPerformedAtChild || result;
    }
    return result;
}

void HabilisOptimizer::tw_rewriteStoringSG(PhysNodePtr root)
{
    if ( root->getPhysicalOperator()->getPhysicalName() == "physicalStore" )
    {
        PhysNodePtr child = root->getChildren()[0];
        if (child->isSgNode() && !child->isStoringSg() && child->getChildren()[0]->subTreeOutputFullChunks())
        {
            PhysOpPtr storeOp = root->getPhysicalOperator();
            ArrayDesc storeSchema = storeOp->getSchema();

            ArrayDistribution distro = child->getDistribution();
            if (distro != ArrayDistribution(psHashPartitioned))
                throw SYSTEM_EXCEPTION(SCIDB_SE_OPTIMIZER, SCIDB_LE_NOT_IMPLEMENTED) << " storing arrays in non-roro distribution";

            PhysNodePtr newSg = n_buildSgNode(storeSchema, psHashPartitioned, true);
            PhysNodePtr grandChild = child->getChildren()[0];
            n_cutOutNode(root);
            n_cutOutNode(child);
            n_addParentNode(grandChild, newSg);

            newSg->inferBoundaries();
            newSg->inferDistribution();

            root = newSg;
        }
    }

    for (size_t i =0; i<root->getChildren().size(); i++)
    {
        tw_rewriteStoringSG(root->getChildren()[i]);
    }
}

/**
 *  Insert any needed repart() operators into the physical plan.
 */
bool HabilisOptimizer::tw_insertRepartNodes(PhysNodePtr nodep)
{
    bool subtreeModified = false;

    // Leaf node?  Done.
    const size_t N_CHILDREN = nodep->getChildren().size();
    if (N_CHILDREN == 0)
    {
        return false;
    }
    
    // Handle children first.  Change the tree from bottom to top, so that any inferences about
    // boundaries and distributions can percolate up.
    //
    for (size_t i = 0; i < N_CHILDREN; ++i)
    {
        subtreeModified |= tw_insertRepartNodes(nodep->getChildren()[i]);
    }

    // Now for the current node.  Ask it: want to repartition any input schema?
    vector<ArrayDesc> schemas(nodep->getChildSchemas());
    assert(schemas.size() == N_CHILDREN);
    vector<ArrayDesc const*> repartPtrs(N_CHILDREN);
    nodep->getPhysicalOperator()->requiresRepart(schemas, repartPtrs);
    if (repartPtrs.empty())
    {
        // Nothing to do here, but keep the inference chain going.
        if (subtreeModified) {
            nodep->inferBoundaries();
            nodep->inferDistribution();
        }
        return subtreeModified;
    }

    // Scan the children... if any are themselves repart operators, they were manually inserted.
    // (We know this because we are walking the query tree from leaves to root.)  Therefore
    // we'll not do any automatic repartitioning; manual repartitioning takes precedence.
    //
    for (size_t i = 0; i < N_CHILDREN; ++i)
    {
        if (nodep->getChildren()[i]->isRepartNode())
        {
            LOG4CXX_INFO(logger, "Inputs to query " << _query->getQueryID()
                         << " " << nodep->getPhysicalOperator()->getLogicalName()
                         << " operator are manually repartitioned");
            if (subtreeModified) {
                nodep->inferBoundaries();
                nodep->inferDistribution();
            }
            return subtreeModified;
        }
    }

    // The repartSchema vector describes how the nodep operator wants
    // each of its children repartitioned.
    //
    OperatorLibrary* oplib = OperatorLibrary::getInstance();
    PhysicalOperator::Parameters repartParms(1);
    size_t numReparts = 0;
    for (size_t i = 0; i < N_CHILDREN; ++i)
    {
        if (repartPtrs[i] == 0)
        {
            // This child's schema is fine, no change.
            continue;
        }
        ArrayDesc const& repartSchema = *repartPtrs[i];
        numReparts += 1;

        // Wrap desired schema in Parameters object.
        repartParms[0] = shared_ptr<OperatorParam>(
            new OperatorParamSchema(make_shared<ParsingContext>(), repartSchema));

        // Create repartOp and bind to its parameter(s) and query.
        PhysOpPtr repartOp = oplib->createPhysicalOperator("repart", "physicalRepart", repartParms, repartSchema);
        repartOp->setQuery(_query);

        // Create phys. plan node for repartOP and splice it in above child[i].
        PhysNodePtr repartNode(new PhysicalQueryPlanNode(repartOp, false/*agg*/, false/*ddl*/, false/*tile*/));
        n_addParentNode(nodep->getChildren()[i], repartNode);

        // Re-run inferences for new repart child.
        repartNode->inferBoundaries();
        repartNode->inferDistribution();
    }
    
    // If requiresRepart() gave us a non-empty vector, it better have at least one repartSchema for us.
    assert(numReparts > 0);

    // Re-run inferences for this node and we are done.
    nodep->inferBoundaries();
    nodep->inferDistribution();
    return true;
}

void HabilisOptimizer::tw_insertChunkMaterializers(PhysNodePtr root)
{
    if ( root->hasParent() && root->getChildren().size() != 0)
    {
        PhysNodePtr parent = root->getParent();
        if (root->isSgNode() == false && root->getPhysicalOperator()->getTileMode() != parent->getPhysicalOperator()->getTileMode())
        {
            ArrayDesc const& schema = root->getPhysicalOperator()->getSchema();
            Value formatParameterValue;
            formatParameterValue.setInt64(MaterializedArray::RLEFormat);
            boost::shared_ptr<Expression> formatParameterExpr = boost::make_shared<Expression> ();
            formatParameterExpr->compile(false, TID_INT64, formatParameterValue);
            PhysicalOperator::Parameters params;
            params.push_back(boost::shared_ptr<OperatorParam> (new OperatorParamPhysicalExpression(boost::make_shared<ParsingContext>(), formatParameterExpr, true)));

            PhysOpPtr materializeOp = OperatorLibrary::getInstance()->createPhysicalOperator("materialize", "impl_materialize", params, schema);
            materializeOp->setQuery(_query);

            PhysNodePtr materializeNode(new PhysicalQueryPlanNode(materializeOp, false, false, false));
            n_addParentNode(root, materializeNode);
            materializeNode->inferBoundaries();
            materializeNode->inferDistribution();
        }
    }

    for (size_t i =0; i < root->getChildren().size(); i++)
    {
        tw_insertChunkMaterializers(root->getChildren()[i]);
    }
}

boost::shared_ptr<Optimizer> Optimizer::create()
{
    LOG4CXX_DEBUG(logger, "Creating Habilis optimizer instance")
    return boost::shared_ptr<Optimizer> (new HabilisOptimizer());
}

} // namespace
