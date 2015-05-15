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
 * @brief Basic class for all optimizers
 *
 * @author knizhnik@garret.ru
 */
#include "query/optimizer/Optimizer.h"
#include "network/NetworkManager.h"

using namespace boost;

namespace scidb
{
    boost::shared_ptr< LogicalQueryPlanNode> Optimizer::logicalRewriteIfNeeded(const boost::shared_ptr<Query>& query,
                                                                               boost::shared_ptr< LogicalQueryPlanNode> node)
    {
        //rewrite load(array,'filename') into store(input(array,'filename'),array)

        //Note: this rewrite mechanism should be
        //  1. generic
        //  2. user-extensible

        //Note: optimizer also performs rewrites like "sum" -> "sum2(sum)" but we can't do these here because:
        //  1. they are physical; not logical
        //  2. they are recursive. We don't want logical rewrites to be recursive.

        OperatorLibrary *olib =  OperatorLibrary::getInstance();
        if (node->getLogicalOperator()->getLogicalName()=="load")
        {
            boost::shared_ptr< LogicalOperator> loadOperator = node->getLogicalOperator();

            LogicalOperator::Parameters loadParameters = loadOperator->getParameters();
            ArrayDesc outputSchema = loadOperator->getSchema();

            boost::shared_ptr< LogicalOperator> inputOperator = olib->createLogicalOperator("input");
            inputOperator->setParameters(loadParameters);
            inputOperator->setSchema(outputSchema);

            //Load have schema as first argument as input, but it checks if this schema NOT anonymous,
            //so we just convert it into array reference
            const string &schemaName = boost::dynamic_pointer_cast<OperatorParamSchema>(loadParameters[0])->getSchema().getName();
            boost::shared_ptr<OperatorParam> paramArrayRef = boost::shared_ptr<OperatorParam>(
                                new OperatorParamArrayReference(
                                    loadParameters[0]->getParsingContext(),
                                    "",
                                    schemaName,
                                    true));

            if ( query->getInstancesCount() == 1) {
                boost::shared_ptr< LogicalOperator> storeOperator = olib->createLogicalOperator("store");
                storeOperator->addParameter(paramArrayRef);

                std::vector< ArrayDesc> storeInputSchemas;
                storeInputSchemas.push_back(inputOperator->getSchema());

                storeOperator->setSchema(storeOperator->inferSchema(storeInputSchemas, query));

                boost::shared_ptr< LogicalQueryPlanNode> inputNode(
                    new  LogicalQueryPlanNode (node->getParsingContext(),
                                                     inputOperator));

                boost::shared_ptr< LogicalQueryPlanNode> storeNode(
                    new  LogicalQueryPlanNode (node->getParsingContext(),
                                                     storeOperator));

                //load instance does not have any children. so the input instance will also have none.
                assert(node->getChildren().size()==0);

                storeNode->addChild(inputNode);
                return storeNode;
            } else {
                LogicalOperator::Parameters sgParams(3);
                Value ival(TypeLibrary::getType(TID_INT32));
                ival.setInt32(psHashPartitioned);
                sgParams[0] = boost::shared_ptr<OperatorParam>(
                    new OperatorParamLogicalExpression(node->getParsingContext(),
                                                       boost::shared_ptr<LogicalExpression>(new Constant(node->getParsingContext(),
                                                                                                         ival, TID_INT32)),
                                                       TypeLibrary::getType(TID_INT32), true));
                ival.setInt64(-1);
                sgParams[1] = boost::shared_ptr<OperatorParam>(
                    new OperatorParamLogicalExpression(node->getParsingContext(),
                                                       boost::shared_ptr<LogicalExpression>(new Constant(node->getParsingContext(),
                                                                                                         ival, TID_INT32)),
                                                       TypeLibrary::getType(TID_INT32), true));
                sgParams[2] = paramArrayRef;

                // propagate the strict flag to SG
                ssize_t strictIndex = -1;
                if (inputOperator->getParameters().size() >= 6 &&
                    inputOperator->getParameters()[5]->getParamType() == PARAM_LOGICAL_EXPRESSION) {
                    strictIndex = 5;

                } else if (inputOperator->getParameters().size() >= 7 ) {
                    assert(inputOperator->getParameters()[6]->getParamType() == PARAM_LOGICAL_EXPRESSION);
                    strictIndex = 6;
                }
                if (strictIndex>0) {
                    sgParams.push_back(inputOperator->getParameters()[strictIndex]);
                    if (isDebug()) {
                        OperatorParamLogicalExpression* lExp =
                           static_cast<OperatorParamLogicalExpression*>(inputOperator->getParameters()[strictIndex].get());
                        SCIDB_ASSERT(lExp->isConstant());
                        assert(lExp->getExpectedType()==TypeLibrary::getType(TID_BOOL));
                    }
                }

                boost::shared_ptr< LogicalOperator> sgOperator = olib->createLogicalOperator("sg");
                sgOperator->setParameters(sgParams);

                std::vector< ArrayDesc> sgInputSchemas;
                sgInputSchemas.push_back(inputOperator->getSchema());

                sgOperator->setSchema(sgOperator->inferSchema(sgInputSchemas,query));

                boost::shared_ptr< LogicalQueryPlanNode> inputNode(
                    new  LogicalQueryPlanNode (node->getParsingContext(),
                                                     inputOperator));

                boost::shared_ptr< LogicalQueryPlanNode> sgNode(
                    new  LogicalQueryPlanNode (node->getParsingContext(),
                                                     sgOperator));

                //load node does not have any children. so the input node will also have none.
                assert(node->getChildren().size()==0);

                sgNode->addChild(inputNode);

                return sgNode;
            }
        }
        else if (AggregateLibrary::getInstance()->hasAggregate(node->getLogicalOperator()->getLogicalName()))
        {
           boost::shared_ptr< LogicalOperator> oldStyleOperator = node->getLogicalOperator();
           boost::shared_ptr< LogicalOperator> aggOperator = olib->createLogicalOperator("aggregate");
           aggOperator->setSchema(oldStyleOperator->getSchema());
           LogicalOperator::Parameters oldStyleParams = oldStyleOperator->getParameters();

           if (node->getLogicalOperator()->getLogicalName()=="count")
           {
               boost::shared_ptr<OperatorParam> asterisk (new OperatorParamAsterisk(node->getParsingContext()));

               boost::shared_ptr<OperatorParam> aggCall ( new OperatorParamAggregateCall (node->getParsingContext(),
                                                                                   node->getLogicalOperator()->getLogicalName(),
                                                                                   asterisk,
                                                                                   ""));
               aggOperator->addParameter(aggCall);

           }
           else if (oldStyleParams.size() == 0)
           {
               ArrayDesc const& inputSchema = node->getChildren()[0]->getLogicalOperator()->getSchema();
               boost::shared_ptr<OperatorParamReference> attRef ( new OperatorParamAttributeReference(node->getParsingContext(),
                                                                                               inputSchema.getName(),
                                                                                               inputSchema.getAttributes()[0].getName(),
                                                                                               true));
               attRef->setInputNo(0);
               attRef->setObjectNo(0);

               boost::shared_ptr<OperatorParam> aggCall ( new OperatorParamAggregateCall (node->getParsingContext(),
                                                                                   node->getLogicalOperator()->getLogicalName(),
                                                                                   attRef,
                                                                                   ""));
               aggOperator->addParameter(aggCall);
           }
           if (oldStyleParams.size() != 0) {
               ((LogicalOperator::Properties&)aggOperator->getProperties()).tile = false;
           }

           for (size_t i =0; i<oldStyleParams.size(); i++)
           {
               if (oldStyleParams[i]->getParamType() == PARAM_ATTRIBUTE_REF)
               {
                   boost::shared_ptr<OperatorParam> aggCall ( new OperatorParamAggregateCall (oldStyleParams[i]->getParsingContext(),
                                                                                       node->getLogicalOperator()->getLogicalName(),
                                                                                       oldStyleParams[i],
                                                                                       ""));
                   aggOperator->addParameter(aggCall);
               }
               else if (oldStyleParams[i]->getParamType() == PARAM_DIMENSION_REF)
               {
                   aggOperator->addParameter(oldStyleParams[i]);
               }
           }

           boost::shared_ptr< LogicalQueryPlanNode> aggInstance( new  LogicalQueryPlanNode (node->getParsingContext(), aggOperator));
           assert(node->getChildren().size() == 1);
           aggInstance->addChild(node->getChildren()[0]);
           return aggInstance;
        }
        else
        {
           return node;
        }
    }
}
