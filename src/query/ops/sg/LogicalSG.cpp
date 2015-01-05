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

/*
 * @file LogicalSG.cpp
 *
 * @author roman.simakov@gmail.com
 * @brief This file implement logical operator SCATTER/GATHER
 */

#include "query/Operator.h"
#include <smgr/io/Storage.h>


namespace scidb
{

/**
 * @brief The operator: sg().
 *
 * @par Synopsis:
 *   sg( srcArray, partitionSchema, instanceId=-1, outputArray="", isStore=true, offsetVector=null)
 *
 * @par Summary:
 *   SCATTER/GATHER distributes array chunks over the instances of a cluster.
 *   The result array is returned.
 *   It is the only operator that uses the network manager.
 *   Typically this operator is inserted by the optimizer into the physical plan.
 *
 * @par Input:
 *   - srcArray: the source array, with srcAttrs and srcDims.
 *   - partitionSchema:<br>
 *     0 = psReplication, <br>
 *     1 = psHashPartitioned,<br>
 *     2 = psLocalInstance,<br>
 *     3 = psByRow,<br>
 *     4 = psByCol,<br>
 *     5 = psUndefined.<br>
 *   - instanceId:<br>
 *     -2 = to coordinator (same with 0),<br>
 *     -1 = all instances participate,<br>
 *     0..#instances-1 = to a particular instance.<br>
 *     [TO-DO: The usage of instanceId, in calculating which instance a chunk should go to, requires further documentation.]
 *   - outputArray: if not empty, the result will be stored into this array
 *   - isStore: whether to store into the specified outputArray.<br>
 *     [TO-DO: Donghui believes this parameter is not needed and should be removed.]
 *   - offsetVector: a vector of #dimensions values.<br>
 *     To calculate which instance a chunk belongs, the chunkPos is augmented with the offset vector before calculation.
 *
 * @par Output array:
 *        <
 *   <br>   srcAttrs
 *   <br> >
 *   <br> [
 *   <br>   srcDims
 *   <br> ]
 *
 * @par Examples:
 *   n/a
 *
 * @par Errors:
 *   n/a
 *
 * @par Notes:
 *
 */
class LogicalSG: public LogicalOperator
{
public:
    LogicalSG(const std::string& logicalName, const std::string& alias):
        LogicalOperator(logicalName, alias)
    {
        ADD_PARAM_INPUT();
        ADD_PARAM_CONSTANT("uint32");
        ADD_PARAM_VARIES();
    }

    std::vector<boost::shared_ptr<OperatorParamPlaceholder> > nextVaryParamPlaceholder(const std::vector< ArrayDesc> &schemas)
    {
        std::vector<boost::shared_ptr<OperatorParamPlaceholder> > res;

        // sanity check: #parameters is at least 1 (i.e. partitionSchema), but no more than #dims+4.
        if (_parameters.size()==0 || _parameters.size() > schemas[0].getDimensions().size() + 4) {
            assert(false);
        }

        // the param is before the offset vector
        else if (_parameters.size() <= 3) {
            res.push_back(END_OF_VARIES_PARAMS());

            switch (_parameters.size()) {
            case 1:
                res.push_back(PARAM_CONSTANT("int64"));
                break;
            case 2:
                res.push_back(PARAM_OUT_ARRAY_NAME());
                break;
            case 3:
                res.push_back(PARAM_CONSTANT("bool"));
                break;
            default:
                assert(false);
                break;
            }
        }

        // the param is in the offset vector
        else if (_parameters.size() < schemas[0].getDimensions().size() + 4) {
            // along with the first value in the offset, we say the vector is optional
            if (_parameters.size()==4) {
                res.push_back(END_OF_VARIES_PARAMS());
            }
            res.push_back(PARAM_CONSTANT("int64"));
        }

        // after the offset vector
        else {
            assert(_parameters.size() == schemas[0].getDimensions().size() + 4);
            res.push_back(END_OF_VARIES_PARAMS());
        }

        return res;
    }

    /**
     * The schema of output array is the same as input
     */
    ArrayDesc inferSchema(std::vector< ArrayDesc> inputSchemas, boost::shared_ptr< Query> query)
    {
        assert(inputSchemas.size() == 1);
        ArrayDesc const& desc = inputSchemas[0];
        std::string resultArrayName = desc.getName();
        PartitioningSchema ps;

        /* validate the partitioning schema
         */
        ps = (PartitioningSchema)
            evaluate(((boost::shared_ptr<OperatorParamLogicalExpression>&)_parameters[0])->getExpression(), query, TID_INT32).getInt32();
        if (! isValidPartitioningSchema(ps, false)) // false = not allow optional data associated with the partitioning schema
        {
            throw USER_EXCEPTION(SCIDB_SE_REDISTRIBUTE, SCIDB_LE_REDISTRIBUTE_ERROR);
        } 

        /* get the name of the supplied result array
         */
        if (_parameters.size() >= 3)
        {
            std::string suppliedResultName = ((boost::shared_ptr<OperatorParamReference>&)_parameters[2])->getObjectName();
            if (!suppliedResultName.empty())
            {
                resultArrayName = suppliedResultName;
            }
        }
        return ArrayDesc(resultArrayName, desc.getAttributes(), desc.getDimensions());
    }

    void inferArrayAccess(boost::shared_ptr<Query>& query)
    {
        LogicalOperator::inferArrayAccess(query);

        if (_parameters.size() < 3)
        {
            return;
        }
        std::string resultArrayName = ((boost::shared_ptr<OperatorParamReference>&)_parameters[2])->getObjectName();
        if (resultArrayName.empty())
        {
            return;
        }

        bool storeResult = true;
        if (_parameters.size() >= 4)
        {
            Expression expr;
            expr.compile(((boost::shared_ptr<OperatorParamLogicalExpression>&)_parameters[3])->getExpression(),
                         query, false, TID_BOOL);
            Value const& value = expr.evaluate();
            assert (expr.getType() == TID_BOOL);
            storeResult = value.getBool();
        }
        if (storeResult)
        {
            assert(resultArrayName.find('@') == std::string::npos);
            boost::shared_ptr<SystemCatalog::LockDesc>  lock(new SystemCatalog::LockDesc(resultArrayName,
                                                                                         query->getQueryID(),
                                                                                         Cluster::getInstance()->getLocalInstanceId(),
                                                                                         SystemCatalog::LockDesc::COORD,
                                                                                         SystemCatalog::LockDesc::WR));
            boost::shared_ptr<SystemCatalog::LockDesc> resLock = query->requestLock(lock);
            assert(resLock);
            assert(resLock->getLockMode() >= SystemCatalog::LockDesc::WR);
        }
    }
};


DECLARE_LOGICAL_OPERATOR_FACTORY(LogicalSG, "sg")


} //namespace
