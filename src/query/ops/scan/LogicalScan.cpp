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
 * LogicalScan.cpp
 *
 *  Created on: Mar 9, 2010
 *      Author: Emad
 */
#include "query/Operator.h"
#include "system/SystemCatalog.h"
#include "system/Exceptions.h"


namespace scidb
{

/**
 * @brief The operator: scan().
 *
 * @par Synopsis:
 *   scan( srcArray [, ifTrim] )
 *
 * @par Summary:
 *   Produces a result array that is equivalent to a stored array.
 *
 * @par Input:
 *   - srcArray: the array to scan, with srcAttrs and srcDims.
 *   - ifTrim: whether to turn an unbounded array to a bounded array. Default value is false.
 *
 * @par Output array:
 *        <
 *   <br>   srcAttrs
 *   <br> >
 *   <br> [
 *   <br>   srcDims (ifTrim=false), or trimmed srcDims (ifTrim=true).
 *   <br> ]
 *
 * @par Examples:
 *   n/a
 *
 * @par Errors:
 *   n/a
 *
 * @par Notes:
 *   n/a
 *
 */
class LogicalScan: public  LogicalOperator
{
public:
    LogicalScan(const std::string& logicalName, const std::string& alias):
                    LogicalOperator(logicalName, alias)
    {
        _properties.tile = true;

        // - With ADD_PARAM_INPUT()
        //   which is a typical way of providing an input array name,
        //   the array name will NOT appear in _parameters.
        // - With ADD_PARAM_IN_ARRAY_NAME2(),
        //   the array name will appear in _parameters.
        //   So the next parameter will be _parameters[1].
        ADD_PARAM_IN_ARRAY_NAME2(PLACEHOLDER_ARRAY_NAME_VERSION|PLACEHOLDER_ARRAY_NAME_INDEX_NAME);
        ADD_PARAM_VARIES()
    }

    std::vector<boost::shared_ptr<OperatorParamPlaceholder> > nextVaryParamPlaceholder(const std::vector< ArrayDesc> &schemas)
    {
        std::vector<boost::shared_ptr<OperatorParamPlaceholder> > res;
        res.push_back(END_OF_VARIES_PARAMS());
        if (_parameters.size() == 1) {
            res.push_back(PARAM_CONSTANT(TID_BOOL));
        }
        return res;
    }

    void inferArrayAccess(boost::shared_ptr<Query>& query)
    {
        LogicalOperator::inferArrayAccess(query);

        assert(!_parameters.empty());
        assert(_parameters.front()->getParamType() == PARAM_ARRAY_REF);

        const string& arrayName = ((boost::shared_ptr<OperatorParamReference>&)_parameters.front())->getObjectName();

        assert(arrayName.find('@') == std::string::npos);

        ArrayDesc srcDesc;
        SystemCatalog::getInstance()->getArrayDesc(arrayName,srcDesc);
        if (srcDesc.isTransient())
        {
            boost::shared_ptr<SystemCatalog::LockDesc> lock(boost::make_shared<SystemCatalog::LockDesc>(arrayName,
                                                                                         query->getQueryID(),
                                                                                         Cluster::getInstance()->getLocalInstanceId(),
                                                                                         SystemCatalog::LockDesc::COORD,
                                                                                         SystemCatalog::LockDesc::WR));
            boost::shared_ptr<SystemCatalog::LockDesc> resLock(query->requestLock(lock));

            assert(resLock);
            assert(resLock->getLockMode() >= SystemCatalog::LockDesc::WR);
        }
        else
        {
            LogicalOperator::inferArrayAccess(query); // take read lock as per usual
        }
    }

    ArrayDesc inferSchema(std::vector< ArrayDesc> inputSchemas, boost::shared_ptr< Query> query)
    {
        assert(inputSchemas.size() == 0);
        assert(_parameters.size() == 1 || _parameters.size() == 2);
        assert(_parameters[0]->getParamType() == PARAM_ARRAY_REF);
        shared_ptr<OperatorParamArrayReference>& arrayRef = (shared_ptr<OperatorParamArrayReference>&)_parameters[0];
        assert(arrayRef->getArrayName().find('@') == string::npos);
        assert(arrayRef->getObjectName().find('@') == string::npos);

        if (arrayRef->getVersion() == ALL_VERSIONS) {
            throw USER_QUERY_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_WRONG_ASTERISK_USAGE2, _parameters[0]->getParsingContext());
        }
        ArrayDesc schema;
        SystemCatalog* systemCatalog = SystemCatalog::getInstance();

        systemCatalog->getArrayDesc(arrayRef->getObjectName(), arrayRef->getVersion(), schema);

        schema.addAlias(arrayRef->getObjectName());

        // Trim if the user wishes to.
        if (_parameters.size() == 2 // the user provided a true/false clause
            &&                       // and it's true
            evaluate(
                    ((boost::shared_ptr<OperatorParamLogicalExpression>&)_parameters[1])->getExpression(),
                    query,
                    TID_BOOL
                    ).getBool()
            )
        {
            schema.trim();

            // Without this change, harness test other.between_sub2 may fail.
            //
            // Once you trim the schema, the array is not the original array anymore.
            // Some operators, such as concat(), may go to the system catalog to find schema for input arrays if named.
            // We should make sure they do not succeed.
            schema.setName("");
        }

        return schema;
    }
};

DECLARE_LOGICAL_OPERATOR_FACTORY(LogicalScan, "scan")

} //namespace scidb

