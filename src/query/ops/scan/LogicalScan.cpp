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
 *   scan( srcArray )
 *
 * @par Summary:
 *   Produces a result array that is equivalent to a stored array.
 *
 * @par Input:
 *   - srcArray: the array to scan, with srcAttrs and srcDims
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
        ADD_PARAM_IN_ARRAY_NAME2(PLACEHOLDER_ARRAY_NAME_VERSION|PLACEHOLDER_ARRAY_NAME_INDEX_NAME); //0
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

        if (arrayRef->getVersion() == ALL_VERSIONS) {
            throw USER_QUERY_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_WRONG_ASTERISK_USAGE2, _parameters[0]->getParsingContext());
        }
        ArrayDesc schema;
        SystemCatalog* systemCatalog = SystemCatalog::getInstance();

        systemCatalog->getArrayDesc(arrayRef->getObjectName(), arrayRef->getVersion(), schema);

        schema.addAlias(arrayRef->getObjectName());
        schema.trim();
        return schema;
    }
};

DECLARE_LOGICAL_OPERATOR_FACTORY(LogicalScan, "scan")

} //namespace scidb

