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
 * LogicalRemoveVersions.cpp
 *
 *  Created on: Jun 11, 2014
 *      Author: sfridella
 */

#include "query/Operator.h"
#include "system/Exceptions.h"
#include "system/SystemCatalog.h"


using namespace std;

namespace scidb {

/**
 * @brief The operator: remove_versions().
 *
 * @par Synopsis:
 *   remove_versions( targetArray, oldestVersionToSave )
 *
 * @par Summary:
 *   Removes all versions of targetArray that are older than
 *   oldestVersionToSave
 *
 * @par Input:
 *   - targetArray: the array which is targeted.
 *   - oldestVersionToSave: the version, prior to which all versions will be removed.
 *
 * @par Output array:
 *   NULL
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
class LogicalRemoveVersions: public LogicalOperator
{
public:
    LogicalRemoveVersions(const string& logicalName, const std::string& alias):
        LogicalOperator(logicalName, alias)
	{
            ADD_PARAM_IN_ARRAY_NAME()
            ADD_PARAM_CONSTANT("uint64")    
            _properties.exclusive = true;
            _properties.ddl = true;
	}
    
    ArrayDesc inferSchema(std::vector< ArrayDesc> schemas, boost::shared_ptr< Query> query)
    {
        assert(schemas.size() == 0);
        return ArrayDesc();
    }

    void inferArrayAccess(boost::shared_ptr<Query>& query)
    {
        LogicalOperator::inferArrayAccess(query);
        assert(_parameters.size() == 2);
        assert(_parameters[0]->getParamType() == PARAM_ARRAY_REF);
        const string& arrayName = 
            ((boost::shared_ptr<OperatorParamReference>&)_parameters[0])->getObjectName();
        assert(arrayName.find('@') == std::string::npos);
        VersionID targetVersion =
            evaluate(((boost::shared_ptr<OperatorParamLogicalExpression>&)_parameters[1])->getExpression(),
                     query, 
                     TID_INT64).getInt64();
        boost::shared_ptr<SystemCatalog::LockDesc>  lock(
            new SystemCatalog::LockDesc(arrayName,
                                        query->getQueryID(),
                                        Cluster::getInstance()->getLocalInstanceId(),
                                        SystemCatalog::LockDesc::COORD,
                                        SystemCatalog::LockDesc::RM)
            );
        lock->setArrayVersion(targetVersion);
        boost::shared_ptr<SystemCatalog::LockDesc> resLock = query->requestLock(lock);
        assert(resLock);
        assert(resLock->getLockMode() >= SystemCatalog::LockDesc::RM);
    }
};

DECLARE_LOGICAL_OPERATOR_FACTORY(LogicalRemoveVersions, "remove_versions")


}  // namespace scidb
