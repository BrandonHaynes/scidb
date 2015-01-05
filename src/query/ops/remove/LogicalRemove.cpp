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
 * LogicalRemove.cpp
 *
 *  Created on: Apr 17, 2010
 *      Author: Knizhnik
 */

#include "query/Operator.h"
#include "system/Exceptions.h"
#include "system/SystemCatalog.h"


using namespace std;

namespace scidb {

/**
 * @brief The operator: remove().
 *
 * @par Synopsis:
 *   remove( arrayToRemove )
 *
 * @par Summary:
 *   Drops an array.
 *
 * @par Input:
 *   - arrayToRemove: the array to drop.
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
class LogicalRemove: public LogicalOperator
{
public:
	LogicalRemove(const string& logicalName, const std::string& alias):
	    LogicalOperator(logicalName, alias)
	{
		ADD_PARAM_IN_ARRAY_NAME()
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
        assert(_parameters.size() == 1);
        assert(_parameters[0]->getParamType() == PARAM_ARRAY_REF);
        const string& arrayName = ((boost::shared_ptr<OperatorParamReference>&)_parameters[0])->getObjectName();
        assert(arrayName.find('@') == std::string::npos);
        boost::shared_ptr<SystemCatalog::LockDesc>  lock(new SystemCatalog::LockDesc(arrayName,
                                                                                     query->getQueryID(),
                                                                                     Cluster::getInstance()->getLocalInstanceId(),
                                                                                     SystemCatalog::LockDesc::COORD,
                                                                                     SystemCatalog::LockDesc::RM));
        boost::shared_ptr<SystemCatalog::LockDesc> resLock = query->requestLock(lock);
        assert(resLock);
        assert(resLock->getLockMode() >= SystemCatalog::LockDesc::RM);
    }
};

DECLARE_LOGICAL_OPERATOR_FACTORY(LogicalRemove, "remove")


}  // namespace scidb
