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
 * LogicalRename.cpp
 *
 *  Created on: Apr 17, 2010
 *      Author: Knizhnik
 */

#include <boost/format.hpp>

#include "query/Operator.h"
#include "system/Exceptions.h"
#include "system/SystemCatalog.h"

namespace scidb {

using namespace std;
using namespace boost;

/**
 * @brief The operator: rename().
 *
 * @par Synopsis:
 *   rename( oldArray, newArray )
 *
 * @par Summary:
 *   Changes the name of an array.
 *
 * @par Input:
 *   - oldArray: an existing array.
 *   - newArray: the new name of the array.
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
class LogicalRename: public LogicalOperator
{
public:
	LogicalRename(const string& logicalName, const std::string& alias):
	    LogicalOperator(logicalName, alias)
	{
	    _properties.exclusive = true;
	    _properties.ddl = true;
		ADD_PARAM_IN_ARRAY_NAME()
		ADD_PARAM_OUT_ARRAY_NAME()
	}

    ArrayDesc inferSchema(std::vector<ArrayDesc> schemas, boost::shared_ptr<Query> query)
    {
        assert(schemas.size() == 0);
        assert(_parameters.size() == 2);
        assert(((boost::shared_ptr<OperatorParam>&)_parameters[1])->getParamType() == PARAM_ARRAY_REF);

        const string &newArrayName = ((boost::shared_ptr<OperatorParamReference>&)_parameters[1])->getObjectName();

        if (SystemCatalog::getInstance()->containsArray(newArrayName))
        {
            throw USER_QUERY_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_ARRAY_ALREADY_EXIST,
                _parameters[1]->getParsingContext()) << newArrayName;
        }

        return ArrayDesc();
	}

    void inferArrayAccess(boost::shared_ptr<Query>& query)
    {
        LogicalOperator::inferArrayAccess(query);
        assert(_parameters.size() > 0);
        assert(_parameters[0]->getParamType() == PARAM_ARRAY_REF);
        const string& oldArrayName = ((boost::shared_ptr<OperatorParamReference>&)_parameters[0])->getObjectName();
        assert(oldArrayName.find('@') == std::string::npos);
        boost::shared_ptr<SystemCatalog::LockDesc> lock(new SystemCatalog::LockDesc(oldArrayName,
                                                                                    query->getQueryID(),
                                                                                    Cluster::getInstance()->getLocalInstanceId(),
                                                                                    SystemCatalog::LockDesc::COORD,
                                                                                    SystemCatalog::LockDesc::RNF));
        boost::shared_ptr<SystemCatalog::LockDesc> resLock = query->requestLock(lock);
        assert(resLock);
        assert(resLock->getLockMode() >= SystemCatalog::LockDesc::RNF);
    }
};

DECLARE_LOGICAL_OPERATOR_FACTORY(LogicalRename, "rename")


}  // namespace ops
