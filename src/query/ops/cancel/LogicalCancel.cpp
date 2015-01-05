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
 * \file LogicalCancel.cpp
 *
 * \author roman.simakov@gmail.com
 * \brief Cancel operator cancels query with given ID
 */

#include "query/Operator.h"
#include "system/Exceptions.h"
#include "system/SystemCatalog.h"
#include "query/executor/SciDBExecutor.h"


using namespace std;

namespace scidb {

/**
 * @brief The operator: cancel().
 *
 * @par Synopsis:
 *   cancel( queryId )
 *
 * @par Summary:
 *   Cancels a query by ID.
 *
 * @par Input:
 *   - queryId: the query ID that can be obtained from the SciDB log or via the list() command.
 *
 * @par Output array:
 *   n/a
 *
 * @par Examples:
 *   n/a
 *
 * @par Errors:
 *   - SCIDB_SE_QPROC::SCIDB_LE_QUERY_NOT_FOUND: if queryId does not exist.
 *
 * @par Notes:
 *   - This operator is designed for internal use.
 *
 */class LogicalCancel: public LogicalOperator
{
public:
    LogicalCancel(const string& logicalName, const std::string& alias):
	    LogicalOperator(logicalName, alias)
	{
        ADD_PARAM_CONSTANT(TID_INT64)
        _properties.ddl = true;
	}

    ArrayDesc inferSchema(std::vector<ArrayDesc> schemas, boost::shared_ptr<Query> query)
	{
        int64_t queryID = evaluate(dynamic_pointer_cast<OperatorParamLogicalExpression>(
            _parameters[0])->getExpression(), query, TID_INT64).getInt64();
        try
        {
            query->getQueryByID(queryID, true);
        }
        catch(const Exception& e)
        {
            if (SCIDB_LE_QUERY_NOT_FOUND == e.getLongErrorCode())
            {
                throw CONV_TO_USER_QUERY_EXCEPTION(e, _parameters[0]->getParsingContext());
            }
            else
            {
                throw;
            }
        }
        return ArrayDesc();
	}
};

DECLARE_LOGICAL_OPERATOR_FACTORY(LogicalCancel, "cancel")


}  // namespace scidb
