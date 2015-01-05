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
 * @file LogicalNormalize.cpp
 *
 *  Created on: Mar 9, 2010
 */

#include "query/Operator.h"

namespace scidb
{

/**
 * @brief The operator: normalize().
 *
 * @par Synopsis:
 *   normalize( srcArray )
 *
 * @par Summary:
 *   Produces a result array by dividing each element of a 1-attribute vector by the square root of the sum of squares of the elements.
 *
 * @par Input:
 *   - srcArray: the source array with srcAttrs and srcDims. There should be exactly one attribute (of double type) and exactly one dimension.
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
class LogicalNormalize : public  LogicalOperator
{
  public:
	LogicalNormalize(const std::string& logicalName, const std::string& alias):
	    LogicalOperator(logicalName, alias)
	{
		ADD_PARAM_INPUT()
	}

    ArrayDesc inferSchema(std::vector< ArrayDesc> schemas, boost::shared_ptr< Query> query)
    {
        assert(schemas.size());

        if (schemas[0].getAttributes().size() != 1 && (schemas[0].getAttributes().size() != 2 || !schemas[0].getAttributes()[1].isEmptyIndicator()))
            throw USER_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_OP_NORMALIZE_ERROR1);
        if (schemas[0].getDimensions().size() != 1)
            throw USER_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_OP_NORMALIZE_ERROR2);
        if (schemas[0].getAttributes()[0].getType() != TID_DOUBLE)
            throw USER_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_OP_NORMALIZE_ERROR3);
        return schemas[0];
	}
};

DECLARE_LOGICAL_OPERATOR_FACTORY(LogicalNormalize, "normalize")

} //namespace scidb
