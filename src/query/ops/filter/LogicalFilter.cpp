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
 * LogicalFilter.cpp
 *
 *  Created on: Apr 11, 2010
 *      Author: Knizhnik
 */

#include <query/Operator.h>

namespace scidb {

using namespace std;

/**
 * @brief The operator: filter().
 *
 * @par Synopsis:
 *   filter( srcArray, expression )
 *
 * @par Summary:
 *   The filter operator returns an array the with the same schema as the input
 *   array. The result is identical to the input except that those cells for
 *   which the expression evaluates either false or null are marked as being
 *   empty.
 *
 * @par Input:
 *   - srcArray: a source array with srcAttrs and srcDims.
 *   - expression: an expression which takes a cell in the source array as input and evaluates to either True or False.
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
class LogicalFilter: public  LogicalOperator
{
public:
	LogicalFilter(const std::string& logicalName, const std::string& alias):
	        LogicalOperator(logicalName, alias)
    {
        _properties.tile = true;
		ADD_PARAM_INPUT()
        ADD_PARAM_EXPRESSION("bool")
    }

    virtual bool compileParamInTileMode(size_t paramNo) { 
        return paramNo == 0;
    }

    ArrayDesc inferSchema(std::vector< ArrayDesc> schemas, boost::shared_ptr< Query> query)
	{
		assert(schemas.size() == 1);
		assert(_parameters.size() == 1);
		assert(_parameters[0]->getParamType() == PARAM_LOGICAL_EXPRESSION);

		return addEmptyTagAttribute(schemas[0]);
	}
};

DECLARE_LOGICAL_OPERATOR_FACTORY(LogicalFilter, "filter")


}  // namespace scidb
