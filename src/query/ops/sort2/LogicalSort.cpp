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
 * LogicalSort2.cpp
 *
 *  Created on: Aug 15, 2010
 *      Author: knizhnik@garret.ru
 */

#include "query/Operator.h"
#include "system/Exceptions.h"

using namespace std;

namespace scidb
{

/**
 * @brief The operator: sort2().
 *
 * @par Synopsis:
 *   sort2( srcArray )
 *
 * @par Summary:
 *   This internal operator is used by the second phase of sort to merge results from different instances.
 *
 * @par Input:
 *   - srcArray: the source array with srcAttrs and srcDim.
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
 *   - For internal use.
 *
 */
class LogicalSort2 : public  LogicalOperator
{
public:
	LogicalSort2(const std::string& logicalName, const std::string& alias):
	    LogicalOperator(logicalName, alias)
	{
        _properties.secondPhase = true;
        ADD_PARAM_INPUT()
	}

    ArrayDesc inferSchema(std::vector< ArrayDesc> schemas, boost::shared_ptr< Query> query)
    {
        assert(schemas.size() == 1);
        ArrayDesc const& schema = schemas[0];
        return ArrayDesc(schema.getName(), schema.getAttributes(), schema.getDimensions());
	}
};

DECLARE_LOGICAL_OPERATOR_FACTORY(LogicalSort2, "sort2")

} //namespace scidb
