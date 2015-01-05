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
 * LogicalTranspose.cpp
 *
 *  Created on: Mar 9, 2010
 */

#include "query/Operator.h"
#include "system/Exceptions.h"


namespace scidb
{

/**
 * @brief The operator: transpose().
 *
 * @par Synopsis:
 *   transpose( srcArray )
 *
 * @par Summary:
 *   Produces an array with the same data in srcArray but with the list of dimensions reversd.
 *
 * @par Input:
 *   - srcArray: a source array with srcAttrs and srcDims.
 *
 * @par Output array:
 *        <
 *   <br>   srcAttrs.
 *   <br> >
 *   <br> [
 *   <br>   reverse order of srcDims
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
class LogicalTranspose : public LogicalOperator
{
public:
    /**
     * Create the transpose operator.
     * @param logicalName the operator name
     * @param alias the alias
     */
    LogicalTranspose(const std::string& logicalName, const std::string& alias):
        LogicalOperator(logicalName, alias)
    {
        ADD_PARAM_INPUT()
    }

    /**
     * Determine the schema of the output.
     * @param schemas the shapes of all the input arrays, only one expected
     * @param query the query context
     * @return the 0th element of schemas with the dimensions in reverse order
     */
    ArrayDesc inferSchema(std::vector< ArrayDesc> schemas, boost::shared_ptr< Query> query)
    {
        assert(schemas.size() == 1);
        assert(_parameters.size() == 0);

        ArrayDesc const& schema = schemas[0];

        Dimensions const& dims(schema.getDimensions());   
        Dimensions transDims(dims.size());

        for (size_t i = 0, n = dims.size(); i < n; i++)
        {
            transDims[n-i-1] = dims[i];
        }

        return ArrayDesc(schema.getName(), schema.getAttributes(), transDims);
	}

};

DECLARE_LOGICAL_OPERATOR_FACTORY(LogicalTranspose, "transpose")

} //namespace scidb
