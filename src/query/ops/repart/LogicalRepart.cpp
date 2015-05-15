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
 * LogicalRepart.cpp
 *
 *  Created on: Apr 20, 2010
 *      Author: Knizhnik
 */

#include <query/Operator.h>
#include <system/SystemCatalog.h>
#include <system/Exceptions.h>

using namespace std;

namespace scidb
{

/**
 * @brief The operator: repart().
 *
 * @par Synopsis:
 *   repart( srcArray, schema )
 *
 * @par Summary:
 *   Produces a result array similar to the source array, but with different chunk sizes, different chunk overlaps, or both.
 *
 * @par Input:
 *   - srcArray: the source array with srcAttrs and srcDims.
 *   - schema: the desired schema.
 *
 * @par Output array:
 *        <
 *   <br>   srcAttrs
 *   <br> >
 *   <br> [
 *   <br>   dimensions from the desired schema
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
class LogicalRepart: public LogicalOperator
{
public:
    LogicalRepart(const string& logicalName, const std::string& alias):
        LogicalOperator(logicalName, alias)
    {
        ADD_PARAM_INPUT()
        ADD_PARAM_SCHEMA()
    }

    ArrayDesc inferSchema(std::vector< ArrayDesc> schemas, boost::shared_ptr< Query> query)
    {
        assert(schemas.size() == 1);
        assert(_parameters.size() == 1);

        ArrayDesc schemaParam = ((boost::shared_ptr<OperatorParamSchema>&)_parameters[0])->getSchema();

        ArrayDesc const& srcArrayDesc = schemas[0];
        Attributes const& srcAttributes = srcArrayDesc.getAttributes();
        Dimensions const& srcDimensions = srcArrayDesc.getDimensions();
        Dimensions dstDimensions = schemaParam.getDimensions();

        if (schemaParam.getName().size() == 0)
        {
            schemaParam.setName(srcArrayDesc.getName());
        }

        if (srcDimensions.size() != dstDimensions.size())
        {
            ostringstream left, right;
            printDimNames(left, srcDimensions);
            printDimNames(right, dstDimensions);
            throw USER_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_DIMENSION_COUNT_MISMATCH)
                << "repart" << left.str() << right.str();
        }

        for (size_t i = 0, n = srcDimensions.size(); i < n; i++)
        {
            if (srcDimensions[i].getStartMin() != dstDimensions[i].getStartMin())
                throw USER_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_OP_REPART_ERROR3);
            if (!(srcDimensions[i].getEndMax() == dstDimensions[i].getEndMax() 
                               || (srcDimensions[i].getEndMax() < dstDimensions[i].getEndMax() 
                                   && ((srcDimensions[i].getLength() % srcDimensions[i].getChunkInterval()) == 0
                                       || srcArrayDesc.getEmptyBitmapAttribute() != NULL))))
                throw USER_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_OP_REPART_ERROR4);
            if (srcDimensions[i].getStartMin() == MIN_COORDINATE)
                throw USER_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_OP_REPART_ERROR5);
        }
        return ArrayDesc(schemaParam.getName(), srcAttributes, dstDimensions, schemaParam.getFlags());
    }
};

DECLARE_LOGICAL_OPERATOR_FACTORY(LogicalRepart, "repart")


} //namespace
