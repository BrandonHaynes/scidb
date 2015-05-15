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
 * LogicalCast.cpp
 *
 *  Created on: Apr 20, 2010
 *      Author: Knizhnik
 */

#include "query/Operator.h"
#include "system/SystemCatalog.h"
#include "system/Exceptions.h"

using namespace std;

namespace scidb
{

/**
 * @brief The operator: cast().
 *
 * @par Synopsis:
 *   cast( srcArray, schemaArray | schema )
 *
 * @par Summary:
 *   Produces a result array with data from srcArray but with the provided schema.
 *   There are three primary purposes:
 *   - To change names of attributes or dimensions.
 *   - To change types of attributes
 *   - To change a non-integer dimension to an integer dimension.
 *   - To change a nulls-disallowed attribute to a nulls-allowed attribute.
 *
 * @par Input:
 *   - srcArray: a source array.
 *   - schemaArray | schema: an array or a schema, from which attrs and dims will be used by the output array.
 *
 * @par Output array:
 *        <
 *   <br>   attrs
 *   <br> >
 *   <br> [
 *   <br>   dims
 *   <br> ]
 *
 * @par Examples:
 *   - Given array A <quantity: uint64, sales:double> [year, item] =
 *     <br> year, item, quantity, sales
 *     <br> 2011,  2,      7,     31.64
 *     <br> 2011,  3,      6,     19.98
 *     <br> 2012,  1,      5,     41.65
 *     <br> 2012,  2,      9,     40.68
 *     <br> 2012,  3,      8,     26.64
 *   - cast(A, <q:uint64, s:double>[y=2011:2012,2,0, i=1:3,3,0]) <q:uint64, s:double> [y, i] =
 *     <br>  y,    i,      q,       s
 *     <br> 2011,  2,      7,     31.64
 *     <br> 2011,  3,      6,     19.98
 *     <br> 2012,  1,      5,     41.65
 *     <br> 2012,  2,      9,     40.68
 *     <br> 2012,  3,      8,     26.64
 *
 * @par Errors:
 *   n/a
 *
 * @par Notes:
 *   n/a
 *
 */
class LogicalCast: public LogicalOperator
{
public:
    LogicalCast(const string& logicalName, const std::string& alias):
        LogicalOperator(logicalName, alias)
    {
        _properties.tile = true;
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
        Attributes const& dstAttributes = schemaParam.getAttributes();
        Dimensions dstDimensions = schemaParam.getDimensions();

        if (schemaParam.getName().size() == 0)
        {
            schemaParam.setName(srcArrayDesc.getName());
        }

        const boost::shared_ptr<ParsingContext> &pc = _parameters[0]->getParsingContext();
        if (srcAttributes.size() != dstAttributes.size()
            && srcAttributes.size() != schemaParam.getAttributes(true).size())
        {
            throw USER_QUERY_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_ATTR_COUNT_MISMATCH, pc)
                << srcArrayDesc << schemaParam;
        }

        for (size_t i = 0, n = srcAttributes.size(); i < n; i++)
        {
            if (srcAttributes[i].getType() != dstAttributes[i].getType())
            {
                //Check if we can cast source type to destination.
                //If no give proper error message
                try
                {
                    FunctionLibrary::getInstance()->findConverter(
                            srcAttributes[i].getType(), dstAttributes[i].getType());
                }
                catch (const Exception &e)
                {
                    throw USER_QUERY_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_OP_CAST_ERROR10, pc)
                            << srcAttributes[i].getName()
                            << srcAttributes[i].getType()
                            << dstAttributes[i].getType();
                }
            }
            if  ( dstAttributes[i].getFlags()!= srcAttributes[i].getFlags() &&
                  dstAttributes[i].getFlags()!= (srcAttributes[i].getFlags() | AttributeDesc::IS_NULLABLE ))
                throw USER_QUERY_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_OP_CAST_ERROR3, pc)  << dstAttributes[i].getName();
        }

        if (srcDimensions.size() != dstDimensions.size())
        {
            ostringstream left, right;
            printDimNames(left, srcDimensions);
            printDimNames(right, dstDimensions);
            throw USER_QUERY_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_DIMENSION_COUNT_MISMATCH, pc)
                << "cast" << left.str() << right.str();
        }

        for (size_t i = 0, n = srcDimensions.size(); i < n; i++) {
            DimensionDesc const& srcDim = srcDimensions[i];
            DimensionDesc const& dstDim = dstDimensions[i];
            if (!(srcDim.getEndMax() == dstDim.getEndMax()
                  || (srcDim.getEndMax() < dstDim.getEndMax()
                      && ((srcDim.getLength() % srcDim.getChunkInterval()) == 0
                          || srcArrayDesc.getEmptyBitmapAttribute() != NULL))))
                throw USER_QUERY_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_OP_CAST_ERROR5, pc) << dstDim.getBaseName();

            if (!srcDim.getEndMax() != dstDim.getEndMax()) {
                _properties.tile = false;
            }

            //If input is bounded and cast is unbounded -- don't change the bounds of input.
            //Changing bounds is not cheap, especially the lower bound.
            //If you want to change the bounds, properly, use subarray.
            dstDimensions[i] = DimensionDesc(dstDim.getBaseName(),
                                             dstDim.getNamesAndAliases(),
                                             srcDim.getStartMin(),
                                             srcDim.getCurrStart(),
                                             srcDim.getCurrEnd(),
                                             dstDim.getEndMax() == MAX_COORDINATE && srcDim.getCurrEnd() != MIN_COORDINATE ? srcDim.getEndMax() : dstDim.getEndMax(),
                                             srcDim.getChunkInterval(),
                                             srcDim.getChunkOverlap());
        }
        return ArrayDesc(schemaParam.getName(), dstAttributes, dstDimensions, schemaParam.getFlags());
    }
};

DECLARE_LOGICAL_OPERATOR_FACTORY(LogicalCast, "cast")

} //namespace
