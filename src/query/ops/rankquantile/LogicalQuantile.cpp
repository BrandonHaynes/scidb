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
 * LogicalQuantile.cpp
 *  Created on: Mar 11, 2011
 *      Author: poliocough@gmail.com
 */

#include <boost/shared_ptr.hpp>
#include <boost/foreach.hpp>

#include <query/Operator.h>
#include <system/Exceptions.h>
#include <query/LogicalExpression.h>

using namespace std;

namespace scidb
{

/**
 * @brief The operator: quantile().
 *
 * @par Synopsis:
 *   quantile( srcArray, numQuantiles [, attr {, groupbyDim}*] )
 *
 * @par Summary:
 *   Computes the quantiles of an array, based on the ordering of attr (within each group as specified by groupbyDim, if specified).
 *   If groupbyDim is not specified, global ordering will be performed.
 *   If attr is not specified, the first attribute will be used.
 *
 * @par Input:
 *   - srcArray: the source array with srcAttrs and srcDims.
 *   - numQuantiles: the number of quantiles.
 *   - attr: which attribute to sort on. The default is the first attribute.
 *   - groupbyDim: if provided, the ordering will be performed among the records in the same group.
 *
 * @par Output array:
 *        <
 *   <br>   percentage: a double value from 0.0 to 1.0
 *   <br>   attr_quantile: the source attribute name followed by '_quantile'.
 *   <br> >
 *   <br> [
 *   <br>   groupbyDims (if provided)
 *   <br>   quantile: start=0, end=numQuantiles, chunkInterval=numQuantiles+1
 *   <br> ]
 *
 * @par Examples:
 *   - Given array A <v:int64> [i=0:5,3,0] =
 *     <br> i,  v
 *     <br> 0,  0
 *     <br> 1,  1
 *     <br> 2,  2
 *     <br> 3,  3
 *     <br> 4,  4
 *     <br> 5,  5
 *   - quantile(A, 2) <percentage, v_quantile>[quantile=0:2,3,0] =
 *     <br> {quantile} percentage, v_quantile
 *     <br> {0}           0,         0
 *     <br> {1}           0.5,       2
 *     <br> {2}           1,         5
 *
 * @par Errors:
 *   n/a
 *
 * @par Notes:
 *   n/a
 *
 */
class LogicalQuantile: public LogicalOperator
{
public:
    LogicalQuantile(const std::string& logicalName, const std::string& alias):
        LogicalOperator(logicalName, alias)
    {
        ADD_PARAM_INPUT();
        ADD_PARAM_CONSTANT("uint32");
        ADD_PARAM_VARIES();
    }

    std::vector<boost::shared_ptr<OperatorParamPlaceholder> > nextVaryParamPlaceholder(const std::vector<ArrayDesc> &schemas)
    {
        std::vector<boost::shared_ptr<OperatorParamPlaceholder> > res;
        res.push_back(END_OF_VARIES_PARAMS());
        if (_parameters.size() == 1)
        {
            res.push_back(PARAM_IN_ATTRIBUTE_NAME("void"));
        }
        else
        {
            res.push_back(PARAM_IN_DIMENSION_NAME());
        }

        return res;
    }
    ArrayDesc inferSchema(std::vector<ArrayDesc> schemas, boost::shared_ptr<Query> query)
    {
        ArrayDesc const& input = schemas[0];

        assert(schemas.size() == 1);

        uint32_t numQuantilesPlusOne = 1 +
                evaluate(((boost::shared_ptr<OperatorParamLogicalExpression>&)_parameters[0])->getExpression(), query, TID_UINT32).getUint32();
        if (numQuantilesPlusOne < 2) {
            throw USER_QUERY_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_DLA_ERROR17,
                _parameters[0]->getParsingContext());
        }

        string attName = _parameters.size() > 1 ? ((boost::shared_ptr<OperatorParamReference>&)_parameters[1])->getObjectName() :
                                                    input.getAttributes()[0].getName();

        bool found = false;
        AttributeDesc inputAttribute;
        BOOST_FOREACH(const AttributeDesc& att, input.getAttributes())
        {
            if (att.getName() == attName)
            {
                found = true;
                inputAttribute = att;
            }
        }
        if (!found) {
            throw USER_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_DLA_ERROR14);
        }

        Attributes outputAttrs;
        outputAttrs.push_back(AttributeDesc(0, "percentage", TID_DOUBLE, 0,0));
        outputAttrs.push_back(AttributeDesc(1, attName + "_quantile", inputAttribute.getType(), AttributeDesc::IS_NULLABLE, 0));

        Dimensions outputDims;
        if (_parameters.size()>2)
        {
            size_t i, j;
            for (i = 0; i < _parameters.size() - 2; i++)
            {
                const string& dimName = ((boost::shared_ptr<OperatorParamReference>&)_parameters[i + 2])->getObjectName();
                const string& dimAlias = ((boost::shared_ptr<OperatorParamReference>&)_parameters[i + 2])->getArrayName();
                for (j = 0; j < input.getDimensions().size(); j++)
                {
                    DimensionDesc const& dim = input.getDimensions()[j];
                    if (dim.hasNameAndAlias(dimName, dimAlias))
                    {
                        if (dim.getEndMax() == MAX_COORDINATE) {
                            throw USER_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_QUANTILE_REQUIRES_BOUNDED_ARRAY);
                        }

                        //no overlap
                        outputDims.push_back(DimensionDesc( dim.getBaseName(),
                                                            dim.getNamesAndAliases(),
                                                            dim.getStartMin(),
                                                            dim.getCurrStart(),
                                                            dim.getCurrEnd(),
                                                            dim.getEndMax(),
                                                            dim.getChunkInterval(),
                                                            0));
                        break;
                    }
                }
                if (j >= input.getDimensions().size()) {
                    throw USER_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_DLA_ERROR16);
                }
            }
        }

        DimensionDesc quantileDim(
                "quantile",
                0,                     // startMin
                0,                     // currStart
                numQuantilesPlusOne-1, // currEnd
                numQuantilesPlusOne-1, // endMax
                numQuantilesPlusOne,   // chunkInterval
                0);                    // chunkOverlap
        outputDims.push_back(quantileDim);

        return ArrayDesc(input.getName()+"_quantile", outputAttrs, outputDims);
    }
};

DECLARE_LOGICAL_OPERATOR_FACTORY(LogicalQuantile, "quantile")

}
