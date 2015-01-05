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
 * LogicalRegrid.cpp
 *
 *  Created on: Jul 25, 2011
 *      Author: poliocough@gmail.com
 */

#include <boost/shared_ptr.hpp>

#include "query/Operator.h"
#include "system/Exceptions.h"
#include "query/LogicalExpression.h"

namespace scidb {

using namespace std;


/**
 * @brief The operator: regrid().
 *
 * @par Synopsis:
 *   regrid( srcArray {, blockSize}+ {, AGGREGATE_CALL}+ )
 *   <br> AGGREGATE_CALL := AGGREGATE_FUNC(inputAttr) [as resultName]
 *   <br> AGGREGATE_FUNC := approxdc | avg | count | max | min | sum | stdev | var | some_use_defined_aggregate_function
 *
 * @par Summary:
 *   Partitions the cells in the source array into blocks (with the given blockSize in each dimension), and for each block,
 *   calculates the required aggregates.
 *
 * @par Input:
 *   - srcArray: the source array with srcAttrs and srcDims.
 *   - A list of blockSizes, one for each dimension.
 *   - 1 or more aggregate calls.
 *     Each aggregate call has an AGGREGATE_FUNC, an inputAttr and a resultName.
 *     The default resultName is inputAttr followed by '_' and then AGGREGATE_FUNC.
 *     For instance, the default resultName for sum(sales) is 'sales_sum'.
 *     The count aggregate may take * as the input attribute, meaning to count all the items in the group including null items.
 *     The default resultName for count(*) is 'count'.
 *
 * @par Output array:
 *        <
 *   <br>   the aggregate calls' resultNames
 *   <br> >
 *   <br> [
 *   <br>   srcDims, with reduced size in every dimension
 *   <br> ]
 *
 * @par Examples:
 *   n/a
 *
 * @par Errors:
 *   n/a
 *
 * @par Notes:
 *   - Regrid does not allow a block to span chunks. So for every dimension, the chunk interval needs to be a multiple of the block size.
 *
 */
class LogicalRegrid: public LogicalOperator
{
public:
    LogicalRegrid(const std::string& logicalName, const std::string& alias):
        LogicalOperator(logicalName, alias)
    {
        ADD_PARAM_INPUT()
        ADD_PARAM_VARIES()
    }

    std::vector<boost::shared_ptr<OperatorParamPlaceholder> > nextVaryParamPlaceholder(const std::vector<ArrayDesc> &schemas)
    {
        std::vector<boost::shared_ptr<OperatorParamPlaceholder> > res;
        if (_parameters.size() < schemas[0].getDimensions().size())
        {
            res.push_back(PARAM_CONSTANT("int64"));
        }
        else if (_parameters.size() == schemas[0].getDimensions().size())
        {
            res.push_back(PARAM_AGGREGATE_CALL());
        }
        else
        {
            res.push_back(END_OF_VARIES_PARAMS());
            res.push_back(PARAM_AGGREGATE_CALL());
        }
        return res;
    }

    ArrayDesc inferSchema(std::vector<ArrayDesc> schemas, boost::shared_ptr<Query> query)
    {
        assert(schemas.size() == 1);

        ArrayDesc const& inputDesc = schemas[0];
        size_t nDims = inputDesc.getDimensions().size();
        Dimensions outDims(nDims);

        for (size_t i = 0; i < nDims; i++)
        {
            int64_t interval = evaluate(((boost::shared_ptr<OperatorParamLogicalExpression>&)_parameters[i])->getExpression(),
                                        query, TID_INT64).getInt64();
            if (interval <= 0)
                throw USER_QUERY_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_OP_REGRID_ERROR1,
                                           _parameters[i]->getParsingContext());
            DimensionDesc const& srcDim = inputDesc.getDimensions()[i];
            outDims[i] = DimensionDesc( srcDim.getBaseName(),
                                        srcDim.getNamesAndAliases(),
                                        srcDim.getStart(),
                                        srcDim.getStart(),
                                        srcDim.getEndMax() == MAX_COORDINATE ? MAX_COORDINATE : srcDim.getStart() + (srcDim.getLength() + interval - 1)/interval - 1,
                                        srcDim.getEndMax() == MAX_COORDINATE ? MAX_COORDINATE : srcDim.getStart() + (srcDim.getLength() + interval - 1)/interval - 1,
                                        srcDim.getChunkInterval(),
                                        0  );
        }

        ArrayDesc outSchema(inputDesc.getName(), Attributes(), outDims);

        for (size_t i = nDims, j=_parameters.size(); i<j; i++)
        {
            bool isInOrderAggregation = false;
            addAggregatedAttribute( (shared_ptr <OperatorParamAggregateCall> &) _parameters[i], inputDesc, outSchema,
                    isInOrderAggregation);
        }

        AttributeDesc et ((AttributeID) outSchema.getAttributes().size(), DEFAULT_EMPTY_TAG_ATTRIBUTE_NAME,  TID_INDICATOR, AttributeDesc::IS_EMPTY_INDICATOR, 0);
        outSchema.addAttribute(et);

        return outSchema;
    }
};

DECLARE_LOGICAL_OPERATOR_FACTORY(LogicalRegrid, "regrid")


}  // namespace ops
