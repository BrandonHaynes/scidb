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

#include <query/Operator.h>
#include <system/Exceptions.h>
#include <query/LogicalExpression.h>

namespace scidb {

using namespace std;

/**
 * @brief The operator: regrid().
 *
 * @par Synopsis:
 *   regrid( srcArray {, blockSize}+ {, AGGREGATE_CALL}+ {, chunkSize}* )
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
 *   - 0 or numDims chunk sizes.
 *     If no chunk size is given, the chunk sizes from the input dims will be used.
 *     If at least one chunk size is given, the number of chunk sizes must be equal to the number of dimensions,
 *     and the specified chunk sizes will be used.
 *
 * @par Output array:
 *        <
 *   <br>   the aggregate calls' resultNames
 *   <br> >
 *   <br> [
 *   <br>   srcDims, with reduced size in every dimension, and the provided chunk sizes if any.
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
            // We must have numDims block sizes.
            res.push_back(PARAM_CONSTANT(TID_INT64));
        }
        else if (_parameters.size() == schemas[0].getDimensions().size())
        {
            // We must have at least one aggregate call.
            res.push_back(PARAM_AGGREGATE_CALL());
        }
        else
        {
            // The rest params are optional.
            res.push_back(END_OF_VARIES_PARAMS());

            boost::shared_ptr<OperatorParam> lastParam = _parameters[_parameters.size() - 1];
            if (lastParam->getParamType() == PARAM_AGGREGATE_CALL) {
                // If the last one was an aggregate call, this can can be either another aggregate call or a chunk size
                res.push_back(PARAM_AGGREGATE_CALL());
                res.push_back(PARAM_CONSTANT(TID_INT64));
            }
            else {
                // If the last one was a chunk size, this one must also be a chunk size.
                res.push_back(PARAM_CONSTANT(TID_INT64));
            }
        }
        return res;
    }

    ArrayDesc inferSchema(std::vector<ArrayDesc> schemas, boost::shared_ptr<Query> query)
    {
        assert(schemas.size() == 1);

        ArrayDesc const& inputDesc = schemas[0];
        size_t nDims = inputDesc.getDimensions().size();
        Dimensions outDims(nDims);

        // How many parameters are of each type.
        size_t numAggregateCalls = 0;
        size_t numChunkSizes = 0;
        for (size_t i = nDims, n = _parameters.size(); i < n; ++i)
        {
            if (_parameters[i]->getParamType() == PARAM_AGGREGATE_CALL)
            {
                ++numAggregateCalls;
            }
            else // chunk size
            {
                ++numChunkSizes;
            }
        }
        if (numChunkSizes && numChunkSizes != nDims) {
            throw USER_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_NUM_CHUNKSIZES_NOT_MATCH_NUM_DIMS) << "regrid()";
        }

        // Generate the output dims.
        for (size_t i = 0; i < nDims; i++)
        {
            int64_t blockSize = evaluate(((boost::shared_ptr<OperatorParamLogicalExpression>&)_parameters[i])->getExpression(),
                                        query, TID_INT64).getInt64();
            if (blockSize <= 0) {
                throw USER_QUERY_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_OP_REGRID_ERROR1,
                                           _parameters[i]->getParsingContext());
            }
            DimensionDesc const& srcDim = inputDesc.getDimensions()[i];
            int64_t chunkSize = static_cast<int64_t>(srcDim.getChunkInterval());
            if (numChunkSizes) {
                size_t index = i + nDims + numAggregateCalls;
                chunkSize = evaluate(((boost::shared_ptr<OperatorParamLogicalExpression>&)_parameters[index])->getExpression(),
                        query, TID_INT64).getInt64();
                if (chunkSize<=0) {
                    throw USER_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_CHUNK_SIZE_MUST_BE_POSITIVE);
                }
            }
            outDims[i] = DimensionDesc( srcDim.getBaseName(),
                                        srcDim.getNamesAndAliases(),
                                        srcDim.getStartMin(),
                                        srcDim.getStartMin(),
                                        srcDim.getEndMax() == MAX_COORDINATE ? MAX_COORDINATE : srcDim.getStartMin() + (srcDim.getLength() + blockSize - 1)/blockSize - 1,
                                        srcDim.getEndMax() == MAX_COORDINATE ? MAX_COORDINATE : srcDim.getStartMin() + (srcDim.getLength() + blockSize - 1)/blockSize - 1,
                                        static_cast<size_t>(chunkSize),
                                        0  );
        }

        ArrayDesc outSchema(inputDesc.getName(), Attributes(), outDims);

        for (size_t i = nDims, j=nDims+numAggregateCalls; i<j; i++)
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
