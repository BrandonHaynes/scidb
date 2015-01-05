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
 * LogicalCumulate.cpp
 *  Created on: 9/24/2013
 *      Author: paulgeoffreybrown@gmail.com
 *              Donghui Zhang
 */
#include <query/Operator.h>
#include <boost/shared_ptr.hpp>
#include <boost/foreach.hpp>

using namespace std;
using namespace boost;

namespace scidb
{

/**
 * @brief  The operator: cumulate().
 *
 * @par Synopsis:
 *  cumulate ( inputArray {, AGGREGATE_ALL}+ [, aggrDim] )
 *  <br> AGGREGATE_CALL := AGGREGATE_FUNC ( inputAttribute ) [ AS aliasName ]
 *  <br> AGGREGATE_FUNC := approxdc | avg | count | max | min | sum | stdev | var | some_use_defined_aggregate_function
 *
 * @par Summary:
 *
 *   Calculates a running aggregate over some aggregate along some fluxVector (a single dimension of the inputArray).
 *
 * @par Input:
 *
 *    - inputArray: an input array
 *    - 1 or more aggregate calls.
 *    - aggrDim: the name of a dimension along with aggregates are computed.
 *      Default is the first dimension.
 *
 * @par Output array:
 *        <
 *   <br>  The aggregate calls' aliasNames with corresponding types.
 *   <br> >
 *   <br> [
 *   <br>   The output array has the same size and shape as the inputArray.
 *   <br> ]
 *
 *
 *  @par Examples:
 *  @verbatim

     input:                cumulate(input, sum(v) as sum_v, count(*) as cnt, I)
    +-I->
   J|     00   01   02   03              00       01       02       03
    V   +----+----+----+----+        +--------+--------+--------+--------+
    00  | 01 |    | 02 |    |   00   | (1, 1) |        | (3, 2) |        |
        +----+----+----+----+        +--------+--------+--------+--------+
    01  |    | 03 |    | 04 |   01   |        | (3, 1) |        | (7, 2) |
        +----+----+----+----+        +--------+--------+--------+--------+
    02  | 05 |    | 06 |    |   02   | (5, 1) |        | (11, 2)|        |
        +----+----+----+----+        +--------+--------+--------+--------+
    03  |    | 07 |    | 08 |   03   |        | (7, 1) |        | (15, 2)|
        +----+----+----+----+        +--------+--------+--------+--------+
    @endverbatim
 *
 *  @par Errors:
 *
 *  @par Notes:
 *    - For now, cumulate does NOT handle input array that have overlaps.
 */
class LogicalCumulate: public LogicalOperator
{
public:

    LogicalCumulate(const std::string& logicalName, const std::string& alias):
                   LogicalOperator(logicalName, alias)
    {
        //
        //  Because the operator needs to sweep through its inputs to compute
        // a single cell of output, the most efficient way to implement
        // cumulate(...) is to materialize each chunk. This makes is possible
        // to support tile mode access to the result of cumulate(...).
        _properties.tile = true;

        ADD_PARAM_INPUT()
        ADD_PARAM_AGGREGATE_CALL()
        ADD_PARAM_VARIES()
    }

    /**
     *  @see LogicalOperator::nextVaryParamPlaceholder()
     */
    std::vector<boost::shared_ptr<OperatorParamPlaceholder> >
                 nextVaryParamPlaceholder(const std::vector<ArrayDesc> &schemas)
    {
        std::vector<boost::shared_ptr<OperatorParamPlaceholder> > res;
        res.push_back(END_OF_VARIES_PARAMS());

        assert(_parameters.size()>0); // at least one aggregate call was already in there

        if (_parameters[_parameters.size()-1]->getParamType() != PARAM_DIMENSION_REF) {
            res.push_back(PARAM_AGGREGATE_CALL());
            res.push_back(PARAM_IN_DIMENSION_NAME());
        }
        return res;
    }

    /**
     *  @see LogicalOperator::inferSchema()
     *
     *  The output array for the cumulate(...) will have the same size and
     *  shape as the input array, and one attribute for each of
     *  the aggregate expressions.
     *
     */
    ArrayDesc inferSchema( std::vector<ArrayDesc> inputArraySchemas,
                           boost::shared_ptr<Query> query)
    {
        //  Check that there is exactly one input array, and at least one
        // aggregate in the _parameters list.
        //
        if (inputArraySchemas.size() != 1 ||
                _parameters.size() == 0 ||
                _parameters[0]->getParamType() != PARAM_AGGREGATE_CALL )
        {
            throw USER_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_CUMULATE_NEEDS_AGGREGATES);
        }

        //  Start by creating an output Schema ArrayDesc that uses the
        // input schema's name, and the the same size and shape as the
        // input schema. Initially the output schema has no attributes.
        //
        ArrayDesc const& inputSchema = inputArraySchemas[0];
        Dimensions const& inputDims  = inputSchema.getDimensions();

        ArrayDesc outputSchema( inputSchema.getName(), Attributes(), inputDims );

        // TO-DO: deal with dimensions with overlaps later
        //
        for (size_t i=0; i<inputDims.size(); ++i) {
            if (inputDims[i].getChunkOverlap()>0) {
                throw USER_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_CUMULATE_NO_OVERLAP);
            }
        }

        // Iterate over the _parameters, adding an output attribute for
        // each aggregate expression, and checking that the flux vector
        // dimension is present in the input schema.
        bool hasDimension = false;

        for (size_t i = 0, len = _parameters.size(); i<len; i++)
        {
            switch(_parameters[i]->getParamType())
            {
            case PARAM_AGGREGATE_CALL:
            {
                if (hasDimension) {
                    throw USER_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_CUMULATE_DIM_AFTER_AGGREGATES);
                }
                bool isInOrderAggregation = true;
                addAggregatedAttribute(
                       (shared_ptr <OperatorParamAggregateCall> &)_parameters[i],
                       inputSchema,
                       outputSchema,
                       isInOrderAggregation);
                break;
            }
            case PARAM_DIMENSION_REF:
            {
                SCIDB_ASSERT ( !hasDimension ); // There can be only 1 dimension
                string dimName =  dynamic_pointer_cast<OperatorParamDimensionReference>(_parameters[i])->getObjectName();
                bool found = false;
                for (size_t j = 0; j < inputDims.size(); j++) {
                    if (inputDims[j].hasNameAndAlias(dimName)) {
                        found = true;
                        break;
                    }
                }
                if ( !found ) {
                    throw USER_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_DLA_ERROR16);
                }
                hasDimension = true;
                break;
            }
            default:
                throw USER_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_DLA_ERROR16);
            break;

            }
        }

        //  Return the output schema.
        return addEmptyTagAttribute(outputSchema);
    }
};

DECLARE_LOGICAL_OPERATOR_FACTORY( LogicalCumulate, "cumulate" )

} // namespace
