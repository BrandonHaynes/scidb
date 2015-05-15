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
 * LogicalAggregate.cpp
 *
 *  Created on: Jul 7, 2011
 *      Author: poliocough@gmail.com
 */

#include "query/Operator.h"
#include "system/Exceptions.h"
#include "query/LogicalExpression.h"
#include "query/TypeSystem.h"
#include "query/Aggregate.h"
#include "util/arena/Set.h"

namespace scidb {

using namespace std;
using namespace boost;

/**
 * @brief The operator: aggregate().
 *
 * @par Synopsis:
 *   aggregate( srcArray {, AGGREGATE_CALL}+ {, groupbyDim}* {, chunkSize}* )
 *   <br> AGGREGATE_CALL := AGGREGATE_FUNC(inputAttr) [as resultName]
 *   <br> AGGREGATE_FUNC := approxdc | avg | count | max | min | sum | stdev | var | some_use_defined_aggregate_function
 *
 * @par Summary:
 *   Calculates aggregates over groups of values in an array, given the aggregate types and attributes to aggregate on. <br>
 *
 * @par Input:
 *   - srcArray: a source array with srcAttrs and srcDims.
 *   - 1 or more aggregate calls.
 *     Each aggregate call has an AGGREGATE_FUNC, an inputAttr and a resultName.
 *     The default resultName is inputAttr followed by '_' and then AGGREGATE_FUNC.
 *     For instance, the default resultName for sum(sales) is 'sales_sum'.
 *     The count aggregate may take * as the input attribute, meaning to count all the items in the group including null items.
 *     The default resultName for count(*) is 'count'.
 *   - 0 or more dimensions that together determines the grouping criteria.
 *   - 0 or numGroupbyDims chunk sizes.
 *     If no chunk size is given, the groupby dims will inherit chunk sizes from the input array.
 *     If at least one chunk size is given, the number of chunk sizes must be equal to the number of groupby dimensions,
 *     and the groupby dimensions will use the specified chunk sizes.
 *
 * @par Output array:
 *        <
 *   <br>   The aggregate calls' resultNames.
 *   <br> >
 *   <br> [
 *   <br>   The list of groupbyDims if provided (with the specified chunk sizes if provided),
 *   <br>   or
 *   <br>   'i' if no groupbyDim is provided.
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
 *   - aggregate(A, count(*), max(quantity), sum(sales), year) <count: uint64, quantity_max: uint64, sales_sum: double> [year] =
 *     <br> year, count, quantity_max, sales_sum
 *     <br> 2011,   2,      7,           51.62
 *     <br> 2012,   3,      9,          108.97
 *
 * @par Errors:
 *   n/a
 *
 * @par Notes:
 *   - All the aggregate functions ignore null values, except count(*).
 *
 */
class LogicalAggregate: public  LogicalOperator
{
public:
    LogicalAggregate(const std::string& logicalName, const std::string& alias):
        LogicalOperator(logicalName, alias)
    {
        _properties.tile = true;
        ADD_PARAM_INPUT()
        ADD_PARAM_VARIES()
    }

    std::vector<boost::shared_ptr<OperatorParamPlaceholder> >
    nextVaryParamPlaceholder(const std::vector< ArrayDesc> &schemas)
    {
        std::vector<boost::shared_ptr<OperatorParamPlaceholder> > res;

        // All parameters are optional.
        res.push_back(END_OF_VARIES_PARAMS());

        if (_parameters.size() == 0)
        {
            // The first parameter must be an aggregate call.
            res.push_back(PARAM_AGGREGATE_CALL());
        }
        else
        {
            boost::shared_ptr<OperatorParam> lastParam = _parameters[_parameters.size() - 1];
            if (lastParam->getParamType() == PARAM_AGGREGATE_CALL)
            {
                // If the previous parameter was an aggregate call, this one can be another aggregate call or a dim name.
                // Note that this one cannot be a chunk size, because that would mean providing a chunk size without any dim name.
                res.push_back(PARAM_AGGREGATE_CALL());
                res.push_back(PARAM_IN_DIMENSION_NAME());
            }
            else if (lastParam->getParamType() == PARAM_DIMENSION_REF)
            {
                // If the previous parameter was a dim name, this one can be either another dim name or a chunk size.
                // A note on the type of chunk size: even though a chunk size should have TID_UINT64, we use TID_INT64 here.
                // The purpose is that, if the user provides a negative number, we catch it and error out;
                // while a TID_UINT64 will silently accept a negative number and populates the chunk size field with it.
                res.push_back(PARAM_IN_DIMENSION_NAME());
                res.push_back(PARAM_CONSTANT(TID_INT64));
            }
            else // chunk size
            {
                // Once we reach the section of chunk sizes, we can only provide more chunk sizes.
                res.push_back(PARAM_CONSTANT(TID_INT64));
            }
        }

        return res;
    }

    /**
     * @param inputDims  the input dimensions.
     * @param outDims    the output dimensions.
     * @param param      the OperatorParam object.
     * @param chunkSize  the chunkSize for the dimension; -1 means to use the input dimension size.
     */
    void addDimension(Dimensions const& inputDims,
                      Dimensions& outDims,
                      shared_ptr<OperatorParam> const& param,
                      size_t chunkSize)
    {
        boost::shared_ptr<OperatorParamReference> const& reference =
            (boost::shared_ptr<OperatorParamReference> const&) param;

        string const& dimName = reference->getObjectName();
        string const& dimAlias = reference->getArrayName();

        for (size_t j = 0, n = inputDims.size(); j < n; j++)
        {
            if (inputDims[j].hasNameAndAlias(dimName, dimAlias))
            {
                //no overlap
                outDims.push_back(DimensionDesc( inputDims[j].getBaseName(),
                                                 inputDims[j].getNamesAndAliases(),
                                                 inputDims[j].getStartMin(),
                                                 inputDims[j].getCurrStart(),
                                                 inputDims[j].getCurrEnd(),
                                                 inputDims[j].getEndMax(),
                                                 chunkSize==static_cast<size_t>(-1) ? inputDims[j].getChunkInterval() : chunkSize,
                                                 0));
                return;
            }
        }
        throw SYSTEM_EXCEPTION(SCIDB_SE_QPROC, SCIDB_LE_DIMENSION_NOT_EXIST)
            << dimName << "aggregate input" << inputDims;
    }

    ArrayDesc inferSchema(vector< ArrayDesc> schemas, boost::shared_ptr< Query> query)
    {
        assert(schemas.size() == 1);
        ArrayDesc const& input = schemas[0];

        Dimensions const& inputDims = input.getDimensions();

        Attributes outAttrs;
        Dimensions outDims;

        if (_parameters.empty())
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_SYNTAX, SCIDB_LE_WRONG_OPERATOR_ARGUMENTS_COUNT2) << "aggregate";
        }

        removeDuplicateDimensions();

        // How many parameters are of each type.
        size_t numAggregateCalls = 0;
        size_t numGroupbyDims = 0;
        size_t numChunkSizes = 0;
        for (size_t i = 0, n = _parameters.size(); i < n; ++i)
        {
            if (_parameters[i]->getParamType() == PARAM_AGGREGATE_CALL)
            {
                ++numAggregateCalls;
            }
            else if (_parameters[i]->getParamType() == PARAM_DIMENSION_REF)
            {
                ++numGroupbyDims;
            }
            else // chunk size
            {
                ++numChunkSizes;
            }
        }
        if (numChunkSizes && numChunkSizes != numGroupbyDims) {
            throw USER_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_NUM_CHUNKSIZES_NOT_MATCH_NUM_DIMS) << "aggregate()";
        }

        for (size_t i = 0, n = _parameters.size(); i < n; ++i)
        {
            if (_parameters[i]->getParamType() == PARAM_DIMENSION_REF)
            {
                int64_t chunkSize = -1;
                if (numChunkSizes) {
                    // E.g. here are the parameters:
                    //       0           1      2        3            4
                    // AGGREGATE_CALL,  dim1,  dim2,  chunkSize1,  chunkSize2
                    // i=1 ==> index = 3
                    // i=2 ==> index = 4
                    size_t index = i + numGroupbyDims;
                    assert(index < _parameters.size());
                    chunkSize = evaluate(((boost::shared_ptr<OperatorParamLogicalExpression>&)_parameters[index])->getExpression(),
                            query, TID_INT64).getInt64();
                    if (chunkSize<=0) {
                        throw USER_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_CHUNK_SIZE_MUST_BE_POSITIVE);
                    }
                }
                addDimension(inputDims, outDims, _parameters[i], static_cast<size_t>(chunkSize));
            }
        }

        bool grand = true;
        if (outDims.size() == 0)
        {
            outDims.push_back(DimensionDesc("i", 0, 0, 0, 0, 1, 0));
        }
        else
        {
            _properties.tile = false;
            grand = false;
        }

        ArrayDesc outSchema(input.getName(), Attributes(), outDims);
        for (size_t i =0, n = _parameters.size(); i<n; i++)
        {
            if (_parameters[i]->getParamType() == PARAM_AGGREGATE_CALL)
            {
                bool isInOrderAggregation = false;
                addAggregatedAttribute( (shared_ptr <OperatorParamAggregateCall> &) _parameters[i], input,
                                        outSchema, isInOrderAggregation);
            }
        }

        if(!grand)
        {
            AttributeDesc et ((AttributeID) outSchema.getAttributes().size(),
                              DEFAULT_EMPTY_TAG_ATTRIBUTE_NAME,
                              TID_INDICATOR,
                              AttributeDesc::IS_EMPTY_INDICATOR,
                              0);
            outSchema.addAttribute(et);
        }
        return outSchema;
    }

private:

    /**
     * Remove duplicate dimension references from the parameter list.
     */
    void removeDuplicateDimensions()
    {
        typedef std::pair<std::string, std::string> NameAndAlias;
        typedef mgd::set<NameAndAlias> DimNameSet;

        DimNameSet seen;
        LogicalOperator::Parameters tmp;
        tmp.reserve(_parameters.size());

        for (size_t i = 0; i < _parameters.size(); ++i) {
            if (_parameters[i]->getParamType() == PARAM_DIMENSION_REF) {
                const OperatorParamDimensionReference* dimRef =
                    safe_dynamic_cast<OperatorParamDimensionReference*>(_parameters[i].get());
                NameAndAlias key(dimRef->getObjectName(), dimRef->getArrayName());
                DimNameSet::iterator pos = seen.find(key);
                if (pos == seen.end()) {
                    tmp.push_back(_parameters[i]);
                    seen.insert(key);
                }
                // ...else a duplicate, ignore it.
            } else {
                tmp.push_back(_parameters[i]);
            }
        }

        _parameters.swap(tmp);
    }
};

DECLARE_LOGICAL_OPERATOR_FACTORY(LogicalAggregate, "aggregate")

}  // namespace scidb
