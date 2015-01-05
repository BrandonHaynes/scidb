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
 * LogicalVariableWindow.cpp
 *  Created on: Feb 9, 2012
 *      Author: poliocough@gmail.com
 */

#include "query/Operator.h"

namespace scidb
{

/**
 * @brief The operator: variable_window().
 *
 * @par Synopsis:
 *   variable_window( srcArray, dim, leftEdge, rightEdge {, AGGREGATE_CALL}+ )
 *   <br> AGGREGATE_CALL := AGGREGATE_FUNC(inputAttr) [as resultName]
 *   <br> AGGREGATE_FUNC := approxdc | avg | count | max | min | sum | stdev | var | some_use_defined_aggregate_function
 *
 * @par Summary:
 *   Produces a result array with the same dimensions as the source array, where each cell stores some aggregates calculated over
 *   a 1D window covering the current cell.
 *   The window has fixed number of non-empty elements. For instance, when rightEdge is 1, the window extends to the right-hand side however number of
 *   coordinatesthat are needed, to cover the next larger non-empty cell.
 *
 * @par Input:
 *   - srcArray: a source array with srcAttrs and srcDims.
 *   - dim: along which dimension is the window defined.
 *   - leftEdge: how many cells to the left of the current cell are included in the window.
 *   - rightEdge: how many cells to the right of the current cell are included in the window.
 *   - 1 or more aggregate calls.
 *     Each aggregate call has an AGGREGATE_FUNC, an inputAttr and a resultName.
 *     The default resultName is inputAttr followed by '_' and then AGGREGATE_FUNC.
 *
 * @par Output array:
 *        <
 *   <br>   the list of aggregate attribute names. Each is source attribute name followed by "_" then the aggregate function name.
 *   <br> >
 *   <br> [
 *   <br>   srcDims
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
 *   - variable_window(A, item, 1, 0, sum(quantity)) <quantity_sum: uint64> [year, item] =
 *     <br> year, item, quantity_sum
 *     <br> 2011,  2,      7
 *     <br> 2011,  3,      13
 *     <br> 2012,  1,      5
 *     <br> 2012,  2,      14
 *     <br> 2012,  3,      17
 *
 * @par Errors:
 *   n/a
 *
 * @par Notes:
 *   - For a dense array, this is a special case of window().
 *   - For the aggregate function approxdc(), the attribute name is currently non-conventional. It is xxx_ApproxDC instead of xxx_approxdc. Should change.
 *
 */
class LogicalVariableWindow : public LogicalOperator
{
public:
    LogicalVariableWindow(const std::string& logicalName, const std::string& alias):
        LogicalOperator(logicalName, alias)
    {
        ADD_PARAM_INPUT();
        ADD_PARAM_IN_DIMENSION_NAME();
        ADD_PARAM_CONSTANT("int64");
        ADD_PARAM_CONSTANT("int64");
        ADD_PARAM_AGGREGATE_CALL();
        ADD_PARAM_VARIES();
    }

    std::vector<boost::shared_ptr<OperatorParamPlaceholder> > nextVaryParamPlaceholder(const std::vector< ArrayDesc> &schemas)
    {
        std::vector<boost::shared_ptr<OperatorParamPlaceholder> > res;
        res.push_back(END_OF_VARIES_PARAMS());
        res.push_back(PARAM_AGGREGATE_CALL());
        return res;
    }

    ArrayDesc inferSchema(std::vector<ArrayDesc> schemas, boost::shared_ptr<Query> query)
    {
        assert(schemas.size() == 1);

        int64_t wstart = evaluate(((boost::shared_ptr<OperatorParamLogicalExpression>&)_parameters[1])->getExpression(), query, TID_INT64).getInt64();
        int64_t wend = evaluate(((boost::shared_ptr<OperatorParamLogicalExpression>&)_parameters[2])->getExpression(), query, TID_INT64).getInt64();

        if (wstart<0)
        {
            throw USER_QUERY_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_OP_WINDOW_ERROR3, _parameters[1]->getParsingContext());
        }

        if (wend<0)
        {
            throw USER_QUERY_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_OP_WINDOW_ERROR3, _parameters[2]->getParsingContext());
        }

        if (wend + wstart + 1 <= 1)
        {
            throw USER_QUERY_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_OP_WINDOW_ERROR4,
                _parameters[1]->getParsingContext());
        }

        Dimensions const& dims = schemas[0].getDimensions();
        Dimensions outDims(dims.size());
        for (size_t i = 0, n = dims.size(); i < n; i++)
        {
            DimensionDesc const& srcDim = dims[i];
            outDims[i] = DimensionDesc(srcDim.getBaseName(),
                                       srcDim.getNamesAndAliases(),
                                       srcDim.getStartMin(),
                                       srcDim.getCurrStart(),
                                       srcDim.getCurrEnd(),
                                       srcDim.getEndMax(),
                                       srcDim.getChunkInterval(),
                                       0);
        }

        ArrayDesc output (schemas[0].getName(), Attributes(), outDims);
        for(size_t i =3; i<_parameters.size(); i++)
        {
            bool isInOrderAggregation = true;
            addAggregatedAttribute( (shared_ptr <OperatorParamAggregateCall> &) _parameters[i], schemas[0], output,
                    isInOrderAggregation);
        }

        if ( schemas[0].getEmptyBitmapAttribute())
        {
            AttributeDesc const* eAtt = schemas[0].getEmptyBitmapAttribute();
            output.addAttribute(AttributeDesc( output.getAttributes().size(), eAtt->getName(), eAtt->getType(), eAtt->getFlags(), eAtt->getDefaultCompressionMethod()));
        }
        else
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "variable_window only supports emptyable arrays. Use regular window() instead.";
        }

        return output;
    }

};

DECLARE_LOGICAL_OPERATOR_FACTORY(LogicalVariableWindow, "variable_window")

} //namespace
