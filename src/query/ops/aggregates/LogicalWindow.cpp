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
 * LogicalWindow.cpp
 *
 *  Created on: Apr 11, 2010
 *      Author: Knizhnik, poliocough@gmail.com, 
 *              Paul Brown <paulgeoffreybrown@gmail.com> 
 */

#include <boost/shared_ptr.hpp>
#include <log4cxx/logger.h>

#include "query/Operator.h"
#include "system/Exceptions.h"
#include "query/LogicalExpression.h"
#include "WindowArray.h"

namespace scidb
{

using namespace std;

/**
 * @brief The operator: window().
 *
 * @par Synopsis:
 *   window( srcArray {, leftEdge, rightEdge}+ {, AGGREGATE_CALL}+ [, METHOD ] )
 *   <br> AGGREGATE_CALL := AGGREGATE_FUNC(inputAttr) [as resultName]
 *   <br> AGGREGATE_FUNC := approxdc | avg | count | max | min | sum | stdev | var | some_use_defined_aggregate_function
 *   <br> METHOD := 'materialize' | 'probe'
 *
 * @par Summary:
 *   Produces a result array with the same size and dimensions as the source 
 *   array, where each ouput cell stores some aggregate calculated over a 
 *   window around the corresponding cell in the source array. A pair of 
 *   window specification values (leftEdge, rightEdge) must exist for every 
 *   dimension in the source and output array. 
 *
 * @par Input:
 *   - srcArray: a source array with srcAttrs and srcDims.
 *   - leftEdge: how many cells to the left of the current cell (in one dimension) are included in the window.
 *   - rightEdge: how many cells to the right of the current cell (in one dimension) are included in the window.
 *   - 1 or more aggregate calls.
 *     Each aggregate call has an AGGREGATE_FUNC, an inputAttr and a resultName.
 *     The default resultName is inputAttr followed by '_' and then AGGREGATE_FUNC.
 *     For instance, the default resultName for sum(sales) is 'sales_sum'.
 *     The count aggregate may take * as the input attribute, meaning to count all the items in the group including null items.
 *     The default resultName for count(*) is 'count'.
 *   - An optional final argument that specifies how the operator is to perform
 *     its calculation. At the moment, we support two internal algorithms: 
 *     "materialize" (which materializes an entire source chunk before 
 *     computing the output windows) and "probe" (which probes the source 
 *     array for the data in each window). In general, materializing the input
 *     is a more efficient strategy, but when we're using thin(...) in 
 *     conjunction with window(...), we're often better off using probes, 
 *     rather than materilization. This is a decision that the optimizer needs 
 *     to make. 
 *
 * @par Output array:
 *        <
 *   <br>   the aggregate calls' resultNames
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
 *   - window(A, 0, 0, 1, 0, sum(quantity)) <quantity_sum: uint64> [year, item] =
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
 *   n/a
 *
 */
class LogicalWindow: public LogicalOperator
{
public:
   
    LogicalWindow(const std::string& logicalName, const std::string& alias):
            LogicalOperator(logicalName, alias)
    {
        //
        //  Input to the operator consists of an input array, followed 
        // by a variable list of paramaters. 
        ADD_PARAM_INPUT();
        ADD_PARAM_VARIES();
    }

    /**
     * @see LogicalOperator::nextVaryParamPlaceholder()
     */
    std::vector<boost::shared_ptr<OperatorParamPlaceholder> > nextVaryParamPlaceholder(const std::vector<ArrayDesc> &schemas)
    {
        //
        //  The arguments to the window(...) operator are: 
        //     window( srcArray {, leftEdge, rightEdge}+ {, AGGREGATE_CALL}+ [, METHOD ] )
        // 
        //   * There must be as many {, leftEdge, rightEdge}+ pairs as there are dimensions 
        //     in srcArray. 
        //   * There must be at least one aggregate.
        //   * The (optional) [, METHOD] is a string. 
        // 
        std::vector<boost::shared_ptr<OperatorParamPlaceholder> > res;

        if (_parameters.size() < schemas[0].getDimensions().size() * 2) 
        {
            res.push_back(PARAM_CONSTANT("int64"));
        } else if (_parameters.size() == schemas[0].getDimensions().size() * 2) 
        {
            res.push_back(PARAM_AGGREGATE_CALL());
        } else 
        {
            //  In this part we expect either an optional aggregate 
            // or an optional string AFTER aggregate. If the last one 
            // was an aggregate, the next one might be an aggregate, 
            // or a string. But otherwise (it was a string) then all 
            // you can have is the END_OF_VARIES.
            if (_parameters[_parameters.size() - 1]->getParamType() ==
PARAM_AGGREGATE_CALL)
            {
                res.push_back(PARAM_AGGREGATE_CALL());
                res.push_back(PARAM_CONSTANT(TID_STRING));
            }
            res.push_back(END_OF_VARIES_PARAMS());
        }
        return res;
    }

    /**
     *  Construct the description of the output array based on the input 
     *
     *   The output array of the window(...) operator is the same size and 
     *  shape as the input, and has a set of attributes the same size and 
     *  type as the aggregates. 
     *
     *  @param ArrayDesc& desc - ArrayDesc of the input array
     *
     *  @return ArrayDesc - ArrayDesc of output array from window(...) op
     * 
     */
    inline ArrayDesc createWindowDesc(ArrayDesc const& desc)
    {
        Dimensions const& dims = desc.getDimensions();
        Dimensions aggDims(dims.size());
        for (size_t i = 0, n = dims.size(); i < n; i++)
        {
            DimensionDesc const& srcDim = dims[i];
            aggDims[i] = DimensionDesc(srcDim.getBaseName(),
                                       srcDim.getNamesAndAliases(),
                                       srcDim.getStartMin(),
                                       srcDim.getCurrStart(),
                                       srcDim.getCurrEnd(),
                                       srcDim.getEndMax(),
                                       srcDim.getChunkInterval(), 
                                       0);
        }

        ArrayDesc output (desc.getName(), Attributes(), aggDims);

        //
        //  Process the variadic parameters to the operator. Check 
        // that the aggregates make sense, and check for the presence 
        // of the optional variable argument that tells the operator 
        // which algorithm to use. 
        for (size_t i = dims.size() * 2, size = _parameters.size(); i < size; i++)
        {
            boost::shared_ptr<scidb::OperatorParam> param = _parameters[i];

            switch ( param->getParamType() ) { 
               case PARAM_AGGREGATE_CALL:
               {
                   bool isInOrderAggregation = true;
                   addAggregatedAttribute( (shared_ptr <OperatorParamAggregateCall> &) param, desc, output,
                           isInOrderAggregation);
                   break;
               }
               case PARAM_LOGICAL_EXPRESSION:
               { 
                   //
                   //  If there is a Logical Expression at this point, 
                   // it needs to be a constant string, and the string 
                   // needs to be one of the two legitimate algorithm names. 
                   const boost::shared_ptr<OperatorParamLogicalExpression> paramLogicalExpression = boost::static_pointer_cast<OperatorParamLogicalExpression>(param);
                   if ((paramLogicalExpression->isConstant()) && 
                       (TID_STRING == paramLogicalExpression->getExpectedType
().typeId())) 
                   { 
                       string s(boost::static_pointer_cast<Constant>(paramLogicalExpression->getExpression())->getValue().getString());

                       if (!((s == WindowArray::PROBE) || (s == WindowArray::MATERIALIZE ))) 
                       {
                           stringstream ss; 
                           ss << WindowArray::PROBE << " or " << WindowArray::MATERIALIZE;
                           throw USER_QUERY_EXCEPTION(SCIDB_SE_INFER_SCHEMA, 
                                                      SCIDB_LE_OP_WINDOW_ERROR5,
                                                      _parameters[i]->getParsingContext()) 
                           << ss.str();
                       } 
                   }
               }
                break;
               default: 
                 throw USER_QUERY_EXCEPTION(SCIDB_SE_INFER_SCHEMA, 
                                            SCIDB_LE_OP_WINDOW_ERROR5,
                                            _parameters[i]->getParsingContext());
            }
        }

        if ( desc.getEmptyBitmapAttribute())
        {
            AttributeDesc const* eAtt = desc.getEmptyBitmapAttribute();
            output.addAttribute(AttributeDesc(output.getAttributes().size(), eAtt->getName(),
                eAtt->getType(), eAtt->getFlags(), eAtt->getDefaultCompressionMethod()));
        }

        return output;
    }

    /**
     *  @see LogicalOperator::inferSchema()
     */
    ArrayDesc inferSchema(std::vector<ArrayDesc> schemas, boost::shared_ptr<Query> query)
    {
        SCIDB_ASSERT(schemas.size() == 1);

        ArrayDesc const& desc = schemas[0];
        size_t nDims = desc.getDimensions().size();
        vector<WindowBoundaries> window(nDims);
        size_t windowSize = 1;
        for (size_t i = 0, size = nDims * 2, boundaryNo = 0; i < size; i+=2, ++boundaryNo)
        {
            int64_t boundaryLower = 
                    evaluate(((boost::shared_ptr<OperatorParamLogicalExpression>&)_parameters[i])->getExpression(), query, TID_INT64).getInt64();

            if (boundaryLower < 0)
                throw USER_QUERY_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_OP_WINDOW_ERROR3,
                    _parameters[i]->getParsingContext());
;
            int64_t boundaryUpper = 
                    evaluate(((boost::shared_ptr<OperatorParamLogicalExpression>&)_parameters[i+1])->getExpression(), query, TID_INT64).getInt64();

            if (boundaryUpper < 0)
                throw USER_QUERY_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_OP_WINDOW_ERROR3,
                    _parameters[i]->getParsingContext());
;
            window[boundaryNo] = WindowBoundaries(boundaryLower,boundaryUpper);
            windowSize *= window[boundaryNo]._boundaries.second + window[boundaryNo]._boundaries.first + 1;

        }
        if (windowSize <= 1)
            throw USER_QUERY_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_OP_WINDOW_ERROR4,
                _parameters[0]->getParsingContext());
        return createWindowDesc(desc);
    }

private: 
    static const std::string PROBE;
    static const std::string MATERIALIZE;

};

DECLARE_LOGICAL_OPERATOR_FACTORY(LogicalWindow, "window")

}  // namespace scidb
