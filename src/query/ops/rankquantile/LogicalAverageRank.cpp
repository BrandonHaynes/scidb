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
 * LogicalAverageRank.cpp
 *  Created on: May 11, 2011
 *      Author: poliocough@gmail.com
 */

#include <boost/shared_ptr.hpp>
#include <boost/foreach.hpp>

#include <query/Operator.h>
#include <system/Exceptions.h>
#include <query/LogicalExpression.h>
#include "RankCommon.h"

using namespace std;

namespace scidb
{

/**
 * @brief The operator: avg_rank().
 *
 * @par Synopsis:
 *   avg_rank( srcArray [, attr {, groupbyDim}*] )
 *
 * @par Summary:
 *   Ranks the array elements, where each element is ranked as the average of the upper bound (UB) and lower bound (LB) rankings.
 *   The LB ranking of an element E is the number of elements less than E, plus 1.
 *   The UB ranking of an element E is the number of elements less than or equal to E, plus 1.
 *
 * @par Input:
 *   - srcArray: a source array with srcAttrs and srcDims.
 *   - 0 or 1 attribute to rank with. If no attribute is provided, the first attribute is used.
 *   - an optional list of groupbyDims used to group the elements, such that the rankings are calculated within each group.
 *     If no groupbyDim is provided, the whole array is treated as one group.
 *
 * @par Output array:
 *        <
 *   <br>   attr: the source attribute to rank with.
 *   <br>   attr_rank: the source attribute name, followed by '_rank'.
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
 *   - avg_rank(A, sales, year) <sales:double, sales_rank: uint64> [year, item] =
 *     <br> year, item, sales, sales_rank
 *     <br> 2011,  2,   31.64,    2
 *     <br> 2011,  3,   19.98,    1
 *     <br> 2012,  1,   41.65,    3
 *     <br> 2012,  2,   40.68,    2
 *     <br> 2012,  3,   26.64,    1
 *
 * @par Errors:
 *   n/a
 *
 * @par Notes:
 *   - For any element with a distinct value, its UB ranking and LB ranking are equal.
 *
 */
class LogicalAverageRank: public LogicalOperator
{
public:
    LogicalAverageRank(const std::string& logicalName, const std::string& alias):
        LogicalOperator(logicalName, alias)
    {
        ADD_PARAM_INPUT()
        ADD_PARAM_VARIES()
    }

    std::vector<boost::shared_ptr<OperatorParamPlaceholder> > nextVaryParamPlaceholder(const std::vector<ArrayDesc> &schemas)
    {
        std::vector<boost::shared_ptr<OperatorParamPlaceholder> > res;
        res.push_back(END_OF_VARIES_PARAMS());
        if (_parameters.size() == 0)
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

        string attName = _parameters.size() > 0 ? ((boost::shared_ptr<OperatorParamReference>&)_parameters[0])->getObjectName() :
                                                    input.getAttributes()[0].getName();

        AttributeID inputAttributeID = 0;
        bool found = false;
        BOOST_FOREACH(const AttributeDesc& att, input.getAttributes())
        {
            if (att.getName() == attName)
            {
                found = true;
                inputAttributeID = att.getId();
            }
        }

        if (!found) {
            throw USER_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_DLA_ERROR14);
        }

        AttributeDesc rankedAttribute = input.getAttributes()[inputAttributeID];
        if (rankedAttribute.isEmptyIndicator()) {
            throw USER_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_DLA_ERROR15);
        }

        Dimensions dims = input.getDimensions();
        if (_parameters.size()>1)
        {
            vector<int> groupBy(_parameters.size()-1);
            size_t i, j;
            for (i = 0; i < _parameters.size() - 1; i++)
            {
                const string& dimName = ((boost::shared_ptr<OperatorParamReference>&)_parameters[i + 1])->getObjectName();
                const string& dimAlias = ((boost::shared_ptr<OperatorParamReference>&)_parameters[i + 1])->getArrayName();
                for (j = 0; j < dims.size(); j++)
                {
                    if (dims[j].hasNameAndAlias(dimName, dimAlias))
                    {
                        break;
                    }
                }

                if (j >= dims.size())
                {
                    throw USER_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_DIMENSION_NOT_EXIST)
                        << dimName << "input" << dims;
                }
            }
        }

        return getRankingSchema(input, inputAttributeID);
    }
};


DECLARE_LOGICAL_OPERATOR_FACTORY(LogicalAverageRank, "avg_rank")

}
