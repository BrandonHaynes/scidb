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
 * LogicalRank.cpp
 *  Created on: Mar 11, 2011
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
 * @brief The operator: rank().
 *
 * @par Synopsis:
 *   rank( srcArray [, attr {, groupbyDim}*] )
 *
 * @par Summary:
 *   Computes the rankings of an array, based on the ordering of attr (within each group as specified by the list of groupbyDims, if provided).
 *   If groupbyDims is not specified, global ordering will be performed.
 *   If attr is not specified, the first attribute will be used.
 *
 * @par Input:
 *   - srcArray: the source array with srcAttrs and srcDims.
 *   - attr: which attribute to sort on. The default is the first attribute.
 *   - groupbyDim: if provided, the ordering will be performed among the records in the same group.
 *
 * @par Output array:
 *        <
 *   <br>   attr: only the specified attribute in srcAttrs is retained.
 *   <br>   attr_rank: the source attribute name followed by '_rank'.
 *   <br> >
 *   <br> [
 *   <br>   srcDims: the shape does not change.
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
class LogicalRank: public LogicalOperator
{
public:
    LogicalRank(const std::string& logicalName, const std::string& alias):
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
                if (j >= dims.size()) {
                    throw USER_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_DLA_ERROR16);
                }
            }
        }

        return getRankingSchema(input, inputAttributeID);
    }
};


DECLARE_LOGICAL_OPERATOR_FACTORY(LogicalRank, "rank")

}
