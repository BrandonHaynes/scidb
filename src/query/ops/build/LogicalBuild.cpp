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
 * LogicalBuild.cpp
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
 * @brief The operator: build().
 *
 * @par Synopsis:
 *   build( schemaArray | schema, expression, mustBeConstant = false )
 *
 * @par Summary:
 *   Produces a result array according to a given schema, and populates values based on the given expression. The schema must have a single attribute.
 *
 * @par Input:
 *   - schemaArray | schema: an array or a schema, from which attrs and dims will be used by the output array.
 *   - expression: the expression which is used to compute values for the output array.
 *   - mustBeConstant: whether the expression must be a constant.
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
 *   - Given array A <quantity: uint64> [year, item] =
 *     <br> year, item, quantity
 *     <br> 2011,  2,      7
 *     <br> 2011,  3,      6
 *     <br> 2012,  1,      5
 *     <br> 2012,  2,      9
 *     <br> 2012,  3,      8
 *   - build(A, 0) <quantity: uint64> [year, item] =
 *     <br> year, item, quantity
 *     <br> 2011,  1,      0
 *     <br> 2011,  2,      0
 *     <br> 2011,  3,      0
 *     <br> 2012,  1,      0
 *     <br> 2012,  2,      0
 *     <br> 2012,  3,      0
 *     Note that the cell (2011, 1), which was empty in the source array, is populated.
 *
 * @par Errors:
 *   - SCIDB_SE_INFER_SCHEMA::SCIDB_LE_OP_BUILD_ERROR2, if the source array has more than one attribute.
 *
 * @par Notes:
 *   - The build operator can only take as input bounded dimensions.
 *
 */
class LogicalBuild: public LogicalOperator
{
public:
    LogicalBuild(const string& logicalName, const std::string& alias):
        LogicalOperator(logicalName, alias)
    {
        ADD_PARAM_SCHEMA()
        ADD_PARAM_EXPRESSION(TID_VOID)
        ADD_PARAM_VARIES()
    }

    std::vector<boost::shared_ptr<OperatorParamPlaceholder> > nextVaryParamPlaceholder(const std::vector< ArrayDesc> &schemas)
    {
        std::vector<boost::shared_ptr<OperatorParamPlaceholder> > res;
        if (_parameters.size() == 3)
        {
            res.push_back(END_OF_VARIES_PARAMS());
        }
        else
        {
            res.push_back(END_OF_VARIES_PARAMS());
            res.push_back(PARAM_CONSTANT(TID_BOOL));
        }
        return res;
    }

    ArrayDesc inferSchema(std::vector<ArrayDesc> schemas, boost::shared_ptr<Query> query)
    {
        assert(schemas.size() == 0);
        assert(_parameters.size() == 2 || _parameters.size() == 3);
        bool asArrayLiteral = false;
        if (_parameters.size() == 3)
        {
            asArrayLiteral = evaluate(((boost::shared_ptr<OperatorParamLogicalExpression>&)_parameters[2])->getExpression(),
                query, TID_BOOL).getBool();
        }

        ArrayDesc desc = ((boost::shared_ptr<OperatorParamSchema>&)_parameters[0])->getSchema();
        if (!asArrayLiteral && desc.getAttributes(true).size() != 1)
            throw USER_QUERY_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_OP_BUILD_ERROR2,
                                       _parameters[0]->getParsingContext());

        if (desc.getName().size() == 0)
        {
            desc.setName("build");
        }

        // Check dimensions
        Dimensions const& dims = desc.getDimensions();
        for (size_t i = 0, n = dims.size();  i < n; i++)
        {
            if (dims[i].getLength() == INFINITE_LENGTH && !asArrayLiteral)
                throw USER_QUERY_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_OP_BUILD_ERROR3,
                                           _parameters[0]->getParsingContext());
        }

        if (asArrayLiteral)
        {
            bool good = true;
            //Check second argument type (must be string) and constness
            try
            {
                Expression e;
                e.compile(((boost::shared_ptr<OperatorParamLogicalExpression>&)_parameters[1])->getExpression(),
                    query, false, TID_STRING);
                good = e.isConstant();
            }
            catch(...)
            {
                good = false;
            }
            if (!good)
            {
                throw USER_QUERY_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_INVALID_ARRAY_LITERAL,
                    _parameters[1]->getParsingContext());
            }
        }

        return desc;
    }
};

DECLARE_LOGICAL_OPERATOR_FACTORY(LogicalBuild, "build")

} //namespace
