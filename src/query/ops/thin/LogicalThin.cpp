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
 * LogicalThin.cpp
 *
 *  Created on: Jul 19, 2010
 *      Author: Knizhnik
 */

#include <query/Operator.h>
#include <system/Exceptions.h>
#include "ThinArray.h"

namespace scidb {

using namespace std;

/***
 * Helper function to generate descriptor of thin array
 ***/
inline ArrayDesc createThinDesc(ArrayDesc const& desc, Coordinates const& from, Coordinates const& step, boost::shared_ptr<Query> const& query)
{
    Dimensions const& dims = desc.getDimensions();
    Dimensions newDims(dims.size());
    for (size_t i = 0, n = dims.size(); i < n; i++) {
        DimensionDesc const& srcDim = dims[i];
        Coordinate last = 
            computeLastCoordinate(srcDim.getCurrLength(),
                                  srcDim.getStartMin(),
                                  from[i], step[i]);
        newDims[i] = DimensionDesc(srcDim.getBaseName(),
                                   srcDim.getNamesAndAliases(),
                                   0,
                                   0,
                                   last,
                                   last,
                                   srcDim.getChunkInterval()/step[i], 0);
    }
    return ArrayDesc(desc.getName(), desc.getAttributes(), newDims);
}


/**
 * @brief The operator: thin().
 *
 * @par Synopsis:
 *   thin( srcArray {, start, step}+ )
 *
 * @par Summary:
 *   Selects regularly-spaced elements of the source array in each dimension.
 *   A (start, step) pair must be provided for every dimension.
 *
 * @par Input:
 *   - srcArray: a source array with srcAttrs and srcDims.
 *   - start: the starting coordinate of a dimension.
 *   - step: how many coordinates to advance to the next coordinate to select. A step of 1 means to select everything.
 *
 * @par Output array:
 *        <
 *   <br>   srcAttrs.
 *   <br> >
 *   <br> [
 *   <br>   srcDims where every dimension's start is changed to 0.
 *   <br> ]
 *
 * @par Examples:
 *   n/a
 *
 * @par Errors:
 *   - SCIDB_SE_SYNTAX::SCIDB_LE_WRONG_OPERATOR_ARGUMENTS_COUNT2: if not all dimensions have a pair of (start, step).
 *   - SCIDB_SE_INFER_SCHEMA::SCIDB_LE_OP_THIN_ERROR1: if a step is not a divier of chunk size.
 *
 * @par Notes:
 *   n/a
 *
 */
class LogicalThin: public  LogicalOperator
{
  public:
    LogicalThin(const std::string& logicalName, const std::string& alias):
        LogicalOperator(logicalName, alias)
        {
        ADD_PARAM_INPUT()
        ADD_PARAM_VARIES()
        }

    std::vector<boost::shared_ptr<OperatorParamPlaceholder> > nextVaryParamPlaceholder(
            const std::vector<ArrayDesc> &schemas) {
        std::vector<boost::shared_ptr<OperatorParamPlaceholder> > res;
        if (_parameters.size() == schemas[0].getDimensions().size() * 2)
            res.push_back(END_OF_VARIES_PARAMS());
        else
            res.push_back(PARAM_CONSTANT("int64"));
        return res;
    }

    ArrayDesc inferSchema(std::vector< ArrayDesc> schemas, boost::shared_ptr< Query> query)
    {
        ArrayDesc const& desc = schemas[0];
        Dimensions const& dims = desc.getDimensions();
        size_t nDims = dims.size();

        assert(schemas.size() == 1);
        assert(_parameters.size() == nDims*2);

        Coordinates from(nDims);
        Coordinates step(nDims);
        for (size_t i = 0; i < nDims; i++) {
            from[i] = evaluate(((boost::shared_ptr<OperatorParamLogicalExpression>&)_parameters[i*2])->getExpression(), query, TID_INT64).getInt64();
            step[i] = evaluate(((boost::shared_ptr<OperatorParamLogicalExpression>&)_parameters[i*2+1])->getExpression(), query, TID_INT64).getInt64();
            if (step[i] <= 0)
                throw USER_QUERY_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_OP_THIN_ERROR5,
                                          _parameters[i*2+1]->getParsingContext());
            if (dims[i].getChunkInterval() % step[i] != 0)
                throw USER_QUERY_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_OP_THIN_ERROR1,
                                          _parameters[i*2+1]->getParsingContext());

            if (from[i] < dims[i].getStartMin())
                throw USER_QUERY_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_OP_THIN_ERROR2,
                                          _parameters[i*2]->getParsingContext());

            if (from[i] - dims[i].getStartMin() >= step[i])
                throw USER_QUERY_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_OP_THIN_ERROR3,
                                          _parameters[i*2]->getParsingContext());

            if (dims[i].getChunkInterval() < step[i])
                throw USER_QUERY_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_OP_THIN_ERROR4,
                                          _parameters[i*2+1]->getParsingContext());
        }
        return createThinDesc(desc, from, step, query);
    }
};

DECLARE_LOGICAL_OPERATOR_FACTORY(LogicalThin, "thin")


}  // namespace scidb
