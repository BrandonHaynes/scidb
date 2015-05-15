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
 * LogicalMatch.cpp
 *
 *  Created on: Apr 04, 2012
 *      Author: Knizhnik
 */

#include <query/Operator.h>
#include <system/SystemCatalog.h>
#include <system/Exceptions.h>
#include <array/Metadata.h>

using namespace std;

namespace scidb
{

class LogicalMatch: public LogicalOperator
{
  public:
    LogicalMatch(const string& logicalName, const std::string& alias):
        LogicalOperator(logicalName, alias)
    {
    	ADD_PARAM_INPUT()
    	ADD_PARAM_INPUT()
        ADD_PARAM_CONSTANT("int64");
        ADD_PARAM_CONSTANT("int64");
    }

    ArrayDesc inferSchema(std::vector< ArrayDesc> schemas, boost::shared_ptr< Query> query)
    {
        assert(schemas.size() == 2);

        ArrayDesc  const& patternDesc = schemas[0];
        ArrayDesc  const& catalogDesc = schemas[1];
        Attributes const& catalogAttributes = catalogDesc.getAttributes(true);
        Dimensions const& catalogDimensions = catalogDesc.getDimensions();
        Attributes const& patternAttributes = patternDesc.getAttributes(true);
        Dimensions resultDimensions = patternDesc.getDimensions();
        size_t totalAttributes = catalogAttributes.size() + patternAttributes.size() + 1 + catalogDimensions.size();
        Attributes matchAttributes(totalAttributes);

        if (catalogDimensions.size() != resultDimensions.size())
        {
            stringstream left, right;
            printDimNames(left, resultDimensions);
            printDimNames(right, catalogDimensions);
            throw USER_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_DIMENSION_COUNT_MISMATCH)
                << "match" << left.str() << right.str();
        }
        for (size_t i = 0, n = catalogDimensions.size(); i < n; i++) {
            if (!(catalogDimensions[i].getStartMin() == resultDimensions[i].getStartMin()
                  && catalogDimensions[i].getChunkInterval() == resultDimensions[i].getChunkInterval()
                  && catalogDimensions[i].getChunkOverlap() == resultDimensions[i].getChunkOverlap()))
            {
                // XXX To do: implement requiresRepart() method, remove interval/overlap checks
                // above, use SCIDB_LE_START_INDEX_MISMATCH here.
                throw USER_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_ARRAYS_NOT_CONFORMANT);
            }
        }

        size_t j = 0;
        for (size_t i = 0, n = patternAttributes.size(); i < n; i++, j++) {
            AttributeDesc const& attr = patternAttributes[i];
            matchAttributes[j] = AttributeDesc(j, attr.getName(), attr.getType(), attr.getFlags(),
                                               attr.getDefaultCompressionMethod(), attr.getAliases(), &attr.getDefaultValue(),
                                               attr.getDefaultValueExpr());
        }
        for (size_t i = 0, n = catalogAttributes.size(); i < n; i++, j++) {
            AttributeDesc const& attr = catalogAttributes[i];
            matchAttributes[j] = AttributeDesc(j, "match_" + attr.getName(), attr.getType(), attr.getFlags(),
                                               attr.getDefaultCompressionMethod(), attr.getAliases(), &attr.getDefaultValue(),
                                               attr.getDefaultValueExpr());
        }
        for (size_t i = 0, n = catalogDimensions.size(); i < n; i++, j++) {
            matchAttributes[j] = AttributeDesc(j, "match_" + catalogDimensions[i].getBaseName(), TID_INT64, 0, 0);
        }
        matchAttributes[j] = AttributeDesc(j, DEFAULT_EMPTY_TAG_ATTRIBUTE_NAME, TID_INDICATOR, AttributeDesc::IS_EMPTY_INDICATOR, 0);

        int64_t maxCollisions = evaluate(((boost::shared_ptr<OperatorParamLogicalExpression>&)_parameters[1])->getExpression(),
                                          query, TID_INT64).getInt64();
        if (maxCollisions <= 0 || (int32_t)maxCollisions != maxCollisions)  { 
            throw USER_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_WRONG_OPERATOR_ARGUMENT2) << "positive";
        }
        resultDimensions.push_back(DimensionDesc("collision", 0, 0, maxCollisions-1, maxCollisions-1, (uint32_t)maxCollisions, 0));
        return ArrayDesc("match", matchAttributes, resultDimensions);
    }
};

REGISTER_LOGICAL_OPERATOR_FACTORY(LogicalMatch, "match");


} //namespace
