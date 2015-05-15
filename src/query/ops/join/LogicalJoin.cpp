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
 * LogicalJoin.cpp
 *
 *  Created on: Apr 20, 2010
 *      Author: Knizhnik
 */

#include <boost/foreach.hpp>
#include <query/Operator.h>
#include <system/SystemCatalog.h>
#include <system/Exceptions.h>
#include <array/Metadata.h>

using namespace std;

namespace scidb
{

/**
 * @brief The operator: join().
 *
 * @par Synopsis:
 *   join( leftArray, rightArray )
 *
 * @par Summary:
 *   Combines the attributes of two arrays at matching dimension values.
 *   The two arrays must have the same dimension start coordinates, the same chunk size, and the same chunk overlap.
 *   The join result has the same dimension names as the first input.
 *   The cell in the result array contains the concatenation of the attributes from the two source cells.
 *   If a pair of join dimensions have different lengths, the result array uses the smaller of the two.
 *
 * @par Input:
 *   - leftArray: the left-side source array with leftAttrs and leftDims.
 *   - rightArray: the right-side source array with rightAttrs and rightDims.
 *
 * @par Output array:
 *        <
 *   <br>   leftAttrs + rightAttrs: in case an attribute in rightAttrs conflicts with an attribute in leftAttrs, '_2' will be appended.
 *   <br> >
 *   <br> [
 *   <br>   leftDims
 *   <br> ]
 *
 * @par Examples:
 *   n/a
 *
 * @par Errors:
 *   n/a
 *
 * @par Notes:
 *   - join() is a special case of cross_join() with all pairs of dimensions given.
 *
 */
class LogicalJoin: public LogicalOperator
{
  public:
    LogicalJoin(const string& logicalName, const std::string& alias):
        LogicalOperator(logicalName, alias)
    {
    	ADD_PARAM_INPUT()
    	ADD_PARAM_INPUT()
    }

    ArrayDesc inferSchema(std::vector< ArrayDesc> schemas, boost::shared_ptr< Query> query)
    {
        assert(schemas.size() == 2);

        ArrayDesc const& leftArrayDesc = schemas[0];
        ArrayDesc const& rightArrayDesc = schemas[1];
        Attributes const& leftAttributes = leftArrayDesc.getAttributes();
        Dimensions leftDimensions = leftArrayDesc.getDimensions();
        Attributes const& rightAttributes = rightArrayDesc.getAttributes();
        Dimensions const& rightDimensions = rightArrayDesc.getDimensions();
        size_t totalAttributes = leftAttributes.size() + rightAttributes.size();
        int nBitmaps = 0;
        nBitmaps += (leftArrayDesc.getEmptyBitmapAttribute() != NULL);
        nBitmaps += (rightArrayDesc.getEmptyBitmapAttribute() != NULL);
        if (nBitmaps == 2) {
            totalAttributes -= 1;
        }

        Attributes joinAttributes(totalAttributes);
        size_t j = 0;
        for (size_t i = 0, n = leftAttributes.size(); i < n; i++) {
            AttributeDesc const& attr = leftAttributes[i];
            if (!attr.isEmptyIndicator()) {
                joinAttributes[j] = AttributeDesc(j, attr.getName(), attr.getType(), attr.getFlags(),
                    attr.getDefaultCompressionMethod(), attr.getAliases(), &attr.getDefaultValue(),
                    attr.getDefaultValueExpr());
                joinAttributes[j].addAlias(leftArrayDesc.getName());
                j += 1;
            }
        }
        for (size_t i = 0, n = rightAttributes.size(); i < n; i++, j++) {
            AttributeDesc const& attr = rightAttributes[i];
            joinAttributes[j] = AttributeDesc(j, attr.getName(), attr.getType(), attr.getFlags(),
                attr.getDefaultCompressionMethod(), attr.getAliases(), &attr.getDefaultValue(),
                attr.getDefaultValueExpr());
            joinAttributes[j].addAlias(rightArrayDesc.getName());
        }
        if (j < totalAttributes) {
            joinAttributes[j] = AttributeDesc(j, DEFAULT_EMPTY_TAG_ATTRIBUTE_NAME,  TID_INDICATOR,
                AttributeDesc::IS_EMPTY_INDICATOR, 0);
        }

        if(leftDimensions.size() != rightDimensions.size())
        {
            ostringstream left, right;
            printDimNames(left, leftDimensions);
            printDimNames(right, rightDimensions);
            throw USER_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_DIMENSION_COUNT_MISMATCH)
                << "join" << left.str() << right.str();
        }

        ostringstream ss;
        int mismatches = 0;
        for (size_t i = 0, n = leftDimensions.size(); i < n; i++)
        {
            if(leftDimensions[i].getStartMin() != rightDimensions[i].getStartMin())
            {
                if (mismatches++) {
                    ss << ", ";
                }
                ss << '[' << leftDimensions[i] << "] != [" << rightDimensions[i] << ']';
            }
        }
        if (mismatches)
        {
            throw USER_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_START_INDEX_MISMATCH) << ss.str();
        }

        Dimensions joinDimensions;
        for (size_t i = 0, n = leftDimensions.size(); i < n; i++)
        {
            assert(leftDimensions[i].getStartMin() == rightDimensions[i].getStartMin());
            DimensionDesc &lDim = leftDimensions[i];
            joinDimensions.push_back(
                   DimensionDesc(
                           lDim.getBaseName(),
                           lDim.getNamesAndAliases(),
                           lDim.getStartMin(),
                           lDim.getCurrStart(),
                           lDim.getCurrEnd(),
                           lDim.getEndMax(),
                           lDim.getChunkInterval(),
                           min(lDim.getChunkOverlap(), rightDimensions[i].getChunkOverlap())
                       )
                   );
           joinDimensions[i].addAlias(leftArrayDesc.getName());

           Coordinate newCurrStart = max(leftDimensions[i].getCurrStart(), rightDimensions[i].getCurrStart());
           Coordinate newCurrEnd = min(leftDimensions[i].getCurrEnd(), rightDimensions[i].getCurrEnd());
           Coordinate newEndMax = min(leftDimensions[i].getEndMax(), rightDimensions[i].getEndMax());
           joinDimensions[i].setCurrStart(newCurrStart);
           joinDimensions[i].setCurrEnd(newCurrEnd);
           joinDimensions[i].setEndMax(newEndMax);

           BOOST_FOREACH(const ObjectNames::NamesPairType &rDimName, rightDimensions[i].getNamesAndAliases())
           {
               BOOST_FOREACH(const string &alias, rDimName.second)
               {
                   joinDimensions[i].addAlias(alias, rDimName.first);
               }
           }
        }
        return ArrayDesc(leftArrayDesc.getName() + rightArrayDesc.getName(), joinAttributes, joinDimensions);
    }
};

DECLARE_LOGICAL_OPERATOR_FACTORY(LogicalJoin, "join")


} //namespace
