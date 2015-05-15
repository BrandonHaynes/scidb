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
 * LogicalCrossJoin.cpp
 *
 *  Created on: Mar 09, 2011
 *      Author: Knizhnik
 */

#include <query/Operator.h>
#include <system/SystemCatalog.h>
#include <system/Exceptions.h>
#include <array/Metadata.h>

using namespace std;

namespace scidb
{

/**
 * @brief The operator: cross_join().
 *
 * @par Synopsis:
 *   cross_join( leftArray, rightArray {, attrLeft, attrRight}* )
 *
 * @par Summary:
 *   Calculates the cross product of two arrays, with 0 or more equality conditions on the dimensions.
 *   Assume p pairs of equality conditions exist. The result is an (m+n-p) dimensional array.
 *   From the coordinates of each cell in the result array, a single cell in leftArray and a single cell in rightArray can be located.
 *   The cell in the result array contains the concatenation of the attributes from the two source cells.
 *   If a pair of join dimensions have different lengths, the result array uses the smaller of the two.
 *
 * @par Input:
 *   - leftArray: the left-side source array with leftAttrs and leftDims.
 *   - rightArray: the right-side source array with rightAttrs and rightDims.
 *   - 0 or more pairs of an attribute from leftArray and an attribute from rightArray.
 *
 * @par Output array:
 *        <
 *   <br>   leftAttrs + rightAttrs
 *   <br> >
 *   <br> [
 *   <br>   leftDims + (rightDims - leftDims)
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
 *
 *   - Given array B <v:uint64> [k] =
 *     <br> k,  v
 *     <br> 1,  10
 *     <br> 2,  20
 *     <br> 3,  30
 *     <br> 4,  40
 *     <br> 5,  50
 *   - cross_join(A, B, item, k) <quantity: uint64, sales:double, v:uint64> [year, item] =
 *     <br> year, item, quantity, sales,  v
 *     <br> 2011,  2,      7,     31.64,  20
 *     <br> 2011,  3,      6,     19.98,  30
 *     <br> 2012,  1,      5,     41.65,  10
 *     <br> 2012,  2,      9,     40.68,  20
 *     <br> 2012,  3,      8,     26.64,  30
 *
 * @par Errors:
 *   - SCIDB_SE_OPERATOR::SCIDB_LE_OP_CROSSJOIN-ERROR2: if the number of input dimensions is not even.
 *   - SCIDB_SE_INFER_SCHEMA::SCIDB_LE_ARRAYS_NOT_FONFORMANT: if any join dimension is not an integer dimension, or if a pair of join dimensions
 *     do not have the same start, chunk interval, or overlap.
 *
 * @par Notes:
 *   - Joining non-integer dimensions does not work.
 *
 */
class LogicalCrossJoin: public LogicalOperator
{
  public:
    LogicalCrossJoin(const string& logicalName, const std::string& alias):
        LogicalOperator(logicalName, alias)
    {
    	ADD_PARAM_INPUT()
        ADD_PARAM_INPUT()
        ADD_PARAM_VARIES()           
    }

    std::vector<boost::shared_ptr<OperatorParamPlaceholder> >
    nextVaryParamPlaceholder(const std::vector< ArrayDesc> &schemas)
    {
        std::vector<boost::shared_ptr<OperatorParamPlaceholder> > res;
        res.push_back(END_OF_VARIES_PARAMS());
        res.push_back(PARAM_IN_DIMENSION_NAME());
        return res;
    }

    ArrayDesc inferSchema(std::vector< ArrayDesc> schemas, boost::shared_ptr< Query> query)
    {
        if ((_parameters.size() & 1) != 0) {
            throw USER_EXCEPTION(SCIDB_SE_OPERATOR, SCIDB_LE_OP_CROSSJOIN_ERROR2);
        }
        assert(schemas.size() == 2);

        ArrayDesc const& leftArrayDesc = schemas[0];
        ArrayDesc const& rightArrayDesc = schemas[1];
        Attributes const& leftAttributes = leftArrayDesc.getAttributes();
        Dimensions leftDimensions = leftArrayDesc.getDimensions();
        Attributes const& rightAttributes = rightArrayDesc.getAttributes();
        Dimensions const& rightDimensions = rightArrayDesc.getDimensions();
        size_t totalAttributes = leftAttributes.size() + rightAttributes.size();
        AttributeDesc const* leftBitmap = leftArrayDesc.getEmptyBitmapAttribute();
        AttributeDesc const* rightBitmap = rightArrayDesc.getEmptyBitmapAttribute();
        if (leftBitmap && rightBitmap) { 
            totalAttributes -= 1;
        }

        Attributes CrossJoinAttributes(totalAttributes);
        size_t j = 0;
        for (size_t i = 0, n = leftAttributes.size(); i < n; i++) {
            AttributeDesc const& attr = leftAttributes[i];
            if (!attr.isEmptyIndicator()) {
                CrossJoinAttributes[j] = AttributeDesc(j, attr.getName(), attr.getType(), attr.getFlags(),
                    attr.getDefaultCompressionMethod(), attr.getAliases(), &attr.getDefaultValue(),
                    attr.getDefaultValueExpr());
                CrossJoinAttributes[j].addAlias(leftArrayDesc.getName());
                j += 1;
            }
        }
        for (size_t i = 0, n = rightAttributes.size(); i < n; i++, j++) {
            AttributeDesc const& attr = rightAttributes[i];
            CrossJoinAttributes[j] = AttributeDesc(j, attr.getName(), attr.getType(), attr.getFlags(),
                attr.getDefaultCompressionMethod(), attr.getAliases(), &attr.getDefaultValue(),
                attr.getDefaultValueExpr());
            CrossJoinAttributes[j].addAlias(rightArrayDesc.getName());
        }
        if (leftBitmap && !rightBitmap) { 
            AttributeDesc const& attr = *leftBitmap;
            CrossJoinAttributes[j] = AttributeDesc(j, attr.getName(), attr.getType(), attr.getFlags(),
                attr.getDefaultCompressionMethod(), attr.getAliases(), &attr.getDefaultValue(),
                attr.getDefaultValueExpr());
            CrossJoinAttributes[j].addAlias(leftArrayDesc.getName());
        }

        size_t nRightDims = rightDimensions.size();
        size_t nLeftDims = leftDimensions.size();
        Dimensions CrossJoinDimensions(nLeftDims + nRightDims - _parameters.size()/2);
        vector<int> CrossJoinOnDimensions(nRightDims, -1);
        uint64_t leftCrossJoinOnMask = 0;
        uint64_t rightCrossJoinOnMask = 0;
        for (size_t p = 0, np = _parameters.size(); p < np; p += 2) {
            shared_ptr<OperatorParamDimensionReference> leftDim = (shared_ptr<OperatorParamDimensionReference>&)_parameters[p];
            shared_ptr<OperatorParamDimensionReference> rightDim = (shared_ptr<OperatorParamDimensionReference>&)_parameters[p+1];
            
            const string &leftDimName = leftDim->getObjectName();
            const string &rightDimName = rightDim->getObjectName();
            const string &leftDimArray = leftDim->getArrayName();
            const string &rightDimArray = rightDim->getArrayName();

            ssize_t l = leftArrayDesc.findDimension(leftDimName, leftDimArray);
            if (l < 0) {
                throw USER_QUERY_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_DIMENSION_NOT_EXIST,
                                           leftDim->getParsingContext())
                    << leftDimName << "lefthand" << leftDimensions;
            }
            if ((leftCrossJoinOnMask & ((uint64_t)1 << l)) != 0) {
                // Dimension should be specified only once in parameter list.
                throw USER_QUERY_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_OP_CROSSJOIN_ERROR1,
                                           leftDim->getParsingContext());
            }
            leftCrossJoinOnMask |=  (uint64_t)1 << l;
            
            ssize_t r = rightArrayDesc.findDimension(rightDimName, rightDimArray);
            if (r < 0) {
                throw USER_QUERY_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_DIMENSION_NOT_EXIST,
                                           rightDim->getParsingContext())
                    << rightDimName << "righthand" << rightDimensions;
            }
            if ((rightCrossJoinOnMask & ((uint64_t)1 << r)) != 0) {
                // Dimension should be specified only once in parameter list.
                throw USER_QUERY_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_OP_CROSSJOIN_ERROR1,
                                           rightDim->getParsingContext());
            }
            rightCrossJoinOnMask |=  (uint64_t)1 << r;
            
            // Differences in chunk size and overlap are now handled via PhysicalCrossJoin::requiresRepart().
            if (leftDimensions[l].getStartMin() != rightDimensions[r].getStartMin()) {
                ostringstream ss;
                ss << leftDimensions[l] << " != " << rightDimensions[r];
                throw USER_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_START_INDEX_MISMATCH) << ss.str();
            }
            
            if (CrossJoinOnDimensions[r] >= 0) {
                throw USER_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_OP_CROSSJOIN_ERROR1);
            }
            CrossJoinOnDimensions[r] = (int)l;
        }                                           

        j = 0;
        for (size_t i = 0; i < nLeftDims; i++) { 
            CrossJoinDimensions[j] = leftDimensions[i];
            CrossJoinDimensions[j].addAlias(leftArrayDesc.getName());
            ++j;
        }
        for (size_t i = 0; i < nRightDims; i++) { 
            if (CrossJoinOnDimensions[i] < 0)
            {
                CrossJoinDimensions[j] = rightDimensions[i];
                CrossJoinDimensions[j].addAlias(rightArrayDesc.getName());
                ++j;
            }
            else
            {
                DimensionDesc& d = CrossJoinDimensions[CrossJoinOnDimensions[i]];
                DimensionDesc const& right = rightDimensions[i];
                Coordinate newCurrStart = max(d.getCurrStart(), right.getCurrStart());
                Coordinate newCurrEnd = min(d.getCurrEnd(), right.getCurrEnd());
                Coordinate newEndMax = min(d.getEndMax(), right.getEndMax());
                d.setCurrStart(newCurrStart);
                d.setCurrEnd(newCurrEnd);
                d.setEndMax(newEndMax);
            }
        }
        return ArrayDesc(leftArrayDesc.getName() + rightArrayDesc.getName(), CrossJoinAttributes, CrossJoinDimensions);
    }
};

DECLARE_LOGICAL_OPERATOR_FACTORY(LogicalCrossJoin, "cross_join")


} //namespace
