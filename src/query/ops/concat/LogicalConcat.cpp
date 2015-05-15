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
 * LogicalConcat.cpp
 *
 *  Created on: Apr 20, 2010
 *      Author: Knizhnik
 */

#include <query/Operator.h>
#include <system/SystemCatalog.h>
#include <system/Exceptions.h>

using namespace std;

namespace scidb
{

/**
 * @brief The operator: concat().
 *
 * @par Synopsis:
 *   concat( srcArray1, srcArray2 )
 *
 * @par Summary:
 *   Produces a result array as the concatenation of two source arrays. The concatenation is performed by the first dimension.
 *
 * @par Input:
 *   - srcArray1: the first source array with srcAttrs and srcDims1.
 *   - srcArray2: the second source array with srcAttrs and srcDim2, where srcDim2 may differ from srcDims1 only in the start/end of the first dimension.
 *
 * @par Output array:
 *        <
 *   <br>   srcAttrs
 *   <br> >
 *   <br> [
 *   <br>   dims: same with srcDims1 and srcDims2, except in start/end of the first dimension.
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
 *   - concat(A, A) <quantity: uint64, sales:double> [year, item] =
 *     <br> year, item, quantity, sales
 *     <br> 2011,  2,      7,     31.64
 *     <br> 2011,  3,      6,     19.98
 *     <br> 2012,  1,      5,     41.65
 *     <br> 2012,  2,      9,     40.68
 *     <br> 2012,  3,      8,     26.64
 *     <br> 2013,  2,      7,     31.64
 *     <br> 2013,  3,      6,     19.98
 *     <br> 2014,  1,      5,     41.65
 *     <br> 2014,  2,      9,     40.68
 *     <br> 2014,  3,      8,     26.64
 *
 * @par Errors:
 *  n/a
 *
 * @par Notes:
 *   n/a
 *
 */
class LogicalConcat: public LogicalOperator
{
public:
    LogicalConcat(const string& logicalName, const std::string& alias) :
        LogicalOperator(logicalName, alias)
    {
        ADD_PARAM_INPUT()
        ADD_PARAM_INPUT()
    }

    ArrayDesc inferSchema(std::vector<ArrayDesc> schemas, boost::shared_ptr<Query> query)
    {
        size_t i;
        assert(schemas.size() == 2);
        ArrayDesc const& leftArrayDesc = schemas[0];
        ArrayDesc const& rightArrayDesc = schemas[1];

        // Check that attrbiutes are the same
        Attributes leftAttributes = leftArrayDesc.getAttributes();
        Attributes rightAttributes = rightArrayDesc.getAttributes();

        if (leftAttributes.size() != rightAttributes.size()
                && (leftAttributes.size() != rightAttributes.size() + 1
                        || !leftAttributes[leftAttributes.size() - 1].isEmptyIndicator())
                && (leftAttributes.size() + 1 != rightAttributes.size()
                        || !rightAttributes[rightAttributes.size() - 1].isEmptyIndicator()))
            throw USER_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_ARRAYS_NOT_CONFORMANT);

        size_t nAttrs = min(leftAttributes.size(), rightAttributes.size());
        for (i = 0; i < nAttrs; i++) {
            if (leftAttributes[i].getName() != rightAttributes[i].getName()
                    || leftAttributes[i].getType()
                            != rightAttributes[i].getType()) {
                throw USER_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_ARRAYS_NOT_CONFORMANT);
            }
        }
        Attributes newAttributes = (
                leftAttributes.size() > rightAttributes.size() ?
                        leftAttributes : rightAttributes);
        for (i = 0; i < nAttrs; i++) {
            if (leftAttributes[i].isNullable()
                    && !rightAttributes[i].isNullable()) {
                newAttributes[i] = leftAttributes[i];
            } else if (!leftAttributes[i].isNullable()
                    && rightAttributes[i].isNullable()) {
                newAttributes[i] = rightAttributes[i];
            }
        }

        // Check dimensions
        Dimensions const& leftDimensions = leftArrayDesc.getDimensions();
        Dimensions const& rightDimensions = rightArrayDesc.getDimensions();
        if (leftDimensions.size() != rightDimensions.size())
            throw USER_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_ARRAYS_NOT_CONFORMANT);
        size_t nDims = leftDimensions.size();
        Dimensions newDimensions(nDims);

        ArrayDesc lDesc, rDesc;
        if ((SystemCatalog::getInstance()->getArrayDesc(schemas[0].getName(), lDesc, false) &&
                lDesc.getDimensions()[0].getLength() == INFINITE_LENGTH) ||
            (SystemCatalog::getInstance()->getArrayDesc(schemas[1].getName(), rDesc, false) &&
                rDesc.getDimensions()[0].getLength() == INFINITE_LENGTH) ||
            (leftDimensions[0].getLength() == INFINITE_LENGTH ||
                rightDimensions[0].getLength() == INFINITE_LENGTH))
        {
            throw USER_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_OP_CONCAT_ERROR1);
        }

        newDimensions[0] = DimensionDesc(leftDimensions[0].getBaseName(),
                leftDimensions[0].getNamesAndAliases(),
                leftDimensions[0].getStartMin(),
                leftDimensions[0].getCurrStart(),
                leftDimensions[0].getCurrEnd() + rightDimensions[0].getLength(),
                leftDimensions[0].getEndMax() + rightDimensions[0].getLength(),
                leftDimensions[0].getChunkInterval(),
                leftDimensions[0].getChunkOverlap());
        if (leftDimensions[0].getChunkInterval()
                != rightDimensions[0].getChunkInterval()
                || leftDimensions[0].getChunkOverlap()
                        != rightDimensions[0].getChunkOverlap())
        {
            // XXX To do: implement requiresRepart() method, remove interval/overlap checks.
            throw USER_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_ARRAYS_NOT_CONFORMANT);
        }

        for (i = 1; i < nDims; i++) {
            if (leftDimensions[i].getLength() != rightDimensions[i].getLength()
                    || leftDimensions[i].getStartMin()
                            != rightDimensions[i].getStartMin()
                    || leftDimensions[i].getChunkInterval()
                            != rightDimensions[i].getChunkInterval()
                    || leftDimensions[i].getChunkOverlap()
                            != rightDimensions[i].getChunkOverlap())
                throw USER_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_ARRAYS_NOT_CONFORMANT);
            newDimensions[i] = leftDimensions[i];
        }
        return ArrayDesc(leftArrayDesc.getName() + rightArrayDesc.getName(),
                newAttributes, newDimensions);
    }
};

DECLARE_LOGICAL_OPERATOR_FACTORY(LogicalConcat, "concat")

} //namespace
