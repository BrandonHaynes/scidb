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
 * LogicalMerge.cpp
 *
 *  Created on: Apr 20, 2010
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
 * @brief The operator: merge().
 *
 * @par Synopsis:
 *   merge( leftArray, rightArray )
 *
 * @par Summary:
 *   Combines elements from the input arrays the following way:
 *   for each cell in the two inputs, if the cell of leftArray is not empty, the attributes from that cell are selected and placed in the output array;
 *   otherwise, the attributes from the corresponding cell in rightArray are taken.
 *   The two arrays should have the same attribute list, number of dimensions, and dimension start index.
 *   If the dimensions are not the same size, the output array uses the larger of the two.
 *
 * @par Input:
 *   - leftArray: the left-hand-side array.
 *   - rightArray: the right-hand-side array.
 *
 * @par Output array:
 *        <
 *   <br>   leftAttrs: which is equivalent to rightAttrs.
 *   <br> >
 *   <br> [
 *   <br>   max(leftDims, rightDims): for each dim, use the larger of leftDim and rightDim.
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
class LogicalMerge: public LogicalOperator
{
public:
    LogicalMerge(const string& logicalName, const std::string& alias):
        LogicalOperator(logicalName, alias)
    {
    	ADD_PARAM_INPUT()
		ADD_PARAM_INPUT()
		ADD_PARAM_VARIES()
    }

	std::vector<boost::shared_ptr<OperatorParamPlaceholder> > nextVaryParamPlaceholder(const std::vector< ArrayDesc> &schemas)
	{
		std::vector<boost::shared_ptr<OperatorParamPlaceholder> > res;
		res.push_back(PARAM_INPUT());
		res.push_back(END_OF_VARIES_PARAMS());
		return res;
	}

    ArrayDesc inferSchema(std::vector< ArrayDesc> schemas, boost::shared_ptr< Query> query)
    {
        assert(schemas.size() >= 2);
        assert(_parameters.size() == 0);

        Attributes const& leftAttributes = schemas[0].getAttributes();
        Dimensions const& leftDimensions = schemas[0].getDimensions();
        Attributes const* newAttributes = &leftAttributes;
        Dimensions newDims = leftDimensions;
        size_t nDims = newDims.size();

        for (size_t j = 1; j < schemas.size(); j++) {
            Attributes const& rightAttributes = schemas[j].getAttributes();
            Dimensions const& rightDimensions = schemas[j].getDimensions();

            if (nDims != rightDimensions.size()) {
                throw USER_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_DIMENSION_COUNT_MISMATCH)
                    << "merge" << schemas[0] << schemas[j];
            }

            // Report all startIndex problems at once.
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


            for (size_t i = 0; i < nDims; i++) {
                assert(leftDimensions[i].getStartMin() == rightDimensions[i].getStartMin());

                DimensionDesc& dim = newDims[i];
                dim = DimensionDesc(dim.getBaseName(),
                                    dim.getNamesAndAliases(),
                                    min(dim.getStartMin(), rightDimensions[i].getStartMin()),
                                    min(dim.getCurrStart(), rightDimensions[i].getCurrStart()),
                                    max(dim.getCurrEnd(), rightDimensions[i].getCurrEnd()),
                                    max(dim.getEndMax(), rightDimensions[i].getEndMax()),
                                    dim.getChunkInterval(), 
                                    dim.getChunkOverlap());
            }

            if (leftAttributes.size() != rightAttributes.size()
                    && (leftAttributes.size() != rightAttributes.size()+1
                        || !leftAttributes[leftAttributes.size()-1].isEmptyIndicator())
                    && (leftAttributes.size()+1 != rightAttributes.size()
                        || !rightAttributes[rightAttributes.size()-1].isEmptyIndicator()))
            {
                throw USER_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_ATTR_COUNT_MISMATCH)
                    << schemas[0] << schemas[j];
            }

            size_t nAttrs = min(leftAttributes.size(), rightAttributes.size());
            if (rightAttributes.size() > newAttributes->size()) { 
                newAttributes = &rightAttributes;
            }
            for (size_t i = 0; i < nAttrs; i++)
            {
                if (leftAttributes[i].getType() != rightAttributes[i].getType()
                    || leftAttributes[i].getFlags() != rightAttributes[i].getFlags())
                {
                    throw USER_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_ATTR_TYPE_MISMATCH)
                        << leftAttributes[i].getName() << rightAttributes[i].getName()
                        << schemas[0] << schemas[j];
                }
            }
        }
        return ArrayDesc(schemas[0].getName(), *newAttributes, newDims);
    }
};

DECLARE_LOGICAL_OPERATOR_FACTORY(LogicalMerge, "merge")

} //namespace
