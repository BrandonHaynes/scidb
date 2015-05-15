/*
**
* BEGIN_COPYRIGHT
*
* This file is part of SciDB.
* Copyright (C) 2014 SciDB, Inc.
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
 * LogicalCrossBetween.cpp
 *
 *  Created on: August 15, 2014
 *  Author: Donghui Zhang
 */

#include <query/Operator.h>
#include <system/Exceptions.h>

namespace scidb
{

/**
 * @brief The operator: cross_between().
 *
 * @par Synopsis:
 *   cross_between( srcArray, rangesArray )
 *
 * @par Summary:
 *   Produces a result array by cutting out data in one of the rectangular ranges specified in rangesArray.
 *
 * @par Input:
 *   - srcArray: a source array with srcAttrs and srcDims.
 *   - rangesArray: an array with (|srcDims| * 2) attributes all having type int64.
 *
 * @par Output array:
 *        <
 *   <br>   srcAttrs
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
 *   - Given array R <year_low, item_low, year_high, item_high>[i] =
 *     <br> i, year_low, item_low, year_high, item_high
 *     <br> 0, 2011,      3,       2011,       3
 *     <br> 1, 2012,      1,       2012,       2
 *   - cross_between(A, R) <quantity: uint64, sales:double> [year, item] =
 *     <br> year, item, quantity, sales
 *     <br> 2011,  3,      6,     19.98
 *     <br> 2012,  1,      5,     41.65
 *     <br> 2012,  2,      9,     40.68
 *
 * @par Errors:
 *   n/a
 *
 * @par Notes:
 *   - Similar to between().
 *   - The operator only works if the size of the rangesArray is very small.
 *
 */
class LogicalCrossBetween: public  LogicalOperator
{
  public:
	LogicalCrossBetween(const std::string& logicalName, const std::string& alias) : LogicalOperator(logicalName, alias)
	{
        ADD_PARAM_INPUT()
        ADD_PARAM_INPUT()
	}

    ArrayDesc inferSchema(std::vector< ArrayDesc> schemas, boost::shared_ptr< Query> query)
	{
		assert(schemas.size() == 2);

		// verify that rangesArray has (|srcDims| * 2) attributes all having type int64.
		const Dimensions& dimsSrcArray = schemas[0].getDimensions();
		const bool excludeEmptyBitmap = true;
		const Attributes& attrsRangesArray = schemas[1].getAttributes(excludeEmptyBitmap);
		if (dimsSrcArray.size()*2 != attrsRangesArray.size()) {
		    throw USER_EXCEPTION(SCIDB_SE_OPERATOR, SCIDB_LE_CROSSBETWEEN_NUM_ATTRIBUTES_MISMATCH);
		}
		for (size_t i=0; i<attrsRangesArray.size(); ++i) {
		    if (attrsRangesArray[i].getType() != TID_INT64 ) {
		        throw USER_EXCEPTION(SCIDB_SE_OPERATOR, SCIDB_LE_CROSSBETWEEN_RANGES_ARRAY_ATTRIBUTE_NOT_INT64);
		    }
		}
        return addEmptyTagAttribute(schemas[0]);
	}
};

DECLARE_LOGICAL_OPERATOR_FACTORY(LogicalCrossBetween, "cross_between")


}  // namespace scidb
