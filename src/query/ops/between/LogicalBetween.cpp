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
 * LogicalBetween.cpp
 *
 *  Created on: Oct 22, 2010
 *      Author: knizhnik@garret.ru
 */

#include "query/Operator.h"
#include "system/Exceptions.h"


namespace scidb {

/**
 * @brief The operator: between().
 *
 * @par Synopsis:
 *   between( srcArray {, lowCoord}+ {, highCoord}+ )
 *
 * @par Summary:
 *   Produces a result array from a specified, contiguous region of a source array.
 *
 * @par Input:
 *   - srcArray: a source array with srcAttrs and srcDims.
 *   - the low coordinates
 *   - the high coordinates
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
 *   - between(A, 2011, 1, 2012, 2) <quantity: uint64, sales:double> [year, item] =
 *     <br> year, item, quantity, sales
 *     <br> 2011,  2,      7,     31.64
 *     <br> 2012,  1,      5,     41.65
 *     <br> 2012,  2,      9,     40.68
 *
 * @par Errors:
 *   n/a
 *
 * @par Notes:
 *   - Almost the same as subarray. The only difference is that the dimensions retain the original start/end/boundaries.
 *
 */
class LogicalBetween: public  LogicalOperator
{
  public:
	LogicalBetween(const std::string& logicalName, const std::string& alias) : LogicalOperator(logicalName, alias)
	{
		ADD_PARAM_INPUT()
		ADD_PARAM_VARIES()
	}

	std::vector<boost::shared_ptr<OperatorParamPlaceholder> > nextVaryParamPlaceholder(const std::vector<ArrayDesc> &schemas)
	{
		std::vector<boost::shared_ptr<OperatorParamPlaceholder> > res;
        size_t i = _parameters.size();
        Dimensions const& dims = schemas[0].getDimensions();
        size_t nDims = dims.size();
		if (i < nDims*2)
			res.push_back(PARAM_CONSTANT(TID_INT64));
		else
			res.push_back(END_OF_VARIES_PARAMS());
		return res;
	}

    ArrayDesc inferSchema(std::vector< ArrayDesc> schemas, boost::shared_ptr< Query> query)
	{
		assert(schemas.size() == 1);
        return addEmptyTagAttribute(schemas[0]);
	}
};

DECLARE_LOGICAL_OPERATOR_FACTORY(LogicalBetween, "between")


}  // namespace scidb
