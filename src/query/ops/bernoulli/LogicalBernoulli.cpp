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
 * LogicalBernoulli.cpp
 *
 *  Created on: Feb 14, 2010
 *      Author: Knizhnik
 */

#include "query/Operator.h"
#include "system/SystemCatalog.h"
#include "system/Exceptions.h"

using namespace std;

namespace scidb {
    
/**
 * @brief The operator: bernoulli().
 *
 * @par Synopsis:
 *   bernoulli( srcArray, probability [, seed] )
 *
 * @par Summary:
 *   Evaluates whether to include a cell in the result array by generating a random number and checks if it is less than probability.
 *
 * @par Input:
 *   - srcArray: a source array with srcAttrs and srcDims.
 *   - probability: the probability threshold, in [0..1]
 *   - an optional seed for the random number generator.
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
 *   - bernoulli(A, 0.5, 100) <quantity: uint64, sales:double> [year, item] =
 *     <br> year, item, quantity, sales
 *     <br> 2011,  3,      6,     19.98
 *     <br> 2012,  1,      5,     41.65
 *     <br> 2012,  3,      8,     26.64
 *
 * @par Errors:
 *   n/a
 *
 * @par Notes:
 *   n/a
 *
 */
class LogicalBernoulli: public LogicalOperator
{
public:
    LogicalBernoulli(const string& logicalName, const std::string& alias):
        LogicalOperator(logicalName, alias)
    {
    	ADD_PARAM_INPUT()
        ADD_PARAM_CONSTANT("double");
        ADD_PARAM_VARIES() 
    }

	std::vector<boost::shared_ptr<OperatorParamPlaceholder> > nextVaryParamPlaceholder(const std::vector< ArrayDesc> &schemas)
	{
		std::vector<boost::shared_ptr<OperatorParamPlaceholder> > res;
        res.push_back(END_OF_VARIES_PARAMS());
        res.push_back(PARAM_CONSTANT("int64"));
        return res;
	}

    ArrayDesc inferSchema(vector<ArrayDesc> schemas, boost::shared_ptr< Query> query)
    {
        assert(schemas.size() == 1);
        return addEmptyTagAttribute(schemas[0]);
    }
};

DECLARE_LOGICAL_OPERATOR_FACTORY(LogicalBernoulli, "bernoulli")


} //namespace
