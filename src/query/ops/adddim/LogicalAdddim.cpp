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
 * LogicalAdddim.cpp
 *
 *  Created on: Apr 20, 2010
 *      Author: Knizhnik
 */

#include "query/Operator.h"
#include "system/SystemCatalog.h"
#include "system/Exceptions.h"

using namespace std;

namespace scidb {
    
/**
 * @brief The operator: adddim().
 *
 * @par Synopsis:
 *   adddim( srcArray, newDimName )
 *
 * @par Summary:
 *   Produces a result array with one more dimension than the source array.
 *
 * @par Input:
 *   - srcArray: a source array with srcAttrs and srcDims.
 *   - newDimName: the name of a new dimension.
 *
 * @par Output array:
 *        <
 *   <br>   srcAttrs
 *   <br> >
 *   <br> [
 *   <br>   newDimName: type=int64, start=0, end=0, chunk interval=1
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
 *   - adddim(A, loc) <quantity: uint64, sales: double> [loc, year, item] =
 *     <br> loc, year, item, quantity, sales
 *     <br>  0,  2011,  2,      7,     31.64
 *     <br>  0,  2011,  3,      6,     19.98
 *     <br>  0,  2012,  1,      5,     41.65
 *     <br>  0,  2012,  2,      9,     40.68
 *     <br>  0,  2012,  3,      8,     26.64
 *
 * @par Errors:
 *   n/a
 *
 * @par Notes:
 *   n/a
 *
 */
class LogicalAdddim: public LogicalOperator
{
public:
    LogicalAdddim(const string& logicalName, const std::string& alias):
        LogicalOperator(logicalName, alias)
    {
    	ADD_PARAM_INPUT()
    	ADD_PARAM_OUT_DIMENSION_NAME()
    }

    ArrayDesc inferSchema(vector<ArrayDesc> schemas, boost::shared_ptr< Query> query)
    {
        assert(schemas.size() == 1);
        assert(_parameters.size() == 1);
        assert(((boost::shared_ptr<OperatorParam>&)_parameters[0])->getParamType() == PARAM_DIMENSION_REF);

        const string &dimensionName = ((boost::shared_ptr<OperatorParamReference>&)_parameters[0])->getObjectName();
        ArrayDesc const& srcArrayDesc = schemas[0];
        Dimensions const& srcDimensions = srcArrayDesc.getDimensions();
        Dimensions dstDimensions(srcDimensions.size()+1);

        for (size_t i = 0, n = srcDimensions.size(); i < n; i++)
        {
            if (dimensionName == srcDimensions[i].getBaseName())
            {
                throw USER_QUERY_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_DUPLICATE_DIMENSION_NAME,
                        _parameters[0]->getParsingContext()) << dimensionName;
            }
            dstDimensions[i+1] = srcDimensions[i];
        }
        dstDimensions[0] = DimensionDesc(dimensionName, 0, 0, 0, 0, 1, 0);
        return ArrayDesc(srcArrayDesc.getName(), srcArrayDesc.getAttributes(), dstDimensions);
    }
};

DECLARE_LOGICAL_OPERATOR_FACTORY(LogicalAdddim, "adddim")


} //namespace
