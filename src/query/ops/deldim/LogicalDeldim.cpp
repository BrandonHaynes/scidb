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
 * LogicalDeldim.cpp
 *
 *  Created on: Apr 20, 2010
 *      Author: Knizhnik
 */

#include "query/Operator.h"
#include "system/SystemCatalog.h"
#include "system/Exceptions.h"

using namespace std;

namespace scidb
{

/**
 * @brief The operator: deldim().
 *
 * @par Synopsis:
 *   deldim( srcArray )
 *
 * @par Summary:
 *   Produces a result array with one fewer dimension than the source array, by deleting the first dimension which must have size 1.
 *
 * @par Input:
 *   - srcArray: a source array with dim1, dim2, ..., dim_kThe first dimension must have size 1.
 *
 * @par Output array:
 *        <
 *   <br>   attrs
 *   <br> >
 *   <br> [
 *   <br>   dim2, ..., dim_k
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
class LogicalDeldim: public LogicalOperator
{
public:
    LogicalDeldim(const string& logicalName, const std::string& alias):
        LogicalOperator(logicalName, alias)
    {
        ADD_PARAM_INPUT()
    }

    ArrayDesc inferSchema(std::vector<ArrayDesc> schemas, boost::shared_ptr<Query> query)
    {
        assert(schemas.size() == 1);
        assert(_parameters.size() == 0);

        ArrayDesc const& srcArrayDesc = schemas[0];
        Dimensions const& srcDimensions = srcArrayDesc.getDimensions();
        
        if (srcDimensions.size() <= 1)
            throw USER_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_OP_DELDIM_ERROR1);
        
        Dimensions dstDimensions(srcDimensions.size()-1);
        if (srcDimensions[0].getLength() != 1)
            throw USER_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_OP_DELDIM_ERROR2);
        for (size_t i = 0, n = dstDimensions.size(); i < n; i++) {
            dstDimensions[i] = srcDimensions[i+1];
        }
        return ArrayDesc(srcArrayDesc.getName(), srcArrayDesc.getAttributes(), dstDimensions);
    }
};

DECLARE_LOGICAL_OPERATOR_FACTORY(LogicalDeldim, "deldim")


} //namespace
