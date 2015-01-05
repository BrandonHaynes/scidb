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
 * LogicalLookup.cpp
 *
 *  Created on: Jul 26, 2010
 *      Author: Knizhnik
 */

#include "query/Operator.h"
#include "system/Exceptions.h"

namespace scidb {

using namespace std;

/**
 * @brief The operator: lookup().
 *
 * @par Synopsis:
 *   lookup( coordArray, srcArray )
 *
 * @par Summary:
 *   Retrieves the elements from srcArray, using coordinates stored in coordArray.
 *
 * @par Input:
 *   - coordArray: coordDims will be used as the dims in the output array, coordAttrs define coordinates in srcArray.
 *   - srcArray: srcDims and srcAttrs.
 *
 * @par Output array:
 *        <
 *   <br>   srcAttrs: attributes are from srcArray.
 *   <br> >
 *   <br> [
 *   <br>   coordDims: dimensions are from coordArray.
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
class LogicalLookup: public  LogicalOperator
{
public:
	LogicalLookup(const std::string& logicalName, const std::string& alias):
	    LogicalOperator(logicalName, alias)
	{
	    // Lookup operator has two input arrays
        ADD_PARAM_INPUT()
        ADD_PARAM_INPUT()
	}

    ArrayDesc inferSchema(std::vector< ArrayDesc> schemas, boost::shared_ptr< Query> query)
    {
        assert(schemas.size() == 2);
        if (schemas[0].getAttributes(true).size() != schemas[1].getDimensions().size()) { 
            throw USER_EXCEPTION(SCIDB_SE_OPERATOR, SCIDB_LE_LOOKUP_BAD_PARAM);
        }
        return ArrayDesc("lookup",  schemas[1].getAttributes(), schemas[0].getDimensions());
	}
};

DECLARE_LOGICAL_OPERATOR_FACTORY(LogicalLookup, "lookup")


}  // namespace scidb
