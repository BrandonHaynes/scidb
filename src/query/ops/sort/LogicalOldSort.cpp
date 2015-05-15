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
 * LogicalOldSort.cpp
 *
 *  Created on: May 6, 2010
 *      Author: Knizhnik
 *      Author: poliocough@gmail.com
 */

#include <query/Operator.h>
#include <array/SortArray.h>
#include <system/Exceptions.h>

namespace scidb {

/**
 * @brief The operator: old_sort().
 *
 * @par Synopsis:
 *   old_sort( srcArray {, attr [asc | desc]}* {, chunkSize}? )
 *
 * @par Summary:
 *   Produces a 1D array by sorting the non-empty cells of a source array.
 *
 * @par Input:
 *   - srcArray: the source array with srcAttrs and srcDim.
 *   - attr: the list of attributes to sort by. If no attribute is provided, the first attribute will be used.
 *   - asc | desc: whether ascending or descending order of the attribute should be used. The default is asc.
 *   - chunkSize: the size of a chunk in the result array. If not provided, 1M will be used.
 *
 * @par Output array:
 *        <
 *   <br>   srcAttrs: all the attributes are retained.
 *   <br> >
 *   <br> [
 *   <br>   n: start=0, end=MAX_COORDINATE, chunk interval = min{defaultChunkSize, #logical cells in srcArray)
 *   <br> ]
 *
 * @par Examples:
 *   n/a
 *
 * @par Errors:
 *   n/a
 *
 * @par Notes:
 *   Assuming null < NaN < other values
 *
 */
class LogicalOldSort: public LogicalOperator
{
public:
	LogicalOldSort(const std::string& logicalName, const std::string& alias):
	    LogicalOperator(logicalName, alias)
	{
		ADD_PARAM_INPUT()
		ADD_PARAM_VARIES()

        _globalOperatorName = std::pair<std::string, std::string>("sort2", "physicalSort2");
	}

        std::vector<boost::shared_ptr<OperatorParamPlaceholder> > nextVaryParamPlaceholder(const std::vector< ArrayDesc> &schemas)
	{
		std::vector<boost::shared_ptr<OperatorParamPlaceholder> > res;
		res.push_back(PARAM_IN_ATTRIBUTE_NAME("void"));
		res.push_back(PARAM_CONSTANT("int64"));
		res.push_back(END_OF_VARIES_PARAMS());
		return res;
	}
    
        ArrayDesc inferSchema(std::vector< ArrayDesc> schemas, boost::shared_ptr< Query> query)
	{

            //As far as chunk sizes, they can be a pain! So we allow the user to specify an optional chunk size
            //as part of the sort op.
            
            assert(schemas.size() >= 1);
            ArrayDesc const& schema = schemas[0];
            size_t chunkSize = 0;
            for(size_t i =0; i<_parameters.size(); i++)
            {
                if(_parameters[i]->getParamType()==PARAM_LOGICAL_EXPRESSION)
                {
                    chunkSize = evaluate(((boost::shared_ptr<OperatorParamLogicalExpression>&)_parameters[i])->getExpression(),
                                         query, TID_INT64).getInt64();
                    if(chunkSize <= 0)
                    {
                        throw SYSTEM_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_CHUNK_SIZE_MUST_BE_POSITIVE);
                    }
                    break;
                }
            }

            // Use a SortArray object to build the schema
            const bool preservePositions = false;
            SortArray sorter(schema, arena::getArena(), preservePositions, chunkSize);

            return sorter.getOutputArrayDesc();
	}
};

DECLARE_LOGICAL_OPERATOR_FACTORY(LogicalOldSort, "old_sort")


}  // namespace scidb
