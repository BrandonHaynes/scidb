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
 * PhysicalOldSort.cpp
 *
 *  Created on: May 6, 2010
 *      Author: Knizhnik
 *      Author: poliocough@gmail.com
 */

#include <vector>

#include "query/Operator.h"
#include "array/Metadata.h"
#include "array/TupleArray.h"
#include "network/NetworkManager.h"
#include "system/Config.h"
#include "array/MergeSortArray.h"
#include "array/SortArray.h"
#include "array/ParallelAccumulatorArray.h"

using namespace std;
using namespace boost;

namespace scidb {

static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.query.ops.sort"));

class PhysicalOldSort: public PhysicalOperator
{
public:
    PhysicalOldSort(const string& logicalName, const string& physicalName, const Parameters& parameters, const ArrayDesc& schema):
        PhysicalOperator(logicalName, physicalName, parameters, schema)
	{
	}

    virtual PhysicalBoundaries getOutputBoundaries(const std::vector<PhysicalBoundaries> & inputBoundaries,
                                                   const std::vector< ArrayDesc> & inputSchemas) const
    {
        uint64_t numCells = inputBoundaries[0].getNumCells();
        if (numCells == 0)
        {
            return PhysicalBoundaries::createEmpty(1);
        }

        Coordinates start(1);
        start[0] = _schema.getDimensions()[0].getStartMin();
        Coordinates end(1);
        end[0] = _schema.getDimensions()[0].getStartMin() + numCells -1 ;
        return PhysicalBoundaries(start,end);
    }

    /***
     * Sort operates by using the generic array sort utility provided by SortArray
     */
    boost::shared_ptr< Array> execute(vector< boost::shared_ptr< Array> >& inputArrays,
                                      boost::shared_ptr<Query> query)
    {
        assert(inputArrays.size() == 1);
        SortingAttributeInfos sortingAttributeInfos;
        Attributes const& attrs = _schema.getAttributes();
        for (size_t i = 0, n=_parameters.size(); i < n; i++)
        {
            if(_parameters[i]->getParamType() != PARAM_ATTRIBUTE_REF)
            {
                continue;
            }
            shared_ptr<OperatorParamAttributeReference> sortColumn = ((boost::shared_ptr<OperatorParamAttributeReference>&)_parameters[i]);
            SortingAttributeInfo k;
            k.columnNo = sortColumn->getObjectNo();
            k.ascent = sortColumn->getSortAscent();
            if ((size_t)k.columnNo >= attrs.size())
                throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_OP_SORT_ERROR2);
            sortingAttributeInfos.push_back(k);
        }

        if(sortingAttributeInfos.size()==0)
        {
            //No attribute is specified... so let's sort by first attribute ascending
            SortingAttributeInfo k;
            k.columnNo =0;
            k.ascent=true;
            sortingAttributeInfos.push_back(k);
        }

        if ( query->getInstancesCount() > 1) {
            // Prepare context for second phase
            SortContext* ctx = new SortContext();
            ctx->_sortingAttributeInfos = sortingAttributeInfos;
            query->userDefinedContext = ctx;
        }

        const bool preservePosition = false;
        SortArray sorter(_schema, _arena, preservePosition);
        shared_ptr<TupleComparator> tcomp(boost::make_shared<TupleComparator>(sortingAttributeInfos, _schema));

        shared_ptr<Array> ret = sorter.getSortedArray(inputArrays[0], query, tcomp);

        return ret;
    }
};

DECLARE_PHYSICAL_OPERATOR_FACTORY(PhysicalOldSort, "old_sort", "physicalOldSort")

}  // namespace scidb
