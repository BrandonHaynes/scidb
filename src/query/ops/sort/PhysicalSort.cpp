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
 * PhysicalSort.cpp
 *
 *  Created on: Oct 23, 2014
 *      Author: Donghui Zhang
 */

#include <vector>

#include <query/Operator.h>
#include <array/Metadata.h>
#include <array/TupleArray.h>
#include <network/NetworkManager.h>
#include <system/Config.h>
#include <array/MergeSortArray.h>
#include <array/SortArray.h>
#include <util/arena/Map.h>
#include <util/Arena.h>
#include <array/ProjectArray.h>
#include <array/ParallelAccumulatorArray.h>
#include <util/Timing.h>

#include "DistributedSort.h"

using namespace std;
using namespace boost;

namespace scidb {

static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.query.ops.sort"));

class PhysicalSort: public PhysicalOperator
{
public:
    PhysicalSort(const string& logicalName, const string& physicalName, const Parameters& parameters, const ArrayDesc& schema):
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

    /**
     * From the user-provided parameters to the sort() operator, generate SortingAttributeInfos.
     * @param[out] an initially empty vector to receive the SortingAttributeInfos.
     */
    void generateSortingAttributeInfos(SortingAttributeInfos& sortingAttributeInfos)
    {
        assert(sortingAttributeInfos.empty());

        Attributes const& attrs = _schema.getAttributes();
        for (size_t i = 0, n=_parameters.size(); i < n; i++)
        {
            if(_parameters[i]->getParamType() != PARAM_ATTRIBUTE_REF)
            {
                continue;
            }
            shared_ptr<OperatorParamAttributeReference> sortColumn = ((shared_ptr<OperatorParamAttributeReference>&)_parameters[i]);
            SortingAttributeInfo k;
            k.columnNo = sortColumn->getObjectNo();
            k.ascent = sortColumn->getSortAscent();
            if ((size_t)k.columnNo >= attrs.size())
                throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_OP_SORT_ERROR2);
            sortingAttributeInfos.push_back(k);
        }

        // If the users did not provide any sorting attribute, use the first attribute.
        if (sortingAttributeInfos.empty())
        {
            //No attribute is specified... so let's sort by first attribute ascending
            SortingAttributeInfo k;
            k.columnNo =0;
            k.ascent=true;
            sortingAttributeInfos.push_back(k);
        }

        // Add the chunk & cell positions.
        SortingAttributeInfo k;
        const bool excludingEmptyBitmap = true;
        k.columnNo = _schema.getAttributes(excludingEmptyBitmap).size();  // The attribute at nAttr is chunk_pos.
        k.ascent = true;
        sortingAttributeInfos.push_back(k);

        k.columnNo ++; // the next one is cell_pos.
        sortingAttributeInfos.push_back(k);
    }

    /**
     * @see PhysicalOperator::changesDistribution
     */
    virtual bool changesDistribution(std::vector<ArrayDesc> const& inputSchemas) const
    {
        return true;
    }

    /**
     * @see PhysicalOperator::getOutputDistribution
     */
    virtual ArrayDistribution getOutputDistribution(std::vector<ArrayDistribution> const&, std::vector<ArrayDesc> const&) const
    {
        return ArrayDistribution(psUndefined);
    }

    /***
     * Sort operates by using the generic array sort utility provided by SortArray
     */
    shared_ptr< Array> execute(vector< shared_ptr< Array> >& inputArrays,
                                      shared_ptr<Query> query)
    {
        assert(inputArrays.size() == 1);
        ElapsedMilliSeconds timing;

        //
        // Generate the SortingAttributeInfos.
        //
        SortingAttributeInfos sortingAttributeInfos;
        generateSortingAttributeInfos(sortingAttributeInfos);

        //
        // LocalSorting.
        //
        const bool preservePositions = true;
        SortArray sorter(inputArrays[0]->getArrayDesc(), _arena, preservePositions, _schema.getDimensions()[0].getChunkInterval());
        ArrayDesc const& expandedSchema = sorter.getOutputArrayDesc();
        shared_ptr<TupleComparator> tcomp(make_shared<TupleComparator>(sortingAttributeInfos, expandedSchema));
        shared_ptr<MemArray> sortedLocalData = sorter.getSortedArray(inputArrays[0], query, tcomp);

        timing.logTiming(logger, "[sort] Sorting local data");

        // Unless there is a single instance, do a distributed sort.
        // Note that sortedLocalData and expandedSchema have additional fields for the chunk/cell positions.
        // Also note that sortedLocalData->getArrayDesc() differs from expandedSchema, in that:
        //   - expandedSchema._dimensions[0]._endMax = INT_MAX, but
        //   - the schema in sortedLocalData has _endMax which may be the actual number of local records minus 1.
        shared_ptr<MemArray> distributedSortResult = sortedLocalData;
        if (query->getInstancesCount() > 1) {
            DistributedSort ds(query, sortedLocalData, expandedSchema, _arena, sortingAttributeInfos, timing);
            distributedSortResult = ds.sort();
        }

        // Project off the chunk_pos and cell_pos attributes.
        const bool excludeEmptyBitmap = true;
        size_t nAttrs = _schema.getAttributes(excludeEmptyBitmap).size();
        vector<AttributeID> projection(nAttrs+1);
        for (AttributeID i=0; i<nAttrs; ++i) {
            projection[i] = i;
        }
        projection[nAttrs] = nAttrs+2; // this is the empty bitmap attribute.
        return make_shared<ProjectArray>(_schema, distributedSortResult, projection);
    }
};

DECLARE_PHYSICAL_OPERATOR_FACTORY(PhysicalSort, "sort", "physicalSort")

}  // namespace scidb
