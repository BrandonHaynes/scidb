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
 * PhysicalCrossBetween.cpp
 *
 *  Created on: August 15, 2014
 *  Author: Donghui Zhang
 */

#include <query/Operator.h>
#include <array/Metadata.h>
#include <array/Array.h>
#include "BetweenArray.h"
#include <util/SchemaUtils.h>

namespace scidb {

class PhysicalCrossBetween: public  PhysicalOperator
{
public:
    PhysicalCrossBetween(const std::string& logicalName, const std::string& physicalName, const Parameters& parameters, const ArrayDesc& schema):
         PhysicalOperator(logicalName, physicalName, parameters, schema)
    {
    }

    virtual PhysicalBoundaries getOutputBoundaries(const std::vector<PhysicalBoundaries> & inputBoundaries,
                                                  const std::vector< ArrayDesc> & inputSchemas) const
    {
       return inputBoundaries[0];
    }

    /***
     * CrossBetween is a pipelined operator, hence it executes by returning an iterator-based array to the consumer
     * that overrides the chunkIterator method.
     */
    boost::shared_ptr< Array> execute(std::vector< boost::shared_ptr< Array> >& inputArrays,
                                      boost::shared_ptr<Query> query)
    {
        // Ensure inputArray supports random access, and rangesArray is replicated.
        assert(inputArrays.size() == 2);
        shared_ptr<Array> inputArray = ensureRandomAccess(inputArrays[0], query);
        shared_ptr<Array> rangesArray = redistributeToRandomAccess(inputArrays[1], query, psReplication,
                                                                   ALL_INSTANCE_MASK,
                                                                   shared_ptr<DistributionMapper>(),
                                                                   0,
                                                                   shared_ptr<PartitioningSchemaData>());

        // Some variables.
        SchemaUtils schemaUtilsInputArray(inputArray);
        SchemaUtils schemaUtilsRangesArray(rangesArray);
        size_t nDims = schemaUtilsInputArray._dims.size();
        assert(nDims*2 == schemaUtilsRangesArray._attrsWithoutET.size());

        // Scan all attributes of the rangesArray simultaneously, and fill in spatialRanges.
        // Set up a MultiConstIterators to process the array iterators simultaneously.
        SpatialRangesPtr spatialRangesPtr = make_shared<SpatialRanges>(nDims);

        vector<shared_ptr<ConstIterator> > rangesArrayIters(nDims*2);
        for (size_t i=0; i<nDims*2; ++i) {
            rangesArrayIters[i] = rangesArray->getConstIterator(i);
        }
        MultiConstIterators multiItersRangesArray(rangesArrayIters);
        while (!multiItersRangesArray.end()) {
            // Set up a MultiConstIterators to process the chunk iterators simultaneously.
            vector<shared_ptr<ConstIterator> > rangesChunkIters(nDims*2);
            for (size_t i=0; i<nDims*2; ++i) {
                rangesChunkIters[i] = dynamic_pointer_cast<ConstArrayIterator>(rangesArrayIters[i])->getChunk().getConstIterator();
            }
            MultiConstIterators multiItersRangesChunk(rangesChunkIters);
            while (!multiItersRangesChunk.end()) {
                SpatialRange spatialRange(nDims);
                for (size_t i=0; i<nDims; ++i) {
                    const Value& v = dynamic_pointer_cast<ConstChunkIterator>(rangesChunkIters[i])->getItem();
                    spatialRange._low[i] = v.getInt64();
                }
                for (size_t i=nDims; i<nDims*2; ++i) {
                    const Value& v = dynamic_pointer_cast<ConstChunkIterator>(rangesChunkIters[i])->getItem();
                    spatialRange._high[i-nDims] = v.getInt64();
                }
                if (spatialRange.valid()) {
                    spatialRangesPtr->_ranges.push_back(spatialRange);
                }
                ++ multiItersRangesChunk;
            }
            ++ multiItersRangesArray;
        }

        // Return a CrossBetweenArray.
        return boost::shared_ptr< Array>(make_shared<BetweenArray>(_schema, spatialRangesPtr, inputArray));
   }
};

DECLARE_PHYSICAL_OPERATOR_FACTORY(PhysicalCrossBetween, "cross_between", "physicalCrossBetween")

}  // namespace scidb
