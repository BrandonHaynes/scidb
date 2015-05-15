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
 * PhysicalApply.cpp
 *
 *  Created on: Apr 20, 2010
 *      Author: Knizhnik
 */

#include "query/Operator.h"
#include "array/Metadata.h"
#include "CrossJoinArray.h"

using namespace std;
using namespace boost;

namespace scidb {

inline OperatorParamDimensionReference* dimref_cast( const shared_ptr<OperatorParam>& ptr )
{
    return dynamic_cast<OperatorParamDimensionReference*>(ptr.get());
}

class PhysicalCrossJoin: public PhysicalOperator
{
public:
    PhysicalCrossJoin(std::string const& logicalName,
                      std::string const& physicalName,
                      Parameters const& parameters,
                      ArrayDesc const& schema)
        : PhysicalOperator(logicalName, physicalName, parameters, schema)
    {}

    virtual PhysicalBoundaries getOutputBoundaries(
            std::vector<PhysicalBoundaries> const& inputBoundaries,
            std::vector< ArrayDesc> const& inputSchemas) const
    {
        if (inputBoundaries[0].isEmpty() || inputBoundaries[1].isEmpty()) {
            return PhysicalBoundaries::createEmpty(_schema.getDimensions().size());
        }

        size_t numLeftDims = inputSchemas[0].getDimensions().size();
        size_t numRightDims = inputSchemas[1].getDimensions().size();

        Coordinates leftStart = inputBoundaries[0].getStartCoords();
        Coordinates rightStart = inputBoundaries[1].getStartCoords();
        Coordinates leftEnd = inputBoundaries[0].getEndCoords();
        Coordinates rightEnd = inputBoundaries[1].getEndCoords();

        Coordinates newStart, newEnd;

        size_t ldi;
        for (ldi = 0; ldi < numLeftDims; ldi++)
        {
            const DimensionDesc &lDim = inputSchemas[0].getDimensions()[ldi];
            size_t pi;
            for (pi = 0; pi < _parameters.size(); pi += 2)
            {
                const string &lJoinDimName = dimref_cast(_parameters[pi])->getObjectName();
                const string &lJoinDimAlias = dimref_cast(_parameters[pi])->getArrayName();
                if (lDim.hasNameAndAlias(lJoinDimName, lJoinDimAlias))
                {
                    const string &rJoinDimName = dimref_cast(_parameters[pi+1])->getObjectName();
                    const string &rJoinDimAlias = dimref_cast(_parameters[pi+1])->getArrayName();
                    for (size_t rdi = 0; rdi < numRightDims; rdi++)
                    {
                        if (inputSchemas[1].getDimensions()[rdi].hasNameAndAlias(rJoinDimName, rJoinDimAlias))
                        {
                            newStart.push_back(leftStart[ldi] < rightStart[rdi] ?  rightStart[rdi] : leftStart[ldi]);
                            newEnd.push_back(leftEnd[ldi] > rightEnd[rdi] ? rightEnd[rdi] : leftEnd[ldi]);
                            break;
                        }
                    }
                    break;
                }
            }

            if (pi>=_parameters.size())
            {
                newStart.push_back(leftStart[ldi]);
                newEnd.push_back(leftEnd[ldi]);
            }
        }

        for(size_t i=0; i<numRightDims; i++)
        {
            const DimensionDesc &dim = inputSchemas[1].getDimensions()[i];
            bool found = false;

            for (size_t pi = 0; pi < _parameters.size(); pi += 2)
            {
                const string &joinDimName = dimref_cast(_parameters[pi+1])->getObjectName();
                const string &joinDimAlias = dimref_cast(_parameters[pi+1])->getArrayName();
                if (dim.hasNameAndAlias(joinDimName, joinDimAlias))
                {
                    found = true;
                    break;
                }
            }

            if (!found)
            {
                newStart.push_back(rightStart[i]);
                newEnd.push_back(rightEnd[i]);
            }
        }
        return PhysicalBoundaries(newStart, newEnd);
    }

    virtual bool changesDistribution(std::vector<ArrayDesc> const&) const
    {
        //TODO[ap]: if ALL RIGHT SIDE non-join dimensions are one chunk, then true
        return true;
    }

    virtual ArrayDistribution getOutputDistribution(
            std::vector<ArrayDistribution> const& inputDistributions,
            std::vector< ArrayDesc> const& inputSchemas) const
    {
        return ArrayDistribution(psUndefined);
    }

    /**
     * Ensure input array chunk sizes and overlaps match along join-dimension pairs.
     */
    virtual void requiresRepart(vector<ArrayDesc> const& inputSchemas,
                                vector<ArrayDesc const*>& repartPtrs) const
    {
        assert(inputSchemas.size() == 2);
        assert(repartPtrs.size() == 2);

        // We don't expect to be called twice, but that may change later on: wipe any previous result.
        _repartSchemas.clear();

        Dimensions const& leftDims = inputSchemas[0].getDimensions();
        Dimensions rightDims = inputSchemas[1].getDimensions();

        // For each pair of join dimensions, make sure rightArray's chunks and overlaps
        // match... else we need to build a repartSchema to make them match.
        //
        bool needRepart = false;
        for (size_t p = 0, np = _parameters.size(); p < np; p += 2)
        {
            const OperatorParamDimensionReference *lDim = dimref_cast(_parameters[p]);
            const OperatorParamDimensionReference *rDim = dimref_cast(_parameters[p+1]);
            ssize_t l = inputSchemas[0].findDimension(lDim->getObjectName(), lDim->getArrayName());
            ssize_t r = inputSchemas[1].findDimension(rDim->getObjectName(), rDim->getArrayName());
            assert(l >= 0); // was already checked in Logical...::inferSchema()
            assert(r >= 0); // ditto

            if (rightDims[r].getChunkInterval() != leftDims[l].getChunkInterval()) {
                rightDims[r].setChunkInterval(leftDims[l].getChunkInterval());
                needRepart = true;
            }
            if (rightDims[r].getChunkOverlap() != leftDims[l].getChunkOverlap()) {
                rightDims[r].setChunkOverlap(min(leftDims[l].getChunkOverlap(),
                                                 rightDims[r].getChunkOverlap()));
                needRepart = true;
            }
        }

        if (needRepart) {
            // Copy of right array schema, with newly tweaked dimensions.
            _repartSchemas.push_back(make_shared<ArrayDesc>(inputSchemas[1]));
            _repartSchemas.back()->setDimensions(rightDims);

            // Leave left array alone, repartition right array.
            repartPtrs[0] = 0;
            repartPtrs[1] = _repartSchemas.back().get();
        } else {
            // The preferred way of saying "no repartitioning needed".
            repartPtrs.clear();
        }
    }

    /***
     * Join is a pipelined operator, hence it executes by returning an iterator-based array to the consumer
     * that overrides the chunkiterator method.
     */
    boost::shared_ptr<Array> execute(
            vector< boost::shared_ptr<Array> >& inputArrays,
            boost::shared_ptr<Query> query)
    {
        assert(inputArrays.size() == 2);

        shared_ptr<Array> input1 = inputArrays[1];
        if (query->getInstancesCount() == 1 )
        {
            input1 = ensureRandomAccess(input1, query);
        }

        size_t lDimsSize = inputArrays[0]->getArrayDesc().getDimensions().size();
        size_t rDimsSize = inputArrays[1]->getArrayDesc().getDimensions().size();

        vector<int> ljd(lDimsSize, -1);
        vector<int> rjd(rDimsSize, -1);

        for (size_t p = 0, np = _parameters.size(); p < np; p += 2)
        {
            const shared_ptr<OperatorParamDimensionReference> &lDim =
                (shared_ptr<OperatorParamDimensionReference>&)_parameters[p];
            const shared_ptr<OperatorParamDimensionReference> &rDim =
                (shared_ptr<OperatorParamDimensionReference>&)_parameters[p+1];

            rjd[rDim->getObjectNo()] = lDim->getObjectNo();
        }

        size_t k=0;
        for (size_t i = 0; i < rjd.size(); i++)
        {
            if (rjd[i] != -1)
            {
                ljd [ rjd[i] ] = k;
                k++;
            }
        }
        shared_ptr<Array> replicated = redistributeToRandomAccess(input1, query, psReplication,
                                                                  ALL_INSTANCE_MASK,
                                                                  shared_ptr<DistributionMapper>(),
                                                                  0,
                                                                  shared_ptr<PartitioningSchemaData>());
        return make_shared<CrossJoinArray>(_schema, inputArrays[0],
                                           replicated,
                                           ljd, rjd);
    }
};

DECLARE_PHYSICAL_OPERATOR_FACTORY(PhysicalCrossJoin, "cross_join", "physicalCrossJoin")

}  // namespace scidb
