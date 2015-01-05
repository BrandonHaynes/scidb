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

class PhysicalCrossJoin: public PhysicalOperator
{
public:
    PhysicalCrossJoin(std::string const& logicalName,
                      std::string const& physicalName,
                      Parameters const& parameters,
                      ArrayDesc const& schema):
	    PhysicalOperator(logicalName, physicalName, parameters, schema)
	{
	}

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
                const string &lJoinDimName = ((boost::shared_ptr<OperatorParamDimensionReference>&)_parameters[pi])->getObjectName();
                const string &lJoinDimAlias = ((boost::shared_ptr<OperatorParamDimensionReference>&)_parameters[pi])->getArrayName();
                if (lDim.hasNameAndAlias(lJoinDimName, lJoinDimAlias))
                {
                    const string &rJoinDimName = ((boost::shared_ptr<OperatorParamDimensionReference>&)_parameters[pi+1])->getObjectName();
                    const string &rJoinDimAlias = ((boost::shared_ptr<OperatorParamDimensionReference>&)_parameters[pi+1])->getArrayName();
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
                const string &joinDimName = ((boost::shared_ptr<OperatorParamDimensionReference>&)_parameters[pi+1])->getObjectName();
                const string &joinDimAlias = ((boost::shared_ptr<OperatorParamDimensionReference>&)_parameters[pi+1])->getArrayName();
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
            const shared_ptr<OperatorParamDimensionReference> &lDim = (shared_ptr<OperatorParamDimensionReference>&)_parameters[p];
            const shared_ptr<OperatorParamDimensionReference> &rDim = (shared_ptr<OperatorParamDimensionReference>&)_parameters[p+1];

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
        
        return boost::shared_ptr<Array>(new CrossJoinArray(_schema, inputArrays[0], redistribute(input1, query, psReplication), ljd, rjd));
    }
};
    
DECLARE_PHYSICAL_OPERATOR_FACTORY(PhysicalCrossJoin, "cross_join", "physicalCrossJoin")

}  // namespace scidb
