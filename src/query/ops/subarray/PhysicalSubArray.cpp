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
 * PhysicalSubArray.cpp
 *
 *  Created on: May 20, 2010
 *      Author: knizhnik@garret.ru
 */

#include <query/Operator.h>
#include <array/Metadata.h>
#include <array/Array.h>
#include <network/NetworkManager.h>
#include "SubArray.h"

namespace scidb
{

class PhysicalSubArray: public  PhysicalOperator
{
public:
    PhysicalSubArray(const std::string& logicalName, const std::string& physicalName, const Parameters& parameters, const ArrayDesc& schema):
             PhysicalOperator(logicalName, physicalName, parameters, schema)
    {}

    //Return the starting coordinates of the subarray window, relative to the input schema
    inline Coordinates getWindowStart(ArrayDesc const& inputSchema) const
    {
        Dimensions const& dims = inputSchema.getDimensions();
        size_t nDims = dims.size();
        Coordinates result (nDims);
        for (size_t i = 0; i < nDims; i++)
        {
            Value const& low = ((boost::shared_ptr<OperatorParamPhysicalExpression>&)_parameters[i])->getExpression()->evaluate();
            if ( low.isNull() || low.getInt64() < dims[i].getStartMin())
            {
                result[i] = dims[i].getStartMin();
            }
            else
            {
                result[i] = low.getInt64();
            }
        }
        return result;
    }

    //Return the ending coordinates of the subarray window, relative to the input schema
    inline Coordinates getWindowEnd(ArrayDesc const& inputSchema) const
    {
        Dimensions const& dims = inputSchema.getDimensions();
        size_t nDims = dims.size();
        Coordinates result (nDims);
        for (size_t i  = 0; i < nDims; i++)
        {
            Value const& high = ((boost::shared_ptr<OperatorParamPhysicalExpression>&)_parameters[i + nDims])->getExpression()->evaluate();
            if (high.isNull() || high.getInt64() > dims[i].getEndMax())
            {
                result[i] = dims[i].getEndMax();
            }
            else
            {
                result[i] = high.getInt64();
            }
        }
        return result;
    }

    /**
     * @see PhysicalOperator::changesDistribution
     */
    virtual bool changesDistribution(std::vector< ArrayDesc> const& inputSchemas) const
    {
        //If the entire schema is inside the window - we don't change the distribution.
        //Some packages (ahem) like to use subarray(A, null, null, null, null) often to recenter the array at 0,
        //and that does not need an SG.
        ArrayDesc const& inputSchema = inputSchemas[0];
        Coordinates const windowStart = getWindowStart(inputSchema);
        Coordinates const windowEnd = getWindowEnd(inputSchema);
        size_t const nDims = windowStart.size();
        Dimensions const& dims = inputSchema.getDimensions();
        for (size_t i =0; i<nDims; ++i)
        {
            DimensionDesc const& dim = dims[i];
            if( windowStart[i] > dim.getStartMin() || windowEnd[i] < dim.getEndMax())
            {
                return true;
            }
        }
        return false;
    }

    /**
     * @see PhysicalOperator::outputFullChunks
     */
    virtual bool outputFullChunks(std::vector< ArrayDesc> const& inputSchemas) const
    {
        ArrayDesc const& input = inputSchemas[0];
        Coordinates windowStart = getWindowStart(input);
        Coordinates windowEnd = getWindowEnd(input);
        if ( input.coordsAreAtChunkStart(windowStart) &&
             input.coordsAreAtChunkEnd(windowEnd) )
        {
            return true;
        }
        return false;
    }

    //return the delta between the subarray window origin and the input array origin
    virtual DimensionVector getOffsetVector(const std::vector< ArrayDesc> & inputSchemas) const
    {
        ArrayDesc const& desc = inputSchemas[0];
        Dimensions const& inputDimensions = desc.getDimensions();
        size_t numCoords = inputDimensions.size();
        DimensionVector result(numCoords);
        Coordinates windowStart = getWindowStart(inputSchemas[0]);

        for (size_t i = 0; i < numCoords; i++)
        {
            Coordinate arrayStartCoord = (inputDimensions[i]).getStartMin();
            result[i] = windowStart[i]-arrayStartCoord;
        }

        return result;
    }

    /**
     * @see PhysicalOperator::getOutputDistribution
     */
    virtual ArrayDistribution getOutputDistribution(const std::vector<ArrayDistribution> & inputDistributions,
                                                 const std::vector< ArrayDesc> & inputSchemas) const
    {
        if (!changesDistribution(inputSchemas))
        {
            return inputDistributions[0];
        }
        DimensionVector offset = getOffsetVector(inputSchemas);
        boost::shared_ptr<DistributionMapper> distMapper;
        ArrayDistribution inputDistro = inputDistributions[0];
        if (inputDistro.isUndefined() ||
            inputDistro.getPartitioningSchema() == psScaLAPACK ||
            inputDistro.getPartitioningSchema() == psGroupby)
        {
            return ArrayDistribution(psUndefined);
        }
        else
        {
            boost::shared_ptr<DistributionMapper> inputMapper = inputDistro.getMapper();
            if (!offset.isEmpty())
            {
                distMapper = DistributionMapper::createOffsetMapper(offset) ->combine(inputMapper);
            }
            else
            {
                distMapper = inputMapper;
            }
            return ArrayDistribution(inputDistro.getPartitioningSchema(), distMapper);
        }
    }

    /**
     * @see PhysicalOperator::getOutputBoundaries
     */
    virtual PhysicalBoundaries getOutputBoundaries(const std::vector<PhysicalBoundaries> & inputBoundaries,
                                                   const std::vector< ArrayDesc> & inputSchemas) const
    {
        size_t nDims = _schema.getDimensions().size();
        PhysicalBoundaries window(getWindowStart(inputSchemas[0]),
                                  getWindowEnd(inputSchemas[0]));
        PhysicalBoundaries result = inputBoundaries[0].intersectWith(window);

        if (result.isEmpty())
        {
            return PhysicalBoundaries::createEmpty(nDims);
        }

        Coordinates newStart, newEnd;
        for (size_t i =0; i < nDims; i++)
        {
            newStart.push_back(0);
            newEnd.push_back( result.getEndCoords()[i] - result.getStartCoords()[i] );
        }

        return PhysicalBoundaries(newStart, newEnd, result.getDensity());
    }

    /**
     * @see PhysicalOperator::execute
     */
    boost::shared_ptr< Array> execute(std::vector< boost::shared_ptr< Array> >& inputArrays,
                                      boost::shared_ptr< Query> query)
    {
        assert(inputArrays.size() == 1);
        shared_ptr<Array> input = ensureRandomAccess(inputArrays[0], query);

        ArrayDesc const& desc = input->getArrayDesc();
        Dimensions const& srcDims = desc.getDimensions();
        size_t nDims = srcDims.size();
        
        /***
         * Fetch and calculate the subarray window
         */
        Coordinates lowPos = getWindowStart(desc);
        Coordinates highPos = getWindowEnd(desc);
        for(size_t i=0; i<nDims; i++)
        {
            if (lowPos[i] > highPos[i]) {
                return boost::shared_ptr<Array>(new MemArray(_schema,query));
            }
        }
        /***
         * Create an iterator-based array implementation for the operator
         */
        boost::shared_ptr< Array> arr = boost::shared_ptr< Array>( new SubArray(_schema, lowPos, highPos, input, query));
        return arr;
    }
};

DECLARE_PHYSICAL_OPERATOR_FACTORY(PhysicalSubArray, "subarray", "physicalSubArray")

}  // namespace scidb
