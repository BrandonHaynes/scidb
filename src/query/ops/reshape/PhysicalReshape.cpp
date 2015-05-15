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

#include <query/Operator.h>
#include <array/Metadata.h>
#include <network/NetworkManager.h>
#include "ReshapeArray.h"
#include "ShiftArray.h"

using namespace std;
using namespace boost;

namespace scidb {

class PhysicalReshape: public  PhysicalOperator
{
public:
	PhysicalReshape(const string& logicalName, const string& physicalName, const Parameters& parameters, const ArrayDesc& schema):
	    PhysicalOperator(logicalName, physicalName, parameters, schema)
	{
	}

    virtual bool changesDistribution(std::vector< ArrayDesc> const&) const
    {
        return true;
    }

    virtual ArrayDistribution getOutputDistribution(
            std::vector<ArrayDistribution> const&,
            std::vector<ArrayDesc> const&) const
    {
        return ArrayDistribution(psUndefined);
    }

    virtual bool outputFullChunks(std::vector< ArrayDesc> const&) const
    {
        return false;
    }

    inline bool isSameShape(ArrayDesc const& a1, ArrayDesc const& a2) 
    {
        Dimensions const& d1 = a1.getDimensions();
        Dimensions const& d2 = a2.getDimensions();
        if (d1.size() != d2.size()) { 
            return false;
        }
        for (size_t i = 0, n = d1.size(); i < n; i++) { 
            if (d1[i].getLength() != d2[i].getLength()  || d1[i].getStartMin() != d2[i].getStartMin() || d1[i].getChunkInterval() != d2[i].getChunkInterval())
            {
                return false;
            }
        }
        return true;
    }

    inline bool isShift(ArrayDesc const& a1, ArrayDesc const& a2) 
    {
        Dimensions const& d1 = a1.getDimensions();
        Dimensions const& d2 = a2.getDimensions();
        if (d1.size() != d2.size()) { 
            return false;
        }
        for (size_t i = 0, n = d1.size(); i < n; i++) { 
            if (d1[i].getLength() != d2[i].getLength() || d1[i].getChunkInterval() != d2[i].getChunkInterval())
            {
                return false;
            }
        }
        return true;
    }

    virtual PhysicalBoundaries getOutputBoundaries(const std::vector<PhysicalBoundaries> & inputBoundaries,
                                                   const std::vector< ArrayDesc> & inputSchemas) const
    {
        const Dimensions & oldDims = inputSchemas[0].getDimensions();
        const Dimensions & newDims = _schema.getDimensions();
        return inputBoundaries[0].reshape(oldDims, newDims);
    }

	/***
	 * Reshape is a pipelined operator, hence it executes by returning an iterator-based array to the consumer
	 * that overrides the chunkiterator method.
	 */
	shared_ptr<Array> execute(vector< shared_ptr<Array> >& inputArrays, shared_ptr<Query> query)
    {
		assert(inputArrays.size() == 1);
        shared_ptr< Array> array = inputArrays[0];
        ArrayDesc const& desc = array->getArrayDesc();

        if (isSameShape(_schema, desc))
        {
            return shared_ptr<Array>(new DelegateArray(_schema, array, true));
        }
        if (isShift(_schema, desc))
        {
            return shared_ptr<Array>(new ShiftArray(_schema, array));
        }
        array = ensureRandomAccess(array, query);
        return shared_ptr<Array>(new ReshapeArray(_schema, array));
	 }
};
    
DECLARE_PHYSICAL_OPERATOR_FACTORY(PhysicalReshape, "reshape", "physicalReshape")

}  // namespace scidb
