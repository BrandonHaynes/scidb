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
 * PhysicalReverse.cpp
 *
 *  Created on: Aug 6, 2010
 *      Author: Knizhnik
 */

#include <query/Operator.h>
#include <array/Metadata.h>
#include <array/Array.h>
#include "ReverseArray.h"

using namespace boost;
using namespace std;

namespace scidb
{

class PhysicalReverse: public  PhysicalOperator
{
public:
	PhysicalReverse(const string& logicalName, const string& physicalName, const PhysicalOperator::Parameters& parameters, const ArrayDesc& schema):
	     PhysicalOperator(logicalName, physicalName, parameters, schema)
	{
	}

    virtual bool changesDistribution(std::vector<ArrayDesc> const&) const
    {
        //TODO: OPTAPI TESTME, MAPPER?
        return true;
    }

    virtual ArrayDistribution getOutputDistribution(
            std::vector<ArrayDistribution> const & inputDistributions,
            std::vector< ArrayDesc> const& inputSchemas) const
    {
        if (changesDistribution(inputSchemas)) {
            return ArrayDistribution(psUndefined);
        } else {
            return inputDistributions[0];
        }
    }

    virtual PhysicalBoundaries getOutputBoundaries(
            std::vector<PhysicalBoundaries> const& inputBoundaries,
            std::vector< ArrayDesc> const& inputSchemas) const
    {
        if (inputBoundaries[0].isEmpty())
        {
            return PhysicalBoundaries::createEmpty(_schema.getDimensions().size());
        }

        Coordinates newStart, newEnd;

        Coordinates inStart = inputBoundaries[0].getStartCoords();
        Coordinates inEnd = inputBoundaries[0].getEndCoords();

        Dimensions dims = inputSchemas[0].getDimensions();

        for (size_t i = 0; i < dims.size(); i++)
        {
            newStart.push_back(dims[i].getStartMin() + dims[i].getEndMax() - inEnd[i]);
            newEnd.push_back(dims[i].getStartMin() + dims[i].getEndMax() - inStart[i]);
        }

        return PhysicalBoundaries(newStart, newEnd);
    }

	/***
	 * Reverse is a pipelined operator, hence it executes by returning an iterator-based array to the consumer
	 * that overrides the chunkiterator method.
	 */
	boost::shared_ptr<Array> execute(vector< boost::shared_ptr<Array> >& inputArrays, boost::shared_ptr<Query> query)
    {
		assert(inputArrays.size() == 1);
		assert(_parameters.size() == 0);
		shared_ptr<Array> inputArray = ensureRandomAccess(inputArrays[0], query);

		return make_shared<ReverseArray>(_schema, inputArray);
	 }
};
    
DECLARE_PHYSICAL_OPERATOR_FACTORY(PhysicalReverse, "reverse", "physicalReverse")

}  // namespace scidb
