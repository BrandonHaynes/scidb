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
 * PhysicalThin.cpp
 *
 *  Created on: Apr 11, 2010
 *      Author: Knizhnik
 */

#include <query/Operator.h>
#include <array/Metadata.h>
#include <array/Array.h>
#include "ThinArray.h"

using namespace boost;
using namespace std;

namespace scidb
{

class PhysicalThin: public  PhysicalOperator
{
  public:
	PhysicalThin(const string& logicalName, const string& physicalName, const Parameters& parameters, const ArrayDesc& schema):
	     PhysicalOperator(logicalName, physicalName, parameters, schema)
	{
	}

    virtual bool changesDistribution(const std::vector<ArrayDesc>&) const
    {
        //TODO:OPTAPI testme
        return false;
    }

    virtual PhysicalBoundaries getOutputBoundaries(const std::vector<PhysicalBoundaries> & inputBoundaries,
                                                       const std::vector< ArrayDesc> & inputSchemas) const
    {
        if (inputBoundaries[0].isEmpty())
        {
            return PhysicalBoundaries::createEmpty(_schema.getDimensions().size());
        }

        const Dimensions& inDims = inputSchemas[0].getDimensions();

        Coordinates outStart, outEnd;
        for (size_t i =0; i<inDims.size(); i++)
        {
            Coordinate from = ((boost::shared_ptr<OperatorParamPhysicalExpression>&)_parameters[i*2])->getExpression()->evaluate().getInt64();
            Coordinate step = ((boost::shared_ptr<OperatorParamPhysicalExpression>&)_parameters[i*2+1])->getExpression()->evaluate().getInt64();
            Coordinate last = computeLastCoordinate(inDims[i].getCurrLength(),
                                                    inDims[i].getStartMin(),
                                                    from, step);
            outStart.push_back(0);
            outEnd.push_back(last);
        }

        return PhysicalBoundaries(outStart, outEnd, inputBoundaries[0].getDensity());
    }

    //TODO:OPTAPI check negative case
    /***
	 * Thin is a pipelined operator, hence it executes by returning an iterator-based array to the consumer
	 * that overrides the chunkiterator method.
	 */
	boost::shared_ptr<Array> execute(vector< boost::shared_ptr<Array> >& inputArrays, boost::shared_ptr<Query> query)
    {
		assert(inputArrays.size() == 1);
        boost::shared_ptr<Array> input = inputArrays[0];
        Dimensions const& dstDims = _schema.getDimensions();
        size_t nDims = dstDims.size();        
		assert(_parameters.size() == nDims*2);
        Coordinates from(nDims);        
        Coordinates step(nDims);
        for (size_t i = 0; i < nDims; i++) { 
            from[i] = ((boost::shared_ptr<OperatorParamPhysicalExpression>&)_parameters[i*2])->getExpression()->evaluate().getInt64();
            step[i] = ((boost::shared_ptr<OperatorParamPhysicalExpression>&)_parameters[i*2+1])->getExpression()->evaluate().getInt64();
        }
		return boost::shared_ptr<Array>(new ThinArray(_schema, input, from, step));
	 }
};
    
DECLARE_PHYSICAL_OPERATOR_FACTORY(PhysicalThin, "thin", "physicalThin")

}  // namespace scidb
