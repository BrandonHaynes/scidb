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
 * PhysicalXgrid.cpp
 *
 *  Created on: Apr 11, 2010
 *      Author: Knizhnik
 */

#include "query/Operator.h"
#include "array/Metadata.h"
#include "array/Array.h"
#include "query/ops/xgrid/XgridArray.h"


namespace scidb {

using namespace boost;
using namespace std;

class PhysicalXgrid : public  PhysicalOperator
{
  public:
    PhysicalXgrid(std::string const& logicalName,
                  std::string const& physicalName,
                  Parameters const& parameters,
                  ArrayDesc const& schema):
        PhysicalOperator(logicalName, physicalName, parameters, schema)
    {
    }

    virtual bool changesDistribution(std::vector<ArrayDesc> const&) const
    {
        //TODO:OPTAPI testme
        return false;
    }

    virtual PhysicalBoundaries getOutputBoundaries(
            std::vector<PhysicalBoundaries> const& inputBoundaries,
            std::vector< ArrayDesc> const& inputSchemas) const
    {
        if (inputBoundaries[0].isEmpty()) {
            return PhysicalBoundaries::createEmpty(
                        _schema.getDimensions().size());
        }

        const Coordinates& inStart = inputBoundaries[0].getStartCoords();
        const Coordinates& inEnd = inputBoundaries[0].getEndCoords();
        const Dimensions& inDims = inputSchemas[0].getDimensions();

        Coordinates outStart, outEnd;
        for (size_t i =0; i<inDims.size(); i++)
        {
            int32_t grid = ((boost::shared_ptr<OperatorParamPhysicalExpression>&)_parameters[i])->getExpression()->evaluate().getInt32();
            outStart.push_back( inDims[i].getStartMin() + grid * (inStart[i] - inDims[i].getStartMin()) );
            outEnd.push_back( inDims[i].getStartMin() + grid * (inEnd[i] - inDims[i].getStartMin()) + grid - 1 );
        }

        return PhysicalBoundaries(outStart, outEnd, inputBoundaries[0].getDensity());
    }

    //TODO:OPTAPI check negative case
    /***
	 * Xgrid is a pipelined operator, hence it executes by returning an iterator-based array to the consumer
	 * that overrides the chunkiterator method.
	 */
    boost::shared_ptr<Array> execute(
            vector< boost::shared_ptr<Array> >& inputArrays,
            boost::shared_ptr<Query> query)
    {
		assert(inputArrays.size() == 1);
		assert(_parameters.size() == _schema.getDimensions().size());
		return boost::shared_ptr<Array>(new XgridArray(_schema, inputArrays[0]));
	 }
};
    
DECLARE_PHYSICAL_OPERATOR_FACTORY(PhysicalXgrid, "xgrid", "physicalXgrid")

}  // namespace scidb
