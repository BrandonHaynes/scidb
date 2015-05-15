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
 * PhysicalRegrid.cpp
 *
 *  Created on: Jul 25, 2011
 *      Author: poliocough@gmail.com
 */

#include "Aggregator.h"

namespace scidb
{

class PhysicalRegrid: public AggregatePartitioningOperator
{
  private:
     vector<uint64_t> _grid;

  public:
     PhysicalRegrid(const string& logicalName, const string& physicalName, const Parameters& parameters, const ArrayDesc& schema):
         AggregatePartitioningOperator(logicalName, physicalName, parameters, schema)
    {
    }

    virtual void initializeOperator(ArrayDesc const& inputSchema)
    {
        AggregatePartitioningOperator::initializeOperator(inputSchema);
        for (size_t i =0; i< inputSchema.getDimensions().size(); i++)
        {
            _grid.push_back (((boost::shared_ptr<OperatorParamPhysicalExpression>&)_parameters[i])->getExpression()->evaluate().getInt64());
        }
    }

    virtual void transformCoordinates(CoordinateCRange inPos,CoordinateRange outPos)
    {
        //"Just tell me where to go!"
        for (size_t i=0, n=inPos.size(); i!=n ; ++i)
        {
            outPos[i] = _schema.getDimensions()[i].getStartMin() + (inPos[i] - _schema.getDimensions()[i].getStartMin())/_grid[i];
        }
    }
};

DECLARE_PHYSICAL_OPERATOR_FACTORY(PhysicalRegrid, "regrid", "physical_regrid")

}  // namespace scidb
