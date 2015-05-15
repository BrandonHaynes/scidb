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
 * PhysicalAggregate.cpp
 *
 *  Created on: Jul 25, 2011
 *      Author: poliocough@gmail.com
 */

#include "Aggregator.h"

namespace scidb
{

class PhysicalAggregate: public AggregatePartitioningOperator
{
  private:
     DimensionGrouping     _grouping;

  public:
    PhysicalAggregate(const string& logicalName,
                      const string& physicalName,
                      const Parameters& parameters,
                      const ArrayDesc& schema)
        : AggregatePartitioningOperator(logicalName, physicalName, parameters, schema)
    { }

    virtual void initializeOperator(ArrayDesc const& inputSchema)
    {
        AggregatePartitioningOperator::initializeOperator(inputSchema);
        Dimensions const& inputDims = inputSchema.getDimensions();
        Dimensions groupBy;
        for (size_t i =0, n = _parameters.size(); i<n; i++)
        {
            if (_parameters[i]->getParamType() == PARAM_DIMENSION_REF)
            {
                boost::shared_ptr<OperatorParamReference> const& reference =
                                                (boost::shared_ptr<OperatorParamReference> const&) _parameters[i];
                string const& dimName = reference->getObjectName();
                string const& dimAlias = reference->getArrayName();
                for (size_t j = 0; j < inputDims.size(); j++)
                {
                    if (inputDims[j].hasNameAndAlias(dimName, dimAlias)) {
                        groupBy.push_back(inputDims[j]);
                        break;
                    }
                }
            }
        }
        _grouping = DimensionGrouping(inputSchema.getDimensions(), groupBy);
    }

    virtual void transformCoordinates(CoordinateCRange inPos,CoordinateRange outPos)
    {
        // Now that #3709 is fixed, we can safely assert that...
        assert(!outPos.empty());
        assert(outPos.size() <= inPos.size());

        _grouping.reduceToGroup(inPos,outPos);
    }
};

DECLARE_PHYSICAL_OPERATOR_FACTORY(PhysicalAggregate, "aggregate", "physical_aggregate")

}  // namespace scidb
