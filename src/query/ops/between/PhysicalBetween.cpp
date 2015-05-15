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
 * PhysicalBetween.cpp
 *
 *  Created on: May 20, 2010
 *      Author: knizhnik@garret.ru
 */

#include <query/Operator.h>
#include <array/Metadata.h>
#include <array/Array.h>
#include "BetweenArray.h"

namespace scidb {

class PhysicalBetween: public  PhysicalOperator
{
public:
    PhysicalBetween(const std::string& logicalName, const std::string& physicalName, const Parameters& parameters, const ArrayDesc& schema):
         PhysicalOperator(logicalName, physicalName, parameters, schema)
    {
    }

    Coordinates getWindowStart(const boost::shared_ptr<Query>& query) const
    {
        Dimensions const& dims = _schema.getDimensions();
        size_t nDims = dims.size();
        Coordinates result(nDims);
        for (size_t i = 0; i < nDims; i++)
        {
            Value const& coord = ((boost::shared_ptr<OperatorParamPhysicalExpression>&)_parameters[i])->getExpression()->evaluate();
            if ( coord.isNull() || coord.get<int64_t>() < dims[i].getStartMin())
            {
                result[i] = dims[i].getStartMin();
            }
            else
            {
                result[i] = coord.get<int64_t>();
            }
        }
        return result;
    }

    Coordinates getWindowEnd(const boost::shared_ptr<Query>& query) const
    {
        Dimensions const& dims = _schema.getDimensions();
        size_t nDims = dims.size();
        Coordinates result(nDims);
        for (size_t i = 0; i < nDims; i++)
        {
            Value const& coord = ((boost::shared_ptr<OperatorParamPhysicalExpression>&)_parameters[i + nDims])->getExpression()->evaluate();
            if (coord.isNull() || coord.getInt64() > dims[i].getEndMax())
            {
                result[i] = dims[i].getEndMax();
            }
            else
            {
                result[i] = coord.getInt64();
            }
        }
        return result;
    }

   virtual PhysicalBoundaries getOutputBoundaries(const std::vector<PhysicalBoundaries> & inputBoundaries,
                                                  const std::vector< ArrayDesc> & inputSchemas) const
    {
       shared_ptr<Query> query(Query::getValidQueryPtr(_query));
       PhysicalBoundaries window(getWindowStart(query), getWindowEnd(query));
       return inputBoundaries[0].intersectWith(window);
    }

   /***
    * Between is a pipelined operator, hence it executes by returning an iterator-based array to the consumer.
    */
   boost::shared_ptr< Array> execute(std::vector< boost::shared_ptr< Array> >& inputArrays,
                                     boost::shared_ptr<Query> query)
   {
      assert(inputArrays.size() == 1);

      shared_ptr<Array> inputArray = ensureRandomAccess(inputArrays[0], query);

      Coordinates lowPos = getWindowStart(query);
      Coordinates highPos = getWindowEnd(query);
      SpatialRangesPtr spatialRangesPtr = make_shared<SpatialRanges>(lowPos.size());
      if (isDominatedBy(lowPos, highPos)) {
          spatialRangesPtr->_ranges.push_back(SpatialRange(lowPos, highPos));
      }
      return boost::shared_ptr<Array>(make_shared<BetweenArray>(_schema, spatialRangesPtr, inputArray));
   }
};

DECLARE_PHYSICAL_OPERATOR_FACTORY(PhysicalBetween, "between", "physicalBetween")

}  // namespace scidb
