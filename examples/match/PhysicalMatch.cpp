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
 *  Created on: Apr 04, 2012
 *      Author: Knizhnik
 */

#include "query/Operator.h"
#include "array/Metadata.h"
#include "MatchArray.h"


using namespace std;
using namespace boost;

namespace scidb {

class PhysicalMatch: public PhysicalOperator
{
  public:
    PhysicalMatch(const string& logicalName, const string& physicalName, const Parameters& parameters, const ArrayDesc& schema):
        PhysicalOperator(logicalName, physicalName, parameters, schema)
    {}

    virtual DistributionRequirement getDistributionRequirement (const std::vector< ArrayDesc> & inputSchemas) const
    {
        return DistributionRequirement(DistributionRequirement::Collocated);
    }

    //TODO: when this operator works as expected and is debugged - add a correct getOutputBoundaries implementation here

    /***
     * Match is a pipelined operator, hence it executes by returning an iterator-based array to the consumer
     * that overrides the chunkiterator method.
     */
    boost::shared_ptr<Array> execute(vector< boost::shared_ptr<Array> >& inputArrays, boost::shared_ptr<Query> query)
    {
        assert(inputArrays.size() == 2);
        int64_t error = ((boost::shared_ptr<OperatorParamPhysicalExpression>&)_parameters[0])->getExpression()->evaluate().getInt64();
        return boost::shared_ptr<Array>(new MatchArray(_schema, inputArrays[0], inputArrays[1], error));
    }
};

REGISTER_PHYSICAL_OPERATOR_FACTORY(PhysicalMatch, "match", "physicalMatch");

}  // namespace scidb
