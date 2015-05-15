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
#include "MergeArray.h"


using namespace std;
using namespace boost;

namespace scidb {

class PhysicalMerge: public PhysicalOperator
{
public:
    PhysicalMerge(const string& logicalName, const string& physicalName,
                  const Parameters& parameters, const ArrayDesc& schema)
        : PhysicalOperator(logicalName, physicalName, parameters, schema)
    { }

    virtual DistributionRequirement getDistributionRequirement (const std::vector< ArrayDesc> & inputSchemas) const
    {
        return DistributionRequirement(DistributionRequirement::Collocated);
    }

    virtual PhysicalBoundaries getOutputBoundaries(const std::vector<PhysicalBoundaries> & inputBoundaries,
                                                   const std::vector< ArrayDesc> & inputSchemas) const
    {
        return inputBoundaries[0].unionWith(inputBoundaries[1]);
    }

    /**
     * Ensure input array chunk sizes and overlaps match.
     */
    virtual void requiresRepart(vector<ArrayDesc> const& inputSchemas,
                                vector<ArrayDesc const*>& repartSchemas) const
    {
        repartByLeftmost(inputSchemas, repartSchemas);
    }

    /***
     * Merge is a pipelined operator, hence it executes by returning an iterator-based array to the consumer
     * that overrides the chunkiterator method.
     */
    boost::shared_ptr<Array> execute(vector< boost::shared_ptr<Array> >& inputArrays, boost::shared_ptr<Query> query)
    {
        assert(inputArrays.size() >= 2);
        return boost::shared_ptr<Array>(new MergeArray(_schema, inputArrays));
    }
};
    
DECLARE_PHYSICAL_OPERATOR_FACTORY(PhysicalMerge, "merge", "physicalMerge")

}  // namespace scidb
