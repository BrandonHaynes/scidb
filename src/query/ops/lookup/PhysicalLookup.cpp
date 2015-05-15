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
 * PhysicalLookup.cpp
 *
 *  Created on: Jul 26, 2010
 *      Author: Knizhnik
 */

#include "query/Operator.h"
#include "array/Metadata.h"
#include "array/Array.h"
#include "query/ops/lookup/LookupArray.h"
#include "network/NetworkManager.h"
#include "query/QueryProcessor.h"


namespace scidb {

using namespace boost;
using namespace std;

class PhysicalLookup: public  PhysicalOperator
{
public:
	PhysicalLookup(const string& logicalName, const string& physicalName, const Parameters& parameters, const ArrayDesc& schema):
	     PhysicalOperator(logicalName, physicalName, parameters, schema)
	{
	}

    virtual bool changesDistribution(const std::vector<ArrayDesc>&) const
    {
        return true;
    }

    virtual ArrayDistribution getOutputDistribution(
            std::vector<ArrayDistribution> const&,
            std::vector<ArrayDesc> const&) const
    {
        return ArrayDistribution(psLocalInstance);
    }

	/***
	 * Lookup is a pipelined operator, hence it executes by returning an iterator-based array to the consumer
	 * that overrides the chunkiterator method.
	 */
	boost::shared_ptr<Array> execute(vector< boost::shared_ptr<Array> >& inputArrays, boost::shared_ptr<Query> query)
    {
		assert(inputArrays.size() == 2);
		assert(_parameters.size() == 0);

        boost::shared_ptr<Array> left = inputArrays[0];
        boost::shared_ptr<Array> right = inputArrays[1];

        const shared_ptr<DistributionMapper> distMapper;
        const shared_ptr<PartitioningSchemaData> psData;
        left = redistributeToRandomAccess(left, query, psLocalInstance,
                                          query->isCoordinator() ? query->getInstanceID() : query->getCoordinatorID(),
                                          distMapper, 0, psData);

        right = redistributeToRandomAccess(right, query, psLocalInstance,
                                          query->isCoordinator() ? query->getInstanceID() : query->getCoordinatorID(),
                                          distMapper, 0, psData);
        if (!query->isCoordinator()) {
            return boost::shared_ptr<Array>(new MemArray(_schema,query));
        }

        return boost::shared_ptr<Array>(new LookupArray(_schema, left, right, query));
    }
};

DECLARE_PHYSICAL_OPERATOR_FACTORY(PhysicalLookup, "lookup", "physicalLookup")

}  // namespace scidb
