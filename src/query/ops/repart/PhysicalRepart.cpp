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

/**
 * @file PhysicalRepart.cpp
 *
 * @author Konstantin Knizhnik <knizhnik@garret.ru>
 * @author apoliakov@paradigm4.com
 */

#include <query/Operator.h>
#include <array/Metadata.h>
#include <network/NetworkManager.h>
#include <array/DelegateArray.h>
#include "../redimension/RedimensionCommon.h"

using namespace std;
using namespace boost;

namespace scidb {

class PhysicalRepart : public RedimensionCommon
{
public:
    PhysicalRepart(string const& logicalName,
                   string const& physicalName,
                   Parameters const& parameters,
                   ArrayDesc const & schema) :
       RedimensionCommon(logicalName, physicalName, parameters, schema)
    {}

    //True if this is a no-op (just a metadata change, doesn't change chunk sizes or overlap)
    bool isNoop(ArrayDesc const& inputSchema) const
    {
        return samePartitioning(_schema, inputSchema);
    }

    virtual bool changesDistribution(vector<ArrayDesc> const& inputSchemas) const
    {
        return !isNoop(inputSchemas[0]);
    }

    virtual PhysicalBoundaries getOutputBoundaries(vector<PhysicalBoundaries> const& sourceBoundaries,
                                                   vector<ArrayDesc> const& sourceSchema) const
    {
        return sourceBoundaries[0];
    }

    virtual ArrayDistribution getOutputDistribution(vector<ArrayDistribution> const& sourceDistribution,
                                                    vector< ArrayDesc> const& inputSchemas) const
    {
        if (isNoop(inputSchemas[0]))
        {
            return sourceDistribution[0];
        }
        return ArrayDistribution(psUndefined);
    }

    virtual bool outputFullChunks(std::vector< ArrayDesc> const& inputSchemas) const
    {
        return isNoop(inputSchemas[0]);
    }

    boost::shared_ptr<Array> execute(vector< shared_ptr<Array> >& sourceArray, shared_ptr<Query> query)
    {
        shared_ptr<Array> input = sourceArray[0];
        if (isNoop(input->getArrayDesc()))
        {
            return shared_ptr<Array> (new DelegateArray(_schema, input, true) );
        }

        Attributes const& destAttrs = _schema.getAttributes(true); // true = exclude empty tag.

        vector<AggregatePtr> aggregates (destAttrs.size());
        vector<size_t> attrMapping(destAttrs.size());
        vector<size_t> dimMapping(_schema.getDimensions().size());

        for (size_t i=0; i<attrMapping.size(); ++i)
        {
            attrMapping[i]=i;
        }
        for (size_t i=0; i<dimMapping.size(); ++i)
        {
            dimMapping[i]=i;
        }

        ElapsedMilliSeconds timing;
        shared_ptr<Array> res = redimensionArray(input,
                                                 attrMapping,
                                                 dimMapping,
                                                 aggregates,
                                                 query,
                                                 timing,
                                                 AUTO);
        return res;
    }
};

// Note that the name "physicalRepart" is known in QueryPlan.h.
DECLARE_PHYSICAL_OPERATOR_FACTORY(PhysicalRepart, "repart", "physicalRepart")

}  // namespace scidb
