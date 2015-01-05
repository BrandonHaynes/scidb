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
 * PhysicalOldUnpack.cpp
 *
 *  Created on: Apr 20, 2010
 *      Author: Knizhnik
 */

#include "query/Operator.h"
#include "array/Metadata.h"
#include "UnpackArray.h"
#include "UnpackUnalignedArray.h"

using namespace std;
using namespace boost;

namespace scidb {


class PhysicalOldUnpack: public PhysicalOperator
{
public:
    PhysicalOldUnpack(std::string const& logicalName,
                   std::string const& physicalName,
                   Parameters const& parameters,
                   ArrayDesc const& schema):
    PhysicalOperator(logicalName, physicalName, parameters, schema)
    {
    }
    
    virtual bool changesDistribution(std::vector<ArrayDesc> const&) const
    {
        return true;
    }

    virtual bool outputFullChunks(std::vector<ArrayDesc> const&) const
    {
        return false;
    }

    virtual ArrayDistribution getOutputDistribution(
            std::vector<ArrayDistribution> const&,
            std::vector<ArrayDesc> const&) const
    {
        return ArrayDistribution(psUndefined);
    }

    virtual PhysicalBoundaries getOutputBoundaries(
            std::vector<PhysicalBoundaries> const& inputBoundaries,
            std::vector< ArrayDesc> const& inputSchemas) const
    {
        return inputBoundaries[0].reshape(inputSchemas[0].getDimensions(), _schema.getDimensions());
    }

    /***
     * Unpack is a pipelined operator, hence it executes by returning an iterator-based array to the consumer
     * that overrides the chunkiterator method.
     */
    boost::shared_ptr<Array> execute(
            vector< boost::shared_ptr<Array> >& inputArrays,
            boost::shared_ptr<Query> query)
    {
        assert(inputArrays.size() == 1);
        boost::shared_ptr<Array> inputArray = ensureRandomAccess(inputArrays[0], query);
        Dimensions const& dims = inputArray->getArrayDesc().getDimensions();
        size_t lastDim = dims.size()-1;
        bool isAligned = dims[lastDim].getLength() % dims[lastDim].getChunkInterval() == 0;
        return boost::shared_ptr<Array>(isAligned
                                        ? (Array*)new UnpackArray(_schema, inputArray, query)
                                        : (Array*)new UnpackUnalignedArray(_schema, inputArray, query));
    }
};

DECLARE_PHYSICAL_OPERATOR_FACTORY(PhysicalOldUnpack, "old_unpack", "physicalOldUnpack")

}  // namespace scidb

