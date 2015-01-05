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
 * PhysicalSort2.cpp
 *
 *  Created on: Aug 15, 2010
 *      Author: knizhnik@garret.ru
 */
#include "query/Operator.h"
#include "query/QueryProcessor.h"
#include "array/Metadata.h"
#include "array/MergeSortArray.h"

using namespace boost;

namespace scidb
{

class PhysicalSort2 : public  PhysicalOperator
{
public:
    PhysicalSort2(std::string const& logicalName,
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

    virtual bool outputFullChunks(std::vector< ArrayDesc> const&) const
    {
        return false;
    }

    virtual ArrayDistribution getOutputDistribution(
            std::vector<ArrayDistribution> const&,
            std::vector< ArrayDesc> const&) const
    {
        return ArrayDistribution(psUndefined);
    }

    virtual PhysicalBoundaries getOutputBoundaries(
            std::vector<PhysicalBoundaries> const& inputBoundaries,
            std::vector< ArrayDesc> const& inputSchemas) const
    {
        return inputBoundaries[0];
    }

    boost::shared_ptr<Array> execute(std::vector< boost::shared_ptr<Array> >& inputArrays, boost::shared_ptr<Query> query)
    {
        assert (query->getCoordinatorID() == COORDINATOR_INSTANCE);

        if (inputArrays.size() > 1) {
            std::auto_ptr<SortContext> ctx ((SortContext*)query->userDefinedContext);
            boost::shared_ptr<TupleComparator> tcomp(new TupleComparator(ctx->keys, _schema));
            boost::shared_ptr<Array> result(new MergeSortArray(query, _schema, inputArrays, tcomp));
            ctx.reset();
            return result;
        }
        return inputArrays[0];
    }
};

DECLARE_PHYSICAL_OPERATOR_FACTORY(PhysicalSort2, "sort2", "physicalSort2")

} //namespace scidb
