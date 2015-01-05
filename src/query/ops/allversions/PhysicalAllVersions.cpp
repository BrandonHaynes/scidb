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
 * @file PhysicalAllVersions.cpp
 *
 * @author knizhnik@garret.ru
 *
 * Physical implementation of VERSIONS operator for versionsing data from text files
 */

#include <string.h>

#include "query/Operator.h"
#include "AllVersionsArray.h"
#include "system/SystemCatalog.h"

using namespace std;
using namespace boost;

namespace scidb
{

class PhysicalAllVersions: public PhysicalOperator
{
public:
    PhysicalAllVersions(const string& logicalName, const string& physicalName, const Parameters& parameters, const ArrayDesc& schema):
        PhysicalOperator(logicalName, physicalName, parameters, schema)
    {
    }

    virtual bool changesDistribution(std::vector< ArrayDesc> const&) const
    {
        return true;
    }

    virtual bool outputFullChunks(std::vector< ArrayDesc> const&) const
    {
        return false;
    }

    virtual ArrayDistribution getOutputDistribution(
            std::vector<ArrayDistribution> const& inputDistributions,
            std::vector< ArrayDesc> const& inputSchemas) const
    {
        return ArrayDistribution(psUndefined);
    }

    boost::shared_ptr<Array> execute(vector< boost::shared_ptr<Array> >& inputArrays, boost::shared_ptr<Query> query)
    {
        assert(inputArrays.size() == 0);
        assert(_parameters.size() == 1);

        const string &arrayName = ((boost::shared_ptr<OperatorParamReference>&)_parameters[0])->getObjectName();

        ArrayDesc arrayDesc;
        SystemCatalog::getInstance()->getArrayDesc(arrayName, arrayDesc);
        return boost::shared_ptr<Array>(new AllVersionsArray(_schema,
                                                             SystemCatalog::getInstance()->getArrayVersions(arrayDesc.getId()),
                                                             query));
    }
};

DECLARE_PHYSICAL_OPERATOR_FACTORY(PhysicalAllVersions, "allversions", "physicalAllVersions")

} //namespace
