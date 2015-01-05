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
 * @file PhysicalMStat.cpp
 *
 * @author roman.simakov@gmail.com
 *
 * Physical implementation of mstat operator for gathering malloc info from instances
 */

#include <string.h>
#include <sstream>
#ifndef __APPLE__
#include <malloc.h>
#endif
#include "query/Operator.h"
#include "array/TupleArray.h"

using namespace std;
using namespace boost;

namespace scidb
{

class PhysicalMStat: public PhysicalOperator
{
public:
    PhysicalMStat(std::string const& logicalName,
                  std::string const& physicalName,
                  Parameters const& parameters,
                  ArrayDesc const& schema):
        PhysicalOperator(logicalName, physicalName, parameters, schema)
    {
    }

    virtual bool changesDistribution(std::vector< ArrayDesc> const&) const
    {
        return true;
    }

    virtual ArrayDistribution getOutputDistribution(
            std::vector<ArrayDistribution> const&,
            std::vector< ArrayDesc> const&) const
    {
        return ArrayDistribution(psLocalInstance);
    }

    boost::shared_ptr<Array> execute(
            vector< boost::shared_ptr<Array> >& inputArrays,
            boost::shared_ptr<Query> query)
    {
        const size_t numNodes = Cluster::getInstance()->getInstanceMembership()->getInstances().size();
        const size_t instanceId = Cluster::getInstance()->getLocalInstanceId();
        vector< boost::shared_ptr<Tuple> > tuples(numNodes);
        for (size_t i = 0; i < numNodes; i++) {
            if (i == instanceId) {
                Tuple& tuple = *new Tuple(10);
                tuples[i] = boost::shared_ptr<Tuple>(&tuple);
#ifndef __APPLE__
                struct mallinfo mi = mallinfo();
                tuple[0].setInt32(mi.arena);
                tuple[1].setInt32(mi.ordblks);
                tuple[2].setInt32(mi.smblks);
                tuple[3].setInt32(mi.hblks);
                tuple[4].setInt32(mi.hblkhd);
                tuple[5].setInt32(mi.usmblks);
                tuple[6].setInt32(mi.fsmblks);
                tuple[7].setInt32(mi.uordblks);
                tuple[8].setInt32(mi.fordblks);
                tuple[9].setInt32(mi.keepcost);
#else
                tuple[0].setInt32(0);
                tuple[1].setInt32(0);
                tuple[2].setInt32(0);
                tuple[3].setInt32(0);
                tuple[4].setInt32(0);
                tuple[5].setInt32(0);
                tuple[6].setInt32(0);
                tuple[7].setInt32(0);
                tuple[8].setInt32(0);
                tuple[9].setInt32(0);
#endif
            }
        }
        return boost::shared_ptr<Array>(new TupleArray(_schema, tuples));
    }
};

DECLARE_PHYSICAL_OPERATOR_FACTORY(PhysicalMStat, "mstat", "physicalMstat")

} //namespace
