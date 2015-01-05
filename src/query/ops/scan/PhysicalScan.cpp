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
 * PhysicalScan.cpp
 *
 *  Created on: Oct 28, 2010
 *      Author: knizhnik@garret.ru
 */
#include <boost/make_shared.hpp>
#include "query/Operator.h"
#include "array/TransientCache.h"
#include "array/DBArray.h"
#include "array/Metadata.h"
#include "system/SystemCatalog.h"

namespace scidb
{

class PhysicalScan: public  PhysicalOperator
{
  public:
    PhysicalScan(const std::string& logicalName, const std::string& physicalName, const Parameters& parameters, const ArrayDesc& schema):
    PhysicalOperator(logicalName, physicalName, parameters, schema)
    {
        _arrayName = ((boost::shared_ptr<OperatorParamReference>&)parameters[0])->getObjectName();
    }

    virtual ArrayDistribution getOutputDistribution(const std::vector<ArrayDistribution> & inputDistributions,
                                                 const std::vector< ArrayDesc> & inputSchemas) const
    {
        SystemCatalog* systemCatalog = SystemCatalog::getInstance();
        PartitioningSchema ps = systemCatalog->getPartitioningSchema(_schema.getId());
        return ArrayDistribution(ps);
    }

    virtual PhysicalBoundaries getOutputBoundaries(const std::vector<PhysicalBoundaries> & inputBoundaries,
                                                   const std::vector< ArrayDesc> & inputSchemas) const
    {
        SystemCatalog* systemCatalog = SystemCatalog::getInstance();
        Coordinates lowBoundary = systemCatalog->getLowBoundary(_schema.getId());
        Coordinates highBoundary = systemCatalog->getHighBoundary(_schema.getId());

        return PhysicalBoundaries(lowBoundary, highBoundary);
    }

    virtual void preSingleExecute(shared_ptr<Query> query)
    {
        if (_schema.isTransient())
        {
            shared_ptr<const InstanceMembership> membership(Cluster::getInstance()->getInstanceMembership());

            if ((membership->getViewId() != query->getCoordinatorLiveness()->getViewId()) ||
                (membership->getInstances().size() != query->getInstancesCount()))
            {
                throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_NO_QUORUM2);
            }
         }
    }

    boost::shared_ptr< Array> execute(std::vector< boost::shared_ptr< Array> >& inputArrays,
                                      boost::shared_ptr<Query> query)
    {
        if (_schema.isTransient())
        {
            if (MemArrayPtr a = transient::lookup(_schema,query))
            {
                return a;                                   // ...temp array
            }
            else
            {
               return make_shared<MemArray>(_schema,query); // ...empty array
            }
        }
        else
        {
            return boost::shared_ptr<Array>(DBArray::newDBArray(_schema, query));
        }
    }


  private:
    string _arrayName;
};

DECLARE_PHYSICAL_OPERATOR_FACTORY(PhysicalScan, "scan", "physicalScan")

} //namespace scidb
