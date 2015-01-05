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
 * PhysicalRemoveVersions.cpp
 *
 *  Created on: Jun 11, 2014
 *      Author: sfridella
 */

#include <boost/foreach.hpp>
#include <deque>

#include "query/Operator.h"
#include "query/QueryProcessor.h"
#include "array/DBArray.h"
#include "smgr/io/Storage.h"
#include "system/SystemCatalog.h"

using namespace std;
using namespace boost;

namespace scidb {

class PhysicalRemoveVersions: public PhysicalOperator
{
public:
   PhysicalRemoveVersions(const string& logicalName, const string& physicalName, const Parameters& parameters, const ArrayDesc& schema):
   PhysicalOperator(logicalName, physicalName, parameters, schema)
   {
   }

   void preSingleExecute(shared_ptr<Query> query)
   {
       shared_ptr<const InstanceMembership> membership(Cluster::getInstance()->getInstanceMembership());
       assert(membership);
       if (((membership->getViewId() != query->getCoordinatorLiveness()->getViewId()) ||
            (membership->getInstances().size() != query->getInstancesCount()))) {
           throw SYSTEM_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_NO_QUORUM2);
       }

       ArrayDesc arrayDesc;
       const string &arrayName = 
           ((boost::shared_ptr<OperatorParamReference>&)_parameters[0])->getObjectName();
       VersionID targetVersion = 
           ((boost::shared_ptr<OperatorParamPhysicalExpression>&)
            _parameters[1])->getExpression()->evaluate().getInt64();

       SystemCatalog::getInstance()->getArrayDesc(arrayName, arrayDesc, true);
       _lock = boost::shared_ptr<SystemCatalog::LockDesc>(
           new SystemCatalog::LockDesc(arrayName,
                                       query->getQueryID(),
                                       Cluster::getInstance()->getLocalInstanceId(),
                                       SystemCatalog::LockDesc::COORD,
                                       SystemCatalog::LockDesc::RM)
           );
       _lock->setArrayId(arrayDesc.getUAId());
       _lock->setArrayVersion(targetVersion);
       SystemCatalog::getInstance()->updateArrayLock(_lock);

       shared_ptr<Query::ErrorHandler> ptr(new RemoveErrorHandler(_lock));
       query->pushErrorHandler(ptr);
   }

    boost::shared_ptr<Array> execute(vector< boost::shared_ptr<Array> >& inputArrays, 
                                     boost::shared_ptr<Query> query)
    {
        getInjectedErrorListener().check();

        /* Remove target versions from storage
         */
        assert(_parameters.size() == 2);
        ArrayDesc arrayDesc;
        const string &arrayName = ((boost::shared_ptr<OperatorParamReference>&)_parameters[0])->getObjectName();
        VersionID targetVersion =
            ((boost::shared_ptr<OperatorParamPhysicalExpression>&)
             _parameters[1])->getExpression()->evaluate().getInt64();
        if (SystemCatalog::getInstance()->getArrayDesc(arrayName, targetVersion, arrayDesc, true))
        {
            StorageManager::getInstance().removeVersions(query->getQueryID(),
                                                         arrayDesc.getUAId(),
                                                         arrayDesc.getId());
        }

        return boost::shared_ptr<Array>();
    }

    void postSingleExecute(shared_ptr<Query> query)
    {
        SystemCatalog::getInstance()->deleteArrayVersions(_lock->getArrayName(),
                                                          _lock->getArrayVersion());
    }

private:


   boost::shared_ptr<SystemCatalog::LockDesc> _lock;
};

DECLARE_PHYSICAL_OPERATOR_FACTORY(PhysicalRemoveVersions, "remove_versions", "physicalRemoveVersions")

}  // namespace ops
