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
 * PhysicalStore.cpp
 *
 *  Created on: Apr 16, 2010
 *      Author: Knizhnik
 */

#include <boost/foreach.hpp>

#include "query/Operator.h"
#include "query/QueryProcessor.h"
#include "query/TypeSystem.h"
#include "array/Metadata.h"
#include "array/Array.h"
#include "array/DBArray.h"
#include "array/TransientCache.h"
#include "system/SystemCatalog.h"
#include "network/NetworkManager.h"
#include "smgr/io/Storage.h"
#include "query/Statistics.h"

#include "array/ParallelAccumulatorArray.h"
#include "array/DelegateArray.h"
#include "system/Config.h"
#include "system/SciDBConfigOptions.h"

using namespace std;
using namespace boost;

namespace scidb {

class PhysicalStore: public PhysicalOperator
{
  private:
   ArrayUAID _arrayUAID;   /**< UAID of new array */
   ArrayID _arrayID;   /**< ID of new array */
   VersionID _lastVersion;
   shared_ptr<SystemCatalog::LockDesc> _lock;

  public:
   PhysicalStore(const string& logicalName, const string& physicalName, const Parameters& parameters, const ArrayDesc& schema):
        PhysicalOperator(logicalName, physicalName, parameters, schema),
        _arrayUAID(0),
        _arrayID(0),
        _lastVersion(0)
   {}

   void preSingleExecute(shared_ptr<Query> query)
   {
        ArrayDesc parentArrayDesc;
        shared_ptr<const InstanceMembership> membership(Cluster::getInstance()->getInstanceMembership());
        assert(membership);
        if ((membership->getViewId() != query->getCoordinatorLiveness()->getViewId()) ||
            (membership->getInstances().size() != query->getInstancesCount()))
        {
            throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_NO_QUORUM2);
        }

        _lock = make_shared<SystemCatalog::LockDesc>(_schema.getName(),
                                                     query->getQueryID(),
                                                     Cluster::getInstance()->getLocalInstanceId(),
                                                     SystemCatalog::LockDesc::COORD,
                                                     SystemCatalog::LockDesc::WR);
        shared_ptr<Query::ErrorHandler> ptr(make_shared<UpdateErrorHandler>(_lock));
        query->pushErrorHandler(ptr);

     /* array does not yet exist? ...*/

        if (!SystemCatalog::getInstance()->getArrayDesc(_schema.getName(),parentArrayDesc,false))
        {
            if (_schema.getId() != 0)
            {
                throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_ARRAY_DOESNT_EXIST) << _schema.getName();
            }
            _lock->setLockMode(SystemCatalog::LockDesc::CRT);
            BOOST_VERIFY(SystemCatalog::getInstance()->updateArrayLock(_lock));
            parentArrayDesc = _schema;
            SystemCatalog::getInstance()->addArray(parentArrayDesc, psHashPartitioned);
        }
        else
        if (parentArrayDesc.isTransient())
        {
            if (_schema.getId() != parentArrayDesc.getId())
            {
                throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_ARRAY_DOESNT_EXIST) << _schema.getName();
            }
            _lock->setArrayId       (_arrayUAID   = parentArrayDesc.getUAId());
            _lock->setArrayVersion  (_lastVersion = 0);
            _lock->setArrayVersionId(_arrayID     = parentArrayDesc.getId());
            BOOST_VERIFY(SystemCatalog::getInstance()->updateArrayLock(_lock));
            return;
        }
        else // exists but not transient, so get its latest version
        {
            if (_schema.getId() != parentArrayDesc.getId())
            {
                throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_ARRAY_DOESNT_EXIST) << _schema.getName();
            }
            _lastVersion = SystemCatalog::getInstance()->getLastVersion(parentArrayDesc.getId());
        }

        _arrayUAID = parentArrayDesc.getUAId();
        _lock->setArrayId(_arrayUAID);
        _lock->setArrayVersion(_lastVersion+1);

        BOOST_VERIFY(SystemCatalog::getInstance()->updateArrayLock(_lock));

        {
            Dimensions newVersionDims(_schema.getDimensions());

            BOOST_FOREACH (DimensionDesc& d, newVersionDims)
            {
                d.setCurrStart(MAX_COORDINATE);
                d.setCurrEnd(MIN_COORDINATE);
            }
            _schema = ArrayDesc(ArrayDesc::makeVersionedName(_schema.getName(), _lastVersion+1), parentArrayDesc.getAttributes(), newVersionDims);
        }

        SystemCatalog::getInstance()->addArray(_schema, psHashPartitioned);
        _arrayID = _schema.getId();
        _lock->setArrayVersionId(_arrayID);
        BOOST_VERIFY(SystemCatalog::getInstance()->updateArrayLock(_lock));
   }

    virtual void postSingleExecute(shared_ptr<Query> query)
    {
        assert(_lock);
        if (_arrayID!=0 && !_schema.isTransient())
        {
            SystemCatalog::getInstance()->createNewVersion(_arrayUAID, _arrayID);
        }
    }

    virtual PhysicalBoundaries getOutputBoundaries(const std::vector<PhysicalBoundaries> & inputBoundaries,
                                                   const std::vector< ArrayDesc>         & inputSchemas) const
    {
        return inputBoundaries.front();
    }

    virtual DistributionRequirement getDistributionRequirement(const std::vector< ArrayDesc> & inputSchemas) const
    {
        return DistributionRequirement(DistributionRequirement::SpecificAnyOrder,
                                       vector<ArrayDistribution>(1,ArrayDistribution(psHashPartitioned)));
    }

    /**
     * Record the array 't' in the transient array cache. Implements a callback
     * that is suitable for use as a query finalizer.
     */
    static void recordTransient(const MemArrayPtr& t,const QueryPtr& query)
    {
        if (query->wasCommitted())                       // Was committed ok?
        {
            transient::record(t);                        // ...record in cache
        }
    }

    shared_ptr<Array> execute(vector< shared_ptr<Array> >& inputArrays, shared_ptr<Query> query)
    {
        assert(inputArrays.size() == 1);
        VersionID    version = ArrayDesc::getVersionFromName (_schema.getName());
        string baseArrayName = ArrayDesc::makeUnversionedName(_schema.getName());

        if (_schema.isTransient())                       // Storing to transient?
        {
            MemArrayPtr        p(new MemArray(_schema,query)); // materialized copy
            PhysicalBoundaries b(PhysicalBoundaries::createEmpty(_schema.getDimensions().size()));

         /* Pick the best append mode that the source array will support...*/

            bool vertical = inputArrays[0]->getSupportedAccess() >= Array::MULTI_PASS;

            p->append(inputArrays[0],vertical);          // ...materialize it

         /* Run back over the chunks one more time to compute the physical bounds
            of the array...*/

            for (shared_ptr<ConstArrayIterator> i(p->getConstIterator(0)); !i->end(); ++(*i))
            {
                b.updateFromChunk(&i->getChunk());       // ...update bounds
            }

            SystemCatalog::getInstance()->updateArrayBoundaries(_schema,b);
            query->pushFinalizer(bind(&recordTransient,p,_1));
            getInjectedErrorListener().check();          // ...for error injection
            return p;                                    // ...return the copy
        }

        if (!_lock)
        {
           _lock = shared_ptr<SystemCatalog::LockDesc>(new SystemCatalog::LockDesc(baseArrayName,
                                                                                   query->getQueryID(),
                                                                                   Cluster::getInstance()->getLocalInstanceId(),
                                                                                   SystemCatalog::LockDesc::WORKER,
                                                                                   SystemCatalog::LockDesc::WR));
           _lock->setArrayVersion(version);
           shared_ptr<Query::ErrorHandler> ptr(new UpdateErrorHandler(_lock));
           query->pushErrorHandler(ptr);

           Query::Finalizer f = bind(&UpdateErrorHandler::releaseLock,_lock,_1);
           query->pushFinalizer(f);
           SystemCatalog::ErrorChecker errorChecker(bind(&Query::validate, query));
           if (!SystemCatalog::getInstance()->lockArray(_lock, errorChecker))
           {
              throw USER_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_CANT_INCREMENT_LOCK)<< baseArrayName;
           }
        }

        shared_ptr<Array>  srcArray    (inputArrays[0]);
        ArrayDesc const&   srcArrayDesc(srcArray->getArrayDesc());
        shared_ptr<Array>  dstArray    (DBArray::newDBArray(_schema.getName(), query)); // We can't use _arrayID because it's not initialized on remote instances
        ArrayDesc const&   dstArrayDesc(dstArray->getArrayDesc());

        query->getReplicationContext()->enableInboundQueue(dstArrayDesc.getId(), dstArray);

        size_t nAttrs = dstArrayDesc.getAttributes().size();

        if (nAttrs == 0)
        {
            return dstArray;
        }

        if (nAttrs > srcArrayDesc.getAttributes().size())
        {
            assert(nAttrs == srcArrayDesc.getAttributes().size()+1);
            srcArray = boost::shared_ptr<Array>(new NonEmptyableArray(srcArray));
        }

        // Perform parallel evaluation of aggregate
        shared_ptr<JobQueue> queue = PhysicalOperator::getGlobalQueueForOperators();
        size_t nJobs = srcArray->getSupportedAccess() == Array::RANDOM ? Config::getInstance()->getOption<int>(CONFIG_RESULT_PREFETCH_QUEUE_SIZE) : 1;
        vector< shared_ptr<StoreJob> > jobs(nJobs);
        Dimensions const& dims = dstArrayDesc.getDimensions();
        size_t nDims = dims.size();
        for (size_t i = 0; i < nJobs; i++) {
            jobs[i] = make_shared<StoreJob>(i, nJobs, dstArray, srcArray, nDims, nAttrs, query);
        }
        for (size_t i = 0; i < nJobs; i++) {
            queue->pushJob(jobs[i]);
        }

        PhysicalBoundaries bounds = PhysicalBoundaries::createEmpty(nDims);
        int errorJob = -1;
        for (size_t i = 0; i < nJobs; i++) {
            if (!jobs[i]->wait()) {
                errorJob = i;
            }
            else {
                bounds = bounds.unionWith(jobs[i]->bounds);
            }
        }
        if (errorJob >= 0) {
            jobs[errorJob]->rethrow();
        }

        //Destination array is mutable: collect the coordinates of all chunks created by all jobs
        set<Coordinates, CoordinatesLess> createdChunks;
        for(size_t i =0; i < nJobs; i++)
        {
            createdChunks.insert(jobs[i]->getCreatedChunks().begin(), jobs[i]->getCreatedChunks().end());
        }
        //Insert tombstone entries
        StorageManager::getInstance().removeDeadChunks(dstArrayDesc, createdChunks, query);

        SystemCatalog::getInstance()->updateArrayBoundaries(_schema, bounds);
        query->getReplicationContext()->replicationSync(dstArrayDesc.getId());
        query->getReplicationContext()->removeInboundQueue(dstArrayDesc.getId());

        StorageManager::getInstance().flush();
        getInjectedErrorListener().check();
        return dstArray;
    }
};

DECLARE_PHYSICAL_OPERATOR_FACTORY(PhysicalStore, "store", "physicalStore")

}  // namespace ops
