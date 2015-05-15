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
 * @file PhysicalResult.cpp
 *
 * @author roman.simakov@gmail.com
 *
 * @brief This file implements physical SCATTER/GATHER operator
 */

#include "boost/make_shared.hpp"
#include <log4cxx/logger.h>

#include <array/DBArray.h>
#include <array/DelegateArray.h>
#include <array/TransientCache.h>
#include <query/Operator.h>
#include <query/QueryProcessor.h>
#include <network/NetworkManager.h>
#include <network/BaseConnection.h>
#include <network/MessageUtils.h>
#include <system/SystemCatalog.h>
#include <smgr/io/Storage.h>

using namespace boost;
using namespace std;

namespace scidb
{

// Logger for network subsystem. static to prevent visibility of variable outside of file
static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.ops.sg"));

/**
 * Record the array 't' in the transient array cache. Implements a callback
 * that is suitable for use as a query finalizer.
 */
static void recordTransient(const MemArrayPtr& t,const QueryPtr& query)
{
    if (query->wasCommitted())                           // Was committed ok?
    {
        transient::record(t);                            // ...record in cache
    }
}

/**
 * Physical implementation of SCATTER/GATHER operator.
 * This physical operator must be inserted into physical plan by optimizer
 * without any logical instance in logical plan.
 */
class PhysicalSG: public PhysicalOperator
{
private:
    ArrayID _arrayID;   /**< ID of new array */
    ArrayID _updateableArrayID;   /**< ID of new array */
    boost::shared_ptr<SystemCatalog::LockDesc> _lock;

  public:
    PhysicalSG(const string& logicalName, const string& physicalName, const Parameters& parameters, const ArrayDesc& schema):
        PhysicalOperator(logicalName, physicalName, parameters, schema), _arrayID((ArrayID)~0), _updateableArrayID((ArrayID)~0)
    {
    }

    std::string getArrayNameForStore() const
    {
        std::string arrayName;
        if (_parameters.size() >= 3 &&
            !static_cast<OperatorParamReference*>(_parameters[2].get())->getObjectName().empty()) {
            arrayName = _schema.getName();
        }
        assert(arrayName.empty() ||
               ArrayDesc::makeUnversionedName(arrayName) ==
               static_cast<OperatorParamReference*>(_parameters[2].get())->getObjectName());

        return arrayName;
    }

    PartitioningSchema getPartitioningSchema() const
    {
        assert(_parameters[0]);
        OperatorParamPhysicalExpression* pExp = static_cast<OperatorParamPhysicalExpression*>(_parameters[0].get());
        PartitioningSchema ps = static_cast<PartitioningSchema>(pExp->getExpression()->evaluate().getInt32());
        return ps;
    }

    InstanceID getInstanceId() const
    {
        InstanceID instanceId = ALL_INSTANCE_MASK;
        if (_parameters.size() >=2 )
        {
            OperatorParamPhysicalExpression* pExp = static_cast<OperatorParamPhysicalExpression*>(_parameters[1].get());
            instanceId = static_cast<InstanceID>(pExp->getExpression()->evaluate().getInt64());
        }
        return instanceId;
    }

    void preSingleExecute(boost::shared_ptr<Query> query)
    {
        if (getArrayNameForStore().empty()) {
            return;
        }
        preSingleExecuteForStore(query);
    }

    void preSingleExecuteForStore(boost::shared_ptr<Query>& query)
    {
        ArrayDesc desc;
        shared_ptr<const InstanceMembership> membership(Cluster::getInstance()->getInstanceMembership());
        assert(membership);
        if ((membership->getViewId() != query->getCoordinatorLiveness()->getViewId()) ||
            (membership->getInstances().size() != query->getInstancesCount())) {
            throw SYSTEM_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_NO_QUORUM2);
        }
        string const& arrayName = _schema.getName();
        assert(arrayName == getArrayNameForStore());
        _lock = boost::shared_ptr<SystemCatalog::LockDesc>(new SystemCatalog::LockDesc(arrayName,
                                                                                       query->getQueryID(),
                                                                                       Cluster::getInstance()->getLocalInstanceId(),
                                                                                       SystemCatalog::LockDesc::COORD,
                                                                                       SystemCatalog::LockDesc::WR));
        shared_ptr<Query::ErrorHandler> ptr(new UpdateErrorHandler(_lock));
        query->pushErrorHandler(ptr);

        bool rc = false;
        PartitioningSchema ps = getPartitioningSchema();

        Dimensions const& dims =  _schema.getDimensions();
        size_t nDims = dims.size();
        Dimensions newVersionDims(nDims);
        bool arrayExists = SystemCatalog::getInstance()->getArrayDesc(arrayName, desc, false);
        VersionID lastVersion = 0;
        if (!arrayExists) {
            _lock->setLockMode(SystemCatalog::LockDesc::CRT);
            rc = SystemCatalog::getInstance()->updateArrayLock(_lock);
            assert(rc);
            desc = _schema;
            SystemCatalog::getInstance()->addArray(desc, psHashPartitioned);
        }
        else
        if (desc.isTransient())
        {
             _schema.setIds(desc.getId(),desc.getUAId(),0);
             _schema.setTransient(true);
            _lock->setArrayId       (desc.getUAId());
            _lock->setArrayVersion  (0);
            _lock->setArrayVersionId(desc.getId());
            BOOST_VERIFY(SystemCatalog::getInstance()->updateArrayLock(_lock));
            return;
        }
        else
        {
            lastVersion = SystemCatalog::getInstance()->getLastVersion(desc.getId());
        }

        for (size_t i = 0; i < nDims; i++) {
            DimensionDesc const& dim = dims[i];
            newVersionDims[i] = dim;
        }
        _updateableArrayID = desc.getId();

        _lock->setArrayId(_updateableArrayID);
        _lock->setArrayVersion(lastVersion+1);
        rc = SystemCatalog::getInstance()->updateArrayLock(_lock);
        assert(rc);

        _schema = ArrayDesc(ArrayDesc::makeVersionedName(desc.getName(), lastVersion+1),
                            desc.getAttributes(), newVersionDims);
        SystemCatalog::getInstance()->addArray(_schema, ps);
        _arrayID = _schema.getId();
        _lock->setArrayVersionId(_arrayID);
        rc = SystemCatalog::getInstance()->updateArrayLock(_lock);
        assert(rc);
        rc = rc; // Eliminate warnings
    }

    void postSingleExecute(shared_ptr<Query>)
    {
        if (_updateableArrayID!=ArrayID(~0) && !_schema.isTransient())
        {
            SystemCatalog::getInstance()->createNewVersion(_updateableArrayID, _arrayID);
        }
    }

    bool outputFullChunks(std::vector< ArrayDesc> const&) const
    {
        return true;
    }

    ArrayDistribution getOutputDistribution(const std::vector<ArrayDistribution> & inputDistributions,
                                                 const std::vector< ArrayDesc> & inputSchemas) const
    {
        PartitioningSchema ps = getPartitioningSchema();

        DimensionVector offset = getOffsetVector(inputSchemas);

        boost::shared_ptr<DistributionMapper> distMapper;

        if ( !offset.isEmpty() )
        {
            distMapper = DistributionMapper::createOffsetMapper(offset);
        }

        return ArrayDistribution(ps,distMapper);
    }

    DimensionVector getOffsetVector(const vector<ArrayDesc> & inputSchemas) const
    {
        if (_parameters.size() <= 4)
        {
            return DimensionVector();
        }
        else
        {
            DimensionVector result(_schema.getDimensions().size());
            assert (_parameters.size() == _schema.getDimensions().size() + 4);
            for (size_t i = 0; i < result.numDimensions(); i++)
            {
                result[i] = ((boost::shared_ptr<OperatorParamPhysicalExpression>&)_parameters[i+4])->getExpression()->evaluate().getInt64();
            }
            return result;
        }
    }

    PhysicalBoundaries getOutputBoundaries(const std::vector<PhysicalBoundaries> & inputBoundaries,
                                                   const std::vector< ArrayDesc> & inputSchemas) const
    {
        return inputBoundaries[0];
    }

    boost::shared_ptr<Array> execute(vector< boost::shared_ptr<Array> >& inputArrays, boost::shared_ptr<Query> query)
    {
        const PartitioningSchema ps = getPartitioningSchema();
        const InstanceID instanceID = getInstanceId();
        DimensionVector offsetVector = getOffsetVector(vector<ArrayDesc>());
        shared_ptr<Array> srcArray = inputArrays[0];
        boost::shared_ptr <DistributionMapper> distMapper;
        if (!offsetVector.isEmpty())
        {
            distMapper = DistributionMapper::createOffsetMapper(offsetVector);
        }
        const std::string arrayName = getArrayNameForStore();

        bool enforceDataIntegrity=false;
        if (_parameters.size() >= 4)
        {
            assert(_parameters[3]->getParamType() == PARAM_PHYSICAL_EXPRESSION);
            OperatorParamPhysicalExpression* paramExpr = static_cast<OperatorParamPhysicalExpression*>(_parameters[3].get());
            enforceDataIntegrity = paramExpr->getExpression()->evaluate().getBool();
            assert(paramExpr->isConstant());
        }

        const bool storeResult = !arrayName.empty();

        if (!storeResult) {

            return redistributeToRandomAccess(srcArray, query, ps,
                                              instanceID, distMapper, 0,
                                              shared_ptr<PartitioningSchemaData>(),
                                              enforceDataIntegrity);
        }

        // storing directly into a DB array

        assert(!arrayName.empty());

        VersionID version    = ArrayDesc::getVersionFromName (arrayName);
        string baseArrayName = ArrayDesc::makeUnversionedName(arrayName);

        if (!_lock)
        {
            _lock = boost::shared_ptr<SystemCatalog::LockDesc>(new SystemCatalog::LockDesc(baseArrayName,
                                                                                           query->getQueryID(),
                                                                                           Cluster::getInstance()->getLocalInstanceId(),
                                                                                           SystemCatalog::LockDesc::WORKER,
                                                                                           SystemCatalog::LockDesc::WR));
            _lock->setArrayVersion(version);
            shared_ptr<Query::ErrorHandler> ptr(new UpdateErrorHandler(_lock));
            query->pushErrorHandler(ptr);

            Query::Finalizer f = bind(&UpdateErrorHandler::releaseLock,
                                      _lock, _1);
            query->pushFinalizer(f);
            SystemCatalog::ErrorChecker errorChecker = bind(&Query::validate, query);
            bool rc = SystemCatalog::getInstance()->lockArray(_lock, errorChecker);
            if (!rc) {
                throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_CANT_INCREMENT_LOCK)
                << baseArrayName;
            }
        }
        if (srcArray->getArrayDesc().getAttributes().size() != _schema.getAttributes().size())
        {
            srcArray = boost::shared_ptr<Array>(new NonEmptyableArray(srcArray));
        }

        boost::shared_ptr<Array> outputArray;
        if (true) {
            // This is the deprecated behavior as of 14.10
            outputArray = redistribute(srcArray, query, ps, arrayName, instanceID, distMapper);
        } else {
            // This is the behavior to replace the deprecated above,
            // it needs to be complimeted by data collision checks, see #4332
            ArrayID outputArrayId(INVALID_ARRAY_ID);

            if (_schema.isTransient()) {
                outputArray.reset(new MemArray(_schema,query));
                outputArrayId = _schema.getUAId();
                query->pushFinalizer(bind(&recordTransient,outputArray,_1));
            } else  {
                // open persistent array
                outputArray = DBArray::newDBArray(arrayName, query);
                outputArrayId = outputArray->getHandle();
                assert(outputArrayId >0);
                query->getReplicationContext()->enableInboundQueue(outputArrayId, outputArray);
            }

            // redistribute:
            set<Coordinates, CoordinatesLess> newChunkCoordinates;
            redistributeToArray(srcArray, outputArray,  &newChunkCoordinates,
                                query, ps, instanceID, distMapper, 0,
                                shared_ptr<PartitioningSchemaData>(),
                                enforceDataIntegrity);

            if (!_schema.isTransient()) {
                // insert tombstones:
                StorageManager::getInstance().removeDeadChunks(outputArray->getArrayDesc(), newChunkCoordinates, query);
                // stop replication
                query->getReplicationContext()->replicationSync(outputArrayId);
                query->getReplicationContext()->removeInboundQueue(outputArrayId);
                // commit to disk
                StorageManager::getInstance().flush();
            }

            PhysicalBoundaries bounds = PhysicalBoundaries::createFromChunkList(outputArray, newChunkCoordinates);
            SystemCatalog::getInstance()->updateArrayBoundaries(_schema, bounds);
        }
        getInjectedErrorListener().check();

        return outputArray;
    }
};

DECLARE_PHYSICAL_OPERATOR_FACTORY(PhysicalSG, "sg", "impl_sg")

} //namespace
