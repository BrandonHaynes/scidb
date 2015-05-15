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
 * @file PhysicalInput.cpp
 *
 * @author roman.simakov@gmail.com
 *
 * Physical implementation of INPUT operator for inputing data from text file
 * which is located on coordinator
 */

#include <string.h>
#include <log4cxx/logger.h>

#include "query/Operator.h"
#include "query/QueryProcessor.h"
#include "network/NetworkManager.h"
#include "InputArray.h"
#include "system/Cluster.h"

using namespace std;
using namespace boost;

namespace scidb
{

// Logger for network subsystem. static to prevent visibility of variable outside of file
static log4cxx::LoggerPtr oplogger(log4cxx::Logger::getLogger("scidb.ops.impl_input"));

class PhysicalInput : public PhysicalOperator
{
public:
    PhysicalInput(std::string const& logicalName,
                  std::string const& physicalName,
                  Parameters const& parameters,
                  ArrayDesc const& schema):
    PhysicalOperator(logicalName, physicalName, parameters, schema),
    _shadowVersion(0), _shadowAID(INVALID_ARRAY_ID), _shadowUAID(INVALID_ARRAY_ID)
    {
    }

    virtual bool changesDistribution(std::vector<ArrayDesc> const&) const
    {
        return true;
    }

    int64_t getSourceInstanceID() const
    {
        if (_parameters.size() >= 3)
        {
            assert(_parameters[2]->getParamType() == PARAM_PHYSICAL_EXPRESSION);
            boost::shared_ptr<OperatorParamPhysicalExpression> paramExpr = (boost::shared_ptr<OperatorParamPhysicalExpression>&)_parameters[2];
            assert(paramExpr->isConstant());
            return paramExpr->getExpression()->evaluate().getInt64();
        }
        return COORDINATOR_INSTANCE_MASK;
    }

    virtual ArrayDistribution getOutputDistribution(
            std::vector<ArrayDistribution> const&,
            std::vector<ArrayDesc> const&) const
    {
        InstanceID sourceInstanceID = getSourceInstanceID();
        if (sourceInstanceID == ALL_INSTANCE_MASK) {
            //The file is loaded from multiple instances - the distribution could be possibly violated - assume the worst
            return ArrayDistribution(psUndefined);
        }
        else {
            return ArrayDistribution(psLocalInstance,
                                     boost::shared_ptr<DistributionMapper>(),
                                     sourceInstanceID);
        }
    }

    void preSingleExecute(boost::shared_ptr<Query> query)
    {
        string shadowArrayName;

        if (_parameters.size() >= 6 &&
            _parameters[5]->getParamType() == PARAM_ARRAY_REF) {
            shadowArrayName = ((boost::shared_ptr<OperatorParamArrayReference>&)_parameters[5])->getObjectName();
        } else {
            // no shadow array
            return;
        }

        shared_ptr<const InstanceMembership> membership(Cluster::getInstance()->getInstanceMembership());
        assert(membership);
        if ((membership->getViewId() != query->getCoordinatorLiveness()->getViewId()) ||
            (membership->getInstances().size() != query->getInstancesCount())) {
            throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_NO_QUORUM2);
        }

        //All arrays are currently stored as round-robin. Let's store shadow arrays round-robin as well
        //TODO: revisit this when we allow users to store arrays with specified distributions
        PartitioningSchema ps = psHashPartitioned;
        ArrayDesc shadowArrayDesc = InputArray::generateShadowArraySchema(_schema, shadowArrayName);
        assert(shadowArrayName == shadowArrayDesc.getName());

        LOG4CXX_DEBUG(oplogger, "Preparing catalog for shadow array " << shadowArrayName);
        assert(query->isCoordinator());

        boost::shared_ptr<SystemCatalog::LockDesc> lock(new SystemCatalog::LockDesc(shadowArrayName,
                                                                                    query->getQueryID(),
                                                                                    Cluster::getInstance()->getLocalInstanceId(),
                                                                                    SystemCatalog::LockDesc::COORD,
                                                                                    SystemCatalog::LockDesc::WR));
        shared_ptr<Query::ErrorHandler> ptr(new UpdateErrorHandler(lock));
        query->pushErrorHandler(ptr);

        ArrayDesc desc;
        bool arrayExists = SystemCatalog::getInstance()->getArrayDesc(shadowArrayName, desc, false);
        VersionID lastVersion = 0;
        if (!arrayExists) {
            lock->setLockMode(SystemCatalog::LockDesc::CRT);
            bool updatedArrayLock = SystemCatalog::getInstance()->updateArrayLock(lock);
            SCIDB_ASSERT(updatedArrayLock);
            desc = shadowArrayDesc;
            SystemCatalog::getInstance()->addArray(desc, ps);
        } else {
            if (desc.getAttributes().size() != shadowArrayDesc.getAttributes().size() ||
                desc.getDimensions().size() != shadowArrayDesc.getDimensions().size())
            {
                throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_ARRAY_ALREADY_EXIST) << desc.getName();
            }
            lastVersion = SystemCatalog::getInstance()->getLastVersion(desc.getId());
        }
        _shadowVersion = lastVersion+1;
        LOG4CXX_DEBUG(oplogger, "Use version " << _shadowVersion << " of shadow array " << shadowArrayName);
        _shadowUAID = desc.getId();
        lock->setArrayId(_shadowUAID);
        lock->setArrayVersion(_shadowVersion);
        bool updatedArrayLock = SystemCatalog::getInstance()->updateArrayLock(lock);
        SCIDB_ASSERT(updatedArrayLock);

        string shadowArrayVersionName = ArrayDesc::makeVersionedName(shadowArrayName, _shadowVersion);
        LOG4CXX_DEBUG(oplogger, "Create shadow array " << shadowArrayVersionName);
        shadowArrayDesc = ArrayDesc(shadowArrayVersionName,  desc.getAttributes(), desc.getDimensions());
        SystemCatalog::getInstance()->addArray(shadowArrayDesc, ps);

        _shadowAID = shadowArrayDesc.getId();
        lock->setArrayVersionId(shadowArrayDesc.getId());
        updatedArrayLock = SystemCatalog::getInstance()->updateArrayLock(lock);
        SCIDB_ASSERT(updatedArrayLock);
    }

    void postSingleExecute(boost::shared_ptr<Query> query)
    {
        if(_shadowUAID != INVALID_ARRAY_ID) {
            assert (_parameters.size() >= 6 &&
                    _parameters[5]->getParamType() == PARAM_ARRAY_REF);

            VersionID newVersionID = SystemCatalog::getInstance()->createNewVersion(_shadowUAID, _shadowAID);
            LOG4CXX_DEBUG(oplogger, "Created new shadow version " << newVersionID << " of shadow array ID" << _shadowAID);
            assert(newVersionID == _shadowVersion);
        }
    }

    boost::shared_ptr<Array> execute(vector< boost::shared_ptr<Array> >& inputArrays,
                                     boost::shared_ptr<Query> query)
    {
        assert(inputArrays.size() == 0);
        assert(_parameters.size() >= 2);

        assert(_parameters[1]->getParamType() == PARAM_PHYSICAL_EXPRESSION);
        boost::shared_ptr<OperatorParamPhysicalExpression> paramExpr = (boost::shared_ptr<OperatorParamPhysicalExpression>&)_parameters[1];
        assert(paramExpr->isConstant());
        const string fileName = paramExpr->getExpression()->evaluate().getString();

        InstanceID sourceInstanceID = getSourceInstanceID();

        if (sourceInstanceID != COORDINATOR_INSTANCE_MASK &&
            sourceInstanceID != ALL_INSTANCE_MASK &&
            (size_t)sourceInstanceID >= query->getInstancesCount())
            throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_INVALID_INSTANCE_ID) << sourceInstanceID;

        if (sourceInstanceID == COORDINATOR_INSTANCE_MASK) {
            sourceInstanceID = (query->isCoordinator() ? query->getInstanceID() : query->getCoordinatorID());
        }

        int64_t maxErrors = 0;
        string shadowArrayName;
        InstanceID myInstanceID = query->getInstanceID();
        bool enforceDataIntegrity = false;
        string format;
        if (_parameters.size() >= 4)
        {
            assert(_parameters[3]->getParamType() == PARAM_PHYSICAL_EXPRESSION);
            paramExpr = (boost::shared_ptr<OperatorParamPhysicalExpression>&)_parameters[3];
            assert(paramExpr->isConstant());
            format = paramExpr->getExpression()->evaluate().getString();
            if (_parameters.size() >= 5)
            {
                assert(_parameters[4]->getParamType() == PARAM_PHYSICAL_EXPRESSION);
                paramExpr = (boost::shared_ptr<OperatorParamPhysicalExpression>&)_parameters[4];
                assert(paramExpr->isConstant());
                maxErrors = paramExpr->getExpression()->evaluate().getInt64();
                if (_parameters.size() >= 6)
                {
                    if (_parameters[5]->getParamType() == PARAM_ARRAY_REF) {
                        shadowArrayName = ((boost::shared_ptr<OperatorParamArrayReference>&)_parameters[5])->getObjectName();
                        if (_shadowVersion > 0) {
                            shadowArrayName = ArrayDesc::makeVersionedName(shadowArrayName, _shadowVersion);
                            assert(_shadowAID != INVALID_ARRAY_ID && _shadowUAID != INVALID_ARRAY_ID);
                        }
                    } else {
                        assert(_parameters[5]->getParamType() == PARAM_PHYSICAL_EXPRESSION);
                        OperatorParamPhysicalExpression* paramExpr = static_cast<OperatorParamPhysicalExpression*>(_parameters[5].get());
                        assert(paramExpr->isConstant());
                        enforceDataIntegrity = paramExpr->getExpression()->evaluate().getBool();
                    }
                    if (_parameters.size() >= 7) {
                        assert(_parameters[5]->getParamType() == PARAM_ARRAY_REF);
                        assert(_parameters[6]->getParamType() == PARAM_PHYSICAL_EXPRESSION);
                        OperatorParamPhysicalExpression* paramExpr = static_cast<OperatorParamPhysicalExpression*>(_parameters[6].get());
                        assert(paramExpr->isConstant());
                        enforceDataIntegrity = paramExpr->getExpression()->evaluate().getBool();
                    }
                    assert(_parameters.size() <= 7);
                }
            }
        }

        boost::shared_ptr<Array> result;
        bool emptyArray = (sourceInstanceID != ALL_INSTANCE_MASK &&
                           sourceInstanceID != myInstanceID);
        InputArray* ary = new InputArray(_schema, format, query,
                                         emptyArray,
                                         enforceDataIntegrity,
                                         maxErrors,
                                         shadowArrayName,
                                         sourceInstanceID == ALL_INSTANCE_MASK);
        result.reset(ary);

        if (emptyArray) {
            // No need to actually open the file.  (In fact, if the file is a pipe and
            // double-buffering is enabled, opening it would wrongly steal data intended for
            // some other instance!  See ticket #4466.)
            SCIDB_ASSERT(ary->inEmptyMode());
        } else {
            try
            {
                ary->openFile(fileName);
            }
            catch(const Exception& e)
            {
                if (e.getLongErrorCode() != SCIDB_LE_CANT_OPEN_FILE)
                {
                    // Only expecting an open failure, but whatever---pass it up.
                    throw;
                }

                if (sourceInstanceID == myInstanceID)
                {
                    // If mine is the one-and-only load instance, let
                    // callers see the open failure.
                    throw;
                }

                // No *local* file to load... but we must return the
                // InputArray result, since even in its failed state it
                // knows how to cooperate with subsequent SG pulls of the
                // shadow array.  An empty MemArray won't do.
                //
                // The open failure itself has already been logged.

                assert(ary->inEmptyMode()); // ... regardless of emptyArray value above.
            }
        }

        return result;
    }

    private:
    VersionID _shadowVersion;
    ArrayID _shadowAID;
    ArrayID _shadowUAID;
};

DECLARE_PHYSICAL_OPERATOR_FACTORY(PhysicalInput, "input", "impl_input")

} //namespace
