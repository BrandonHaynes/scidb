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

///
/// @file MPIPhysical.cpp
///
///

// std C++
#include <sstream>
#include <string>

// std C
#include <time.h>

// de-facto standards
#include <boost/make_shared.hpp>
#include <boost/scoped_ptr.hpp>
#include <boost/shared_array.hpp>
#include <log4cxx/logger.h>

// SciDB
#include <array/Metadata.h>
#include <log4cxx/logger.h>
#include <query/Operator.h>

#include <system/Exceptions.h>
#include <system/Utils.h>

#include <mpi/MPIUtils.h>
#include <mpi/MPISlaveProxy.h>
#include <mpi/MPILauncher.h>
#include <mpi/MPIPhysical.hpp>
#include <util/shm/SharedMemoryIpc.h>

using namespace boost;

namespace scidb {
static const bool DBG = false;
static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.query.ops.mpi"));

///
/// some operators may not be able to work in degraded mode while they are being implemented
/// this call can make them exit if that is the case.
/// TODO: add a more explicit message of what is happening
void throwIfDegradedMode(shared_ptr<Query>& query) {
    const boost::shared_ptr<const InstanceMembership> membership =
    Cluster::getInstance()->getInstanceMembership();
    if ((membership->getViewId() != query->getCoordinatorLiveness()->getViewId()) ||
        (membership->getInstances().size() != query->getInstancesCount())) {
        // because we can't yet handle the extra data from
        // replicas that we would be fed in "degraded mode"
        throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_NO_QUORUM2);
    }
}

void MPIPhysical::setQuery(const boost::shared_ptr<Query>& query)
{
    boost::shared_ptr<Query> myQuery = _query.lock();
    if (myQuery) {
        assert(query==myQuery);
        assert(_ctx);
        return;
    }
    PhysicalOperator::setQuery(query);
    _ctx = boost::shared_ptr<MpiOperatorContext>(new MpiOperatorContext(query));
    _ctx = MpiManager::getInstance()->checkAndSetCtx(query,_ctx);
}

void MPIPhysical::postSingleExecute(shared_ptr<Query> query)
{
    // On a non-participating launcher instance it is difficult
    // to determine when the launch is complete without a sync point.
    // postSingleExecute() is run after all instances report success of their execute() phase,
    // that is effectively a sync point.
    assert(query->isCoordinator());
    assert(_mustLaunch);
    assert(_ctx);
    const uint64_t lastIdInUse = _ctx->getLastLaunchIdInUse();

    boost::shared_ptr<MpiLauncher> launcher(_ctx->getLauncher(lastIdInUse));
    assert(launcher);
    if (launcher && launcher == _launcher) {
        LOG4CXX_DEBUG(logger, "MPIPhysical::postSingleExecute: destroying last launcher for launch = " << lastIdInUse);
        assert(lastIdInUse == _launchId);

        launcher->destroy();
        _launcher.reset();
    }
    _ctx.reset();
}

bool MPIPhysical::launchMPISlaves(shared_ptr<Query>& query, const size_t maxSlaves)
{
    LOG4CXX_DEBUG(logger, "MPIPhysical::launchMPISlaves(query, maxSlaves: " << maxSlaves << ") called.");

    assert(maxSlaves <= query->getInstancesCount());

    // This barrier guarantees MPIPhysical::setQuery is called on all instances
    // before any slaves are launched.
    // It also makes sure a non-participating launcher waits for the current launch to finish before starting a new one.
    syncBarrier(0, query);
    syncBarrier(1, query);

    _launchId = _ctx->getNextLaunchId(); // bump the launch ID by 1

    Cluster* cluster = Cluster::getInstance();
    const boost::shared_ptr<const InstanceMembership> membership = cluster->getInstanceMembership();
    const string& installPath = MpiManager::getInstallPath(membership);

    uint64_t lastIdInUse = _ctx->getLastLaunchIdInUse();
    assert(lastIdInUse < _launchId);

    boost::shared_ptr<MpiSlaveProxy> slave;

    // check if our logical ID is within the set of instances that will have a corresponding slave
    InstanceID iID = query->getInstanceID();
    if ( iID < maxSlaves) {
        slave = boost::make_shared<MpiSlaveProxy>(_launchId, query, installPath);
        _ctx->setSlave(slave);
    }

    _mustLaunch = query->isCoordinator();
    if (_mustLaunch) {

        boost::shared_ptr<MpiLauncher> oldLauncher = _ctx->getLauncher(lastIdInUse);
        if (oldLauncher) {
            assert(lastIdInUse == oldLauncher->getLaunchId());
            LOG4CXX_DEBUG(logger, "MPIPhysical::launchMPISlaves(): destroying last launcher for launch = " << lastIdInUse);
            oldLauncher->destroy();
            oldLauncher.reset();
        }
        _launcher = boost::shared_ptr<MpiLauncher>(MpiManager::getInstance()->newMPILauncher(_launchId, query));
        _ctx->setLauncher(_launcher);
        std::vector<std::string> args;
        _launcher->launch(args, membership, maxSlaves);
    }

    if ( iID < maxSlaves) {
        assert(slave);

        //-------------------- Get the handshake
        LOG4CXX_DEBUG(logger, "MPIPhysical::launchMPISlaves(): slave->waitForHandshake() 1 called.");
        slave->waitForHandshake(_ctx);
        LOG4CXX_DEBUG(logger, "MPIPhysical::launchMPISlaves(): slave->waitForHandshake() 1 returned.");
    }

    if ( iID < maxSlaves || _mustLaunch) {
        // After the handshake the old slave must be gone
        LOG4CXX_DEBUG(logger, "MPIPhysical::launchMPISlaves():"
                      << " lastLaunchIdInUse=" << lastIdInUse
                      << " launchId=" << _launchId);


        boost::shared_ptr<MpiSlaveProxy> oldSlave = _ctx->getSlave(lastIdInUse);
        if (oldSlave) {
            assert(lastIdInUse == oldSlave->getLaunchId());
            LOG4CXX_DEBUG(logger, "MPIPhysical::launchMPISlaves(): oldSlave->destroy() & .reset()");
            oldSlave->destroy();
            oldSlave.reset();
        }
        _ctx->complete(lastIdInUse);
    }

    if ( iID < maxSlaves) {

        _ipcName = mpi::getIpcName(installPath, cluster->getUuid(), query->getQueryID(),
                                   cluster->getLocalInstanceId(), _launchId);

        LOG4CXX_DEBUG(logger, "MPIPhysical::launchMPISlaves(): instance " << iID << " slave started.");
        return true;
    } else {
        LOG4CXX_DEBUG(logger, "MPIPhysical::launchMPISlaves(): instance " << iID << " slave bypass.");
        return false;
    }
}

// XXX TODO: consider returning std::vector<scidb::SharedMemoryPtr>
// XXX TODO: which would require supporting different types of memory (double, char etc.)
std::vector<MPIPhysical::SMIptr_t> MPIPhysical::allocateMPISharedMemory(size_t numBufs,
                                                                        size_t elemSizes[],
                                                                        size_t numElems[],
                                                                        string dbgNames[])
{
    LOG4CXX_DEBUG(logger, "MPIPhysical::allocateMPISharedMemory(numBufs "<<numBufs<<",,,)");

    if(logger->isTraceEnabled()) {
        LOG4CXX_TRACE(logger, "MPIPhysical::allocateMPISharedMemory(): allocations are: ");
        for(size_t ii=0; ii< numBufs; ii++) {
            LOG4CXX_TRACE(logger, "MPIPhysical::allocateMPISharedMemory():"
                                   << " elemSizes["<<ii<<"] "<< dbgNames[ii] << " len " << numElems[ii]);
        }
    }

    std::vector<SMIptr_t> shmIpc(numBufs);
    bool preallocate = Config::getInstance()->getOption<bool>(CONFIG_PREALLOCATE_SHARED_MEM);
    for(size_t ii=0; ii<numBufs; ii++) {
        std::stringstream suffix;
        suffix << "." << ii ;
        std::string ipcNameFull= _ipcName + suffix.str();
        LOG4CXX_TRACE(logger, "IPC name = " << ipcNameFull);
        shmIpc[ii] = SMIptr_t(mpi::newSharedMemoryIpc(ipcNameFull, preallocate)); // can I get 'em off ctx instead?
        _ctx->addSharedMemoryIpc(_launchId, shmIpc[ii]);
        // to include a 1000 * (2^31/1000+1) test case
        ssize_t elemBytes = elemSizes[ii] * numElems[ii];
        LOG4CXX_DEBUG(logger, "MPIPhysical::allocateMPISharedMemory():"
                               << " elemSizes["<<ii<<"]= " << elemSizes[ii]
                               << ", numElems["<<ii<<"]= " << numElems[ii]
                               << ", elemBytes= " << elemBytes );
        ASSERT_EXCEPTION(elemBytes >= 0, "bad elemBytes");
        char* ptr = MpiLauncher::initIpcForWrite(shmIpc[ii].get(), elemBytes);
        assert(ptr); ptr=ptr;
    }
    return shmIpc;
}

void MPIPhysical::releaseMPISharedMemoryInputs(std::vector<MPIPhysical::SMIptr_t>& shmIpc, size_t resultIpcIndx)
{
    for(size_t i=0; i<shmIpc.size(); i++) {
        if (!shmIpc[i]) {
            continue;
        }
        SharedMemoryIpc *ipc = shmIpc[i].get();
        ipc->close();

        if (i!=resultIpcIndx) {
            ipc->unmap();
        }
        if (!ipc->remove()) {
            throw (SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_OPERATION_FAILED) << "shared_memory_remove");
        }
    }
}

} // namespace
