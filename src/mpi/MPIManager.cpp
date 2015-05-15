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

#include <sys/types.h>
#include <signal.h>
#include <ostream>
#include <boost/function.hpp>
#include <boost/shared_ptr.hpp>
#include <boost/bind.hpp>
#include <boost/make_shared.hpp>
#include <log4cxx/logger.h>
#include <system/Cluster.h>
#include <system/Utils.h>
#include <system/Config.h>
#include <util/Network.h>
#include <util/FileIO.h>
#include <mpi/MPIManager.h>
#include <mpi/MPISlaveProxy.h>
#include <mpi/MPIUtils.h>
#include <network/proto/scidb_msg.pb.h>

using namespace std;

namespace scidb
{
namespace mpi
{
SharedMemoryIpc::SharedMemoryIpcType_t getShmIpcType()
{
    if (scidb::Config::getInstance()->getOption<string>(CONFIG_MPI_SHM_TYPE) == "FILE") {
        return SharedMemoryIpc::FILE_TYPE;
    }
    return SharedMemoryIpc::SHM_TYPE;
}
}

static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.mpi"));

bool startsWith(const std::string& left, const std::string& right)
{
  if (right.size() > left.size()) {
    return false;
  }
  return (0 == left.compare(0, right.size(), right));
}

MpiManager::MpiManager()
  : _isReady(false),
    _mpiResourceTimeout(MPI_RESOURCE_TIMEOUT_SEC)
{
    const time_t MIN_CLEANUP_PERIOD_SEC = 5;
    scidb::Scheduler::Work workItem = boost::bind(&MpiManager::initiateCleanup);
    _cleanupScheduler = scidb::getScheduler(workItem, MIN_CLEANUP_PERIOD_SEC);

    string mpiTypeStr = scidb::Config::getInstance()->getOption<string>(CONFIG_MPI_TYPE);
    assert(!mpiTypeStr.empty());
    _mpiInstallDir = scidb::Config::getInstance()->getOption<string>(CONFIG_MPI_DIR);
    assert(!_mpiInstallDir.empty());

    if (startsWith(mpiTypeStr, mpi::getMpiTypeStr(mpi::MPICH12)) ) {
      _mpiType = mpi::MPICH12;
      _mpiDaemonBin = mpi::MPICH12_DAEMON_BIN;
      _mpiLauncherBin = mpi::MPICH_LAUNCHER_BIN;
    } else if (startsWith(mpiTypeStr, mpi::getMpiTypeStr(mpi::MPICH14)) ) {
      _mpiType = mpi::MPICH14;
      _mpiDaemonBin = mpi::MPICH_DAEMON_BIN;
      _mpiLauncherBin = mpi::MPICH_LAUNCHER_BIN;
    } else if (startsWith(mpiTypeStr, mpi::getMpiTypeStr(mpi::OMPI16)) ) {
      _mpiType = mpi::OMPI16;
      _mpiDaemonBin = mpi::OMPI_DAEMON_BIN;
      _mpiLauncherBin = mpi::OMPI_LAUNCHER_BIN;
    } else {
      throw (SYSTEM_EXCEPTION(SCIDB_SE_CONFIG, SCIDB_LE_ERROR_IN_CONFIGURATION_FILE)
             << string("Unrecognized MPI type value: ")+mpiTypeStr);
   }
}

void MpiManager::initiateCleanup()
{
    scidb::WorkQueue::WorkItem item = boost::bind(&MpiErrorHandler::cleanAll);
    boost::shared_ptr<scidb::WorkQueue> q = scidb::getWorkQueue();
    try {
        q->enqueue(item);
    } catch (const scidb::WorkQueue::OverflowException& e) {
        LOG4CXX_ERROR(logger, "Overflow exception from the work queue: "<<e.what());
        assert(false);
        // it is not fatal, try again
        MpiManager::getInstance()->cleanup();
    }
}
void MpiManager::cleanup()
{
    _cleanupScheduler->schedule();
}
void MpiManager::init()
{
    boost::shared_ptr<scidb::NetworkMessageFactory> factory = scidb::getNetworkMessageFactory();

    boost::shared_ptr<MpiMessageHandler> msgHandler(new MpiMessageHandler());
    factory->addMessageType(scidb::mtMpiSlaveHandshake,
                            boost::bind(&MpiMessageHandler::createMpiSlaveHandshake, msgHandler,_1),
                            boost::bind(&MpiMessageHandler::handleMpiSlaveHandshake, msgHandler,_1));

    factory->addMessageType(scidb::mtMpiSlaveResult,
                            boost::bind(&MpiMessageHandler::createMpiSlaveResult, msgHandler,_1),
                            boost::bind(&MpiMessageHandler::handleMpiSlaveResult, msgHandler,_1));

    scidb::NetworkMessageFactory::MessageHandler emptyHandler;
    factory->addMessageType(scidb::mtMpiSlaveCommand,
                            boost::bind(&MpiMessageHandler::createMpiSlaveCommand, msgHandler,_1),
                            emptyHandler);
}

/**
 * Creates symbolic links to mpirun, orted, and scidb_mpi_slave
 * @return true if links are created (or have already existed), false otherwise
 */
void MpiManager::initMpiLinks(const std::string& installPath,
                              const std::string& mpiPath,
                              const std::string& pluginPath)
{
    // mpi
    string target = getMpiDir(installPath);

    int rc = ::symlink(mpiPath.c_str(), target.c_str());
    if (rc !=0 && errno != EEXIST) {
        int err = errno;
        throw (SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_SYSCALL_ERROR)
                       << "symlink" << rc << err << target);
    }

    // mpi_slave_scidb
    target = mpi::getSlaveBinFile(installPath);
    rc = ::symlink(mpi::getSlaveSourceBinFile(pluginPath).c_str(), target.c_str());
    if (rc !=0 && errno != EEXIST) {
        int err = errno;
        throw (SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_SYSCALL_ERROR)
               << "symlink" << rc << err << target);
    }
}

void MpiManager::forceInitMpi()
{
    ScopedMutexLock lock(_mutex);
    initMpi();
}

void MpiManager::initMpi()
{
    const boost::shared_ptr<const InstanceMembership> membership =
        Cluster::getInstance()->getInstanceMembership();
    const std::string& installPath = MpiManager::getInstallPath(membership);

    std::string dir = mpi::getLogDir(installPath);
    if (!scidb::File::createDir(dir)) {
        throw (SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_CANT_CREATE_DIRECTORY)
               << dir);
    }
    dir = mpi::getPidDir(installPath);
    if (!scidb::File::createDir(dir)) {
        throw (SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_CANT_CREATE_DIRECTORY)
               << dir);
    }
    if (mpi::getShmIpcType() == SharedMemoryIpc::FILE_TYPE) {
        dir = mpi::getIpcDir(installPath);
        if (!scidb::File::createDir(dir)) {
            throw (SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_CANT_CREATE_DIRECTORY)
                   << dir);
        }
    }
    initMpiLinks(installPath,
                 scidb::Config::getInstance()->getOption<string>(CONFIG_MPI_DIR),
                 scidb::Config::getInstance()->getOption<string>(CONFIG_PLUGINSDIR));
    _isReady = true;
    // read the proc table and kill all the mpi procs started by the old process of this instance
    MpiErrorHandler::killAllMpiProcs();
    cleanup();
}

bool MpiManager::checkForError(scidb::QueryID queryId, double startTime, double timeout)
{
    Query::validateQueryPtr(Query::getQueryByID(queryId));
    return (!mpi::hasExpired(startTime, timeout));
}

boost::shared_ptr<MpiOperatorContext>
MpiManager::checkAndSetCtx(const boost::shared_ptr<Query>& query,
                           const boost::shared_ptr<MpiOperatorContext>& ctx)
{
    const scidb::QueryID queryId = query->getQueryID();
    LOG4CXX_TRACE(logger, "MpiManager::checkAndSetCtx: queryID="<<queryId << ", ctx=" << ctx.get());
    ScopedMutexLock lock(_mutex);
    if (!_isReady) {
        assert(ctx);
        initMpi();
    }
    // XXX TODO: Currently we cannot run more than one query which use scalapack concurrently.
    //           The issue is that scidb_blacs_gridinfo_() from src/dense_linear_algebra/dlaScaLA/scalapackEmulation/blacs_info_fake.c
    //           uses a set of static variables. Note though that cascading scalapack operators is allowed because
    //           each scalapack operator is followed by a blocking SG and the operator tree is executed sequentially
    //           in a depth-first order. The other sources of concurrency such as
    //           ParrallelAccumulatorArray, Physical(Redimension)Store
    //           should not be a problem for the same reason (of blocking SG).
    //           This also implies that MpiOperatorContext must be inserted by the operator first (not by the slave messages).
    //           Hence, assert(ctx) above.
    const double start = mpi::getTimeInSecs();
    const double timeout(_mpiResourceTimeout);
    bool waitResult(true);
    while (_ctxMap.find(queryId) == _ctxMap.end() && _ctxMap.size() != 0) {
        if (!waitResult) {
            // we alredy unsuccessfully tried to wait
            throw (SYSTEM_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_RESOURCE_BUSY)
                   << "too many queries");
        }
        Event::ErrorChecker ec =
           boost::bind(&MpiManager::checkForError, queryId, start, timeout);
        waitResult = _event.wait(_mutex, ec);
    }
    return checkAndSetCtxAsync(query, ctx);
}

boost::shared_ptr<MpiOperatorContext>
MpiManager::checkAndSetCtxAsync(const boost::shared_ptr<Query>& query,
                                const boost::shared_ptr<MpiOperatorContext>& ctx)
{
    const scidb::QueryID queryId = query->getQueryID();

    LOG4CXX_TRACE(logger, "MpiManager::checkAndSetCtxAsync: queryID="<<queryId << ", ctx=" << ctx.get());
    ScopedMutexLock lock(_mutex);

    query->validate();

    std::pair<ContextMap::iterator, bool> res = _ctxMap.insert(ContextMap::value_type(queryId, ctx));
    if (res.second) { // did it get inserted ?
        assert((*res.first).second == ctx);
        try {
            if (!ctx) {
                assert(false);
                throw (SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_UNREACHABLE_CODE)
                       << "MpiManager::checkAndSetCtxAsync");
            }
            boost::shared_ptr<MpiErrorHandler> eh(new MpiErrorHandler(ctx));
            Query::Finalizer f = boost::bind(&MpiErrorHandler::finalize, eh, _1);
            query->pushFinalizer(f);
            query->pushErrorHandler(eh);
        } catch (const scidb::Exception& e) {
            removeCtx(queryId);
            cleanup();
            throw;
        }
        cleanup();
    }
    return (*res.first).second;
}

bool MpiManager::removeCtx(scidb::QueryID queryId)
{
    LOG4CXX_DEBUG(logger, "MpiManager::removeCtx: queryID="<<queryId);
    ScopedMutexLock lock(_mutex);
    return (_ctxMap.erase(queryId) > 0);
}

void MpiOperatorContext::setSlave(const boost::shared_ptr<MpiSlaveProxy>& slave)
{
    setSlaveInternal(slave->getLaunchId(), slave);
}

void
MpiOperatorContext::setSlaveInternal(uint64_t launchId,
                                     const boost::shared_ptr<MpiSlaveProxy>& slave)
{
    ScopedMutexLock lock(_mutex);
    LaunchMap::iterator iter = getIter(launchId);
    iter->second->_slave = slave;
}

void
MpiOperatorContext::setLauncherInternal(uint64_t launchId,
                                        const boost::shared_ptr<MpiLauncher>& launcher)
{
    ScopedMutexLock lock(_mutex);
    LaunchMap::iterator iter = getIter(launchId);
    iter->second->_launcher = launcher;
}

bool
MpiOperatorContext::addSharedMemoryIpc(uint64_t launchId,
                                       const boost::shared_ptr<SharedMemoryIpc>& ipc)
{
    ScopedMutexLock lock(_mutex);
    LaunchMap::iterator iter = getIter(launchId);
    bool isInserted = iter->second->_shmIpcs.insert(ipc).second;
    assert(isInserted);
    return isInserted;
}

boost::shared_ptr<MpiSlaveProxy>
MpiOperatorContext::getSlave(uint64_t launchId)
{
    ScopedMutexLock lock(_mutex);
    LaunchMap::iterator iter = _launches.find(launchId);
    if (iter == _launches.end()) {
        return boost::shared_ptr<MpiSlaveProxy>();
    }
    return iter->second->_slave;
}

boost::shared_ptr<MpiLauncher>
MpiOperatorContext::getLauncher(uint64_t launchId)
{
    ScopedMutexLock lock(_mutex);
    LaunchMap::iterator iter = _launches.find(launchId);
    if (iter == _launches.end()) {
        return boost::shared_ptr<MpiLauncher>();
    }
    return iter->second->_launcher;
}

boost::shared_ptr<SharedMemoryIpc>
MpiOperatorContext::getSharedMemoryIpc(uint64_t launchId, const std::string& name)
{
    ScopedMutexLock lock(_mutex);
    LaunchMap::iterator iter = _launches.find(launchId);
    if (iter == _launches.end()) {
        return boost::shared_ptr<SharedMemoryIpc>();
    }
    boost::shared_ptr<SharedMemoryIpc> key(new SharedMemory(name));
    LaunchInfo::ShmIpcSet::iterator ipcIter = iter->second->_shmIpcs.find(key);
    if (ipcIter == iter->second->_shmIpcs.end()) {
        return boost::shared_ptr<SharedMemoryIpc>();
    }
    return (*ipcIter);
}

boost::shared_ptr<scidb::ClientMessageDescription>
MpiOperatorContext::popMsg(uint64_t launchId,
                           LaunchErrorChecker& errChecker)
{
    ScopedMutexLock lock(_mutex);
    LaunchMap::iterator iter = _launches.find(launchId);
    while ((iter == _launches.end()) ||
           (!iter->second->_msg)) {
        Event::ErrorChecker ec =
        boost::bind(&MpiOperatorContext::checkForError, this, launchId, errChecker);
        _event.wait(_mutex, ec);
        iter = _launches.find(launchId);
    }
    boost::shared_ptr<scidb::ClientMessageDescription> msg;
    (iter->second->_msg).swap(msg);
    return msg;
}

void
MpiOperatorContext::pushMsg(uint64_t launchId,
                            const boost::shared_ptr<scidb::ClientMessageDescription>& msg)
{
    ScopedMutexLock lock(_mutex);
    const bool dontUpdateLastInUse = false;
    LaunchMap::iterator iter = getIter(launchId, dontUpdateLastInUse);
    iter->second->_msg = msg;

    _event.signal();
}

void MpiOperatorContext::clear(LaunchCleaner& cleaner)
{
    if (cleaner) {
        for (LaunchMap::iterator iter = _launches.begin();
             iter != _launches.end(); ++iter) {
            cleaner(iter->first, iter->second.get());
        }
    }
    _launches.clear();
}

bool
MpiOperatorContext::checkForError(uint64_t launchId, LaunchErrorChecker& errChecker)
{
    Query::validateQueryPtr(_query.lock());

    LaunchMap::iterator iter = _launches.find(launchId);
    if (iter != _launches.end() && iter->second->_msg) {
        // the message is ready, we must have missed the signal on timeout
        return false;
    }
    if (errChecker && !errChecker(launchId, this)) {
        return false;
    }
    return true;
}

MpiOperatorContext::LaunchMap::iterator
MpiOperatorContext::getIter(uint64_t launchId, bool updateLastLaunchId)
{
    LaunchMap::iterator iter = _launches.find(launchId);
    if (iter == _launches.end()) {

        if (_lastLaunchIdInUse >= launchId) {
            // When we are populating a new context from the operator,
            // the last launch ID in use is not allowed to decrease.
            // The contract is that the launch IDs must strictly increase.
            throw (SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_UNKNOWN_ERROR)
                   << "MPI-based operator context does not allow for decreasing launch IDs");
        }
        if (_launches.size() > 1) {
            // each launch must be serialized by the coordinator,
            // workers also process launches serially, so at any moment
            // there can be messages from at most 2 launches (slaves).
            throw (SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_UNKNOWN_ERROR)
                   << "MPI-based operator context is corrupted");
        }
        boost::shared_ptr<LaunchInfo> linfo(new LaunchInfo);
        iter = _launches.insert(make_pair(launchId, linfo)).first;
    }
    if (updateLastLaunchId) {
        _lastLaunchIdInUse = std::max(_lastLaunchIdInUse,launchId);
    }
    return iter;
}

MessagePtr MpiMessageHandler::createMpiSlaveCommand(MessageID id)
{
    return scidb::MessagePtr(new scidb_msg::MpiSlaveCommand());
}

MessagePtr MpiMessageHandler::createMpiSlaveHandshake(MessageID id)
{
    return scidb::MessagePtr(new scidb_msg::MpiSlaveHandshake());
}

void MpiMessageHandler::handleMpiSlaveHandshake(const boost::shared_ptr<scidb::MessageDescription>& messageDesc)
{
    handleMpiSlaveMessage<scidb_msg::MpiSlaveHandshake, scidb::mtMpiSlaveHandshake>(messageDesc);
}

MessagePtr MpiMessageHandler::createMpiSlaveResult(MessageID id)
{
    return scidb::MessagePtr(new scidb_msg::MpiSlaveResult());
}

void MpiMessageHandler::handleMpiSlaveResult(const boost::shared_ptr<scidb::MessageDescription>& messageDesc)
{
    handleMpiSlaveMessage<scidb_msg::MpiSlaveResult, scidb::mtMpiSlaveResult>(messageDesc);
}

void MpiMessageHandler::handleMpiSlaveDisconnect(uint64_t launchId,
                                                 const boost::shared_ptr<scidb::Query>& query)
{
    if (!query) {
        return;
    }
    boost::shared_ptr< scidb::ClientMessageDescription >
        cliMsg(new MpiOperatorContext::EofMessageDescription());
    try {
        MpiMessageHandler::processMessage(launchId, cliMsg, query);
    } catch (const scidb::Exception& e) {
        query->handleError(e.copy());
    }
}

void MpiMessageHandler::processMessage(uint64_t launchId,
                                       const boost::shared_ptr<scidb::ClientMessageDescription>& cliMsg,
                                       const boost::shared_ptr<scidb::Query>& query)
{
    try {
        boost::shared_ptr<MpiOperatorContext> emptyCtx;
        boost::shared_ptr<MpiOperatorContext> ctx =
            MpiManager::getInstance()->checkAndSetCtxAsync(query, emptyCtx);
        assert(ctx);

        if (!ctx) {
            // the context did not exist before this call
            // XXX TODO: Currently, we do not allow more than one MPI query to execute concurrently.
            //           The serialization is implemented in MpiManager::checkAndSetCtx() called by MPIPhysical.
            //           That method can block for a long time; therefore, we must not call it
            //           when processing a client message (on the network thread).
            //           So, to still guarantee the serialized query execution,
            //           MPIPhysical::launchSlaves() inserts a barrier which guarantees
            //           that all instances insert a new MpiOperatorContext before any slaves are spawned
            //           (and any slave message are sent). Here, we error out if the context is not already set.
            //           When the serialized access restriction is lifted, we should be able to remove the barrier
            //           and allow MpiMessageHandler::processMessage() insert a new context if it does not exist.
            assert(false);
            throw (SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_UNKNOWN_ERROR)
                   << "MPI-based operator context not found");
        }
        ctx->pushMsg(launchId, cliMsg);

        LOG4CXX_DEBUG(logger, "MpiMessageHandler::processMessage: queryID="<< query->getQueryID()
                      << ", ctx = " << ctx.get()
                      << ", launchId = " << launchId
                      << ", messageType = " << cliMsg->getMessageType()
                      << ", queryID = " << cliMsg->getQueryId());
    }
    catch (const scidb::Exception& e)
    {
        LOG4CXX_ERROR(logger, "MpiMessageHandler::processMessage:"
                      " Error occurred in slave message handler: "
                      << e.what()
                      << ", messageType = " << cliMsg->getMessageType()
                      << ", queryID = " << cliMsg->getQueryId());
        throw;
    }
}

MpiErrorHandler::~MpiErrorHandler()
{}

void MpiErrorHandler::handleError(const boost::shared_ptr<Query>& query)
{
    MpiManager::getInstance()->removeCtx(query->getQueryID());

    MpiOperatorContext::LaunchCleaner cleaner =
        boost::bind(&MpiErrorHandler::clean, query->getQueryID(), _1, _2);
    _ctx->clear(cleaner);
}

void MpiErrorHandler::finalize(const boost::shared_ptr<Query>& query)
{
    MpiManager::getInstance()->removeCtx(query->getQueryID());

    const uint64_t lastIdInUse = _ctx->getLastLaunchIdInUse();

    LOG4CXX_TRACE(logger, "MpiErrorHandler::finalize: destorying last slave for launch = " << lastIdInUse);

    boost::shared_ptr<MpiSlaveProxy> slave = _ctx->getSlave(lastIdInUse);
    if (slave) {
        slave->destroy();
    }
}

void MpiErrorHandler::clean(scidb::QueryID queryId, uint64_t launchId,
                            MpiOperatorContext::LaunchInfo* info)
{
    MpiOperatorContext::LaunchInfo::ShmIpcSet& ipcs = info->_shmIpcs;
    for (MpiOperatorContext::LaunchInfo::ShmIpcSet::const_iterator ipcIter = ipcs.begin();
         ipcIter != ipcs.end(); ++ipcIter) {
        const boost::shared_ptr<SharedMemoryIpc>& ipc = *ipcIter;

        ipc->close();
        if (!ipc->remove()) {
            LOG4CXX_ERROR(logger, "Failed to remove shared memory IPC = "
                          << ipc->getName());
        }
    }
    if (info->_slave) {
        try {
            info->_slave->destroy(true);
            assert(launchId == info->_slave->getLaunchId());
        } catch (std::exception& e) {
            LOG4CXX_ERROR(logger, "Failed to destroy slave for launch = "
                                  << info->_slave->getLaunchId()
                          << " ("<< queryId << ")"
                          << " because: "<<e.what());
        }
    }
    if (info->_launcher) {
        try {
            info->_launcher->destroy(true);
            assert(launchId == info->_launcher->getLaunchId());
        } catch (std::exception& e) {
            LOG4CXX_ERROR(logger, "Failed to destroy launcher for launch = "
                          << info->_launcher->getLaunchId()
                          << " ("<< queryId << ")"
                          << " because: "<<e.what());
        }
    }
}

static void getQueryId(std::set<scidb::QueryID>* queryIds,
                       const boost::shared_ptr<scidb::Query>& q)
{
    queryIds->insert(q->getQueryID());
    LOG4CXX_DEBUG(logger, "Next query ID: "<<q->getQueryID());
}

void MpiErrorHandler::cleanupLauncherPidFile(const std::string& installPath,
                                             const std::string& clusterUuid,
                                             const std::string& fileName)
{
    const char *myFuncName = "MpiErrorHandler::cleanupLauncherPidFile";
    std::vector<pid_t> pids;
    if (!mpi::readPids(fileName, pids)) {
        LOG4CXX_WARN(logger, myFuncName << ": cannot read pids for: "<<fileName);
        return;
    }
    assert(pids.size()==2);
    LOG4CXX_DEBUG(logger,  myFuncName << ": killing launcher pid group = " << pids[0]);

    // kill launcher's proc group
    if (!killProc(installPath, clusterUuid, -pids[0])) {
        LOG4CXX_DEBUG(logger,  myFuncName << ": removing file: "<<fileName);
        scidb::File::remove(fileName.c_str(), false);
    }
}

void MpiErrorHandler::cleanupSlavePidFile(const std::string& installPath,
                                          const std::string& clusterUuid,
                                          const std::string& fileName,
                                          QueryID queryId)
{
    const char *myFuncName = "MpiErrorHandler::cleanupSlavePidFile";
    std::vector<pid_t> pids;
    if (!mpi::readPids(fileName, pids)) {
        LOG4CXX_WARN(logger, myFuncName << ": cannot read pids for: "<<fileName);
        return;
    }
    bool removeFile = true;

    // kill slave
    LOG4CXX_DEBUG(logger, myFuncName << ": killing slave pid = "<<pids[0]);
    if (killProc(installPath, clusterUuid, pids[0], queryId)) {
        removeFile = false;
    }

    // kill slave parent (orted/hydra_pmi_proxy)
    LOG4CXX_DEBUG(logger, myFuncName << ": killing slave ppid ="<<pids[1]);
    if (killProc(installPath, clusterUuid, pids[1], queryId)) {
        removeFile = false;
    }
    if (removeFile) {
        LOG4CXX_DEBUG(logger, myFuncName << ": removing file: "<<fileName);
        scidb::File::remove(fileName.c_str(), false);
    }
}

void MpiErrorHandler::cleanAll()
{
    const char *myFuncName = "MpiErrorHandler::cleanAll";
    // read ipc files
    const boost::shared_ptr<const InstanceMembership> membership =
        Cluster::getInstance()->getInstanceMembership();
    const std::string& installPath = MpiManager::getInstallPath(membership);

    std::string uuid = Cluster::getInstance()->getUuid();
    InstanceID myInstanceId = Cluster::getInstance()->getLocalInstanceId();

    std::string ipcDirName = mpi::getIpcDir(installPath);
    LOG4CXX_TRACE(logger, myFuncName << ": SHM directory: "<< ipcDirName);

    // read shared memory ipc files
    std::list<std::string> ipcFiles;
    try {
        scidb::File::readDir(ipcDirName.c_str(), ipcFiles);
    } catch (const scidb::SystemException& e) {
        LOG4CXX_WARN(logger, myFuncName << ": unable to read: "
                     << ipcDirName <<" because: "<<e.what());
        if (e.getLongErrorCode() != scidb::SCIDB_LE_SYSCALL_ERROR) {
            throw;
        }
        ipcFiles.clear();
    }

    // read pid files
    std::string pidDirName = mpi::getPidDir(installPath);
    LOG4CXX_TRACE(logger, myFuncName << ": PID directory: "<< pidDirName);

    std::list<std::string> pidFiles;
    try {
        scidb::File::readDir(pidDirName.c_str(), pidFiles);
    } catch (const scidb::SystemException& e) {
        LOG4CXX_WARN(logger, myFuncName << ": unable to read: "<< pidDirName
                     <<" because: "<<e.what());
        if (e.getLongErrorCode() != scidb::SCIDB_LE_SYSCALL_ERROR) {
            throw;
        }
        pidFiles.clear();
    }

    // collect query IDs AFTER reading the files
    std::set<QueryID> queryIds;
    boost::function<void (const boost::shared_ptr<scidb::Query>&)> f;
    f = boost::bind(&getQueryId, &queryIds, _1);
    scidb::Query::listQueries(f);

    // identify dead shm objects
    for (std::list<std::string>::iterator iter = ipcFiles.begin();
         iter != ipcFiles.end(); ++iter) {
        const std::string& fileName = *iter;
        LOG4CXX_DEBUG(logger, myFuncName << ": next SHM object: "<<fileName);
        uint64_t queryId=INVALID_QUERY_ID;
        uint64_t launchId=0;
        uint64_t instanceId=myInstanceId;

        if (!mpi::parseSharedMemoryIpcName(fileName,
                                           uuid,
                                           instanceId,
                                           queryId,
                                           launchId)) {
            // ignore file with unknown name
            continue;
        }

        if (instanceId != myInstanceId
            || queryIds.find(queryId) != queryIds.end()) {
            // ignore live file
            continue;
        }

        const string ipcFileName = ipcDirName+"/"+fileName;

        LOG4CXX_DEBUG(logger, myFuncName << ": removing SHM object: "<< ipcFileName);

        if (File::remove(ipcFileName.c_str(), false) != 0) {
            LOG4CXX_ERROR(logger, myFuncName << ": failed to remove SHM object: "<< ipcFileName);
        }
    }

    // identify dead pid files
    for (std::list<std::string>::iterator iter = pidFiles.begin();
         iter != pidFiles.end(); ++iter) {
        const std::string& fileName = *iter;
        LOG4CXX_DEBUG(logger, myFuncName << ": next pid object: "<<fileName);
        QueryID queryId=INVALID_QUERY_ID;
        uint64_t launchId=0;
        int n=0;
        int rc = ::sscanf(fileName.c_str(), "%"PRIu64".%"PRIu64".%n", &queryId, &launchId, &n);
        if (rc == EOF || rc < 2) {
            // ignore file with unknown name
            continue;
        }
        if (queryIds.find(queryId) != queryIds.end()) {
            // ignore live file
            continue;
        }
        assert(n >= 0);
        size_t len = fileName.size();
        assert(static_cast<size_t>(n) <= len);

        if (fileName.compare(n, len-n, mpi::LAUNCHER_BIN)==0) {
            cleanupLauncherPidFile(installPath, uuid, pidDirName+"/"+fileName);
        } else if (fileName.compare(n, len-n, mpi::SLAVE_BIN)==0) {
            cleanupSlavePidFile(installPath, uuid, pidDirName+"/"+fileName);
        }
    }
}

void MpiErrorHandler::killAllMpiProcs()
{
    const char *myFuncName = "MpiErrorHandler::killAllMpiProcs";

    const boost::shared_ptr<const InstanceMembership> membership =
        Cluster::getInstance()->getInstanceMembership();
    const std::string& installPath = MpiManager::getInstallPath(membership);

    std::string clusterUuid = Cluster::getInstance()->getUuid();

    // read pid files
    std::string procDirName = mpi::getProcDirName();

    std::list<std::string> pidFiles;
    try {
        scidb::File::readDir(procDirName.c_str(), pidFiles);
    } catch (const scidb::SystemException& e) {
        LOG4CXX_WARN(logger, myFuncName << ": unable to read: "
                     << procDirName <<" because: "<<e.what());
        if (e.getLongErrorCode() != scidb::SCIDB_LE_SYSCALL_ERROR) {
            throw;
        }
        pidFiles.clear();
    }
    // identify dead pid files
    for (std::list<std::string>::iterator iter = pidFiles.begin();
         iter != pidFiles.end(); ++iter) {
        const std::string& fileName = *iter;
        LOG4CXX_TRACE(logger, myFuncName << ": next pid object: "<<fileName);
        pid_t pid=0;
        int rc = ::sscanf(fileName.c_str(), "%d", &pid);
        if (rc == EOF || rc < 1) {
            // ignore file with unknown name
            continue;
        }
        assert(pid >= 1);
        killProc(installPath, clusterUuid, pid);
    }
}

bool MpiErrorHandler::killProc(const std::string& installPath,
                               const std::string& clusterUuid,
                               pid_t pid,
                               QueryID queryId)
{
    const char *myFuncName = "MpiErrorHandler::killProc";
    bool doesProcExist = false;
    if (pid > 0) {
        if (!MpiManager::getInstance()->canRecognizeProc(installPath, clusterUuid, pid, queryId)) {
            return doesProcExist;
        }
        LOG4CXX_DEBUG(logger, myFuncName << ": killing pid = "<<pid);
    } else {
        LOG4CXX_DEBUG(logger, myFuncName << ": killing process group pid = "<<pid);
    }
    assert(pid != ::getpid());
    assert(pid != ::getppid());
    assert(pid > 1 || pid < -1);

    // kill proc
    int rc = ::kill(pid, SIGKILL);
    int err = errno;
    if (rc!=0) {
        if (err==EINVAL) {
            assert(false);
            throw (SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_SYSCALL_ERROR)
                   << "kill" << rc << err << pid);
        }
        if (err!=ESRCH) {
            LOG4CXX_ERROR(logger, myFuncName << ": failed to kill pid = "
                          << pid << " err = " << err);
        } else {
            LOG4CXX_DEBUG(logger, myFuncName << ": failed to kill pid = "
                          << pid << " err = " << err);
        }
    }
    doesProcExist = (rc==0 || err!=ESRCH);
    return doesProcExist;
}

bool MpiManager::canRecognizeProc(const std::string& installPath,
                                  const std::string& clusterUuid,
                                  pid_t pid,
                                  QueryID myQueryId)

{
    const char *myFuncName = "MpiErrorHandler::canRecognizeProc";
    assert(pid>0);
    stringstream ss;
    ss<<pid;
    const std::string pidFileName = ss.str();

    std::string procName;
    if (!mpi::readProcName(pidFileName, procName)) {
        LOG4CXX_WARN(logger, myFuncName << ": cannot read:" << pidFileName);
        return false;
    }

    if (procName != mpi::getSlaveBinFile(installPath) &&
        procName != getLauncherBinFile(installPath) &&
        procName != getDaemonBinFile(installPath)) {
        LOG4CXX_TRACE(logger, myFuncName <<
                      ": unrecognizable process (pid="<<pid<<") command line: "<<procName);
        return false;
    }

    std::string procEnvVar;
    if (!mpi::readProcEnvVar(pidFileName,
                        mpi::SCIDBMPI_ENV_VAR,
                             procEnvVar)) {
        LOG4CXX_DEBUG(logger, myFuncName << ": cannot extract " << mpi::SCIDBMPI_ENV_VAR << " from " << pidFileName);
        return false;
    }
    uint64_t queryId(INVALID_QUERY_ID);
    uint64_t launchId(0);
    if (!mpi::parseScidbMPIEnvVar(procEnvVar, clusterUuid, queryId, launchId)) {
        LOG4CXX_DEBUG(logger, myFuncName << ": cannot parse " << procEnvVar << " from " << pidFileName);
        return false;
    }

    if (queryId == myQueryId) {
        return true;
    }
    return (!Query::getQueryByID(queryId, false));
}

///XXX TODO tigor: find a better place and implementation
const std::string& MpiManager::getInstallPath(const boost::shared_ptr<const InstanceMembership>& membership)
{
    const Instances& instances = membership->getInstanceConfigs();
    InstanceID myId = Cluster::getInstance()->getLocalInstanceId();

    for (Instances::const_iterator i = instances.begin();
         i != instances.end(); ++i) {

        InstanceID currId = i->getInstanceId();

        if (currId == myId) {
            const std::string& path = i->getPath();
            if (!scidb::isFullyQualified(path)) {
                throw (USER_EXCEPTION(SCIDB_SE_STORAGE, SCIDB_LE_NON_FQ_PATH_ERROR) << path);
            }
            return path;
        }
    }
    assert(false);
    throw (SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_UNKNOWN_ERROR)
           << "Cluster membership is missing my instance ID");

    // to make compiler happy
    std::string* p(NULL);
    return *p;
}

MpiLauncher* MpiManager::newMPILauncher(uint64_t launchId, const boost::shared_ptr<scidb::Query>& q)
{
  if (_mpiType == mpi::MPICH12 ) {
    return new MpiLauncherMPICH12(launchId, q);
  } else if (_mpiType == mpi::MPICH14 ) {
    return new MpiLauncherMPICH(launchId, q);
  } else if (_mpiType == mpi::OMPI16 ) {
    return new MpiLauncherOMPI(launchId, q);
  }
  throw (SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_UNREACHABLE_CODE)
         << "MpiManager::newMPILauncher");
  return NULL;
}

MpiLauncher* MpiManager::newMPILauncher(uint64_t launchId, const boost::shared_ptr<scidb::Query>& q, uint32_t timeout)
{
  if (_mpiType == mpi::MPICH12 ) {
    return new MpiLauncherMPICH12(launchId, q, timeout);
  } else if (_mpiType == mpi::MPICH14 ) {
    return new MpiLauncherMPICH(launchId, q, timeout);
  } else if (_mpiType == mpi::OMPI16 ) {
    return new MpiLauncherOMPI(launchId, q, timeout);
  }
  throw (SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_UNREACHABLE_CODE)
         << "MpiManager::newMPILauncher");
  return NULL;
}


}
