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

#include <stdio.h>
#include <errno.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <signal.h>
#include <map>
#include <list>
#include <vector>
#include <sstream>
#include <boost/shared_ptr.hpp>
#include <boost/scoped_array.hpp>
#include <boost/bind.hpp>
#include <boost/function.hpp>
#include <boost/asio.hpp>
#include <boost/make_shared.hpp>
#include <log4cxx/logger.h>
#include <util/Thread.h>
#include <util/Network.h>
#include <util/FileIO.h>
#include <system/Cluster.h>
#include <query/Query.h>
#include <mpi/MPILauncher.h>
#include <mpi/MPIManager.h>
#include <mpi/MPIUtils.h>
#include <util/shm/SharedMemoryIpc.h>
#include <util/Network.h>
#include <util/WorkQueue.h>
#include <util/JobQueue.h>
#include <system/Config.h>

using namespace std;

namespace scidb
{
static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.mpi"));

#if defined(NDEBUG)
static const bool DBG = false;
#else
static const bool DBG = true;
#endif

MpiLauncher::MpiLauncher(uint64_t launchId, const boost::shared_ptr<Query>& q)
  : _pid(0),
    _status(0),
    _queryId(q->getQueryID()),
    _launchId(launchId),
    _query(q),
    _waiting(false),
    _inError(false),
    _MPI_LAUNCHER_KILL_TIMEOUT(scidb::getLivenessTimeout()),
    _preallocateShm(Config::getInstance()->getOption<bool>(CONFIG_PREALLOCATE_SHARED_MEM))
{
}

MpiLauncher::MpiLauncher(uint64_t launchId, const boost::shared_ptr<Query>& q, uint32_t timeout)
  : _pid(0),
    _status(0),
    _queryId(q->getQueryID()),
    _launchId(launchId),
    _query(q),
    _waiting(false),
    _inError(false),
    _MPI_LAUNCHER_KILL_TIMEOUT(timeout),
    _preallocateShm(Config::getInstance()->getOption<bool>(CONFIG_PREALLOCATE_SHARED_MEM))
{
}

void MpiLauncher::getPids(vector<pid_t>& pids)
{
    ScopedMutexLock lock(_mutex);
    if (_pid <= 1) {
        throw InvalidStateException(REL_FILE, __FUNCTION__, __LINE__)
            << " MPI launcher is not running";
    }
    pids.push_back(_pid);
}

void MpiLauncher::launch(const vector<string>& slaveArgs,
                         const boost::shared_ptr<const InstanceMembership>& membership,
                         const size_t maxSlaves)
{
    vector<string> extraEnvVars;
    vector<string> args;
    {
        ScopedMutexLock lock(_mutex);
        if (_pid != 0 || _waiting) {
            throw InvalidStateException(REL_FILE, __FUNCTION__, __LINE__)
                << " MPI launcher is already running";
        }
        boost::shared_ptr<Query> query(Query::getValidQueryPtr(_query));

        buildArgs(extraEnvVars, args, slaveArgs, membership, query, maxSlaves);
    }
    pid_t pid = fork();

    if (pid < 0) {
        // error
        int err = errno;
        throw (SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_SYSCALL_ERROR)
               << "fork" << pid << err <<"");

    } else if (pid > 0) {
        // parent
        ScopedMutexLock lock(_mutex);
        if (_pid != 0 || _waiting) {
            throw InvalidStateException(REL_FILE, __FUNCTION__, __LINE__)
                << " MPI launcher is corrupted after launch";
        }
        _pid = pid;
        LOG4CXX_DEBUG(logger, "MPI launcher process spawned, pid="<<_pid);
        return;

    }  else {
        // child
        becomeProcGroupLeader();
        recordPids();
        setupLogging();

        if (DBG) {
            std::cerr << "LAUNCHER pid="<<getpid()
             << ", pgid="<< ::getpgid(0)
             << ", ppid="<< ::getppid()<<std::endl;
        }

        closeFds();
        boost::scoped_array<const char*> argv(new const char*[args.size()+1]);
        initExecArgs(args, argv);
        const char *path = argv[0];

        if (DBG) {
            std::cerr << "LAUNCHER pid="<<::getpid()<<" args for "<<path<<" are ready" << std::endl;
            for (size_t i=0; i<args.size(); ++i) {
                const char * arg = argv[i];
                if (!arg) break;
                cerr << "LAUNCHER arg["<<i<<"] = "<< argv[i] << std::endl;
            }
        }

        for (vector<string>::const_iterator iter = extraEnvVars.begin();
             iter !=  extraEnvVars.end() ; ++iter) {
            const string& var= *iter;
            if (::putenv(const_cast<char*>(var.c_str()))!= 0) {
                perror("LAUNCHER putenv");
                _exit(1);
            }
        }

        int rc = ::execv(path, const_cast<char* const*>(argv.get()));

        assert(rc == -1);
        rc=rc; // avoid compiler warning

        perror("LAUNCHER execve");
        _exit(1);
    }
    throw (SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_UNREACHABLE_CODE)
           << "MpiLauncher::launch");
}

bool MpiLauncher::isRunning()
{
    pid_t pid=0;
    int status=0;
    {
        ScopedMutexLock lock(_mutex);
        if (_pid<=0) { return false; }
        pid = _pid;
    }

    const bool doNotWait=true;
    bool rc = waitForExit(pid, &status, doNotWait);

    if (!rc) {
        return true;
    }

    ScopedMutexLock lock(_mutex);
    _pid = -pid;
    _status = status;

    return false;
}

void MpiLauncher::destroy(bool force)
{
    pid_t pid=0;
    int status=0;
    string pidFile;
    {
        ScopedMutexLock lock(_mutex);
        if (_pid == 0 || _waiting) {
            throw (InvalidStateException(REL_FILE, __FUNCTION__, __LINE__)
                   << " MPI launcher already destroyed");
        }
        _waiting = true;
        pid = _pid;
        status = _status;
        pidFile = mpi::getLauncherPidFile(_installPath, _queryId, _launchId);

        if (pid > 0) {
            if (!force) {
                scheduleKillTimer();
            } else { // kill right away
                boost::shared_ptr<boost::asio::deadline_timer> dummyTimer;
                boost::system::error_code dummyErr;
                handleKillTimeout(dummyTimer, dummyErr);
            }
        }
        if (force) {
            _inError=true;
        }
    }
    if (pid < 0) {
        completeLaunch(-pid, pidFile, status);
        return;
    }
    bool rc = waitForExit(pid,&status);
    assert(rc); rc=rc;
    {
        ScopedMutexLock lock(_mutex);
        if (!_waiting || pid != _pid) {
             throw InvalidStateException(REL_FILE, __FUNCTION__, __LINE__)
                 << " MPI launcher is corrupted after collecting process exit code";
         }

        _pid = -pid;
        _status = status;

        if (_killTimer) {
            size_t n = _killTimer->cancel();
            assert(n<2); n=n;
        }
    }
    completeLaunch(pid, pidFile, status);
}

void MpiLauncher::completeLaunch(pid_t pid, const std::string& pidFile, int status)
{
    // rm args file(s)
    for (std::set<std::string>::const_iterator i=_ipcNames.begin();
         i != _ipcNames.end(); ++i) {
        const std::string& ipcName = *i;
        boost::scoped_ptr<SharedMemoryIpc> shmIpc(mpi::newSharedMemoryIpc(ipcName,isPreallocateShm()));
        shmIpc->remove();
        shmIpc.reset();
    }

    const string clusterUuid = Cluster::getInstance()->getUuid();
    MpiErrorHandler::cleanupLauncherPidFile(_installPath,
                                            clusterUuid,
                                            pidFile);
    // rm log file
    if (!logger->isTraceEnabled() && !_inError) {
        string logFileName = mpi::getLauncherLogFile(_installPath, _queryId, _launchId);
        scidb::File::remove(logFileName.c_str(), false);
    }

    if (WIFSIGNALED(status)) {
        LOG4CXX_ERROR(logger, "SciDB MPI launcher (pid="<<pid<<") terminated by signal = "
                      << WTERMSIG(status) << (WCOREDUMP(status)? ", core dumped" : ""));
        throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_OPERATION_FAILED)
              << "MPI launcher process";

    } else if (WIFEXITED(status)) {
        int rc = WEXITSTATUS(status);
        if (rc != 0) {
            LOG4CXX_ERROR(logger, "SciDB MPI launcher (pid="<<_pid<<") exited with status = " << rc);
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_OPERATION_FAILED)
                  << "MPI launcher process";

        } else {
            LOG4CXX_DEBUG(logger, "SciDB MPI launcher (pid="<<_pid<<") exited with status = " << rc);
            return;
        }
    }
    throw (SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_UNREACHABLE_CODE)
           << "MpiLauncher::completeLaunch");
}

 void MpiLauncher::handleKillTimeout(boost::shared_ptr<boost::asio::deadline_timer>& killTimer,
                                     const boost::system::error_code& error)
 {
     ScopedMutexLock lock(_mutex);

     if (error == boost::asio::error::operation_aborted) {
         assert(_pid < 0);
         LOG4CXX_TRACE(logger, " MPI launcher kill timer cancelled");
         return;
     }
     if (error) {
         assert(false);
         LOG4CXX_WARN(logger, "MPI launcher kill timer encountered error"<<error);
     }
     if (_pid <= 0) {
         LOG4CXX_WARN(logger, "MPI launcher kill timer cannot kill pid="<<_pid);
         return;
     }
     if (!_waiting) {
         assert(false);
         LOG4CXX_ERROR(logger, "MPI launcher kill timer cannot kill pid="<<_pid);
         throw InvalidStateException(REL_FILE, __FUNCTION__, __LINE__)
             << " MPI launcher process cannot be killed";
     }
     LOG4CXX_WARN(logger, "MPI launcher is about to kill group pid="<<_pid);

     // kill launcher's proc group
     const string clusterUuid = Cluster::getInstance()->getUuid();
     MpiErrorHandler::killProc(_installPath, clusterUuid, -_pid);
 }

static void validateLauncherArg(const std::string& arg)
{
    const char *notAllowed = " \n";
    if (arg.find_first_of(notAllowed) != string::npos) {
        throw (SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_INVALID_FUNCTION_ARGUMENT)
               << (string("MPI launcher argument with whitespace: ")+arg));
    }
}

void MpiLauncherOMPI::buildArgs(vector<string>& envVars,
                                vector<string>& args,
                                const vector<string>& slaveArgs,
                                const boost::shared_ptr<const InstanceMembership>& membership,
                                const boost::shared_ptr<Query>& query,
                                const size_t maxSlaves)
{
    for (vector<string>::const_iterator iter=slaveArgs.begin();
         iter!=slaveArgs.end(); ++iter) {
        validateLauncherArg(*iter);
    }

    const Instances& instances = membership->getInstanceConfigs();

    map<InstanceID,const InstanceDesc*> sortedInstances;
    getSortedInstances(sortedInstances, instances, query);

    ostringstream buf;
    const string clusterUuid = Cluster::getInstance()->getUuid();
    buf << query->getQueryID();
    const string queryId  = buf.str();
    buf.str("");
    buf << getLaunchId();
    const string launchId = buf.str();

    // preallocate memory
    const size_t ARGS_PER_INSTANCE = 13;
    const size_t ARGS_PER_LAUNCH = 4;
    const size_t MPI_PREFIX_CORRECTION = 2;
    size_t totalArgsNum = ARGS_PER_LAUNCH +
       (ARGS_PER_INSTANCE+slaveArgs.size()) * std::min(maxSlaves, sortedInstances.size()) -
       MPI_PREFIX_CORRECTION;
    args.clear();
    args.reserve(totalArgsNum);
    InstanceID myId = Cluster::getInstance()->getLocalInstanceId();

    args.push_back(string("")); //place holder for the binary
    args.push_back(string("--verbose"));
    args.push_back(string("--tag-output"));
    args.push_back(string("--timestamp-output"));

    // first, find my own install path, and add coordinator arguments
    for (map<InstanceID,const InstanceDesc*>::const_iterator i = sortedInstances.begin();
         i !=  sortedInstances.end(); ++i) {

        assert(i->first<sortedInstances.size());
        const InstanceDesc* desc = i->second;
        assert(desc);
        InstanceID currId = desc->getInstanceId();
        assert(currId < instances.size());

        if (currId != myId) {
            continue;
        }
        assert(args[0].empty());
        const string& installPath = desc->getPath();
        setInstallPath(installPath);
        args[0] = MpiManager::getInstance()->getLauncherBinFile(installPath);
        validateLauncherArg(args[0]);

        //XXX HACK: this will place the coordinator first on the list and thus give it rank=0
        //XXX this works only if the coordinator is the zeroth instance.
        addPerInstanceArgsOMPI(myId, desc, clusterUuid, queryId,
                           launchId, slaveArgs, args);
    }

    assert(!args[0].empty());

    // second, loop again to actually start all the instances
    size_t count = 1;
    for (map<InstanceID,const InstanceDesc*>::const_iterator i = sortedInstances.begin();
         i !=  sortedInstances.end() && count<maxSlaves; ++i,++count) {

        const InstanceDesc* desc = i->second;
        InstanceID currId = desc->getInstanceId();

        if (currId == myId) {
            --count;
            continue;
        }
        addPerInstanceArgsOMPI(myId, desc, clusterUuid, queryId,
                           launchId, slaveArgs, args);
    }
    uint64_t shmSize(0);
    vector<string>::iterator iter=args.begin();
    iter += ARGS_PER_LAUNCH;

    // compute arguments size
    const size_t DELIM_SIZE=sizeof('\n');
    for (; iter!=args.end(); ++iter) {
        string& arg = (*iter);
        shmSize += (arg.size()+DELIM_SIZE);
    }

    LOG4CXX_TRACE(logger, "MPI launcher arguments size = " << shmSize);

    envVars.push_back(mpi::getScidbMPIEnvVar(mpi::getShmIpcType(), clusterUuid, queryId, launchId));

    // Create shared memory to pass the arguments to the launcher
    string ipcName = mpi::getIpcName(getInstallPath(), clusterUuid, queryId, myId, launchId) + ".launch_args";
    bool rc = addIpcName(ipcName);
    assert(rc); rc = rc;

    LOG4CXX_TRACE(logger, "MPI launcher arguments ipc = " << ipcName);

    boost::scoped_ptr<SharedMemoryIpc> shmIpc(mpi::newSharedMemoryIpc(ipcName,isPreallocateShm()));
    char* ptr = initIpcForWrite(shmIpc.get(), shmSize);
    assert(ptr);

    size_t off = 0;
    iter=args.begin();
    iter += ARGS_PER_LAUNCH;
    for (; iter!=args.end(); ++iter) {
        string& arg = (*iter);

        if (off == 0) {
        } else if (arg == "-H") {
            *(ptr+off) = '\n';
            ++off;
        } else {
            *(ptr+off) = ' ';
            ++off;
        }
        memcpy((ptr+off), arg.data(), arg.size());
        off += arg.size();
        arg.clear();
    }
    *(ptr+off) = '\n';
    ++off;
    assert(off <= shmSize);
    shmIpc->close();
    shmIpc->flush();

    assert(args.size() >= ARGS_PER_LAUNCH+2);
    args.resize(ARGS_PER_LAUNCH+2);
    args[ARGS_PER_LAUNCH+0] = "--app";
    args[ARGS_PER_LAUNCH+1] = mpi::getIpcFile(getInstallPath(),ipcName);
    validateLauncherArg(args[ARGS_PER_LAUNCH+1]);
}

void MpiLauncherOMPI::addPerInstanceArgsOMPI(const InstanceID myId, const InstanceDesc* desc,
                                             const string& clusterUuid,
                                             const string& queryId,
                                             const string& launchId,
                                             const vector<string>& slaveArgs,
                                             vector<string>& args)
{
    InstanceID currId = desc->getInstanceId();

    ostringstream instanceIdStr;
    instanceIdStr << currId;

    const string& host = desc->getHost();
    const string& installPath = desc->getPath();

    ostringstream portStr;
    portStr << desc->getPort();

    // mpirun command line:
    // [":", "-H", <IP>, "-np", <#>, "-wd", <path>, "--prefix", <path>, "-x", "LD_LIBRARY_PATH"]*
    validateLauncherArg(host);
    args.push_back("-H");
    args.push_back(host);

    args.push_back("-np");
    args.push_back("1");

    validateLauncherArg(installPath);
    args.push_back("-wd");
    args.push_back(installPath);

    if (currId != myId) {
        // XXX NOTE: --prefix is not appended for this instance (the coordinator)
        // and this instance's arguments go first in the argument list because of
        // of an apparent bug in mpirun handling of --prefix
        const string mpiDir = MpiManager::getMpiDir(installPath);
        validateLauncherArg(mpiDir);
        args.push_back("--prefix");
        args.push_back(mpiDir);
    }
    args.push_back("-x");
    args.push_back("LD_LIBRARY_PATH");

    const string slaveBinFile = mpi::getSlaveBinFile(installPath);
    validateLauncherArg(slaveBinFile);
    args.push_back(slaveBinFile);

    // slave args
    args.push_back(instanceIdStr.str());
    args.push_back(portStr.str());

    args.insert(args.end(), slaveArgs.begin(), slaveArgs.end());
}

void MpiLauncher::getSortedInstances(map<InstanceID,const InstanceDesc*>& sortedInstances,
                                     const Instances& instances,
                                     const boost::shared_ptr<Query>& query)
{
    for (Instances::const_iterator i = instances.begin(); i != instances.end(); ++i) {
        InstanceID id = i->getInstanceId();
        try {
            // lid should be equal mpi rank
            InstanceID lid = query->mapPhysicalToLogical(id);
            sortedInstances[lid] = &(*i);
        } catch(SystemException& e) {
            if (e.getLongErrorCode() != SCIDB_LE_INSTANCE_OFFLINE) {
                throw;
            }
        }
    }
    assert(sortedInstances.size() == query->getInstancesCount());
}

void MpiLauncher::closeFds()
{
    //XXX TODO: move to Sysinfo
    long maxfd = ::sysconf(_SC_OPEN_MAX);
    if (maxfd<2) {
        maxfd = 1024;
    }

    cerr << "LAUNCHER: maxfd = " << maxfd << endl;

    // close all fds except for stdin,stderr,stdout
    for (long fd=3; fd <= maxfd ; ++fd) {
        int rc = scidb::File::closeFd(fd);
        rc=rc; // avoid compiler warning
    }
}

void MpiLauncher::becomeProcGroupLeader()
{
    if (setpgid(0,0) != 0) {
        perror("setpgid");
        _exit(1);
    }
}

void MpiLauncher::setupLogging()
{
    std::string path =
        mpi::getLauncherLogFile(_installPath, _queryId, _launchId);
    mpi::connectStdIoToLog(path);
}

void MpiLauncher::recordPids()
{
    assert(!_installPath.empty());
    string path =
        mpi::getLauncherPidFile(_installPath, _queryId, _launchId);
    mpi::recordPids(path);
}

void MpiLauncher::initExecArgs(const vector<string>& args,
                               boost::scoped_array<const char*>& argv)
{
     size_t argsSize = args.size();
     if (argsSize<1) {
         cerr << "LAUNCHER: initExecArgs failed to get args:" << argsSize << endl;
         _exit(1);
     }
     for (size_t i=0; i < argsSize; ++i) {
         argv[i] = args[i].c_str();
     }
     argv[argsSize] = NULL;
 }

void MpiLauncher::scheduleKillTimer()
{
    // this->_mutex must be locked
    assert (_pid > 1);
    assert(!_killTimer);
    _killTimer = boost::shared_ptr<boost::asio::deadline_timer>(new boost::asio::deadline_timer(getIOService()));
    int rc = _killTimer->expires_from_now(boost::posix_time::seconds(_MPI_LAUNCHER_KILL_TIMEOUT));
    if (rc != 0) {
        throw (SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_SYSCALL_ERROR)
               << "boost::asio::expires_from_now" << rc << rc << _MPI_LAUNCHER_KILL_TIMEOUT);
    }
    _killTimer->async_wait(boost::bind(&MpiLauncher::handleKillTimeout,
                                      shared_from_this(), _killTimer,
                                      boost::asio::placeholders::error));
}

bool MpiLauncher::waitForExit(pid_t pid, int *status, bool noWait)
{
    int opts = 0;
    if (noWait) {
        opts = WNOHANG;
    }
    while(true) {

        pid_t rc = ::waitpid(pid,status,opts);

        if ((rc == -1) && (errno==EINTR)) {
            continue;
        }
        if (rc == 0 && noWait) {
            return false;
        }
        if ((rc <= 0) || (rc != pid)) {
            int err = errno;
            throw (SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_SYSCALL_ERROR)
                   << "wait" << rc << err << pid);
        }
        return true;
    }
    throw (SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_UNREACHABLE_CODE)
           << "MpiLauncher::waitForExit");
    return false;
}

void MpiLauncherMPICH::buildArgs(vector<string>& envVars,
                                 vector<string>& args,
                                 const vector<string>& slaveArgs,
                                 const boost::shared_ptr<const InstanceMembership>& membership,
                                 const boost::shared_ptr<Query>& query,
                                 const size_t maxSlaves)
{
    for (vector<string>::const_iterator iter=slaveArgs.begin();
         iter!=slaveArgs.end(); ++iter) {
        validateLauncherArg(*iter);
    }

    const Instances& instances = membership->getInstanceConfigs();

    map<InstanceID,const InstanceDesc*> sortedInstances;
    getSortedInstances(sortedInstances, instances, query);

    ostringstream buf;
    const string clusterUuid = Cluster::getInstance()->getUuid();
    buf << query->getQueryID();
    const string queryId  = buf.str();
    buf.str("");
    buf << getLaunchId();
    const string launchId = buf.str();

    // preallocate memory
    const size_t ARGS_PER_INSTANCE = 8;
    size_t ARGS_PER_LAUNCH = 3;
    size_t totalArgsNum = ARGS_PER_LAUNCH +
                         (ARGS_PER_INSTANCE+slaveArgs.size()) *
                          std::min(maxSlaves, sortedInstances.size()) ;
    args.clear();
    args.reserve(totalArgsNum);
    boost::shared_ptr<vector<string> > hosts = boost::make_shared<vector<string> >();
    hosts->reserve(std::min(maxSlaves, sortedInstances.size()));

    InstanceID myId = Cluster::getInstance()->getLocalInstanceId();

    args.push_back(string("")); //place holder for the binary
    const size_t SLAVE_BIN_INDX = args.size()-1;
    args.push_back(string("-verbose"));
    args.push_back(string("-prepend-rank"));

    string nic = scidb::Config::getInstance()->getOption<string>(CONFIG_MPI_IF);
    if (!nic.empty()) {
        args.push_back(string("-iface"));
        validateLauncherArg(nic);
        args.push_back(nic);
        ARGS_PER_LAUNCH +=2;
        totalArgsNum +=2;
        args.reserve(totalArgsNum);
    }
    // To disable shared memory for local communication:
    // args.push_back(string("-genv"));
    // args.push_back(string("MPICH_NO_LOCAL"));
    // args.push_back(string("1"));

    // HYDRA_PROXY_RETRY_COUNT: The value of this environment variable determines the
    // number of retries a proxy does to establish a connection to the main server.

    // loop over all the instances
    size_t count = 0;
    for (map<InstanceID,const InstanceDesc*>::const_iterator i = sortedInstances.begin();
         i !=  sortedInstances.end() && count<maxSlaves; ++i,++count) {

        const InstanceDesc* desc = i->second;
        InstanceID currId = desc->getInstanceId();

        if (currId == myId) {
            assert(args[SLAVE_BIN_INDX].empty());
            setInstallPath(desc->getPath());
            args[SLAVE_BIN_INDX] = MpiManager::getInstance()->getLauncherBinFile(getInstallPath());
            validateLauncherArg(args[SLAVE_BIN_INDX]);
        }
        addPerInstanceArgsMPICH(myId, desc, clusterUuid, queryId,
                                launchId, slaveArgs, args, *hosts);
    }
    const size_t DELIM_SIZE=sizeof('\n');

    // compute arguments/configfile size
    uint64_t shmSizeArgs(0);
    vector<string>::iterator iter=args.begin();
    iter += ARGS_PER_LAUNCH;
    for (; iter!=args.end(); ++iter) {
        string& arg = (*iter);
        shmSizeArgs += (arg.size()+DELIM_SIZE);
    }

    resolveHostNames(hosts);

    // compute hostfile size
    int64_t shmSizeHosts(0);
    for (iter=hosts->begin(); iter!=hosts->end(); ++iter) {
        string& host = (*iter);
        shmSizeHosts += (host.size()+DELIM_SIZE);
    }

    LOG4CXX_TRACE(logger, "MPI launcher arguments size = " << shmSizeArgs << ", hosts size = " << shmSizeHosts);

    // Create shared memory to pass the arguments to the launcher
    assert(!args[SLAVE_BIN_INDX].empty());
    assert(!getInstallPath().empty());
    string ipcNameArgs  = mpi::getIpcName(getInstallPath(), clusterUuid, queryId, myId, launchId) + ".launch_args";
    string ipcNameHosts = mpi::getIpcName(getInstallPath(), clusterUuid, queryId, myId, launchId) + ".launch_hosts";
    string ipcNameExec  = mpi::getIpcName(getInstallPath(), clusterUuid, queryId, myId, launchId) + ".launch_exec";

    string execContents = getLauncherSSHExecContent(clusterUuid, queryId, launchId,
                                                    MpiManager::getInstance()->getDaemonBinFile(getInstallPath()));

    bool rc = addIpcName(ipcNameArgs);
    assert(rc);
    rc = addIpcName(ipcNameHosts);
    assert(rc);
    rc = addIpcName(ipcNameExec);
    assert(rc); rc=rc;

    LOG4CXX_TRACE(logger, "MPI launcher arguments ipcArgs = " << ipcNameArgs <<
          ", ipcHosts = "<<ipcNameHosts << ", ipcExec = "<<ipcNameExec);

    boost::scoped_ptr<SharedMemoryIpc> shmIpcArgs (mpi::newSharedMemoryIpc(ipcNameArgs, isPreallocateShm()));
    boost::scoped_ptr<SharedMemoryIpc> shmIpcHosts(mpi::newSharedMemoryIpc(ipcNameHosts,isPreallocateShm()));
    boost::scoped_ptr<SharedMemoryIpc> shmIpcExec (mpi::newSharedMemoryIpc(ipcNameExec, isPreallocateShm()));

    char* ptrArgs (MpiLauncher::initIpcForWrite(shmIpcArgs.get(),  shmSizeArgs));
    char* ptrHosts(MpiLauncher::initIpcForWrite(shmIpcHosts.get(), shmSizeHosts));
    char* ptrExec (MpiLauncher::initIpcForWrite(shmIpcExec.get(),  execContents.size()));

    // marshall the arguments into the shared memory
    size_t off = 0;
    iter=args.begin();
    iter += ARGS_PER_LAUNCH;

    for ( ; iter!=args.end(); ++iter) {
        string& arg = (*iter);

        memcpy((ptrArgs+off), arg.data(), arg.size());
        off += arg.size();

        char del = (arg == ":")  ? '\n' : ' ';

        *(ptrArgs+off) = del;
        ++off;
        arg.clear();
    }

    assert(off <= shmSizeArgs);
    assert((*(ptrArgs+off-3)) == ' ');
    assert((*(ptrArgs+off-2)) == ':');
    assert((*(ptrArgs+off-1)) == '\n');

    // MPICH is somewhat particular about new lines and whitespaces
    *(ptrArgs+(off-3)) = ' ';
    *(ptrArgs+(off-2)) = ' ';
    *(ptrArgs+(off-1)) = '\n';

    shmIpcArgs->close();
    shmIpcArgs->flush();

    off = 0;
    for (iter=hosts->begin(); iter!=hosts->end(); ++iter) {
        string& host = (*iter);

        memcpy((ptrHosts+off), host.data(), host.size());
        off += host.size();

        char del = '\n';

        *(ptrHosts+off) = del;
        ++off;
        host.clear();
    }
    shmIpcHosts->close();
    shmIpcHosts->flush();

    memcpy(ptrExec, execContents.data(), execContents.size());
    shmIpcExec->close();
    shmIpcExec->flush();

    // add references to the configfile,hostfile on the mpirun command line
    assert(args.size() >= ARGS_PER_LAUNCH+6);
    args.resize(ARGS_PER_LAUNCH+6);
    args[ARGS_PER_LAUNCH+0] = "-launcher-exec";
    args[ARGS_PER_LAUNCH+1] = mpi::getIpcFile(getInstallPath(),ipcNameExec);
    validateLauncherArg(args[ARGS_PER_LAUNCH+1]);

    if (int rc = ::chmod(args[ARGS_PER_LAUNCH+1].c_str(), S_IRUSR|S_IXUSR) != 0) {
        //make it executable
        int err=errno;
        throw (SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_SYSCALL_ERROR)
               << "chmod" << rc << err << args[ARGS_PER_LAUNCH+1]+" S_IRUSR|S_IXUSR");
    }
    args[ARGS_PER_LAUNCH+2] = "-f";
    args[ARGS_PER_LAUNCH+3] = mpi::getIpcFile(getInstallPath(),ipcNameHosts);
    validateLauncherArg(args[ARGS_PER_LAUNCH+3]);
    args[ARGS_PER_LAUNCH+4] = "-configfile";
    args[ARGS_PER_LAUNCH+5] = mpi::getIpcFile(getInstallPath(),ipcNameArgs);
    validateLauncherArg(args[ARGS_PER_LAUNCH+5]);
    envVars.push_back(mpi::getScidbMPIEnvVar(mpi::getShmIpcType(), clusterUuid, queryId, launchId));
}

void MpiLauncher::resolveHostNames(boost::shared_ptr<vector<string> >& hosts)
{
    boost::shared_ptr<JobQueue>  jobQueue  = boost::make_shared<JobQueue>();
    boost::shared_ptr<WorkQueue> workQueue = boost::make_shared<WorkQueue>(jobQueue, hosts->size(), hosts->size());
    workQueue->start();

    for (size_t i=0; i<hosts->size(); ++i) {
        string& host = (*hosts)[i];
        string service;
        scidb::ResolverFunc func = boost::bind(&MpiLauncher::handleHostNameResolve, workQueue, hosts, i, _1, _2);
        scidb::resolveAsync(host, service, func);
    }

    for (size_t i=0; i<hosts->size(); ++i) {
        jobQueue->popJob()->execute();
    }
    // Every resolveAsync call results in one job on jobQueue.
    // So, once all the jobs have been executed, the results are recorded and the queues can be destroyed.
    workQueue.reset();
    jobQueue.reset();
}

void MpiLauncher::handleHostNameResolve(const boost::shared_ptr<WorkQueue>& workQueue,
                                        boost::shared_ptr<vector<string> >& hosts,
                                        size_t indx,
                                        const boost::system::error_code& error,
                                        boost::asio::ip::tcp::resolver::iterator endpoint_iterator)
{
    // No long-running computation must be done here (e.g. network IO, disk IO, intensive CPU-bound tasks)
    WorkQueue::WorkItem item;
    item = boost::bind(&MpiLauncher::processHostNameResolve, hosts, indx, error, endpoint_iterator);
    workQueue->enqueue(item);
}

void MpiLauncher::processHostNameResolve(boost::shared_ptr<vector<string> >& hosts,
                                         size_t indx,
                                         const boost::system::error_code& error,
                                         boost::asio::ip::tcp::resolver::iterator endpoint_iterator)
{
    assert(hosts);
    assert(indx<hosts->size());

    using namespace boost::asio;
    ip::tcp::resolver::iterator end;
    const string& host = (*hosts)[indx];

    if (error) {
        LOG4CXX_WARN(logger, "Unable to resolve '" << host<<"' because: "
                     << error.value() << "('" << error.message() << "')");
        return;
    }

    // look for IPv4 addresses only (MPICH does not appear to support IPv6)
    list<ip::tcp::endpoint> results;
    for( ; endpoint_iterator != end ; ++endpoint_iterator) {
        ip::tcp::endpoint ep = *endpoint_iterator;
        if (ep.address().is_v4()) {
            results.push_back(ep);
        }
        if (results.size() > 1) {
            break;
        }
    }

    if (results.size() != 1) {
        // not sure which one to choose, so defer to MPICH
        LOG4CXX_WARN(logger, "Unable to resolve '" << host<<"' unambiguously");
        return;
    }

    LOG4CXX_TRACE(logger, "Resolved '" << host <<"' to '"<<(*results.begin()).address().to_string()<<"'");

    (*hosts)[indx] = (*results.begin()).address().to_string();
}

void MpiLauncherMPICH::addPerInstanceArgsMPICH(const InstanceID myId,
                                               const InstanceDesc* desc,
                                               const string& clusterUuid,
                                               const string& queryId,
                                               const string& launchId,
                                               const vector<string>& slaveArgs,
                                               vector<string>& args,
                                               vector<string>& hosts,
                                               const bool addWdir)
{
    InstanceID currId = desc->getInstanceId();

    ostringstream instanceIdStr;
    instanceIdStr << currId;

    const string& installPath = desc->getPath();

    ostringstream portStr;
    portStr << desc->getPort();

    // MPICH -f <hostfile>:
    const string& host = desc->getHost();
    validateLauncherArg(host);
    hosts.push_back(host);

    // MPICH -configfile <configfile>:
    // [":", "-n", 1, "-wdir", <path>, <slave_bin_path>, <slave_required_args> <slave_opt_args> ]*
    args.push_back("-n");
    args.push_back("1");

    if (addWdir) {
        validateLauncherArg(installPath);
        args.push_back("-wdir");
        args.push_back(installPath);
    }
    const string slaveBinFile = mpi::getSlaveBinFile(installPath);
    validateLauncherArg(slaveBinFile);
    args.push_back(slaveBinFile);

    // required slave args
    args.push_back(instanceIdStr.str());
    args.push_back(portStr.str());

    // optional slave args
    // XXX TODO: move them into an environment variable
    args.insert(args.end(), slaveArgs.begin(), slaveArgs.end());
    args.push_back(":");
}

string
MpiLauncherMPICH::getLauncherSSHExecContent(const string& clusterUuid, const string& queryId,
                                            const string& launchId, const string& daemonBinPath)
{
    stringstream script;
    script << "#!/bin/sh\n"
           << "args=\"\"\n"
           << "bin=\""<< daemonBinPath <<"\"\n"
           << "info=" << mpi::getScidbMPIEnvVar(mpi::getShmIpcType(), clusterUuid, queryId, launchId)<<"\n"
           << "for a in $@ ; do\n"
           << "case $a in\n"
           << "\"$bin\") args=\"$args $info\" ;;\n"
           << "\"\\\"$bin\\\"\") args=\"$args $info\" ;;\n"
           << "\"\\'$bin\\'\") args=\"$args $info\" ;;\n"
           << "esac\n"
           << "args=\"$args $a\"\n"
           << "done\n"
           << "exec /usr/bin/ssh $args";
    return script.str();
}

// local helper for MpiLauncher::initIpcForWrite() which follows
std::string formatThrowMsg(const SharedMemoryIpc::SystemErrorException& e){
    std::stringstream ss;
    ss << e.what() << " Errcode: " << e.getErrorCode();
    return ss.str();
}

// local helper for MpiLauncher::initIpcForWrite() which follows
std::string formatLog4Msg(const SharedMemoryIpc::SystemErrorException& e){
    std::stringstream ss;
    ss << formatThrowMsg(e)
       << " [originating in file: " << e.getFile() << " at line: " << e.getLine() << "]";
    return ss.str();
}

char* MpiLauncher::initIpcForWrite(SharedMemoryIpc* shmIpc, uint64_t shmSize)
{
    assert(shmIpc);
    char* ptr(NULL);
    try {
        shmIpc->create(SharedMemoryIpc::RDWR);
        shmIpc->truncate(shmSize);
        ptr = reinterpret_cast<char*>(shmIpc->get());
    }  catch(scidb::SharedMemoryIpc::NoShmMemoryException& e) {
        LOG4CXX_ERROR(logger, "initIpcForWrite: Not enough shared memory: " << formatLog4Msg(e));
        throw (SYSTEM_EXCEPTION(SCIDB_SE_NO_MEMORY, SCIDB_LE_MEMORY_ALLOCATION_ERROR) << formatThrowMsg(e)); 
    }  catch(scidb::SharedMemoryIpc::ShmMapErrorException& e) {
        LOG4CXX_ERROR(logger, "initIpcForWrite: Cannot map shared memory: " << formatLog4Msg(e));
        throw (SYSTEM_EXCEPTION(SCIDB_SE_NO_MEMORY, SCIDB_LE_MEMORY_ALLOCATION_ERROR) << formatThrowMsg(e)); 
    } catch(scidb::SharedMemoryIpc::SystemErrorException& e) {
        LOG4CXX_ERROR(logger, "initIpcForWrite: Cannot map shared memory: " << formatLog4Msg(e));
        throw (SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_OPERATION_FAILED_WITH_ERRNO)
               << "shared_memory_mmap" << e.getErrorCode());
    } catch(scidb::SharedMemoryIpc::InvalidStateException& e) {
        // not a SystemErrorException
        std::stringstream log4msg;
        log4msg << " errcode: none"
                << " [originally in file: " << e.getFile() << " at line: " << e.getLine() << "]";
        LOG4CXX_ERROR(logger, "initIpcForWrite: Unexpected error while mapping shared memory: " << log4msg);
        throw (SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_UNKNOWN_ERROR) << e.what()
               << " errcode: not available");
    }
    assert(ptr);
    return ptr;
}

void MpiLauncherMPICH12::buildArgs(vector<string>& envVars,
                                 vector<string>& args,
                                 const vector<string>& slaveArgs,
                                 const boost::shared_ptr<const InstanceMembership>& membership,
                                 const boost::shared_ptr<Query>& query,
                                 const size_t maxSlaves)
{
    for (vector<string>::const_iterator iter=slaveArgs.begin();
         iter!=slaveArgs.end(); ++iter) {
        validateLauncherArg(*iter);
    }

    const Instances& instances = membership->getInstanceConfigs();

    map<InstanceID,const InstanceDesc*> sortedInstances;
    getSortedInstances(sortedInstances, instances, query);

    ostringstream buf;
    const string clusterUuid = Cluster::getInstance()->getUuid();
    buf << query->getQueryID();
    const string queryId  = buf.str();
    buf.str("");
    buf << getLaunchId();
    const string launchId = buf.str();

    // preallocate memory
    const size_t ARGS_PER_INSTANCE = 6;
    size_t ARGS_PER_LAUNCH = 7;
    size_t totalArgsNum = ARGS_PER_LAUNCH +
                         (ARGS_PER_INSTANCE+slaveArgs.size()) *
                          std::min(maxSlaves, sortedInstances.size()) ;
    args.clear();
    args.reserve(totalArgsNum);
    boost::shared_ptr<vector<string> > hosts = boost::make_shared<vector<string> >();
    hosts->reserve(std::min(maxSlaves, sortedInstances.size()));

    InstanceID myId = Cluster::getInstance()->getLocalInstanceId();

    args.push_back(string("")); //place holder for the binary

    string ipcNameHosts;
    string ipcNameExec;

    args.push_back("-bootstrap-exec");
    args.push_back("");
    const size_t EXEC_INDX=2;
    args.push_back("-f");
    args.push_back("");
    const size_t HOST_INDX=4;
    args.push_back("-wdir");
    args.push_back("/tmp");

    envVars.push_back(mpi::getScidbMPIEnvVar(mpi::getShmIpcType(), clusterUuid, queryId, launchId));

    map<InstanceID,const InstanceDesc*>::const_iterator myIter = sortedInstances.find(myId);

    ASSERT_EXCEPTION((myIter != sortedInstances.end()), "Cannot find myself in the membership");

    // the launcher does not have to participate in the launch
    {
        const InstanceDesc* desc = myIter->second;
        assert(args[0].empty());
        setInstallPath(desc->getPath());

        ipcNameHosts = mpi::getIpcName(getInstallPath(), clusterUuid, queryId, myId, launchId) + ".launch_hosts";
        ipcNameExec  = mpi::getIpcName(getInstallPath(), clusterUuid, queryId, myId, launchId) + ".launch_exec";

        args[0] = MpiManager::getInstance()->getLauncherBinFile(getInstallPath());
        validateLauncherArg(args[0]);
        args[EXEC_INDX] = mpi::getIpcFile(getInstallPath(),ipcNameExec);
        validateLauncherArg(args[EXEC_INDX]);
        args[HOST_INDX] = mpi::getIpcFile(getInstallPath(),ipcNameHosts);
        validateLauncherArg(args[HOST_INDX]);
    }

    // loop over all the instances
    size_t count = 0;
    for (map<InstanceID,const InstanceDesc*>::const_iterator i = sortedInstances.begin();
         i !=  sortedInstances.end() && count<maxSlaves; ++i,++count) {

        const InstanceDesc* desc = i->second;
        addPerInstanceArgsMPICH(myId, desc, clusterUuid, queryId,
                                launchId, slaveArgs, args, *hosts, false);
    }

    const size_t DELIM_SIZE=sizeof('\n');

    resolveHostNames(hosts);

    // compute hostfile size
    uint64_t shmSizeHosts(0);
    for (vector<string>::iterator iter=hosts->begin(); iter!=hosts->end(); ++iter) {
        string& host = (*iter);
        shmSizeHosts += (host.size()+DELIM_SIZE);
    }

    LOG4CXX_TRACE(logger, "MPI launcher hosts shm size = " << shmSizeHosts);

    // Create shared memory to pass the arguments to the launcher
    assert(!args[0].empty());
    assert(!getInstallPath().empty());

    string execContents = getLauncherSSHExecContent(clusterUuid, queryId, launchId,
                                                    MpiManager::getInstance()->getDaemonBinFile(getInstallPath()));
    bool rc = addIpcName(ipcNameHosts);
    assert(rc);
    rc = addIpcName(ipcNameExec);
    assert(rc); rc=rc;

    LOG4CXX_TRACE(logger, "MPI launcher arguments ipcHosts = "<<ipcNameHosts << ", ipcExec = "<<ipcNameExec);

    boost::scoped_ptr<SharedMemoryIpc> shmIpcHosts(mpi::newSharedMemoryIpc(ipcNameHosts,isPreallocateShm()));
    boost::scoped_ptr<SharedMemoryIpc> shmIpcExec (mpi::newSharedMemoryIpc(ipcNameExec, isPreallocateShm()));

    char* ptrHosts(MpiLauncher::initIpcForWrite(shmIpcHosts.get(), shmSizeHosts));
    char* ptrExec (MpiLauncher::initIpcForWrite(shmIpcExec.get(),  execContents.size()));

    size_t off = 0;
    for (vector<string>::iterator iter=hosts->begin(); iter!=hosts->end(); ++iter) {
        string& host = (*iter);

        memcpy((ptrHosts+off), host.data(), host.size());
        off += host.size();

        char del = '\n';

        *(ptrHosts+off) = del;
        ++off;
        host.clear();
    }
    shmIpcHosts->close();
    shmIpcHosts->flush();

    memcpy(ptrExec, execContents.data(), execContents.size());
    shmIpcExec->close();
    shmIpcExec->flush();

    if (int rc = ::chmod(args[EXEC_INDX].c_str(), S_IRUSR|S_IXUSR) != 0) {
      //make exec shm/file executable
      int err=errno;
      throw (SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_SYSCALL_ERROR)
             << "chmod" << rc << err << args[EXEC_INDX]+" S_IRUSR|S_IXUSR");
    }
    // trim the last ':'
    assert(args[args.size()-1]==string(":"));
    args.resize(args.size()-1);

    if (logger->isTraceEnabled()) {
      size_t sizeArgs = 0;
      for (vector<string>::iterator iter=args.begin();
           iter!=args.end(); ++iter) {
        string& arg = (*iter);
        sizeArgs += arg.size();
        LOG4CXX_TRACE(logger, "MPI launcher argument = " << arg);
      }
      LOG4CXX_TRACE(logger, "MPI launcher arguments size = " << sizeArgs);
    }
}

} //namespace
