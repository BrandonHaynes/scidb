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

// std C++
#include <string>
#include <iostream>

// std C
#include <stdlib.h>
#include <unistd.h>

// de-facto standards
#include <mpi.h>

// scidb public (include/)
#include <mpi/MPIUtils.h>
#include <SciDBAPI.h>
#include <util/Network.h>
#include <util/NetworkMessage.h>
#include <util/shm/SharedMemoryIpc.h>


// scidb internals
#include <dense_linear_algebra/blas/initMathLibs.h>
#include <dense_linear_algebra/dlaScaLA/slaving/pdgesvdSlave.hpp>
#include <dense_linear_algebra/dlaScaLA/slaving/pdgemmSlave.hpp>
#include <dense_linear_algebra/scalapackUtil/test/slaving/mpiCopySlave.hpp>
#include <dense_linear_algebra/scalapackUtil/test/slaving/mpiRankSlave.hpp>
#include <network/BaseConnection.h>
#include <network/proto/scidb_msg.pb.h>

// usings
using namespace std;

// types
typedef uint64_t QueryID;
typedef uint64_t InstanceID;

// forward decls
uint64_t str2uint64(const char *str);
uint32_t str2uint32(const char *str);
int setupMpi();
int runScidbCommands(uint32_t port,
                      const std::string& clusterUuid,
                      QueryID queryId,
                      InstanceID instanceId,
                      uint64_t rank,
                      uint64_t launchId,
                      int argc, char* argv[]);

void mpiErrorHandler(MPI::Comm& comm, int *a1, ...)
{
    ::abort();
}


namespace scidb
{
namespace mpi
{
// not thread-safe !
SharedMemoryIpc::SharedMemoryIpcType_t sShmType(SharedMemoryIpc::SHM_TYPE);
void setShmIpcType(SharedMemoryIpc::SharedMemoryIpcType_t type)
{
    assert (type == SharedMemoryIpc::FILE_TYPE ||
            type == SharedMemoryIpc::SHM_TYPE);
    sShmType = type;
}
SharedMemoryIpc::SharedMemoryIpcType_t getShmIpcType()
{
    return sShmType;
}
} //namespace mpi
} //namespace scidb

/**
 * Implementation of scidb::MessageDesc which is aware of DLA specific messages
 */
class MpiMessageDesc : public scidb::MessageDesc
{
 public:
    MpiMessageDesc() : scidb::MessageDesc() {}
    MpiMessageDesc(boost::shared_ptr<scidb::SharedBuffer> binary)
    : scidb::MessageDesc(binary) {}
    virtual ~MpiMessageDesc() {}
    virtual bool validate();
    protected:
    virtual scidb::MessagePtr createRecord(scidb::MessageID messageType);
 private:
    MpiMessageDesc(const MpiMessageDesc&);
    MpiMessageDesc& operator=(const MpiMessageDesc&);
};

scidb::MessagePtr
MpiMessageDesc::createRecord(scidb::MessageID messageType)
{
    if (messageType < scidb::mtSystemMax) {
        return scidb::MessageDesc::createRecord(messageType);
    }

    boost::shared_ptr<scidb::NetworkMessageFactory> msgFactory;
    if (messageType == scidb::mtMpiSlaveResult) {
        return scidb::MessagePtr(new scidb_msg::MpiSlaveResult());
    }
    if (messageType == scidb::mtMpiSlaveHandshake) {
        return scidb::MessagePtr(new scidb_msg::MpiSlaveHandshake());
    }
    if (messageType == scidb::mtMpiSlaveCommand) {
        return scidb::MessagePtr(new scidb_msg::MpiSlaveCommand());
    }
    cerr <<  "SLAVE: unknown message type " << messageType << std::endl;
    MPI_Abort(MPI_COMM_WORLD, 910);
    return scidb::MessagePtr();
}

bool MpiMessageDesc::validate()
{
    if (MessageDesc::validate()) {
        return true;
    }
    scidb::MessageID msgId = getMessageType();

    return (msgId == scidb::mtMpiSlaveResult ||
            msgId == scidb::mtMpiSlaveHandshake ||
            msgId == scidb::mtMpiSlaveCommand);
}

/**
 * Slave's interface to SciDB
 *
 */
class MpiMasterProxy
{
    public:
    /**
     * Destructor
     */
    virtual ~MpiMasterProxy()
    {
        if (_connection) {
            try {
                const scidb::SciDB& sciDB = scidb::getSciDB();
                sciDB.disconnect(_connection);
            } catch (const std::exception& e) {
                cerr << "SLAVE: failure in disconnect: "<<e.what()<<std::endl;
            }
            _connection = NULL;
        }
    }

    /// Constructor
    MpiMasterProxy(uint32_t port, const std::string& clusterUuid, uint64_t queryId,
                   uint64_t instanceId, uint64_t rank,
                   uint64_t launchId)
    : _port(port), _clusterUuid(clusterUuid), _queryId(queryId), _instanceId(instanceId), _rank(rank),
      _launchId(launchId), _connection(NULL)
    {
    }

    /// internal use only
    scidb::BaseConnection* getConnection()
    {
        return _connection;
    }

    /**
     * Send the initial handshake message to SciDB and get the next command from SciDB
     * @param [out] nextCmd
     * @return shared memory ipc name
     * @throw scidb::Exception
     */
    void sendHandshake(scidb::mpi::Command& nextCmd)
    {
        if (_connection) {
            cerr << "SLAVE: connection to SciDB already open "<<std::endl;
            MPI_Abort(MPI_COMM_WORLD, 999);
        }
        const scidb::SciDB& sciDB = scidb::getSciDB();
        _connection = reinterpret_cast<scidb::BaseConnection*>(sciDB.connect("localhost", _port));
        if (!_connection) {
            cerr << "SLAVE: cannot connect to SciDB "<<std::endl;
            MPI_Abort(MPI_COMM_WORLD, 911);
        }
        boost::shared_ptr<scidb::MessageDesc> handshakeMessage(new MpiMessageDesc());
        handshakeMessage->initRecord(scidb::mtMpiSlaveHandshake);
        handshakeMessage->setQueryID(_queryId);
        boost::shared_ptr<scidb_msg::MpiSlaveHandshake> record = handshakeMessage->getRecord<scidb_msg::MpiSlaveHandshake>();

        record->set_cluster_uuid(_clusterUuid);
        record->set_instance_id(_instanceId);
        record->set_launch_id(_launchId);
        record->set_rank(_rank);
        record->set_pid(::getpid());
        record->set_ppid(::getppid());

        sendReceive(handshakeMessage, &nextCmd);
    }

    /**
     * Send the status of the previous command to SciDB and get the next command from SciDB
     * @param [out] nextCmd
     * @param [in] status command status
     * @throw scidb::Exception
     */
    void sendResult(int64_t status, scidb::mpi::Command& nextCmd)
    {
        sendResult(status, &nextCmd);
    }

    /**
     * Send the status of the previous command to SciDB
     * @param [in] status command status
     * @throw scidb::Exception
     */
    void sendResult(int64_t status)
    {
        sendResult(status, NULL);
    }
    private:

    void sendResult(int64_t status, scidb::mpi::Command* nextCmd)
    {
        boost::shared_ptr<scidb::MessageDesc> resultMessage(new MpiMessageDesc());
        resultMessage->initRecord(scidb::mtMpiSlaveResult);
        resultMessage->setQueryID(_queryId);
        boost::shared_ptr<scidb_msg::MpiSlaveResult> record = resultMessage->getRecord<scidb_msg::MpiSlaveResult>();
        record->set_status(status);
        record->set_launch_id(_launchId);

        sendReceive(resultMessage, nextCmd);
    }

    void sendReceive(boost::shared_ptr<scidb::MessageDesc>& resultMessage, scidb::mpi::Command* nextCmd)
    {
        if (nextCmd == NULL) {
            _connection->send(resultMessage);
            return;
        }
        boost::shared_ptr<MpiMessageDesc> commandMessage =
            _connection->sendAndReadMessage<MpiMessageDesc>(resultMessage);

        boost::shared_ptr<scidb_msg::MpiSlaveCommand> cmdMsg =
            commandMessage->getRecord<scidb_msg::MpiSlaveCommand>();
        const string commandStr = cmdMsg->command();
        nextCmd->setCmd(commandStr);

        typedef google::protobuf::RepeatedPtrField<std::string> ArgsType;
        const ArgsType& args = cmdMsg->args();

        for(ArgsType::const_iterator iter = args.begin();
            iter != args.end(); ++iter) {

            const std::string& arg = *iter;
            nextCmd->addArg(arg);
        }
    }

    private:
    MpiMasterProxy(const MpiMasterProxy&);
    MpiMasterProxy& operator=(const MpiMasterProxy&);

    friend void handleBadHandshake(QueryID queryId,
                                   InstanceID instanceId,
                                   uint64_t launchId,
                                   MpiMasterProxy& scidbProxy);

    uint32_t _port;
    std::string _clusterUuid;
    uint64_t _queryId;
    uint64_t _instanceId;
    uint64_t _rank;
    uint64_t _launchId;
    scidb::BaseConnection* _connection;
};

/// test routines
void handleSlowStart(const char *timeoutStr);
void handleSlowSlave(const std::vector<std::string>& args,
                      MpiMasterProxy& scidbProxy);
void handleEchoCommand(const std::vector<std::string>& args,
                       int64_t& result);
void handleBadMessageFlood(QueryID queryId,
                           InstanceID instanceId,
                           uint64_t launchId,
                           MpiMasterProxy& scidbProxy);
void handleBadHandshake(QueryID queryId,
                        InstanceID instanceId,
                        uint64_t launchId,
                        MpiMasterProxy& scidbProxy);
void handleBadStatus(QueryID queryId,
                     InstanceID instanceId,
                     uint64_t launchId,
                     MpiMasterProxy& scidbProxy);
void handleAbnormalExit(const std::vector<std::string>& args);

void setupLogging(const std::string& installPath,
                  uint64_t queryId, uint64_t launchId)
{
    std::string logFile = scidb::mpi::getSlaveLogFile(installPath, queryId, launchId);
    scidb::mpi::connectStdIoToLog(logFile);
}

std::string getDir(const std::string& filePath)
{
    size_t found = filePath.find_last_of("/");
    if (found==std::string::npos) {
        return ".";
    }
    if (found==0) {
        return "/";
    }
    return filePath.substr(0,found);
}


/**
 * DLA (MPI) Slave process entry
 * @param argc >=6
 * @param argv:
 * [1] - cluster UUID
 * [2] - query ID
 * [3] - instance ID (XXXX logical ? physical ?)
 * [4] - launch ID
 * [5] - SciDB instance port
 */

int main(int argc, char* argv[])
{
    if(false) {
        // allow for attachng gdb before a fault occurs
        // because not getting a core file after mpi prints stack trace.
        // this is a useful debugging method, so its good to leave code for it.
        char hostname[256];
        ::gethostname(hostname, sizeof(hostname));
        std::cerr << "DLA_RUN read for attach at pid " << ::getpid() << std::endl ;
        int i=0 ;
        while(i==0) {
            ::sleep(5);
        }
    }

    try
    {
        scidb::earlyInitMathLibEnv();  // environ changes must precede multi-threading.
    }
    catch(const std::exception &e)
    {
        cerr << "SLAVE: Failed to initialize math lib environ: " << e.what() << endl;
        exit(900); // MPI is not initialized yet, so no MPI_Abort()
    }

    const int MIN_ARGC = 3;

    if (argc < MIN_ARGC) {
        cerr << "SLAVE: Invalid args" << std::endl;
        exit(901); // MPI is not initialized yet, so no MPI_Abort()
    }

    // Find out my data directory (aka installPath), and chdir there
    const string installPath = getDir(argv[0]);
    if (::chdir(installPath.c_str()) != 0) {
      cerr << "SLAVE: Unable to chdir to " << installPath << std::endl;
      exit(902); // MPI is not initialized yet, so no MPI_Abort()
    }

    // Get common runtime values from the environment
    std::string procEnvVar;
    if (!scidb::mpi::readProcEnvVar(string("self"), scidb::mpi::SCIDBMPI_ENV_VAR, procEnvVar)) {
      cerr << "SLAVE: Unable to read /proc/self (pid="<< ::getpid() <<")" << std::endl;
      exit(903); // MPI is not initialized yet, so no MPI_Abort()
    }

    uint64_t queryId(0);
    uint64_t launchId(0);
    string clusterUuidStr;
    uint32_t shmType(0);

    if (!scidb::mpi::parseScidbMPIEnvVar(procEnvVar, shmType, queryId, launchId, clusterUuidStr)) {
        cerr << "SLAVE: Unable to parse env variable: "
             << scidb::mpi::SCIDBMPI_ENV_VAR << "=" << procEnvVar << std::endl;
        exit(904); // MPI is not initialized yet, so no MPI_Abort()
    }

    scidb::mpi::setShmIpcType(shmType);

    // Get instance specific runtime values from the arguments
    const char* instanceIdStr = argv[1];
    const char* portStr = argv[2];

    // Record my existence
    string path =
      scidb::mpi::getSlavePidFile(installPath, queryId, launchId);
    scidb::mpi::recordPids(path);

    setupLogging(installPath, queryId, launchId);

    // TODO:
    // doing the MPI_Init() early is a change from what we were doing earlier.
    // If a query is cancelled, and the operator throws an exception in response
    // before the operator learns the pid of its corresponding MPI process
    // there is a chance that MPI process may spin for a long time waiting
    // for the other ranks to appear.  If there were a timeout settable
    // via mpirun (or otherwise) to have them give up after a certain amount of
    // time, that would be useful.  Until then, we'll just watch and see
    // whether this turns out to be a problem in practice.
    // tigor: it seems not to work properly with SciDB if MPI_Init is after setupLogging()

    MPI_Init(&argc, &argv); // note extra level of indirection on arguments

    int rank = setupMpi(); // post log set-up
    srand(rank); // each process needs a unique sequence of random numbers

    // Log my runtime information
    std::cerr << "SLAVE pid="<< ::getpid() <<":" << std::endl;
    for (int i=0; i < argc; ++i) {
        std::cerr << "ARG["<<i<<"]="<<argv[i] << std::endl;
    }

    std::cerr << "CLUSTER UUID="<<clusterUuidStr << std::endl;
    std::cerr << "QUERY ID=" << queryId << std::endl;
    std::cerr << "LAUNCH ID="<< launchId << std::endl;

    uint32_t port = str2uint32(portStr);
    if (port == 0) {
        cerr << "SLAVE: Invalid port arg: " << portStr << std::endl;
        MPI_Abort(MPI_COMM_WORLD, 905);
    }
    InstanceID instanceId = str2uint64(instanceIdStr);

    if (argc>MIN_ARGC) {
      // For debugging only
      const int START_DELAY_INDEX = MIN_ARGC;
      handleSlowStart(argv[START_DELAY_INDEX]);
    }

    try {
      runScidbCommands(port, clusterUuidStr, queryId,
                       instanceId, static_cast<uint64_t>(rank),
                       launchId, argc, argv);
    }
    catch (const scidb::SystemException &e)
    {
        if (e.getShortErrorCode() == scidb::SCIDB_SE_NETWORK) {
            cerr << "SLAVE: Connection with SciDB error" << std::endl;
            MPI_Abort(MPI_COMM_WORLD, 990);  // an "expected" error
        }
        throw ; // TODO: further review:  can we get a stack trace and still MPI_Abort() to clean up faster/better?
    }

    MPI_Finalize();
    exit(EXIT_SUCCESS);
}

/// Convert ascii to uint64_t
uint64_t str2uint64(const char *str)
{
    char *ptr=0;
    errno = 0;
    int64_t num = strtoll(str,&ptr,10);
    if (errno !=0 || str == 0 || (*str) == 0 || (*ptr) != 0 || num<0) {
        cerr << "SLAVE: Invalid numeric string for uint64_t: " << str << std::endl;
        MPI_Abort(MPI_COMM_WORLD, 906);
    }
    return num;
}

/// Convert ascii to uint32_t
uint32_t str2uint32(const char *str)
{
    char *ptr=0;
    errno = 0;
    int32_t num = strtol(str,&ptr,10);
    if (errno !=0 || str == 0 || (*str) == 0 || (*ptr) != 0 || num<0) {
        cerr << "SLAVE: Invalid numeric string for uint32_t: " << str << std::endl;
        MPI_Abort(MPI_COMM_WORLD, 907);
    }
    return num;
}

int setupMpi()
{
    //
    //  Determine this processes's rank.
    //
    int rank;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    assert(rank >= 0);  // TODO: must invoke MPI_Abort() instead of abort()

    //
    // Set up an error handler
    //
    MPI::Errhandler eh =
       MPI::Comm::Create_errhandler((MPI::Comm::Errhandler_fn*)  &mpiErrorHandler);

    MPI::COMM_WORLD.Set_errhandler(eh);

    //
    //  Check number of processes (why?)
    //
    int size;
    MPI_Comm_size(MPI_COMM_WORLD, &size);
    assert(size > 0);               // TODO: must invoke MPI_Abort() instead of abort()
    assert(size > rank);            // TODO: must invoke MPI_Abort() instead of abort()

    cerr << "SLAVE: rank: "<<rank<<" size: "<< size  << endl;
    return rank;
}


int runScidbCommands(uint32_t port,
                         const std::string& clusterUuid,
                         QueryID queryId,
                         InstanceID instanceId,
                         uint64_t rank,
                         uint64_t launchId,
                         int argc, char* argv[])
{
    MpiMasterProxy scidbProxy(port, clusterUuid, queryId,
                              instanceId, rank, launchId);
    // Handshake
    scidb::mpi::Command scidbCommand;
    scidbProxy.sendHandshake(scidbCommand);

    /// MPI -- NB: only needs to be inited for DLA commands?
    //             might have other commands that won't use it
    //             even though we are running under MPI ?
    int64_t INFO=0;  // all slave proxys expect 0 for success
                     // TODO: change this to fail and add explicit success overwrites

    while (scidbCommand.getCmd() != scidb::mpi::Command::EXIT) {
        cerr << "SLAVE: command: "<< scidbCommand << std::endl;

        if(scidbCommand.getCmd() == "DLAOP") {
            enum dummy {ARG_IPCNAME=0, ARG_NBUFS, ARG_DLAOP};
            const std::vector<std::string>& args = scidbCommand.getArgs();
            // TODO JHM; eliminate cerr debug messages
            cerr << "SLAVE: NUMARGS = " << args.size() << std::endl;

            const string ipcName(args[ARG_IPCNAME]);
            const string dlaOp(args[ARG_DLAOP]);
            const unsigned nBufs = atoi(args[ARG_NBUFS].c_str());

            cerr << "SLAVE: ARG_IPCNAME = " << ipcName << std::endl;
            cerr << "SLAVE: ARG_DLAOP = " << dlaOp << std::endl;
            cerr << "SLAVE: ARG_NBUFS = " << nBufs << std::endl;

            const size_t MAX_BUFS = 20;
            if(nBufs > MAX_BUFS) {
                cerr << "SLAVE: ARG_NBUFS is invalid " << std::endl;
                MPI_Abort(MPI_COMM_WORLD, 999);
            }

            // now get the buffers sent by the master

            // NOTE: leave the following comment for a while, just to match up with
            // non-scidb version, until I factor them
            // HACK: set NPROW and NPCOL in bufs[1] and [2]
            // rest of problem will be made up inside pdgesvdSlave

            void* bufs[MAX_BUFS];
            size_t sizes[MAX_BUFS];
            boost::scoped_ptr<scidb::SharedMemoryIpc> shMems[MAX_BUFS];

            for (size_t i=0; (i < nBufs && i < MAX_BUFS); i++) {
                shMems[i].reset();
                bufs[i] = NULL;
                sizes[i] = 0;

                std::stringstream shMemName;          // name Nth buffer
                shMemName << ipcName << "." << i ;

                try {
                    shMems[i].reset(scidb::mpi::newSharedMemoryIpc(shMemName.str()));

                    scidb::SharedMemoryIpc::AccessMode mode =
                        i < 1 ? scidb::SharedMemoryIpc::RDONLY :
                                scidb::SharedMemoryIpc::RDWR ;
                    shMems[i]->open(mode);
                    // it would be nice to give the name in the .open() call
                    // so the constructor can be no-arg, which would simplify the code
                    bufs[i] = reinterpret_cast<char*>(shMems[i]->get());
                    sizes[i] = static_cast<uint64_t>(shMems[i]->getSize());
                } catch(scidb::SharedMemoryIpc::SystemErrorException& e) {
                    cerr << "SLAVE: Cannot map shared memory: " << e.what() << std::endl;
                    MPI_Abort(MPI_COMM_WORLD, 908);
                } catch(scidb::SharedMemoryIpc::InvalidStateException& e) {
                    cerr << "SLAVE: Bug in mapping shared memory: " << e.what() << std::endl;
                    MPI_Abort(MPI_COMM_WORLD, 909);
                }
                assert(bufs[i]);

                cerr << "SLAVE: IPC BUF at:"<< bufs[i] << std::endl;
                cerr << "SLAVE: IPC size = " << sizes[i] << std::endl;
            }

            // dispatch on the dla operator
            if(dlaOp == "pdgesvd_") {
                INFO = scidb::pdgesvdSlave(bufs, sizes, nBufs);
            } else if (dlaOp == "pdgemm_") {
                INFO = scidb::pdgemmSlave(bufs, sizes, nBufs);
            } else if (dlaOp == "mpirank") {
                INFO = scidb::mpirankSlave(bufs, sizes, nBufs);
            } else if (dlaOp == "mpicopy") {
                cerr << "runScidbCommands: calling mpiCopySlave()" << std::endl;
                INFO = scidb::mpiCopySlave(bufs, sizes, nBufs);
            } else {
                cerr << "runScidbCommands: DLAOP '" << dlaOp << "' not implemented" << std::endl;
                MPI_Abort(MPI_COMM_WORLD, 999);
                handleAbnormalExit(scidbCommand.getArgs());
            }
        }
        else if(scidbCommand.getCmd() == "ECHO") {
            handleEchoCommand(scidbCommand.getArgs(), INFO);
        }
        else if(scidbCommand.getCmd() == "SLOW_SLAVE") {
            handleSlowSlave(scidbCommand.getArgs(), scidbProxy);
        }
        else if(scidbCommand.getCmd() == "ABNORMAL_EXIT") {
            handleAbnormalExit(scidbCommand.getArgs());
        }
        else if(scidbCommand.getCmd() == "BAD_MSG_FLOOD") {
            handleBadMessageFlood(queryId, instanceId, launchId, scidbProxy);
        }
        else if(scidbCommand.getCmd() == "BAD_HANDSHAKE") {
            handleBadHandshake(queryId, instanceId, launchId, scidbProxy);
        }
        else if(scidbCommand.getCmd() == "BAD_STATUS") {
            handleBadStatus(queryId, instanceId, launchId, scidbProxy);
        }

        scidbCommand.clear();

        // no cleanup needed, destructors and process exit do it all
        scidbProxy.sendResult(INFO, scidbCommand);
    }

    return INFO ? EXIT_FAILURE : EXIT_SUCCESS;
}

void handleEchoCommand(const std::vector<std::string>& args,
                       int64_t& result)
{
    cerr << "SLAVE: NUMARGS = " << args.size() << std::endl;

    if (args.size() != 2) {
        cerr << "SLAVE: NUMARGS for ECHO is invalid" << std::endl;
        MPI_Abort(MPI_COMM_WORLD, 999);
    }

    const string ipcInName(args[0]);
    const string ipcOutName(args[1]);

    // now get the buffers sent by the master

    boost::scoped_ptr<scidb::SharedMemoryIpc> shmIn(scidb::mpi::newSharedMemoryIpc(ipcInName));
    char* bufIn(NULL);
    size_t sizeIn(0);
    boost::scoped_ptr<scidb::SharedMemoryIpc> shmOut(scidb::mpi::newSharedMemoryIpc(ipcOutName));
    char* bufOut(NULL);
    size_t sizeOut(0);

    try {
        shmIn->open(scidb::SharedMemoryIpc::RDONLY);
        bufIn  = reinterpret_cast<char*>(shmIn->get());
        sizeIn = shmIn->getSize();
    } catch(scidb::SharedMemoryIpc::SystemErrorException& e) {
        cerr << "SLAVE: Cannot map shared memory: " << e.what() << std::endl;
        exit(4);
    } catch(scidb::SharedMemoryIpc::InvalidStateException& e) {
        cerr << "SLAVE: Bug in mapping shared memory: " << e.what() << std::endl;
        MPI_Abort(MPI_COMM_WORLD, 905);
    }
    if (!bufIn) {
        cerr << "SLAVE: Cannot map input shared memory buffer" << std::endl;
        MPI_Abort(MPI_COMM_WORLD, 999);
    }
    try {
        shmOut->open(scidb::SharedMemoryIpc::RDWR);
        bufOut = reinterpret_cast<char*>(shmOut->get());
        sizeOut = shmOut->getSize();
    } catch(scidb::SharedMemoryIpc::SystemErrorException& e) {
        cerr << "SLAVE: Cannot map shared memory: " << e.what() << std::endl;
        MPI_Abort(MPI_COMM_WORLD, 904);
    } catch(scidb::SharedMemoryIpc::InvalidStateException& e) {
        cerr << "SLAVE: Bug in mapping shared memory: " << e.what() << std::endl;
        MPI_Abort(MPI_COMM_WORLD, 905);
    }
    if (!bufOut) {
        cerr << "SLAVE: Cannot map output shared memory buffer" << std::endl;
        MPI_Abort(MPI_COMM_WORLD, 999);
    }
    if (sizeIn != sizeOut) {
        cerr << "SLAVE: Input and output shared memory buffer differ in size" << std::endl;
        MPI_Abort(MPI_COMM_WORLD, 999);
    }
    memcpy(bufOut, bufIn, sizeOut);
    result = 1;
}

void handleBadMessageFlood(QueryID queryId,
                           InstanceID instanceId,
                           uint64_t launchId,
                           MpiMasterProxy& scidbProxy)
{
    const size_t MSG_NUM = 10000;
    cerr << "SLAVE: sending "<< MSG_NUM <<" wrong messages from BAD_MSG_FLOOD" << std::endl;
    // SciDB is not waiting for messages with launch_id+1, so it should not queue up these messages
    assert(launchId>0);
    for (size_t i=0; i < MSG_NUM; ++i)
    {
        boost::shared_ptr<scidb::MessageDesc> wrongMessage(new MpiMessageDesc());
        wrongMessage->initRecord(scidb::mtMpiSlaveHandshake);
        wrongMessage->setQueryID(queryId);
        boost::shared_ptr<scidb_msg::MpiSlaveHandshake> wrongRecord = wrongMessage->getRecord<scidb_msg::MpiSlaveHandshake>();
        wrongRecord->set_cluster_uuid("");
        wrongRecord->set_instance_id(0);
        if (i%2==0) {
            wrongRecord->set_launch_id(launchId);
        } else {
            wrongRecord->set_launch_id(launchId+1);
        }
        wrongRecord->set_rank(0);
        wrongRecord->set_pid(0);
        wrongRecord->set_ppid(0);
        scidbProxy.getConnection()->send(wrongMessage);
    }
 }

void handleBadHandshake(QueryID queryId,
                        InstanceID instanceId,
                        uint64_t launchId,
                        MpiMasterProxy& scidbProxy)
{
    cerr << "SLAVE: sending wrong message from BAD_HANDSHAKE" << std::endl;

    scidb::mpi::Command nextCmd;

    boost::shared_ptr<scidb::MessageDesc> wrongMessage(new MpiMessageDesc());
    wrongMessage->initRecord(scidb::mtMpiSlaveHandshake);
    wrongMessage->setQueryID(queryId);

    // SciDB is not expecting a handshake message at this time in the current launch
    boost::shared_ptr<scidb_msg::MpiSlaveHandshake> wrongRecord = wrongMessage->getRecord<scidb_msg::MpiSlaveHandshake>();

    wrongRecord->set_cluster_uuid("");
    wrongRecord->set_instance_id(0);
    wrongRecord->set_launch_id(launchId);
    wrongRecord->set_rank(0);
    wrongRecord->set_pid(::getpid());
    wrongRecord->set_ppid(::getppid());

    scidbProxy.sendReceive(wrongMessage, &nextCmd);

    if (nextCmd.getCmd() != scidb::mpi::Command::EXIT) {
        MPI_Abort(MPI_COMM_WORLD, 999);
    }

    MPI_Finalize();
    exit(EXIT_SUCCESS);
}

void handleBadStatus(QueryID queryId,
                     InstanceID instanceId,
                     uint64_t launchId,
                     MpiMasterProxy& scidbProxy)
{
    cerr << "SLAVE: sending malformed status from BAD_STATUS" << std::endl;
    char buf[1]; buf[0]='\0';
    boost::shared_ptr<scidb::SharedBuffer> binary(new scidb::MemoryBuffer(buf, 1));

    boost::shared_ptr<scidb::MessageDesc> wrongMessage(new MpiMessageDesc(binary));
    wrongMessage->initRecord(scidb::mtMpiSlaveResult);
    wrongMessage->setQueryID(queryId);
    boost::shared_ptr<scidb_msg::MpiSlaveResult> wrongRecord = wrongMessage->getRecord<scidb_msg::MpiSlaveResult>();

    wrongRecord->set_status(0);
    wrongRecord->set_launch_id(launchId);

    scidbProxy.getConnection()->send(wrongMessage);
    // SciDB should drop the connection after this message, causing this process to exit
}

void handleSlowSlave(const std::vector<std::string>& args,
                       MpiMasterProxy& scidbProxy)
{
    cerr << "SLAVE: NUMARGS = " << args.size() << std::endl;

    if (args.size() != 1) {
        cerr << "SLAVE: NUMARGS for SLOW_SLAVE is invalid" << std::endl;
        MPI_Abort(MPI_COMM_WORLD, 999);
    }

    uint32_t timeout = str2uint32(args[0].c_str());

    cerr << "SLAVE: sleeping for " << timeout << " sec" << std::endl;
    sleep(timeout);

    scidb::mpi::Command nextCmd;

    cerr << "SLAVE: sending bogus result " << timeout << std::endl;
    scidbProxy.sendResult(static_cast<int64_t>(timeout), nextCmd);

    if (nextCmd.getCmd() != scidb::mpi::Command::EXIT) {
        MPI_Abort(MPI_COMM_WORLD, 999);
    }

    cerr << "SLAVE: sleeping for " << timeout << " sec" << std::endl;
    sleep(timeout);

    MPI::Finalize();
    exit(EXIT_SUCCESS);
}

void handleAbnormalExit(const std::vector<std::string>& args)
{
    cerr << "SLAVE: NUMARGS = " << args.size() << std::endl;

    if (args.size() != 1) {
        cerr << "SLAVE: NUMARGS for ABNORMALEXIT is invalid" << std::endl;
        MPI_Abort(MPI_COMM_WORLD, 999);
    }

    uint32_t exitCode = str2uint32(args[0].c_str());
    cerr << "SLAVE: exiting with " << exitCode << std::endl;
    exit(exitCode);
}

void handleSlowStart(const char *timeoutStr)
{
    uint32_t timeout = str2uint32(timeoutStr);
    cerr << "SLAVE: sleeping for " << timeout << " sec before start" << std::endl;
    sleep(timeout);
}

