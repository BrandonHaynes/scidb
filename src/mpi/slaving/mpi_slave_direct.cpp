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
int initMpi(int argc, char* argv[]);
int runScidbCommands(uint32_t port,
                      const std::string& clusterUuid,
                      QueryID queryId,
                      InstanceID instanceId,
                      uint64_t launchId,
                      int argc, char* argv[]);

void mpiErrorHandler(MPI::Comm& comm, int *a1, ...)
{
    ::abort();
}


/**
 * DLA (MPI) Slave process entry, under scidb
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


    int rank = initMpi(argc, argv);
    srand(rank); // give each process a unique set of numbers


    int exitStatus = EXIT_SUCCESS;

    try {
        exitStatus = runScidbCommands(0, "clusterUuid", 0, 0, 0, argc, argv);
    }
    catch (...) {
        MPI_Abort(MPI_COMM_WORLD, 990);
    }

    MPI_Finalize();
    _exit(exitStatus);
}

/// Convert ascii to uint64_t
uint64_t str2uint64(const char *str)
{
    char *ptr=0;
    errno = 0;
    int64_t num = strtoll(str,&ptr,10);
    if (errno !=0 || str == 0 || (*str) == 0 || (*ptr) != 0 || num<0) {
        cerr << "SLAVE: Invalid numeric string for uint64_t: " << str << std::endl;
        exit(8);
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
        exit(9);
    }
    return num;
}

int initMpi(int argc, char* argv[])
{
    MPI_Init(&argc, &argv);
    //
    //  Determine this processes's rank.
    //
    int rank;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    assert(rank >= 0);
    if(rank==0) cout << "SLAVE: rank: "<< rank  << " is ready (stdout)" << endl;
    if(rank==0) cerr << "SLAVE: rank: "<< rank  << " is ready (stderr)" << endl;

    MPI::Errhandler eh = 
       MPI::Comm::Create_errhandler((MPI::Comm::Errhandler_fn*)  &mpiErrorHandler);

    MPI::COMM_WORLD.Set_errhandler(eh);
    if(rank==0) cerr << "SLAVE: error handler set" << endl;

    //
    //  Check number of processes (why?)
    //
    int size;
    MPI_Comm_size(MPI_COMM_WORLD, &size);
    assert(size > 0);
    assert(size > rank);
    if(rank==0) cerr << "SLAVE: size: "<< size  << endl;
    return rank;
}


int runScidbCommands(uint32_t port,
                         const std::string& clusterUuid,
                         QueryID queryId,
                         InstanceID instanceId,
                         uint64_t launchId,
                         int argc, char* argv[])
{
    int64_t INFO=0;  // all slave proxys expect 0 for success
                     // TODO: change this to fail and add explicit success overwrites

    string description("");

    const size_t MAX_BUFS = 20;

    // HACK: set NPROW and NPCOL in bufs[1] and [2]
    // rest of problem will be made up inside pdgesvdSlave
    scidb::PdgesvdArgs svdArgs;
    assert(argc >= 3);
    int64_t order = atoi(argv[1]);
    svdArgs.NPROW =     atoi(argv[2]);
    svdArgs.NPCOL =     atoi(argv[3]);

    // this is more memory than needed
    // we only actually need the portion
    // for the particular instance
    double* A = new double[order*order]; // A
    double scale = 1.0/RAND_MAX ;
    for(int64_t c=0; c < order; c++ ) {
        double * column = A + c*order;
        for(int64_t r=0; r < order; r++ ) {
            column[r] = 1.0 + scale*rand();
        }
    }

    // now make buffers as if sent by the master
    void* bufs[MAX_BUFS];
    bufs[0] = &svdArgs;
    bufs[1] = A;
    bufs[2] = new double[order*order]; // S
    bufs[3] = new double[order*order]; // U
    bufs[4] = new double[order*order]; // VT
    size_t sizes[MAX_BUFS];
    sizes[0] = sizeof(svdArgs);
    sizes[1] = order*order*sizeof(double);
    sizes[2] = sizes[1];
    sizes[3] = sizes[1];
    sizes[4] = sizes[1];
    const unsigned nBufs = 5;
    const string dlaOp("pdgesvd_");
    stringstream ss;
    ss << "pdgesvd_ @ size " << order ;
    description = ss.str();

    // dispatch on the dla operator
    if(dlaOp == "pdgesvd_") {
        bool debugOverwriteArgs=true;
        INFO = scidb::pdgesvdSlave(bufs, sizes, nBufs, debugOverwriteArgs);
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
    }

    return INFO ? EXIT_FAILURE : EXIT_SUCCESS;
}

void handleAbnormalExit(const std::vector<std::string>& args)
{
    cerr << "SLAVE: NUMARGS = " << args.size() << std::endl;

    if (args.size() != 1) {
        cerr << "SLAVE: NUMARGS for ABNORMALEXIT is invalid" << std::endl;
        exit(99);
    }

    uint32_t exitCode = str2uint32(args[0].c_str());
    cerr << "SLAVE: exiting with " << exitCode << std::endl;
    exit(exitCode);
}
