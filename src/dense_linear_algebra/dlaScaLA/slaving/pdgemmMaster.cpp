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

// standards

// de-facto standards
#include <boost/make_shared.hpp>
#include <boost/numeric/conversion/cast.hpp>
#include <log4cxx/logger.h>

// scidb include
#include <array/Metadata.h>
#include <mpi/MPILauncher.h>
#include <mpi/MPIManager.h>
#include <mpi/MPISlaveProxy.h>
#include <mpi/MPIUtils.h>
#include <query/Operator.h>
#include <query/Query.h>
#include <system/Cluster.h>
#include <system/Exceptions.h>
#include <system/Utils.h>
#include <util/shm/SharedMemoryIpc.h>

// local include
#include "pdgemmMaster.hpp"
#include "pdgemmSlave.hpp" // for argument structure

using namespace std;
using namespace scidb;

namespace scidb
{
static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.libdense_linear_algebra"));


void pdgemmMaster(Query* query,  // or do I need only the ctx?
                   boost::shared_ptr<MpiOperatorContext>& ctx, // query->getOperatorCtxt returns superclass
                   boost::shared_ptr<MpiSlaveProxy>& slave,  // need ctx->getSlave();
                   const string& ipcName, // can this be in the ctx too?
                   void*  argsBuf,
                   const slpp::int_t& NPROW, const slpp::int_t& NPCOL,
                   const slpp::int_t& MYPROW, const slpp::int_t& MYPCOL, const slpp::int_t& MYPNUM,
                   const char &TRANSA, const char &TRANSB,
                   const slpp::int_t& M, const slpp::int_t &N, const slpp::int_t &K,
                   const double* ALPHA,
                   const double* A, const slpp::int_t& IA, const slpp::int_t& JA, const slpp::desc_t& DESC_A,
                   const double* B, const slpp::int_t& IB,  const slpp::int_t& JB,  const slpp::desc_t& DESC_B,
                   const double* BETA,
                   double* C, const slpp::int_t& IC, const slpp::int_t& JC, const slpp::desc_t& DESC_C,
                   slpp::int_t& INFO)  // real pdgemm has no info!!!
{
    enum dummy { DBG=0 };
    static const char ARG_NUM_SHM_BUFFERS[] = "4";  // ARGS + AA, BB, CC
    INFO = 1 ;

    if(DBG) {
        std::cerr << "argsBuf:" << argsBuf << std::endl;
        std::cerr << "A:" << (void*)(A) << std::endl;
        std::cerr << "B:" << (void*)(B) << std::endl;
        std::cerr << "C:" << (void*)(C) << std::endl;
    }

    // marshall all arguments except the buffers A,B,C into a struct:
    PdgemmArgs* args = reinterpret_cast<PdgemmArgs*>(argsBuf) ;
    args->NPROW = NPROW;
    args->NPCOL = NPCOL;
    args->MYPROW = MYPROW;
    args->MYPCOL = MYPCOL;
    args->MYPNUM = MYPNUM;

    args->TRANSA = TRANSA ;
    args->TRANSB = TRANSB ;

    args->ALPHA=*ALPHA ;
    args->BETA=*BETA ;
    args->M = M ;
    args->N = N ;
    args->K = K ;

    args->A.I = IA ;
    args->A.J = JA ;
    args->A.DESC = DESC_A ;

    args->B.I = IB ;
    args->B.J = JB ;
    args->B.DESC = DESC_B ;

    args->C.I = IC ;
    args->C.J = JC ;
    args->C.DESC = DESC_C ;

    if(DBG) {
        std::cerr << "argsBuf:  ----------------------------" << std::endl ;
        std::cerr << *args << std::endl;
        std::cerr << "argsBuf:  end-------------------------" << std::endl ;
    }

    //
    // ready to send stuff to the proxy
    //

    //-------------------- Send command
    mpi::Command cmd;
    cmd.setCmd(string("DLAOP")); // common command for all DLAOPS (TODO: to a header)
    cmd.addArg(ipcName);
    cmd.addArg(ARG_NUM_SHM_BUFFERS);
    cmd.addArg("pdgemm_");             // sub-command name (TODO:factor to a header, replace in mpi_slave as well)
    slave->sendCommand(cmd, ctx);       // at this point the command and ipcName are sent
                                        // our slave finds and maps the buffers by name
                                        // based on ipcName

    // TODO: factor this ScaLAPACK pattern (here to end)
    LOG4CXX_DEBUG(logger, "pdgemmMaster(): calling slave->waitForStatus(ctx)");
    int64_t status = slave->waitForStatus(ctx, false); // raise=false so we can customize the exception message
    LOG4CXX_DEBUG(logger, "pdgemmMaster(): slave->waitForStatus(ctx) returned " << status);

    // assign the result
    INFO = boost::numeric_cast<slpp::int_t, int64_t>(status);

    // slaving cleanups
    cmd.clear();
    cmd.setCmd(string("EXIT"));
    slave->sendCommand(cmd, ctx);
    slave->waitForExit(ctx);
}

} // namespace scidb
