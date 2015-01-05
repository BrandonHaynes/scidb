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
#include <boost/make_shared.hpp>
#include <boost/numeric/conversion/cast.hpp>
#include <log4cxx/logger.h>

#include <query/Operator.h>
#include <array/Metadata.h>
#include <system/Cluster.h>
#include <query/Query.h>
#include <system/Exceptions.h>
#include <system/Utils.h>
#include <mpi/MPILauncher.h>
#include <mpi/MPIUtils.h>
#include <util/shm/SharedMemoryIpc.h>
#include <mpi/MPIManager.h>
#include <mpi/MPISlaveProxy.h>

#include "pdgesvdMaster.hpp"
#include "pdgesvdSlave.hpp"
#include "pdgesvdMasterSlave.hpp"

using namespace std;
using namespace scidb;

namespace scidb
{
static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.libdense_linear_algebra"));


//
// the arguments to the prototype must be call-compatible with those for
// the FORTRAN pdgesvd_ subroutine, so that this can be substituted for
// calls to pdgesvd
//

// before calling this, you must have done (roughly):
// new MpiOperatorContext(query); // could be routine on query instead of setOperatorContext
// new MpiErrorHandler(ctx, NUM_LAUNCH_TESTS ???) ;
// query->pushErrorHandler(eh);
// query->pushFinalizer(boost::bind(&MpiErrorHandler::finalize, eh, _1);
// f.clear();
// eh.reset();
// query->setOperatorContext(ctx);
// membership check
// getInstallPath(membership, query); 
// new MpiSlaveProxy(...)
// ctx->setSlave()
// new MpiLauncher()
// ctx->setLauncher
// query->setOperatorContext(ctx);
// slave->waitForHandshae(ctx);
// cleanup oldSlave
void pdgesvdMaster(Query* query,  // or do I need only the ctx?
                   boost::shared_ptr<MpiOperatorContext>& ctx, // query->getOperatorCtxt returns superclass
                   boost::shared_ptr<MpiSlaveProxy>& slave,  // need ctx->getSlave();
                   const string& ipcName, // can this be in the ctx too?
                   void*  argsBuf,
                   const slpp::int_t& NPROW, const slpp::int_t& NPCOL,
                   const slpp::int_t& MYPROW, const slpp::int_t& MYPCOL, const slpp::int_t& MYPNUM,
                   const char &jobU, const char &jobVT,
                   const slpp::int_t& M, const slpp::int_t &N,
                   double* A, const slpp::int_t& IA, const slpp::int_t& JA, const slpp::desc_t& DESC_A,
                   double* S, 
                   double* U,  const slpp::int_t& IU,  const slpp::int_t& JU,  const slpp::desc_t& DESC_U,
                   double* VT, const slpp::int_t& IVT, const slpp::int_t& JVT, const slpp::desc_t& DESC_VT,
                   slpp::int_t& INFO)
{
    enum dummy { DBG=0 };
    static const char ARG_NUM_SHM_BUFFERS[] = "5";  // ARGS + A, S, U, and VT
    INFO = 1 ; 

    pdgesvdMarshallArgs(argsBuf, NPROW,  NPCOL, MYPROW, MYPCOL, MYPNUM,
                                 jobU, jobVT, M, N,
                                 NULL /*A*/,  IA,  JA,  DESC_A,
                                 NULL /*S*/,
                                 NULL /*U*/,  IU,  JU,  DESC_U,
                                 NULL /*VT*/, IVT, JVT, DESC_VT);

    //
    // ready to send stuff to the proxy
    //

    //-------------------- Send command
    mpi::Command cmd;
    cmd.setCmd(string("DLAOP")); // common command for all DLAOPS (TODO: to a header)
    cmd.addArg(ipcName);
    cmd.addArg(ARG_NUM_SHM_BUFFERS);
    cmd.addArg("pdgesvd_");             // sub-command name (TODO:factor to a header, replace in mpi_slave_scidb as well)
    slave->sendCommand(cmd, ctx);       // at this point the command and ipcName are sent
                                        // our slave finds and maps the buffers by name
                                        // based on ipcName

    // TODO: factor this ScaLAPACK pattern (here to end)
    LOG4CXX_DEBUG(logger, "pdgesvdMaster(): calling slave->waitForStatus(ctx)");
    int64_t status = slave->waitForStatus(ctx, false); // raise=false so we can customize the exception message
    LOG4CXX_DEBUG(logger, "pdgesvdMaster(): slave->waitForStatus(ctx) returned " << status);

    // assign the result
    INFO = boost::numeric_cast<slpp::int_t, int64_t>(status);

    // slaving cleanups
    cmd.clear();
    cmd.setCmd(string("EXIT"));
    slave->sendCommand(cmd, ctx);
    slave->waitForExit(ctx);
}

} // namespace scidb
