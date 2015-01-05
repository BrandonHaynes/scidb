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

#ifndef PDGEMM_MASTER__H
#define PDGEMM_MASTER__H

// SciDB
#include <mpi/MPIManager.h>
#include <scalapackUtil/scalapackFromCpp.hpp>

//
// this file contains routines that are rpc-like "wrappers" for ScaLAPACK calls,
// which allow them to run in a separate process from SciDB.  This is because ScaLAPACK
// runs on MPI, which is not tolerant of node failures.  On node failure, it will either
// kill all MPI processes in the "communcatior" group, or, if it is set not to do that,
// the "communicator" group becomes unuseable until the process is restarted.  Neither of
// these MPI behaviors is compatible with a database server that needs to run 24/7.
//
// Note that the ScaLAPACK routines are actually written in FORTRAN and do not have a
// specific C or C++ API, therfore the types of the arguents in the prototypes are designed
// to permit calling FORTRAN from C++, and it is those types that are required in the
// corresponding wrappers.
//
// The followin ScaLAPACK "driver routines" are supported at this time:
//
// ScaLAPACK    (MASTER-SIDE) WRAPPER
// pdgemm_     pdgemmMaster
//

namespace scidb {

///
/// This method causes a ScaLAPACK pdgemm_ to be run on data A with outputs in
/// S (sigma or singular values)
/// U (U or left singular vectors)
/// V* (V congugate-transpose or right singular vectors congugate transpose)
///
/// The difference is that the "Master" version sends the command to a program
/// called "mpi_slave_scidb" and that process mmap/shmem's the buffer into its address
/// space, calls pdgemm_() and returns.  The slave process is used so that if
/// MPI experiences a failure, it will not cause the typical MPI problems that
/// MPI failures cause for their processes, such as termination and/or inability
/// to communicate to other MPI processes after the error, with no recourse
/// except restart.
///
void pdgemmMaster(Query* query,  // or do I need only the ctx?
                   boost::shared_ptr<MpiOperatorContext>& ctx,
                   boost::shared_ptr<MpiSlaveProxy>& slave,  // need ctx->getSlave();
                   const string& ipcName, // can this be in the ctx too?
                   void * argsBuf,
                   // the following args are common to all scalapack slave operators:
                   const slpp::int_t& NPROW, const slpp::int_t& NPCOL,
                   const slpp::int_t& MYPROW, const slpp::int_t& MYPCOL, const slpp::int_t& MYPNUM,
                   // the follow argument types match the ScaLAPACK FORTRAN-compatible ones:
                   const char &TRANSA, const char &TRANSB,
                   const slpp::int_t& M, const slpp::int_t &N, const slpp::int_t &K,
                   const double *ALPHA,
                   const double *A, const slpp::int_t &IA, const slpp::int_t &JA, const slpp::desc_t& DESC_A,
                   const double *B,  const slpp::int_t &IB,  const slpp::int_t &JB,  const slpp::desc_t& DESC_B,
                   const double *BETA,
                   double *C, const slpp::int_t &IC, const slpp::int_t &JC, const slpp::desc_t& DESC_C,
                   slpp::int_t &INFO);

} // namespace scidb
#endif // PDGEMM_MASTER__H

