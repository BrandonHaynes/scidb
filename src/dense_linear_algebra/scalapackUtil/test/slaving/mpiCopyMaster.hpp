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
#ifndef MPI_COPY_MASTER__H
#define MPI_COPY_MASTER__H

// SciDB
#include "../../scalapackFromCpp.hpp"
#include <mpi/MPIManager.h>

//
// this file contains routines that are rpc-like "wrappers" for ScaLAPACK
// (and sometimes MPI) calls,
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

namespace scidb {

void mpiCopyMaster(Query* query,
                   boost::shared_ptr<MpiOperatorContext>& ctx, // query->getOperatorCtxt
                   boost::shared_ptr<MpiSlaveProxy>& slave,
                   const string& ipcName, // can this be in the ctx too?
                   void * argsBuf,
                   // the following args are common to all scalapack slave operators:
                   const slpp::int_t& NPROW, const slpp::int_t& NPCOL,
                   const slpp::int_t& MYPROW, const slpp::int_t& MYPCOL, const slpp::int_t& MYPNUM,
                   // the follow argument types are just for the copy operator
                   double *IN, const slpp::desc_t& DESC_IN,
                   double *OUT, const slpp::desc_t& DESC_OUT,
                   slpp::int_t &INFO);

} // namespace scidb
#endif // MPI_COPY_MASTER__H

