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

#ifndef PDGESVD_MASTER_SLAVE__H
#define PDGESVD_MASTER_SLAVE__H

// SciDB
#include <scalapackUtil/scalapackFromCpp.hpp>
#include <mpi/MPIManager.h>

//
// this file contains routines that are use by both xxxMaster and xxxSlave routines
//

namespace scidb {

///
/// A "constructor" for the pdgesvdSlave() argument buffer.
/// We use a function, rather than a constructor, because it depends on runtime state,
/// in a way that a constructor should probably not depend on, at this time.
///
void pdgesvdMarshallArgs(void*  argsBuf,
                         const slpp::int_t& NPROW, const slpp::int_t& NPCOL,
                         const slpp::int_t& MYPROW, const slpp::int_t& MYPCOL, const slpp::int_t& MYPNUM,
                         const char &jobU, const char &jobVT,
                         const slpp::int_t& M, const slpp::int_t &N,
                         double* A, const slpp::int_t& IA, const slpp::int_t& JA, const slpp::desc_t& DESC_A,
                         double* S,
                         double* U,  const slpp::int_t& IU,  const slpp::int_t& JU,  const slpp::desc_t& DESC_U,
                         double* VT, const slpp::int_t& IVT, const slpp::int_t& JVT, const slpp::desc_t& DESC_VT);


} // namespace scidb
#endif // PDGESVD_MASTER_SLAVE__H

