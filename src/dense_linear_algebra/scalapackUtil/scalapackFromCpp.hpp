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
#ifndef SLPP__HPP
#define SLPP__HPP
//
/// these declarations allow C++ to make the following scalapack execute correctly
/// though ScaLAPACK is written in FORTRAN
///

// TODO JHM ; need revised naming scheme and organization of ScaLAPACK "helper" code in general

#include <ostream>

#include <sys/types.h>

#include "scalapackTypes.hpp"

extern "C" {
    // these declarations let one call the FORTRAN-interface-only ScaLAPACK FORTRAN code
    // correctly from non-FORTRAN code.  The "extern "C"" prevents name-mangling which
    // FORTRAN doesn't do, unfortunately there is no "extern "FORTRAN"" to add the trailing _
    // 

    // scalapack setup
    void sl_init_(slpp::int_t&, slpp::int_t&, slpp::int_t&);

    // scalapack tools
    void descinit_(slpp::desc_t& desc,
                   const slpp::int_t& m, const slpp::int_t& n,
                   const slpp::int_t& mb, const slpp::int_t& nb,
                   const slpp::int_t& irSrc, const slpp::int_t& icSrc, const slpp::int_t& icTxt,
                   const slpp::int_t& lld, slpp::int_t& info);
   
    void pdelset_(double* data, const slpp::int_t& row, const slpp::int_t& col,
                  const slpp::desc_t& desc, const double& val);
    void pdelget_(const char& SCOPE, const char& TOP, double& ALPHA, const double* A,
                  const slpp::int_t& IA, const slpp::int_t& JA, const slpp::desc_t& DESCA);

    slpp::int_t numroc_(const slpp::int_t&, const slpp::int_t&, const slpp::int_t&, const slpp::int_t&, const slpp::int_t&);


    // scalapack redist
    void pdgemr2d_(const slpp::int_t& M, const slpp::int_t& N,
                   void* A, const slpp::int_t& IA, const slpp::int_t& JA, const slpp::int_t& DESC_A,
                   void* B, const slpp::int_t& IB, const slpp::int_t& JB, const slpp::int_t& DESC_B,
                   const slpp::int_t& GCONTEXT);

    // scalapack
        // matrix multiply
    void pdgemm_(const char &TRANSA, const char &TRANSB,
                 const slpp::int_t& M, const slpp::int_t &N, const slpp::int_t &K,
                 double *ALPHA,
                 double *A, const slpp::int_t &IA, const slpp::int_t &JA, const slpp::desc_t& DESC_A,
                 double *B, const slpp::int_t &IB, const slpp::int_t &JB, const slpp::desc_t& DESC_B,
                 double *BETA,
                 double *C, const slpp::int_t &IC, const slpp::int_t &JC, const slpp::desc_t& DESC_C);
        // SVD
    void pdgesvd_(const char &jobU, const char &jobVT,
                  const slpp::int_t& M, const slpp::int_t &N,
                  double *A, const slpp::int_t &IA, const slpp::int_t &JA, const slpp::desc_t& DESC_A,
                  double *S,
                  double *U,  const slpp::int_t &IU,  const slpp::int_t &JU,  const slpp::desc_t& DESC_U,
                  double *VT, const slpp::int_t &IVT, const slpp::int_t &JVT, const slpp::desc_t& DESC_VT,
                  double *WORK, const slpp::int_t &LWORK, slpp::int_t &INFO);

    // blacs
    typedef blacs::int_t bl_int_t;
    void blacs_pinfo_(bl_int_t& mypnum, bl_int_t& nprocs);
    void blacs_get_(const bl_int_t& ICTXT, const bl_int_t& WHAT, bl_int_t& VAL);
    void blacs_gridinit_(const bl_int_t& ICTXT, const char&,
                         const bl_int_t& NPROW, const bl_int_t& NPCOL);
    void blacs_gridinfo_(const bl_int_t&, const bl_int_t&,
                         const bl_int_t&, bl_int_t&, bl_int_t&);
    bl_int_t blacs_pnum_(const bl_int_t& cntxt, const bl_int_t& myPRow, const bl_int_t& myPCol);
    void blacs_gridexit_(const bl_int_t&);
    void blacs_abort_(const bl_int_t& ctxt, const bl_int_t& errorNum);
    void blacs_exit_(const bl_int_t&);
}

#endif // SLPP__HPP
