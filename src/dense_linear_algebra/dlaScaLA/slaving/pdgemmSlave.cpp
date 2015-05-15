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

// std C
#include <stdlib.h>
#include <iostream>

// de-facto standards
#include <boost/scoped_array.hpp>

// SciDB

// local
#include "pdgemmSlave.hpp"
#include "slaveTools.h"

namespace scidb
{
#if !defined(NDEBUG) && defined(SCALAPACK_DEBUG)
    enum dbgdummy { DBG=1 };
#else
    enum dbgdummy { DBG=0 };
#endif


///
/// @return INFO = the status of the psgemm_()
///
slpp::int_t pdgemmSlave(void* bufs[], size_t sizes[], unsigned count)
{
    enum dummy {BUF_ARGS=0, BUF_A, BUF_B, BUF_C, NUM_BUFS };

    for(size_t i=0; i < count; i++) {
        if(DBG) {
            std::cerr << "pdgemmSlave: buffer at:"<< bufs[i] << std::endl;
            std::cerr << "pdgemmSlave: bufsize =" << sizes[i] << std::endl;
        }
    }

    if(count < NUM_BUFS) {
        std::cerr << "pdgemmSlave: master sent " << count << " buffers, but " << NUM_BUFS << " are required." << std::endl;
        ::exit(99); // something that does not look like a signal
    }

    // take a COPY of args (because we will have to patch DESC.CTXT)
    scidb::PdgemmArgs args = *reinterpret_cast<PdgemmArgs*>(bufs[BUF_ARGS]) ;
    if(DBG) {
        std::cerr << "pdgemmSlave: args {" << std::endl ;
        std::cerr << args << std::endl;
        std::cerr << "}" << std::endl ;
    }

    // set up the scalapack grid
    if(DBG) std::cerr << "pdgemmSlave: NPROW:"<<args.NPROW<<" NPCOL:"<<args.NPCOL<<std::endl;
    slpp::int_t ICTXT=-1; // will be overwritten by sl_init

    // call scalapack tools routine to initialize a scalapack grid and give us its
    // context
    sl_init_(ICTXT/*out*/, args.NPROW/*in*/, args.NPCOL/*in*/);
    slpp::int_t NPROW=-1, NPCOL=-1, MYPROW=-1, MYPCOL=-1, MYPNUM=-1; // illegal vals
    getSlaveBLACSInfo(ICTXT/*in*/, NPROW, NPCOL, MYPROW, MYPCOL, MYPNUM);

    if(NPROW != args.NPROW || NPCOL != args.NPCOL ||
       MYPROW != args.MYPROW || MYPCOL != args.MYPCOL || MYPNUM != args.MYPNUM){
        if(DBG) {
            std::cerr << "scalapack general parameter mismatch" << std::endl;
            std::cerr << "args NPROW:"<<args.NPROW<<" NPCOL:"<<args.NPCOL
                      << "MYPROW:"<<args.MYPROW<<" MYPCOL:"<<args.MYPCOL<<"MYPNUM:"<<MYPNUM
                      << std::endl;
            std::cerr << "ScaLAPACK NPROW:"<<NPROW<<" NPCOL:"<<NPCOL
                      << "MYPROW:"<<MYPROW<<" MYPCOL:"<<MYPCOL<<"MYPNUM:"<<MYPNUM
                      << std::endl;
        }
    }

    const slpp::int_t one = 1 ;
    const slpp::int_t  LTD_A = std::max(one, numroc_( args.A.DESC.N, args.A.DESC.NB, MYPCOL, /*CSRC_A*/0, NPCOL ));
    const slpp::int_t  LTD_B = std::max(one, numroc_( args.B.DESC.N, args.B.DESC.NB, MYPCOL, /*CSRC_B*/0, NPCOL ));
    const slpp::int_t  LTD_C = std::max(one, numroc_( args.C.DESC.N, args.C.DESC.NB, MYPCOL, /*CSRC_C*/0, NPCOL ));

    if(DBG) {
        std::cerr << "##################################################" << std::endl;
        std::cerr << "####pdgemmSlave##################################" << std::endl;
        std::cerr << "one:" << one << std::endl;
        std::cerr << "args.A.DESC.MB:" << args.A.DESC.MB << std::endl;
        std::cerr << "MYPROW:" << MYPROW << std::endl;
        std::cerr << "NPROW:" << NPROW << std::endl;
    }

    // size check args
    SLAVE_ASSERT_ALWAYS( sizes[BUF_ARGS] >= sizeof(PdgemmArgs));

    // size check A,B,C -- debugs first
    // to include a 1000 * (2^31/1000+1) test case
    ssize_t SIZE_A = ssize_t(args.A.DESC.LLD) * LTD_A ;
    ssize_t SIZE_B = ssize_t(args.B.DESC.LLD) * LTD_B ;
    ssize_t SIZE_C = ssize_t(args.C.DESC.LLD) * LTD_C ;
    if(DBG) {
        if(sizes[BUF_A] != SIZE_A *sizeof(double)) {
            std::cerr << "sizes[BUF_A]: " << sizes[BUF_A]
                      << " != SIZE_A: " << SIZE_A
                      << " * sizeof(DOUBLE): " << sizeof(double)
                      << " note: args.A.DESC.LLD: " << args.A.DESC.LLD
                      << " LTD_A: " << LTD_A << std::endl;
        }
        if(sizes[BUF_B] != SIZE_B *sizeof(double)) {
            std::cerr << "sizes[BUF_B]: " << sizes[BUF_B]
                      << " != SIZE_B: " << SIZE_B
                      << " * sizeof(DOUBLE): " << sizeof(double)
                      << " note: args.A.DESC.LLD: " << args.A.DESC.LLD
                      << " LTD_B: " << LTD_B << std::endl;
        }
        if(sizes[BUF_C] != SIZE_C *sizeof(double)) {
            std::cerr << "sizes[BUF_C]: " << sizes[BUF_C]
                      << " != SIZE_C: " << SIZE_C
                      << " * sizeof(DOUBLE): " << sizeof(double)
                      << " note: args.A.DESC.LLD: " << args.A.DESC.LLD
                      << " LTD_C: " << LTD_C << std::endl;
        }
    }
    // to include a 1000 * (2^31/1000+1) test case
    SLAVE_ASSERT_ALWAYS(sizes[BUF_A] >= SIZE_A * sizeof(double));
    SLAVE_ASSERT_ALWAYS(sizes[BUF_B] >= SIZE_B * sizeof(double));
    SLAVE_ASSERT_ALWAYS(sizes[BUF_C] >= SIZE_C * sizeof(double));

    // sizes are correct, give the pointers their names
    double* A = reinterpret_cast<double*>(bufs[BUF_A]) ;
    double* B = reinterpret_cast<double*>(bufs[BUF_B]) ;
    double* C = reinterpret_cast<double*>(bufs[BUF_C]) ;

    // debug that the input is readable and show its contents
    if(DBG) {
        for(int ii=0; ii < SIZE_A; ii++) {
            std::cerr << "Pgrid("<< MYPROW << "," << MYPCOL << ") A["<<ii<<"] = " << A[ii] << std::endl;
        }
        for(int ii=0; ii < SIZE_B; ii++) {
            std::cerr << "Pgrid("<< MYPROW << "," << MYPCOL << ") B["<<ii<<"] = " << B[ii] << std::endl;
        }
        for(int ii=0; ii < SIZE_C; ii++) {
            std::cerr << "Pgrid("<< MYPROW << "," << MYPCOL << ") C["<<ii<<"] = " << C[ii] << std::endl;
        }
    }


    // ScaLAPACK: the DESCS are complete except for the correct context
    args.A.DESC.CTXT= ICTXT ;
    // (no DESC for S)
    args.B.DESC.CTXT= ICTXT ;
    args.C.DESC.CTXT= ICTXT ;

    if(true || DBG) {    // we'll leave this on in Cheshire.0 and re-evaluate later
        std::cerr << "pdgemmSlave: argsBuf is: {" << std::endl;
        std::cerr << args << std::endl;
        std::cerr << "}" << std::endl << std::endl;

        std::cerr << "pdgemmSlave: calling pdgemm_ for computation, with args:" << std::endl ;
        std::cerr << "TRANSA: " << args.TRANSA
                  << ", TRANSB: " << args.TRANSB
                  << ", M: " << args.M
                  << ", N: " << args.N
                  << ", K: " << args.K << std::endl;

        std::cerr << "ALPHA: " << args.ALPHA << std::endl;

        std::cerr << "A: " <<  (void*)(A)
                  << ", A.I: " << args.A.I
                  << ", A.J: " << args.A.J << std::endl;
        std::cerr << ", A.DESC: " << args.A.DESC << std::endl;

        std::cerr << "B: " <<  (void*)(B)
                  << ", B.I: " << args.B.I
                  << ", B.J: " << args.B.J << std::endl;
        std::cerr << ", B.DESC: " << args.B.DESC << std::endl;

        std::cerr << "BETA: " << args.BETA << std::endl;

        std::cerr << "C: " <<  (void*)(C)
                  << ", C.I: " << args.C.I
                  << ", C.J: " << args.C.J << std::endl;
        std::cerr << ", C.DESC: " << args.C.DESC << std::endl;
    }

    //////////////////////////////////////////////////////////////////////
    //////////////////////////////////////////////////////////////////////
    //////////////////////////////////////////////////////////////////////
    pdgemm_( args.TRANSA, args.TRANSB, args.M, args.N, args.K,
             &args.ALPHA,
             A,  args.A.I,  args.A.J,  args.A.DESC,
             B,  args.B.I,  args.B.J,  args.B.DESC,
             &args.BETA,
             C, args.C.I, args.C.J, args.C.DESC);

    if(true || DBG) {    // we'll leave this on in Cheshire.0 and re-evaluate later
        std::cerr << "pdgemmSlave: pdgemm_ complete (pdgemm_ has no result INFO)" << std::endl;
    }

    if (DBG) {
        std::cerr << "pdgemmSlave outputs: {" << std::endl;
        // debug prints of the outputs:
        for(int ii=0; ii < SIZE_C; ii++) {
            std::cerr << " C["<<ii<<"] = " << C[ii] << std::endl;
        }
        std::cerr << "}" << std::endl;
    }

    // TODO: what is the check on the pdgemm_ (pblas call) for successful completion?
    if (DBG) std::cerr << "pdgemmSlave returning successfully:" << std::endl;
    slpp::int_t INFO = 0 ;
    return INFO ;
}

} // namespace scidb
