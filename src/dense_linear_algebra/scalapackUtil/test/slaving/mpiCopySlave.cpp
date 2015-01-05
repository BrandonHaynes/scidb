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

#include <iostream>

#include <assert.h>
#include <stdlib.h>

// dla
#include <dlaScaLA/slaving/slaveTools.h>

// local
#include "mpiCopySlave.hpp"

namespace scidb
{
    
slpp::int_t mpiCopySlave(void* bufs[], size_t sizes[], unsigned count)
{
    enum dummy  {DBG=0};
    enum dummy2 {BUF_ARGS=0, BUF_IN, BUF_OUT, NUM_BUFS };

    if(DBG) {
        std::cerr << "mpiCopySlave(): entered" << std::endl;
        for(size_t i=0; i < count; i++) {
            std::cerr << "mpiCopySlave: buffer at:"<< bufs[i] << std::endl;
            std::cerr << "mpiCopySlave: bufsize =" << sizes[i] << std::endl;
        }
    }

    if(count < NUM_BUFS) {
        std::cerr << "mpiCopySlave: master sent " << count << " buffers, but " << NUM_BUFS << " are required." << std::endl;
        ::exit(99); // something that does not look like a signal
    }

    // take a COPY of args (because we will have to patch DESC.CTXT)
    scidb::MPICopyArgs args = *reinterpret_cast<MPICopyArgs*>(bufs[BUF_ARGS]) ;
    if(DBG) {
        std::cerr << "mpiCopySlave: args --------------------------" << std::endl ;
        std::cerr << args << std::endl;
        std::cerr << "mpiCopySlave: args end ----------------------" << std::endl ;
    }

    // set up the scalapack grid
    if(DBG) std::cerr << "##### sl_init() NPROW:"<<args.NPROW<<" NPCOL:"<<args.NPCOL<<std::endl;
    slpp::int_t ICTXT=-1; // will be overwritten by sl_init

    // call scalapack tools routine to initialize a scalapack grid and give us its
    // context
    sl_init_(ICTXT/*out*/, args.NPROW/*in*/, args.NPCOL/*in*/); 

    slpp::int_t NPROW=-1, NPCOL=-1, MYPROW=-1, MYPCOL=-1, MYPNUM=-1; // illegal vals
    getSlaveBLACSInfo(ICTXT/*in*/, NPROW, NPCOL, MYPROW, MYPCOL, MYPNUM);

    if(NPROW  != args.NPROW  || NPCOL  != args.NPCOL ||
       MYPROW != args.MYPROW || MYPCOL != args.MYPCOL ||
       MYPNUM != args.MYPNUM){
        std::cerr << "scalapack general parameter mismatch:" << std::endl;
        std::cerr << "args:" << std::endl;
        std::cerr << "NP=("<<args.NPROW<<", "<<args.NPCOL <<")"<< std::endl;
        std::cerr << "MYP("<<args.MYPROW<<", "<<args.MYPCOL<<")"<< std::endl;
        std::cerr << "MYPNUM" <<args.MYPNUM << std::endl;
        std::cerr << "ScaLAPACK:" << std::endl;
        std::cerr << "NP=("<<NPROW<<", "<<NPCOL <<")"<< std::endl;
        std::cerr << "MYP("<<MYPROW<<", "<<MYPCOL<<")"<< std::endl;
        std::cerr << "MYPNUM" <<MYPNUM << std::endl;
        ::exit(99); // something that does not look like a signal
    }

    const slpp::int_t& LLD_IN = args.IN.DESC.LLD ;
    const slpp::int_t one = 1 ;
    const slpp::int_t  LTD_IN = std::max(one, numroc_( args.IN.DESC.N, args.IN.DESC.NB, MYPCOL, /*CSRC_IN*/0, NPCOL ));
    const slpp::int_t& MP = LLD_IN ;
    const slpp::int_t& NQ = LTD_IN ;

    // size check args
    if( sizes[BUF_ARGS] != sizeof(MPICopyArgs)) {
        assert(false); // TODO: correct way to fail
        ::exit(99); // something that does not look like a signal
    }

    // size check IN
    slpp::int_t SIZE_IN = MP*NQ ;
    if( sizes[BUF_IN] != SIZE_IN * sizeof(double)) {
        std::cerr << "slave: error size mismatch:" << std::endl;
        std::cerr << "sizes[BUF_IN]" << sizes[BUF_IN] << std::endl;
        std::cerr << "MP * NQ = " << MP <<"*"<<NQ<<"="<< MP*NQ << std::endl;
        assert(false); // TODO: correct way to fail
        ::exit(99); // something that does not look like a signal
    }

    // size check OUT
    slpp::int_t SIZE_OUT = SIZE_IN;
    if( sizes[BUF_OUT] != SIZE_OUT *sizeof(double)) {
        std::cerr << "sizes[BUF_OUT]:"<<sizes[BUF_OUT];
        std::cerr << "MP * NQ = " << MP <<"*"<<NQ<<"="<< MP*NQ << std::endl;
        assert(false); // TODO: correct way to fail
        ::exit(99); // something that does not look like a signal
    }

    // sizes are correct, give the pointers their names
    double* IN = reinterpret_cast<double*>(bufs[BUF_IN]) ;
    double* OUT = reinterpret_cast<double*>(bufs[BUF_OUT]) ;

    // here's the whole thing: copy IN to OUT
    // TODO: use memcpy instead
    for(int ii=0; ii < SIZE_OUT; ii++) {
        OUT[ii] = IN[ii] ;
    }

    return 0;
}

} // end namespace

