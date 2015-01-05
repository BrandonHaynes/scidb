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

// local
#include "pdgesvdSlave.hpp" // for argument structure

using namespace std;

namespace scidb
{

void pdgesvdMarshallArgs(void*  argsBuf,
                         const slpp::int_t& NPROW, const slpp::int_t& NPCOL,
                         const slpp::int_t& MYPROW, const slpp::int_t& MYPCOL, const slpp::int_t& MYPNUM,
                         const char &jobU, const char &jobVT,
                         const slpp::int_t& M, const slpp::int_t &N,
                         double* A, const slpp::int_t& IA, const slpp::int_t& JA, const slpp::desc_t& DESC_A,
                         double* S,
                         double* U,  const slpp::int_t& IU,  const slpp::int_t& JU,  const slpp::desc_t& DESC_U,
                         double* VT, const slpp::int_t& IVT, const slpp::int_t& JVT, const slpp::desc_t& DESC_VT)
{
    enum dummy { DBG=0 };

    if(DBG) {
        std::cerr << "argsBuf:" << argsBuf << std::endl;
        std::cerr << "A:" << (void*)(A) << std::endl;
        std::cerr << "S:" << (void*)(S) << std::endl;
        std::cerr << "U:" << (void*)(U) << std::endl;
        std::cerr << "VT:" << (void*)(VT) << std::endl;
    }

    // marshall all arguments except the buffers A,S,U,&VT into a struct:
    scidb::PdgesvdArgs * args = reinterpret_cast<scidb::PdgesvdArgs*>(argsBuf) ;
    args->NPROW = NPROW;
    args->NPCOL = NPCOL;
    args->MYPROW = MYPROW;
    args->MYPCOL = MYPCOL;
    args->MYPNUM = MYPNUM;

    args->jobU = jobU ;
    args->jobVT = jobVT ;
    args->M = M ;
    args->N = N ;

    args->A.I = IA ;
    args->A.J = JA ;
    args->A.DESC = DESC_A ;

    args->U.I = IU ;
    args->U.J = JU ;
    args->U.DESC = DESC_U ;

    args->VT.I = IVT ;
    args->VT.J = JVT ;
    args->VT.DESC = DESC_VT ;

    if(DBG) {
        std::cerr << "argsBuf:  ----------------------------" << std::endl ;
        std::cerr << *args << std::endl;
        std::cerr << "argsBuf:  end-------------------------" << std::endl ;
    }
}


} // namespace scidb
