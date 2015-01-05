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

#include <sstream>

#include <log4cxx/logger.h>

#include <array/Array.h>
#include "../array/ArrayExtractOp.hpp"
#include "reformat.hpp"
#include "../scalapackUtil/scalapackFromCpp.hpp"

using namespace scidb;

namespace scidb
{
// TODO: put me back in operator
// allocate scalapack-format memory for the inputArray
// inputMem = new double(rows * cols);


slpp::desc_t scidbDistrib(const slpp::desc_t& DESCA)
{
    // A is distributed cyclically in 1 dimension only.
    // we want to redistribute it to an aribitrary processor grid
    // using pdgemr2d.  To do this, we need a ScaLAPACK distribution
    // which matches the block distribution of A-in-scidb
    //
    // If we treat A as a single row of blocks, that could map onto the procesor
    // grid in the same way.  We could then copy submatrix rows of the 1-D
    // into the destination matrix with whatever distribution it had when givin
    // to this routine.
    //
    // DESCA will have the correct M,N, MB,NB etc, but it will have an illegal
    // DTYPE because its not in normal block-cyclic.  Well'll make a new descriptor
    // and then access it in chunk-row segments.

    size_t heightInChunks = (DESCA.M+DESCA.MB-1)/DESCA.MB ; // divide, rounding up

    slpp::desc_t DESC_SCIDB(DESCA) ; // most things stay the same
    DESC_SCIDB.DTYPE = 1 ;
    DESC_SCIDB.CTXT = -1 ;  // we'll use B's context to do the work
    DESC_SCIDB.M = min(DESCA.M, DESCA.MB);
    DESC_SCIDB.N = DESCA.N * heightInChunks;

    return DESC_SCIDB;
}

#if 0 // experiment for later milestone
void redistScidbToScaLAPACK(double *A, const slpp::desc_t& DESCA, double *B, const slpp::desc_t& DESCB)
{
    // see note above about how DESC_SCIDB is a view of Scidb data that ScaLAPACK
    // can access with a 1D block-cyclic
    slpp::desc_t DESCA_SCIDB = scidbDistrib(DESCA) ; 

    std::cerr << "redistScidbToScaLAPACK: DESCA ************" << std::endl ;
    std::cerr << DESCA  << std::endl;
    std::cerr << "redistScidbToScaLAPACK: DESCA_SCIDB ************" << std::endl ;
    std::cerr << DESCA_SCIDB  << std::endl;
    std::cerr << "redistScidbToScaLAPACK: DESCB ************" << std::endl ;
    std::cerr << DESCB  << std::endl;

    size_t heightInChunks = (DESCA.M+DESCA.MB-1)/DESCA.MB ; // divide, rounding up
    size_t widthInChunks =  (DESCA.N+DESCA.NB-1)/DESCA.NB ; // divide, rounding up
    for(size_t rowChunk=0 ; rowChunk < heightInChunks ; rowChunk++) {
        // or row=0 ; row < M, row += MB

        // copy a row from A, using DESCA_SCIDB as a trick
        slpp::int_t blockRows = std::min(DESCA.MB, DESCA.M-slpp::int_t(rowChunk)*DESCA.MB) ;
        size_t Arow = rowChunk*heightInChunks;
        size_t A1Dcolumn = rowChunk*widthInChunks;

        std::cerr <<"redistScidbToScaLAPACK: rowChunk: " << rowChunk << std::endl; 
        std::cerr <<"redistScidbToScaLAPACK: blockRows: " << blockRows << std::endl; 
        std::cerr <<"redistScidbToScaLAPACK: blockCols: " << DESCA.N << std::endl; 
        std::cerr <<"redistScidbToScaLAPACK: JA: " << A1Dcolumn << std::endl; 
        std::cerr <<"redistScidbToScaLAPACK: Arow: " << Arow << std::endl; 
        pdgemr2d_(blockRows, DESCA.N, A, /*IA*/0, /*JA*/A1Dcolumn, DESCA_SCIDB, B, Arow, 0, DESCB, DESCB.CTXT);
        // TODO: where's INFO in that call? how do I check for failure?
    }
}
#endif

void infoG2L_zero_based(slpp::int_t globalRow, slpp::int_t globalCol, const slpp::desc_t& desc,
                        slpp::int_t NPROW, slpp::int_t NPCOL, slpp::int_t MYPROW, slpp::int_t MYPCOL,
                        slpp::int_t& localRowOut, slpp::int_t& localColOut)
{
    // Until we have a 100% clean re-implementation of INFOG2L in 0-based logic,
    // we'll use this wrapper to call the Fortran one.  The inline makes it no
    // worse than doing the adjustments manually, but sets up the division of labor
    // so that we can replace INFOG2L() with an all C++ re-entrant version
    // INFOG2L is f77 code and can legally be complied without a stack.

    // TODO: there is a C++ version of INFOG2L but it was probably
    //       enabled a little prematurely in r6467.
    //       But now that INFOG2L conversion is done only once per block,
    //       we don't have to worry so much about its efficiency.
    //       If we decide its important to make optimizations based on
    //       {R,C}SRC == 0 and/or that the arithmetic might be easier if
    //       all done 0-based, then resurrect it from r6466 and make it right.

    slpp::int_t IIA, JJA, IAROW, IACOL;  // scidb_infog2l_ outputs
    scidb_infog2l_(globalRow+1, globalCol+1, desc, NPROW, NPCOL, MYPROW, MYPCOL, IIA, JJA, IAROW, IACOL); // call Fortran
    localRowOut = IIA-1 ;
    localColOut = JJA-1 ;
}

ReformatToScalapack::ReformatToScalapack(double* data, const slpp::desc_t& desc,
                                         int64_t minrow, int64_t mincol,
                                         slpp::int_t  NPROW, slpp::int_t  NPCOL,
                                         slpp::int_t MYPROW, slpp::int_t MYPCOL)

:
    _data(data),
    _desc(desc),
    _desc_1d(scidbDistrib(desc)),
    _minrow(minrow),
    _mincol(mincol),
    _NPROW(NPROW),
    _NPCOL(NPCOL),
    _MYPROW(MYPROW),
    _MYPCOL(MYPCOL),
    _blockState(ReformatToScalapack::BlockEnded) // allow only blockBegin() next
{}




} // end namespace scidb
