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
#ifndef REFORMAT__HPP
#define REFORMAT__HPP

// std C++
#include <iostream>

// std C
#include <stdlib.h>

// defacto std
#include <boost/shared_array.hpp>
#include <boost/shared_ptr.hpp>

// SciDB
#include <system/ErrorCodes.h>
#include <system/Exceptions.h>
#include <util/Platform.h>

// local
#include <dlaScaLA/scalapackEmulation/scalapackEmulation.hpp>

// local

namespace scidb
{

template<typename int_tt >
inline int_tt ceilScaled(const int_tt& val, const int_tt& s) {
    return (val + s - 1) / s * s ;
} 

template<typename int_tt >
inline int_tt floorScaled(const int_tt& val, const int_tt& s) {
    return val / s * s ;
} 

slpp::desc_t    scidbDistrib(const slpp::desc_t& DESCA);

void infoG2L_zero_based(slpp::int_t globalRow, slpp::int_t globalCol, const slpp::desc_t& desc,
                        slpp::int_t NPROW, slpp::int_t NPCOL, slpp::int_t MYPROW, slpp::int_t MYPCOL,
                        slpp::int_t& localRowOut, slpp::int_t& localColOut);
///
/// template argument for the extractToOp<> function
/// 
/// This operator is used as the template arg to extractToOp<Op_tt>(Array)
/// extractToOp passes over every cell of every chunk in the Array
/// at that node and calls Op_tt::operator()(val, row, col).  This operator
/// subtracts ctor arguments {minrow, mincol} from {row,col} and
/// stores the result in "data" which is the local instance's share
/// of a ScaLAPACK-format ScaLAPACK matrix.
///
/// SciDB chunks in psScaLAPACK distribution are written as ScaLAPACK blocks.
/// It is an error to use ReformatToScalapack on SciDB arrays that are not
/// in psScaLAPACK distribution.
/// This is why this class name is "Reformat..." instead of "Redistribute..."
/// 
/// Ctor args: 
/// + data: pointer to the ScaLAPACK array of doubles
/// + desc: the ScaLAPACK descriptor of "data"
/// + (minrow, mincol): the minimum value of the SciDB dimensions, such
/// that the SciDB array value at [minrow,mincol] can be stored at ScaLAPACK
/// location [0,0] (in the global description of both)
/// 
class ReformatToScalapack {
public:
    ReformatToScalapack(double* data, const slpp::desc_t& desc,
                        int64_t minrow, int64_t mincol,
                        slpp::int_t  NPROW, slpp::int_t  NPCOL,
                        slpp::int_t MYPROW, slpp::int_t MYPCOL);
    void    blockBegin();
    void    blockEnd();
    void    operator()(double val, size_t scidbRow, size_t scidbCol);
private:
    enum BlockState { BlockEnded, BlockEmpty, BlockInProgress };
    double*             _data ;
    slpp::desc_t        _desc ;
    slpp::desc_t        _desc_1d ;
    int64_t             _minrow ;
    int64_t             _mincol ;
    int64_t             _NPROW ;
    int64_t             _NPCOL ;
    int64_t             _MYPROW ;
    int64_t             _MYPCOL ;
    enum BlockState     _blockState ;
    int64_t             _toLocalRow ;
    int64_t             _toLocalCol ;

};

inline void ReformatToScalapack::blockBegin() {
    if (ReformatToScalapack::BlockEnded != _blockState) {
        // required order is blockBegin(), [operator()()...] , blockEnd()
        // blockBegin() must have been called after blockBegin() or operator()()
        throw (SYSTEM_EXCEPTION(SCIDB_SE_OPERATOR, SCIDB_LE_OPERATION_FAILED) << "blockBegin() after operator()() or blockEnd()");
    }
    _blockState = ReformatToScalapack::BlockEmpty; // allows operator()() or blockEnd() next
}

inline void ReformatToScalapack::blockEnd() {
    if (ReformatToScalapack::BlockEnded == _blockState) {
        // required order is blockBegin(), [operator()()...] , blockEnd()
        // blockEnd() must have been called after blockEnd()
        throw (SYSTEM_EXCEPTION(SCIDB_SE_OPERATOR, SCIDB_LE_OPERATION_FAILED) << "blockEnd() after blockEnd()");
    }
    _blockState = ReformatToScalapack::BlockEnded; // allow blockBegin() next
}

/// this inlines into a loop that iterates over a "chunk" of SciDB data.
/// however, the SciDB data may be sparse, and many values of Row,Col will be skipped.
/// Col advances more frequently than Row, so-called "Column Major" order, which is at odds
/// with ScaLAPACK standard 'Row Major"  order, common in most numerical codes for dense data.
/// So its important that the ScaLAPACK memory that is being written fit in, e.g. L2 caches to
/// since this is a "memory transpose."
/// If the SciDB chunk and ScaLAPACK block are allowed to grow to exceed that size, it would be
/// faster to extract the data into Column-Major order in the ScaLAPACK memory, and then conduct
/// and cache-aware in-place transpose.
/// However, since ScaLAPACK itself will probably slow down if MB x NB x sizeof({single,double})
/// starts exceeding L2, we assume that the chunks we are receiving are already L2-friendly,
/// so we won't worry about this right away.
/// We can profile vs cache-misses to see if this does become a cache miss hotspot.

inline void ReformatToScalapack::operator()(double val, size_t scidbRow, size_t scidbCol)
{
#if !defined(NDEBUG) && defined(SCALAPACK_DEBUG)
    // TODO: convert to LOG4CXX_TRACE()
    std::cerr << "ReformatToScalapack::operator()(" << static_cast<void*>(_data)
              << ", " << scidbRow
              << ", " << col
              << ", " << val
              << ");" << std::endl;
#endif
    if (val == 0.0) return ; // no re-zeroing of already zeroed memory (bandwidth conservation)

    // the minimum scidb{Row,Col} may not be (0,0). to change to
    // 0-based indices, we subtract (_minrow, _mincol)
    slpp::int_t globalRow = scidbRow-_minrow ; // ftn: PDELSET().IA-1 = INFOG2L().GRINDX-1 = INFOG2L().GRCPY
    slpp::int_t globalCol = scidbCol-_mincol ; // ftn: PDELSET().JA-1 = INFOG2L().GCINDX-1 = INFOG2L().GCCPY

    const bool isUsingPdelset=false;
    if(isUsingPdelset) {
#if !defined(NDEBUG) && defined(SCALAPACK_DEBUG)
        // TODO: convert to LOG4CXX_TRACE()
        // retained only for A/B timing tests to measure improvement vs the old way
        std::cerr << "    scidb_pdelset_(" << static_cast<void*>(_data)
                  << ", globalRow+1=" << globalRow+1
                  << ", globalCol+1=" << globalCol+1
                  << "_desc , val=" << val << ");" << std::endl ;
#endif

        // Fortran Call: SUBROUTINE SCIDB_PDELSET( A, IA, JA, DESCA, ALPHA )
        scidb_pdelset_(_data, globalRow+1, globalCol+1, _desc, val); // +1: converts to Fortran 1-based indexing
    } else {
        // we'll do scidb_pdelset_ functionality here.
        // 1. to amortize the cost of INFOG2L, which PDELSET() calls every time.
        // 2. to get rid of another Fortran dependency.

        // here's useful info about the Fortran code we mimick:
        // Fortran: SUBROUTINE PDELSET( A, IA, JA, DESCA, ALPHA )
        // Fortran: does CALL INFOG2L(    IA,      JA, DESCA, NPROW, NPCOL, MYROW, MYCOL,    IIA,    JJA, IAROW, IACOL )
        // Fortran  SUBROUTINE INFOG2L(GRINDX, GCINDEX, DESCA, NPROW, NPCOL, MYROW, MYCOL, LRINDX, LCINDX, RSRC, CSRC )

        // important correspondences
        // here          PDELSET      INFOG2L
        // 0-based       1-based  1-based 0-based
        // --------------------------------------
        // globalRow+1   IA       GRINDX, GRCPY+1
        // globalCol+1   JA       GCINDX, GCCPY+1
        // localRow+1    IIA      LRINDX
        // localCol+1    JJA      LCINDX


        assert(ReformatToScalapack::BlockEnded != _blockState) ; // blockBegin() must precede operator()

        if (ReformatToScalapack::BlockEmpty == _blockState) {
            // first value provided for this block

            // slow conversion from global to local coordinates
            // but memoize the result to be re-used for all values in the same block
            //
            // otherwise infoG2L becomes a significant contributor to the extractArrayToScalapack() bottleneck.

            slpp::int_t localRow, localCol; // infoG2L_zero_based() outputs
            infoG2L_zero_based(globalRow, globalCol, _desc, _NPROW, _NPCOL, _MYPROW, _MYPCOL, localRow, localCol);

            // for one chunk and its corresponding ScaLAPACK block,
            // the difference between local{Row,Col} and global{Row,Col} is constant.
            // So if we save the difference, we can re-use that offset as a fast
            // conversion for all other values in this same block.
            // This amortizes the cost of infoG2L, which was contributing a cpu bottleneck
            // during
            _toLocalRow = localRow - globalRow ;
            _toLocalCol = localCol - globalCol ;

            // BlockInProgress state means we've memoized the above two values
            // and won't repeat the above calcs until after a blockEnd() then blockBegin() has occurred.
            _blockState = ReformatToScalapack::BlockInProgress;
        }

        // fast convert from global to local
        const slpp::int_t localRow = globalRow + _toLocalRow ;
        const slpp::int_t localCol = globalCol + _toLocalCol ;

        // check that the short-cut mapping from global to local is correct during Debug builds
        if(isDebug()) {
            // this is a very slow check
            // we we will check the memoization against infoG2L for every value
            // which we will not do in a Release build, because its so slow.
            slpp::int_t localRowCheck, localColCheck; // infoG2L_zero_based() outputs
            infoG2L_zero_based(globalRow, globalCol, _desc, _NPROW, _NPCOL, _MYPROW, _MYPCOL, localRowCheck, localColCheck);
            assert(localRow == localRowCheck);
            assert(localCol == localColCheck);
        }

        // write _data in column-major layout required by ScaLAPACK
        // to include a 1000 * (2^31/1000+1) test case
        ssize_t columnOffset = ssize_t(localCol) * _desc.LLD;
        ASSERT_EXCEPTION(columnOffset >= 0, "bad offset");
        _data[ localRow + columnOffset ] = val ; // PDELSET: $ A( IIA+(JJA-1)*DESCA( LLD_ ) ) = ALPHA

        // NOTE: localCol varies faster than localRow in Scidb, so writing
        //       in column-major order will have extraordinarily high
        //       L1 & L2 cache rates and even for L3 when MB*NB*8 exceeds size(L3) / num cores
        //       at that point it slows all the way down to being memory-bandwidth limited
        //       for each and every value, since there will be 3 levels of cache miss on
        //       every write.
        // TODO: when MB=NB and the block doesn't fit in L1, it will be faster to e.g. write it
        //       in row-major order, and then in endBlock() invoke a fast cache-oblivious efficient
        //       in-place transpose into column-major order.
        //       This will be important when using chunks/blocks that are not e.g. 32x32
        //       but are growing toward 1K x 1K (8 mebibytes) which will require more
        //       than one core's share of L3 on typical Westmere, Sandybridge, IvyBridge, and Haswell
        //       processors.
        //
    }
}


///
/// template argument for the OpArray<> class
/// 
/// This operator is used to create an array from ScaLAPACK-format memory,
/// by constructing an OpArray<ReformatFromScalapack>.
///
/// Each time the OpArray<Op_tt> must produce a chunk, the chunk is filled
/// by calling Op_tt::operator()(row, col), which returns a double which
/// is the value of the array at SciDB integer dimensions (row,col)
/// 
/// SciDB chunks in psScaLAPACK distribution are read from ScaLAPACK blocks.
/// It is an error to use ReformatFromScalapack to produce a SciDB array that is
/// in psScaLAPACK distribution.
/// This is why this class name is "Reformat..." instead of "Redistribute..."
///
/// Ctor args: 
/// + data: pointer to the ScaLAPACK array of doubles
/// + desc: the ScaLAPACK descriptor of "data"
/// + (minrow, mincol): the minimum value of the SciDB dimensions, such
/// that the ScaLAPACK value [0,0] will be returned as ScidB array [minrow,mincol]
/// 
template<class Data_tt>
class ReformatFromScalapack {
public:
    ReformatFromScalapack(const Data_tt& data, const slpp::desc_t desc,
                              int64_t minrow, int64_t mincol, bool global=false)
    :
        _data(data),
        _desc(desc),
        _minrow(minrow),
        _mincol(mincol),
        _global(global)
    {
#if !defined(NDEBUG) && defined(SCALAPACK_DEBUG)
        std::cerr << "ReformatFrom _desc.DTYPE = " << _desc.DTYPE << std::endl;
        std::cerr << "ReformatFrom _desc.CTXT = " << _desc.CTXT << std::endl;
        std::cerr << "ReformatFrom _desc.M,N = " << _desc.M << "," << _desc.N << std::endl;
        std::cerr << "ReformatFrom _desc.MB,NB = " << _desc.MB << "," << _desc.NB << std::endl;
        std::cerr << "ReformatFrom _desc.RSRC,CSRC = " << _desc.RSRC << "," << _desc.CSRC << std::endl;
        std::cerr << "ReformatFrom _desc.LLD = " << _desc.LLD << std::endl;
#endif
    }

    inline double operator()(int64_t row, int64_t col) const
    {
        // I make it work in the local-only case by using a space for the
        // first two parameters.  This only permits it to work in the local process,
        // and not in SPMD style.  Otherwise it might have been more like
        // the line that precedes it.  But we are going to let SciDB do any post-operator
        // redistribution to other instances, since it uses a scheme that differs from
        // ScaLAPACK

        // SPMD: scidb_pdelget_('A', ' ', val, _data.get(), row-_minrow+1, col-_mincol+1, _desc);
        double val = static_cast<double>(0xbadbeef); // not strictly necessary

        // note: haven't seen a global matrix yet, so that's only handled for the
        //       1D case operator()(row), below
        scidb_pdelget_(' ', ' ', val, _data.get(), row-_minrow+1, col-_mincol+1, _desc);

#if !defined(NDEBUG) && defined(SCALAPACK_DEBUG)
        std::cerr << "ReformatFrom::operator()("<<row<<","<<col<<")" << std::endl ;
        std::cerr << "    minrow,mincol:"<<_minrow<<","<<_mincol<<")" << std::endl ;
        std::cerr << "    scidb_pdelget_(" << static_cast<void*>(_data.get())
                  << ", " << row - _minrow + 1
                  << ", " << col - _mincol + 1
                  << ", " << val
                  << ");" << std::endl;
#endif
        // TODO JHM ; check info here and in ReformatToScalapack
        return val;
    }

    // single-dimension version such as the 'values' of an SVD
    inline double operator()(int64_t row) const {
        using namespace scidb;

        #if !defined(NDEBUG) && defined(SCALAPACK_DEBUG)
            std::cerr << "ReformatFrom::operator()("<<row<<");" << std::endl;
        #endif

        double val = static_cast<double>(0xbadbeef); // not strictly necessary

        slpp::int_t R = row-_minrow ;
        if(_global) {
            // like the S vector output by pdgesvd() ... available at every host
            // so we can just take the value directly from the array
            val = _data.get()[R] ;
            if(R >= _desc.M) {
                throw (SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_UNKNOWN_ERROR) << "R >= _desc.M");
            }

            #if !defined(NDEBUG) && defined(SCALAPACK_DEBUG)
                std::cerr << "    _data.get()[R]="<<R<<"]" << "; val <- " << val << std::endl; 
            #endif
        } else {
            scidb_pdelget_(' ', ' ', val, _data.get(), row-_minrow+1, 1,  _desc);

            #if !defined(NDEBUG) && defined(SCALAPACK_DEBUG)
                std::cerr << "    scidb_pdelget_(' ', ' ', val, _data.get(), R+1=" << R+1
                          << ", C==1" << ", _desc={M=" << _desc.M 
                          << ", N=" << _desc.N << "})" << "; val <- " << val << std::endl; 
            #endif
        }
        return val;
    }

private:
    Data_tt             _data ;
    slpp::desc_t        _desc ;
    int64_t             _minrow ;
    int64_t             _mincol ;
    bool                _global ;
};

} // end namespace scidb

#endif // REFORMAT__HPP
