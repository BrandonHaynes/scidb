#ifndef BLOCK_CYCLIC__H
#define BLOCK_CYCLIC__H
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

///
/// @file BlockCyclic.h
/// @brief ScaLAPACK support requires having ScaLAPACK-compatible chunk distributions.
///        There are functions in ScaLAPACK to make such distributions, but we can't
///        do that directly from SciDB, because we can't allow SciDB to be an MPI
///        process (MPI is contra-indicated for server processes).
///        These functions allow us to use SciDB's chunk-distribution code
///        to do the same thing. (Though not as efficiently as MPI can)
///

#include <stdint.h>

#include <array/Metadata.h>

namespace scidb
{
class Query;
// A "proc" means a node in a systolic array of processes.
// These correspond to pairs of (SciDB instanceID, MPI slave rank)

typedef uint32_t procNum_t ;     // instanceID == MPI rank number in our scheme
                                 // process grid manipulations / systolic arrays
                                 // block cyclic distributions

                                 // 31 bits should suffice for now (2G limit)
                                 // we can catch illegal casts of 32bit and larger
                                 // numbers by limit this to 31 bits instead of 32

                                 // note we only use 31 bits so that if signed
                                 // negative numbers get cast to this by mistake
                                 // they will have the high bit set and we can
                                 // range check that all values < 2^31-1
                                 // rather using the full uint range.

                                 // the max to that even though unsigned so that
                                 // negative int -> unsinged is easy to catch
                                 // in a single range check.
                                 // all arithmetic is unsigned, but
                                 // the MPI & ScaLAPACK were compiled with
                                 // TODO JHM; this will be integrated after
                                 // 64-bit MPI & ScaLAPACK are configured into CMake
                                 // (right now its standard packages that are 32-bit)
                                 // however, it really is always unsigned
                                 // TODO JHM;
                                 // and initialize it to max(uprocNum_t)/2


// manage 2D Block Cyclic via pairs of numbers
// (can extend up to 4D in theory, but only 2D in ScaLAPACK)
template<class int_tt>
class RowCol {
public:
    int_tt  row;
    int_tt  col;
};

template<class int_tt>
std::ostream& operator<<(std::ostream& os, const RowCol<int_tt>& val) {
    os << "(" << val.row << "," << val.col << ")" ;
    return os;
}


typedef RowCol<procNum_t> procRowCol_t ;

//
// optimal grid construction is O(P) = sqrt(P), P=number of instances
//
class ProcGrid {
public:
                        ProcGrid(const procNum_t numInstances);
    procRowCol_t        useableGridSize(const procRowCol_t& MN, const RowCol<procNum_t>& MNB) const;
                        // grid to instance number and back
    procNum_t           procNum(const procRowCol_t& gridPos,
                                const procRowCol_t& useGridSize) const;

    procRowCol_t        gridPos(const procNum_t pNum,
                                const procRowCol_t& useGridSize) const;

    // static members
    static procNum_t    gridPos1D(const procNum_t pos, const procNum_t chunkSize, const procNum_t nRowsOrCols);

private:
    static procNum_t    procNumLimit();
    static procRowCol_t findFactorization(procNum_t numProc);

    procRowCol_t _maxGridSize;
};


/**
 * This class implements the PartitioningSchemaData API, which is to say that it
 * contains parameterization of a psScaLAPACK distribution which isn't computable from
 * the Array (matrix) alone, but depends on the ScaLAPACK operator and other matrices
 * participating in that operator, and has to be supplied explicitly by the operator
 * calling the redistribute() function.
 * One can consider this class to be an adaptor to (or wrapper around) the pre-existing
 * (and somewhat evolving) methods of the ProcGrid class and its associated types
 * and functions which define the ScaLAPACK block-cyclic distribution in
 * ScaLAPACK's terms separately as much as possible to avoid confusing it with the
 * SciDB terminology.
 */
class PartitioningSchemaDataForScaLAPACK: public PartitioningSchemaData {
public:
    /**
     * @param blacsGridSize contains the first two arguments to set_fake_blacs_gridinfo_(),
     * @param blacsBlockSize contains the common blocksize of all ScaLAPACK matrices as
     *        participating in the ScaLAPACK operator
     */
    PartitioningSchemaDataForScaLAPACK(procRowCol_t blacsGridSize, RowCol<procNum_t> blacsBlockSize)
        : _blacsGridSize(blacsGridSize), _blacsBlockSize(blacsBlockSize){}

    /**
     * @see PartitioningSchemaData::getID
     */
    virtual PartitioningSchema getID() { return psScaLAPACK; }

    /**
     * map a scidb chunk to a particular instance where it must be located to be
     * accessed by memory-mapped ScaLAPACK slave operators (in Fortran) which
     * expect/require them to be located in particular SciDB processes where the
     * corresponding MPI/ScaLAPACK processes require them to be.
     *
     * @param  chunkPos coordinates of the upper-left corner of the chunk
     * @param  query    current query
     * @return the InstanceID where that chunk must go according to ScaLAPACK rules
     * @see    redistribute() and getInstanceForChunk() (Operator.cpp)
     *
     */
    virtual InstanceID getInstanceID(const Coordinates& chunkPos, const Query& query) const ;
private:

    procRowCol_t        _blacsGridSize;     // size of the ScaLAPACK compute grid for the operation in question
                                            // as used in the BLACS function
    RowCol<procNum_t>   _blacsBlockSize;    // ScaLAPACK block size
};


} // end namespace scidb

#endif // BLOCK_CYCLIC__H
