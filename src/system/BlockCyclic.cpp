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

/**
 * @file blockCyclic.cpp
 * @brief support for using scidb processes with algorithms that require blockCyclic layouts
 */

// standards
#include <assert.h>
#include <cmath>
#include <iostream>
#include <limits>
// defacto standards
#include <boost/numeric/conversion/cast.hpp>
#include <log4cxx/logger.h>
// scidb
#include <system/BlockCyclic.h>
#include <query/Query.h>

namespace scidb
{

static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.system.blockcyclic"));

// this holds some functions that are needed to support
// executing some operators via ScaLAPACK, which has particular
// requirements on data distribution

/// handy template for turning counts to block counts
///      given integers n,d,
///      returns [math, not C:] ceil(n/d),
template<typename int_tt, typename int2_tt>
    inline int_tt divCeil(int_tt n, int2_tt d) {
    return (n + d - 1) / d ;
}

// ProcGrid public methods ---------------------------------------

/// 
/// ProcGrid::ProcGrid
/// an object that factors the count of processes into the most
/// square 2D grid possible and then implements a number of functions
/// needed to use that process grid to allow SciDB to work with
/// ScaLAPACK slave processes.
/// 
ProcGrid::ProcGrid(const procNum_t numInstances)
:
    _maxGridSize(findFactorization(std::min(numInstances, procNumLimit())))
{
    // why? because procNum_t is unsigned, limiting it to this number
    //      catches bad casts from negative signed ints of the same
    //      size.
    const procNum_t limit = std::numeric_limits<procNum_t>::max()/2;
    if(numInstances >= limit) {
        assert(false);
        throw SYSTEM_EXCEPTION(scidb::SCIDB_SE_INTERNAL, SCIDB_LE_INVALID_FUNCTION_ARGUMENT);
    }
    //std::cerr << "ProcGrid::ProcGrid: _maxGridSize:" << _maxGridSize << std::endl;
}

///
/// ProcGrid::useableGridSize
/// when the chunks of the array are smaller than the process grid,
/// we only use and launch a subset of the original grid.  This
/// computes that subset
///
procRowCol_t ProcGrid::useableGridSize(const procRowCol_t& matrixSize,
                                       const procRowCol_t& blockSize) const
{
    // can only use as much of the grid as the matrix will cover
    // so get matrix size in chunks
    procNum_t chunkMaxRow = divCeil(matrixSize.row, blockSize.row);
    procNum_t chunkMaxCol = divCeil(matrixSize.col, blockSize.col);

    // return the minimum of the two sizes
    procRowCol_t result;
    result.row = std::min(_maxGridSize.row, chunkMaxRow);
    result.col = std::min(_maxGridSize.col, chunkMaxCol);
    //std::cerr << "ProcGrid::useableGridSize():" << result << std::endl;
    return result;
}

/// ProcGrid::procNum
/// give the 2D position of a process in the grid, this method
/// returns a number that is both the instanceID in SciDB and
/// the MPI rank of that process.  This defines the term
/// "procNum" and its corresonding type "procNum_t"
///
procNum_t ProcGrid::procNum(const procRowCol_t& gridPos,
                            const procRowCol_t& useGridSize) const
{
    // ScaLAPACK process grid, row-major order flavor.
    // CAREFUL!
    // We use row-major order to make instance number match
    // the sl_init() call (a ScaLAPACK helper, in FORTRAN),
    // which chooses the row-major ordering of the process grid
    // at the BLACS level.
    // Can't answer why sl_init() does it that way, there may be
    // a reason.  Don't know if ScaLAPACK itself assumes that since
    // the BLACS can number them in row- or column-major order.
    const procNum_t result = gridPos.row * useGridSize.col + gridPos.col;

    if(result >= useGridSize.row * useGridSize.col) {
        throw SYSTEM_EXCEPTION(scidb::SCIDB_SE_INTERNAL, SCIDB_LE_INVALID_FUNCTION_ARGUMENT);
    }

    return result;
}

/// ProcGrid::gridPos
/// given the procNum, return the 2D position in the process grid
/// of that procNum
// TODO JHM: would be nice to have a unit test that getPnum(getGridPos(x)) = x 
//           where do those go?
procRowCol_t ProcGrid::gridPos(const procNum_t procNum,
                               const procRowCol_t& useGridSize) const
{
    // NOTE: its row-major order to match ScaLAPACK
    procRowCol_t gridPos;
    gridPos.row = procNum / useGridSize.col ;
    gridPos.col = procNum - (gridPos.row * useGridSize.col) ;
    return gridPos;
}

// ProcGrid private methods ---------------------------------------

// ProcGrid private static methods --------------------------------


/// ProcGrid::gridPos1D
/// the calculation of which proc in the procGrid is going to handle a particular
/// chunk is the same in both the row and column direction.  This method
/// computes either axis ... that is the meaning of the "1D"
/// so you give it either:
/// (proc row, dim[0].getChunkInteval(), dim[0].getLenght()) OR
/// (proc col, dim[1].getChunkInteval(), dim[1].getLenght())
/// and it returns the corresponding coordinate in the procGrid
procNum_t ProcGrid::gridPos1D(const procNum_t pos, const procNum_t chunkSize, const procNum_t nRowsOrCols)
{   // static member
    return (pos/chunkSize) % nRowsOrCols;
}

/// ProcGrid::procNumLimit()
/// This provides the architecture/API (but not a serious implementation)
/// for a tool we will need for testing, that can artifically limit the number
/// of processes that are eligible for use in the process grid.  The idea is
/// that we will be able to change this number [by some as-yet unidentified means]
/// between DLA/ScaLAPACK testing runs without having to restart the SciDB
/// with a new number of instances, just to test that all the fucntions work
/// correctly.  Right now, the implementation is a non-useful "place holder"
/// that reads an integer from an environment variable.  What we want to do
/// is to instead have some sort of query or "magic file" that can change
/// the value between queries that test ScaLAPACK-based functionality.
procNum_t ProcGrid::procNumLimit()
{
    // limit is the lowest unsigned number that could have been cast from
    // an signed number of the same size.  This catches common mistakes
    // when an interface uses unsigned numbers that don't need the largest
    // range possible

    // TODO: find a name and header for the resuse of the expression below 
    procNum_t limit = std::numeric_limits<procNum_t>::max()/2 ;

    procNum_t result = limit -1 ;
#ifndef NDEBUG
    // when we have an operator to provide debugMax, use what it sets
    // and remove the NDEBUG.  This provides a hook for testing that
    // we want have in NDEBUG builds when we have a standard mechanism
    // for setting test parameters to avoid server restarts
    procNum_t debugMax = result; // change me to debug,
    result = std::min(result, debugMax); 
#endif
    assert(result < limit);
    return result;
}

///
/// ProcGRid:findFactorization
///
/// finds largest first factor up to sqrt(P), which makes it find the most-square
/// rectangle that it can.  However if P is prime, for example it will return
/// a 1 x P grid.
///
/// can be slow on large clusters as it grows with O(P) = sqrt(P),
/// P is the number of procs allowed
///
/// it could be improved to find factors of slightly smaller rectangles that
/// are not factors of numProc, but which tile most matrices better.
/// for example when numProc = 101, 10 x 10 is probably a faster configuration
/// than 1 x 101 for many matrices with edges in the 10's and 100's on edge.
///

/// don't call this more than once per query... it will be somewhat
/// expensive as the total number of instances rises
procRowCol_t ProcGrid::findFactorization(procNum_t numProc)
{
    procRowCol_t gridSize = {1,1};

    procNum_t searchLimit = boost::numeric_cast<procNum_t>(::sqrt(numProc));
    for(procNum_t ii = 1; ii <= searchLimit; ++ii) {
        if(numProc % ii == 0) {
            gridSize.row = ii ;        // find largest factor smaller than sqrt(P)
        }   
    }   
    gridSize.col = numProc / gridSize.row  ; // leaves any non-rectangular portion behind

    assert(gridSize.row <= gridSize.col);  // rectangles must be horizontal
    return gridSize ;
}

///////////////////////////////////////////////////////////////////////
// This is an example of how we can choose special values
// for certain cases.  We cannot know whether we will need that kind
// kind of optimization until later in the development process.
///////////////////////////////////////////////////////////////////////
procNum_t getFactorizationSpecialCases(procNum_t numMPIProc)
{
    // TODO JHM ; just turn this into a table lookup for
    //      values below 100
    //      above that, ....?
    if(numMPIProc <= 9) {
        return 1; // ScaLAPACK recommendation
    }

    // change to even-number case analysis
    procNum_t E = numMPIProc/2*2 ; // subtract 1 when odd
    switch(E){
    case  2: return 1 ;     // 1 x 2
    case  4: return 2 ;     // 2 x 2
    case  6: return 2 ;     // 2 x 3
    case  8: return 2 ;     // 2 x 4
    // note, the above 4 cases are for testing only, and require
    // the early return if(numMPIProc <=9) to be commented out
    case 10: return 2 ;     // 2 x 5
    case 12: return 3 ;     // 3 x 4
    case 14: return 2 ;     // 2 x 7
    case 16: return 4 ;     // 4 x 4
    case 18: return 3 ;     // 3 x 6
    case 20: return 4 ;     // 4 x 5
    case 22: return 2 ;     // 2 x 11 (may be better than 3 x 7)
    case 24: return 4 ;     // 4 x 6
    case 26: return 2 ;     // 2 x 13 (may be better than 4 x 6)
    case 28: return 4;      // 4 x 7 
    case 30: return 3;      // 3 x 5 
    case 32: return 4;      // 4 x 8
    case 34: return 4;      // 4 x 17 (may be better than 4 x 8) 
    case 36: return 6;      // 6 x 6
    case 38: return 4;      // 2 x 19 (may be better than 6 x 6)
    case 40: return 4;      // 5 x 8
    case 42: return 6;      // 6 x 7
    case 44: return 6;      // 6 x 7
    case 46: return 6;      // 6 x 7
    case 48: return 6;      // 6 x 8 
    case 50: return 5;      // 5 x 10 (may be better than 7 x 7)
    }
    return procNum_t( ::sqrt(numMPIProc) ) ; // e.g for  50, would return 7 -> 7 x 7
}

InstanceID PartitioningSchemaDataForScaLAPACK::getInstanceID(const Coordinates& chunkPos, const Query& query) const {
    //
    // Compute instanceID for the SciDB instance that is the master
    // to the ScaLAPACK-MPI process where the chunk will be processed by
    // ScaLAPACK.
    // Currently, the InstanceID == MPI rank because the MPI code mapped them that way,
    // but nothing in SciDB should assume that, because it can easily need to change in the
    // future.  So here we simply forward to the procGrid code that manages the MPI/ScaLAPACK side
    // of things.
    //

    const ProcGrid * const procGrid = query.getProcGrid();

    procRowCol_t gridPos;
    gridPos.row = procGrid->gridPos1D(chunkPos[0], _blacsBlockSize.row, _blacsGridSize.row);
    gridPos.col = procGrid->gridPos1D(chunkPos[1], _blacsBlockSize.col, _blacsGridSize.col);
    procNum_t mpiRank = procGrid->procNum(gridPos, _blacsGridSize);

    LOG4CXX_TRACE(logger, "PartitioningSchemaDataScaLAPACK::getInstanceID(chunkPos=(" << chunkPos[0] << ", " <<chunkPos[1] << ")"
                          << ", _blacsBlockSize (" << _blacsBlockSize.row << "," << _blacsBlockSize.col << ")"
                          << ", _blacsGridSize (" << _blacsGridSize.row << "," << _blacsGridSize.col << ")"
                          << "-> gridPos (" << gridPos.row << "," << gridPos.col << ")"
                          << "-> rank " << mpiRank);
    return mpiRank;

}

} // namespace
