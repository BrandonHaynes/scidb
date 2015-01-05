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

/*
 * PhysicalSpgemm.cpp
 *
 *  Created on: November 4, 2013
 */

// C++
#include <limits>
#include <limits>
#include <sstream>

// boost
#include <boost/unordered_map.hpp>

// scidb
#include <query/Operator.h>
#include <util/Platform.h>
#include <array/Tile.h>
#include <array/TileIteratorAdaptors.h>
#include <system/Sysinfo.h>

// local
#include "../LAErrors.h"
#include "CSRBlock.h"
#include "CSRBlockVector.h"
#include "SpAccumulator.h"
#include "SpAccumulatorUtils.h"
#include "SpgemmBlock.h"
#include "SpgemmBlock_impl.h"
#include "spgemmSemiringTraits.h"


namespace scidb
{
using namespace boost;
using namespace scidb;

// for performance analysis
struct SpgemmTimes {
    SpgemmTimes() : totalSecs(0.0) {;}
    std::vector<double>     redistributeLeftSecs;  //
    std::vector<double>     redistributeRightSecs;  // per psByCol rotation
    std::vector<double>     loadLeftSecs;           // per psByCol rotation
    std::vector<double>     loadLeftCopySecs;       // per psByCol rotation
    std::vector<double>     loadRightSecs;          // per psByCol rotation
    std::vector<double>     blockFindSecs;            // per psByCol rotation
    std::vector<double>     blockMultSecs;            // per psByCol rotation
    std::vector<double>     flushSecs;              // per psByCol Rotation
    double                  totalSecs;
};

/**
* print a SpgemmTimes on an ostream
* @param os    -- the ostream to print on
* @param times -- the SpgemmTimes structure
* @return      -- the ostream, os, that was passed in
*/
std::ostream& operator<<(ostream& os, const SpgemmTimes& times)
{
    os << "spgemm(): " << std::endl;
    for(size_t ii=0; ii<times.redistributeRightSecs.size(); ++ii) {
        os << "round: " << ii << "--------------" << std::endl ;
        os << "redistributeLeftSecs: " << times.redistributeLeftSecs[ii] << std::endl ;
        os << "redistributeRightSecs: " << times.redistributeRightSecs[ii] << std::endl ;
        os << "loadLeftSecs:          " << times.loadLeftSecs[ii] << std::endl ;
        os << "  loadLeftCopySecs:    " << times.loadLeftCopySecs[ii] << std::endl ;
        os << "loadRightSecs:         " << times.loadRightSecs[ii] << std::endl ;
        os << "blockFindSecs:         " << times.blockFindSecs[ii] << std::endl ;
        os << "blockMultSecs:         " << times.blockMultSecs[ii] << std::endl ;
        os << "flushSecs:             " << times.flushSecs[ii] << std::endl ;
    }
    os << "--------------------------------" << std::endl ;
    os << " totalSecs: " << times.totalSecs << std::endl ;

    return os;
}


class PhysicalSpgemm : public  PhysicalOperator
{
    TypeEnum _typeEnum; // the value type as an enum
    Type _type; // the value type as a type

public:
    PhysicalSpgemm(std::string const& logicalName,
                     std::string const& physicalName,
                     Parameters const& parameters,
                     ArrayDesc const& schema):
        PhysicalOperator(logicalName, physicalName, parameters, schema),
        _typeEnum(typeId2TypeEnum(schema.getAttributes()[0].getType())),
        _type(TypeLibrary::getType( schema.getAttributes()[0].getType() ))
    {
    }

    virtual bool changesDistribution(std::vector<ArrayDesc> const&) const
    {
        return true;
    }

    virtual ArrayDistribution getOutputDistribution(
            std::vector<ArrayDistribution> const& inputDistributions,
            std::vector<ArrayDesc> const& inputSchemas) const
    {
        return ArrayDistribution(psByRow);
    }

    boost::shared_ptr< Array> execute(std::vector< boost::shared_ptr< Array> >& inputArrays, boost::shared_ptr<Query> query);

private:
    /**
     * same args as execute(), but templated on the a class corresponding to the semiring (arithmetic rules for + and *)
     * that will be used during the sparse multiplication.
     */
    typedef enum dummy { SRING_PLUS_STAR, SRING_MIN_PLUS, SRING_MAX_PLUS, SRING_COUNT_MULTS } sringEnum_t ;
    template<class SemiringTraits_tt>
    boost::shared_ptr<Array> executeTraited(std::vector< boost::shared_ptr< Array> >& inputArrays,
                                            boost::shared_ptr<Query>& query);

    /**
     * Multiply two arrays, with an SPMD algorithm.
     *
     * @param[in]  resultArray  the iterator for the result array
     * @param[in]  leftArray -- a BY_ROWS subset of the rows of the total leftArray
     * @param[in]  rightArray -- a BY_COLS subset of the columns of the total rightArray
     * @param[in]  query  the query context
     *
     * @note -- It is the caller's responsibility to call this method once per unique subset of columns that are
     *          present on each instance in successive BY_ROWS re-distributions (Rotated Cannon-style in the rows).
     *          It is also the callers responsibilyt to call the re-distrubte of the columns between calls to this method.
     *          The leftArray subset is assumed to never change, and the overall algorithm will then produce output
     *          in a BY_ROWS distribution.
     */
    template<class SemiringTraits_tt>
    void spGemmColumnSubset(shared_ptr<Array>& leftArray, shared_ptr<Array>& rightArray,
                            shared_ptr<ArrayIterator>& resultArray, shared_ptr<Query>& query, SpgemmTimes& times);

    /**
     * get the chunk positions of an array, sorted in a particular order.
     * small detail factored from spGemmColumnSubset
     * @param array
     * @param result a container for the algorithm to fill [TODO: change to accept an output random-access iterator (random to support sort())
     */
    template<class CoordinatesComparator_tt>
    void getChunkPositions(shared_ptr<Array>& array, vector<Coordinates>& result);

    /**
     * copy a chunk of data to a CSRBlock, with optional return of a list of rows used by the chunk
     * @param chunk             the source of data
     * @param spBlock           the destination of the data
     * @param rowsInUseOptional [optional] pointer to a set which will be filled with the rows that are in use.
     */
    template<class SemiringTraits_tt, class Block_tt>
    void   copyChunkToBlock(const scidb::ConstChunk& chunk, boost::shared_ptr<Block_tt>& spBlock,
                            std::set<Coordinate>* rowsInUseOptional, const shared_ptr<Query>& query);

    /**
     * convenience routine for performance timing
     * @return the floating-point equivalent of clock_gettime(CLOCK_MONOTONIC_RAW,)
     */
    double getMonotonicrawSecs();

    /**
     *
     * @return the floating-point equivalent of clock_gettime(CLOCK_THREAD_CPUTIME_ID,)
     */
    double getThreadSecs();
};


boost::shared_ptr< Array> PhysicalSpgemm::execute(std::vector< boost::shared_ptr< Array> >& inputArrays, boost::shared_ptr<Query> query)
{
    assert(inputArrays.size()==2); // should not happen to developer, else inferSchema() did not raise an exception as it should have

    sringEnum_t sringE= SRING_PLUS_STAR ; // the standard ring (TYPE, +,*) over all supported types is the default.
    // get string from the optional 3rd argument, if present.
    // it holds the name of alternative ring arithmetic to use
    std::string namedOptionStr;
    if (_parameters.size() >= 1) {
        assert(_parameters[0]->getParamType() == PARAM_PHYSICAL_EXPRESSION);
        typedef boost::shared_ptr<OperatorParamPhysicalExpression> ParamType_t ;
        ParamType_t& paramExpr = reinterpret_cast<ParamType_t&>(_parameters[0]);
        assert(paramExpr->isConstant());
        namedOptionStr = paramExpr->getExpression()->evaluate().getString();

        if (namedOptionStr == "min.+") {
            if (_typeEnum == TE_FLOAT || _typeEnum == TE_DOUBLE)
                sringE= SRING_MIN_PLUS ;
            else
                throw (SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_OPERATION_FAILED)
                       << "PhysicalSpgemm::execute(): the 'min.+' option supports only float or double attributes");
        } else if (namedOptionStr == "max.+") {
            if (_typeEnum == TE_FLOAT || _typeEnum == TE_DOUBLE)
                sringE= SRING_MAX_PLUS ;
            else
                throw (SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_OPERATION_FAILED)
                       << "PhysicalSpgemm::execute(): the 'max.+' option supports only float or double attributes");
        } else if (namedOptionStr == "count-mults") {
            if (_typeEnum == TE_FLOAT || _typeEnum == TE_DOUBLE)
                sringE= SRING_COUNT_MULTS ;
            else
                throw (SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_OPERATION_FAILED)
                       << "PhysicalSpgemm::execute(): the 'count-mults' option supports only float or double attributes");
        } else {
            throw (SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_OPERATION_FAILED)
                   << "PhysicalSpgemm::execute(): no such option '" << namedOptionStr << "'");
        }
    }


    switch(_typeEnum) {
    case TE_FLOAT:
        switch (sringE) {
        case SRING_PLUS_STAR:
            return executeTraited<SemiringTraitsPlusStarZeroOne<float> >(inputArrays, query);
        case SRING_MIN_PLUS:
            return executeTraited<SemiringTraitsMinPlusInfZero <float> >(inputArrays, query);
        case SRING_MAX_PLUS:
            return executeTraited<SemiringTraitsMaxPlusMInfZero <float> >(inputArrays, query);
        case SRING_COUNT_MULTS:
            return executeTraited<SemiringTraitsCountMultiplies <float> >(inputArrays, query);
        default:
            assert(false);
        }
    case TE_DOUBLE:
        switch (sringE) {
        case SRING_PLUS_STAR:
            return executeTraited<SemiringTraitsPlusStarZeroOne<double> >(inputArrays, query);
        case SRING_MIN_PLUS:
            return executeTraited<SemiringTraitsMinPlusInfZero <double> >(inputArrays, query);
        case SRING_MAX_PLUS:
            return executeTraited<SemiringTraitsMaxPlusMInfZero <double> >(inputArrays, query);
        case SRING_COUNT_MULTS:
            return executeTraited<SemiringTraitsCountMultiplies <double> >(inputArrays, query);
        default:
            assert(false);
        }
    //case TE_BOOL:    // Someday.  Lets get the packing issues right first.
    default:
        SCIDB_UNREACHABLE();
    }

    return shared_ptr<Array>();
}


template<class SemiringTraits_tt>
boost::shared_ptr<Array> PhysicalSpgemm::executeTraited(std::vector< boost::shared_ptr< Array> >& inputArrays, boost::shared_ptr<Query>& query)
{
    SpgemmTimes times;

    // Create a result array.
    shared_ptr<MemArray> resultArray = make_shared<MemArray>(_schema, query);
    shared_ptr<ArrayIterator> resultArrayIter = resultArray->getIterator(0);

    // We need to duplicate the right array to all instances, and multiply with the local chunks.
    // One option is to duplicate.
    // Another option is to rotate, which is what is used. In more detail:
    // In each rotation, the chunks in the same col are distributed to the same instance, with a 'shift'.
    // E.g. if in the first rotation, a whole column goes to instance 5, in the next rotation the columns will go to instance 6.
    // warning: distribution of columns is NOT optimal for large instanceCounts (where communication limits even weak scaling),
    //          or for small matrices with a chunk size that is smaller than necessary.
    // redistribute the left array, so that chunks in the same row are distributed to the same instance.
    double timeOrigin = getMonotonicrawSecs(); // TODO: refactor as getSecs(MONOTONIC_RAW) etc
    shared_ptr<Array> leftArray = redistribute(inputArrays[0], query, psByRow);
    times.redistributeLeftSecs.push_back(getMonotonicrawSecs()-timeOrigin) ;

    shared_ptr<Array> rightArray = inputArrays[1];
    const size_t instanceCount = query->getInstancesCount();
    for (size_t i=0; i<instanceCount; ++i) {
        // next subset of the columns of rightArray
        double timeLocal= getMonotonicrawSecs();
        rightArray = redistribute(rightArray, query, psByCol, "", ALL_INSTANCES_MASK, shared_ptr<DistributionMapper>(), i);
        times.redistributeRightSecs.push_back(getMonotonicrawSecs()-timeLocal) ;

        // do the sub-calculation for that column subset
        spGemmColumnSubset<SemiringTraits_tt>(leftArray, rightArray, resultArrayIter, query, times);
    }

    times.totalSecs = getMonotonicrawSecs() - timeOrigin;
    if(getenv("SPGEMM_STDERR_TIMINGS")) {
        std::cerr << times << std::flush;
    }
    if(getenv("SPGEMM_CLIENT_WARNING_TIMINGS")) {
        query->postWarning(SCIDB_PLUGIN_WARNING("LAlinear", LA_WARNING4) << times);
    }

    return resultArray;
}


template<class SemiringTraits_tt>
void PhysicalSpgemm::spGemmColumnSubset(shared_ptr<Array>& leftArray, shared_ptr<Array>& rightArray,
                                        shared_ptr<ArrayIterator>& resultArray, shared_ptr<Query>& query, SpgemmTimes& times)
{
    typedef typename SemiringTraits_tt::Value_t Value_t;
    typedef typename SemiringTraits_tt::OpAdd_t OpAdd_t ;
    typedef typename SemiringTraits_tt::IdAdd_t IdAdd_t ;
    typedef CSRBlock<Value_t> LeftBlock_t; // chunks will be converted to matrix blocks which are efficient for sparse operations

    typedef SpgemmBlock<Value_t> RightBlock_t;
    typedef boost::unordered_map<Coordinate, shared_ptr<RightBlock_t> > RightBlockMap_t; // a map of a column of right blocks

    times.loadRightSecs.push_back(0) ;
    times.loadLeftSecs.push_back(0) ;
    times.loadLeftCopySecs.push_back(0) ;
    times.blockFindSecs.push_back(0) ;
    times.blockMultSecs.push_back(0) ;
    times.flushSecs.push_back(0) ;

    // method invariants:
    size_t leftChunkRowSize = leftArray->getArrayDesc().getDimensions()[0].getChunkInterval();
    size_t leftChunkColSize = leftArray->getArrayDesc().getDimensions()[1].getChunkInterval();

    assert(leftArray ->getArrayDesc().getDimensions()[1].getLength() ==
           rightArray->getArrayDesc().getDimensions()[0].getLength()); // a fundamental requirement of matrix arithmetic

    // GRR. if it were not for SpAccumulator needing OpAdd_t, we could have passed the traits to this routine
    //      as an enum, and it would not need to be templated on the semiring until just before the block spgemm<SemiringTraits_t>(...) call
    //      and the only reason this needs to be as high as it is, is to keep reusing the SPA_t's storage repeatedly
    Coordinate resultMinCol =     _schema.getDimensions()[1].getStartMin();
    Coordinate resultArrayWidth = _schema.getDimensions()[1].getLength();
    typedef SpAccumulator<Value_t, OpAdd_t> SPA_t; // an SPA efficiently accumulates (sparse row * sparse matrix).
    SPA_t sparseRowAccumulator(resultMinCol, resultArrayWidth);     // TODO ...we can go block-relative on this and reduce the size
                                                                    //         and adjust offset now that we flush each row to a single chunk

    // get positions of all left and right chunks
    vector<Coordinates> leftChunkPositions;
    getChunkPositions<CoordinatesComparatorRMO>(leftArray, leftChunkPositions);
    vector<Coordinates> rightChunkPositions;
    getChunkPositions<CoordinatesComparatorCMO>(rightArray, rightChunkPositions);

    // for every column of chunks in the right array.
    vector<Coordinates>::iterator itChunkPositionsRight = rightChunkPositions.begin();
    shared_ptr<ConstArrayIterator> arrayIterRight = rightArray->getConstIterator(0);

    Coordinate lastColMonotonic = std::numeric_limits<Coordinate>::min();
    while (itChunkPositionsRight != rightChunkPositions.end()) {
        double timeRightStart=getMonotonicrawSecs() ;

        // PART 1: load a column of right chunks into memory blocks (owned by rightBlockMap)
        RightBlockMap_t rightBlockMap;

        // for chunks in a single column
        Coordinate chunkCol = (*itChunkPositionsRight)[1]; // stay in this column
        assert(lastColMonotonic <= chunkCol);
        lastColMonotonic = chunkCol;                        // to support the above assertion

        while (true) {
            bool success = arrayIterRight->setPosition(*itChunkPositionsRight);
            SCIDB_ASSERT(success);

            // allocate the right kind and size of data structure for doing Spgemm (SpgemmBlock)
            // for a right-hand-side chunk, based on the pattern of non-zeros of the chunk
            // (e.g. nnz count, number of rows/cols occupied, etc).
            ConstChunk const& curChunk = arrayIterRight->getChunk();
            size_t nnzEstimate = curChunk.count();

            ssize_t chunkRows = curChunk.getLastPosition(false)[0] - curChunk.getFirstPosition(false)[0] + 1;
            ssize_t chunkCols = curChunk.getLastPosition(false)[1] - curChunk.getFirstPosition(false)[1] + 1;

            shared_ptr<RightBlock_t> rightBlock =
                SpgemmBlockFactory<SemiringTraits_tt>((*itChunkPositionsRight)[0], (*itChunkPositionsRight)[1],
                                                      chunkRows, chunkCols, nnzEstimate);

            // copy chunk to the SpgemmBlock
            copyChunkToBlock<SemiringTraits_tt, RightBlock_t>(curChunk, rightBlock, NULL, query);

            if (!rightBlock->empty()) {
                rightBlockMap.insert(std::pair<Coordinate, shared_ptr<RightBlock_t> >((*itChunkPositionsRight)[0], rightBlock));
            }
            // next chunk in list of sorted chunks, until the chunk column changes
            ++itChunkPositionsRight;
            if (itChunkPositionsRight == rightChunkPositions.end() || (*itChunkPositionsRight)[1] != chunkCol) {
                break;
            }
        }
        double copyRightSecs = getMonotonicrawSecs() - timeRightStart;
        times.loadRightSecs.back() += copyRightSecs;

        // PART 2: for each column of right chunks, above, go through every row of left chunks
        //         to multiply the left row of chunks by the colunn of right chunks

        // for every row of chunks in the left array.
        shared_ptr<ConstArrayIterator> leftArrayIt = leftArray->getConstIterator(0);
        vector<Coordinates>::iterator leftPosIt = leftChunkPositions.begin();
        while(leftPosIt != leftChunkPositions.end()) {
            double timeLeftStart=getMonotonicrawSecs() ;
            // part 2A: load a row of right chunks into memory blocks (owned by leftBlockList)
            //          while also finding the set of rows occupied by these blocks (leftRowsInUse)
            typedef pair<Coordinate, shared_ptr<LeftBlock_t> > ColBlockPair_t ;
            typedef std::vector<ColBlockPair_t> LeftBlockList_t;  // TODO: should this be made a list?
            typedef typename std::vector< ColBlockPair_t >::iterator LeftBlockListIt_t;
            LeftBlockList_t leftBlockList;
                                                                // TODO: the tree here is too expensive when it becomes ultra-sparse
            typedef std::set<Coordinate> LeftRowOrderedSet_t ;  // TODO: try making this std::map<pair<Coord, std::set<pair<Coord, shared_ptr<Block_t>> >
            LeftRowOrderedSet_t leftRowsInUse;                  //       and iteration will skip blocks not involved in the row, rather
                                                                //       than looking them up in the map and then checking.

            // for every chunk in the left row of chunks
            Coordinate chunkRow = (*leftPosIt)[0]; // stay in this row-of-chunks
            while (true) {
                // copy chunk to block
                shared_ptr<LeftBlock_t> leftBlock = make_shared<LeftBlock_t>((*leftPosIt)[0], (*leftPosIt)[1],
                                                                             leftChunkRowSize, leftChunkColSize, 0);
                bool success = leftArrayIt->setPosition(*leftPosIt);
                SCIDB_ASSERT(success);
                double timeLeftCopyStart=getMonotonicrawSecs() ;
                copyChunkToBlock<SemiringTraits_tt, LeftBlock_t>(leftArrayIt->getChunk(), leftBlock, &leftRowsInUse, query);
                double chunkCopySecs = getMonotonicrawSecs() - timeLeftCopyStart;
                times.loadLeftCopySecs.back() += chunkCopySecs;

                if (!leftBlock->empty()) {
                    leftBlockList.push_back(std::pair<Coordinate, shared_ptr<LeftBlock_t> >((*leftPosIt)[1], leftBlock));
                }
                // next chunk in list of sorted chunks, until the chunk column changes
                ++leftPosIt;
                if (leftPosIt == leftChunkPositions.end() || (*leftPosIt)[0] != chunkRow) {
                    break;
                }
            }

            double leftCopySecs = getMonotonicrawSecs() - timeLeftStart;
            times.loadLeftSecs.back() += leftCopySecs ;
            double timeBlockFindStart=getMonotonicrawSecs() ;

            // part 2B: for every row in the blocks in leftBlockList, multiply by the corresponding block in rightBlockMap
            //          while accumulating the resulting row in the SPA
            //
            Coordinates resultChunkPos(2);
            resultChunkPos[0] = chunkRow; resultChunkPos[1] = chunkCol ;

            // for every row used in the left row-of-chunks
            shared_ptr<ChunkIterator> currentResultChunk; // lazy creation by sparseRowAccumulator
            for(typename LeftRowOrderedSet_t::iterator rowIt=leftRowsInUse.begin(); rowIt != leftRowsInUse.end(); ++rowIt) {
                Coordinate leftRow = *(rowIt);
                // for each block along that row in the left row-of-chunks
                for(LeftBlockListIt_t leftBlocksIt=leftBlockList.begin(); leftBlocksIt != leftBlockList.end(); ++leftBlocksIt) {
                    Coordinate leftBlockCol = (*leftBlocksIt).first ;
                    // find the corresponding right chunk
                    typename RightBlockMap_t::iterator rightBlocksIt = rightBlockMap.find(leftBlockCol); // same rightBlockRow as leftBlockCol
                    if (rightBlocksIt != rightBlockMap.end()) { // if a matching rightBlock was found
                        LeftBlock_t&  leftBlock  = *(leftBlocksIt->second);
                        RightBlock_t& rightBlock = *(rightBlocksIt->second);
                        // leftBlock[leftRow,:] * rightBlock[:,:]
                        double timeBlockMultStart=getMonotonicrawSecs() ;
                        spGemm<SemiringTraits_tt>(leftRow, leftBlock, rightBlock, sparseRowAccumulator);
                        times.blockMultSecs.back() += (getMonotonicrawSecs() - timeBlockMultStart);
                    }
                } // end for each block along that row in the left row-of-chunks
                // the result row is totally accumulated in the SPA
                currentResultChunk = spAccumulatorFlushToChunk<IdAdd_t>(sparseRowAccumulator, leftRow,
                                                               resultArray, currentResultChunk, resultChunkPos,
                                                               _typeEnum, _type, query);
            } // end- for every row used in the left row of chunks
            double multSecs = getMonotonicrawSecs() - timeBlockFindStart;
            times.blockFindSecs.back() += (multSecs - times.blockMultSecs.back());

            if (currentResultChunk) {          // at least one of the rows in the output chunk had a non-zero
                double timeFlushStart = getMonotonicrawSecs();
                currentResultChunk->flush();
                double flushSecs = getMonotonicrawSecs() - timeFlushStart;
                times.flushSecs.back() += flushSecs;
            }
        } // end every row of chunks in left array
    } // end every column of chunks in right array

    // the time inside the block multiply was counted twice.
    // we correct it now by subtracting the blockMult time from the blockFind time
    times.blockFindSecs.back() -= times.blockMultSecs.back();
} // end method


template<class CoordinatesComparator_tt>
void PhysicalSpgemm::getChunkPositions(shared_ptr<Array>& array, vector<Coordinates>& result)
{
    shared_ptr<CoordinateSet> unsorted = array->findChunkPositions();
    result.reserve(unsorted->size()); // for O(n) insertion time
    result.insert(result.begin(), unsorted->begin(), unsorted->end());
    sort(result.begin(), result.end(), CoordinatesComparator_tt());
}

template<class SemiringTraits_tt, class Block_tt>
void PhysicalSpgemm::copyChunkToBlock(ConstChunk const& chunk,
                                      shared_ptr<Block_tt>& spBlock,
                                      std::set<Coordinate>* rowsInUseOptional,
                                      const shared_ptr<Query>& query)
{
    typedef typename SemiringTraits_tt::Value_t Value_t ;
    typedef typename SemiringTraits_tt::IdAdd_t IdAdd_t ;

    bool priorRowValid = false; // TODO: if there is a reserved value of Coordinate, we can eliminate this bool
    Coordinate priorRow = 0;    // initializing only to silence the compiler warning, that cannot actually happen

    shared_ptr<ConstChunkIterator> itChunk = chunk.getConstIterator();
    if( !dynamic_cast<RLETileConstChunkIterator*>(itChunk.get())) {
        // tile is not assured of actually having a tile ... switch to an iterator that
        // makes the tile API continue to function even in this case of "legacy" chunks
        itChunk = make_shared<
                     TileConstChunkIterator<
                        shared_ptr<ConstChunkIterator> > >(itChunk, query);
    }

    assert(itChunk->getLogicalPosition()>=0);

    // use about 1/2 of L1, the other half is for the destination
    const size_t MAX_VALUES_TO_GET = Sysinfo::INTEL_L1_DATA_CACHE_BYTES/2/sizeof(Value_t);

    // for all non-zeros in chunk:
    Coordinates coords(2);
    assert(itChunk->getLogicalPosition() >= 0);
    for (position_t offset = itChunk->getLogicalPosition(); offset >= 0; ) {
        boost::shared_ptr<BaseTile> tileData;
        boost::shared_ptr<BaseTile> tileCoords;
        offset = itChunk->getData(offset, MAX_VALUES_TO_GET,
                                  tileData, tileCoords);
        if (!tileData) {
            assert(!tileCoords);
            break;
        }

        assert(tileData);
        assert(tileCoords);
        assert(tileData->size() == tileCoords->size());
        assert(tileData->size() > 0);

        // XXX TODO: should provide Tile<>::iterators instead of dealing with the encodings etc.
        Tile<Coordinates, ArrayEncoding >* coordTileTyped =
           safe_dynamic_cast< Tile<Coordinates, ArrayEncoding >* >(tileCoords.get());

        assert(coordTileTyped);
        RLEEncoding<Value_t>* dataTyped = safe_dynamic_cast< RLEEncoding <Value_t>* >(tileData->getEncoding());
        typename RLEEncoding<Value_t>::const_iterator dataIter = dataTyped->begin();

        assert(coordTileTyped->size() == tileCoords->size());
        assert(dataTyped->size() == tileData->size());

        for (size_t i=0, n=coordTileTyped->size(); i < n; ++i, ++dataIter) {
            assert(dataIter != dataTyped->end());
            const Value_t val = (*dataIter);
            // because we know the additive ID is an annihilator for the ring
            // skip the processing of any factors equal to it:
            // this is the abstract definition of sparse multiplication over a semiring with annihilator
            // which is the mathematical structure that SemiringTraits defines
            if (val != IdAdd_t::value()) {
                coordTileTyped->at(i,coords);
                assert(coords.size()==2);
                spBlock->append(coords[0], coords[1], val);
                if(rowsInUseOptional) {
                    if(!priorRowValid || priorRow != coords[0]) {
                        priorRow = coords[0];
                        priorRowValid = true;
                        rowsInUseOptional->insert(priorRow);
                    }
                }
            }
        }
    }
}

double PhysicalSpgemm::getMonotonicrawSecs()
{
    struct timespec ts;
    if (clock_gettime(CLOCK_MONOTONIC, &ts) != 0) {
        assert(false);
        throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_CANT_GET_SYSTEM_TIME);
    }
    return double(ts.tv_sec + 1e-9 * ts.tv_nsec);
}

double PhysicalSpgemm::getThreadSecs()
{
    struct timespec ts;
    if (clock_gettime(CLOCK_THREAD_CPUTIME_ID, &ts) != 0) {
        assert(false);
        throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_CANT_GET_SYSTEM_TIME);
    }
    return double(ts.tv_sec + 1e-9 * ts.tv_nsec);
}


REGISTER_PHYSICAL_OPERATOR_FACTORY(PhysicalSpgemm, "spgemm", "PhysicalSpgemm");


} // end namespace scidb
