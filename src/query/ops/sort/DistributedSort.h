/*
**
* BEGIN_COPYRIGHT
*
* This file is part of SciDB.
* Copyright (C) 2014 SciDB, Inc.
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
 * Distributedsort.h
 *
 *  Created on: Nov 25, 2014
 *      Author: Donghui Zhang
 */

#ifndef DISTRIBUTEDSORT_H_
#define DISTRIBUTEDSORT_H_

#include <vector>

#include <query/Operator.h>
#include <array/Metadata.h>
#include <array/TupleArray.h>
#include <network/NetworkManager.h>
#include <system/Config.h>
#include <array/MergeSortArray.h>
#include <array/SortArray.h>
#include <util/arena/Map.h>
#include <util/arena/Set.h>
#include <util/Arena.h>
#include <util/PointerRange.h>
#include <util/Timing.h>
#include <util/SchemaUtils.h>

#include "ArchiveWrapper.h"

namespace scidb
{
/**
 * Collection of methods and data structures around the distributed-sort mechanism.
 */
class DistributedSort
{
public:
    /**
     * A Splitter consists of a record (a vector of Values), including the additional chunk/cell pos to ensure uniqueness.
     * To use Splitter as a key in containers, define the container with SplitterComparator.
     *
     */
    typedef Value* Splitter;

    /**
     * How to serialize a splitter into/from a boost::archive.
     * @param ar       a Boost::archive object.
     * @param splitter a splitter object.
     */
    template<class Archive>
    void serializeSplitter(Archive& a, Splitter& splitter);

    /**
     * SplitterAndCounts groups a Splitter object and its global count & local counts.
     * To use SplitterAndCounts as a key in containers is straightforward, as ::operator<() is defined.
     */
    struct SplitterAndCounts
    {
        /**
         * The total number of global records that are less than the splitter.
         */
        size_t _globalCount;

        /**
         * A splitter object.
         */
        Splitter _splitter;

        /**
         * A vector of size = _numInstances.
         * _localCounts[i] stores the number of records from instance i that are less than the splitter.
         * @note the sum of _localCounts[i] is equal to _globalCount.
         */
        vector<size_t> _localCounts;

        SplitterAndCounts():_globalCount(0), _splitter(NULL)
        {}
    };

    /**
     * @param query                  the query context.
     * @param sortedLocalData        the sorted local array.
     * @param expandedSchema         the schema same as sortedLocalData->getArrayDesc(), but with dimHigh = INT_MAX.
     * @param arena                  a memory arena to allocate memory from.
     * @param sortingAttributeInfos  a const reference to the passed SortingAttributeInfos will be stored.
     * @param timing                 a reference to an ElapsedMilliSeconds object, to print timing info to scidb.log.
     */
    DistributedSort(
            shared_ptr<Query> query,
            shared_ptr<MemArray> const& sortedLocalData,
            ArrayDesc const& expandedSchema,
            arena::ArenaPtr arena,
            SortingAttributeInfos const& sortingAttributeInfos,
            ElapsedMilliSeconds& timing);

    virtual ~DistributedSort()
    {
    }

    /**
     * This is the main sort routine.
     */
    boost::shared_ptr<MemArray> sort();

protected: // data members
    /**
     * Query context.
     */
    boost::shared_ptr<Query> _query;

    /**
     * Sorted local data.
     */
    boost::shared_ptr<MemArray> _sortedLocalData;

    /**
     * Some info about the sorted local data.
     */
    const SchemaUtils _schemaUtils;

    /**
     * The number of SciDB instances.
     */
    const size_t _numInstances;

    /**
     * My instance ID.
     */
    const InstanceID _myInstanceID;

    /**
     * Array iterators for sorted local array.
     * @note After the local sorted array is generated, all the _sortedLocalDataArrayIterators
     *       (for attributes in the sorting key) MUST remain valid and must point to the same chunkPos, until the sort completes.
     *       This protocol saves the time to check and assign array iterators.
     */
    std::vector<boost::shared_ptr<ConstArrayIterator> > _sortedLocalDataArrayIterators;

    /**
     * Chunk iterators for sorted local array.
     * @note Unlike the array iterators, the chunk iterators may or may not be valid.
     *       So if a member function needs to use chunk iterators, it needs to set them before using;
     *       the only exception is fillSplitterFromChunkIterators(Splitter&), which requires valid chunk iterators be pre-set.
     * @note For chunk iterators, the guarantee is: if for one of the sorting attributes the chunk iterator is valid, then
     *       the other chunk iterators (for other attributes in the sorting key) must be valid and must point to the same cellPos.
     */
    std::vector<boost::shared_ptr<ConstChunkIterator> > _sortedLocalDataChunkIterators;

    /**
     * The memory arena to be used to allocate memory from.
     * @note This MUST be a resetting arena. DistributedSort member functions do NOT give memory back to the arena.
     */
    arena::ArenaPtr _arena;

    /**
     * A reference to SortingAttributeInfos, which describes the attributeID and ASC/DESC for the sorting key.
     */
    SortingAttributeInfos const& _sortingAttributeInfos;

    /**
     * A TupleComparator object used to compare two Splitter objects.
     * MergeSortArray needs this version.
     */
    boost::shared_ptr<TupleComparator> _tupleComparator;

    /**
     * A TupleLessThan object used to compare two Splitter objects.
     * STL needs this version.
     */
    TupleLessThan _tupleLessThan;

    /**
     * A set of SplitterAndCounts objects.
     * @note The set is ordered in increasing order of _globalCount.
     * @note Selected iterators to set elements are stored in another data member _anchors.
     */
    typedef mgd::set<SplitterAndCounts> SetOfSplitterAndCounts;
    SetOfSplitterAndCounts _setOfSplitterAndCounts;

    /**
     * A vector of desired global counts.
     * @note The size of the vector is (_numInstances+1).
     *       _desiredCounts[k] = total # desired records in the first k instances.
     * @note The goal of the anchors-finding phase is to fill in _anchors, such that
     *       _anchors[k]->_globalCount = _desiredCounts[k].
     */
    vector<size_t> _desiredCounts;

    /**
     * A vector of splitters, from the first record in each local chunk.
     * The relationship between an index in the vector and the coordinate chunkPos[0] is as follows:
     * chunkPos[0] = index * chunkInterval + dimStart.
     */
    typedef mgd::vector<Splitter> FirstRecordInEachChunk;
    FirstRecordInEachChunk _firstRecordInEachChunk;

    /**
     * A sequence of _numInstances+1 splitters and their counts, as candidate splitters.
     * Perfect anchors are those s.t. _anchors[k]->_globalCount = _desiredCounts[k].
     */
    std::vector<SetOfSplitterAndCounts::const_iterator> _anchors;

    /**
     * local and global number of records, plus whether this instance knows their values (for debug purposes).
     */
    size_t _localNumRecords, _globalNumRecords;
    bool _doIKnowLocalNumRecords, _doIKnowGlobalNumRecords;

    /**
     * For printing out timing information in the log file.
     */
    ElapsedMilliSeconds& _timing;

protected: // Member functions that may be overloaded, to implement other versions of the distributed-sort framework.
    /**
     * @param error  an integer indicating how much does the current list of _anchors differ from _desiredCounts.
     * @return whether the error is tolerable.
     * @note By default, we use exact splitting, i.e. we tolerate zero error.
     */
    virtual bool errorTolerable(size_t error)
    {
        return error==0;
    }

    /**
     * Generate a list of IDs (from 1 to _numInstances-1), such that a new splitter should be generated for each ID.
     * @param[out] anchorIDs  place holder for the anchorIDs.
     * @note IDs 0 and _numInstances do NOT need to be picked, because they are known after determineGlobalMinMaxSplitterAndCounts() is called.
     * @note The function will wipe the previous content of anchorIDs.
     * @note Before calling this function, _desiredCounts and _anchors must have been filled already.
     * @note By default, all anchorIDs (where a perfect anchor has not been found yet) will be returned.
     */
    virtual void pickAnchorIDsToGenerateNewSplitters(vector<size_t>& anchorIDs) const;

    /**
     * An optional optimization is to remove the splitters from _setOfSplitterAndCounts,
     * that are useless in helping deciding anchors, in that they do not tightly bound any of _desiredCounts.
     *
     * @note This step is an optimization, with tradeoffs.
     *       The benefit of doing it is to speed up subsequent searches in _setOfSplitterAndCounts.
     *       The drawback is that it takes time to do the removal.
     * @note Theoretically, removing those useless splitters may save memory; but in our current implementation, where a resetting arena
     *       is used, the memory is not freed until the algorithm finishes.
     *
     * By default, the algorithm does NOT remove useless splitters.
     */
    virtual void removeUselessSplitters()
    {}

    /**
     * The last step, which only occurs in non-exact splitting, is to shuffle data around instance boundaries,
     * so that all chunks but the last one are completely full.
     *
     * @param arrayBeforeAdjusting  the redistributed array, where all records in instance i <= all records in instance j (if i<j).
     * @return the adjusted array, where all chunks but the last one are completely full.
     *
     * By default, we use exactly splitting, and this function is not called.
     */
    virtual shared_ptr<MemArray> redistributeToAdjustBoundaries(shared_ptr<MemArray> const& arrayBeforeAdjusting)
    {
        assert(false);
        return shared_ptr<MemArray>();
    }

private: // private member functions.
    /**
     * Read the values at _sortedLocalDataChunkIterators, into the passed splitter reference.
     * @param[inout] splitter  placeholder for the splitter to fill. Memory must have been pre-allocated.
     * @note All chunk iterators (for the sorting attributes) must be pointing to valid array cells.
     */
    void fillSplitterFromChunkIterators(Splitter & splitter) const;

    /**
     * Read the values at a give localIndex (0-based index in the local-sorted result), into the passed splitter reference.
     * @param[in]    localIndex the 0-based index in the local-sorted result.
     * @param[inout] splitter   place holder for the splitter to be filled. Memory must have been pre-allocated.
     * @note This function sets the chunk iterators to localIndex2Coords(localIndex).
     */
    void fillSplitterFromChunkIterators(size_t localIndex, Splitter & splitter);

    /**
     * Build the vector _firstRecordInEachChunk.
     * @note this function computes _localNumRecords.
     */
    void buildFirstRecordInEachChunk();

    /**
     * Determine global min/max splitterAndCounts.
     * @note This step exchanges information with other instances.
     * @param[out] globalMinSplitterAndCounts  place holder for the min.
     *                                         _globalCount = 0;
     *                                         _splitter = globalMinKey
     *                                         _localCounts = <0, 0, ..., 0>
     * @param[out] globalMaxSplitterAndCounts  place holder for the max.
     *                                         _globalCount = total number of records in all instances.
     *                                         _splitter = globalMaxKey with the 'chunk_pos' field increased by 1.
     *                                         _localCounts = {number of records in each instance}.
     * @note This function will allocate memory for the two splitters so the caller does not need to pre-allocate.
     * @note This function computes _globalNumRecords.
     */
    void determineGlobalMinMaxSplitterAndCounts(SplitterAndCounts& globalMinSplitterAndCounts, SplitterAndCounts& globalMaxSplitterAndCounts);

    /**
     * Fill in _desiredCounts.
     * @note _desiredCounts depends only on (a) _globalNumRecords, (b) _numInstances, and (c) chunkInterval.
     */
    void fillDesiredCounts();

    /**
     * From the current _setOfSplitterAndCounts, fill _anchors[i] using the best match for every _desiredCounts[i].
     * @note Before calling this function, _anchors[0] and _anchors[_numInstances] must have been set already.
     * @return linear sum of the differences between each _desiredCounts[i] and _anchors[i]._globalCount.
     */
    size_t pickBestAnchorCandidates();

    /**
     * Given a splitter, tell how many local records are smaller than it.
     * @param splitter  the reference to a splitter objects.
     * @return how many local records are smaller than the splitter.
     */
    size_t lookupLocalCount(Splitter const&);

    /**
     * @return the total number of records in the local instance.
     * @note Before calling this function, _doIKnowLocalNumRecords must have been set true.
     */
    size_t getLocalNumRecords() const
    {
        assert(_doIKnowLocalNumRecords);
        return _localNumRecords;
    }

    /**
     * @return the total number of records across all instances.
     * @note Before calling this function, _doIKnowGlobalNumRecords must have been set true.
     */
    size_t getGlobalNumRecords() const
    {
        assert(_doIKnowGlobalNumRecords);
        return _globalNumRecords;
    }

    /**
     * Distribute the local-sorted array based on _anchors.
     * @return the distributed array.
     */
    shared_ptr<MemArray> distributeBasedOnAnchors();

    /**
     * @return an arbitrary attributeID that is part of the sorting key.
     * @note The purpose of getting such an attribute is to tell whether an array iterator or chunk iterator is valid.
     * @see _sortedLocalDataArrayIterators and  _sortedLocalDataChunkIterators.
     */
    AttributeID oneSortingAttribute() const
    {
        assert(! _sortingAttributeInfos.empty());
        const AttributeID attrID = _sortingAttributeInfos.begin()->columnNo;
        assert( attrID < _sortedLocalDataChunkIterators.size());

        return attrID;
    }

    /**
     * Allocate space for a splitter object.
     * @param[out] splitter  placeholder for a splitter object.
     * @note
     */
    void allocateSplitter(Splitter& splitter)
    {
        splitter = arena::newVector<Value>(*_arena, _schemaUtils._nAttrsWithoutET);
    }

    /**
     * @param a 0-based local index of a record.
     * @return the cellPos of the record in the sorted local data.
     */
    Coordinates localIndex2Coords(size_t localIndex)
    {
        Coordinates coords(1);
        coords[0] = static_cast<Coordinate>(localIndex) + _schemaUtils._dims[0].getStartMin();
        return coords;
    }

    /**
     * @param a 1D Coordinates in the sorted local data.
     * @return a 0-based local index for that cell.
     */
    size_t coords2LocalIndex(CoordinateCRange coords)
    {
        Coordinate localIndex = coords[0] - _schemaUtils._dims[0].getStartMin();
        assert(localIndex >= 0);
        return static_cast<size_t>(localIndex);
    }
};

/**
 * Comparing two SplitterAndCounts objects.
 */
inline bool operator<( DistributedSort::SplitterAndCounts const& sc1, DistributedSort::SplitterAndCounts const& sc2)
{
    return sc1._globalCount < sc2._globalCount;
}

template<class Archive>
void DistributedSort::serializeSplitter(Archive& ar, DistributedSort::Splitter& splitter)
{
    if (Archive::is_loading::value) {
        allocateSplitter(splitter);
    }
    for (SortingAttributeInfos::const_iterator it = _sortingAttributeInfos.begin(); it != _sortingAttributeInfos.end(); ++it) {
        const AttributeID attrID = it->columnNo;
        ar & splitter[attrID];
    }
}

} // namespace

#endif /* DISTRIBUTEDSORT_H_ */
