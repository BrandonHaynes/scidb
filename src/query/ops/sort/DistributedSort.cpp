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
 * DistributedSort.cpp
 *
 *  Created on: Nov 25, 2014
 *      Author: Donghui Zhang
 */

#include <array/ParallelAccumulatorArray.h>

#include "DistributedSort.h"
#include "ArrayBreaker.h"

using namespace std;
using namespace boost;

namespace scidb
{

static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.query.ops.sort"));

DistributedSort::DistributedSort(
        shared_ptr<Query> query,
        shared_ptr<MemArray> const& sortedLocalData,
        ArrayDesc const& expandedSchema,
        arena::ArenaPtr parentArena,
        SortingAttributeInfos const& sortingAttributeInfos,
        ElapsedMilliSeconds& timing)
: _query(query),
  _sortedLocalData(sortedLocalData),
  _schemaUtils(expandedSchema),
  _numInstances(_query->getInstancesCount()),
  _myInstanceID(_query->getInstanceID()),
  _sortedLocalDataArrayIterators(_schemaUtils._nAttrsWithoutET),
  _sortedLocalDataChunkIterators(_schemaUtils._nAttrsWithoutET),
  _arena(arena::newArena(arena::Options("DistributedSort").parent(parentArena).resetting(true).recycling(false).pagesize(64*MiB))),
  _sortingAttributeInfos(sortingAttributeInfos),
  _tupleComparator(new TupleComparator(sortingAttributeInfos, _schemaUtils._schema)),
  _tupleLessThan(&*_tupleComparator),
  _setOfSplitterAndCounts(_arena),
  _desiredCounts(_numInstances+1),
  _firstRecordInEachChunk(_arena),
  _anchors(_numInstances+1),
  _localNumRecords(0),
  _globalNumRecords(0),
  _doIKnowLocalNumRecords(false),
  _doIKnowGlobalNumRecords(false),
  _timing(timing)
{
    // Open array iterators.
    for (SortingAttributeInfos::const_iterator it = _sortingAttributeInfos.begin(); it != _sortingAttributeInfos.end(); ++it) {
        const AttributeID attrID = it->columnNo;
        _sortedLocalDataArrayIterators[attrID] = sortedLocalData->getConstIterator(attrID);
    }

    // Initialize anchors to non-existing iterators.
    for (size_t i=0; i<=_numInstances; ++i) {
        _anchors[i] = _setOfSplitterAndCounts.end();
    }
}

void DistributedSort::fillSplitterFromChunkIterators(Splitter & splitter) const
{
    assert(splitter);  // memory must have been allocated before.
    for (SortingAttributeInfos::const_iterator it = _sortingAttributeInfos.begin(); it != _sortingAttributeInfos.end(); ++it) {
        const AttributeID attrID = it->columnNo;
        assert(_sortedLocalDataChunkIterators[attrID] && !_sortedLocalDataChunkIterators[attrID]->end());
        splitter[attrID] = _sortedLocalDataChunkIterators[attrID]->getItem();
    }
}

void DistributedSort::fillSplitterFromChunkIterators(size_t localIndex, Splitter & splitter)
{
    assert(localIndex < getLocalNumRecords());

    // What are the cellPos and chunkPos where the splitter should be loaded from?
    Coordinates cellPos = localIndex2Coords(localIndex);
    Coordinates chunkPos = cellPos;
    _schemaUtils._schema.getChunkPositionFor(chunkPos);

    // Prepare the chunk iterators, if needed to.
    const AttributeID oneAttr = oneSortingAttribute();
    shared_ptr<ConstChunkIterator>& oneChunkIter = _sortedLocalDataChunkIterators[oneAttr];
    shared_ptr<ConstArrayIterator>& oneArrayIter = _sortedLocalDataArrayIterators[oneAttr];
    SCIDB_ASSERT(oneArrayIter && !oneArrayIter->end());

    // If the chunk iterators are not at the right cell...
    if (!oneChunkIter || oneChunkIter->end() || localIndex != coords2LocalIndex(oneChunkIter->getPosition())) {
        // If they are not even at the right chunk...
        if (!oneChunkIter || oneChunkIter->end() ||
                !_schemaUtils._schema.isCellPosInChunk(oneChunkIter->getPosition(), chunkPos)) {
            for (SortingAttributeInfos::const_iterator it = _sortingAttributeInfos.begin(); it != _sortingAttributeInfos.end(); ++it) {
                const AttributeID attrID = it->columnNo;
                assert(_sortedLocalDataArrayIterators[attrID]);
                const bool mustSucceed = _sortedLocalDataArrayIterators[attrID]->setPosition(chunkPos);
                SCIDB_ASSERT(mustSucceed);
                _sortedLocalDataChunkIterators[attrID] = _sortedLocalDataArrayIterators[attrID]->getChunk().getConstIterator();
            }
        }

        // Set chunk positions.
        for (SortingAttributeInfos::const_iterator it = _sortingAttributeInfos.begin(); it != _sortingAttributeInfos.end(); ++it) {
            const AttributeID attrID = it->columnNo;
            assert(_sortedLocalDataChunkIterators[attrID] && !_sortedLocalDataChunkIterators[attrID]->end());
            const bool mustSucceed = _sortedLocalDataChunkIterators[attrID]->setPosition(cellPos);
            SCIDB_ASSERT(mustSucceed);
        }
    }

    // Fill from the chunk iterators.
    fillSplitterFromChunkIterators(splitter);
}

void DistributedSort::buildFirstRecordInEachChunk()
{
    _firstRecordInEachChunk.clear();

    // Reset the array iterators, so that they point to the first chunk.
    for (SortingAttributeInfos::const_iterator it = _sortingAttributeInfos.begin(); it != _sortingAttributeInfos.end(); ++it) {
        const AttributeID attrID = it->columnNo;
        _sortedLocalDataArrayIterators[attrID]->reset();
    }

    // Create a splitter from the first record of every chunk.
    const AttributeID oneAttr = oneSortingAttribute();
    while (! _sortedLocalDataArrayIterators[oneAttr]->end()) {
        // Get the chunk iterators.
        for (SortingAttributeInfos::const_iterator it = _sortingAttributeInfos.begin(); it != _sortingAttributeInfos.end(); ++it) {
            const AttributeID attrID = it->columnNo;
            _sortedLocalDataChunkIterators[attrID] = _sortedLocalDataArrayIterators[attrID]->getChunk().getConstIterator();
        }

        // Fill a new splitter, from the first record in the chunk iterators.
        Splitter splitter;
        allocateSplitter(splitter);
        fillSplitterFromChunkIterators(splitter);
        _firstRecordInEachChunk.push_back(splitter);

        // Advance to the next chunk.
        for (SortingAttributeInfos::const_iterator it = _sortingAttributeInfos.begin(); it != _sortingAttributeInfos.end(); ++it) {
            const AttributeID attrID = it->columnNo;
            ++(*_sortedLocalDataArrayIterators[attrID]);
        }
    }

    // Set the total number of records.
    _localNumRecords = 0;
    if (! _firstRecordInEachChunk.empty()) {
        size_t numRecordsInAllButLastChunk = (_firstRecordInEachChunk.size()-1) * _schemaUtils._dims[0].getChunkInterval();

        Coordinates lastChunkPos = localIndex2Coords(numRecordsInAllButLastChunk);
        for (SortingAttributeInfos::const_iterator it = _sortingAttributeInfos.begin(); it != _sortingAttributeInfos.end(); ++it) {
            const AttributeID attrID = it->columnNo;
            _sortedLocalDataArrayIterators[attrID]->setPosition(lastChunkPos);
        }
        size_t numRecordsInLastChunk = _sortedLocalDataArrayIterators[oneAttr]->getChunk().count();

        _localNumRecords = numRecordsInAllButLastChunk + numRecordsInLastChunk;
    }
    _doIKnowLocalNumRecords = true;
}

void DistributedSort::determineGlobalMinMaxSplitterAndCounts(SplitterAndCounts& minSplitterAndCounts, SplitterAndCounts& maxSplitterAndCounts)
{
    assert(!minSplitterAndCounts._splitter && !maxSplitterAndCounts._splitter);

    // Some common variables.
    IArchiveWrapper iArchiveWrapper;  // To receive from other instances.
    OArchiveWrapper oArchiveWrapper;  // To send to other instances.

    // Define the min-max SplitterAndCounts objects.
    // Right now the counts are all zero and the splitters are not allocated yet.
    minSplitterAndCounts._globalCount = 0;
    minSplitterAndCounts._localCounts.resize(_numInstances);
    for (size_t i=0; i<_numInstances; ++i) {
        minSplitterAndCounts._localCounts[i] = 0;
    }

    maxSplitterAndCounts._globalCount = 0;
    maxSplitterAndCounts._localCounts.resize(_numInstances);
    for (size_t i=0; i<_numInstances; ++i) {
        maxSplitterAndCounts._localCounts[i] = 0;
    }

    // If there is at least one local record, fill:
    //   - minSplitterAndCounts._splitter: from the min record.
    //   - maxSplitterAndCounts._splitter: from the max record, but increase the cell_pos by 1 to make it strictly larger than all records.
    //   - maxSplitterAndCounts._localCounts[_myInstanceID]: the number of local records.
    //   - maxSplitterAndCounts._globalCount: the number of local records.
    // At the same time, record local count.
    if (! _firstRecordInEachChunk.empty()) {
        // min splitter; note that its counts (all zeros) are already valid, if only local records are counted.
        minSplitterAndCounts._splitter = *_firstRecordInEachChunk.begin();

        // max splitter and counts.
        allocateSplitter(maxSplitterAndCounts._splitter);
        assert(_schemaUtils._dims[0].getCurrEnd() >= _schemaUtils._dims[0].getCurrStart()); // at least one record.
        maxSplitterAndCounts._globalCount = getLocalNumRecords();
        maxSplitterAndCounts._localCounts[_myInstanceID] = maxSplitterAndCounts._globalCount;
        fillSplitterFromChunkIterators(maxSplitterAndCounts._globalCount-1, maxSplitterAndCounts._splitter);

        // Increase the chunk_pos in the max splitter by 1, to make sure the splitter is larger than all local records.
        Value& value = maxSplitterAndCounts._splitter[_schemaUtils._attrsWithoutET.size()-1];
        value.set<int64_t>(value.get<int64_t>()+1);
    }

    // Broadcast to other instances:
    //   <localCount, minLocalSplitter, maxLocalSplitter>, if localCount > 0;
    //   or
    //   <0>, if localCount = 0.
    boost::archive::binary_oarchive* oArchive = oArchiveWrapper.reset();
    (*oArchive) & maxSplitterAndCounts._globalCount;
    if (maxSplitterAndCounts._globalCount > 0) {
        serializeSplitter(*oArchive, minSplitterAndCounts._splitter);
        serializeSplitter(*oArchive, maxSplitterAndCounts._splitter);
    }
    const bool copyToSharedBuffer = true;
    BufBroadcast(oArchiveWrapper.getSharedBuffer(copyToSharedBuffer), _query);

    // Receive from other instances, and merge into minSplitterAndCounts and maxSplitterAndCounts.
    for (InstanceID i=0; i<_numInstances; ++i) {
        if (i == _myInstanceID) {
            continue;
        }

        // Receive; update counts.
        shared_ptr<SharedBuffer> buffer = BufReceive(i, _query);
        archive::binary_iarchive* iArchive = iArchiveWrapper.reset(buffer);
        size_t localCount = 0;
        (*iArchive) & localCount;
        maxSplitterAndCounts._globalCount += localCount;
        maxSplitterAndCounts._localCounts[i] = localCount;

        // Update splitters.
        if (localCount > 0) {
            Splitter minSplitter;
            serializeSplitter(*iArchive, minSplitter);
            if (!minSplitterAndCounts._splitter || _tupleComparator->compare(minSplitter, minSplitterAndCounts._splitter) < 0) {
                // We do not worry about freeing memory used by the original min splitter, because we are using a resetting arena;
                // i.e. all memory will be deallocated in one shot, when _arena goes out of scope.
                minSplitterAndCounts._splitter = minSplitter;
            }

            Splitter maxSplitter;
            serializeSplitter(*iArchive, maxSplitter);
            if (!maxSplitterAndCounts._splitter || _tupleComparator->compare(maxSplitter, maxSplitterAndCounts._splitter) > 0) {
                maxSplitterAndCounts._splitter = maxSplitter;
            }
        }
    }

    _globalNumRecords = maxSplitterAndCounts._globalCount;
    _doIKnowGlobalNumRecords = true;
}

size_t DistributedSort::pickBestAnchorCandidates()
{
    assert(_anchors.size() == _numInstances+1);
    assert(! _setOfSplitterAndCounts.empty());
    assert(_anchors[0]->_globalCount == 0);
    assert(_anchors[_numInstances]->_globalCount > 0);
    assert(_anchors[0] == _setOfSplitterAndCounts.begin());
    assert(_anchors[_numInstances] == --_setOfSplitterAndCounts.end());

    size_t totalError = 0;  // The linear sum of the difference between _anchors[i]._globalCount and _desiredCount[i].
    SplitterAndCounts dummySplitterAndCounts; // Used for binary search in _setOfSplitterAndCounts.

    for (size_t i=1; i<_numInstances; ++i) {
        // Search for the smallest element in _setOfSplitterAndCounts, that is no less than _desiredCounts[i].
        dummySplitterAndCounts._globalCount = _desiredCounts[i];
        SetOfSplitterAndCounts::const_iterator itHigh = _setOfSplitterAndCounts.lower_bound(dummySplitterAndCounts);
        assert(itHigh != _setOfSplitterAndCounts.end());
        assert(itHigh->_globalCount >= _desiredCounts[i]);

        // If a perfect match is found, use it as _anchors[i].
        size_t errorHigh = itHigh->_globalCount - _desiredCounts[i];
        if (errorHigh==0) {
            _anchors[i] = itHigh;
            continue;
        }

        // If itHigh is not a perfect match, find itLow (the largest element smaller than _desiredCounts[i], and
        // pick the anchor from itLow and itHigh, whichever has a _globalCount closer to _desiredCounts[i].
        assert(itHigh != _setOfSplitterAndCounts.begin());
        SetOfSplitterAndCounts::const_iterator itLow = itHigh;
        --itLow;
        assert(itLow->_globalCount < _desiredCounts[i]);
        size_t errorLow = _desiredCounts[i] - itLow->_globalCount;
        if (errorHigh <= errorLow) {
            _anchors[i] = itHigh;
            totalError += errorHigh;
        }
        else {
            _anchors[i] = itLow;
            totalError += errorLow;
        }
    }
    return totalError;
}

void DistributedSort::fillDesiredCounts()
{
    _desiredCounts[0] = 0;
    size_t globalNumRecords = getGlobalNumRecords();
    size_t chunkInterval = _schemaUtils._dims[0].getChunkInterval();
    assert(chunkInterval>0);
    size_t numChunks = (globalNumRecords + chunkInterval - 1) / chunkInterval;
    size_t numChunksPerInstance = (numChunks + _numInstances - 1) / _numInstances;
    size_t numRecordsPerInstance = numChunksPerInstance * chunkInterval;

    size_t remainingNumRecords = globalNumRecords;
    for (size_t i=1; i<=_numInstances; ++i) {
        if (remainingNumRecords >= numRecordsPerInstance) {
            remainingNumRecords -= numRecordsPerInstance;
        }
        else {
            remainingNumRecords = 0;
        }
        _desiredCounts[i] = globalNumRecords - remainingNumRecords;
    }
    assert(_desiredCounts[_numInstances] == globalNumRecords);
}

void DistributedSort::pickAnchorIDsToGenerateNewSplitters(vector<size_t>& anchorIDs) const
{
    anchorIDs.clear();
    for (size_t i=1; i<_numInstances; ++i) {
        assert(_anchors[i] != _setOfSplitterAndCounts.end());
        if (_desiredCounts[i] > _desiredCounts[i-1] && _desiredCounts[i] != _anchors[i]->_globalCount) {
            anchorIDs.push_back(i);
        }
    }
}

size_t DistributedSort::lookupLocalCount(Splitter const& splitter)
{
    // Special case: there is no local record.
    size_t localNumRecords = getLocalNumRecords();
    if (localNumRecords==0) {
        return 0;
    }

    // Find the "next chunk", i.e. the first chunk whose first record >= the splitter.
    // That chunk and later chunks do NOT need to be searched.
    // The reason is that these chunks *only* contain records >= the splitter,
    // while this function asks for the number of records < the splitter.
    FirstRecordInEachChunk::const_iterator itNext = lower_bound(
            _firstRecordInEachChunk.begin(),
            _firstRecordInEachChunk.end(),
            splitter,
            _tupleLessThan);

    // The case when no local records < the splitter.
    if (itNext == _firstRecordInEachChunk.begin()) {
        return 0;
    }

    // Below we will do a binary search in the "current chunk", i.e. the chunk immediately before the "next chunk".
    // Note that the "previous chunks" do NOT need to be searched.
    // The reason is that all of them are filled with records < the splitter,
    // and the number of records in them can simply be calculated as chunkInterval * #previousChunks.
    //
    // We'll start with getting the first/last localIndex of the records in the chunk.
    const size_t chunkInterval = _schemaUtils._dims[0].getChunkInterval();
    assert(chunkInterval>0);
    size_t lowLocalIndex = chunkInterval * (itNext - _firstRecordInEachChunk.begin() - 1);
    size_t highLocalIndex = lowLocalIndex + chunkInterval - 1;
    if (highLocalIndex >= localNumRecords) {
        highLocalIndex = localNumRecords - 1;
    }
    assert(lowLocalIndex <= highLocalIndex && highLocalIndex < localNumRecords);

    // At this point, we know the record at lowLocalIndex < the splitter,
    // but we have no idea how the record at highLocalIndex compares with the splitter.
    // So let's find out.
    Splitter tmpSplitter;
    allocateSplitter(tmpSplitter);
    fillSplitterFromChunkIterators(highLocalIndex, tmpSplitter);
    int comparison = _tupleComparator->compare(tmpSplitter, splitter);
    if (comparison < 0) { // All records in the chunk should be counted.
        return highLocalIndex+1;
    }
    else if (comparison == 0) { // All records in the chunk, except the one at highLocalIndex, are less than the splitter.
        return highLocalIndex;
    }

    // Ok, record@lowLocalIndex < splitter < record@highLocalIndex.
    // Let's do a binary search.
    while (lowLocalIndex+1 < highLocalIndex) {
        size_t midLocalIndex = (lowLocalIndex+highLocalIndex)/2;
        assert(lowLocalIndex<midLocalIndex && midLocalIndex<highLocalIndex);
        fillSplitterFromChunkIterators(midLocalIndex, tmpSplitter);
        int comparison = _tupleComparator->compare(tmpSplitter, splitter);
        if (comparison < 0) {
            lowLocalIndex = midLocalIndex;
        }
        else if (comparison == 0) {
            // All records before midLocalIndex < splitter.
            return midLocalIndex;
        }
        else {
            highLocalIndex = midLocalIndex;
        }
    }

    // record@lowLocalIndex < splitter, but later records are not.
    assert(lowLocalIndex+1 == highLocalIndex);
    return highLocalIndex;
}

/**
 * A function of type BreakerOnCoordinates, that is used when DistributedSort calls breakOneArrayIntoMultiple().
 * @see ArrayBreaker.h for descriptions on the parameters and the return value.
 * @param additionalInfo     a pointer to a vector of numInstances+1 desired local counts. That is,
 *                           (*additionaInfo)[k] = total # desired local records in the first k instances.
 * @see _desiredCounts, which is a vector of desired GLOBAL counts.
 * @note the return value is equal to smallest index i in the additionalInfo vector, s.t.
 *       (*additionalInfo)[i] <= coords2LocalIndex(cellPos), and (*additionalInfo)[i+1] > coords2LocalIndex(cellPos).
 *       E.g. if the vector is {0, 100, 200}, a localIndex=199 should be sent to instance 1, because 100<=199 and 200>199.
 */
size_t breakerOnOneDimCoordinatesAndDividers(
        Coordinates const& cellPos,
        size_t previousResult,
        shared_ptr<Query>& query,
        Dimensions const& dims,
        void* additionalInfo)
{
    assert(cellPos.size() == 1);
    assert(dims.size()==1);
    assert(additionalInfo);

    vector<size_t> const& dividers = *(reinterpret_cast<vector<size_t> const*>(additionalInfo));
    assert(dividers.size() == query->getInstancesCount()+1);
    assert(dividers[0] == 0);
    assert(dividers[query->getInstancesCount()] > 0);

    size_t localIndex = cellPos[0] - dims[0].getStartMin(); // = DistributedSort::coords2LocalIndex(cellPos);
    const size_t localNumRecords = *dividers.rbegin();

    assert(localNumRecords > 0);

    if (localIndex >= localNumRecords) {
        // If the index is out of bound, we will return the last instance for which at least one record will be sent to.
        // E.g. Suppose there are 97 records all of which needs to be sent to instance 0 (out of 3 instances),
        // We know additionalInfo should be {0, 97, 97, 97}.
        // The function should return instance 0.
        for (vector<size_t>::const_iterator it = dividers.begin(); it!=dividers.end(); ++it) {
            if (*it == localNumRecords) {
                return it - dividers.begin() - 1;
            }
        }
        assert(false);
    }

    if (previousResult+1 < dividers.size()) {
        if (dividers[previousResult] <= localIndex && localIndex < dividers[previousResult+1]) {
            return previousResult;
        }
    }

    for (vector<size_t>::const_iterator it = dividers.begin(); it!=dividers.end(); ++it) {
        if (localIndex < *it) {
            return it - dividers.begin() - 1;
        }
    }

    assert(false);
    return 0;
}


shared_ptr<MemArray> DistributedSort::distributeBasedOnAnchors()
{
    // Break the array into _numInstances outbound arrays.
    vector<shared_ptr<Array> > outboundArrays(_numInstances);
    for (size_t i=0; i<_numInstances; ++i) {
        outboundArrays[i] = make_shared<MemArray>(_schemaUtils._schema, _query);
    }
    vector<size_t> anchorLocalCounts(_numInstances+1);
    for (size_t i=0; i<=_numInstances; ++i) {
        anchorLocalCounts[i] = _anchors[i]->_localCounts[_myInstanceID];
    }
    if (getLocalNumRecords() > 0) {
        const bool isBreakerConsecutive = true;
        breakOneArrayIntoMultiple(
                _sortedLocalData,
                outboundArrays,
                _query,
                breakerOnOneDimCoordinatesAndDividers,
                isBreakerConsecutive,
                reinterpret_cast<void*>(&anchorLocalCounts)
                );
    }

    // Prepare inbound arrays, to receive pieces from other instances.
    // Also, move outboundArrays[_myInstanceID] to inboundArrays[_myInstanceID].
    shared_ptr<RemoteArrayContext> remoteArrayContext = make_shared<RemoteArrayContext>(_numInstances);
    vector<shared_ptr<Array> > inboundArrays(_numInstances);
    for (InstanceID i = 0; i < _numInstances; i++)
    {
        if (i != _myInstanceID) {
            inboundArrays[i] = RemoteArray::create(remoteArrayContext, _schemaUtils._schema, _query->getQueryID(), i);
        } else {
            inboundArrays[i] = outboundArrays[i];
            outboundArrays[i].reset();
        }
    }

    // Store the outbound arrays in the remoteArrayContext, to service pull requests from other instances.
    for (InstanceID i = 0; i < _numInstances; i++) {
        if (i == _myInstanceID) {
            continue;
        }
        if (Config::getInstance()->getOption<int>(CONFIG_RESULT_PREFETCH_QUEUE_SIZE) > 1
                && outboundArrays[i]->getSupportedAccess() == Array::RANDOM) {
            boost::shared_ptr<ParallelAccumulatorArray> paa = boost::make_shared<ParallelAccumulatorArray>(outboundArrays[i]);
            outboundArrays[i] = paa;
            paa->start(_query);
        } else {
            outboundArrays[i] = make_shared<AccumulatorArray>(outboundArrays[i], _query);
        }
        remoteArrayContext->setOutboundArray(i, outboundArrays[i]);
    }

    // Record the remoteArrayContext in the Query context.
    syncBarrier(0, _query);
    ASSERT_EXCEPTION(!_query->getOperatorContext(), "In DistributedSort, operator context is supposed to be empty.");
    _query->setOperatorContext(remoteArrayContext);

    // Set up a MemArray of MergeSortArray over the inboundArrays, to pull from the other instances and merge.
    shared_ptr<vector<size_t> > streamSizes = shared_ptr<vector<size_t> >(new vector<size_t>(_numInstances));
    for (size_t i=0; i<_numInstances; ++i) {
        // Here is how to decide how many records will be sent from instance i to me (_myInstanceID).
        // _anchors[_myInstanceID+1]->_localCounts[i] includes the number of records instance i will send to me or instances with a smaller ID.
        // All I need to do is to subtract from it _anchors[_myInstanceID]->_localCount[i].
        (*streamSizes)[i] = _anchors[_myInstanceID+1]->_localCounts[i] - _anchors[_myInstanceID]->_localCounts[i];
    }
    shared_ptr<Array> mergeSortResult = make_shared<MergeSortArray>(
            _query, _schemaUtils._schema, inboundArrays, _tupleComparator,
            // The parameter below is the offset to be added to the coordinate of every cell.
            // Let's use an example to illustrate why what's chosen is correct:
            // Suppose I'm instance 2, and _anchors[2]->_globalCount = 2000.
            // I know there will be 2000 records in instances 0 or 1.
            // So the first record I'm about to generate should be at offset 2000.
            _anchors[_myInstanceID]->_globalCount,
            streamSizes
            );
    const bool isVertical = false; // the MemArray cannot scan data vertically, because the MergeSortArray is streaming.
    shared_ptr<MemArray> resultArray = shared_ptr<MemArray>(new MemArray(mergeSortResult, _query, isVertical));
    syncSG(_query);

    // Unset the operator context.
    syncBarrier(1, _query);
    _query->unsetOperatorContext();

    // Return the merge-sort result of the inbound arrays.
    return resultArray;
}

shared_ptr<MemArray> DistributedSort::sort()
{
    // Some common variables.
    assert(_myInstanceID < _numInstances);
    IArchiveWrapper iArchiveWrapper;  // Received from other instances.
    OArchiveWrapper oArchiveWrapper;  // Send to other instances.
    SplitterAndCounts dummySplitterAndCounts;  // A splitterAndCount object with only _globalCount filled, for searching purposes.

    // From the first record of every local chunk, fill _firstRecordInEachChunk.
    buildFirstRecordInEachChunk();
    _timing.logTiming(logger, "[sort] Getting first record of every local chunk");

    // Cooperate with other instances to compute global minSplitterAndCounts and maxSplitterAndCounts.
    SplitterAndCounts minSplitterAndCounts, maxSplitterAndCounts;
    determineGlobalMinMaxSplitterAndCounts(minSplitterAndCounts, maxSplitterAndCounts);
    _timing.logTiming(logger, "[sort] Determining global min/max splitters");

    // A short cut: if there is no record, just return an empty array.
    if (maxSplitterAndCounts._globalCount == 0) {
        return make_shared<MemArray>(_schemaUtils._schema, _query);
    }

    // Insert the min/max-SplitterAndCounts to _setOfSplitterAndCounts, and add references to _anchors[0] and _anchors[_numInstances].
    _setOfSplitterAndCounts.insert(minSplitterAndCounts);
    _setOfSplitterAndCounts.insert(maxSplitterAndCounts);
    _anchors[0] = _setOfSplitterAndCounts.begin();
    _anchors[_numInstances] = --_setOfSplitterAndCounts.end();

    // Fill _desiredCounts.
    fillDesiredCounts();

    // Iteratively refine _anchors, until the error (from _desiredCounts) is tolerable.
    size_t totalError = 0;
    size_t numIterations = 0;

    while (true) {
        Query::validateQueryPtr(_query);

        // Get the current best anchors.
        totalError = pickBestAnchorCandidates();
        LOG4CXX_DEBUG(logger, "[sort] picked splitters (iteration " << numIterations++ << "), remaining error = " << totalError);

        // If the error is tolerable, break from the while loop.
        if (errorTolerable(totalError)) {
            break;
        }

        // Get a list of anchorIDs near which candidate splitters should be generated.
        vector<size_t> anchorIDsToGenerateNewSplitters;
        pickAnchorIDsToGenerateNewSplitters(anchorIDsToGenerateNewSplitters);
        assert(! anchorIDsToGenerateNewSplitters.empty());

        // Define a 2D-array structure, called allCandidates, to store information about the candidate splitters.
        // Each row corresponds to the (multiple) candidates generated by one particular instance.
        // The algorithm will proceed by filling the rows corresponding to local candidates.
        // Further, for each such candidate splitter, only the local count will be filled.
        // In the remaining steps of the while loop, the full 2D array will be filled.
        //
        // We'll use a running example throughout the while loop, to illustrate the algorithm.
        // We will monitor the content of allCandidates on instance 1.
        // (At the end of the running example, all instances will arrive at the SAME content of allCandidates.)
        //
        // [running example:]
        //    * Assume there are three instances: 0, 1, 2.
        //    * We know before entering the while loop, _setSplitterAndCounts contains two entries with
        //      _globalCount = 0 and globalNumRecords, respectively.
        //    * The job is to determine two splitters that will have globalCount = desiredCounts[1] and desiredCounts[2].
        //    * Initially, the content of allCandidates on instance 1 will be:
        //      - allCandidates[0] = [ ]
        //      - allCandidates[1] = [ ]
        //      - allCandidates[2] = [ ]
        //
        vector<vector<SplitterAndCounts> > allCandidates(_numInstances);

        // This step fills the counts of the local candidates, using only local data.
        // (In later steps, the counts will be updated to include information from other instances.)
        //
        // For each element in anchorIDsToGenerateNewSplitters:
        //   - Pick the two entries in _setOfSplitterAndCounts that tightly bound desiredCounts[anchorID].
        //   - Generate a new splitter and its local count, based on local data.
        //
        // [running example]
        //    * Assume anchorIDsToGenerateNewSplitters = [1, 2].
        //    * Assume, by examining the local data, this step generates two splitters
        //      - sa (25 local records smaller than it)
        //      - sb (40 local records smaller than it)
        //    * The content of allCandidates will be:
        //      - allCandidates[0] = [ ]
        //      - allCandidates[1] = [ {sa, _globalCount=25, _localCounts=[0, 25, 0]}, {sb, _globalCount=40, _localCounts=[0, 40, 0]} ]
        //      - allCandidates[2] = [ ]
        //
        for (vector<size_t>::const_iterator it = anchorIDsToGenerateNewSplitters.begin(); it!=anchorIDsToGenerateNewSplitters.end(); ++it) {
            size_t anchorID = *it;
            assert(anchorID > 0 && anchorID < _numInstances);

            // pickAnchorIDsToGenerateNewSplitters shouldn't have picked this anchorID if a perfect anchor was already known here.
            assert(_desiredCounts[anchorID] != _anchors[anchorID]->_globalCount);

            dummySplitterAndCounts._globalCount = _desiredCounts[anchorID];
            SetOfSplitterAndCounts::const_iterator itHigh = _setOfSplitterAndCounts.lower_bound(dummySplitterAndCounts);
            assert(itHigh != _setOfSplitterAndCounts.end());
            assert(itHigh != _setOfSplitterAndCounts.begin());
            assert(itHigh->_globalCount > _desiredCounts[anchorID]);
            SetOfSplitterAndCounts::const_iterator itLow = itHigh;
            --itLow;
            assert(itLow->_globalCount < _desiredCounts[anchorID]);

            size_t lowGlobalCount = itLow->_globalCount;
            size_t lowLocalCount = itLow->_localCounts[_myInstanceID];
            size_t highGlobalCount = itHigh->_globalCount;
            size_t highLocalCount = itHigh->_localCounts[_myInstanceID];

            // If between low and high, there is no local record, skip generating a splitter.
            assert(lowLocalCount <= highLocalCount);
            if (lowLocalCount == highLocalCount) {
                continue;
            }

            // We use an example to illustrate how to generate a candidateLocalIndex.
            // Assume:
            //   *itLow  = {some splitter, _globalCount=100, _localCounts=[38, 20, 27]}
            //   *itHigh = {some splitter, _globalCount=200, _localCounts=[90, 40, 70]}
            //   desiredCount = 125.
            // Because
            //   (125-100) / (200-100) = 1/4,
            // We want to find a local splitter at offset X, such that
            //   (X-20)/(40-20) = 1/4.
            // So X =
            //        (40 - 20)
            //      * (125 - 100)
            //      / (200 - 100)
            //      + 20
            //
            size_t candidateLocalIndex = (highLocalCount - lowLocalCount)
                                  * (_desiredCounts[anchorID] - lowGlobalCount)
                                  / (highGlobalCount - lowGlobalCount)
                                  + lowLocalCount;
            assert(candidateLocalIndex >= lowLocalCount);
            assert(candidateLocalIndex < highLocalCount);

            SplitterAndCounts candidate;
            candidate._globalCount = candidateLocalIndex;
            allocateSplitter(candidate._splitter);
            fillSplitterFromChunkIterators(candidateLocalIndex, candidate._splitter);

            // Unless the splitter is already known by _setOfSplitterAndCounts, insert it into allCandidates.
            assert(_tupleComparator->compare(itLow->_splitter, candidate._splitter) <= 0);
            assert(_tupleComparator->compare(candidate._splitter, itHigh->_splitter) < 0);
            if (_tupleComparator->compare(itLow->_splitter, candidate._splitter) < 0) {
                candidate._localCounts.resize(_numInstances);
                for (InstanceID i=0; i<_numInstances; ++i) {
                    candidate._localCounts[i] = 0;
                }
                candidate._localCounts[_myInstanceID] = candidateLocalIndex;

                allCandidates[_myInstanceID].push_back(candidate);
            }
        }

        // Broadcast the local splitters & their counts. In particular:
        // - num local splitters;
        // - for each splitter:
        //   * the splitter;
        //   * the local count of the splitter.
        archive::binary_oarchive* oArchive = oArchiveWrapper.reset();
        const size_t numLocalSplitters = allCandidates[_myInstanceID].size();
        (*oArchive) & numLocalSplitters;
        for (vector<SplitterAndCounts>::iterator it = allCandidates[_myInstanceID].begin(); it!=allCandidates[_myInstanceID].end(); ++it) {
            serializeSplitter(*oArchive, it->_splitter);
            assert(it->_globalCount == it->_localCounts[_myInstanceID]);
            (*oArchive) & it->_globalCount;
        }
        const bool copyToSharedBuffer = true;
        BufBroadcast(oArchiveWrapper.getSharedBuffer(copyToSharedBuffer), _query);

        // Receive the splitters & counts from every remote instance, and insert into allCandidates.
        // Along with each received candidate, lookup the localCount and update the counts in allCandidates.
        // At the end, broadcast the localCounts for all received candidates.
        //
        // [running example]
        //    * Assume we (on instance 1) receive from instance 0 one splitter sc with count 33. Via lookup, we find the localCount for sc is 27.
        //      The content of allCandidates will be:
        //      - allCandidates[0] = [ {sc, _globalCount=60, _localCounts=[33, 27,  0]} ]
        //      - allCandidates[1] = [ {sa, _globalCount=25, _localCounts=[ 0, 25,  0]}, {sb, _globalCount=40, _localCounts=[0, 40, 0]} ]
        //      - allCandidates[2] = [ ]
        //
        //    * Next, assume we receive from instance 2 one splitter sd with count 30. Suppose the localCount is 41.
        //      The content of allCandidates will be:
        //      - allCandidates[0] = [ {sc, _globalCount=60, _localCounts=[33, 27,  0]} ]
        //      - allCandidates[1] = [ {sa, _globalCount=25, _localCounts=[ 0, 25,  0]}, {sb, _globalCount=40, _localCounts=[0, 40, 0]} ]
        //      - allCandidates[2] = [ {sd, _globalCount=71, _localCounts=[ 0, 41, 30]} ]
        //
        //    * At the end of the step, the two local counts [27, 41] will be broadcasted.
        //
        oArchive = oArchiveWrapper.reset();

        for (InstanceID instanceID = 0; instanceID<_numInstances; ++instanceID) {
            if (instanceID == _myInstanceID) {
                continue;
            }
            archive::binary_iarchive* iArchive = iArchiveWrapper.reset(BufReceive(instanceID, _query));
            size_t num = 0;
            (*iArchive) & num;
            allCandidates[instanceID].resize(num);
            for (size_t i=0; i<num; ++i) {
                // Get the splitter and senderLocalCount.
                serializeSplitter(*iArchive, allCandidates[instanceID][i]._splitter);
                size_t senderLocalCount = 0;
                (*iArchive) & senderLocalCount;

                // Lookup myLocalCount for the splitter.
                size_t myLocalCount = lookupLocalCount(allCandidates[instanceID][i]._splitter);

                // Fill allCandidates.
                allCandidates[instanceID][i]._localCounts.resize(_numInstances);
                for (size_t k=0; k<_numInstances; ++k) {
                    allCandidates[instanceID][i]._localCounts[k] = 0;
                }
                allCandidates[instanceID][i]._globalCount = senderLocalCount + myLocalCount;
                allCandidates[instanceID][i]._localCounts[instanceID] = senderLocalCount;
                allCandidates[instanceID][i]._localCounts[_myInstanceID] = myLocalCount;

                // Add myLocalCount to the output archive.
                (*oArchive) & myLocalCount;
            }
        }

        BufBroadcast(oArchiveWrapper.getSharedBuffer(copyToSharedBuffer), _query);

        // Receive local counts from other instances, and update allCandidates.
        //
        // [running example]
        //    * Assume we receive from instance 0 its local counts for sa, sb, and sd as: [30, 38, 42].
        //      The content of allCandidates will be:
        //      - allCandidates[0] = [ {sc, _globalCount=60, _localCounts=[33, 27,  0]} ]
        //      - allCandidates[1] = [ {sa, _globalCount=55, _localCounts=[30, 25,  0]}, {sb, _globalCount=78, _localCounts=[38, 40, 0]} ]
        //      - allCandidates[2] = [ {sd, _globalCount=113, _localCounts=[42, 41, 30]} ]
        //
        //    * Assume we receive from instance 0 its local counts for sc, sa, and sb as: [27, 27, 27].
        //      The content of allCandidates will be:
        //      - allCandidates[0] = [ {sc, _globalCount=87,  _localCounts=[33, 27, 27]} ]
        //      - allCandidates[1] = [ {sa, _globalCount=82,  _localCounts=[30, 25, 27]}, {sb, _globalCount=105, _localCounts=[38, 40, 27]} ]
        //      - allCandidates[2] = [ {sd, _globalCount=113, _localCounts=[42, 41, 30]} ]
        //
        for (InstanceID senderID = 0; senderID<_numInstances; ++senderID) {
            if (senderID == _myInstanceID) {
                continue;
            }
            archive::binary_iarchive* iArchive = iArchiveWrapper.reset(BufReceive(senderID, _query));

            for (InstanceID toFillID = 0; toFillID<_numInstances; ++toFillID) {
                if (toFillID == senderID) {
                    continue;
                }
                for (size_t i=0, n=allCandidates[toFillID].size(); i<n; ++i) {
                    size_t count = 0;
                    (*iArchive) & count;
                    allCandidates[toFillID][i]._globalCount += count;
                    assert(allCandidates[toFillID][i]._localCounts[senderID] == 0);
                    allCandidates[toFillID][i]._localCounts[senderID] = count;
                }
            }
        }

        // Insert all the candidates into _setSplitterAndCounts.
        for (InstanceID instanceID = 0; instanceID<_numInstances; ++instanceID) {
            for (vector<SplitterAndCounts>::const_iterator it = allCandidates[instanceID].begin(); it!=allCandidates[instanceID].end(); ++it) {
                _setOfSplitterAndCounts.insert(*it);
            }
        }

        // Remove from it the useless splitters (i.e. every splitter whose globalCount does not tightly bound any desiredCounts).
        removeUselessSplitters();
    } // while (true)

    _timing.logTiming(logger, "[sort] Picking anchors");

    // Distribute the data based on the anchors.
    shared_ptr<MemArray> distributedArray = distributeBasedOnAnchors();
    _timing.logTiming(logger, "[sort] Distributing data based on anchors");

    if (totalError == 0) {
        return distributedArray;
    }

    // Adjust boundaries and coordinates.
    shared_ptr<MemArray> afterAdjustingBoundaries = redistributeToAdjustBoundaries(distributedArray);
    _timing.logTiming(logger, "[sort] Adjusting boundaries");
    return afterAdjustingBoundaries;
}

}
// namespace scidb
