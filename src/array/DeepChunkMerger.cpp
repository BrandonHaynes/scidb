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
 * @file DeepChunkMerger.cpp
 *
 *  Created on: Jan 17, 2013
 *      Author: Donghui Zhang
 */

#include "DeepChunkMerger.h"
#include <log4cxx/logger.h>
using namespace boost;
using namespace std;

namespace scidb
{
static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.array.deepChunkMerge"));

DeepChunkMerger::DeepChunkMerger(MemChunk& dst, MemChunk const& with, boost::shared_ptr<Query> const& query)
: _dst(dst), _with(with), _query(query), _pinBufferWith(with),
  _numFinishedPPositionsDst(0), _numFinishedPPositionsWith(0), _numOverlappedPPositions(0)
{
}

void DeepChunkMerger::merge()
{
    assert(_dst.getAttributeDesc().isEmptyIndicator() == _with.getAttributeDesc().isEmptyIndicator());

    // The case when both chunks are empty-bitmap chunks.
    if (_dst.getAttributeDesc().isEmptyIndicator()) {
        mergeEmptyBitmapChunks();
        return;
    }

    // Get payloads
    _payloadDst.reset( new ConstRLEPayload((char*)_dst.getData()) );
    _payloadWith.reset( new ConstRLEPayload((char*)_with.getData()) );

    // If both chunks have empty bitmaps attached at the end, merge at the segment level; otherwise, merge at the cell level.
    if (_dst.getSize() > _payloadDst->packedSize() && _with.getSize() > _payloadWith->packedSize()) {
        mergeAtSegmentLevel();
    }
    else {
        _dst.shallowMerge(_with, _query);
    }
}

void DeepChunkMerger::advanceSmallerEmptyBitmapSegment(
        bool isFromDst,
        ConstRLEEmptyBitmap::Segment& segmentSmaller, ConstRLEEmptyBitmap::Segment& segmentLarger,
        size_t& numFinishedPPositionsSmaller, size_t& numFinishedPPositionsLarger,
        ConstRLEEmptyBitmap::SegmentIterator& itSmaller, ConstRLEEmptyBitmap::SegmentIterator& itLarger)
{
    assert(segmentSmaller._lPosition < segmentLarger._lPosition);

    ConstRLEEmptyBitmap::Segment segment;
    segment._lPosition = segmentSmaller._lPosition;
    segment._pPosition = localToGlobalPPosition(isFromDst, segmentSmaller._pPosition);

    // If the smaller segment is completely before the larger segment, output the full smaller segment
    if (segmentSmaller._lPosition + segmentSmaller._length <= segmentLarger._lPosition) {
        segment._length = segmentSmaller._length;
        numFinishedPPositionsSmaller += segment._length;
        ++ itSmaller;
    }

    // Otherwise, the two segments have overlap
    else {
        segment._length = segmentLarger._lPosition - segmentSmaller._lPosition;
        numFinishedPPositionsSmaller += segment._length;
        itSmaller.advanceWithinSegment(segment._length);
    }

    appendIntermediateEmptyBitmapSegment(segment);
}

void DeepChunkMerger::advanceEmptyBitmapSegment(
        bool isFromDst,
        ConstRLEEmptyBitmap::Segment& segmentSmaller,
        size_t& numFinishedPPositionsSmaller,
        ConstRLEEmptyBitmap::SegmentIterator& itSmaller)
{
    ConstRLEEmptyBitmap::Segment segment;
    segment._lPosition = segmentSmaller._lPosition;
    segment._pPosition = localToGlobalPPosition(isFromDst, segmentSmaller._pPosition);

    segment._length = segmentSmaller._length;
    numFinishedPPositionsSmaller += segment._length;
    ++ itSmaller;

    appendIntermediateEmptyBitmapSegment(segment);
}

void DeepChunkMerger::advanceBothEmptyBitmapSegmentsBy(
        position_t length,
        ConstRLEEmptyBitmap::Segment const& segmentDst, ConstRLEEmptyBitmap::Segment const& segmentWith,
        ConstRLEEmptyBitmap::SegmentIterator& itDst, ConstRLEEmptyBitmap::SegmentIterator& itWith)
{
    assert(segmentDst._lPosition == segmentWith._lPosition);
    assert(length>0);
    assert(length==min(segmentDst._length, segmentWith._length));

    ConstRLEEmptyBitmap::Segment segment;
    segment._lPosition = segmentDst._lPosition;
    segment._pPosition = localToGlobalPPosition(true, segmentDst._pPosition); // true = isFromDst
    segment._length = length;

    _numFinishedPPositionsDst += length;
    _numFinishedPPositionsWith += length;
    _numOverlappedPPositions += length;

    if (segmentDst._length == length) {
        ++itDst;
    } else {
        itDst.advanceWithinSegment(length);
    }

    if (segmentWith._length == length) {
        ++itWith;
    } else {
        itWith.advanceWithinSegment(length);
    }

    appendIntermediateEmptyBitmapSegment(segment);
}

void DeepChunkMerger::mergeEmptyBitmapChunks()
{
    assert(!_payloadDst);
    assert(!_payloadWith);
    assert(!_bitmapDst);
    assert(!_bitmapWith);

    _bitmapDst.reset( new ConstRLEEmptyBitmap(_dst) );
    _bitmapWith.reset( new ConstRLEEmptyBitmap(_with) );
    ConstRLEEmptyBitmap::SegmentIterator itDst(_bitmapDst.get());
    ConstRLEEmptyBitmap::SegmentIterator itWith(_bitmapWith.get());

    // When both chunks have unfinished segments.
    while (!itDst.end() && !itWith.end()) {
        ConstRLEEmptyBitmap::Segment segmentDst, segmentWith;
        itDst.getVirtualSegment(segmentDst);
        itWith.getVirtualSegment(segmentWith);

        // Do the two segments have the same start lPosition?
        if (segmentDst._lPosition == segmentWith._lPosition) {
            position_t minLength = min(segmentDst._length, segmentWith._length);
            advanceBothEmptyBitmapSegmentsBy(minLength, segmentDst, segmentWith, itDst, itWith);
        }
        else {
            // the _dst segment has a smaller start lPosition?
            if (segmentDst._lPosition < segmentWith._lPosition) {
                advanceSmallerEmptyBitmapSegment(true, segmentDst, segmentWith, _numFinishedPPositionsDst, _numFinishedPPositionsWith, itDst, itWith);
            }
            else {
                advanceSmallerEmptyBitmapSegment(false, segmentWith, segmentDst, _numFinishedPPositionsWith, _numFinishedPPositionsDst, itWith, itDst);
            }
        }
    }

    // Drain the _dst side.
    while (!itDst.end()) {
        ConstRLEEmptyBitmap::Segment segment;
        itDst.getVirtualSegment(segment);
        advanceEmptyBitmapSegment(true, segment, _numFinishedPPositionsDst, itDst); // true = isDst
    }

    // Drain the _with side.
    while (!itWith.end()) {
        ConstRLEEmptyBitmap::Segment segment;
        itWith.getVirtualSegment(segment);
        advanceEmptyBitmapSegment(false, segment, _numFinishedPPositionsWith, itWith);
    }

    // Write back to _dst.
    RLEEmptyBitmap mergedBitmap;
    BOOST_FOREACH(ConstRLEEmptyBitmap::Segment const& segment, _intermediateBitmapSegments) {
        mergedBitmap.addSegment(segment);
    }
    _dst.allocate(mergedBitmap.packedSize());
    mergedBitmap.pack((char*)_dst.getData());
    _dst.write(_query);
}

void DeepChunkMerger::appendIntermediateEmptyBitmapSegment(ConstRLEEmptyBitmap::Segment& segment)
{
    bool appended = false;
    if (! _intermediateBitmapSegments.empty()) {
        ConstRLEEmptyBitmap::Segment& lastSegment = _intermediateBitmapSegments.back();
        if (lastSegment._lPosition + lastSegment._length == segment._lPosition) {
            appended = true;
            lastSegment._length += segment._length;
        }
    }

    if (!appended) {
        _intermediateBitmapSegments.push_back(segment);
    }
}

void DeepChunkMerger::mergeAtSegmentLevel()
{
    assert(_payloadDst);
    assert(_payloadWith);
    assert(!_bitmapDst);
    assert(!_bitmapWith);

    // Get the bitmaps
    _bitmapDst.reset( new ConstRLEEmptyBitmap((char*)_dst.getData() + _payloadDst->packedSize()) );
    _bitmapWith.reset( new ConstRLEEmptyBitmap((char*)_with.getData() + _payloadWith->packedSize()) );

    // Get the iterators
    ConstRLEEmptyBitmap::SegmentIterator itBitmapDst(_bitmapDst.get());
    ConstRLEEmptyBitmap::SegmentIterator itBitmapWith(_bitmapWith.get());
    ConstRLEPayload::SegmentIterator itPayloadDst(_payloadDst.get());
    ConstRLEPayload::SegmentIterator itPayloadWith(_payloadWith.get());

    ConstRLEEmptyBitmap::Segment bitmapSegmentDst, bitmapSegmentWith;

    // When both chunks have unfinished bitmap segments.
    while (!itBitmapDst.end() && !itBitmapWith.end()) {
        itBitmapDst.getVirtualSegment(bitmapSegmentDst);
        itBitmapWith.getVirtualSegment(bitmapSegmentWith);

        // Do the two segments have the same start lPosition?
        if (bitmapSegmentDst._lPosition == bitmapSegmentWith._lPosition) {
            // Let minBitmapLength = min{bitmapSegmentDst.length, bitmapSegmentWith.length}.
            // The next minBitmapLength logical positions are 'solid' in both chunks.
            // We need to skip the payloads in _dst, and copy the payloads from _with.
            //
            // We advance the payload iterators first, with the understanding that the three variables (_numOverlappedPPositions, etc.) reflect
            // the state before this advancing.
            //
            position_t minBitmapLength = min(bitmapSegmentDst._length, bitmapSegmentWith._length);
            advanceBothPayloadIteratorsBy(minBitmapLength, itPayloadDst, itPayloadWith);
            advanceBothEmptyBitmapSegmentsBy(minBitmapLength, bitmapSegmentDst, bitmapSegmentWith, itBitmapDst, itBitmapWith);
        }
        else {
            // Let minBitmapLength = min{diff between the two lPositions, length of the smaller one}.
            // The next minBitmapLength logical positions are solid in the chunk with a smaller lPosition, but empty in the other chunk.
            // We need to copy the payloads from the smaller side.
            //
            if (bitmapSegmentDst._lPosition < bitmapSegmentWith._lPosition) {
                position_t minBitmapLength = min(bitmapSegmentDst._length, (bitmapSegmentWith._lPosition - bitmapSegmentDst._lPosition));
                advancePayloadIteratorBy(true, minBitmapLength, itPayloadDst); // true = advancing _dst
                advanceSmallerEmptyBitmapSegment(true, bitmapSegmentDst, bitmapSegmentWith, _numFinishedPPositionsDst, _numFinishedPPositionsWith, itBitmapDst, itBitmapWith);
            }
            else {
                position_t minBitmapLength = min(bitmapSegmentWith._length, (bitmapSegmentDst._lPosition - bitmapSegmentWith._lPosition));
                advancePayloadIteratorBy(false, minBitmapLength, itPayloadWith); // false = not advancing _dst (so advancing _with)
                advanceSmallerEmptyBitmapSegment(false, bitmapSegmentWith, bitmapSegmentDst, _numFinishedPPositionsWith, _numFinishedPPositionsDst, itBitmapWith, itBitmapDst);
            }
        }
    }

    // Drain the _dst side.
    while (!itBitmapDst.end()) {
        itBitmapDst.getVirtualSegment(bitmapSegmentDst);
        advancePayloadIteratorBy(true, bitmapSegmentDst._length, itPayloadDst); // true = advancing _dst
        advanceEmptyBitmapSegment(true, bitmapSegmentDst, _numFinishedPPositionsDst, itBitmapDst);
    }

    // Drain the _with side.
    while (!itBitmapWith.end()) {
        itBitmapWith.getVirtualSegment(bitmapSegmentWith);
        advancePayloadIteratorBy(false, bitmapSegmentWith._length, itPayloadWith); // false = not advancing _dst (so advancing _with)
        advanceEmptyBitmapSegment(false, bitmapSegmentWith, _numFinishedPPositionsWith, itBitmapWith);
    }

    // Write back to _dst.
    RLEEmptyBitmap mergedBitmap;
    BOOST_FOREACH(ConstRLEEmptyBitmap::Segment const& segment, _intermediateBitmapSegments) {
        mergedBitmap.addSegment(segment);
    }
    RLEPayload mergedPayload(TypeLibrary::getType(_dst.getAttributeDesc().getType()));
    fillMergedPayloadUsingIntermediateResult(mergedPayload);

    _dst.allocate(mergedPayload.packedSize() + mergedBitmap.packedSize());
    mergedPayload.pack((char*)_dst.getData());
    mergedBitmap.pack((char*)_dst.getData() + mergedPayload.packedSize());
    _dst.write(_query);
}

void DeepChunkMerger::fillMergedPayloadUsingIntermediateResult(RLEPayload& mergedPayload)
{
    uint32_t numRealVals = 0;  // how many real values that are already stored in the payload
    vector<char> varPart;

    BOOST_FOREACH(IntermediatePayloadSegment const& inputSegment, _intermediatePayloadSegments) {
        RLEPayload::Segment outputSegment;
        outputSegment._null = inputSegment._null;
        outputSegment._pPosition = inputSegment._pPosition;
        outputSegment._same = inputSegment._same;
        outputSegment._valueIndex = (inputSegment._null ? inputSegment._valueIndex : numRealVals);

        int realLength = inputSegment._length; // how many real values to insert to the payload
        if (inputSegment._null) {
            realLength = 0;
        } else if (inputSegment._same) {
            realLength = 1;
        }
        numRealVals += realLength;

        mergedPayload.appendAPartialSegmentOfValues(outputSegment, varPart,
                inputSegment._isFromDst ? *(_payloadDst.get()) : *(_payloadWith.get()),
                inputSegment._valueIndex, realLength); // realLength may be 0, but we still need to insert the segment (of missing values)
    }

    // The last (dummy) segment
    if (! _intermediatePayloadSegments.empty()) {
        IntermediatePayloadSegment const& lastSegment = _intermediatePayloadSegments.back();
        mergedPayload.flush(lastSegment._pPosition + lastSegment._length);
    }

    // The var part.
    mergedPayload.setVarPart(varPart);
}

position_t DeepChunkMerger::localToGlobalPPosition(bool isFromDst, position_t localPPosition)
{
    if (isFromDst) {
        assert(_numOverlappedPPositions <= _numFinishedPPositionsWith);
        return localPPosition + _numFinishedPPositionsWith - _numOverlappedPPositions;
    }
    assert(_numOverlappedPPositions <= _numFinishedPPositionsDst);
    return localPPosition + _numFinishedPPositionsDst - _numOverlappedPPositions;
}

void DeepChunkMerger::advancePayloadIteratorBy(bool isFromDst, position_t length, ConstRLEPayload::SegmentIterator& itPayload)
{
    IntermediatePayloadSegment outputSegment;
    outputSegment._isFromDst = isFromDst;

    position_t remainingLength = length;
    assert(remainingLength > 0);

    while (remainingLength > 0) {
        assert(!itPayload.end());
        ConstRLEPayload::SegmentWithLength inputSegment;
        itPayload.getVirtualSegment(inputSegment);
        position_t stepSize = min(remainingLength, inputSegment._length);
        position_t pPosition = localToGlobalPPosition(isFromDst, inputSegment._pPosition);
        outputSegment.AssignValuesExceptIsFromDst(pPosition, stepSize, itPayload, inputSegment);
        appendIntermediatePayloadSegment(outputSegment);

        itPayload.advanceBy(stepSize);
        remainingLength -= stepSize;
    }
}

void DeepChunkMerger::advanceBothPayloadIteratorsBy(position_t length,
        ConstRLEPayload::SegmentIterator& itPayloadDst, ConstRLEPayload::SegmentIterator& itPayloadWith)
{
    IntermediatePayloadSegment outputSegment;
    outputSegment._isFromDst = false;  // always use the values from _with

    // Skip the payload segments in _dst
    position_t remainingLength = length;
    assert(remainingLength > 0);

    while (remainingLength > 0) {
        assert(!itPayloadDst.end());
        ConstRLEPayload::SegmentWithLength inputSegment;
        itPayloadDst.getVirtualSegment(inputSegment);
        position_t stepSize = min(remainingLength, inputSegment._length);
        itPayloadDst.advanceBy(stepSize);
        remainingLength -= stepSize;
    }

    // Generate output segments from _dst
    remainingLength = length;
    assert(remainingLength > 0);

    while (remainingLength > 0) {
        assert(!itPayloadWith.end());
        ConstRLEPayload::SegmentWithLength inputSegment;
        itPayloadWith.getVirtualSegment(inputSegment);
        position_t stepSize = min(remainingLength, inputSegment._length);
        position_t pPosition = localToGlobalPPosition(false, inputSegment._pPosition);       // isFromDst = false
        outputSegment.AssignValuesExceptIsFromDst(pPosition, stepSize, itPayloadWith, inputSegment);
        appendIntermediatePayloadSegment(outputSegment);

        itPayloadWith.advanceBy(stepSize);
        remainingLength -= stepSize;
    }
}

void DeepChunkMerger::appendIntermediatePayloadSegment(IntermediatePayloadSegment const& outputSegment)
{
    bool appended = false;
    if (! _intermediatePayloadSegments.empty()) {
        IntermediatePayloadSegment& last = _intermediatePayloadSegments.back();
        if (last._currSeg == outputSegment._currSeg && last._isFromDst == outputSegment._isFromDst) {
            assert(last._pPosition + last._length == outputSegment._pPosition);
            assert(last._same == outputSegment._same);

            last._length += outputSegment._length;
            appended = true;
        }
    }

    if (!appended) {
        _intermediatePayloadSegments.push_back(outputSegment);
    }
}

} // end of namespace scidb
