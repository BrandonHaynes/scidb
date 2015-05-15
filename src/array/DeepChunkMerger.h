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
 *  Created on: Jan 23, 2013
 *      Author: Donghui Zhang
 */

#ifndef DEEPCHUNKMERGER_H_
#define DEEPCHUNKMERGER_H_

#include <array/MemArray.h>
#include <vector>
#include <boost/scoped_ptr.hpp>
#include <boost/foreach.hpp>

namespace scidb
{
/**
 * Helper class for deep chunk merge.
 * It stores intermediate payload & bitmap data.
 * It performs deep merge both when merging bitmap chunks and when merging data chunks.
 */
class DeepChunkMerger
{
private:
    /**
     * Intermediate segment for the RLEPayload part of the merged chunk.
     * The valueIndex is a 'finger' into one of the source payloads.
     */
    struct IntermediatePayloadSegment
    {
        uint32_t   _valueIndex: 30; // [local] meaningful *only* in the context of one of the two payloads. It is the valueIndex in one of _dst or _with
        uint32_t   _same: 1;        // [local]
        uint32_t   _null: 1;        // [local]
        size_t     _currSeg;        // [local] multiple calls to appendIntermediatePayloadSegment will append to the same output segment, if they are dealing with the same input segment
        position_t _pPosition;      // [global] E.g. if _dst has 5 earlier physical positions and _with has 6 earlier physical positions, the next pPosition is 11.
        position_t _length;         // [global] #positions of this segment
        bool       _isFromDst;      // [global] Whether valueIndex/same/null are from _dst.

        /**
         * Assign all values, except isFromDst, based on a SegmentIterator.
         * @param[in] pPosition       the 'global' pPosition
         * @param[in] length          the 'global' length
         * @param[in] segmentIterator a segment iterator in the input payload
         * @param[in] segment         a segment in the input payload
         */
        void AssignValuesExceptIsFromDst(position_t pPosition, position_t length, ConstRLEPayload::SegmentIterator& segmentIterator, ConstRLEPayload::SegmentWithLength const& segment)
        {
            _pPosition = pPosition;
            _length = length;
            _valueIndex = segment._valueIndex;
            _same = segment._same;
            _null = segment._null;
            _currSeg = segmentIterator.getCurrSeg();
        }

        /**
         * Assign all values, except isFromDst, based on a SegmentIterator.
         * This version is a wrapper to the other version that has an additional parameter of SegmentWithLength.
         * @param[in] pPosition       the 'global' pPosition
         * @param[in] length          the 'global' length
         * @param[in] segmentIterator a segment iterator in the input payload
         */
        void AssignValuesExceptIsFromDst(position_t pPosition, position_t length, ConstRLEPayload::SegmentIterator& segmentIterator)
        {
            ConstRLEPayload::SegmentWithLength segment;
            segmentIterator.getVirtualSegment(segment);
            AssignValuesExceptIsFromDst(pPosition, length, segmentIterator, segment);
        }
    };

    MemChunk& _dst;
    MemChunk const& _with;
    shared_ptr<Query> _query;
    PinBuffer _pinBufferWith;    // The constructor of DeepChunkMerger will try to pin the source chunk; ok if cannot pin.

    scoped_ptr<ConstRLEPayload> _payloadDst, _payloadWith;
    scoped_ptr<ConstRLEEmptyBitmap> _bitmapDst, _bitmapWith;
    std::vector<IntermediatePayloadSegment> _intermediatePayloadSegments;
    std::vector<ConstRLEEmptyBitmap::Segment> _intermediateBitmapSegments;

    /**
     * When scanning the empty-bitmap segments, given a pPosition in one chunk, the corresponding global pPosition is the local pPosition,
     * increased by _numFinishedPPositions in the other chunk, minus _numOverlappedPPositions.
     *
     * @example
     * Let the empty-bitmap segments in the two chunks are:
     *                                   lPosition: 0  1  2  3  4  5  6  7  8  9  10  11  12  13
     *     segments in _dst (and local pPositions):       0--1           2--3--4
     *    segments in _with (and local pPositions):                0        1--2--3
     *  segments in result (and global pPositions):       0--1     2     3  4--5  6
     *
     * That is, _dst has three segments, the first one having lPosition=2 and length=2, and so on.
     * There is an overlap between the second segment of _dst and the second segment of _with.
     * For the purpose of generating resulted empty-bitmap segments, both segment will be cut, such that
     * each resulted segment either completed cover both source ranges (like [8..9]), or only contains data from a single source.
     *
     * At lPosition 0 (initialization):
     *     _numOverlappedPPositions = _numFinishedPPositionsDst = _numFinishedPPositionsWith = 0.
     * At lPosition 5 (about to process the first _with segment):
     *     _numFinishedPPositionsDst is 2.
     *     So while the _with segment has a local pPosition=0, the global pPosition=2.
     * At lPosition 7 (about to process the second _dst_with segment):
     *     _numFinishedPPositionsDst=2; _numFinishedPPositionsWith=1.
     *     So while the _dst segment has a local pPosition=2, the global pPosition=3.
     *     Note that the length of the result segment is 1, since at lPosition=8 the two segments overlap.
     * At lPosition 8 (about to ignore the two cells - lPositions 8 and 9 - in _dst and generate a result segment from values in _dst:
     *     _numFinishedPPositionsDst=3; _numFinishedPPositionsWith=1.
     *     So while the _with segment has a local pPosition=1, the global pPosition=4.
     * At lPosition 10 (about to generate a segment of length 1 using data from _with):
     *     _numFinishedPPositionsDst=5; _numFinishedPPositionsWith=3; _numOverlappedPPositions=2.
     *     So while the _with segment has a local pPosition=3, the global pPosition = 3 + 5 - 2 = 6.
     *
     * These variables are modified in the advanceEmptyBitmap methods, not the advanePayload methods.
     */
    size_t _numFinishedPPositionsDst, _numFinishedPPositionsWith, _numOverlappedPPositions;

public:
    /**
     * @param dst   destination chunk
     * @param with  source chunk
     * @param query the query object
     */
    DeepChunkMerger(MemChunk& dst, MemChunk const& with, boost::shared_ptr<Query> const& query);

    /**
     * Merge _with into _dst (parameters of the constructor).
     * @pre Both chunks have to be in RLE format.
     */
    void merge();

private:
    /**
     * Efficient merge, at the level of segments (of ConstRLEPayload and ConstRLEEmptyBitmap).
     * @pre _payloadDst and _payloadWith are available.
     */
    void mergeAtSegmentLevel();

    /**
     * Merge chunks, when they are empty bitmap chunks.
     */
    void mergeEmptyBitmapChunks();

    /**
     * Append an empty-bitmap segment to the end of _intermediateBitmapSegments.
     * The newly appended segment may need to be merged into the existing last segment.
     * @note This function does NOT change _numOverlappedPPositions, _numFinishedPPositionsDst, or _numFinishedPPositionsWith;
     */
    void appendIntermediateEmptyBitmapSegment(ConstRLEEmptyBitmap::Segment& segment);

    /**
     * Produce the next segment of merged empty bitmap, when the two source segments have different starting lPositions.
     * @note To increase code reuse, the same function is called either when the _dst side has a smaller lPosition or when the _with side has a smaller lPosition.
     *
     * @param[in]    isFromDst       whether the side with a smaller lPosition is the _dst side
     * @param[inout] segmentSmaller  the segment of the smaller side, i.e. the starting lPosition is smaller
     * @param[inout] segmentLarger   the segment of the larger side
     * @param[inout] numFinishedPPositionsSmaller  the _numFinishedPPositions member variable of the smaller side
     * @param[inout] numFinishedPPositionsLarger   the _numFinishedPPositions member variable of the larger side
     * @param[inout] itSmaller  the SegmentIterator of the smaller side
     * @param[inout] itLarger   the SegmentIterator of the larger side
     */
    void advanceSmallerEmptyBitmapSegment(
            bool isFromDst,
            ConstRLEEmptyBitmap::Segment& segmentSmaller, ConstRLEEmptyBitmap::Segment& segmentLarger,
            size_t& numFinishedPPositionsSmaller, size_t& numFinishedPPositionsLarger,
            ConstRLEEmptyBitmap::SegmentIterator& itSmaller, ConstRLEEmptyBitmap::SegmentIterator& itLarger);

    /**
     * Produce the next segment of merged empty bitmap, using a segment from one chunk, when the other chunk had been drained.
     *
     * @param[in]    isFromDst       whether the side with a smaller lPosition is the _dst side
     * @param[inout] segmentSmaller  the segment of the smaller side, i.e. the starting lPosition is smaller
     * @param[inout] numFinishedPPositionsSmaller  the _numFinishedPPositions member variable of the smaller side
     * @param[inout] itSmaller  the SegmentIterator of the smaller side
     */
    void advanceEmptyBitmapSegment(
            bool isFromDst,
            ConstRLEEmptyBitmap::Segment& segmentSmaller,
            size_t& numFinishedPPositionsSmaller,
            ConstRLEEmptyBitmap::SegmentIterator& itSmaller);

    /**
     * Produce the next segment of merged empty bitmap, when the two source segments have the same starting lPositions.
     * @note To increase code reuse, the same function is called regardless to which side has a smaller length.
     *
     * @param[in]    length          the length to advance -- can't exceed the min length of the two source empty-bitmap chunk segments
     * @param[in]    segmentDst      the segment of the _dst chunk
     * @param[in]    segmentLarger   the segment of the _with chunk
     * @param[inout] itDst           the SegmentIterator of the _dst chunk
     * @param[inout] itWith          the SegmentIterator of the _with chunk
     */
    void advanceBothEmptyBitmapSegmentsBy(
            position_t length,
            ConstRLEEmptyBitmap::Segment const& segmentDst, ConstRLEEmptyBitmap::Segment const& segmentWith,
            ConstRLEEmptyBitmap::SegmentIterator& itDst, ConstRLEEmptyBitmap::SegmentIterator& itWith);

    /**
     * Produce a series of segments for the merged payload, to cover a specified number of physical locations.
     * These locations must be consecutive both in the logical space and in the physical space.
     * However, the range may cover multiple payload segments, because a sub-range of consecutive cells with the same value may have its own payload segment.
     * Also, the range's start (or end) may be in the middle of some payload segment.
     *
     * @param[in]    isFromDst       whether this is the _dst side chunk
     * @param[in]    length          how many positions to advance
     * @param[inout] itPayload       a SegmentIterator in the RLEPayload
     *
     * @note This function may call appendIntermediatePayloadSegment() to append new intermediate payload segments to the end of _payloadSegments.
     * @note This function will definitely advance the iterator (@param itPayload).
     *   It may also modify the payload segment (@param itPayload), by loading subsequent segments from the source chunks,
     *   in case the input segment did not cover the whole @param length.
     */
    void advancePayloadIteratorBy(bool isFromDst, position_t length, ConstRLEPayload::SegmentIterator& itPayload);

    /**
     * Produce a series of segments for the merged payload, to cover a specified number of physical locations.
     * These locations must be consecutive both in the logical space and in the physical space, in both chunks.
     * The function generates values from the values in _with, and ignores the values in _dst.
     *
     * @param[in]    length             how many positions to advance
     * @param[inout] itPayloadDst       a SegmentIterator in the _dst chunk's payload
     * @param[inout] itPayloadWith      a SegmentIterator in the _with chunk's payload
     *
     * @note This function may call appendIntermediatePayloadSegment() to append new intermediate payload segments to the end of _payloadSegments.
     * @note This function will definitely advance the iterators (@param itPayloadDst and @param itPayloadWith).
     *   It may also modify the payload segments (@param itPayloadDst and @param itPayloadWith), by loading subsequent segments from the source chunks,
     *   in case the input segments did not cover the whole @param length.
     */
    void advanceBothPayloadIteratorsBy(position_t length,
            ConstRLEPayload::SegmentIterator& itPayloadDst, ConstRLEPayload::SegmentIterator& itPayloadWith);

    /**
     * Using what's stored in the intermediate payload segments, fill an initially empty mergedPayload.
     * @param[out] mergedPayload    placeholder for the merged payload
     */
    void fillMergedPayloadUsingIntermediateResult(RLEPayload& mergedPayload);

    /**
     * Append a segment to the end of _intermediatePayloadSegments.
     * Note that this segment may be merged into the last segment already in there.
     */
    void appendIntermediatePayloadSegment(IntermediatePayloadSegment const& outputSegment);

    /**
     * Convert a local pPosition to a global pPosition.
     * @param[in] isFromDst      whether the local pPosition is from the _dst side
     * @param[in] localPPosition the local pPosition
     * @return    the global pPosition
     *
     * @see _numOverlappedPPositions
     */
    position_t localToGlobalPPosition(bool isFromDst, position_t localPPosition);
};

}
#endif /* DEEPCHUNKMERGER_H_ */
