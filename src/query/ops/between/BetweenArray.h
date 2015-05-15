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
 * @file BetweenArray.h
 *
 * @brief The implementation of the array iterator for the between operator
 *
 * The array iterator for the between maps incoming getChunks calls into the
 * appropriate getChunks calls for its input array. Then, if the requested chunk
 * fits in the between range, the entire chunk is returned as-is. Otherwise,
 * the appropriate piece of the chunk is carved out.
 *
 * NOTE: In the current implementation if the between window stretches beyond the
 * limits of the input array, the behavior of the operator is undefined.
 *
 * The top-level array object simply serves as a factory for the iterators.
 */

#ifndef BETWEEN_ARRAY_H_
#define BETWEEN_ARRAY_H_

#include <string>
#include <array/DelegateArray.h>
#include <array/Metadata.h>
#include <array/SpatialRangesChunkPosIterator.h>

namespace scidb
{

using namespace std;
using namespace boost;

class BetweenArray;
class BetweenArrayIterator;
class BetweenChunkIterator;

typedef boost::shared_ptr<SpatialRanges> SpatialRangesPtr;
typedef boost::shared_ptr<SpatialRangesChunkPosIterator> SpatialRangesChunkPosIteratorPtr;

class BetweenChunk : public DelegateChunk
{
    friend class BetweenChunkIterator;
public:
    boost::shared_ptr<ConstChunkIterator> getConstIterator(int iterationMode) const;

    void setInputChunk(ConstChunk const& inputChunk);

    BetweenChunk(BetweenArray const& array, DelegateArrayIterator const& iterator, AttributeID attrID);

private:
    BetweenArray const& array;
    SpatialRange myRange;  // the firstPosition and lastPosition of this chunk.
    bool fullyInside;
    bool fullyOutside;
    boost::shared_ptr<ConstArrayIterator> emptyBitmapIterator;
};
    
class BetweenChunkIterator : public ConstChunkIterator, CoordinatesMapper
{
public:
    int getMode() {
        return _mode;
    }

    Value& getItem();
    bool isEmpty();
    bool end();
    void operator ++();
    Coordinates const& getPosition();
    bool setPosition(Coordinates const& pos);
    void reset();
    ConstChunk const& getChunk();

    BetweenChunkIterator(BetweenChunk const& chunk, int iterationMode);

  protected:
    BetweenArray const& array;
    BetweenChunk const& chunk;
    boost::shared_ptr<ConstChunkIterator> inputIterator;
    Coordinates currPos;
    int _mode;
    bool hasCurrent;
    bool _ignoreEmptyCells;
    MemChunk shapeChunk;
    boost::shared_ptr<ConstChunkIterator> emptyBitmapIterator;
    TypeId type;

    /**
     * Several member functions of class SpatialRanges takes a hint, on where the last successful search.
     */
    size_t _hintForSpatialRanges;
};

class ExistedBitmapBetweenChunkIterator : public BetweenChunkIterator
{
public:
    virtual  Value& getItem();

    ExistedBitmapBetweenChunkIterator(BetweenChunk const& chunk, int iterationMode);

private:
     Value _value;
};
     
   
class NewBitmapBetweenChunkIterator : public BetweenChunkIterator
{
public:
    virtual  Value& getItem();

    NewBitmapBetweenChunkIterator(BetweenChunk const& chunk, int iterationMode);

protected:
     Value _value;
};

class EmptyBitmapBetweenChunkIterator : public NewBitmapBetweenChunkIterator
{
public:
    virtual Value& getItem();
    virtual bool isEmpty();

    EmptyBitmapBetweenChunkIterator(BetweenChunk const& chunk, int iterationMode);
};

/**
 * ====== NOTE FROM Donghui Z. ON UNIFYING THE TWO ITERATORS ===========
 *
 * Prior to the 14.8 release, there were two iterators for BetweenArray.
 * They differ in their way to find the next chunk that has data and intersects the between ranges.
 *   - A "random" iterator computes the next chunkPos purely from the between ranges, and asks inputArray whether the chunk exists.
 *   - A "sequential" iterator asks inputArray for the next chunk, and checks to see if its range intersects the between ranges.
 * There was a threshold parameter BetweenArray::BETWEEN_SEQUENTIAL_INTERATOR_THRESHOLD = 6000.
 *
 * Donghui Z. believes the separation is artificial and non-optimal. It is possible that when running a query,
 * sometimes the "random" iterator can find the next chunk faster and sometimes the "sequential" iterator can find faster.
 * So Donghui decided to creatively integrate the two iterator into one, as follows:
 *   - A "combined" iterator alternates in asking inputArray for the next chunk and computing the next chunkPos from the
 *     between ranges, and use whichever gets there first.
 *
 * Also, this class uses a SpatialRangesChunkIterator to iterate over the chunkPos in the logical space.
 * Per THE REQUEST TO JUSTIFY LOGICAL-SPACE ITERATION (see RegionCoordinatesIterator.h),
 * here is why this is ok.
 * The above described "combined" iterator will not forever iterate over the logical space (until a valid chunkPos is found).
 * Each iteration step is accompanied with a probing, of whether the next existing chunk intersects the query range.
 *
 * ====== BELOW ARE Alex P.'s ORIGINAL NOTE DESCRIBING THE TWO-ITERATOR APPROACH ============
 *
 * Between Array has two ArrayIterator types:
 * 1. BetweenArrayIterator advances chunks (operator++) by finding the next chunk inside the between box
 *    and probing input to see if that chunk exists. Assume the between box describes b logical chunks,
 *    and the underlying input array has n chunks - the iteration using this iterator will run in O( b * lg(n))
 *
 * 2. BetweenArraySequentialIterator advances chunks by asking input for its next chunk, and, if that chunk does
 *    not overlap with the between box, continuing to ask for the next input chunk until we either find a chunk
 *    that fits or we run out of chunks. If the input has n chunks present, this iteration will run in O(n).
 *
 * Sometimes b is small (selecting just a few cells) and sometimes b is large (selecting a 10-20 chunks
 * from a very sparse array). The number n is a count of actual (not logical) chunks and we don't know how big
 * that is, but assuming about 1TB storage per SciDB instance and 10MB per chunk, we can expect upper bound on
 * n to be about 100,000. I've never seen real arrays from customers above 5,000 chunks.
 *
 * 100,000 / lg(100,000) ~= 6,000. So if b is below that number, use BetweenArrayIterator. Otherwise, use
 * BetweenArraySequentialIterator. [poliocough, 4/14/12]
 */
class BetweenArrayIterator : public DelegateArrayIterator
{
    friend class BetweenChunkIterator;
public:

	/***
	 * Constructor for the between iterator
	 * Here we initialize the current position vector to all zeros, and obtain an iterator for the appropriate
	 * attribute in the input array.
	 */
	BetweenArrayIterator(BetweenArray const& between, AttributeID attrID, AttributeID inputAttrID);

	/***
	 * The end call checks whether we're operating with the last chunk of the between
	 * window.
	 */
	virtual bool end();

	/***
	 * The ++ operator advances the current position to the next chunk of the between
	 * window.
	 */
	virtual void operator ++();

	/***
	 * Simply returns the current position
	 * Initial position is a vector of zeros of appropriate dimensionality
	 */
	virtual Coordinates const& getPosition();

	/***
	 * Here we only need to check that we're not moving beyond the bounds of the between window
	 */
	virtual bool setPosition(Coordinates const& pos);

	/***
	 * Reset simply changes the current position to all zeros
	 */
	virtual void reset();

protected:
    BetweenArray const& array;
    SpatialRangesChunkPosIteratorPtr _spatialRangesChunkPosIteratorPtr;
	Coordinates pos; 
    bool hasCurrent;

    /**
     * @see BetweenChunkIterator::_hintForSpatialRanges
     */
    size_t _hintForSpatialRanges;

    /**
     * Increment inputIterator at least once,
     * then advance the two iterators to the next chunk that (a) exists in the database; and (b) intersects a query range.
     *   - Upon success: hasCurrent = true; pos = both iterators' position; chunkInitialized = false;
     *   - Upon failure: hasCurrent = false.
     *
     * @preconditions:
     *   - inputIterator is pointing to a chunk that exists in the database.
     *     (It may or may NOT intersect any query range.)
     *   - spatialRangesChunkPosIteratorPtr is pointing to a chunk intersecting some query range.
     *     (It may or may NOT exist in the database.)
     *
     * @note: by "exists in the database", we mean in the local SciDB instance.
     * @note: in reset(), do NOT call this function if the initial position is already valid.
     */
    void advanceToNextChunkInRange();
};

class BetweenArray : public DelegateArray
{
    friend class BetweenChunk;
    friend class BetweenChunkIterator;
    friend class BetweenArrayIterator;
    friend class ExistedBitmapBetweenChunkIterator;
    friend class NewBitmapBetweenChunkIterator;

public:
    BetweenArray(ArrayDesc const& desc, SpatialRangesPtr const& spatialRangesPtr, boost::shared_ptr<Array> const& input);

    DelegateArrayIterator* createArrayIterator(AttributeID attrID) const;
    DelegateChunk* createChunk(DelegateArrayIterator const* iterator, AttributeID attrID) const;

private:
    /**
     * The original spatial ranges.
     */
    SpatialRangesPtr _spatialRangesPtr;

    /**
     * The modified spatial ranges where every SpatialRange._low is reduced by (interval-1).
     * The goal is to quickly tell, from a chunk's chunkPos, whether the chunk overlaps a spatial range.
     * In particular, a chunk overlaps, if and only if the extended spatial range contains the chunkPos.
     * E.g. Let there be chunk with chunkPos=0 and interval 10. A range [8, 19] intersects the chunk's space,
     * equivalently, the modified range [-1, 19] contains 0.
     */
    SpatialRangesPtr _extendedSpatialRangesPtr;
};

} //namespace

#endif /* BETWEEN_ARRAY_H_ */
