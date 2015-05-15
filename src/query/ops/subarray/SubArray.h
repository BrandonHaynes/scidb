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
 * @file SubArray.h
 *
 * @brief The implementation of the array iterator for the subarray operator
 *
 * The array iterator for the subarray maps incoming getChunks calls into the
 * appropriate getChunks calls for its input array. Then, if the requested chunk
 * fits in the subarray range, the entire chunk is returned as-is. Otherwise,
 * the appropriate piece of the chunk is carved out.
 *
 * NOTE: In the current implementation if the subarray window stretches beyond the
 * limits of the input array, the behavior of the operator is undefined.
 *
 * The top-level array object simply serves as a factory for the iterators.
 * @author Konstantin Knizhnik <knizhnik@garret.ru>
 * @author poliocough@gmail.com
 */

#ifndef SUB_ARRAY_H_
#define SUB_ARRAY_H_

#include <string>
#include "array/DelegateArray.h"
#include "array/Metadata.h"

namespace scidb
{

using namespace std;
using namespace boost;

class SubArray;
class SubArrayIterator;

void subarrayMappingArray(string const& dimName, string const& mappingArrayName, string const& tmpMappingArrayName,
                          Coordinate from, Coordinate till, boost::shared_ptr<Query> const& query);

/***
 * NOTE: This looks like a candidate for an intermediate abstract class: PositionConstArrayIterator.
 */
class SubArrayIterator : public DelegateArrayIterator
{
protected:
    bool setInputPosition(size_t i);
    void fillSparseChunk(size_t i);

    SubArray const& array;
    Coordinates outPos;
    Coordinates inPos;
    bool hasCurrent;

    Coordinates outChunkPos;
    MemChunk sparseBitmapChunk;
    MemChunk sparseChunk;
    // outIterator must be defined AFTER sparseXChunk because it needs to be destroyed BEFORE
    boost::shared_ptr<ChunkIterator> outIterator;

  public:
	/***
	 * Constructor for the subarray iterator
	 * Here we initialize the current position vector to all zeros, and obtain an iterator for the appropriate
	 * attribute in the input array.
	 */
	SubArrayIterator(SubArray const& subarray, AttributeID attrID, bool doReset = true);

	virtual ~SubArrayIterator()
	{}

	/***
	 * The end call checks whether we're operating with the last chunk of the subarray
	 * window.
	 */
	virtual bool end();

	/***
	 * The ++ operator advances the current position to the next chunk of the subarray
	 * window.
	 */
	virtual void operator ++();

	/***
	 * Simply returns the current position
	 * Initial position is a vector of zeros of appropriate dimensionality
	 */
	virtual Coordinates const& getPosition();

	/***
	 * Here we only need to check that we're not moving beyond the bounds of the subarray window
	 */
	virtual bool setPosition(Coordinates const& pos);

	/***
	 * Reset simply changes the current position to all zeros
	 */
	virtual void reset();

    virtual ConstChunk const& getChunk();
};

class MappedSubArrayIterator : public SubArrayIterator
{
protected:
    set<Coordinates,CoordinatesLess>::const_iterator _mIter;

public:
    MappedSubArrayIterator(SubArray const& subarray, AttributeID attrID);

    virtual ~MappedSubArrayIterator()
    {}

    virtual bool setPosition(Coordinates const& pos);

    virtual void operator ++();

    virtual void reset();
};

class SubArray : public DelegateArray
{
    friend class SubArrayIterator;
    friend class MappedSubArrayIterator;

  private:
    Coordinates subarrayLowPos;
    Coordinates subarrayHighPos;
    Dimensions const& dims;
    Dimensions const& inputDims;
    bool aligned;

    bool _useChunkSet;
    set<Coordinates, CoordinatesLess> _chunkSet;

    void buildChunkSet();
    void addChunksToSet(Coordinates outChunkCoords, size_t dim = 0);

    /**
     * SubArray has two ArrayIterator types:
     * 1. SubArrayIterator probes the space of all possible chunks
     * 2. MappedSubArrayIterator first builds a map of all chunks that are present.
     *
     * Building the map is preferred when the input array is very sparse and the subarray box can contain millions of possible chunks.
     * In 99% of the cases, walking along one attribute and collecting the chunk coordinates is very cheap.
     *
     * Between has a very similar two-iterator system. See comment in BetweenArray.h for why 6,000 is a good number.
     * We should merge these constants somehow but making them one config does not seem right.
     * [poliocough, 4/18/12]
     *
     * TODO: we should merge these maps together into a unified API:
     * Array
     * {
     *   bool hasChunkCount();
     *   size_t getChunkCount();
     *   bool hasChunkMap();
     *   map<...> getChunkMap();
     * }
     * This could prove useful inside ops like subarray, between, slice. AND it could provide for faster implementation of ops like join.
     */
    static const size_t SUBARRAY_MAP_ITERATOR_THRESHOLD = 6000;

  public:
    SubArray(ArrayDesc& d, Coordinates lowPos, Coordinates highPos,
             boost::shared_ptr<Array>& input,
             boost::shared_ptr<Query> const& query);

    DelegateArrayIterator* createArrayIterator(AttributeID attrID) const;

    void out2in(Coordinates const& out, Coordinates& in) const;
    void in2out(Coordinates const& in, Coordinates& out) const;

};


} //namespace

#endif /* SUB_ARRAY_H_ */
