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
 * @file TupleArray.h
 *
 * @brief In-memory array that allows cells to be manipulated as whole objects
 */

#ifndef TUPLE_ARRAY_H_
#define TUPLE_ARRAY_H_

#include <map>

#include <util/Arena.h>
#include <array/MemArray.h>
#include <query/Expression.h>
#include <query/FunctionDescription.h>

namespace scidb
{

class TupleArray;
class TupleArrayIterator;
class TupleChunk;
class TupleChunkIterator;

struct SortingAttributeInfo
{
    int  columnNo; // zero based
    bool ascent;
};

typedef std::vector<SortingAttributeInfo> SortingAttributeInfos;

/**
 * A class that compare two tuples, each of which is of type (Value*).
 * A vector of SortingAttributeInfo objects are used to guide which pairs of values to compare, and whether ASC/DESC ordering is desired.
 * @note
 *   - It is ok to pass a TupleComparator object to iqsort() and MergeSortArray.
 *     They need operator() to return a 3-choice integer (negative for less than, 0 for equal to, and positive for greater than).
 *   - It is NOT ok to pass a TupleComparator object to an STL container or algorithm (such as std::set or std::lower_bound).
 *     They need operator() to return a 2-choice boolean (true for less than, false for greater than).
 *   - For the latter use case, @see TupleLessThan.
 */
class TupleComparator
{
 private:
    SortingAttributeInfos   const _sortingAttributeInfos;
    ArrayDesc               const _arrayDesc;
    vector<FunctionPointer> _leFunctions;
    vector<FunctionPointer> _eqFunctions;

    // The types of each key are needed in compare() to call isNullOrNan().
    // The types are acquired in the constructor so that they don't need to be calculated again and again in compare().
    vector<DoubleFloatOther> _types;

  public:
    TupleComparator(PointerRange<const SortingAttributeInfo>, const ArrayDesc&);

    /**
     * Null < NaN < a regular double/float value.
     */
    int compare(const Value*, const Value*) const;

    /**
     *  Functor interface to use with iqsort().
     */
    int operator()(Value const* t1, Value const* t2) const
    {
        return compare(t1,t2);
    }

    /**
     * Compare a single attribute.
     */
    int compareOneAttribute(const Value*, const Value*, size_t whichAttribute) const;

    /**
     * Getter for the sorting key.
     */
    SortingAttributeInfos const& getSortingAttributeInfos()
    {
        return _sortingAttributeInfos;
    }
};

/**
 * A wrapper around a TupleComparator object, to support boolean operator() needed by STL.
 */
class TupleLessThan
{
    /**
     * A pointer to a TupleComparator object.
     */
    TupleComparator* _tupleComparator;
public:
    /**
     * @param tupleComparator  pointer to a tupleComparator object.
     */
    TupleLessThan(TupleComparator* tupleComparator): _tupleComparator(tupleComparator)
    {}
    /**
     *  Functor interface to use with an STL container or algorithm.
     */
    bool operator()(Value const* t1, Value const* t2) const
    {
        return _tupleComparator->compare(t1, t2) < 0;
    }
};

struct SortContext
{
    SortingAttributeInfos _sortingAttributeInfos;
};

/**
 * TupleArray is a 1D scidb::Array that wraps over a vector of tuples.
 * It provides append() to append (a subset of) data from an input array.
 * It provides appendTuple() to append a single tuple.
 * It provides sort() to sort the records.
 *
 * One important use case of TupleArray is to support SortArray.
 * SortArray has two flavors, depending on whether preservePositions is true. See SortArray for more details.
 * In response:
 *   - If preservePositions==true: the TupleArray's schema has two more attributes than the input array.
 *     They are: chunk_pos and cell_pos.
 *     And append() will fill in the two extra attributes.
 *   - If preservePositions==false, the TupleArray has the same number of attributes as the input array.
 *
 * @note The TupleArray owns the memory of all the tuples.
 *       Each tuple is generated using arena::newVector().
 *       The arena is a "resetting arena"; memory is freed in one shot in the destructor of TupleArray.
 */
class TupleArray : public Array
{
    friend class TupleChunk;
    friend class TupleChunkIterator;
    friend class TupleArrayIterator;
  public:
    void sort(boost::shared_ptr<TupleComparator> tcomp);

    virtual ArrayDesc const& getArrayDesc() const;

    virtual boost::shared_ptr<ConstArrayIterator> getConstIterator(AttributeID attId) const;

    /**
     * @param outputSchema        the schema of the TupleArray, on the output side.
     * @param inputArrayIterators a vector of the input array iterators.
     * @param inputSchema         the schema of the input array.
     * @param nChunks             how many chunks of data to read from the input array, from its current position.
     * @param sizeHint            an estimate on the total number of tuples (for performance reason), which does not need to be perfect.
     * @param pageSize            the page size used by the resetting arena. See class Arena.
     * @param parentArena         the parent memory arena, from which a child & resetting arena can be attached.
     * @param preservePositions   whether the TupleArray, when appending data from an input array, should append cell position as well.
     *
     * @note The first tuple will have a coordinate = outputSchema.getDimensions()[0].getStartMin().
     */
    TupleArray(ArrayDesc const& outputSchema,
               vector< boost::shared_ptr<ConstArrayIterator> > const& arrayIterators,
               ArrayDesc const& inputSchema,
               size_t nChunks,
               size_t sizeHint,
               size_t pageSize,
               arena::ArenaPtr const& parentArena,
               bool preservePositions);

    /**
     * This is the version used by everywhere in the system except SortArray.
     * This TupleArray is not tightly bound with an input array.
     * Naturally, there is no need to "preserve positions".
     *
     * @param outputSchema    the schema of the TupleArray, on the output side.
     * @param parentArena     the parent memory arena.
     * @param offset          the offset to be added to the coordinate of the first tuple.
     *
     * @note The first tuple will have a coordinate = offset + outputSchema.getDimensions()[0].getStartMin().
     */
    TupleArray(ArrayDesc const& schema,
               arena::ArenaPtr const& parentArena,
               Coordinate offset = 0);

   ~TupleArray();

    /*
     * Append up-to nChunks of data from given input array iterators.
     * @param inputSchema    the schema of the input array.
     * @param arrayIterators the input array iterators.
     * @param nChunks        the number of chunks to read.
     */
    virtual void append(ArrayDesc const& inputSchema, std::vector< boost::shared_ptr<ConstArrayIterator> > const& arrayIterators, size_t nChunks);

    /**
     * Append all data from an input array.
     * @param inputArray an input array.
     */
    virtual void append(boost::shared_ptr<Array> const& inputArray);

    /**
     * Append a single tuple to the array.
     * @param inputTuple  an input tuple.
     * @note This function allocates memory, and copies the inputTuple into the allocated memory.
     */
    virtual void appendTuple(PointerRange<const Value> inputTuple);

    /**
    * Hint for the number of items to be stored in array
    */
    void reserve(size_t capacity)
    {
        _tuples.reserve(capacity);
    }

    /**
     * Truncate size in array descriptor to real number of available tuples
     */
    void truncate();

    size_t getNumberOfTuples() const
    {
        return _tuples.size();
    }

    size_t getTupleArity() const
    {
        return _desc.getAttributes().size();
    }

    /**
     * Compute the memory footprint of a single tuple. Useful for planning purposes.
     * This is NOT equal to the size of a cell inside a structure like a MemArray.
     * MemArrays use RLEPayloads which exhibit much smaller per-value footprint.
     */
    static size_t getTupleFootprint(Attributes const& attrs);

    size_t getTupleFootprint() const
    {
        return getTupleFootprint(_desc.getAttributes());
    }

  protected:
    arena::ArenaPtr const _arena;  // used to allocate each tuple
    ArrayDesc             _desc;
    Coordinate            _start;
    Coordinate            _end;
    mgd::vector<Value*>   _tuples;
    size_t                _chunkSize;

    /**
     * @see SortArray::_preservePositions.
     */
    bool                  _preservePositions;

    /**
     * Each attributeID in the output schema belongs to one of the following four categories.
     */
    enum AttributeKind
    {
        FROM_INPUT_ARRAY,
        POS_FOR_CHUNK,
        POS_FOR_CELL,
        EMPTY_BITMAP
    };
    AttributeKind getAttributeKind(AttributeID attributeID) const;
};

class TupleChunk : public ConstChunk
{
    friend class TupleChunkIterator;
    friend class TupleArrayIterator;
  public:
    const ArrayDesc& getArrayDesc() const;
    const AttributeDesc& getAttributeDesc() const;
    int getCompressionMethod() const;
    Coordinates const& getFirstPosition(bool withOverlap) const;
    Coordinates const& getLastPosition(bool withOverlap) const;
    boost::shared_ptr<ConstChunkIterator> getConstIterator(int iterationMode) const;

    Array const& getArray() const
    {
        return _array;
    }

    TupleChunk(TupleArray const& array, AttributeID attrID);

 private:
    TupleArray const&   _array;
    AttributeID         _attrID;
    Coordinates         _firstPos;
    Coordinates         _lastPos;
};

class TupleArrayIterator : public ConstArrayIterator
{
  public:
    virtual ConstChunk const& getChunk();
    virtual bool end();
    virtual void operator ++();
    virtual Coordinates const& getPosition();
    virtual bool setPosition(Coordinates const& pos);
    virtual void reset();

    TupleArrayIterator(TupleArray const& array, AttributeID attrID);

  private:
    TupleArray const& _array;
    AttributeID       _attrID;
    TupleChunk        _chunk;
    Coordinates       _currPos;
    bool              _hasCurrent;
};

class TupleChunkIterator : public ConstChunkIterator
{
  public:
    int getMode();
    Value& getItem();
    bool isEmpty();
    bool end();
    void operator ++();
    Coordinates const& getPosition();
    bool setPosition(Coordinates const& pos);
    void reset();
    ConstChunk const& getChunk();

    TupleChunkIterator(TupleChunk const& chunk, int iterationMode);

  private:
    bool isVisible() const;

    TupleChunk const& _chunk;
    TupleArray const& _array;
    AttributeID       _attrID;
    Coordinates       _currPos;
    size_t            _last;
    int               _mode;
    size_t            _i;
};

}

#endif
