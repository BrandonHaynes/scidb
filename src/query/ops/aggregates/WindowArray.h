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
 * @file WindowArray.h
 *
 * @brief The implementation of the array iterator for the window operator
 *
 * @author Konstantin Knizhnik <knizhnik@garret.ru>, poliocough@gmail.com,
 *         Paul Brown <paulgeoffreybrown@gmail.com>
 *
 */

#ifndef WINDOW_ARRAY_H_
#define WINDOW_ARRAY_H_

#include <string>
#include <vector>
#include "array/DelegateArray.h"
#include "array/Metadata.h"
#include "query/FunctionDescription.h"
#include "query/Expression.h"
#include "query/Aggregate.h"
#include "array/MemArray.h"

namespace scidb
{

using namespace std;
using namespace boost;

class WindowArray;
class WindowArrayIterator;
class MaterializedWindowChunkIterator;

/**
 *   Structure to hold definition of an individual window.
 *
 *   struct{...} to hold the boundary information about the windows to be
 *  computed over the input array. The window specification consists of a
 *  pair of values for each dimension in the InputArray: the number of steps
 *  preceeding the position for which the window is being computed, and the
 *  number of steps following.
 *
 */
struct WindowBoundaries
{
    WindowBoundaries()
    {
        _boundaries.first = _boundaries.second = 0;
    }

    WindowBoundaries(Coordinate preceding, Coordinate following)
    {
        SCIDB_ASSERT(preceding >= 0);
        SCIDB_ASSERT(following >= 0);

        _boundaries.first = preceding;
        _boundaries.second = following;
    }

    std::pair<Coordinate, Coordinate> _boundaries;
};

/**
 *   Used to process data in an input Chunk consumed/processed by window(...)
 *
 *   This structure is used within the window(...) operator to represent the
 *  state of each input data chunk as it is being processed. Access to the
 *  WindowChunk's state is through the WindowChunkIterator classes. Within
 *  the WindowChunk we process cells from the InputChunk, and for each
 *  "window" of cells in the InputChunk (where the size and shape of the
 *  window is taken from the operator's argument list).
 *
 */
class WindowChunk : public ConstChunk
{
    friend class MaterializedWindowChunkIterator;
    friend class WindowChunkIterator;

  public:
    WindowChunk(WindowArray const& array, AttributeID attrID);

    virtual const ArrayDesc& getArrayDesc() const;
    virtual const AttributeDesc& getAttributeDesc() const;
    virtual Coordinates const& getFirstPosition(bool withOverlap) const;
    virtual Coordinates const& getLastPosition(bool withOverlap) const;
    virtual boost::shared_ptr<ConstChunkIterator> getConstIterator(int iterationMode) const;
    virtual int getCompressionMethod() const;
    virtual Array const& getArray() const;

    /**
     *  When using the materialize algorithm, calculate by how much to step the iterator when it leaves the window(...)
     */
    inline uint64_t getStep() const;

    /**
     *   Set position within chunk referred to by the Iterator.
     */
    void setPosition(WindowArrayIterator const* iterator, Coordinates const& pos);

  private:
    void materialize();
    void pos2coord(uint64_t pos, Coordinates& coord) const;
    uint64_t coord2pos(const Coordinates& coord) const;
    inline bool valueIsNeededForAggregate (const Value & val, const ConstChunk & inputChunk) const;

    WindowArray const& _array;  
    WindowArrayIterator const* _arrayIterator;  
    size_t _nDims;
    Coordinates _arrSize;
    Coordinates _firstPos;
    Coordinates _lastPos;
    AttributeID _attrID;
    AggregatePtr _aggregate;

    //
    //  The existing implementation computes two maps when it decides to
    // materialize a chunk. One of all of the cells in the input chunk that
    // are not missing (_inputMap), and the second of all the cells in the
    // input chunk that are not in the overlapping region (_stateMap).
    std::map<uint64_t, bool> _stateMap;
    std::map<uint64_t, Value> _inputMap;

    //  TODO: We can eliminate one of these two trees, saving space and time.
    //        The idea is to store a single physical tree with elements of
    //        the tree containing enough information to distinguish when
    //        an attribute's contains a missing code (and can therefore
    //        be ignored for the purposes of computing the aggregate, but
    //        must be used as the "center" of an output window computation)
    //        or not.
    bool _materialized;
    boost::shared_ptr<CoordinatesMapper> _mapper;

    /**
     *   Returns true if the chunk's processing algorithm materializes input chunk.
     */
	inline bool isMaterialized() const { return _materialized; };

    Value _nextValue;
};

class WindowChunkIterator : public ConstChunkIterator
{
public:
    virtual int getMode();
    virtual bool isEmpty();
    virtual Value& getItem();
    virtual void operator ++();
    virtual bool end();
    virtual Coordinates const& getPosition();
    virtual bool setPosition(Coordinates const& pos);
    virtual void reset();
    ConstChunk const& getChunk();

    WindowChunkIterator(WindowArrayIterator const& arrayIterator, WindowChunk const& aChunk, int mode);
 
private:
    Value& calculateNextValue();
    bool attributeDefaultIsSameAsTypeDefault() const;

    WindowArray const& _array;
    WindowChunk const& _chunk;
    Coordinates const& _firstPos;
    Coordinates const& _lastPos;
    Coordinates _currPos;
    bool _hasCurrent;
    AttributeID _attrID;
    AggregatePtr _aggregate;
    Value _defaultValue;
    int _iterationMode;
    boost::shared_ptr<ConstChunkIterator> _inputIterator;
    boost::shared_ptr<ConstArrayIterator> _emptyTagArrayIterator;
    boost::shared_ptr<ConstChunkIterator> _emptyTagIterator;
    Value _nextValue;
};

class MaterializedWindowChunkIterator : public ConstChunkIterator
{ 
public:
    MaterializedWindowChunkIterator(WindowArrayIterator const& arrayIterator, WindowChunk const& aChunk, int mode);

    virtual int getMode();
    virtual bool isEmpty();
    virtual Value& getItem();
    virtual void operator ++();
    virtual bool end();
    virtual Coordinates const& getPosition();
    virtual bool setPosition(Coordinates const& pos);
    virtual void reset();
    ConstChunk const& getChunk();
 
private:
    void calculateNextValue();
    void stepToNextValidValue();

    WindowArray const& _array;
    WindowChunk const& _chunk;
    AggregatePtr _aggregate;
    Value _defaultValue;
    int _iterationMode;
    Value _nextValue;

    std::map<uint64_t, bool>const& _stateMap;
    std::map<uint64_t, Value>const& _inputMap;
    std::map<uint64_t, bool>::const_iterator _iter;

    inline bool posIsWithinOverlap ( Coordinates const& pos ) const;
    inline bool posIsWithinOverlap ( uint64_t const& pos ) const;

    Coordinate _currPos;

    size_t _nDims;
    Coordinates _coords;

};

class WindowArrayIterator : public ConstArrayIterator
{
    friend class WindowChunk;
    friend class MaterializedWindowChunkIterator;
    friend class WindowChunkIterator;
  public:
    virtual ConstChunk const& getChunk();
    virtual bool end();
    virtual void operator ++();
    virtual Coordinates const& getPosition();
    virtual bool setPosition(Coordinates const& pos);
    virtual void reset();

    /**
     * Return the algorithm named in the AFL window(...) expression.
     *
     * @return string containing name of algorithm being used
     */
    string const& getMethod() const { return _method; };
 
    WindowArrayIterator(WindowArray const& array, AttributeID id, AttributeID input, string const& method);

  private:
    WindowArray const& array;
    boost::shared_ptr<ConstArrayIterator> iterator;
    Coordinates currPos;
    bool hasCurrent;
    WindowChunk chunk;
    bool chunkInitialized;
    string _method;

};

class WindowArray : public Array
{
    friend class WindowArrayIterator;
    friend class MaterializedWindowChunkIterator;
    friend class WindowChunkIterator;
    friend class WindowChunk;

  public:
    virtual ArrayDesc const& getArrayDesc() const;
    virtual boost::shared_ptr<ConstArrayIterator> getConstIterator(AttributeID attr) const;

    WindowArray(ArrayDesc const& desc,
                boost::shared_ptr<Array> const& inputArray,
                vector<WindowBoundaries> const& window,
                vector<AttributeID> const& inputAttrIDs,
                vector <AggregatePtr> const& aggregates,
                string const& method);

    static const std::string PROBE;
    static const std::string MATERIALIZE;

  private:
    ArrayDesc _desc;
    ArrayDesc _inputDesc;
    vector<WindowBoundaries> _window;
    Dimensions _dimensions;
    boost::shared_ptr<Array> _inputArray;
    vector<AttributeID> _inputAttrIDs;
    vector <AggregatePtr> _aggregates;
    string _method;

};

}

#endif
