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
 * PhysicalRank.cpp
 *  Created on: Mar 11, 2011
 *      Author: poliocough@gmail.com
 */

#include <cmath>
#include <boost/foreach.hpp>

#include <query/Operator.h>
#include <util/Network.h>
#include <array/Metadata.h>
#include <log4cxx/logger.h>
#include <array/MemArray.h>
#include <array/Compressor.h>
#include <util/RegionCoordinatesIterator.h>
#include <util/Timing.h>

#include "RankCommon.h"

using namespace std;
using namespace boost;

namespace scidb
{

double getPercentage(Coordinate quantileIndex, DimensionDesc const& quantileDimension)
{
    double numPositions = quantileDimension.getEndMax() - quantileDimension.getStartMin();
    return (quantileIndex - quantileDimension.getStartMin()) * 1.0 / numPositions;
}

struct QuantileBucket
{
    vector<double> indeces;
    vector<double> maxIndeces;
    vector<Value> values;
};

typedef unordered_map <Coordinates, QuantileBucket> QuantileBucketsMap;

class QuantileChunkIterator : public ConstChunkIterator
{
public:
    QuantileChunkIterator(ArrayDesc const& desc,
                          ConstChunk const* chunk,
                          AttributeID attr,
                          int mode,
                          shared_ptr <QuantileBucketsMap> buckets,
                          shared_ptr <DimensionGrouping> grouping):
        _iterationMode(mode),
        _desc(desc),
        _firstPos(chunk->getFirstPosition(false)),
        _lastPos(chunk->getLastPosition(false)),
        _currPos(_firstPos.size()),
        _attrID(attr),
        _chunk(chunk),
        _buckets(buckets),
        _grouping(grouping),
        _value(TypeLibrary::getType(chunk->getAttributeDesc().getType()))
    {
        reset();
    }

    virtual int getMode()
    {
        return _iterationMode;
    }

    virtual bool isEmpty()
    {
        return false;
    }

    virtual Value& getItem()
    {
        if (!_hasCurrent)
            throw USER_EXCEPTION(SCIDB_SE_UDO, SCIDB_LE_NO_CURRENT_ELEMENT);
        Coordinates reducedCoords(_currPos.size() -1);
        for (size_t i =0; i<reducedCoords.size(); i++)
        {
            reducedCoords[i]=_currPos[i];
        }

        if (reducedCoords.size()==0)
        {
            reducedCoords.push_back(0);
        }

        Coordinate quantileNo = _currPos[_currPos.size()-1];

        if (_attrID == 0)
        {
            double pctValue = getPercentage(quantileNo, _desc.getDimensions()[_desc.getDimensions().size() - 1]);
            _value.setDouble(pctValue);
        }
        else if ( _buckets->count(reducedCoords) == 0 || (*_buckets)[reducedCoords].values.size() == 0)
        {
            _value.setNull();
        }
        else
        {
            _value = (*_buckets)[reducedCoords].values[quantileNo];
        }

        return _value;
    }

    virtual void operator ++()
    {
        if (!_hasCurrent)
            throw USER_EXCEPTION(SCIDB_SE_UDO, SCIDB_LE_NO_CURRENT_ELEMENT);
        for (int i = _currPos.size(); --i >= 0;)
        {
            if (++_currPos[i] > _lastPos[i])
            {
                _currPos[i] = _firstPos[i];
            }
            else
            {
                _hasCurrent = true;
                return;
            }
        }
        _hasCurrent = false;
    }

    virtual bool end()
    {
        return !_hasCurrent;
    }

    virtual Coordinates const& getPosition()
    {
        return _currPos;
    }

    virtual bool setPosition(Coordinates const& pos)
    {
        for (size_t i = 0, n = _currPos.size(); i < n; i++)
        {
            if (pos[i] < _firstPos[i] || pos[i] > _lastPos[i])
            {
                return _hasCurrent = false;
            }
        }
        _currPos = pos;
        return _hasCurrent = true;
    }

    virtual void reset()
    {
        _currPos = _firstPos;
        _hasCurrent = true;
    }

    virtual ConstChunk const& getChunk()
    {
        return *_chunk;
    }

  private:
    int _iterationMode;
    ArrayDesc _desc;
    Coordinates const& _firstPos;
    Coordinates const& _lastPos;
    Coordinates _currPos;
    bool _hasCurrent;
    AttributeID _attrID;
    ConstChunk const* _chunk;
    shared_ptr <QuantileBucketsMap > _buckets;
    shared_ptr <DimensionGrouping> _grouping;
    Value _value;
};

class QuantileChunk : public ConstChunk
{
  public:
    QuantileChunk(Array const& array, ArrayDesc const& desc, AttributeID attrID, shared_ptr <QuantileBucketsMap> buckets, shared_ptr<DimensionGrouping> grouping):
    _array(array),
    _desc(desc),
    _firstPos(_desc.getDimensions().size()),
    _lastPos(_desc.getDimensions().size()),
    _attrID(attrID),
    _buckets(buckets),
    _grouping(grouping)
    {}

    virtual const Array& getArray() const
    {
        return _array;
    }

    virtual const ArrayDesc& getArrayDesc() const
    {
        return _desc;
    }

    virtual const AttributeDesc& getAttributeDesc() const
    {
        return _desc.getAttributes()[_attrID];
    }

    virtual Coordinates const& getFirstPosition(bool withOverlap) const
    {
        return _firstPos;
    }

    virtual Coordinates const& getLastPosition(bool withOverlap) const
    {
        return _lastPos;
    }

    virtual boost::shared_ptr<ConstChunkIterator> getConstIterator(int iterationMode) const
    {
        return boost::shared_ptr<ConstChunkIterator>(new QuantileChunkIterator(_desc, this, _attrID, iterationMode, _buckets, _grouping));
    }

    virtual int getCompressionMethod() const
    {
        return CompressorFactory::NO_COMPRESSION;
    }

    void setPosition(Coordinates const& pos)
    {
        delete materializedChunk;
        materializedChunk = NULL;
        _firstPos = pos;
        Dimensions const& dims = _desc.getDimensions();
        for (size_t i = 0, n = dims.size(); i < n; i++)
        {
            _lastPos[i] = _firstPos[i] + dims[i].getChunkInterval() - 1;
            if (_lastPos[i] > dims[i].getEndMax())
            {
                _lastPos[i] = dims[i].getEndMax();
            }
        }
    }

  private:
    Array const& _array;
    ArrayDesc _desc;
    Coordinates _firstPos;
    Coordinates _lastPos;
    AttributeID _attrID;
    shared_ptr <QuantileBucketsMap> _buckets;
    shared_ptr <DimensionGrouping> _grouping;
};

class QuantileArrayIterator : public ConstArrayIterator
{
public:
    QuantileArrayIterator(Array const& array,
                          ArrayDesc const& desc,
                          shared_ptr <QuantileBucketsMap> buckets,
                          AttributeID attrID,
                          shared_ptr <DimensionGrouping> grouping,
                          shared_ptr <set<size_t> > liveChunks)
        : _desc(desc),
          _currPos(_desc.getDimensions().size()),
          _chunk(array, _desc, attrID, buckets, grouping),
          _liveChunks(liveChunks)
    {
        reset();
    }

    virtual ConstChunk const& getChunk()
    {
        if (!_hasCurrent)
            throw USER_EXCEPTION(SCIDB_SE_UDO, SCIDB_LE_NO_CURRENT_ELEMENT);
        _chunk.setPosition(_currPos);
        return _chunk;
    }

    virtual void reset()
    {
        if (_liveChunks->size())
        {
            _currChunkNo = *(_liveChunks->begin());
            setPosition();
        }
        else
        {
            _hasCurrent = false;
        }
    }

    virtual bool setPosition(Coordinates const& pos)
    {
        Dimensions const& dims = _desc.getDimensions();
        size_t chunkNo = 0;
        for (size_t i = 0, n = _currPos.size(); i < n; i++)
        {
            if (pos[i] < dims[i].getStartMin() || pos[i] > dims[i].getEndMax())
            {
                return _hasCurrent = false;
            }

            chunkNo *= (dims[i].getLength() + dims[i].getChunkInterval() - 1) / dims[i].getChunkInterval();
            chunkNo += (pos[i] - dims[i].getStartMin()) / dims[i].getChunkInterval();
        }
        if (_liveChunks->count(chunkNo))
        {
            _currChunkNo = chunkNo;
            setPosition();
        }
        else
        {
            _hasCurrent = false;
        }
        return _hasCurrent;
    }

    virtual Coordinates const& getPosition()
    {
        return _currPos;
    }

    virtual bool end()
    {
        return !_hasCurrent;
    }

    virtual void operator ++()
    {
        if (!_hasCurrent)
            throw USER_EXCEPTION(SCIDB_SE_UDO, SCIDB_LE_NO_CURRENT_ELEMENT);

        set<size_t>::iterator next = _liveChunks->upper_bound(_currChunkNo);
        if (next == _liveChunks->end())
        {
            _hasCurrent = false;
        }
        else
        {
            _currChunkNo = *next;
            setPosition();
        }
    }

private:
    void setPosition()
    {
        Dimensions const& dims = _desc.getDimensions();
        size_t chunkNo = _currChunkNo;
        for (int i = dims.size(); --i >= 0;)
        {
            size_t chunkInterval = dims[i].getChunkInterval();
            size_t nChunks = (dims[i].getLength() + chunkInterval - 1) / chunkInterval;
            _currPos[i] = dims[i].getStartMin() + (chunkNo % nChunks)*chunkInterval;
            chunkNo /= nChunks;
        }
        _hasCurrent = true;
    }

    ArrayDesc _desc;
    Coordinates _currPos;
    bool _hasCurrent;
    QuantileChunk _chunk;
    size_t _currChunkNo;
    shared_ptr <set<size_t> > _liveChunks;
};

class QuantileArray : public Array
{
public:
    QuantileArray(ArrayDesc const& desc,
                  shared_ptr <QuantileBucketsMap> buckets,
                  shared_ptr <DimensionGrouping> grouping,
                  shared_ptr <set<size_t> > liveChunks):
          _desc(desc),
          _buckets(buckets),
          _grouping(grouping),
          _liveChunks(liveChunks)
    {}

    virtual boost::shared_ptr<ConstArrayIterator> getConstIterator(AttributeID attr) const
    {
        return boost::shared_ptr<ConstArrayIterator>(new QuantileArrayIterator(*this, _desc, _buckets, attr, _grouping, _liveChunks));
    }

    virtual ArrayDesc const& getArrayDesc() const
    {
        return _desc;
    }

private:
    ArrayDesc _desc;
    shared_ptr <QuantileBucketsMap> _buckets;
    shared_ptr <DimensionGrouping> _grouping;
    shared_ptr <set<size_t> > _liveChunks;
};

typedef RowCollection<Coordinates> RowCollectionGroup;      // every chunk is a row
typedef unordered_map <Coordinates, vector<Value> > MapGroupToQuantile;    // one entry per group with non-Null values.

class GroupbyQuantileChunk;

/**
 * @note
 *   attrID = 0: percentage.
 *   attrID = 1: quantile value.
 *
 * @example
 *   When chunk->numQuantilesPlusOne() == 5,
 *   each group has 5 pairs of values:
 *   (0, v1), (0.25, v2), (0.5, v3), (0.75, v4), (1, v5)
 *
 * @note
 *   This class uses a RegionCoordinatesIterator to iterate over the cellPos in the logical chunk space.
 *   Per THE REQUEST TO JUSTIFY LOGICAL-SPACE ITERATION (see RegionCoordinatesIterator.h),
 *   here is why it is (or was) ok.
 *   The quantile() operator, in the SciDB 14.12 release and earlier, returned a not-emptyable array.
 *   So there was no holes in the logical space.
 *   However, now that not-emptyable arrays were removed from the system, in the future we should change quantile()
 *   to generate emptyable arrays. At that time, it will NOT be appropriate to use a RegionCoordinatesIterator.
 */
class GroupbyQuantileChunkIterator : public ConstChunkIterator
{
private:
  int _iterationMode;
  bool _hasCurrent;
  AttributeID _attrID;
  const GroupbyQuantileChunk* _chunk;
  Value _value;

  // A null value.
  Value _nullValue;

  // The iterator that iterates over the groups in the chunk.
  RegionCoordinatesIterator _groupIterator;

  // The index of the value to output, in the current group.
  size_t _indexInCurrentGroup;

  // Pointer to the vector of quantile values for the current group.
  // Null if the current group only has NULL values.
  const vector<Value>* _quantilesInCurrentGroup;

  // _tmpPos is a concatenation of _currentGroup and _indexInCurrentGroup.
  Coordinates _tmpPos;

  // numQuantilesPlusOne
  size_t _numQuantilesPlusOne;

public:
    GroupbyQuantileChunkIterator(
                          GroupbyQuantileChunk const* chunk,
                          AttributeID attr,
                          int mode,
                          size_t numQuantilesPlusOne);

    virtual int getMode()
    {
        return _iterationMode;
    }

    virtual bool isEmpty()
    {
        return false;
    }

    virtual Value& getItem()
    {
        if (!_hasCurrent) {
            throw USER_EXCEPTION(SCIDB_SE_UDO, SCIDB_LE_NO_CURRENT_ELEMENT);
        }

        assert(_indexInCurrentGroup < _numQuantilesPlusOne);
        assert(_numQuantilesPlusOne>1);

        if (_attrID == 0) {  // percent
            double pctValue = static_cast<double>(_indexInCurrentGroup) / (_numQuantilesPlusOne-1);
            _value.setDouble(pctValue);
            return _value;
        } else if (_quantilesInCurrentGroup){ // quantiles exist
            _value = (*_quantilesInCurrentGroup)[_indexInCurrentGroup];
            return _value;
        }

        // else: quantiles do not exist
        return _nullValue;
    }

    virtual void operator ++();

    virtual bool end()
    {
        return !_hasCurrent;
    }

    virtual Coordinates const& getPosition()
    {
        Coordinates without = _groupIterator.getPosition();
        std::copy(without.begin(), without.end(), _tmpPos.begin());
        _tmpPos[_tmpPos.size()-1] = _indexInCurrentGroup;
        return _tmpPos;
    }

    virtual bool setPosition(Coordinates const& pos);

    virtual void reset();

    virtual ConstChunk const& getChunk();
};

/**
 * @note
 *   GroupbyQuantileChunk::setPosition(), which is called by GroupbyQuantileArrayIterator::getChunk(),
 *   will open the rows of pRowCollectionGroup and compute their quantile values.
 */
class GroupbyQuantileChunk : public ConstChunk
{
    friend class GroupbyQuantileChunkIterator;
    friend class GroupbyQuantileArrayIterator;

private:
    Array const& _array;
    Coordinates _firstPos;
    Coordinates _lastPos;
    AttributeID _attrID;

    // First and last groups that define a region covered by this chunk.
    // _firstGroup is equivalent to _firstPos without the last dimension.
    // Same with _lastGroup vs. _lastPos.
    Coordinates _firstGroup;
    Coordinates _lastGroup;

    // Map non-NULL groups to quantile values.
    MapGroupToQuantile _mapGroupToQuantile;

    // How many reported quantile values?
    size_t _numQuantilesPlusOne;

    // Pointer to RowCollectionGroup. Only valid for the quantile chunks.
    shared_ptr<RowCollectionGroup> _pRowCollectionGroup;

public:
    /**
     * @param array               a GroupbyQuantileArray object.
     * @param attrID              which attribute from the source array to compute quantile on.
     * @param numQuantilesPlusOne the user-provided numQuantiles plus one (this is the number of quantile values to output).
     * @param pRowCollectionGroup a shared_ptr to a RowCollection object holding records from the input array.
     */
    GroupbyQuantileChunk(Array const& array, AttributeID attrID, size_t numQuantilesPlusOne,
            shared_ptr<RowCollectionGroup> pRowCollectionGroup) :
    _array(array),
    _firstPos(array.getArrayDesc().getDimensions().size()),
    _lastPos(array.getArrayDesc().getDimensions().size()),
    _attrID(attrID),
    _firstGroup(array.getArrayDesc().getDimensions().size()-1),
    _lastGroup(array.getArrayDesc().getDimensions().size()-1),
    _numQuantilesPlusOne(numQuantilesPlusOne),
    _pRowCollectionGroup(pRowCollectionGroup)
    {
    }

    virtual const Array& getArray() const
    {
        return _array;
    }

    virtual const ArrayDesc& getArrayDesc() const
    {
        return _array.getArrayDesc();
    }

    virtual const AttributeDesc& getAttributeDesc() const
    {
        return _array.getArrayDesc().getAttributes()[_attrID];
    }

    virtual Coordinates const& getFirstPosition(bool withOverlap) const
    {
        return _firstPos;
    }

    virtual Coordinates const& getLastPosition(bool withOverlap) const
    {
        return _lastPos;
    }

    Coordinates const& getFirstGroup() const
    {
        return _firstGroup;
    }

    Coordinates const& getLastGroup() const
    {
        return _lastGroup;
    }

    virtual boost::shared_ptr<ConstChunkIterator> getConstIterator(int iterationMode) const
    {
        return boost::shared_ptr<ConstChunkIterator>(new GroupbyQuantileChunkIterator(this, _attrID, iterationMode, _numQuantilesPlusOne));
    }

    virtual int getCompressionMethod() const
    {
        return CompressorFactory::NO_COMPRESSION;
    }

    void setPosition(Coordinates const& pos);
};

GroupbyQuantileChunkIterator::GroupbyQuantileChunkIterator(
                      GroupbyQuantileChunk const* chunk,
                      AttributeID attr,
                      int mode,
                      size_t numQuantilesPlusOne):
    _iterationMode(mode),
    _hasCurrent(false),
    _attrID(attr),
    _chunk(chunk),
    _value(TypeLibrary::getType(chunk->getAttributeDesc().getType())),
    _nullValue(TypeLibrary::getType(chunk->getAttributeDesc().getType())),
    _groupIterator(chunk->getFirstGroup(), chunk->getLastGroup()),
    _indexInCurrentGroup(0),
    _quantilesInCurrentGroup(NULL),
    _tmpPos(chunk->getArrayDesc().getDimensions().size()),
    _numQuantilesPlusOne(numQuantilesPlusOne)
{
    assert(numQuantilesPlusOne>1);
    _nullValue.setNull();
    reset();
}

void GroupbyQuantileChunkIterator::operator ++()
{
    if (!_hasCurrent) {
        throw USER_EXCEPTION(SCIDB_SE_UDO, SCIDB_LE_NO_CURRENT_ELEMENT);
    }
    if (_indexInCurrentGroup < _numQuantilesPlusOne-1) {
        ++ _indexInCurrentGroup;
        return;
    }

    // Move to the next group.
    _indexInCurrentGroup = 0;
    ++ _groupIterator;

    // If the next group does not exist, set _hasCurrent = NULL.
    if (_groupIterator.end()) {
        _hasCurrent = false;
        if (_attrID==1) { // quantile
            _quantilesInCurrentGroup = NULL;
        }
        return;
    }

    // Ok the next group exists. Try to set the _quantilesInCurrentGroup pointer.
    if (_attrID==1) {
        MapGroupToQuantile::const_iterator it = _chunk->_mapGroupToQuantile.find(_groupIterator.getPosition());
        if (it==_chunk->_mapGroupToQuantile.end()) {
            _quantilesInCurrentGroup = NULL;
        } else {
            _quantilesInCurrentGroup = &(it->second);
        }
    }
}

bool GroupbyQuantileChunkIterator::setPosition(Coordinates const& pos)
{
    assert(pos.size() == _tmpPos.size());
    Coordinates first = _chunk->getFirstPosition(false);
    Coordinates last = _chunk->getLastPosition(false);
    for (size_t i = 0; i<pos.size(); ++i)
    {
        if (pos[i] < first[i] || pos[i] > last[i])
        {
            return _hasCurrent = false;
        }
    }

    // Initialize the group iterator.
    Coordinates groupby(pos.size()-1);
    std::copy(pos.begin(), pos.end()-1, groupby.begin());
    _groupIterator.setPosition(groupby);

    // Location within the group.
    _indexInCurrentGroup = pos[pos.size()-1];

    // Is the current group NULL?
    if (_attrID==1) {
        MapGroupToQuantile::const_iterator it = _chunk->_mapGroupToQuantile.find(groupby);
        if (it==_chunk->_mapGroupToQuantile.end()) {
            _quantilesInCurrentGroup = NULL;
        } else {
            _quantilesInCurrentGroup = &(it->second);
        }
    }

    return _hasCurrent = true;
}

void GroupbyQuantileChunkIterator::reset()
{
    setPosition(_chunk->getFirstPosition(false));   // false - no overlap
}

ConstChunk const& GroupbyQuantileChunkIterator::getChunk()
{
    return *_chunk;
}

/**
 * GroupbyQuantileArrayIterator.
 * @note
 *   To iterate over the chunks in the local instance, the class uses RegionCoordinatesIterator to iterate over all the
 *   chunkPos in space.
 *
 * @note
 *   This class uses a RegionCoordinatesIterator to iterate over the chunkPos in the logical space.
 *   Per THE REQUEST TO JUSTIFY LOGICAL-SPACE ITERATION (see RegionCoordinatesIterator.h),
 *   we should tell why it is ok.
 *   The reason is the same as given in GroupbyQuantileChunkIterator.
 */
class GroupbyQuantileArrayIterator : public ConstArrayIterator
{
private:
    bool _hasCurrent;
    GroupbyQuantileChunk _chunk;
    size_t _instanceID;
    size_t _numInstances;

    // A temporary position, to be returned in getPosition().
    Coordinates _tmpPos;

    // An iterator that iterates over all the groups, regardless to what instance the group should belong to.
    scoped_ptr<RegionCoordinatesIterator> _regionCoordinatesIterator;

private:
    // Get the instanceID for a chunk.
    // The logic is a duplication of Operator.cpp::getInstanceForChunk.
    InstanceID getInstanceForGroup(const Coordinates& group) {
        return VectorHash<Coordinate>()(group) % _numInstances;
    }

    // Is a group local to the instance.
    bool isLocal(const Coordinates& group) {
        return getInstanceForGroup(group) == _instanceID;
    }

    // Keep incrementing _regionCoordinatesIterator until the next local group is reached, or until end() is reached.
    // @note    increment at least once.
    // @return  whether a new local group is reached.
    bool jumpToNextLocalGroup() {
        assert(_regionCoordinatesIterator);
        assert(! _regionCoordinatesIterator->end());

        while (true) {
            ++ (*_regionCoordinatesIterator);
            if (_regionCoordinatesIterator->end()) {
                return false;
            }
            if (isLocal(_regionCoordinatesIterator->getPosition())) {
                return true;
            }
        }
        return false;   // dummy code to avoid compile warning.
    }

public:
    GroupbyQuantileArrayIterator(Array const& array,
                          AttributeID attrID,
                          size_t numQuantilesPlusOne,
                          const shared_ptr<RowCollectionGroup>& pRowCollectionGroup,
                          size_t instanceID,
                          size_t numInstances
                          )
        : _chunk(array, attrID, numQuantilesPlusOne, pRowCollectionGroup),
          _instanceID(instanceID),
          _numInstances(numInstances),
          _tmpPos(array.getArrayDesc().getDimensions().size())
    {
        assert(numQuantilesPlusOne>1);

        // Computes parameter for RegionCoordinatesIterator.
        // The last dimension, i.e. the quantile dimesion, is not used.
        const Dimensions& dims = array.getArrayDesc().getDimensions();
        bool valid = true;
        assert(dims.size()>1);
        size_t nGroupDims = dims.size()-1;
        RegionCoordinatesIteratorParam param(nGroupDims);
        for (size_t i=0; i<nGroupDims; ++i) {
            param._low[i] = dims[i].getStartMin();
            param._high[i] = dims[i].getEndMax();
            if (param._low[i] > param._high[i]) {
                valid = false;
                break;
            }
            param._intervals[i] = dims[i].getChunkInterval();
        }

        if (valid) {
            _regionCoordinatesIterator.reset(new RegionCoordinatesIterator(param));
        }

        reset();
    }

    virtual ConstChunk const& getChunk()
    {
        if (!_hasCurrent) {
            throw USER_EXCEPTION(SCIDB_SE_UDO, SCIDB_LE_NO_CURRENT_ELEMENT);
        }
        _chunk.setPosition(getPosition());
        return _chunk;
    }

    virtual void reset()
    {
        if (!_regionCoordinatesIterator) {
            _hasCurrent = false;
            return;
        }

        _regionCoordinatesIterator->reset();
        assert(! _regionCoordinatesIterator->end());
        if (isLocal(_regionCoordinatesIterator->getPosition())) {
            _hasCurrent = true;
            return;
        }
        _hasCurrent = jumpToNextLocalGroup();
    }

    /*
     * setPosition.
     * Note: input may be in the middle of a chunk.
     */
    virtual bool setPosition(Coordinates const& pos)
    {
        if (!_regionCoordinatesIterator) {
            return false;
        }

        // Get the chunk position.
        Coordinates chunkPos = pos;
        _chunk._array.getArrayDesc().getChunkPositionFor(chunkPos);

        assert(chunkPos.size() == _tmpPos.size());
        assert(chunkPos.size() > 0);
        assert(chunkPos[chunkPos.size()-1] == 0);

        // Get the first n-1 dims as the group.
        Coordinates group(chunkPos.size()-1);
        std::copy(chunkPos.begin(), chunkPos.end()-1, group.begin());
        if (! isLocal(group)) {
            return false;
        }
        if (! _regionCoordinatesIterator->setPosition(group)) {
            return false;
        }

        return _hasCurrent = true;
    }

    virtual Coordinates const& getPosition()
    {
        assert(_regionCoordinatesIterator);
        assert(! _regionCoordinatesIterator->end());
        assert(_hasCurrent);
        Coordinates group = _regionCoordinatesIterator->getPosition();
        std::copy(group.begin(), group.end(), _tmpPos.begin());
        return _tmpPos;
    }

    virtual bool end()
    {
        return !_hasCurrent;
    }

    virtual void operator ++()
    {
        if (!_hasCurrent) {
            throw USER_EXCEPTION(SCIDB_SE_UDO, SCIDB_LE_NO_CURRENT_ELEMENT);
        }
        _hasCurrent = jumpToNextLocalGroup();
    }
};

/**
 * GroupbyQuantileArray.
 */
class GroupbyQuantileArray : public Array
{
private:
    ArrayDesc _desc;
    size_t _numQuantilesPlusOne;
    shared_ptr<RowCollectionGroup> _pRowCollectionGroup;
    QueryID _queryID;

public:
    // The mutex is used to synchronize GroupbyQuantileChunk::setPosition()
    scidb::Mutex _mutexChunkSetPosition;

public:
    GroupbyQuantileArray(ArrayDesc const& desc, shared_ptr<Query>& query, size_t numQuantilesPlusOne, shared_ptr<RowCollectionGroup>& pRowCollectionGroup):
          _desc(desc), _numQuantilesPlusOne(numQuantilesPlusOne), _pRowCollectionGroup(pRowCollectionGroup), _queryID(query->getQueryID())
    {
        assert(numQuantilesPlusOne>1);
        _query=query;
    }

    virtual boost::shared_ptr<ConstArrayIterator> getConstIterator(AttributeID attr) const
    {
        shared_ptr<Query> query(Query::getValidQueryPtr(_query));

        return boost::shared_ptr<ConstArrayIterator>(
                new GroupbyQuantileArrayIterator(*this, attr, _numQuantilesPlusOne, _pRowCollectionGroup,
                        query->getInstanceID(), query->getInstancesCount()));
    }

    virtual ArrayDesc const& getArrayDesc() const
    {
        return _desc;
    }

};

void GroupbyQuantileChunk::setPosition(Coordinates const& pos)
{
    // Different threads need to be synchronized in calling chunk.setPosition(), because the shared _pRowCollectionGroup is modified.
    ScopedMutexLock lock(((GroupbyQuantileArray*)&_array)->_mutexChunkSetPosition);

    if (materializedChunk) {
        delete materializedChunk;
        materializedChunk = NULL;
    }
    _firstPos = pos;
    Dimensions const& dims = _array.getArrayDesc().getDimensions();
    for (size_t i = 0, n = dims.size(); i < n; i++)
    {
        _lastPos[i] = _firstPos[i] + dims[i].getChunkInterval() - 1;

        // Make sure lastPos does not exceed endMax.
        // Note that using currEnd here would be a mistake before we make quantile generate an emptyable array.
        // The reason is that, if we reduce lastPos, quantile() will generate results for the end-of-chunk rows wrong.
        if (_lastPos[i] > dims[i].getEndMax())
        {
            _lastPos[i] = dims[i].getEndMax();
        }
    }

    // Clear the map.
    _mapGroupToQuantile.clear();

    // Set the first & last groups.
    std::copy(_firstPos.begin(), _firstPos.end()-1, _firstGroup.begin());
    std::copy(_lastPos.begin(), _lastPos.end()-1, _lastGroup.begin());

    // If this chunk is not for the quantiles, just return.
    if (_attrID!=1) {
        return;
    }

    // Ok this chunk is for the quantiles.
    // For each group, read from the RowCollectionGroup, sort, and generate quantiles.
    TypeId typeID = _array.getArrayDesc().getAttributes()[_attrID].getType();
    CompareValueVectorsByOneValue compareValueVectors(0, typeID); // In items, 0 is the attribute ID for values.

    vector<Value> quantileOneGroup(_numQuantilesPlusOne);
    RegionCoordinatesIterator it(_firstGroup, _lastGroup);
    while (!it.end()) {
        Coordinates group = it.getPosition();
        if (_pRowCollectionGroup->existsGroup(group)) {
            // Get non-NULL items of the row.
            size_t rowID = _pRowCollectionGroup->rowIdFromExistingGroup(group);
            vector<vector<Value> > items;
            _pRowCollectionGroup->getWholeRow(rowID, items, true, 0, NULL); // true - separate NULL cells; 0 - attribute 0; NULL - discard the NULL items.

            // If all values are NULL, skip this group.
            if (items.size()==0) {
                ++it;
                continue;
            }

            // Sort the row.
            iqsort(&items[0], items.size(), compareValueVectors);

            // Get the quantile values.
            for (size_t i=0; i<_numQuantilesPlusOne; ++i) {
                size_t index = ceil( i * 1.0 * items.size() / (_numQuantilesPlusOne - 1) );
                index = index < 1 ? 1 : index;

                quantileOneGroup[i] = items[index-1][0];
            }

            // Push to map.
            _mapGroupToQuantile.insert(std::pair<Coordinates, vector<Value> >(group, quantileOneGroup));
        }
        ++it;
    } // end while (!it.end())
}

/**
 * PhysicalQuantile.
 */
class PhysicalQuantile: public PhysicalOperator
{
  public:
    PhysicalQuantile(const std::string& logicalName, const std::string& physicalName, const Parameters& parameters, const ArrayDesc& schema):
        PhysicalOperator(logicalName, physicalName, parameters, schema)
    {
    }

    virtual bool changesDistribution(std::vector<ArrayDesc> const&) const
    {
        return true;
    }

    virtual ArrayDistribution getOutputDistribution(const std::vector<ArrayDistribution> & inputDistributions,
                                                 const std::vector< ArrayDesc> & inputSchemas) const
    {
        return ArrayDistribution(psUndefined);
    }

    /**
     * We require that input is distributed round-robin so that our parallel trick works.
     */
    virtual DistributionRequirement getDistributionRequirement(const std::vector<ArrayDesc> & inputSchemas) const
    {
        vector<ArrayDistribution> requiredDistribution;
        requiredDistribution.push_back(ArrayDistribution(psHashPartitioned));
        return DistributionRequirement(DistributionRequirement::SpecificAnyOrder, requiredDistribution);
    }

    void fillQuantiles (shared_ptr<Array> rankings, shared_ptr < QuantileBucketsMap > buckets, DimensionGrouping const& grouping)
    {
        size_t qDim = _schema.getDimensions().size() -1;
        DimensionDesc quantileDimension = _schema.getDimensions()[qDim];

        shared_ptr<ConstArrayIterator> rankArrayIterator = rankings->getConstIterator(1);
        shared_ptr<ConstItemIterator> valueItemIterator = rankings->getItemIterator(0);
        size_t numQuantilesPlusOne = quantileDimension.getEndMax() - quantileDimension.getStartMin() + 1;

        while(! rankArrayIterator->end() )
        {
            boost::shared_ptr<ConstChunkIterator> rankChunkIterator = rankArrayIterator->getChunk().getConstIterator();
            while (! rankChunkIterator->end())
            {
                Coordinates const& pos = rankChunkIterator->getPosition();
                Coordinates reduced = grouping.reduceToGroup(pos);

                if (buckets->count(reduced) != 0)    // have a bucket
                {
                    QuantileBucket &bucket = (*buckets)[reduced];
                    if (bucket.values.size() == 0)  // have a brand new bucket
                    {
                        size_t count = bucket.indeces[0];
                        bucket.indeces=vector<double>(numQuantilesPlusOne);
                        bucket.maxIndeces=vector<double>(numQuantilesPlusOne);
                        bucket.values=vector<Value>(numQuantilesPlusOne);

                        for (size_t i =0; i<numQuantilesPlusOne; i++)
                        {
                            double index = ceil( i * 1.0 * count / (numQuantilesPlusOne - 1) );
                            index = index < 1 ? 1 : index;
                            bucket.indeces[i] = index;
                            bucket.maxIndeces[i] = 0;
                        }
                    }

                    Value rv = rankChunkIterator->getItem();
                    if (!rv.isNull())
                    {
                        double ranking = rv.getDouble();
                        for (size_t i =0; i<bucket.indeces.size(); i++)
                        {
                            if (ranking <= bucket.indeces[i] && ranking > bucket.maxIndeces[i])
                            {
                                valueItemIterator->setPosition(pos);
                                bucket.values[i] = valueItemIterator->getItem();
                                bucket.maxIndeces[i] = ranking;
                            }
                        }
                    }
                }
                ++(*rankChunkIterator);
            }
           ++(*rankArrayIterator);
        }
    }

  public:
    /**
     * execute().
     */
    boost::shared_ptr<Array> execute(std::vector< boost::shared_ptr<Array> >& inputArrays, boost::shared_ptr<Query> query)
    {
        shared_ptr<Array> inputArray = inputArrays[0];
        if (inputArray->getSupportedAccess() == Array::SINGLE_PASS)
        {   //if input supports MULTI_PASS, don't bother converting it
            inputArray = ensureRandomAccess(inputArray, query);
        }

        // timing
        LOG4CXX_DEBUG(logger, "[Quantile] Begins.");
        ElapsedMilliSeconds timing;

        const ArrayDesc& inputSchema = inputArray->getArrayDesc();
        const Attributes& inputAttributes = inputSchema.getAttributes();
        const Dimensions& inputDims = inputSchema.getDimensions();

        // _parameters[0] is numQuantilesPlusOne.
        // _parameters[1], if exists, is the value to compute quantile on.
        // _parameters[2...], if exists, are the groupby dimensions
        string attName = _parameters.size() > 1 ? ((boost::shared_ptr<OperatorParamReference>&)_parameters[1])->getObjectName() :
                                                    inputAttributes[0].getName();

        AttributeID rankedAttributeID = 0;
        for (size_t i =0 ; i< inputAttributes.size(); i++)
        {
            if (inputAttributes[i].getName() == attName)
            {
                rankedAttributeID = inputAttributes[i].getId();
                break;
            }
        }

        // If inputDims = [d0, d1, d2, d3]
        // and groupBy = [d3, d1],
        // we have:
        // groupByDimIDs = [3, 1]
        Dimensions groupBy;
        vector<size_t> groupByDimIDs;
        if (_parameters.size() > 2)
        {
            size_t i, j;
            for (i = 0; i < _parameters.size()-2; i++) {
               const string& dimName = ((boost::shared_ptr<OperatorParamReference>&)_parameters[i + 2])->getObjectName();
               const string& dimAlias = ((boost::shared_ptr<OperatorParamReference>&)_parameters[i + 2])->getArrayName();
               for (j = 0; j < inputDims.size(); j++) {
                   if (inputDims[j].hasNameAndAlias(dimName, dimAlias)) {
                       groupBy.push_back(inputDims[j]);
                       groupByDimIDs.push_back(j);
                       break;
                   }
               }
               assert(j < inputDims.size());
            }
        }

        // For every dimension, determine whether it is a groupby dimension
        shared_ptr<PartitioningSchemaDataGroupby> psdGroupby = make_shared<PartitioningSchemaDataGroupby>();
        psdGroupby->_arrIsGroupbyDim.reserve(inputDims.size());
        for (size_t i=0; i<inputDims.size(); ++i)
        {
            bool isGroupbyDim = false;
            for (size_t j=0; j<groupBy.size(); ++j) {
                if (inputDims[i].getBaseName() == groupBy[j].getBaseName()) {
                    isGroupbyDim = true;
                    break;
                }
            }

            psdGroupby->_arrIsGroupbyDim.push_back(isGroupbyDim);
        }

        // If this is not a groupby quantile, use the original code.
        if (groupBy.size()==0) {
            // timing
            LOG4CXX_DEBUG(logger, "[Quantile] Using the original algorithm, because this is not a group-by quantile.");

            shared_ptr<DimensionGrouping> grouping ( new DimensionGrouping(inputArray->getArrayDesc().getDimensions(), groupBy));
            shared_ptr<RankingStats> rStats (new RankingStats());
            shared_ptr<Array> rankArray(buildRankArray(inputArray, rankedAttributeID, groupBy, query, rStats));

            const size_t nInstances = query->getInstancesCount();
            InstanceID myInstance = query->getInstanceID();

            if (nInstances > 1)
            {
                rankArray = redistributeToRandomAccess(rankArray,query,psHashPartitioned,
                                                       ALL_INSTANCE_MASK,
                                                       shared_ptr<DistributionMapper>(),
                                                       0,
                                                       shared_ptr<PartitioningSchemaData>());

            }
            else
            {
                //we don't need to redistribute the rank array but we still need to pass over it to collect max rankings
                shared_ptr<ConstArrayIterator> aiter = rankArray->getConstIterator(1);
                while (!aiter->end())
                {
                    shared_ptr<ConstChunkIterator> citer = aiter->getChunk().getConstIterator();
                    while(!citer->end())
                    {
                        citer->getItem();
                        ++(*citer);
                    }
                    ++(*aiter);
                }
            }
            LOG4CXX_DEBUG(logger, "Created prerank array");

            shared_ptr <QuantileBucketsMap> buckets(new QuantileBucketsMap);
            shared_ptr <set<size_t> > liveChunks(new set<size_t>);

            BOOST_FOREACH (CountsMap::value_type bucket, rStats->counts)
            {
                Coordinates const& pos = bucket.first;
                double count = bucket.second;

                if (pos.size())
                {
                    LOG4CXX_DEBUG(logger, "Bucket " << pos[0] << " count " << count);
                }
                else
                {
                    LOG4CXX_DEBUG(logger, "Bucket 0 maxRanking " << count);
                }

                Coordinates chunkCoords = pos;
                if (pos.size() > 1)
                {
                    chunkCoords.push_back(0);
                }
                boost::shared_ptr<DistributionMapper> distMapper;
                InstanceID instanceForChunk = getInstanceForChunk(query, chunkCoords, _schema, psHashPartitioned, distMapper, 0, 0);
                if(instanceForChunk == myInstance)
                {
                    LOG4CXX_DEBUG(logger, "Initializing bucket with "<<chunkCoords.size()<<" coords; count "<<count);
                    (*buckets)[chunkCoords].indeces.push_back(count);
                    (*buckets)[chunkCoords].maxIndeces = vector<double> ();
                    (*buckets)[chunkCoords].values = vector<Value> ();

                    size_t chunkNo = _schema.getHashedChunkNumber(chunkCoords);
                    liveChunks->insert(chunkNo);
                }
            }
            rStats.reset();

            fillQuantiles(rankArray, buckets, *grouping);
            for(size_t i =1 ; i < nInstances ; i++)
            {
                rankArray = redistributeToRandomAccess(rankArray,query,psHashPartitioned,
                                                       ALL_INSTANCE_MASK,
                                                       shared_ptr<DistributionMapper>(),
                                                       i,
                                                       shared_ptr<PartitioningSchemaData>());
                fillQuantiles(rankArray, buckets, *grouping);
            }
            rankArray.reset();

            shared_ptr<Array> result = shared_ptr<Array>(new QuantileArray( _schema, buckets, grouping, liveChunks));

            // timing
            timing.logTiming(logger, "[Quantile] original algorithm", false); // false = no restart
            LOG4CXX_DEBUG(logger, "[Quantile] finished!")

            return result;

        } // end if (groupBy.size()==0)

        //
        // Note: below is groupby-quantile.
        //
        LOG4CXX_DEBUG(logger, "[Quantile] Begin redistribution (first phase of group-by quantile).");

        // Extract just the ranking attribute
        AttributeDesc rankedAttribute = inputSchema.getAttributes()[rankedAttributeID];
        Attributes projectAttrs;
        projectAttrs.push_back(AttributeDesc(0, rankedAttribute.getName(), rankedAttribute.getType(), rankedAttribute.getFlags(), rankedAttribute.getDefaultCompressionMethod()));

        AttributeDesc const* emptyTag = inputSchema.getEmptyBitmapAttribute();
        if (emptyTag)
        {
            projectAttrs.push_back(AttributeDesc(1, emptyTag->getName(), emptyTag->getType(), emptyTag->getFlags(), emptyTag->getDefaultCompressionMethod()));
        }

        Dimensions projectDims(inputDims.size());
        for (size_t i = 0, n = inputDims.size(); i < n; i++)
        {
            DimensionDesc const& srcDim = inputDims[i];
            projectDims[i] = DimensionDesc(srcDim.getBaseName(),
                                       srcDim.getNamesAndAliases(),
                                       srcDim.getStartMin(),
                                       srcDim.getCurrStart(),
                                       srcDim.getCurrEnd(),
                                       srcDim.getEndMax(),
                                       srcDim.getChunkInterval(),
                                       0);
        }

        ArrayDesc projectSchema(inputSchema.getName(), projectAttrs, projectDims);

        vector<AttributeID> projection(1);
        projection[0] = rankedAttributeID;

        shared_ptr<Array> projected(make_shared<SimpleProjectArray>(projectSchema, inputArray, projection));

        // Redistribute, s.t. all records in the same group go to the same instance.
        boost::shared_ptr<DistributionMapper> distMapper;
        shared_ptr<Array> redistributed = redistributeToRandomAccess(projected, query, psGroupby,
                                                                     ALL_INSTANCE_MASK,
                                                                     distMapper,
                                                                     0,
                                                                     psdGroupby);

        // timing
        timing.logTiming(logger, "[Quantile] redistribute()");
        LOG4CXX_DEBUG(logger, "[Quantile] Begin reading input array and appending to rcGroup, reporting a timing every 10 chunks.");

        // Build a RowCollection, where each row is a group.
        // There is a single attribute: the ranked attribute.
        Attributes rcGroupAttrs;
        rcGroupAttrs.push_back(AttributeDesc(0, rankedAttribute.getName(), rankedAttribute.getType(), rankedAttribute.getFlags(), rankedAttribute.getDefaultCompressionMethod()));
        shared_ptr<RowCollectionGroup> rcGroup = make_shared<RowCollectionGroup>(query, "", rcGroupAttrs);

        boost::shared_ptr<ConstArrayIterator> srcArrayIterValue = redistributed->getConstIterator(0);
        vector<Value> itemInRowCollectionGroup(1);
        Coordinates group(groupBy.size());
        size_t totalItems = 0;
        size_t chunkID = 0;
        size_t itemID = 0;

        size_t reportATimingAfterHowManyChunks = 10; // report a timing after how many chunks? Report progress, without too many lines.
        while (!srcArrayIterValue->end()) {
            const ConstChunk& chunk = srcArrayIterValue->getChunk();
            boost::shared_ptr<ConstChunkIterator> srcChunkIter = chunk.getConstIterator();
            while (!srcChunkIter->end()) {
                const Coordinates& full = srcChunkIter->getPosition();
                for (size_t i=0; i<groupBy.size(); ++i) {
                    group[i] = full[groupByDimIDs[i]];
                }

                itemInRowCollectionGroup[0] = srcChunkIter->getItem();
                size_t resultRowId = UNKNOWN_ROW_ID;
                rcGroup->appendItem(resultRowId, group, itemInRowCollectionGroup);

                ++(*srcChunkIter);
                ++ itemID;
            }

            ++chunkID;
            totalItems += itemID;
            itemID = 0;
            ++(*srcArrayIterValue);

            // timing
            if (logger->isDebugEnabled() && chunkID % reportATimingAfterHowManyChunks==0) {
                char buf[100];
                sprintf(buf, "[Quantile] reading %lu chunks and %lu items", chunkID, totalItems);
                timing.logTiming(logger, buf, false);
                if (chunkID==100) {
                    reportATimingAfterHowManyChunks = 100;
                    LOG4CXX_DEBUG(logger, "[Quantile] Now reporting a number after 100 chunks.");
                } else if (chunkID==1000) {
                    reportATimingAfterHowManyChunks = 1000;
                    LOG4CXX_DEBUG(logger, "[Quantile] Now reporting a number after 1000 chunks.");
                }
            }
        }
        rcGroup->switchMode(RowCollectionModeRead);

        // timing
        if (logger->isDebugEnabled()) {
            char buf[100];
            sprintf(buf, "[Quantile] overall, reading %lu chunks and %lu items", chunkID, totalItems);
            timing.logTiming(logger, buf, false);
        }

        // Build a GroupbyQuantileArray.
        size_t qDim = _schema.getDimensions().size() -1;
        DimensionDesc quantileDimension = _schema.getDimensions()[qDim];
        size_t numQuantilesPlusOne = quantileDimension.getChunkInterval();
        shared_ptr<Array> result = shared_ptr<Array>(
                new GroupbyQuantileArray(_schema, query, numQuantilesPlusOne, rcGroup) );
        return result;
    }
};

DECLARE_PHYSICAL_OPERATOR_FACTORY(PhysicalQuantile, "quantile", "physicalQuantile")

}
