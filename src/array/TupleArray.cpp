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
 * @file TupleArray.cpp
 */
#include <boost/foreach.hpp>

#include <util/iqsort.h>
#include <array/TupleArray.h>
#include <system/Exceptions.h>
#include <query/Expression.h>
#include <query/FunctionDescription.h>
#include <query/Operator.h>
#include <util/ArrayCoordinatesMapper.h>

namespace scidb
{
using namespace boost;
using namespace std;
using namespace arena;

//
// Tuple comparator
//
int TupleComparator::compare(const Value* t1, const Value* t2) const
{
    assert(t1!=0 && t2!=0);

    for (size_t i = 0, n = _sortingAttributeInfos.size(); i < n; i++) {
        int oneAttributeResult = compareOneAttribute(t1, t2, i);
        if (oneAttributeResult != 0) {
            return oneAttributeResult;
        }
    }
    return 0;
}

int TupleComparator::compareOneAttribute(const Value* t1, const Value* t2, size_t i) const
{
    assert(t1!=0 && t2!=0);
    assert(i < _sortingAttributeInfos.size());

    const int j = _sortingAttributeInfos[i].columnNo;
    const Value* args[2];
    NullNanRegular what1 = getNullNanRegular(t1[j], _types[i]);
    NullNanRegular what2 = getNullNanRegular(t2[j], _types[i]);

    // If the two values are in the same "category".
    if (what1==what2) {
        // Two nulls: compare their missing code.
        if (what1==NULL_VALUE) {
            int32_t missingCode1 = t1[j].getMissingReason();
            int32_t missingCode2 = t2[j].getMissingReason();
            if (missingCode1 == missingCode2) {
                return 0;
            }
            int32_t result = missingCode1 - missingCode2;
            if (! _sortingAttributeInfos[i].ascent) {
                result = - result;
            }
            return result;
        }

        // Two NaN values, regard them equal.
        else if (what1==NAN_VALUE) {
            return 0;
        }

        // Two regular values, use _equFunctions[i] and _leFunctions[i] to compare.
        args[0] = &t1[j];
        args[1] = &t2[j];
        Value res;

        _eqFunctions[i](&args[0], &res, NULL);
        if (!res.getBool()) {
            _leFunctions[i](&args[0], &res, NULL);
            return (res.getBool()) ? (_sortingAttributeInfos[i].ascent ? -1 : 1) : (_sortingAttributeInfos[i].ascent ? 1 : -1);
        }

        return 0;
    }

    // If they are in different 'categories', null < nan < regular.
    int result = what1-what2;
    if (! _sortingAttributeInfos[i].ascent) {
        result = - result;
    }
    return result;
}

TupleComparator::TupleComparator(PointerRange<const SortingAttributeInfo> sortingAttributeInfos, const ArrayDesc& arrayDesc)
              : _sortingAttributeInfos(sortingAttributeInfos.begin(), sortingAttributeInfos.end()),
                _arrayDesc(arrayDesc),
                _leFunctions(sortingAttributeInfos.size()),
                _eqFunctions(sortingAttributeInfos.size()),
                _types(sortingAttributeInfos.size())
{
    for (size_t i = 0; i < _sortingAttributeInfos.size(); i++)
    {
        vector<TypeId> argTypes(2, _arrayDesc.getAttributes()[_sortingAttributeInfos[i].columnNo].getType());
        FunctionDescription functionDesc;
        vector<FunctionPointer> converters;
        if (!FunctionLibrary::getInstance()->findFunction("<", argTypes, functionDesc, converters, false))
            throw USER_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_OPERATOR_NOT_FOUND) << "<" << argTypes[0];
        _leFunctions[i] = functionDesc.getFuncPtr();
        if (!FunctionLibrary::getInstance()->findFunction("=", argTypes, functionDesc, converters, false))
            throw USER_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_OPERATOR_NOT_FOUND) << "=" << argTypes[0];
        _eqFunctions[i] = functionDesc.getFuncPtr();

        const int j = _sortingAttributeInfos[i].columnNo;
        TypeId strType = _arrayDesc.getAttributes()[j].getType();
        _types[i] = getDoubleFloatOther(strType);
    }
}

//
// TupleArray
//
void TupleArray::sort(shared_ptr<TupleComparator> tcomp)
{
    iqsort(&_tuples.front(), _tuples.size(), *tcomp);
}

ArrayDesc const& TupleArray::getArrayDesc() const
{
    return _desc;
}

boost::shared_ptr<ConstArrayIterator> TupleArray::getConstIterator(AttributeID attId) const
{
    return make_shared<TupleArrayIterator>(*this, attId);
}

TupleArray::TupleArray(ArrayDesc const& schema, arena::ArenaPtr const& parentArena,Coordinate offset)
          :   _arena(arena::newArena(arena::Options("TupleArrayValues").parent(parentArena).resetting(true))),
              _desc(schema),
              _start(schema.getDimensions()[0].getStartMin() + offset),
              _end(_start - 1),
              _tuples(_arena),
              _chunkSize(schema.getDimensions()[0].getChunkInterval()),
              _preservePositions(false)
{
    _desc.cutOverlap();
}

TupleArray::TupleArray(ArrayDesc const& outputSchema,
                       vector< boost::shared_ptr<ConstArrayIterator> > const& arrayIterators,
                       ArrayDesc const& inputSchema,
                       size_t nChunks,
                       size_t sizeHint,
                       size_t pageSize,
                       arena::ArenaPtr const& parentArena,
                       bool preservePositions)
    : _arena(arena::newArena(arena::Options("TupleArrayValues").parent(parentArena).resetting(true).pagesize(pageSize))),
      _desc(outputSchema),
      _start(_desc.getDimensions()[0].getStartMin()),
      _end(_desc.getDimensions()[0].getEndMax()),
      _tuples(_arena),
      _chunkSize(_desc.getDimensions()[0].getChunkInterval()),
      _preservePositions(preservePositions)
{
    _tuples.reserve(sizeHint);
    if (_desc.getDimensions().size() != 1)
        throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_MULTIDIMENSIONAL_ARRAY_NOT_ALLOWED);
    append(inputSchema, arrayIterators, nChunks);
    if (_start == MIN_COORDINATE || _end == MAX_COORDINATE) {
        _start = 0;
        _end = _tuples.size()-1;
    } else if (Coordinate(_start + _tuples.size()) <= _end) {
        _end = _start + _tuples.size() - 1;
    }
}

TupleArray::~TupleArray()
{
    size_t n = getTupleArity();                          // Vals in each tuple

    BOOST_REVERSE_FOREACH(Value* t, _tuples)             // For each tuple...
    {
        destroy(*_arena, t, n);                          // ...destroy manually
    }
}

void TupleArray::truncate()
{
    Dimensions newDims(1);
    DimensionDesc const& oldDim = _desc.getDimensions()[0];
    newDims[0] = DimensionDesc(oldDim.getBaseName(),
                               oldDim.getNamesAndAliases(),
                               oldDim.getStartMin(),
                               oldDim.getStartMin(),
                               oldDim.getStartMin() + _tuples.size() - 1,
                               oldDim.getStartMin() + _tuples.size() - 1,
                               oldDim.getChunkInterval(),
                               0);
    _desc = ArrayDesc(_desc.getName(), _desc.getAttributes(), newDims);
    _start = newDims[0].getStartMin();
    _end = newDims[0].getEndMax();
}

size_t TupleArray::getTupleFootprint(Attributes const& attrs)
{
   size_t res = 0;
   size_t n = attrs.size();
   for (size_t i=0; i<n; i++)
   {
       size_t attSize = attrs[i].getSize();
       if (attSize == 0)
       {   //variable size
           attSize = Config::getInstance()->getOption<int>(CONFIG_STRING_SIZE_ESTIMATION);
       }
       res += Value::getFootprint(attSize);
   }
   return res + sizeof(Value*);
}

void TupleArray::append(boost::shared_ptr<Array> const& inputArray)
{
    ArrayDesc const& inputSchema = inputArray->getArrayDesc();
    const bool excludingEmptyBitmap = true;
    const size_t nAttrs = inputSchema.getAttributes(excludingEmptyBitmap).size();
    vector< boost::shared_ptr<ConstArrayIterator> > arrayIterators(nAttrs);
    for (size_t i = 0; i < nAttrs; i++) {
        arrayIterators[i] = inputArray->getConstIterator(i);
    }
    append(inputArray->getArrayDesc(), arrayIterators, (size_t)-1);
}

void TupleArray::append(ArrayDesc const& inputSchema, vector< boost::shared_ptr<ConstArrayIterator> > const& arrayIterators, size_t nChunks)
{
    size_t nAttrsOut = getTupleArity();
    size_t nAttrsIn = arrayIterators.size();

    const unsigned CHUNK_FLAGS =
        ConstChunkIterator::IGNORE_EMPTY_CELLS |
        ConstChunkIterator::IGNORE_OVERLAPS;

    // Use an ArrayCoordinatesMapper to turn chunkPos and cellPos to integers.
    ArrayCoordinatesMapper arrayCoordinatesMapper(inputSchema.getDimensions());

    // The AttributeKind of each attribute.
    vector<AttributeKind> attrKinds(nAttrsOut);
    for (size_t i=0; i<nAttrsOut; ++i) {
        attrKinds[i] = getAttributeKind(i);
    }

    vector< boost::shared_ptr<ConstChunkIterator> > chunkIterators(nAttrsIn);
    Coordinates lows(inputSchema.getDimensions().size());
    Coordinates intervals(inputSchema.getDimensions().size());

    while (nChunks-- != 0 && !arrayIterators[0]->end()) {
        for (size_t i = 0; i < nAttrsIn; i++) {
            chunkIterators[i] = arrayIterators[i]->getChunk().getConstIterator(CHUNK_FLAGS);
        }

        // Compute a low and interval from a chunkPos
        position_t posForChunk = 0;  //
        CoordinateCRange chunkPos = arrayIterators[0]->getPosition();

        if (_preservePositions) {
            posForChunk = arrayCoordinatesMapper.chunkPos2pos(chunkPos);
            arrayCoordinatesMapper.chunkPos2LowsAndIntervals(chunkPos, lows, intervals);
        }

        while (!chunkIterators[0]->end()) {
            assert(! chunkIterators[0]->isEmpty());

            CoordinateCRange cellPos = chunkIterators[0]->getPosition();
            Value* tuple = newVector<Value>(*_arena, nAttrsOut, manual);
            _tuples.push_back(tuple);
            for (size_t i = 0; i < nAttrsOut; i++) {
                switch (attrKinds[i]) {
                case FROM_INPUT_ARRAY:
                    tuple[i] =  chunkIterators[i]->getItem();
                    ++(*chunkIterators[i]);
                    break;
                case POS_FOR_CHUNK:
                    tuple[i].set<int64_t>(posForChunk);
                    break;
                case POS_FOR_CELL:
                    tuple[i].set<int64_t>(arrayCoordinatesMapper.coord2posWithLowsAndIntervals(lows, intervals, cellPos));
                    break;
                case EMPTY_BITMAP:
                    tuple[i].set<bool>(true);
                    break;
                default:
                    assert(false);
                    break;
                }
            }
        }
        for (size_t i = 0; i < nAttrsIn; i++) {
            ++(*arrayIterators[i]);
        }
    }
}

/**
 *  Append a single tuple to the array...
 */
void TupleArray::appendTuple(PointerRange<const Value> tuple)
{
    if (getTupleArity() != tuple.size())
    {
        throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_ATTRIBUTES_MISMATCH);
    }

    Value* t = newVector<Value>(*_arena, tuple.size(), manual);
    _tuples.push_back(t);
    std::copy(tuple.begin(), tuple.end(), t);
    ++_end;
}

TupleArray::AttributeKind TupleArray::getAttributeKind(AttributeID attrID) const
{
    size_t nAttrs = getTupleArity();
    assert(attrID < nAttrs);
    ASSERT_EXCEPTION(_desc.getEmptyBitmapAttribute() != NULL, "TupleArray must have an empty bitmap.");

    if (attrID + 1 == nAttrs) {
        return EMPTY_BITMAP;
    }
    else if (_preservePositions) {
        assert(nAttrs >= 4);  // One (or more) data attribute, pos for chunk, pos for cell, empty bitmap.
        if (attrID+2 == nAttrs) {
            return POS_FOR_CELL;
        }
        else if (attrID+3 == nAttrs) {
            return POS_FOR_CHUNK;
        }
    }
    return FROM_INPUT_ARRAY;
}

//
// TupleArrayIterator
//
ConstChunk const& TupleArrayIterator::getChunk()
{
    if (!_hasCurrent)
        throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_NO_CURRENT_CHUNK);
    _chunk._lastPos = _chunk._firstPos = _currPos;
    if ((_chunk._lastPos[0] += _array._chunkSize - 1) > _array._end) {
        _chunk._lastPos[0] = _array._end;
    }
    return _chunk;
}

bool TupleArrayIterator::end()
{
    return !_hasCurrent;
}

void TupleArrayIterator::operator ++()
{
    if (!_hasCurrent)
        throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_NO_CURRENT_CHUNK);
    _currPos[0] += _array._chunkSize;
    _hasCurrent = (_currPos[0] <= _array._end);
}

Coordinates const& TupleArrayIterator::getPosition()
{
    if (!_hasCurrent)
        throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_NO_CURRENT_CHUNK);
    return _currPos;
}

bool TupleArrayIterator::setPosition(Coordinates const& pos)
{
    if (pos[0] >= _array._start && pos[0] <= _array._end) {
        _currPos[0] = pos[0] - ((pos[0] - _array._start) % _array._chunkSize);
        _hasCurrent = true;
    } else {
        _hasCurrent = false;
    }
    return _hasCurrent;
}

void TupleArrayIterator::reset()
{
    _currPos[0] = _array._start;
    _hasCurrent = _currPos[0] <= _array._end;
}

TupleArrayIterator::TupleArrayIterator(TupleArray const& arr, AttributeID att)
: _array(arr), _attrID(att), _chunk(arr, att), _currPos(1)
{
    reset();
}

//
// Tuple chunk
//

const ArrayDesc& TupleChunk::getArrayDesc() const
{
    return _array._desc;
}

const AttributeDesc& TupleChunk::getAttributeDesc() const
{
    return _array._desc.getAttributes()[_attrID];
}

int TupleChunk::getCompressionMethod() const
{
    return getAttributeDesc().getDefaultCompressionMethod();
}

Coordinates const& TupleChunk::getFirstPosition(bool withOverlap) const
{
    return _firstPos;
}

Coordinates const& TupleChunk::getLastPosition(bool withOverlap) const
{
    return _lastPos;
}

boost::shared_ptr<ConstChunkIterator> TupleChunk::getConstIterator(int iterationMode) const
{
    return boost::make_shared<TupleChunkIterator>(*this, iterationMode);
}

TupleChunk::TupleChunk(TupleArray const& arr, AttributeID att)
: _array(arr), _attrID(att), _firstPos(1), _lastPos(1)
{
}

//
// TupleChunkIterator
//
int TupleChunkIterator::getMode()
{
    return _mode;
}

Value& TupleChunkIterator::getItem()
{
    if (_i > _last)
        throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_NO_CURRENT_CHUNK);
    return _array._tuples[_i][_attrID];
}

bool TupleChunkIterator::isEmpty()
{
    if (_i > _last)
        throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_NO_CURRENT_CHUNK);
    return !_array._tuples[_i];
}

bool TupleChunkIterator::end()
{
    return _i > _last;
}

inline bool TupleChunkIterator::isVisible() const
{
   return !((_mode & IGNORE_NULL_VALUES) && _array._tuples[_i][_attrID].isNull());
}

void TupleChunkIterator::operator ++()
{
    if (_i > _last)
        throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_NO_CURRENT_CHUNK);
    while (++_i <= _last && !isVisible())
        ;
}

Coordinates const& TupleChunkIterator::getPosition()
{
    if (_i > _last)
        throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_NO_CURRENT_CHUNK);
    _currPos[0] = _i + _array._start;
    return _currPos;
}

bool TupleChunkIterator::setPosition(Coordinates const& pos)
{
    if (pos[0] < _chunk._firstPos[0] || pos[0] > _chunk._lastPos[0]) {
        return false;
    }
    _i = size_t(pos[0] - _array._start);
    return isVisible();
}

void TupleChunkIterator::reset()
{
    for (_i = _chunk._firstPos[0] - _array._start; _i <= _last && !isVisible(); _i++);
}

ConstChunk const& TupleChunkIterator::getChunk()
{
    return _chunk;
}

TupleChunkIterator::TupleChunkIterator(TupleChunk const& aChunk, int iterationMode)
: _chunk(aChunk),
  _array(aChunk._array),
  _attrID(aChunk._attrID),
  _currPos(1),
  _last(size_t(_chunk._lastPos[0] - _array._start)),
  _mode(iterationMode)
{
    reset();
}

} // namespace
