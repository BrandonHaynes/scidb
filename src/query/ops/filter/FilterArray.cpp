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
 * FilterArray.cpp
 *
 *  Created on: Apr 11, 2010
 *      Author: Knizhnik
 */

#include <boost/make_shared.hpp>

#include "query/Operator.h"
#include "array/Metadata.h"
#include "array/Array.h"
#include "query/ops/filter/FilterArray.h"
#include "system/Config.h"
#include "system/SciDBConfigOptions.h"


using namespace std;
using namespace boost;

//#define FILTER_CHUNK_CACHE true

namespace scidb {

    //
    // Filter chunk iterator methods
    //
    inline Value& FilterChunkIterator::evaluate()
    {

        for (size_t i = 0, n = _array.bindings.size(); i < n; i++) {
            switch (_array.bindings[i].kind) {
                case BindInfo::BI_ATTRIBUTE:
                    _params[i] = _iterators[i]->getItem();
                    break;

                case BindInfo::BI_COORDINATE:
                    if (_mode & TILE_MODE) {
                        _iterators[i]->getItem().getTile()->getCoordinates(_array.getInputArray()->getArrayDesc(), _array.bindings[i].resolvedId, _iterators[i]->getChunk().getFirstPosition(false), _iterators[i]->getPosition(), _query, _params[i], !(_mode & IGNORE_OVERLAPS));
                    } else {
                        _params[i].setInt64(inputIterator->getPosition()[_array.bindings[i].resolvedId]);
                    }
                    break;

                default:
                    break;
            }
        }
        return (Value&)_array.expression->evaluate(_params);
    }

    inline bool FilterChunkIterator::filter()
    {
        Value const& result = evaluate();
        return !result.isNull() && result.getBool();
    }

    void FilterChunkIterator::moveNext()
    {
        ++(*inputIterator);
        if (!inputIterator->end()) {
            for (size_t i = 0, n = _iterators.size(); i < n; i++) {
                if (_iterators[i] && _iterators[i] != inputIterator) {
                    ++(*_iterators[i]);
                }
            }
        }
    }

    void FilterChunkIterator::nextVisible()
    {
        while (!inputIterator->end()) {
            if ((_mode & TILE_MODE) || filter()) {
                _hasCurrent = true;
                return;
            }
            moveNext();
        }
        _hasCurrent = false;
    }

    void FilterChunkIterator::reset()
    {
        inputIterator->reset();
        if (!inputIterator->end()) {
            for (size_t i = 0, n = _iterators.size(); i < n; i++) {
                if (_iterators[i] && _iterators[i] != inputIterator) {
                    _iterators[i]->reset();
                }
            }
        }
        nextVisible();
    }

    bool FilterChunkIterator::end()
    {
        return !_hasCurrent;
    }

    Value& FilterChunkIterator::getItem()
    {
        if (_mode & TILE_MODE) {
            RLEPayload* newEmptyBitmap = evaluate().getTile();
            RLEPayload::iterator ei(newEmptyBitmap);
            Value& value = inputIterator->getItem();
            RLEPayload* inputPayload = value.getTile();
            RLEPayload::iterator vi(inputPayload);

            if (newEmptyBitmap->count() == INFINITE_LENGTH) {
                assert(newEmptyBitmap->nSegments() == 1);
                if (ei.isNull() == false && ei.checkBit()) {
                    // empty bitmap containing all ones: just return original value
                    return value;
                }
                tileValue.getTile()->clear();
            } else {
                RLEPayload::append_iterator appender(tileValue.getTile());
                Value v;
                while (!ei.end()) {
                    uint64_t count = ei.getRepeatCount();
                    if (ei.isNull() == false && ei.checkBit()) {
                        count = appender.add(vi, count);
                    } else {
                        vi += count;
                    }
                    ei += count;
                }
                appender.flush();
            }
            return tileValue;
        }
        return inputIterator->getItem();
    }

    bool FilterChunkIterator::setPosition(Coordinates const& pos)
    {
        if (inputIterator->setPosition(pos)) {
            for (size_t i = 0, n = _iterators.size(); i < n; i++) {
                if (_iterators[i] && _iterators[i] != inputIterator) {
                    if (!_iterators[i]->setPosition(pos))
                        throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_OPERATION_FAILED) << "setPosition";
                }
            }
            return _hasCurrent = (_mode & TILE_MODE) || filter();
        }
        return _hasCurrent = false;
    }

    void FilterChunkIterator::operator ++()
    {
        moveNext();
        nextVisible();
    }

    FilterChunkIterator::FilterChunkIterator(FilterArrayIterator const& arrayIterator, DelegateChunk const* chunk, int iterationMode)
    : DelegateChunkIterator(chunk, iterationMode),
      _array((FilterArray&)arrayIterator.array),
      _iterators(_array.bindings.size()),
      _params(*_array.expression),
      _mode(iterationMode),
      _type(chunk->getAttributeDesc().getType()),
      _query(Query::getValidQueryPtr(_array._query))
    {
        for (size_t i = 0, n = _array.bindings.size(); i < n; i++) {
            switch (_array.bindings[i].kind) {
              case BindInfo::BI_COORDINATE:
                if (_mode & TILE_MODE) {
                    if (arrayIterator.iterators[i] == arrayIterator.getInputIterator()) {
                        _iterators[i] = inputIterator;
                    } else {
                        _iterators[i] = arrayIterator.iterators[i]->getChunk().getConstIterator(iterationMode);

                    }
                }
                break;
              case BindInfo::BI_ATTRIBUTE:
                if ((AttributeID)_array.bindings[i].resolvedId == arrayIterator.inputAttrID) {
                    _iterators[i] = inputIterator;
                } else {
                    _iterators[i] = arrayIterator.iterators[i]->getChunk().getConstIterator((_mode & TILE_MODE)|IGNORE_EMPTY_CELLS);
                }
                break;
              case BindInfo::BI_VALUE:
                _params[i] = _array.bindings[i].value;
                break;
              default:
                break;
            }
        }
        if (iterationMode & TILE_MODE) {
            tileValue = Value(TypeLibrary::getType(chunk->getAttributeDesc().getType()),Value::asTile);
            if (arrayIterator.emptyBitmapIterator) {
                emptyBitmapIterator = arrayIterator.emptyBitmapIterator->getChunk().getConstIterator(TILE_MODE|IGNORE_EMPTY_CELLS);
            } else {
                ArrayDesc const& arrayDesc = chunk->getArrayDesc();
                Address addr(arrayDesc.getEmptyBitmapAttribute()->getId(), chunk->getFirstPosition(false));
                shapeChunk.initialize(&_array, &arrayDesc, addr, 0);
                emptyBitmapIterator = shapeChunk.getConstIterator(TILE_MODE|IGNORE_EMPTY_CELLS);
            }
        }
        nextVisible();
    }

    inline Value& FilterChunkIterator::buildBitmap()
    {
        Value& value = evaluate();
        RLEPayload* inputPayload = value.getTile();
        RLEPayload::append_iterator appender(tileValue.getTile());
        RLEPayload::iterator vi(inputPayload);
        if (!emptyBitmapIterator->setPosition(inputIterator->getPosition()))
            throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_OPERATION_FAILED) << "setPosition";
        RLEPayload* emptyBitmap = emptyBitmapIterator->getItem().getTile();
        RLEPayload::iterator ei(emptyBitmap);

#ifndef NDEBUG
        position_t prevPos = 0;
#endif

        Value trueVal, falseVal;
        trueVal.setBool(true);
        falseVal.setBool(false);
        while (!ei.end()) {
#ifndef NDEBUG
            position_t currPos = ei.getPPos();
#endif
            assert (prevPos == currPos);
            uint64_t count;
            if (ei.checkBit()) {
                count = min(vi.getRepeatCount(), ei.getRepeatCount());
                appender.add((vi.isNull()==false && vi.checkBit()) ? trueVal : falseVal, count);
                vi += count;
            } else {
                count = ei.getRepeatCount();
                appender.add(falseVal, count);
            }
            ei += count;

#ifndef NDEBUG
            prevPos = currPos + count;
#endif
        }
        appender.flush();
        return tileValue;
    }

    //
    // Exited bitmap chunk iterator methods
    //
    Value& ExistedBitmapChunkIterator::getItem()
    {
        if (_mode & TILE_MODE) {
            return buildBitmap();
        } else {
            _value.setBool(inputIterator->getItem().getBool() && filter());
            return _value;
        }
    }

    ExistedBitmapChunkIterator::ExistedBitmapChunkIterator(FilterArrayIterator const& arrayIterator, DelegateChunk const* chunk, int iterationMode)
    : FilterChunkIterator(arrayIterator, chunk, iterationMode), _value(TypeLibrary::getType(TID_BOOL))
    {
    }

    //
    // New bitmap chunk iterator methods
    //
    Value& NewBitmapChunkIterator::getItem()
    {
        return (_mode & TILE_MODE) ? buildBitmap() : evaluate();
    }

    NewBitmapChunkIterator::NewBitmapChunkIterator(FilterArrayIterator const& arrayIterator, DelegateChunk const* chunk, int iterationMode)
    : FilterChunkIterator(arrayIterator, chunk, iterationMode)
    {
    }

    //
    // Filter array iterator methods
    //

    ConstChunk const& FilterArrayIterator::getChunk()
    {
        chunk->setInputChunk(inputIterator->getChunk());
        chunk->overrideClone(false);
        return *chunk;
    }


    bool FilterArrayIterator::setPosition(Coordinates const& pos)
    {
        chunkInitialized = false;
        if (inputIterator->setPosition(pos)) {
            for (size_t i = 0, n = iterators.size(); i < n; i++) {
                if (iterators[i]) {
                    if (!iterators[i]->setPosition(pos))
                        throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_OPERATION_FAILED) << "setPosition";
                }
            }
            if (emptyBitmapIterator) {
                if (!emptyBitmapIterator->setPosition(pos))
                    throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_OPERATION_FAILED) << "setPosition";
            }
            return true;
        }
        return false;
    }

    void FilterArrayIterator::reset()
    {
        chunkInitialized = false;
        inputIterator->reset();
        for (size_t i = 0, n = iterators.size(); i < n; i++) {
            if (iterators[i] && iterators[i] != inputIterator) {
                iterators[i]->reset();
            }
        }
        if (emptyBitmapIterator) {
            emptyBitmapIterator->reset();
        }
    }

    void FilterArrayIterator::operator ++()
    {
        chunkInitialized = false;
        ++(*inputIterator);
        for (size_t i = 0, n = iterators.size(); i < n; i++) {
            if (iterators[i] && iterators[i] != inputIterator) {
                ++(*iterators[i]);
            }
        }
        if (emptyBitmapIterator) {
            ++(*emptyBitmapIterator);
        }
    }

    FilterArrayIterator::FilterArrayIterator(FilterArray const& array, AttributeID outAttrID, AttributeID inAttrID)
    : DelegateArrayIterator(array, outAttrID, array.getInputArray()->getConstIterator(inAttrID)),
      iterators(array.bindings.size()),
      inputAttrID(inAttrID)
    {
        for (size_t i = 0, n = iterators.size(); i < n; i++) {
            switch (array.bindings[i].kind) {
              case BindInfo::BI_ATTRIBUTE:
                if ((AttributeID)array.bindings[i].resolvedId == inAttrID) {
                    iterators[i] = inputIterator;
                } else {
                    iterators[i] = array.getInputArray()->getConstIterator(array.bindings[i].resolvedId);
                }
                break;
              case BindInfo::BI_COORDINATE:
                if (array._tileMode) {
                    AttributeDesc const* emptyAttr = array.getInputArray()->getArrayDesc().getEmptyBitmapAttribute();
                    if (emptyAttr == NULL || emptyAttr->getId() == inputAttrID) {
                        iterators[i] = inputIterator;
                    } else {
                        iterators[i] = array.getInputArray()->getConstIterator(emptyAttr->getId());
                    }
                }
                break;
              default:
                break;
            }
        }
        if (array._tileMode) {
            AttributeDesc const* emptyAttr = array.getInputArray()->getArrayDesc().getEmptyBitmapAttribute();
            if (emptyAttr != NULL) {
                emptyBitmapIterator = array.getInputArray()->getConstIterator(emptyAttr->getId());
            }
        }
    }

    ConstChunk const& FilterArrayEmptyBitmapIterator::getChunk()
    {
        chunk = array.getEmptyBitmapChunk(this);
        return *chunk->materialize();
    }

    FilterArrayEmptyBitmapIterator::FilterArrayEmptyBitmapIterator(FilterArray const& arr, AttributeID outAttrID, AttributeID inAttrID)
    : FilterArrayIterator(arr, outAttrID, inAttrID),
      array((FilterArray&)arr)
    {}


    //
    // Filter array methods
    //
    DelegateChunk* FilterArray::createChunk(DelegateArrayIterator const* iterator, AttributeID id) const
    {
        DelegateChunk* chunk = DelegateArray::createChunk(iterator, id);
        chunk->overrideClone(!desc.getAttributes()[id].isEmptyIndicator());
        return chunk;
    }


    DelegateChunkIterator* FilterArray::createChunkIterator(DelegateChunk const* chunk, int iterationMode) const
    {
        FilterArrayIterator const& arrayIterator = (FilterArrayIterator const&)chunk->getArrayIterator();
        AttributeDesc const& attr = chunk->getAttributeDesc();
        if (_tileMode/* && chunk->isRLE()*/) {
            iterationMode |= ChunkIterator::TILE_MODE;
        } else {
            iterationMode &= ~ChunkIterator::TILE_MODE;
        }
        iterationMode &= ~ChunkIterator::INTENDED_TILE_MODE;
        return attr.isEmptyIndicator()
            ? (attr.getId() >= inputArray->getArrayDesc().getAttributes().size())
                ? (DelegateChunkIterator*)new NewBitmapChunkIterator(arrayIterator, chunk, iterationMode)
                : (DelegateChunkIterator*)new ExistedBitmapChunkIterator(arrayIterator, chunk, iterationMode)
            : (DelegateChunkIterator*)new FilterChunkIterator(arrayIterator, chunk, iterationMode);
    }

    DelegateArrayIterator* FilterArray::createArrayIterator(AttributeID attrID) const
    {
        AttributeID inputAttrID = attrID;
        if (inputAttrID >= inputArray->getArrayDesc().getAttributes().size()) {
            inputAttrID = 0;
            for (size_t i = 0, n = bindings.size(); i < n; i++) {
                if (bindings[i].kind == BindInfo::BI_ATTRIBUTE) {
                    inputAttrID = (AttributeID)bindings[i].resolvedId;
                    break;
                }
            }
        }
#if FILTER_CHUNK_CACHE
        return attrID == emptyAttrID
            ? (DelegateArrayIterator*)new FilterArrayEmptyBitmapIterator(*this, attrID, inputAttrID)
            : (DelegateArrayIterator*)new FilterArrayIterator(*this, attrID, inputAttrID);
#else
        return new FilterArrayIterator(*this, attrID, inputAttrID);
#endif
    }

    boost::shared_ptr<DelegateChunk> FilterArray::getEmptyBitmapChunk(FilterArrayEmptyBitmapIterator* iterator)
    {
        boost::shared_ptr<DelegateChunk> chunk;
        Coordinates const& pos = iterator->getPosition();
        {
            ScopedMutexLock cs(mutex);
            chunk = cache[pos];
            if (chunk) {
                return chunk;
            }
        }
        chunk = boost::shared_ptr<DelegateChunk>(createChunk(iterator, emptyAttrID));
        chunk->setInputChunk(iterator->getInputIterator()->getChunk());
        chunk->materialize();
        {
            ScopedMutexLock cs(mutex);
            if (cache.size() >= cacheSize) {
                cache.erase(cache.begin());
            }
            cache[pos] = chunk;
        }
        return chunk;
    }

    FilterArray::FilterArray(ArrayDesc const& desc, boost::shared_ptr<Array> const& array,
                             boost::shared_ptr< Expression> expr, boost::shared_ptr<Query>& query,
                             bool tileMode)
    : DelegateArray(desc, array), expression(expr), bindings(expr->getBindings()), _tileMode(tileMode),
      cacheSize(Config::getInstance()->getOption<int>(CONFIG_RESULT_PREFETCH_QUEUE_SIZE)),
      emptyAttrID(desc.getEmptyBitmapAttribute()->getId())
    {
        assert(query);
        _query=query;
    }

}


