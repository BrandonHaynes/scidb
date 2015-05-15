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
 * ApplyArray.cpp
 *
 *  Created on: Apr 11, 2010
 *      Author: Knizhnik
 */

#include "query/Operator.h"
#include "array/Metadata.h"
#include "array/Array.h"
#include "query/ops/apply/ApplyArray.h"

namespace scidb
{

using namespace boost;
using namespace std;

//
// Apply chunk iterator methods
//
inline bool ApplyChunkIterator::isNull()
{
    return false;
}

void ApplyChunkIterator::reset()
{
    _applied = false;
    inputIterator->reset();
    if (!inputIterator->end())
    {
        for (size_t i = 0, n = _iterators.size(); i < n; i++)
        {
            if (_iterators[i] && _iterators[i] != inputIterator)
            {
                _iterators[i]->reset();
            }
        }
        if (isNull())
        {
            ++(*this);
        }
    }
}

bool ApplyChunkIterator::setPosition(Coordinates const& pos)
{
    _applied = false;
    if (inputIterator->setPosition(pos))
    {
        for (size_t i = 0, n = _iterators.size(); i < n; i++)
        {
            if (_iterators[i])
            {
                if (!_iterators[i]->setPosition(pos))
                    throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_OPERATION_FAILED) << "setPosition";
            }
        }
        return !isNull();
    }
    return false;
}

Value& ApplyChunkIterator::getItem()
{
    if (!_applied)
    {
        for (size_t i = 0, n = _bindings.size(); i < n; i++)
        {
            switch (_bindings[i].kind)
            {
                case BindInfo::BI_ATTRIBUTE:
                    _params[i] = _iterators[i]->getItem();
                    break;
                case BindInfo::BI_COORDINATE:
                    if (_mode & TILE_MODE)
                    {
                        _iterators[i]->getItem().getTile()->getCoordinates(_array.getInputArray()->getArrayDesc(), _bindings[i].resolvedId,
                                                                           _iterators[i]->getChunk().getFirstPosition(false),
                                                                           _iterators[i]->getPosition(), _query, _params[i], !(_mode & IGNORE_OVERLAPS));
                    }
                    else
                    {
                        _params[i].setInt64(inputIterator->getPosition()[_bindings[i].resolvedId]);
                    }
                    break;
                default:
                    break;
            }
        }
        _value = (Value*) &_array._expressions[_outAttrId]->evaluate(_params);
        _applied = true;
    }
    return *_value;
}

void ApplyChunkIterator::operator ++()
{
    do
    {
        _applied = false;
        ++(*inputIterator);
        if (!inputIterator->end())
        {
            for (size_t i = 0, n = _iterators.size(); i < n; i++)
            {
                if (_iterators[i] && _iterators[i] != inputIterator)
                {
                    ++(*_iterators[i]);
                }
            }
        }
        else
        {
            break;
        }
    } while (isNull());
}

ApplyChunkIterator::ApplyChunkIterator(ApplyArrayIterator const& arrayIterator, DelegateChunk const* chunk, int iterationMode) :
    DelegateChunkIterator(chunk, iterationMode & ~(INTENDED_TILE_MODE | IGNORE_NULL_VALUES | IGNORE_DEFAULT_VALUES)),
    _array((ApplyArray&) arrayIterator.array),
    _outAttrId(arrayIterator.attr),
    _bindings(_array._bindingSets[_outAttrId]),
    _iterators(_bindings.size()),
    _params(*_array._expressions[_outAttrId]),
    _mode(iterationMode),
    _applied(false),
    _nullable(_array._attributeNullable[_outAttrId]),
    _query(Query::getValidQueryPtr(_array._query))
{
    for (size_t i = 0, n = _bindings.size(); i < n; i++)
    {
        switch (_bindings[i].kind)
        {
            case BindInfo::BI_COORDINATE:
                if (_mode & TILE_MODE)
                {
                    if (arrayIterator.iterators[i] == arrayIterator.getInputIterator())
                    {
                        _iterators[i] = inputIterator;
                    }
                    else
                    {
                        _iterators[i] = arrayIterator.iterators[i]->getChunk().getConstIterator(TILE_MODE | IGNORE_EMPTY_CELLS);
                    }
                }
                break;
            case BindInfo::BI_ATTRIBUTE:
                if ((AttributeID) _bindings[i].resolvedId == arrayIterator.inputAttrID)
                {
                    _iterators[i] = inputIterator;
                }
                else
                {
                    _iterators[i] = arrayIterator.iterators[i]->getChunk().getConstIterator(inputIterator->getMode());
                }
                break;
            case BindInfo::BI_VALUE:
                _params[i] = _bindings[i].value;
                break;
            default:
                break;
        }
    }
    if (isNull())
    {
        ++(*this);
    }
}

//
// Apply array iterator methods
//

bool ApplyArrayIterator::setPosition(Coordinates const& pos)
{
    if (inputIterator->setPosition(pos))
    {
        for (size_t i = 0, n = iterators.size(); i < n; i++)
        {
            if (iterators[i])
            {
                if (!iterators[i]->setPosition(pos))
                    throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_OPERATION_FAILED) << "setPosition";
            }
        }
        return true;
    }
    return false;
}

void ApplyArrayIterator::reset()
{
    inputIterator->reset();
    for (size_t i = 0, n = iterators.size(); i < n; i++)
    {
        if (iterators[i] && iterators[i] != inputIterator)
        {
            iterators[i]->reset();
        }
    }
}

void ApplyArrayIterator::operator ++()
{
    ++(*inputIterator);
    for (size_t i = 0, n = iterators.size(); i < n; i++)
    {
        if (iterators[i] && iterators[i] != inputIterator)
        {
            ++(*iterators[i]);
        }
    }
}

ApplyArrayIterator::ApplyArrayIterator(ApplyArray const& array, AttributeID outAttrID, AttributeID inAttrID) :
    DelegateArrayIterator(array, outAttrID, array.getInputArray()->getConstIterator(inAttrID)),
    iterators(array._bindingSets[outAttrID].size()),
    inputAttrID(inAttrID)
{
    for (size_t i = 0, n = iterators.size(); i < n; i++)
    {
        switch (array._bindingSets[outAttrID][i].kind)
        {
            case BindInfo::BI_ATTRIBUTE:
                if ((AttributeID) array._bindingSets[outAttrID][i].resolvedId == inAttrID)
                {
                    iterators[i] = inputIterator;
                }
                else
                {
                    iterators[i] = array.getInputArray()->getConstIterator(array._bindingSets[outAttrID][i].resolvedId);
                }
                break;
            case BindInfo::BI_COORDINATE:
                if (array._runInTileMode[outAttrID])
                {
                    AttributeDesc const* emptyAttr = array.getInputArray()->getArrayDesc().getEmptyBitmapAttribute();
                    if (emptyAttr == NULL || emptyAttr->getId() == inputAttrID)
                    {
                        iterators[i] = inputIterator;
                    }
                    else
                    {
                        iterators[i] = array.getInputArray()->getConstIterator(emptyAttr->getId());
                    }
                }
                break;
            default:
                ;
                break;
        }
    }
}

//
// Apply array methods
//

DelegateChunkIterator* ApplyArray::createChunkIterator(DelegateChunk const* chunk, int iterationMode) const
{
    StatisticsScope sScope(_statistics);
    ApplyArrayIterator const& arrayIterator = (ApplyArrayIterator const&) chunk->getArrayIterator();
    AttributeDesc const& attr = chunk->getAttributeDesc();
    AttributeID attId = attr.getId();

    if (chunk->inTileMode())
    {
        if ((iterationMode & ChunkIterator::INTENDED_TILE_MODE) || (_expressions[attId].get()))
        {
            iterationMode |= ChunkIterator::TILE_MODE;
        }
    }
    else
    {
        iterationMode &= ~ChunkIterator::TILE_MODE;
    }
    DelegateChunkIterator* res = (_expressions[attId].get()) ? (DelegateChunkIterator*) new ApplyChunkIterator(arrayIterator, chunk, iterationMode)
                                                             : DelegateArray::createChunkIterator(chunk, iterationMode);
    return res;
}

DelegateArrayIterator* ApplyArray::createArrayIterator(AttributeID attrID) const
{
    AttributeID inputAttrID;

    if ( _expressions[attrID].get() )
    {
        vector<BindInfo> const& bindings = _bindingSets[attrID];
        inputAttrID = 0;

        for (size_t i = 0, n = bindings.size(); i < n; i++)
        {
            if (bindings[i].kind == BindInfo::BI_ATTRIBUTE)
            {
                inputAttrID = (AttributeID) bindings[i].resolvedId;
                break;
            }
        }
    }
    else if (desc.getEmptyBitmapAttribute() && attrID == desc.getEmptyBitmapAttribute()->getId() )
    {
        inputAttrID = inputArray->getArrayDesc().getEmptyBitmapAttribute()->getId();
    }
    else
    {
        inputAttrID = attrID;
    }

    return new ApplyArrayIterator(*this, attrID, inputAttrID);
}

DelegateChunk* ApplyArray::createChunk(DelegateArrayIterator const* iterator, AttributeID attrID) const
{
    bool isClone = !_expressions[attrID].get();
    DelegateChunk* chunk = new DelegateChunk(*this, *iterator, attrID, isClone);
    chunk->overrideTileMode(_runInTileMode[attrID]);
    return chunk;
}

ApplyArray::ApplyArray(ArrayDesc const& desc,
                       boost::shared_ptr<Array> const& array,
                       vector<shared_ptr<Expression> > expressions,
                       const boost::shared_ptr<Query>& query,
                       bool tile) :
   DelegateArray(desc, array),
   _expressions(expressions),
   _attributeNullable(desc.getAttributes().size(), false),
   _runInTileMode(desc.getAttributes().size(), tile),
   _bindingSets(desc.getAttributes().size(), vector<BindInfo>(0))
{
    assert(query);
    _query=query;
    for(size_t i =0; i<expressions.size(); i++)
    {
        _attributeNullable[i] = desc.getAttributes()[i].isNullable();
        if (expressions[i].get())
        {
            _runInTileMode[i] = tile && expressions[i]->supportsTileMode();
            _bindingSets[i]= expressions[i]->getBindings();
        }
    }
}

}
