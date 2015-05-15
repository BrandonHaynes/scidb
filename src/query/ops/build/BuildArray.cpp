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
 * BuildArray.cpp
 *
 *  Created on: Apr 11, 2010
 *      Author: Knizhnik
 */

#include <query/Operator.h>
#include <array/Metadata.h>
#include <array/MemArray.h>
#include <network/NetworkManager.h>
#include "BuildArray.h"


namespace scidb {

    using namespace boost;
    using namespace std;

    //
    // Build chunk iterator methods
    //
    int BuildChunkIterator::getMode()
    {
        return iterationMode;
    }

    Value& BuildChunkIterator::getItem()
    {
        if (!hasCurrent)
            throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_NO_CURRENT_ELEMENT);
        
        if (attrID != 0) { 
            return _trueValue;
        }
        const size_t nBindings =  array._bindings.size();

        for (size_t i = 0; i < nBindings; i++) {
            switch (array._bindings[i].kind) {
            case BindInfo::BI_COORDINATE:
            {
                _params[i].setInt64(currPos[array._bindings[i].resolvedId]);
            } break;
            case BindInfo::BI_VALUE:
            {
                _params[i] = array._bindings[i].value;
            } break;
            default:
            assert(false);
            }
        }
        if (_converter) {
            const Value* v = &_expression.evaluate(_params);
            _converter(&v, &_value, NULL);
        }
        else {
            _value = _expression.evaluate(_params);
        }

        if (!_nullable && _value.isNull())
            throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_ASSIGNING_NULL_TO_NON_NULLABLE);

        return _value;
    }

    void BuildChunkIterator::operator ++()
    {
        if (!hasCurrent)
            throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_NO_CURRENT_ELEMENT);
        for (int i = currPos.size(); --i >= 0;) {
            if (++currPos[i] > lastPos[i]) {
                currPos[i] = firstPos[i];
            } else {
                hasCurrent = true;
                return;
            }
        }
        hasCurrent = false;
    }

    bool BuildChunkIterator::end()
    {
        return !hasCurrent;
    }

    bool BuildChunkIterator::isEmpty()
    {
        return false;
    }

    Coordinates const& BuildChunkIterator::getPosition()
    {
        return currPos;
    }

    bool BuildChunkIterator::setPosition(Coordinates const& pos)
    {
        for (size_t i = 0, n = currPos.size(); i < n; i++) {
            if (pos[i] < firstPos[i] || pos[i] > lastPos[i]) {
                return hasCurrent = false;
            }
        }
        currPos = pos;
        return hasCurrent = true;
    }

    void BuildChunkIterator::reset()
    {
        currPos = firstPos;
        hasCurrent = true;
    }

    ConstChunk const& BuildChunkIterator::getChunk()
    {
        return *chunk;
    }

    BuildChunkIterator::BuildChunkIterator(BuildArray& outputArray,
                                           ConstChunk const* aChunk,
                                           AttributeID attr, int mode)
    : iterationMode(mode),
        array(outputArray),
        firstPos(aChunk->getFirstPosition((mode & IGNORE_OVERLAPS) == 0)),
        lastPos(aChunk->getLastPosition((mode & IGNORE_OVERLAPS) == 0)),
        currPos(firstPos.size()),
        attrID(attr),
        chunk(aChunk),
        _converter(outputArray._converter),
        _value(TypeLibrary::getType(aChunk->getAttributeDesc().getType())),
        _expression(*array._expression),
        _params(_expression),
      _nullable(aChunk->getAttributeDesc().isNullable()),
      _query(Query::getValidQueryPtr(array._query))
    {
        _trueValue.setBool(true);
        reset();
    }

    //
    // Build chunk methods
    //
    Array const& BuildChunk::getArray() const 
    { 
        return array;
    }

    const ArrayDesc& BuildChunk::getArrayDesc() const
    {
        return array._desc;
    }

    const AttributeDesc& BuildChunk::getAttributeDesc() const
    {
        return array._desc.getAttributes()[attrID];
    }

        Coordinates const& BuildChunk::getFirstPosition(bool withOverlap) const
    {
        return withOverlap ? firstPosWithOverlap : firstPos;
    }

        Coordinates const& BuildChunk::getLastPosition(bool withOverlap) const
    {
        return withOverlap ? lastPosWithOverlap : lastPos;
    }

        boost::shared_ptr<ConstChunkIterator> BuildChunk::getConstIterator(int iterationMode) const
    {
        return boost::shared_ptr<ConstChunkIterator>(new BuildChunkIterator(array, this, attrID, iterationMode));
    }

    int BuildChunk::getCompressionMethod() const
    {
        return array._desc.getAttributes()[attrID].getDefaultCompressionMethod();
    }

    void BuildChunk::setPosition(Coordinates const& pos)
    {
        firstPos = pos;
        Dimensions const& dims = array._desc.getDimensions();
        for (size_t i = 0, n = dims.size(); i < n; i++) {
            firstPosWithOverlap[i] = firstPos[i] - dims[i].getChunkOverlap();
            if (firstPosWithOverlap[i] < dims[i].getStartMin()) {
                firstPosWithOverlap[i] = dims[i].getStartMin();
            }
            lastPos[i] = firstPos[i] + dims[i].getChunkInterval() - 1;
            lastPosWithOverlap[i] = lastPos[i] + dims[i].getChunkOverlap();
            if (lastPos[i] > dims[i].getEndMax()) {
                lastPos[i] = dims[i].getEndMax();
            }
            if (lastPosWithOverlap[i] > dims[i].getEndMax()) {
                lastPosWithOverlap[i] = dims[i].getEndMax();
            }
        }
    }

    BuildChunk::BuildChunk(BuildArray& arr, AttributeID attr)
    : array(arr),
      firstPos(arr._desc.getDimensions().size()),
      lastPos(firstPos.size()),
      firstPosWithOverlap(firstPos.size()),
      lastPosWithOverlap(firstPos.size()),
      attrID(attr)
    {
    }


    //
    // Build array iterator methods
    //

    void BuildArrayIterator::operator ++()
    {
        if (!hasCurrent) {
            throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_NO_CURRENT_ELEMENT);
        }
        Query::getValidQueryPtr(array._query);
        nextChunk();
    }

    bool BuildArrayIterator::end()
    {
        return !hasCurrent;
    }

    Coordinates const& BuildArrayIterator::getPosition()
    {
        if (!hasCurrent) {
            throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_NO_CURRENT_ELEMENT);
        }
        return currPos;
    }

    void BuildArrayIterator::nextChunk()
    {
        chunkInitialized = false;
        while (true) { 
            int i = dims.size() - 1;
            while ((currPos[i] += dims[i].getChunkInterval()) > dims[i].getEndMax()) { 
                if (i == 0) { 
                    hasCurrent = false;
                    return;
                }
                currPos[i] = dims[i].getStartMin();
                i -= 1;
            }
            if (array._desc.getHashedChunkNumber(currPos) % array.nInstances == array.instanceID) {
                hasCurrent = true;
                return;
            }
        }
    }

    bool BuildArrayIterator::setPosition(Coordinates const& pos)
    {
        Query::getValidQueryPtr(array._query);
        for (size_t i = 0, n = currPos.size(); i < n; i++) {
            if (pos[i] < dims[i].getStartMin() || pos[i] > dims[i].getEndMax()) {
                return hasCurrent = false;
            }
        }
        currPos = pos;
        array._desc.getChunkPositionFor(currPos);
        chunkInitialized = false;
        return hasCurrent = array._desc.getHashedChunkNumber(currPos) % array.nInstances == array.instanceID;
    }

    void BuildArrayIterator::reset()
    {
        Query::getValidQueryPtr(array._query);
        size_t nDims = currPos.size(); 
        for (size_t i = 0; i < nDims; i++) {
            currPos[i] = dims[i].getStartMin();
        }
        currPos[nDims-1] -= dims[nDims-1].getChunkInterval();
        nextChunk();
    }

    ConstChunk const& BuildArrayIterator::getChunk()
    {
        if (!hasCurrent) {
            throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_NO_CURRENT_ELEMENT);
        }
        Query::getValidQueryPtr(array._query);
        if (!chunkInitialized) { 
            chunk.setPosition(currPos);
            chunkInitialized = true;
        }
        return chunk;
    }


    BuildArrayIterator::BuildArrayIterator(BuildArray& arr, AttributeID attrID)
    : array(arr),
      chunk(arr, attrID), 
      dims(arr._desc.getDimensions()),
      currPos(dims.size())
    {
        reset();
    }


    //
    // Build array methods
    //

    ArrayDesc const& BuildArray::getArrayDesc() const
    {
        return _desc;
    }

    boost::shared_ptr<ConstArrayIterator> BuildArray::getConstIterator(AttributeID attr) const
    {
        return boost::shared_ptr<ConstArrayIterator>(new BuildArrayIterator(*(BuildArray*)this, attr));
    }

    BuildArray::BuildArray(boost::shared_ptr<Query>& query,
                           ArrayDesc const& desc,
                           boost::shared_ptr< Expression> expression)
    : _desc(desc),
      _expression(expression),
      _bindings(_expression->getBindings()),
      _converter(NULL),
      nInstances(0),
      instanceID(INVALID_INSTANCE)
    {
       assert(query);
       _query=query;
       nInstances = query->getInstancesCount();
       instanceID = query->getInstanceID();
        for (size_t i = 0; i < _bindings.size(); i++) {
            if (_bindings[i].kind == BindInfo::BI_ATTRIBUTE)
                throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_OP_BUILD_ERROR1);
        }
         TypeId attrType = _desc.getAttributes()[0].getType();

        // Search converter for init value to attribute type
         TypeId exprType = expression->getType();
        if (attrType != exprType) {
            _converter = FunctionLibrary::getInstance()->findConverter(exprType, attrType);
        }
        assert(nInstances > 0 && instanceID < nInstances);
    }
}
