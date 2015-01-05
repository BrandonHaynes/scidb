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
 * @file UnpackArray.cpp
 *
 * @brief Unpack array implementation
 *
 * @author Konstantin Knizhnik <knizhnik@garret.ru>
 */

#include "UnpackArray.h"
#include "query/Operator.h"

namespace scidb {

    using namespace boost;
    using namespace std;

    //
    // Unpack chunk iterator methods
    //
    void UnpackChunkIterator::reset()
    {
        size_t nDims = inPos.size();
        inPos[nDims-1] = first;
        if (inputIterator->setPosition(inPos)) {
            hasCurrent = true;
        } else {
            while (true) {
                if (++inPos[nDims-1] > last) {
                    hasCurrent = false;
                    break;
                }
                if (inputIterator->setPosition(inPos)) {
                    hasCurrent = true;
                    break;
                }
            }
        }
    }

    void UnpackChunkIterator::operator++()
    {
        size_t nDims = inPos.size();
        ++(*inputIterator);
        if (inputIterator->end()) {
            hasCurrent = false;
        } else {
            Coordinates const& newPos = inputIterator->getPosition();
            inPos[nDims-1] = newPos[nDims-1];
            hasCurrent = inPos == newPos && inPos[nDims-1] <= last;
        }
    }

    ConstChunk const& UnpackChunkIterator::getChunk()
    {
        return chunk;
    }

    Value& UnpackChunkIterator::getItem()
    {
        if (!hasCurrent)
            throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_NO_CURRENT_ELEMENT);
        if (attrID < array.dims.size()) {
           _value.setInt64(inputIterator->getPosition()[attrID]);
            return _value;
        } else {
            return inputIterator->getItem();
        }
    }

    bool UnpackChunkIterator::end()
    {
        return !hasCurrent;
    }

    bool UnpackChunkIterator::isEmpty()
    {
        if (!hasCurrent)
            throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_NO_CURRENT_ELEMENT);
        return inputIterator->isEmpty();
    }

    bool UnpackChunkIterator::setPosition(Coordinates const& newPos)
    {
        Coordinate offs = newPos[0] - base;
        if (offs < 0 || offs > last - first) {
            return hasCurrent = false;
        }
        inPos[inPos.size() - 1] = first + offs;
        return hasCurrent = inputIterator->setPosition(inPos);
    }

    Coordinates const& UnpackChunkIterator::getPosition()
    {
        if (!hasCurrent)
            throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_NO_CURRENT_ELEMENT);
        outPos[0] = inPos[inPos.size()-1] - first + base;
        return outPos;
    }

    int UnpackChunkIterator::getMode()
    {
        return mode;
    }

    UnpackChunkIterator::UnpackChunkIterator(UnpackArray const& arr, UnpackChunk const& chk, int iterationMode)
    : array(arr),
      chunk(chk),
      inPos(arr.dims.size()),
      outPos(1),
      attrID(chunk.getAttributeDesc().getId()),
      inputIterator(chunk.inputChunk->getConstIterator(iterationMode & ~(INTENDED_TILE_MODE|IGNORE_DEFAULT_VALUES))),
      mode(iterationMode),
      _query(Query::getValidQueryPtr(array._query))
    {
        size_t nDims = inPos.size();
        base = chunk.getFirstPosition(false)[0];
        array.out2in(base, inPos);
        first = inPos[nDims-1];
        last = chunk.inputChunk->getLastPosition(false)[nDims-1];
        reset();
    }

    //
    // Unpack chunk methods
    //
    UnpackChunk::UnpackChunk(UnpackArray const& arr, DelegateArrayIterator const& iterator, AttributeID attrID)
    : DelegateChunk(arr, iterator, attrID, false),
      array(arr)
    {
    }

    boost::shared_ptr<ConstChunkIterator> UnpackChunk::getConstIterator(int iterationMode) const
    {
        return boost::shared_ptr<ConstChunkIterator>(new UnpackChunkIterator(array, *this, iterationMode));
    }

    void UnpackChunk::initialize(Coordinates const& pos)
    {
        ArrayDesc const& desc = array.getArrayDesc();
        Address addr(attrID, pos);
        chunk.initialize(&array, &desc, addr, desc.getAttributes()[attrID].getDefaultCompressionMethod());
        inputChunk = &iterator.getInputIterator()->getChunk();
        setInputChunk(chunk);
    }

    bool UnpackChunk::isSparse() const
    {
        return false;
    }

    //
    // Unpack array iterator
    //
    ConstChunk const& UnpackArrayIterator::getChunk()
    {
        if (!hasCurrent)
            throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_NO_CURRENT_ELEMENT);
        if (!chunkInitialized) { 
            ((UnpackChunk&)*chunk).initialize(outPos);
            chunkInitialized = true;
        }
        return *chunk;
    }

    void UnpackArrayIterator::operator ++()
    {
        if (!hasCurrent)
            throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_NO_CURRENT_ELEMENT);
        Dimensions const& dims = array.getArrayDesc().getDimensions();
        chunkInitialized = false;
        while ((outPos[0] += dims[0].getChunkInterval()) < Coordinate(dims[0].getLength())) {
            array.out2in(outPos[0], inPos);
            if (inputIterator->setPosition(inPos)) {
                hasCurrent = true;
                return;
            }
        }
        hasCurrent = false;
    }

    bool UnpackArrayIterator::end()
    {
        return !hasCurrent;
    }

    void UnpackArrayIterator::reset()
    {
        Dimensions const& dims = array.getArrayDesc().getDimensions();
        outPos[0] = -Coordinate(dims[0].getChunkInterval());
        hasCurrent = true;
        chunkInitialized = false;
        ++(*this);
    }

    Coordinates const& UnpackArrayIterator::getPosition()
    {
        if (!hasCurrent)
            throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_NO_CURRENT_ELEMENT);
        return outPos;
    }

    bool UnpackArrayIterator::setPosition(Coordinates const& newPos)
    {
        Dimensions const& dims = array.getArrayDesc().getDimensions();
        if ((uint64_t)newPos[0] >= dims[0].getLength()) {
            return hasCurrent = false;
        }
        chunkInitialized = false;
        outPos[0] = newPos[0] - (newPos[0] % dims[0].getChunkInterval());
        array.out2in(outPos[0], inPos);
        return hasCurrent = inputIterator->setPosition(inPos);
    }

    UnpackArrayIterator::UnpackArrayIterator(UnpackArray const& arr, AttributeID attrID, boost::shared_ptr<ConstArrayIterator> inputIterator)
    : DelegateArrayIterator(arr, attrID, inputIterator),
      array(arr),
      inPos(arr.dims.size()),
      outPos(1)
    {
        reset();
    }

    //
    // Unpack array methods
    //

    Coordinate UnpackArray::in2out(Coordinates const& inPos)  const
    {
        uint64_t cellNo = PhysicalBoundaries::getCellNumber(inPos, dims);
        if (cellNo >= INFINITE_LENGTH)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "Internal inconsistency reshaping coordinates";
        }
        return cellNo;
    }

    void UnpackArray::out2in(Coordinate outPos, Coordinates& inPos)  const
    {
        uint64_t offset = outPos;
        inPos = PhysicalBoundaries::getCoordinates(offset, dims);
    }

    DelegateChunk* UnpackArray::createChunk(DelegateArrayIterator const* iterator, AttributeID id) const
    {
       return new UnpackChunk(*this, *iterator, id);
    }

    DelegateArrayIterator* UnpackArray::createArrayIterator(AttributeID id) const
    {
        return new UnpackArrayIterator(*this, id, inputArray->getConstIterator(id < dims.size() ? 0 : id - dims.size()));
    }

    UnpackArray::UnpackArray(ArrayDesc const& desc, boost::shared_ptr<Array> const& array, const boost::shared_ptr<Query>& query)
    : DelegateArray(desc, array),
      dims(array->getArrayDesc().getDimensions())
    {
        assert(query);
        _query=query;
    }
}

