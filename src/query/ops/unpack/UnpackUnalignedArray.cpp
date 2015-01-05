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
 * @file UnpackUnalignedArray.cpp
 *
 * @brief UnpackUnaligned array implementation
 *
 * @author Konstantin Knizhnik <knizhnik@garret.ru>
 */

#include "UnpackUnalignedArray.h"
#include "system/Exceptions.h"
#include "query/Operator.h"

namespace scidb {

    using namespace boost;
    using namespace std;

    //
    // UnpackUnaligned chunk iterator methods
    //
    int UnpackUnalignedChunkIterator::getMode()
    {
        return mode;
    }

    void UnpackUnalignedChunkIterator::reset()
    {
        outPos = first;
        outPos[outPos.size()-1] -= 1;
        hasCurrent = true;
        ++(*this);
    }

    Value& UnpackUnalignedChunkIterator::getItem()
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

    void UnpackUnalignedChunkIterator::operator++()
    {
        while (++outPos[0] <= last[0]) { 
            array.out2in(outPos[0], inPos);          
            if (!inputIterator || !inputIterator->getChunk().contains(inPos, !(mode & IGNORE_OVERLAPS))) {
                inputIterator.reset();
                if (arrayIterator->setPosition(inPos)) { 
                    ConstChunk const& inputChunk = arrayIterator->getChunk();
                    inputIterator = inputChunk.getConstIterator(mode);
                    if (inputIterator->setPosition(inPos)) { 
                        hasCurrent = true;
                        return;
                    }
                }
            } else if (inputIterator->setPosition(inPos)) { 
                hasCurrent = true;
                return;
            }
        }
        hasCurrent = false;
    }

    bool UnpackUnalignedChunkIterator::setPosition(Coordinates const& newPos)
    {
        outPos = newPos;
        array.out2in(newPos[0], inPos);
        inputIterator.reset();
        if (arrayIterator->setPosition(inPos)) { 
            ConstChunk const& inputChunk = arrayIterator->getChunk();
            inputIterator = inputChunk.getConstIterator(mode);
            if (inputIterator->setPosition(inPos)) { 
                return hasCurrent = true;
            }
        }
        return hasCurrent = false;
    }

    Coordinates const& UnpackUnalignedChunkIterator::getPosition()
    {
        if (!hasCurrent)
            throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_NO_CURRENT_ELEMENT);
        return outPos;
    }

    bool UnpackUnalignedChunkIterator::end()
    { 
        return !hasCurrent;
    }

    bool UnpackUnalignedChunkIterator::isEmpty()
    {
        if (!hasCurrent)
            throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_NO_CURRENT_ELEMENT);
        return inputIterator->isEmpty();
    }

    ConstChunk const& UnpackUnalignedChunkIterator::getChunk()
    {
        return chunk;
    }
 
    UnpackUnalignedChunkIterator::UnpackUnalignedChunkIterator(UnpackUnalignedArray const& arr, UnpackUnalignedChunk& chk, int iterationMode)
    : array(arr),
      chunk(chk),
      outPos(1),
      inPos(arr.dims.size()),
      first(chunk.getFirstPosition(!(iterationMode & IGNORE_OVERLAPS))),
      last(chunk.getLastPosition(!(iterationMode & IGNORE_OVERLAPS))),
      arrayIterator(chunk.getArrayIterator().getInputIterator()),
      mode(iterationMode & ~INTENDED_TILE_MODE),
      attrID(chunk.getAttributeDesc().getId()),
      _query(Query::getValidQueryPtr(array._query))
    {
        reset();
    }

    //
    // UnpackUnaligned chunk methods
    //
    bool UnpackUnalignedChunk::isSparse() const
    {
        return false;
    }

    boost::shared_ptr<ConstChunkIterator> UnpackUnalignedChunk::getConstIterator(int iterationMode) const
    {
        return boost::shared_ptr<ConstChunkIterator>(new UnpackUnalignedChunkIterator(array, *(UnpackUnalignedChunk*)this, iterationMode));
    }

    void UnpackUnalignedChunk::initialize(Coordinates const& pos)
    {
        ArrayDesc const& desc = array.getArrayDesc();
        Address addr(attrID, pos);
        chunk.initialize(&array, &desc, addr, desc.getAttributes()[attrID].getDefaultCompressionMethod());
        setInputChunk(chunk);
    }

    UnpackUnalignedChunk::UnpackUnalignedChunk(UnpackUnalignedArray const& arr, DelegateArrayIterator const& iterator, AttributeID attrID)
    : DelegateChunk(arr, iterator, attrID, false),
      array(arr)
    {
    }
      
    //
    // UnpackUnaligned array iterator
    //
    ConstChunk const& UnpackUnalignedArrayIterator::getChunk()
    {
        if (!hasCurrent)
            throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_NO_CURRENT_CHUNK);
        if (!chunkInitialized) {
            ((UnpackUnalignedChunk&)*chunk).initialize(outPos);
            chunkInitialized = true;
        }
        return *chunk;
    }

    bool UnpackUnalignedArrayIterator::end()
    {
        return !hasCurrent;
    }

    void UnpackUnalignedArrayIterator::operator ++()
    {
        if (!hasCurrent)
            throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_NO_CURRENT_ELEMENT);
        Dimensions const& dims = array.getArrayDesc().getDimensions();
        while ((outPos[0] += dims[0].getChunkInterval()) < Coordinate(dims[0].getLength())) {
            array.out2in(outPos[0], inPos);
            inputIterator->setPosition(inPos);
            chunkInitialized = false;
            if (!getChunk().getConstIterator(ChunkIterator::IGNORE_EMPTY_CELLS)->end()) {
                return;
            }
        }
        hasCurrent = false;
    }

    void UnpackUnalignedArrayIterator::reset()
    {
        Dimensions const& dims = array.getArrayDesc().getDimensions(); 
        outPos[0] = -Coordinate(dims[0].getChunkInterval());
        hasCurrent = true;
        chunkInitialized = false;
        ++(*this);
    }


    Coordinates const& UnpackUnalignedArrayIterator::getPosition()
    {
        if (!hasCurrent)
            throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_NO_CURRENT_ELEMENT);
        return outPos;
    }

    bool UnpackUnalignedArrayIterator::setPosition(Coordinates const& newPos)
    {
        Dimensions const& dims = array.getArrayDesc().getDimensions(); 
        if ((uint64_t)newPos[0] >= dims[0].getLength()) {
            return hasCurrent = false;
        }
        outPos = newPos;
        chunkInitialized = false;
        outPos[0] = newPos[0] - (newPos[0] % dims[0].getChunkInterval());
        array.out2in(outPos[0], inPos);
        inputIterator->setPosition(inPos); 
        return hasCurrent = true;
    }

    UnpackUnalignedArrayIterator::UnpackUnalignedArrayIterator(UnpackUnalignedArray const& arr, AttributeID attrID, boost::shared_ptr<ConstArrayIterator> inputIterator)
    : DelegateArrayIterator(arr, attrID, inputIterator),
      array(arr),
      inPos(arr.dims.size()),
      outPos(1)
    {
        reset();
    }

    //
    // UnpackUnaligned array methods
    //

    Coordinate UnpackUnalignedArray::in2out(Coordinates const& inPos)  const
    {
        uint64_t cellNo = PhysicalBoundaries::getCellNumber(inPos, dims);
        if (cellNo >= INFINITE_LENGTH)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "Internal inconsistency reshaping coordinates";
        }
        return cellNo;
    }

    void UnpackUnalignedArray::out2in(Coordinate outPos, Coordinates& inPos)  const
    {
        uint64_t offset = outPos;
        inPos = PhysicalBoundaries::getCoordinates(offset, dims);
    }

    DelegateChunk* UnpackUnalignedArray::createChunk(DelegateArrayIterator const* iterator, AttributeID id) const
    {
       return new UnpackUnalignedChunk(*this, *iterator, id);
    }

    DelegateArrayIterator* UnpackUnalignedArray::createArrayIterator(AttributeID id) const
    {
        return new UnpackUnalignedArrayIterator(*this, id, inputArray->getConstIterator(id < dims.size() ? 0 : id - dims.size()));
    }    

    UnpackUnalignedArray::UnpackUnalignedArray(ArrayDesc const& desc, boost::shared_ptr<Array> const& array, const boost::shared_ptr<Query>& query)
    : DelegateArray(desc, array),
      dims(array->getArrayDesc().getDimensions())
    {
        assert(query);
        _query=query;
    } 
}

