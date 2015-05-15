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
 * @file ThinArray.cpp
 *
 * @brief Thin array implementation
 *
 * @author Konstantin Knizhnik <knizhnik@garret.ru>
 */

#include "ThinArray.h"
#include "system/Exceptions.h"

namespace scidb {

    using namespace boost;
    using namespace std;

    Coordinate computeLastCoordinate(uint64_t dimensionLength,
                                     Coordinate dimensionStart,
                                     Coordinate thinStartingPoint,
                                     Coordinate stepInterval)
    {
        // Integer truncation desirable here as we want the floor of the result
        // which guarantees that the returned coordinate will be within a chunk.
        return ((dimensionLength - thinStartingPoint + 
                 dimensionStart + stepInterval - 1) / stepInterval - 1);
    }

    //
    // Thin chunk iterator methods
    //
    int ThinChunkIterator::getMode()
    {
        return mode;
    }

    void ThinChunkIterator::reset()
    {
        outPos = first;
        outPos[outPos.size()-1] -= 1;
        hasCurrent = true;
        ++(*this);
    }

    void ThinChunkIterator::operator++()
    {
        size_t nDims = outPos.size();
        while (true) { 
            size_t i = nDims-1;
            while (++outPos[i] > last[i]) { 
                if (i == 0) {
                    hasCurrent = false;
                    return;
                }
                outPos[i] = first[i];
                i -= 1;
            }
            array.out2in(outPos, inPos);            
            if (inputIterator && inputIterator->setPosition(inPos)) {
                hasCurrent = true;
                return;
            }
        }
    }

    bool ThinChunkIterator::setPosition(Coordinates const& newPos)
    {
        array.out2in(newPos, inPos);
        outPos = newPos;
        if (inputIterator && inputIterator->setPosition(inPos)) {
            return hasCurrent = true;
        }
        return hasCurrent = false;
    }

    Coordinates const& ThinChunkIterator::getPosition()
    {
        if (!hasCurrent)
            throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_NO_CURRENT_ELEMENT);
        return outPos;
    }

    Value& ThinChunkIterator::getItem()
    {
        if (!hasCurrent)
            throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_NO_CURRENT_ELEMENT);
        return inputIterator->getItem();
    }
    
    bool ThinChunkIterator::end()
    { 
        return !hasCurrent;
    }

    bool ThinChunkIterator::isEmpty()
    {
        if (!hasCurrent)
            throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_NO_CURRENT_ELEMENT);
        return inputIterator->isEmpty();
    }

    ConstChunk const& ThinChunkIterator::getChunk()
    {
        return chunk;
    }
 
    ThinChunkIterator::ThinChunkIterator(ThinArray const& arr, ThinChunk const& chk, int iterationMode)
    : array(arr),
      chunk(chk),
      outPos(arr.getArrayDesc().getDimensions().size()),
      inPos(outPos.size()),
      first(chunk.getFirstPosition(!(iterationMode & IGNORE_OVERLAPS))),
      last(chunk.getLastPosition(!(iterationMode & IGNORE_OVERLAPS))),
      mode(iterationMode)
    {
        
        inputIterator = chk.srcChunk->getConstIterator(iterationMode & ~INTENDED_TILE_MODE);
        reset();
    }

    //
    // Thin chunk methods
    //
    boost::shared_ptr<ConstChunkIterator> ThinChunk::getConstIterator(int iterationMode) const
    {
        return boost::shared_ptr<ConstChunkIterator>(new ThinChunkIterator(array, *this, iterationMode));
    }

    void ThinChunk::initialize(Coordinates const& pos)
    {
        ArrayDesc const& desc = array.getArrayDesc();
        Address addr(attrID, pos);
        chunk.initialize(&array, &desc, addr, desc.getAttributes()[attrID].getDefaultCompressionMethod());
        srcChunk = &iterator.getInputIterator()->getChunk();
        setInputChunk(chunk);
    }

    ThinChunk::ThinChunk(ThinArray const& arr, DelegateArrayIterator const& iterator, AttributeID attrID)
    : DelegateChunk(arr, iterator, attrID, false),
      array(arr)
    {
    }
      
    //
    // Thin array iterator
    //
    ConstChunk const& ThinArrayIterator::getChunk()
    {
        if (!chunkInitialized) 
        { 
            ((ThinChunk&)*chunk).initialize(getPosition());
            chunkInitialized = true;
        }
        return *chunk;
    }
    
    Coordinates const& ThinArrayIterator::getPosition()
    {
        array.in2out(inputIterator->getPosition(), outPos);
        return outPos;
    }

    bool ThinArrayIterator::setPosition(Coordinates const& newPos)
    {
        outPos = newPos;
        array.getArrayDesc().getChunkPositionFor(outPos);
        array.out2in(outPos, inPos);
        chunkInitialized = false;
        return inputIterator->setPosition(inPos);
    }

    bool ThinArrayIterator::end()
    {
        bool atEnd = true;
        
        if (!inputIterator->end()) {
            Coordinates const& currentPosition = getPosition();
            ArrayDesc const& arrayDescriptor = array.getArrayDesc();
            atEnd = !arrayDescriptor.contains(currentPosition);
        }

        return atEnd;
    }

    ThinArrayIterator::ThinArrayIterator(ThinArray const& arr, AttributeID attrID, boost::shared_ptr<ConstArrayIterator> inputIterator)
    : DelegateArrayIterator(arr, attrID, inputIterator),
      array(arr),
      inPos(arr.getArrayDesc().getDimensions().size()),
      outPos(inPos.size())
    {
    }

    //
    // Thin array methods
    //

    void ThinArray::out2in(Coordinates const& outPos, Coordinates& inPos)  const
    { 
        for (size_t i = 0, n = outPos.size(); i < n; i++) { 
            inPos[i] = _from[i] + outPos[i] * _step[i];
        }
    }

    void ThinArray::in2out(Coordinates const& inPos, Coordinates& outPos)  const
    { 
        for (size_t i = 0, n = inPos.size(); i < n; i++) { 
            outPos[i] = (inPos[i] - _from[i] + _step[i] - 1) / _step[i];
        }
    }

    DelegateChunk* ThinArray::createChunk(DelegateArrayIterator const* iterator, AttributeID id) const
    {
       return new ThinChunk(*this, *iterator, id);
    }

    DelegateArrayIterator* ThinArray::createArrayIterator(AttributeID id) const
    {
        return new ThinArrayIterator(*this, id, inputArray->getConstIterator(id));
    }    

    ThinArray::ThinArray(ArrayDesc const& desc, boost::shared_ptr<Array> const& array, Coordinates const& from, Coordinates const& step)
    : DelegateArray(desc, array),
      _from(from),
      _step(step)
    {
    } 
}

