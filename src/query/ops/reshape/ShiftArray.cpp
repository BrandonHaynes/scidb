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
 * @file ShiftArray.cpp
 *
 * @brief Shift array implementation
 *
 * @author Konstantin Knizhnik <knizhnik@garret.ru>
 */

#include "ShiftArray.h"

using namespace boost;
using namespace std;

namespace scidb
{
    //
    // Shift chunk iterator methods
    //
    bool ShiftChunkIterator::setPosition(Coordinates const& newPos)
    {
        array.out2in(newPos, inPos);
        return inputIterator->setPosition(inPos);
    }

    Coordinates const& ShiftChunkIterator::getPosition()
    {
        array.in2out(inputIterator->getPosition(), outPos);
        return outPos;
    }
 
    ShiftChunkIterator::ShiftChunkIterator(ShiftArray const& arr, DelegateChunk const* chunk, int iterationMode)
    : DelegateChunkIterator(chunk, iterationMode),
      array(arr),
      outPos(arr.outDims.size()),
      inPos(arr.inDims.size())
    {
    }

    //
    // Shift chunk methods
    //

    Coordinates const& ShiftChunk::getFirstPosition(bool withOverlap) const
    {
        return firstPos;
    }

    Coordinates const& ShiftChunk::getLastPosition(bool withOverlap) const
    {
        return lastPos;
    }

    void ShiftChunk::setInputChunk(ConstChunk const& inputChunk)
    {
        DelegateChunk::setInputChunk(inputChunk);
        isClone = true;
        array.in2out(inputChunk.getFirstPosition(false), firstPos);
        array.in2out(inputChunk.getLastPosition(false), lastPos);
    }

    ShiftChunk::ShiftChunk(ShiftArray const& arr, DelegateArrayIterator const& iterator, AttributeID attrID)
    : DelegateChunk(arr, iterator, attrID, false),
      array(arr),
      firstPos(arr.outDims.size()),
      lastPos(arr.outDims.size())
    {
    }
      
    //
    // Shift array iterator
    //
    Coordinates const& ShiftArrayIterator::getPosition()
    {
        array.in2out(inputIterator->getPosition(), outPos);
        return outPos;
    }

    bool ShiftArrayIterator::setPosition(Coordinates const& newPos)
    {
        chunkInitialized = false;
        array.out2in(newPos, inPos);
        return inputIterator->setPosition(inPos); 
    }

    ShiftArrayIterator::ShiftArrayIterator(ShiftArray const& arr, AttributeID attrID, boost::shared_ptr<ConstArrayIterator> inputIterator)
    : DelegateArrayIterator(arr, attrID, inputIterator),
      array(arr),
      inPos(arr.inDims.size()),
      outPos(arr.outDims.size())
    {
        reset();
    }

    //
    // Shift array methods
    //

    void ShiftArray::in2out(Coordinates const& inPos, Coordinates& outPos)  const
    { 
        for (size_t i = 0, n = inDims.size(); i < n; i++) { 
            outPos[i] = inPos[i] + outDims[i].getStartMin() - inDims[i].getStartMin();
        }
    }

    void ShiftArray::out2in(Coordinates const& outPos, Coordinates& inPos)  const
    { 
        for (size_t i = 0, n = outDims.size(); i < n; i++) { 
            inPos[i] = outPos[i] + inDims[i].getStartMin() - outDims[i].getStartMin();
        }
    }

    DelegateChunkIterator* ShiftArray::createChunkIterator(DelegateChunk const* chunk, int iterationMode) const
    {
        return new ShiftChunkIterator(*this, chunk, iterationMode);
    }

    DelegateChunk* ShiftArray::createChunk(DelegateArrayIterator const* iterator, AttributeID id) const
    {
       return new ShiftChunk(*this, *iterator, id);
    }

    DelegateArrayIterator* ShiftArray::createArrayIterator(AttributeID id) const
    {
        return new ShiftArrayIterator(*this, id, inputArray->getConstIterator(id));
    }    

    ShiftArray::ShiftArray(ArrayDesc const& desc, boost::shared_ptr<Array> const& array)
    : DelegateArray(desc, array),
      inDims(array->getArrayDesc().getDimensions()),
      outDims(desc.getDimensions())
    {
    } 
}

