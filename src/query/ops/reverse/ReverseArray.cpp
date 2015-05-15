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
 * @file ReverseArray.cpp
 *
 * @brief Reverse array implementation
 *
 * @author Konstantin Knizhnik <knizhnik@garret.ru>
 */

#include <system/Exceptions.h>
#include "ReverseArray.h"

using namespace boost;

namespace scidb
{
    
    //
    // Reverse chunk methods
    //
    Coordinates const& ReverseChunk::getFirstPosition(bool withOverlap) const
    {
        return withOverlap ? firstPosWithOverlap : firstPos;
    }
        
    Coordinates const& ReverseChunk::getLastPosition(bool withOverlap) const
    {
       return withOverlap ? lastPosWithOverlap : lastPos;
     }

    boost::shared_ptr<ConstChunkIterator> ReverseChunk::getConstIterator(int iterationMode) const
    {
        return boost::shared_ptr<ConstChunkIterator>(new ReverseChunkIterator(*this, iterationMode));
    }

    ReverseChunk::ReverseChunk(ReverseArray const& reverse, DelegateArrayIterator const& iterator, AttributeID attrID)
    : DelegateChunk(reverse, iterator, attrID, false),
      array(reverse),
      firstPos(array.dims.size()),
      firstPosWithOverlap(firstPos.size()),
      lastPos(firstPos.size()),
      lastPosWithOverlap(firstPos.size())
    {    
    }
     
    void ReverseChunk::setPosition(Coordinates const& pos)
    {
        firstPos = pos; 
        Dimensions const& dims = array.dims;
        for (size_t i = 0, nDims = dims.size(); i < nDims; i++) { 
            firstPosWithOverlap[i] = firstPos[i] - dims[i].getChunkOverlap();
            if (firstPosWithOverlap[i] < dims[i].getStartMin()) {
                firstPosWithOverlap[i] = dims[i].getStartMin();
            }
            lastPos[i] = firstPos[i] + dims[i].getChunkInterval() - 1;
            if (lastPos[i] > dims[i].getEndMax()) { 
                lastPos[i] = dims[i].getEndMax();
            }
            lastPosWithOverlap[i] = lastPos[i] + dims[i].getChunkOverlap();
            if (lastPosWithOverlap[i] > dims[i].getEndMax()) { 
                lastPosWithOverlap[i] = dims[i].getEndMax();
            }
        }
    }


    //
    // Reverse chunk iterator methods
    //
    int ReverseChunkIterator::getMode()
    {
        return mode;
    }

    Value& ReverseChunkIterator::getItem()
    {
        if (!hasCurrent)
            throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_NO_CURRENT_ELEMENT);
        return inputIterator->getItem();
    }

    bool ReverseChunkIterator::isEmpty()
    {
        if (!hasCurrent)
            throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_NO_CURRENT_ELEMENT);
        return inputIterator->isEmpty();
    }

    bool ReverseChunkIterator::end()
    {
        return !hasCurrent;
    }

    void ReverseChunkIterator::operator ++()
    {
        bool withOverlaps = (mode & IGNORE_OVERLAPS) == 0;
        Coordinates const& first = chunk.getFirstPosition(withOverlaps);
        Coordinates const& last = chunk.getLastPosition(withOverlaps);
        size_t nDims = outPos.size();
        while (true) {
            size_t i = nDims-1;
            while (++outPos[i] > last[i]) { 
                outPos[i] = first[i];
                if (i-- == 0) { 
                    hasCurrent = false;
                    return;
                }
            }
            array.revert(outPos, inPos);
            if (!inputIterator || !inputChunk->contains(inPos, withOverlaps)) {
                boost::shared_ptr<ConstArrayIterator> inputArrayIterator = chunk.getArrayIterator().getInputIterator();
                inputIterator.reset();
                if (inputArrayIterator->setPosition(inPos)) { 
                    inputChunk = &inputArrayIterator->getChunk();
                    inputIterator = inputChunk->getConstIterator(mode);
                } else { 
                    continue;
                }
            }
            if (inputIterator->setPosition(inPos)) {
                hasCurrent = true;
                return;
            }
        }
    }

    Coordinates const& ReverseChunkIterator::getPosition() 
    {
        return outPos;
    }

    bool ReverseChunkIterator::setPosition(Coordinates const& pos)
    {
        outPos = pos;
        array.revert(outPos, inPos);
        if (!inputIterator || !inputChunk->contains(inPos, (mode & IGNORE_OVERLAPS) == 0)) {
            
            boost::shared_ptr<ConstArrayIterator> inputArrayIterator = chunk.getArrayIterator().getInputIterator();
            inputIterator.reset();
            if (inputArrayIterator->setPosition(inPos)) { 
                inputChunk = &inputArrayIterator->getChunk();
                inputIterator = inputChunk->getConstIterator(mode);
            } else { 
                return hasCurrent = false;
            }
        }
        return hasCurrent = inputIterator->setPosition(inPos);
    }

    void ReverseChunkIterator::reset()
    {
        outPos = chunk.getFirstPosition((mode & IGNORE_OVERLAPS) == 0);
        outPos[outPos.size()-1] -= 1; 
        ++(*this);
    }

    ConstChunk const& ReverseChunkIterator::getChunk()
    {
        return chunk;
    }

    ReverseChunkIterator::ReverseChunkIterator(ReverseChunk const& aChunk, int iterationMode)
    : array(aChunk.array),
      chunk(aChunk),
      inputChunk(&aChunk.getInputChunk()),
      inputIterator(inputChunk->getConstIterator(iterationMode & ~INTENDED_TILE_MODE)),
      outPos(array.dims.size()),
      inPos(outPos.size()),
      hasCurrent(false),
      mode(iterationMode & ~INTENDED_TILE_MODE)
    {
        reset();
    }

    //
    // Reverse array iterator methods
    //
    ReverseArrayIterator::ReverseArrayIterator(ReverseArray const& reverse, AttributeID attrID) 
    : DelegateArrayIterator(reverse, attrID, reverse.inputArray->getConstIterator(attrID)),
      array(reverse), 
      outPos(reverse.dims.size()),
      inPos(outPos.size())
    {
        reset();
	}

	bool ReverseArrayIterator::end()
	{
        return !hasCurrent;
    }

	void ReverseArrayIterator::operator ++()
	{
        if (!hasCurrent)
            throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_NO_CURRENT_ELEMENT);
        hasCurrent = nextAvailable();
    }

    bool ReverseArrayIterator::setInputPosition(size_t i) 
    { 
        Dimensions const& dims = array.dims;
        if (i == dims.size()) {
            return inputIterator->setPosition(inPos);
        }
        if (setInputPosition(i+1)) { 
            return true;
        }
        size_t interval = dims[i].getChunkInterval() - 1;
        inPos[i] -= interval;
        bool rc = setInputPosition(i+1);
        inPos[i] += interval;
        return rc;
    }

    bool ReverseArrayIterator::nextAvailable()
    {
        Dimensions const& dims = array.dims;
        chunkInitialized = false;
        while (true) { 
            size_t i = outPos.size()-1;
            while ((outPos[i] += dims[i].getChunkInterval()) > dims[i].getEndMax()) { 
                outPos[i] = dims[i].getStartMin();
                if (i-- == 0) { 
                    return false;
                }
            }
            array.revert(outPos, inPos);
            array.getArrayDesc().getChunkPositionFor(inPos);
            if (setInputPosition(0)) { 
                return true;
            }
        }
        return false;
    } 

	Coordinates const& ReverseArrayIterator::getPosition()
	{ 
        if (!hasCurrent)
            throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_NO_CURRENT_ELEMENT);
        return outPos;
	}

	bool ReverseArrayIterator::setPosition(Coordinates const& pos)
	{
        outPos = pos;
        chunkInitialized = false;
        array.revert(outPos, inPos);
        array.getArrayDesc().getChunkPositionFor(inPos);
        return hasCurrent = setInputPosition(0);
    }

	void ReverseArrayIterator::reset()
	{
		for (size_t i = 0, n = outPos.size(); i < n; i++) {
            outPos[i] = array.dims[i].getStartMin();
        }
        outPos[outPos.size()-1] -= array.dims[outPos.size()-1].getChunkInterval();
        hasCurrent = nextAvailable();
        chunkInitialized = false;
    }
    
    ConstChunk const& ReverseArrayIterator::getChunk()
    {
        if (!hasCurrent)
            throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_NO_CURRENT_ELEMENT);
        ReverseChunk& chunk = (ReverseChunk&)DelegateArrayIterator::getChunk();
        if (!chunkInitialized) {
            chunk.setPosition(outPos);
            chunkInitialized = true;
        }
        return chunk;
    }

    //
    // Reverse array methods
    //
    ReverseArray::ReverseArray(ArrayDesc const& array, boost::shared_ptr<Array> const& input)
    : DelegateArray(array, input),
      dims(desc.getDimensions())
	{
    }
    
    DelegateArrayIterator* ReverseArray::createArrayIterator(AttributeID attrID) const
    {
        return new ReverseArrayIterator(*this, attrID);
    }


    DelegateChunk* ReverseArray::createChunk(DelegateArrayIterator const* iterator, AttributeID attrID) const
    {
        return new ReverseChunk(*this, *iterator, attrID);       
    }

    void ReverseArray::revert(Coordinates const& src, Coordinates& dst) const
    {
        for (size_t i = 0, n = src.size(); i < n; i++) { 
            dst[i] = dims[i].getEndMax() - src[i] + dims[i].getStartMin();
        }
    }
}
