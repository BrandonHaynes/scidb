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
 * @file SliceArray.cpp
 *
 * @brief Slice array implementation
 *
 * @author Konstantin Knizhnik <knizhnik@garret.ru>
 */

#include <system/Exceptions.h>
#include "SliceArray.h"

#include <stdio.h>

namespace scidb
{
    using namespace boost;
    
    //
    // Slice chunk methods
    //
    Array const& SliceChunk::getArray() const 
    { 
        return array;
    }

    const ArrayDesc& SliceChunk::getArrayDesc() const
    {
        return array.desc;
    }
    
    const AttributeDesc& SliceChunk::getAttributeDesc() const
    {
        return array.desc.getAttributes()[attr];
    }

    int SliceChunk::getCompressionMethod() const
    {
        return inputChunk->getCompressionMethod();
    }

    Coordinates const& SliceChunk::getFirstPosition(bool withOverlap) const
    {
        return withOverlap ? firstPosWithOverlap : firstPos;
    }
        
    Coordinates const& SliceChunk::getLastPosition(bool withOverlap) const
    {
       return withOverlap ? lastPosWithOverlap : lastPos;
     }

    boost::shared_ptr<ConstChunkIterator> SliceChunk::getConstIterator(int iterationMode) const
    {
        return boost::shared_ptr<ConstChunkIterator>(array.simple
                                              ? (ConstChunkIterator*)new SimpleSliceChunkIterator(*this, iterationMode)
                                              : (ConstChunkIterator*)new SliceChunkIterator(*this, iterationMode));
    }

    SliceChunk::SliceChunk(SliceArray const& slice, AttributeID attrID)
    : array(slice), 
      attr(attrID),
      firstPos(slice.desc.getDimensions().size()),
      firstPosWithOverlap(firstPos.size()),
      lastPos(firstPos.size()),
      lastPosWithOverlap(firstPos.size())
    {    
    }
     
    void SliceChunk::setInputChunk(ConstChunk const* chunk)
    {
        inputChunk = chunk;
        array.mapPos(firstPos, chunk->getFirstPosition(false));                
        array.mapPos(firstPosWithOverlap, chunk->getFirstPosition(true));
        array.mapPos(lastPos, chunk->getLastPosition(false));        
        array.mapPos(lastPosWithOverlap, chunk->getLastPosition(true));
    }

    //
    // Slice chunk iterator methods
    //
    int SliceChunkIterator::getMode()
    {
        return inputIterator->getMode();
    }

    Value& SliceChunkIterator::getItem()
    {
        if (!hasCurrent)
            throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_NO_CURRENT_ELEMENT);
        return inputIterator->getItem();
    }

    bool SliceChunkIterator::isEmpty()
    {
        return inputIterator->isEmpty();
    }

    bool SliceChunkIterator::end()
    {
        return !hasCurrent;
    }

    void SliceChunkIterator::moveNext()
    {
        uint64_t mask = array.mask;
      TryPos:
        for (int i = inPos.size(); --i >= 0;) { 
            if (!((mask >> i) & 1)) { 
                while (++inPos[i] <= lastPos[i]) { 
                    if (inputIterator->setPosition(inPos)) { 
                        hasCurrent = true;
                        return;
                    }
                    goto TryPos;
                }
                inPos[i] = firstPos[i];
            }
        }
        hasCurrent = false;
    }

    void SliceChunkIterator::operator ++()
    {
        moveNext();
    }

    void SliceChunkIterator::reset() 
    {
        int shift = 1;
        firstPos = inputIterator->getFirstPosition();
        lastPos = inputIterator->getLastPosition();
        uint64_t mask = array.mask;
        for (int i = inPos.size(); --i >= 0;) { 
            if (!((mask >> i) & 1)) { 
                inPos[i] = firstPos[i] - shift;
                shift = 0;
            } else { 
                inPos[i] = array.slice[i];
            }
        }
        moveNext();
    }
    
    Coordinates const& SliceChunkIterator::getPosition() 
    {
        if (!hasCurrent)
            throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_NO_CURRENT_ELEMENT);
        array.mapPos(outPos, inPos);
        return outPos;        
    }

    bool SliceChunkIterator::setPosition(Coordinates const& pos)
    {
        uint64_t mask = array.mask;
        for (size_t i = 0, j = 0, n = inPos.size(); i < n; i++, mask >>= 1) { 
            if (!(mask & 1)) { 
                inPos[i] = pos[j++];
            }
        }
        return hasCurrent = inputIterator->setPosition(inPos);
    }

    ConstChunk const& SliceChunkIterator::getChunk()
    {
        return chunk;
    }

    SliceChunkIterator::SliceChunkIterator(SliceChunk const& aChunk, int iterationMode)
    : array(aChunk.array),
      chunk(aChunk),
      inputIterator(aChunk.inputChunk->getConstIterator(iterationMode & ~INTENDED_TILE_MODE)),
      inPos(array.inputDims.size()),
      outPos(array.desc.getDimensions().size())
    {
        reset();
    }

    //
    // Simple slice chunk iterator methods
    //
    int SimpleSliceChunkIterator::getMode()
    {
        return inputIterator->getMode();
    }

     Value& SimpleSliceChunkIterator::getItem()
    {
        return inputIterator->getItem();
    }

    bool SimpleSliceChunkIterator::isEmpty()
    {
        return inputIterator->isEmpty();
    }

    bool SimpleSliceChunkIterator::end()
    {
        return inputIterator->end();
    }

    void SimpleSliceChunkIterator::operator ++()
    {
        ++(*inputIterator);
    }

    void SimpleSliceChunkIterator::reset() 
    {
        inputIterator->reset();
    }
    
    Coordinates const& SimpleSliceChunkIterator::getPosition() 
    {
        array.mapPos(outPos, inputIterator->getPosition());
        return outPos;        
    }

    bool SimpleSliceChunkIterator::setPosition(Coordinates const& pos)
    {
        uint64_t mask = array.mask;
        for (size_t i = 0, j = 0, n = inPos.size(); i < n; i++, mask >>= 1) { 
            if (!(mask & 1)) { 
                inPos[i] = pos[j++];
            }
        }
        return inputIterator->setPosition(inPos);
    }

    ConstChunk const& SimpleSliceChunkIterator::getChunk()
    {
        return chunk;
    }

    SimpleSliceChunkIterator::SimpleSliceChunkIterator(SliceChunk const& aChunk, int iterationMode)
    : array(aChunk.array),
      chunk(aChunk),
      inputIterator(aChunk.inputChunk->getConstIterator(iterationMode & ~INTENDED_TILE_MODE)),
      inPos(array.inputDims.size()),
      outPos(array.desc.getDimensions().size())
    {
        uint64_t mask = array.mask;
        for (size_t i = 0, n = inPos.size(); i < n; i++, mask >>= 1) { 
            if (mask & 1) { 
                inPos[i] = array.slice[i];
            }
        }
    }

    //
    // Slice array iterator methods
    //
    SliceArrayIterator::SliceArrayIterator(SliceArray const& slice, AttributeID attrID) 
    : array(slice), 
      inputIterator(slice.inputArray->getConstIterator(attrID)),
      chunk(slice, attrID),
      inPos(slice.inputDims.size()),
      outPos(slice.desc.getDimensions().size()),
      chunkInitialized(false)
    {
        reset();
	}

    ConstChunk const& SliceArrayIterator::getChunk()
    {
        if (!hasCurrent)
            throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_NO_CURRENT_ELEMENT);
        if (!chunkInitialized) { 
            ConstChunk const& inputChunk = inputIterator->getChunk();
            chunk.setInputChunk(&inputChunk);
            chunkInitialized = true;
        }
        return chunk;
    }

	bool SliceArrayIterator::end()
	{
        return !hasCurrent;
    }

	void SliceArrayIterator::operator ++()
	{
        moveNext();
    }

    void SliceArrayIterator::moveNext()
    {
        Dimensions const& dims = array.inputDims;
        uint64_t mask = array.mask;
        chunkInitialized = false;
      TryPos:
        for (int i = inPos.size(); --i >= 0;) { 
            if (!((mask >> i) & 1)) { 
                while ((inPos[i] += dims[i].getChunkInterval()) <= dims[i].getEndMax()) { 
                    if (inputIterator->setPosition(inPos)) {
                        hasCurrent = true;
                        return;
                    }
                    goto TryPos;
                }
                inPos[i] = dims[i].getStartMin();
            }
        }
        hasCurrent = false;
    }

	Coordinates const& SliceArrayIterator::getPosition()
	{
        if (!hasCurrent)
            throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_NO_CURRENT_ELEMENT);
        array.mapPos(outPos, inPos);
        return outPos;
    }

	bool SliceArrayIterator::setPosition(Coordinates const& pos)
	{
        uint64_t mask = array.mask;
        for (size_t i = 0, j = 0, n = inPos.size(); i < n; i++, mask >>= 1) { 
            if (!(mask & 1)) { 
                inPos[i] = pos[j++];
            }
        }
        chunkInitialized = false;
        return hasCurrent = inputIterator->setPosition(inPos);
	}

	void SliceArrayIterator::reset()
	{
        Dimensions const& dims = array.inputDims;
        int j = -1;
        uint64_t mask = array.mask;
        for (int i = 0, n = inPos.size(); i < n ; mask >>= 1, i++) { 
            if (!(mask & 1)) { 
                inPos[i] = dims[i].getStartMin();
                j = i;
            } else { 
                inPos[i] = array.slice[i];
            }
        }
        assert(j != -1);
        inPos[j] -= dims[j].getChunkInterval();
        moveNext();
    }
    
    //
    // Infinite slice array iterator methods
    //
    InfiniteSliceArrayIterator::InfiniteSliceArrayIterator(SliceArray const& slice, AttributeID attrID) 
    : array(slice), 
      inputIterator(slice.inputArray->getConstIterator(attrID)),
      chunk(slice, attrID),
      inPos(slice.inputDims.size()),
      outPos(slice.desc.getDimensions().size())
    {
        uint64_t mask = array.mask;
        for (size_t i = 0, n = inPos.size(); i < n; i++, mask >>= 1) { 
            if (mask & 1) { 
                inPos[i] = array.slice[i];
            }
        }
        nextAvailable();
	}

    ConstChunk const& InfiniteSliceArrayIterator::getChunk()
    {
        if (!chunkInitialized) { 
            ConstChunk const& inputChunk = inputIterator->getChunk();
            chunk.setInputChunk(&inputChunk);
            chunkInitialized = true;
        }
        return chunk;
    }

	bool InfiniteSliceArrayIterator::end()
	{
        return inputIterator->end();
    }

	void InfiniteSliceArrayIterator::operator ++()
	{
        ++(*inputIterator);
        nextAvailable();
    }

    void InfiniteSliceArrayIterator::nextAvailable()
    {
        Dimensions const& dims = array.inputDims;
        size_t i, nDims = dims.size(); 
        chunkInitialized = false;
        while (!inputIterator->end()) { 
            Coordinates const& pos = inputIterator->getPosition();
            uint64_t mask = array.mask;
            for (i = 0; i < nDims; i++, mask >>= 1) { 
                if (mask & 1) { 
                    if (pos[i] > inPos[i] || pos[i] + dims[i].getChunkInterval() <= inPos[i]) { 
                        break;
                    }
                }
            }
            if (i == nDims) { 
                return;
            }
            ++(*inputIterator);
        }
    }

	Coordinates const& InfiniteSliceArrayIterator::getPosition()
	{
        array.mapPos(outPos, inputIterator->getPosition());
        return outPos;
    }

	bool InfiniteSliceArrayIterator::setPosition(Coordinates const& pos)
	{
        uint64_t mask = array.mask;
        for (size_t i = 0, j = 0, n = inPos.size(); i < n; i++, mask >>= 1) { 
            if (!(mask & 1)) { 
                inPos[i] = pos[j++];
            }
        }
        chunkInitialized = false;
        return inputIterator->setPosition(inPos);
	}

	void InfiniteSliceArrayIterator::reset()
	{
        inputIterator->reset();
        nextAvailable();
    }
    
    //
    // Slice array methods
    //
    SliceArray::SliceArray(ArrayDesc& aDesc, Coordinates const& aSlice, uint64_t aMask, boost::shared_ptr<Array> input)
    : desc(aDesc), 
      slice(aSlice), 
      mask(aMask),
      inputArray(input),
      inputDims(input->getArrayDesc().getDimensions())
	{
        useInfiniteIterator = false;
        simple = true;
        ArrayDesc const& inputDesc = input->getArrayDesc();
        double numChunksInSlice = 1; //number of logical chunks the resulting "slice" will contain.
        for (size_t i = 0, n = inputDims.size(); i < n; i++, aMask >>= 1) {
            if (!(aMask & 1)){
                numChunksInSlice *= inputDesc.getNumChunksAlongDimension(i);
            } else if (inputDims[i].getChunkInterval() != 1) { 
                simple = false;
            }
        }
        useInfiniteIterator = (numChunksInSlice > SLICE_INFINITE_ITERATOR_THRESHOLD);
    }
    
    string const& SliceArray::getName() const
    {
        return desc.getName(); 
    }

    ArrayID SliceArray::getHandle() const
    {
        return desc.getId(); 
    }

    const ArrayDesc& SliceArray::getArrayDesc() const
    { 
        return desc; 
    }

    boost::shared_ptr<ConstArrayIterator> SliceArray::getConstIterator(AttributeID id) const
	{
		return boost::shared_ptr<ConstArrayIterator>(useInfiniteIterator 
                                              ? (ConstArrayIterator*)new InfiniteSliceArrayIterator(*this, id) 
                                              : (ConstArrayIterator*)new SliceArrayIterator(*this, id));
	}   

    void SliceArray::mapPos(Coordinates& outPos, Coordinates const& inPos) const
    {
        uint64_t mask = this->mask;
        for (size_t i = 0, j = 0, n = inPos.size(); i < n; i++, mask >>= 1) {
            if (!(mask & 1)) { 
                outPos[j++] = inPos[i];
            }
        }
    }
}
