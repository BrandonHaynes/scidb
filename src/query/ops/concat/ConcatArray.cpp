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
 * ConcatArray.cpp
 *
 *  Created on: May 12, 2010
 *      Author: Knizhnik
 */

#include <query/Operator.h>
#include <array/Metadata.h>
#include <array/Array.h>
#include "ConcatArray.h"

namespace scidb 
{
    using namespace boost;
    using namespace std;

    const int CONCAT_DIM = 0;
        
    // 
    // Concat chunk methods
    //

	Coordinates const& ConcatChunk::getFirstPosition(bool withOverlap) const
    {
        return withOverlap ? firstPosWithOverlap : firstPos;
    }
        
	Coordinates const& ConcatChunk::getLastPosition(bool withOverlap) const
    {
        return withOverlap ? lastPosWithOverlap : lastPos;
    }

    void ConcatChunk::setInputChunk(ConstChunk const& inputChunk)
    {
        DelegateChunk::setInputChunk(inputChunk);
        ConcatArrayIterator const& arrayIterator((ConcatArrayIterator const&)iterator);
        Coordinate shift = arrayIterator.shift;
        isClone = inputChunk.getArrayDesc().getDimensions()[CONCAT_DIM].getChunkOverlap() == 0;
        direct = true;

        firstPos = inputChunk.getFirstPosition(false);
        firstPosWithOverlap = inputChunk.getFirstPosition(true);
        lastPos = inputChunk.getLastPosition(false);
        lastPosWithOverlap = inputChunk.getLastPosition(true);

        if (shift != 0) { 
            firstPos[CONCAT_DIM] += shift;
            firstPosWithOverlap[CONCAT_DIM] += shift;
            lastPos[CONCAT_DIM] += shift;
            lastPosWithOverlap[CONCAT_DIM] += shift;
        }
    }

    void ConcatChunk::setProxy()
    {
        DelegateChunk::setInputChunk(shapeChunk);
        isClone = false;
        direct = false;

        firstPos = shapeChunk.getFirstPosition(false);
        firstPosWithOverlap = shapeChunk.getFirstPosition(true);
        lastPos = shapeChunk.getLastPosition(false);
        lastPosWithOverlap = shapeChunk.getLastPosition(true);        
    }
    
      
    ConcatChunk::ConcatChunk(ConcatArray const& array, ConcatArrayIterator const& iterator, AttributeID attrID)
    : DelegateChunk(array, iterator, attrID, true)
    {
    }

    // 
    // Concat direct chunk iterator methods
    //
    
	Coordinates const& ConcatDirectChunkIterator::getPosition()
    {
        Coordinates const& relPos = inputIterator->getPosition();
        ConcatArrayIterator const& arrayIterator((ConcatArrayIterator const&)chunk->getArrayIterator());
        if (arrayIterator.shift != 0) { 
            currPos = relPos;
            currPos[CONCAT_DIM] += arrayIterator.shift;
            return currPos;
        }
        return relPos;
    }

	bool ConcatDirectChunkIterator::setPosition(Coordinates const& pos)
    {
        ConcatArrayIterator const& arrayIterator((ConcatArrayIterator const&)chunk->getArrayIterator());
        if (arrayIterator.shift != 0) { 
            Coordinates relPos = pos;
            relPos[CONCAT_DIM] -= arrayIterator.shift;
            return inputIterator->setPosition(relPos);
        } else {
            return inputIterator->setPosition(pos);
        }
    }

    ConcatDirectChunkIterator::ConcatDirectChunkIterator(DelegateChunk const* chunk, int iterationMode)
    : DelegateChunkIterator(chunk, iterationMode)
    {
    }
 
    //
    // Concat chunk iterator methods
    //

    void ConcatChunkIterator::reset()
    {
        first = chunk->getFirstPosition(!(mode & IGNORE_OVERLAPS));
        last = chunk->getLastPosition(!(mode & IGNORE_OVERLAPS));
        outPos = first;
        outPos[outPos.size()-1] -= 1;
        ++(*this);
    }

    int ConcatChunkIterator::getMode()
    {
        return mode;
    }
 
    void ConcatChunkIterator::operator++()
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
            ConcatArrayIterator const& concatArrayIterator((ConcatArrayIterator const&)chunk->getArrayIterator());
            boost::shared_ptr<ConstArrayIterator> arrayIterator; 
            inPos = outPos;
            if (outPos[CONCAT_DIM] <= concatArrayIterator.lastLeft) { 
                arrayIterator = concatArrayIterator.leftIterator;
            } else {
                inPos[CONCAT_DIM] -=  concatArrayIterator.lastLeft + 1 - concatArrayIterator.firstRight;
                arrayIterator = concatArrayIterator.rightIterator;
            }
            chunkIterator.reset();
            if (arrayIterator->setPosition(inPos)) { 
                ConstChunk const& chunk = arrayIterator->getChunk();
                chunkIterator = chunk.getConstIterator(mode);
                if (chunkIterator->setPosition(inPos)) { 
                    hasCurrent = true;
                    return;
                }
            }
        }
    }

    bool ConcatChunkIterator::end()
    {
        return !hasCurrent;
    }

    Value& ConcatChunkIterator::getItem()
    {
         if (!hasCurrent)
             throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_NO_CURRENT_ELEMENT);
        return chunkIterator->getItem();
    }
    
    bool ConcatChunkIterator::isEmpty()
    {
        if (!hasCurrent)
            throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_NO_CURRENT_ELEMENT);
        return chunkIterator->isEmpty();
    }

    bool ConcatChunkIterator::setPosition(Coordinates const& pos)
    {
        outPos = inPos = pos;
        ConcatArrayIterator const& concatArrayIterator((ConcatArrayIterator const&)chunk->getArrayIterator());
        boost::shared_ptr<ConstArrayIterator> arrayIterator; 
        if (pos[CONCAT_DIM] <= concatArrayIterator.lastLeft) { 
            arrayIterator = concatArrayIterator.leftIterator;
        } else {
            inPos[CONCAT_DIM] -=  concatArrayIterator.lastLeft + 1 - concatArrayIterator.firstRight;
            arrayIterator = concatArrayIterator.rightIterator;
        }
        chunkIterator.reset();
        if (arrayIterator->setPosition(inPos)) { 
            ConstChunk const& chunk = arrayIterator->getChunk();
            chunkIterator = chunk.getConstIterator(mode);
            if (chunkIterator->setPosition(inPos)) { 
                return hasCurrent = true;
            }
        }
        return hasCurrent = false;
    }

    Coordinates const& ConcatChunkIterator::getPosition()
    {
        if (!hasCurrent)
            throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_NO_CURRENT_ELEMENT);
        return outPos;
    }

    ConcatChunkIterator::ConcatChunkIterator(DelegateChunk const* chunk, int iterationMode)
    : DelegateChunkIterator(chunk, iterationMode),
      mode(iterationMode & ~INTENDED_TILE_MODE)
    {
        reset();
    }


   
    // 
    // Concat array iterator methods
    //

    bool ConcatArrayIterator::setPosition(Coordinates const& pos)
    {
        outPos = pos;
        array.getArrayDesc().getChunkPositionFor(outPos);
        Coordinate lastPos = outPos[CONCAT_DIM] + ((ConcatArray&)array).dims[CONCAT_DIM].getChunkInterval() - 1;
        inPos = outPos;
        chunkInitialized = false;
        if (outPos[CONCAT_DIM] <= lastLeft) { 
            shift = 0;
            inputIterator = leftIterator;
            if (inputIterator->setPosition(inPos)) { 
                return hasCurrent = true;
            } else if (lastPos <= lastLeft) { 
                return hasCurrent = false;
            }
        }
        inputIterator = rightIterator;
        shift = lastLeft + 1 - firstRight;
        inPos[CONCAT_DIM] -= shift;
        ArrayDesc const& rightDesc = ((ConcatArray&)array).rightArray->getArrayDesc();
        rightDesc.getChunkPositionFor(inPos);
        while (!inputIterator->setPosition(inPos)) { 
            inPos[CONCAT_DIM] += rightDesc.getDimensions()[CONCAT_DIM].getChunkInterval();
            if (inPos[CONCAT_DIM] + shift > lastPos) { 
                return hasCurrent = false;
            }
        }
        return hasCurrent = true;
    }

    ConstChunk const& ConcatArrayIterator::getChunk()
    {
        if (!hasCurrent)
            throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_NO_CURRENT_ELEMENT);
        ConcatChunk& cc = (ConcatChunk&)*chunk;
        if (!chunkInitialized) { 
            if (((ConcatArray&)array).simpleAppend || Coordinate(outPos[CONCAT_DIM] + concatChunkInterval) - 1 <= lastLeft) { 
                cc.setInputChunk(inputIterator->getChunk());
            } else { 
                ArrayDesc const& desc = array.getArrayDesc();
                Address addr(attr, outPos);
                cc.shapeChunk.initialize(&array, &desc, addr, desc.getAttributes()[attr].getDefaultCompressionMethod());
                cc.setProxy();
            }
            chunkInitialized = true;
        }
        return cc;
    }

    Coordinates const& ConcatArrayIterator::getPosition()
    {
        if (!hasCurrent)
            throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_NO_CURRENT_ELEMENT);
        return outPos;
    }

    void ConcatArrayIterator::reset()
    {
        chunkInitialized = false;
        if (((ConcatArray&)array).simpleAppend) { 
            inputIterator = leftIterator;
            inputIterator->reset();
            if (inputIterator->end()) { 
                inputIterator = rightIterator;
                inputIterator->reset();
                shift = lastLeft + 1 - firstRight;
                if (!inputIterator->end()) { 
                    outPos = inputIterator->getPosition();
                    outPos[CONCAT_DIM] += shift;
                }
            } else { 
                shift = 0;
                outPos = inputIterator->getPosition();
            }
            hasCurrent = !inputIterator->end();
        } else { 
            Dimensions const& dims = ((ConcatArray&)array).dims;
            for (size_t i = 0, nDims = dims.size(); i < nDims; i++) { 
                outPos[i] = dims[i].getStartMin();
            }
            nextVisible();
        }            
    }

    bool ConcatArrayIterator::end()
    {
        return !hasCurrent;
    }

    void ConcatArrayIterator::operator ++()
    {
        chunkInitialized = false;
        if (((ConcatArray&)array).simpleAppend) { 
            ++(*inputIterator);
            if (inputIterator->end()) { 
                if (shift != 0) {                
                    hasCurrent = false;
                    return;
                } else { 
                    shift = lastLeft + 1 - firstRight;
                    inputIterator = rightIterator;
                    if (((ConcatArray&)array).simpleAppend) { 
                        inputIterator->reset();
                        hasCurrent = !inputIterator->end();
                        if (hasCurrent) { 
                            outPos = inputIterator->getPosition();
                            outPos[CONCAT_DIM] += shift;
                        }
                        return;
                    }
                }
            } else { 
                outPos = inputIterator->getPosition();
                if (shift != 0) { 
                    outPos[CONCAT_DIM] += shift;
                    array.getArrayDesc().getChunkPositionFor(outPos);
                }
                hasCurrent = true;
                return;
            }
        }         
        Dimensions const& dims = ((ConcatArray&)array).dims;
        size_t nDims = dims.size();
        outPos[nDims-1] += dims[nDims-1].getChunkInterval();
        nextVisible();
    }

    bool ConcatArrayIterator::setInputPosition(size_t i) 
    { 
        chunkInitialized = false;
        Dimensions const& dims = ((ConcatArray&)array).dims;
        if (i == dims.size()) {
            return inputIterator->setPosition(inPos);
        }
        if (setInputPosition(i+1)) { 
            return true;
        }
        size_t interval = dims[i].getChunkInterval() - 1;
        inPos[i] += interval;
        bool rc = setInputPosition(i+1);
        inPos[i] -= interval;
        return rc;
    }


    bool ConcatArrayIterator::nextVisible()
    {
        Dimensions const& dims = ((ConcatArray&)array).dims; 
        size_t nDims = dims.size();
        while (true) { 
            size_t i = nDims-1;
            while (outPos[i] > dims[i].getEndMax()) { 
                if (i == 0) { 
                    return hasCurrent = false;
                }
                outPos[i] = dims[i].getStartMin();
                i -= 1;
                outPos[i] += dims[i].getChunkInterval();
            }            
            if (outPos[CONCAT_DIM] + dims[CONCAT_DIM].getChunkInterval() - 1 <= lastLeft) { 
                inputIterator = leftIterator;
                shift = 0;
                if (inputIterator->setPosition(outPos)) { 
                    return hasCurrent = true;
                }
            } else if (outPos[CONCAT_DIM] > lastLeft) { 
                inputIterator = rightIterator;
                inPos = outPos;
                shift = lastLeft + 1 - firstRight;
                inPos[CONCAT_DIM] -= shift;
                if (setInputPosition(0)) { 
                    return hasCurrent = true;
                }
            } else { 
                inputIterator = leftIterator;
                inPos = outPos;
                if (setInputPosition(0)) { 
                    shift = 0;
                    return hasCurrent = true;
                }
                inputIterator = rightIterator;
                shift = lastLeft + 1 - firstRight;
                inPos[CONCAT_DIM] -= shift;
                if (setInputPosition(0)) { 
                    return hasCurrent = true;
                }
            }
            outPos[nDims-1] += dims[nDims-1].getChunkInterval();
        }
    }


    ConcatArrayIterator::ConcatArrayIterator(ConcatArray const& array, AttributeID attrID)
    : DelegateArrayIterator(array, attrID, array.leftArray->getConstIterator(attrID)),
        leftIterator(inputIterator),
        rightIterator(array.rightArray->getConstIterator(attrID)),
        outPos(array.dims.size()),
        lastLeft(array.lastLeft),
        firstRight(array.firstRight),
        concatChunkInterval(array.concatChunkInterval)
    {
        reset();
    }

    
    //
    // Concat array methods
    //

    DelegateArrayIterator* ConcatArray::createArrayIterator(AttributeID attrID) const
    {
        return new ConcatArrayIterator(*this, attrID);
    }

    DelegateChunk* ConcatArray::createChunk(DelegateArrayIterator const* iterator, AttributeID id) const
    {
        return new ConcatChunk(*this, *(ConcatArrayIterator const*)iterator, id);
    }

    DelegateChunkIterator* ConcatArray::createChunkIterator(DelegateChunk const* chunk, int iterationMode) const
    {
        return ((ConcatChunk*)chunk)->direct 
            ? (DelegateChunkIterator*)new ConcatDirectChunkIterator(chunk, iterationMode)
            : (DelegateChunkIterator*)new ConcatChunkIterator(chunk, iterationMode);
        
    }

    ConcatArray::ConcatArray(ArrayDesc const& array, boost::shared_ptr<Array> const& left, boost::shared_ptr<Array> const& right)
    : DelegateArray(array, left),
      leftArray(left->getArrayDesc().getAttributes().size() == array.getAttributes().size() ? left : boost::shared_ptr<Array>(new NonEmptyableArray(left))),
      rightArray(right->getArrayDesc().getAttributes().size() == array.getAttributes().size() ? right : boost::shared_ptr<Array>(new NonEmptyableArray(right))),
      dims(desc.getDimensions())
    {
        Dimensions const& leftDimensions = left->getArrayDesc().getDimensions();
        Dimensions const& rightDimensions = right->getArrayDesc().getDimensions();
        lastLeft = leftDimensions[CONCAT_DIM].getEndMax();
        firstRight = rightDimensions[CONCAT_DIM].getStartMin();
        concatChunkInterval = leftDimensions[CONCAT_DIM].getChunkInterval() + leftDimensions[CONCAT_DIM].getChunkOverlap();
        size_t nDims = leftDimensions.size();
        if (leftDimensions[CONCAT_DIM].getChunkOverlap() != 0 
            || leftDimensions[CONCAT_DIM].getLength() % leftDimensions[CONCAT_DIM].getChunkInterval() != 0)
        {
            simpleAppend = false;
        }
        else
        {
            simpleAppend = true;            
            for (size_t i = 0; i < nDims; i++) { 
                if (leftDimensions[i].getChunkInterval() != rightDimensions[i].getChunkInterval()
                    || leftDimensions[i].getChunkOverlap() != rightDimensions[i].getChunkOverlap())
                {
                    simpleAppend = false;
                    break;
                }
            }
        }
    }
}
