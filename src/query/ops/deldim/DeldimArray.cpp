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
 * @file DeldimArray.cpp
 *
 * @brief Deldim array implementation
 *
 * @author Konstantin Knizhnik <knizhnik@garret.ru>
 */

#include "DeldimArray.h"

namespace scidb {

    using namespace boost;
    using namespace std;

    inline void addDim(Coordinates const& src, Coordinates& dst) 
    {
        for (size_t i = 0, n = src.size(); i < n; i++) { 
            dst[i+1] = src[i];
        }
    }
        
    inline void delDim(Coordinates const& src, Coordinates& dst)
    {
        for (size_t i = 0, n = dst.size(); i < n; i++) { 
            dst[i] = src[i+1];
        }
    }
        

    //
    // Deldim chunk iterator methods
    //
    Coordinates const& DeldimChunkIterator::getPosition()
    {
        Coordinates const& inPos = DelegateChunkIterator::getPosition();
        delDim(inPos, outPos);
        return outPos;
    }

    bool DeldimChunkIterator::setPosition(Coordinates const& outPos)
    {
        addDim(outPos, inPos);
        return DelegateChunkIterator::setPosition(inPos);
    }

    DeldimChunkIterator::DeldimChunkIterator(DelegateChunk const* chunk, int iterationMode)
    : DelegateChunkIterator(chunk, iterationMode),
      outPos(chunk->getArrayDesc().getDimensions().size()),
      inPos(outPos.size()+1)
    {
        inPos[0] = chunk->getInputChunk().getFirstPosition(false)[0];
    }

    //
    // Deldim chunk methods
    //
	Coordinates const& DeldimChunk::getFirstPosition(bool withOverlap) const
    {
        return withOverlap ? _firstPosOverlap : _firstPos;
    }

	Coordinates const& DeldimChunk::getLastPosition(bool withOverlap) const
    {
        return withOverlap ? _lastPosOverlap : _lastPos;
    }

    DeldimChunk::DeldimChunk(DeldimArray const& array, DelegateArrayIterator const& iterator, AttributeID attrID)
    : DelegateChunk(array, iterator, attrID, true),
      _firstPos(array.getArrayDesc().getDimensions().size(),0),
      _lastPos(array.getArrayDesc().getDimensions().size(),0),
      _firstPosOverlap(array.getArrayDesc().getDimensions().size(),0),
      _lastPosOverlap(array.getArrayDesc().getDimensions().size(),0)
    {}

    void DeldimChunk::setInputChunk(ConstChunk const& inputChunk)
    {
        DelegateChunk::setInputChunk(inputChunk);
        Coordinates const& fpo = DelegateChunk::getFirstPosition(true);
        delDim(fpo, _firstPosOverlap);
        Coordinates const& fp = DelegateChunk::getFirstPosition(false);
        delDim(fp, _firstPos);
        Coordinates const& lp = DelegateChunk::getLastPosition(false);
        delDim(lp, _lastPos);
        Coordinates const& lpo = DelegateChunk::getLastPosition(true);
        delDim(lpo, _lastPosOverlap);
    }

    //
    // Deldim array iterator

    Coordinates const& DeldimArrayIterator::getPosition()
    {
        Coordinates const& inPos = DelegateArrayIterator::getPosition();
        delDim(inPos, outPos);
        return outPos;
    }

    bool DeldimArrayIterator::setPosition(Coordinates const& outPos)
    {
        addDim(outPos, inPos);
        return DelegateArrayIterator::setPosition(inPos);
    }

    DeldimArrayIterator::DeldimArrayIterator(DeldimArray const& array, AttributeID attrID, boost::shared_ptr<ConstArrayIterator> inputIterator)
    : DelegateArrayIterator(array, attrID, inputIterator),
      outPos(array.getArrayDesc().getDimensions().size()),
      inPos(outPos.size()+1)
    {
        inPos[0] = array.getInputArray()->getArrayDesc().getDimensions()[0].getStart();
    }

    //
    // Deldim array methods
    //

    DelegateChunk* DeldimArray::createChunk(DelegateArrayIterator const* iterator, AttributeID id) const
    {
       return new DeldimChunk(*this, *iterator, id);
    }

    DelegateChunkIterator* DeldimArray::createChunkIterator(DelegateChunk const* chunk, int iterationMode) const
    {
        return new DeldimChunkIterator(chunk, iterationMode);
    }

    DelegateArrayIterator* DeldimArray::createArrayIterator(AttributeID id) const
    {
        return new DeldimArrayIterator(*this, id, inputArray->getConstIterator(id));
    }    

    DeldimArray::DeldimArray(ArrayDesc const& desc, boost::shared_ptr<Array> const& array)
    : DelegateArray(desc, array)
    {
    } 
}

