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
 * JoinArray.h
 *
 *  Created on: Oct 22, 2010
 *      Author: Knizhnik
 */

#include "query/Operator.h"
#include "array/Metadata.h"
#include "array/Array.h"
#include "JoinArray.h"

using namespace std;
using namespace boost;

namespace scidb 
{
    //
    // Chunk iterator
    //
    inline bool JoinChunkIterator::join() 
    { 
        return joinIterator->setPosition(inputIterator->getPosition());
    }

    bool JoinChunkIterator::isEmpty()
    {    
        return inputIterator->isEmpty() || !join();
    }

    bool JoinChunkIterator::end()
    {
        return !hasCurrent;
    }

    void JoinChunkIterator::alignIterators() 
    { 
        while (!inputIterator->end()) {
            if (!(mode & IGNORE_EMPTY_CELLS) || join()) { 
                hasCurrent = true;
                return;
            }
            ++(*inputIterator);
        }
        hasCurrent = false;
    }

    void JoinChunkIterator::reset()
    {
        inputIterator->reset();
        joinIterator->reset();
        alignIterators();
    }

    bool JoinChunkIterator::setPosition(Coordinates const& pos)
    {
        if (inputIterator->setPosition(pos)) { 
            return hasCurrent = !(mode & IGNORE_EMPTY_CELLS) || join();
        }
        return hasCurrent = false;
    }

    void JoinChunkIterator::operator ++()
    {
        ++(*inputIterator);
        alignIterators();
    }

    JoinChunkIterator::JoinChunkIterator(JoinEmptyableArrayIterator const& arrayIterator, DelegateChunk const* chunk, int iterationMode)
    : DelegateChunkIterator(chunk, iterationMode),
      joinIterator(arrayIterator._joinIterator->getChunk().getConstIterator(iterationMode)),
      mode(iterationMode)
    {
        alignIterators();
    }
    
     Value& JoinBitmapChunkIterator::getItem()
    {
        value.setBool(inputIterator->getItem().getBool() && joinIterator->getItem().getBool());
        return value;
    }

    JoinBitmapChunkIterator::JoinBitmapChunkIterator(JoinEmptyableArrayIterator const& arrayIterator, DelegateChunk const* chunk, int iterationMode)
    : JoinChunkIterator(arrayIterator, chunk, iterationMode),
      value(TypeLibrary::getType(TID_BOOL))
    {
    }

    //
    // Array iterator
    //
    bool JoinEmptyableArrayIterator::setPosition(Coordinates const& pos)
    {
        chunkInitialized = false;        
        _hasCurrent = inputIterator->setPosition(pos) && _joinIterator->setPosition(pos);
        return _hasCurrent;
    }

    void JoinEmptyableArrayIterator::reset()
    {
        inputIterator->reset();
        _joinIterator->reset();
        alignIterators();
    }

    void JoinEmptyableArrayIterator::operator ++()
    {
        if (!_hasCurrent)
            throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_NO_CURRENT_POSITION);
        ++(*inputIterator);
        alignIterators();
    }

    bool JoinEmptyableArrayIterator::end()
    {
        return !_hasCurrent;
    }

    void JoinEmptyableArrayIterator::alignIterators()
    {
        _hasCurrent = false;
        chunkInitialized = false;
        while (!inputIterator->end()) {
            if (_joinIterator->setPosition(inputIterator->getPosition())) {
                _hasCurrent = true;
                break;
            }
            ++(*inputIterator);
        }
    }

    ConstChunk const& JoinEmptyableArrayIterator::getChunk()
    {
        chunk->overrideClone(!_chunkLevelJoin);
        return DelegateArrayIterator::getChunk();
    }

    JoinEmptyableArrayIterator::JoinEmptyableArrayIterator(JoinEmptyableArray const& array,
                                                           AttributeID attrID,
                                                           boost::shared_ptr<ConstArrayIterator> input,
                                                           boost::shared_ptr<ConstArrayIterator> join,
                                                           bool chunkLevelJoin)
    : DelegateArrayIterator(array, attrID, input),
      _joinIterator(join),
      _chunkLevelJoin(chunkLevelJoin)
    {
        alignIterators();
    }


    DelegateChunkIterator* JoinEmptyableArray::createChunkIterator(DelegateChunk const* chunk, int iterationMode) const
    {
        JoinEmptyableArrayIterator const& arrayIterator = (JoinEmptyableArrayIterator const&)chunk->getArrayIterator();
        AttributeDesc const& attr = chunk->getAttributeDesc();
        iterationMode &= ~ChunkIterator::INTENDED_TILE_MODE;

        if (!arrayIterator._chunkLevelJoin)
        {
            return new DelegateChunkIterator(chunk, iterationMode);
        }
        else if (attr.isEmptyIndicator())
        {
            return (DelegateChunkIterator*)new JoinBitmapChunkIterator(arrayIterator, chunk, iterationMode);
        }
        else
        {
            return (DelegateChunkIterator*)new JoinChunkIterator(arrayIterator, chunk, iterationMode);
        }
    }

    DelegateArrayIterator* JoinEmptyableArray::createArrayIterator(AttributeID attrID) const
    {
        boost::shared_ptr<ConstArrayIterator> inputIterator;
        boost::shared_ptr<ConstArrayIterator> joinIterator;
        bool chunkLevelJoin = true;
        AttributeID inputAttrID = attrID;

        /*
         * There are two 'levels' of join.
         * First we must ensure that each chunk in LEFT has a matching chunk in RIGHT and vice-versa;
         * otherwise we must exclude non-matching chunk from output. We ALWAYS perform these array-level
         * joins of chunks - regardless of whether the two arrays are emptyable or not.
         *
         * Once you have two matching chunks, you need to make sure that each value in LEFT has a matching
         * value in RIGHT. This is what we call "chunkLevelJoin". At this point, there are cases such as
         * reading an attribute from LEFT and RIGHT is not EMPTYABLE, where we do NOT need to perform a
         * chunk-level join. We can return the entire chunk from LEFT directly.
         */

        if (leftEmptyTagPosition >= 0)
        {   // left array is emptyable
            if ((int)inputAttrID >= leftEmptyTagPosition)
            {
                inputAttrID += 1;
            }
            if (rightEmptyTagPosition >= 0)
            {    // right array is also emptyable: ignore left empty-tag attribute
                if (inputAttrID >= nLeftAttributes)
                {
                    inputIterator = right->getConstIterator(inputAttrID - nLeftAttributes);
                    joinIterator = left->getConstIterator(leftEmptyTagPosition);
                }
                else
                {
                    inputIterator = left->getConstIterator(inputAttrID);
                    joinIterator = right->getConstIterator(rightEmptyTagPosition);
                }
            }
            else
            {   // emptyable array only from left side
                if (attrID == AttributeID(emptyTagPosition))
                {
                    inputIterator = left->getConstIterator(leftEmptyTagPosition);
                    joinIterator = right->getConstIterator(0);
                    chunkLevelJoin = false;
                }
                else if (inputAttrID >= nLeftAttributes)
                {
                    inputIterator = right->getConstIterator(inputAttrID - nLeftAttributes);
                    joinIterator = left->getConstIterator(leftEmptyTagPosition);
                }
                else
                {
                    inputIterator = left->getConstIterator(inputAttrID);
                    joinIterator = right->getConstIterator(0);
                    chunkLevelJoin = false;
                }
            }
        }
        else
        {   // only right array is emptyable
            assert(rightEmptyTagPosition >= 0);
            if (inputAttrID >= nLeftAttributes)
            {
                inputIterator = right->getConstIterator(inputAttrID - nLeftAttributes);
                joinIterator = left->getConstIterator(0);
                chunkLevelJoin = false;
            }
            else
            {
                inputIterator = left->getConstIterator(inputAttrID);
                joinIterator = right->getConstIterator(rightEmptyTagPosition);
            }
        }
        return new JoinEmptyableArrayIterator(*this, attrID, inputIterator, joinIterator, chunkLevelJoin);
    }

    JoinEmptyableArray::JoinEmptyableArray(ArrayDesc const& desc, boost::shared_ptr<Array> leftArr, boost::shared_ptr<Array> rightArr)
    : DelegateArray(desc, leftArr), left(leftArr), right(rightArr)
    {
        ArrayDesc const& leftDesc = left->getArrayDesc();
        ArrayDesc const& rightDesc = right->getArrayDesc();
        nLeftAttributes = leftDesc.getAttributes().size();
        emptyTagPosition = desc.getEmptyBitmapAttribute()->getId();
        leftEmptyTagPosition = leftDesc.getEmptyBitmapAttribute() != NULL ? leftDesc.getEmptyBitmapAttribute()->getId() : -1;
        rightEmptyTagPosition = rightDesc.getEmptyBitmapAttribute() != NULL ? rightDesc.getEmptyBitmapAttribute()->getId() : -1;
    }
}
