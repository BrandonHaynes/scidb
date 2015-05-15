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
 * @file CrossJoinArray.cpp
 *
 * @brief CrossJoin array implementation
 *
 * @author Konstantin Knizhnik <knizhnik@garret.ru>
 * @author poliocough@gmail.com
 */

#include "CrossJoinArray.h"
#include "array/MemArray.h"
#include "system/Exceptions.h"


namespace scidb
{
    using namespace boost;

    //
    // CrossJoin chunk methods
    //
    const ArrayDesc& CrossJoinChunk::getArrayDesc() const
    {
        return array.desc;
    }

    Array const& CrossJoinChunk::getArray() const
    {
        return array;
    }

    const AttributeDesc& CrossJoinChunk::getAttributeDesc() const
    {
        return array.desc.getAttributes()[attr];
    }

    int CrossJoinChunk::getCompressionMethod() const
    {
        return leftChunk->getCompressionMethod();
    }

    Coordinates const& CrossJoinChunk::getFirstPosition(bool withOverlap) const
    {
        return withOverlap ? firstPosWithOverlap : firstPos;
    }

    Coordinates const& CrossJoinChunk::getLastPosition(bool withOverlap) const
    {
       return withOverlap ? lastPosWithOverlap : lastPos;
     }

    shared_ptr<ConstChunkIterator> CrossJoinChunk::getConstIterator(int iterationMode) const
    {
        if ((iterationMode & ChunkIterator::IGNORE_EMPTY_CELLS) == false)
        {
            //the client will ALWAYS use IGNORE_EMPTY_CELLS, right? Let's make sure they do.
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_CHUNK_WRONG_ITERATION_MODE);
        }

        return shared_ptr<ConstChunkIterator>(new CrossJoinChunkIterator(*this, iterationMode));
    }

    CrossJoinChunk::CrossJoinChunk(CrossJoinArray const& cross, AttributeID attrID, bool isLeftAttr)
    : array(cross), attr(attrID), isLeftAttribute(isLeftAttr)
    {
        isEmptyIndicatorAttribute = getAttributeDesc().isEmptyIndicator();
    }

    void CrossJoinChunk::setInputChunk(ConstChunk const* left, ConstChunk const* right)
    {
        leftChunk  = left;
        rightChunk = right;

        firstPos = array.getPosition(left->getFirstPosition(false), right->getFirstPosition(false));
        firstPosWithOverlap = array.getPosition(left->getFirstPosition(true), right->getFirstPosition(true));
        lastPos  = array.getPosition(left->getLastPosition(false), right->getLastPosition(false));
        lastPosWithOverlap  = array.getPosition(left->getLastPosition(true), right->getLastPosition(true));
    }

    bool CrossJoinChunk::isMaterialized() const
    {
        return false;
    }

    //
    // CrossJoin chunk iterator methods
    //
    int CrossJoinChunkIterator::getMode()
    {
        return leftIterator->getMode();
    }

    Value& CrossJoinChunkIterator::getItem()
    {
         if (!hasCurrent)
             throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_NO_CURRENT_ELEMENT);

         if (chunk.isEmptyIndicatorAttribute)
         {
            boolValue.setBool(!isEmpty());
            return boolValue;
        }

        if(chunk.isLeftAttribute)
        {
            return leftIterator->getItem();
        }
        else
        {
            boolValue = (*currentBucket)[currentIndex].second;
            return boolValue;
        }
    }

    bool CrossJoinChunkIterator::isEmpty()
    {
        return false;
    }

    bool CrossJoinChunkIterator::end()
    {
        return !hasCurrent;
    }

    void CrossJoinChunkIterator::operator ++()
    {
        if (!hasCurrent)
            throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_NO_CURRENT_ELEMENT);

        if( ++currentIndex >= (ssize_t) currentBucket->size())
        {
            ++(*leftIterator);
            while (!leftIterator->end())
            {
                Coordinates joinKey(array.nJoinDims);
                array.decomposeLeftCoordinates(leftIterator->getPosition(), joinKey);

                ChunkHash::const_iterator it = rightHash.find(joinKey);
                if (it!=rightHash.end())
                {
                    currentBucket = &(it->second);
                    currentIndex = 0;
                    return;
                }
                ++(*leftIterator);
            }
            hasCurrent = false;
        }
    }

    Coordinates const& CrossJoinChunkIterator::getPosition()
    {
        if (!hasCurrent)
            throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_NO_CURRENT_ELEMENT);

        array.composeOutCoordinates(leftIterator->getPosition(), (*currentBucket)[currentIndex].first, currentPos);
        return currentPos;
    }

    bool CrossJoinChunkIterator::setPosition(Coordinates const& pos)
    {
        Coordinates left(array.nLeftDims);
        Coordinates joinKey(array.nJoinDims);
        Coordinates rightLeftover(array.nRightDims - array.nJoinDims);
        array.decomposeOutCoordinates(pos, left, joinKey, rightLeftover);

        if(!leftIterator->setPosition(left))
        {
            return hasCurrent = false;
        }

        ChunkHash::const_iterator it = rightHash.find(joinKey);
        if (it==rightHash.end())
        {
            return hasCurrent = false;
        }

        currentBucket = &(it->second);
        currentIndex = findValueInBucket(currentBucket, rightLeftover);

        return hasCurrent = (currentIndex!=-1);
    }

    void CrossJoinChunkIterator::reset()
    {
        hasCurrent = false;
        leftIterator->reset();
        Coordinates joinKey(array.nJoinDims);
        while (!leftIterator->end())
        {
            array.decomposeLeftCoordinates(leftIterator->getPosition(), joinKey);
            ChunkHash::const_iterator it = rightHash.find(joinKey);
            if (it!=rightHash.end())
            {
                currentBucket = &(it->second);
                currentIndex = 0;
                hasCurrent = true;
                return;
            }

            ++(*leftIterator);
        }
    }

    ConstChunk const& CrossJoinChunkIterator::getChunk()
    {
        return chunk;
    }

    CrossJoinChunkIterator::CrossJoinChunkIterator(CrossJoinChunk const& aChunk, int iterationMode)
    : array(aChunk.array),
      chunk(aChunk),
      leftIterator(aChunk.leftChunk->getConstIterator(iterationMode & ~INTENDED_TILE_MODE)),
      currentPos(aChunk.array.desc.getDimensions().size()),
      currentBucket(0),
      currentIndex(-1)
    {
        rightHash.clear();
        shared_ptr<ConstChunkIterator> iter = aChunk.rightChunk->getConstIterator(iterationMode & ~INTENDED_TILE_MODE);
        Coordinates joinKey(array.nJoinDims);
        Coordinates rightLeftover(array.nRightDims - array.nJoinDims);
        while(!iter->end())
        {
            array.decomposeRightCoordinates(iter->getPosition(), joinKey, rightLeftover);
            rightHash[joinKey].push_back( make_pair<Coordinates,Value> (rightLeftover, iter->getItem()));
            ++(*iter);
        }

        reset();
    }

    ssize_t CrossJoinChunkIterator::findValueInBucket(HashBucket const* bucket, Coordinates const& coords) const
    {
        CoordinatesLess cless;
        size_t l = 0, r = bucket->size();
        while (l < r) {
            size_t m = (l + r) >> 1;
            if ( cless((*bucket)[m].first, coords))
            {
                l = m + 1;
            }
            else
            {
                r = m;
            }
        }

        if (r < bucket->size() && (*bucket)[r].first == coords)
        {
            return r;
        }

        return -1;
    }

    //
    // CrossJoin array iterator methods
    //
    CrossJoinArrayIterator::CrossJoinArrayIterator(CrossJoinArray const& cross, AttributeID attrID,
                                                   shared_ptr<ConstArrayIterator> left,
                                                   shared_ptr<ConstArrayIterator> right,
                                                   shared_ptr<ConstArrayIterator> input)
    : array(cross),
      attr(attrID),
      leftIterator(left),
      rightIterator(right),
      inputIterator(input),
      chunk(cross, attrID, input == left)
    {
        reset();
	}

    ConstChunk const& CrossJoinArrayIterator::getChunk()
    {
        if (!hasCurrent)
            throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_NO_CURRENT_ELEMENT);
        if (!chunkInitialized) {
            chunk.setInputChunk(&leftIterator->getChunk(), &rightIterator->getChunk());
            chunkInitialized = true;
        }
        return chunk;
    }

    bool CrossJoinArrayIterator::end()
    {
        return !hasCurrent;
    }

    void CrossJoinArrayIterator::operator ++()
    {
        if (hasCurrent) {
            chunkInitialized = false;
            ++(*rightIterator);
            do {
                while (!rightIterator->end()) {
                    if (array.matchPosition(leftIterator->getPosition(), rightIterator->getPosition())) {
                        return;
                    }
                    ++(*rightIterator);
                }
                rightIterator->reset();
                ++(*leftIterator);
            } while (!leftIterator->end());

            hasCurrent = false;
        }
    }

    Coordinates const& CrossJoinArrayIterator::getPosition()
    {
        currentPos = array.getPosition(leftIterator->getPosition(), rightIterator->getPosition());
        return currentPos;
    }

    bool CrossJoinArrayIterator::setPosition(Coordinates const& pos)
    {
        chunkInitialized = false;
        return hasCurrent = leftIterator->setPosition(array.getLeftPosition(pos))
            && rightIterator->setPosition(array.getRightPosition(pos));
    }

    void CrossJoinArrayIterator::reset()
    {
        chunkInitialized = false;
        hasCurrent = false;
        leftIterator->reset();
        while (!leftIterator->end())  {
            rightIterator->reset();
            while (!rightIterator->end()) {
                if (array.matchPosition(leftIterator->getPosition(), rightIterator->getPosition())) {
                    hasCurrent = true;
                    return;
                }
                ++(*rightIterator);
            }
            ++(*leftIterator);
        }
    }

    //
    // CrossJoin array methods
    //
    CrossJoinArray::CrossJoinArray(const ArrayDesc& d,
                                   const shared_ptr<Array>& leftArray,
                                   const shared_ptr<Array>& rightArray,
                                   vector<int> const& ljd,
                                   vector<int> const& rjd)
    : desc(d),
      leftDesc(leftArray->getArrayDesc()),
      rightDesc(rightArray->getArrayDesc()),
      left(leftArray),
      right(rightArray),
      nLeftDims(leftDesc.getDimensions().size()),
      nRightDims(rightDesc.getDimensions().size()),
      nLeftAttrs(leftDesc.getAttributes().size()),
      nRightAttrs(rightDesc.getAttributes().size()),
      leftJoinDims(ljd),
      rightJoinDims(rjd),
      nJoinDims(0)
    {
        for(size_t i=0; i<leftJoinDims.size(); i++)
        {
            if(leftJoinDims[i]!=-1)
            {
                nJoinDims++;
            }
        }

        leftEmptyTagPosition = leftDesc.getEmptyBitmapAttribute() != NULL ? leftDesc.getEmptyBitmapAttribute()->getId() : -1;
        rightEmptyTagPosition = rightDesc.getEmptyBitmapAttribute() != NULL ? rightDesc.getEmptyBitmapAttribute()->getId() : -1;
    }

    bool CrossJoinArray::matchPosition(Coordinates const& left, Coordinates const& right) const
    {
        for (size_t r = 0; r < nRightDims; r++) {
            int l = rightJoinDims[r];
            if (l >= 0) {
                if (left[l] != right[r]) {
                    return false;
                }
            }
        }
        return true;
    }

    void CrossJoinArray::decomposeRightCoordinates(Coordinates const& right, Coordinates& hashKey, Coordinates &rightLeftover) const
    {
        assert(hashKey.size() == nJoinDims);
        assert(rightLeftover.size() == nRightDims - nJoinDims);
        assert(right.size() == nRightDims);

        size_t k=0;
        size_t j=0;
        for(size_t i=0; i<nRightDims; i++)
        {
            if(rightJoinDims[i]!=-1)
            {
                hashKey[k++]=right[i];
            }
            else
            {
                rightLeftover[j++]=right[i];
            }
        }
    }

    void CrossJoinArray::decomposeOutCoordinates(Coordinates const& out, Coordinates& left, Coordinates& hashKey, Coordinates& rightLeftover) const
    {
        assert(out.size() == desc.getDimensions().size());
        assert(left.size() == nLeftDims);
        assert(rightLeftover.size() == nRightDims-nJoinDims);
        assert(hashKey.size() == nJoinDims);

        left.assign(out.begin(), out.begin()+nLeftDims);
        rightLeftover.assign(out.begin()+nLeftDims, out.end());

        for (size_t i =0; i<nLeftDims; i++)
        {
            if(leftJoinDims[i]!=-1)
            {
                hashKey[leftJoinDims[i]] = out[i];
            }
        }
    }

    void CrossJoinArray::decomposeLeftCoordinates(Coordinates const& left, Coordinates& hashKey) const
    {
        assert(left.size() == nLeftDims);
        assert(hashKey.size() == nJoinDims);

        for (size_t i =0; i<nLeftDims; i++)
        {
            if(leftJoinDims[i]!=-1)
            {
                hashKey[leftJoinDims[i]] = left[i];
            }
        }
    }

    void CrossJoinArray::composeOutCoordinates(Coordinates const &left, Coordinates const& rightLeftover, Coordinates& out) const
    {
        assert(left.size() == nLeftDims);
        assert(rightLeftover.size() == nRightDims - nJoinDims);
        assert(out.size() == desc.getDimensions().size());

        memcpy(&out[0], &left[0], nLeftDims*sizeof(Coordinate));
        memcpy(&out[left.size()], &rightLeftover[0], (nRightDims-nJoinDims)*sizeof(Coordinate));
    }

    Coordinates CrossJoinArray::getLeftPosition(Coordinates const& pos) const
    {
        return Coordinates(pos.begin(), pos.begin() + nLeftDims);
    }

    Coordinates CrossJoinArray::getRightPosition(Coordinates const& pos) const
    {
        Coordinates rightPos(nRightDims);
        for (size_t r = 0, i = nLeftDims; r < nRightDims; r++) {
            int l = rightJoinDims[r];
            rightPos[r] = (l >= 0) ? pos[l] : pos[i++];
        }
        return rightPos;
    }


    Coordinates CrossJoinArray::getPosition(Coordinates const& left, Coordinates const& right) const
    {
        Coordinates pos(desc.getDimensions().size());
        for (size_t l = 0; l < nLeftDims; l++) {
            pos[l] = left[l];
        }
         for (size_t r = 0, i = nLeftDims; r < nRightDims; r++) {
            if (rightJoinDims[r] < 0) {
                pos[i++] = right[r];
            }
        }
        return pos;
    }

    const ArrayDesc& CrossJoinArray::getArrayDesc() const
    {
        return desc;
    }

    shared_ptr<ConstArrayIterator> CrossJoinArray::getConstIterator(AttributeID attrID) const
	{
        shared_ptr<ConstArrayIterator> leftIterator;
        shared_ptr<ConstArrayIterator> rightIterator;
        shared_ptr<ConstArrayIterator> inputIterator;
        AttributeID inputAttrID = attrID;

        if (leftEmptyTagPosition >= 0) { // left array is emptyable
            if ((int)inputAttrID >= leftEmptyTagPosition) {
                inputAttrID += 1;
            }
            if (rightEmptyTagPosition >= 0) { // right array is also emptyable: ignore left empty-tag attribute
                if (inputAttrID >= nLeftAttrs) {
                    leftIterator = left->getConstIterator(leftEmptyTagPosition);
                    inputIterator = rightIterator = right->getConstIterator(inputAttrID - nLeftAttrs);
                } else {
                    inputIterator = leftIterator = left->getConstIterator(inputAttrID);
                    rightIterator = right->getConstIterator(rightEmptyTagPosition);
                }
            } else { // emptyable array only from left side
                if (inputAttrID >= nLeftAttrs) {
                    leftIterator = left->getConstIterator(leftEmptyTagPosition);
                    inputIterator = rightIterator = right->getConstIterator(inputAttrID == nLeftAttrs + nRightAttrs ? 0 : inputAttrID - nLeftAttrs);
                } else {
                    inputIterator = leftIterator = left->getConstIterator(inputAttrID);
                    rightIterator = right->getConstIterator(0);
                }
            }
        } else if (rightEmptyTagPosition >= 0) { // only right array is emptyable
            if (inputAttrID >= nLeftAttrs) {
                leftIterator = left->getConstIterator(0);
                inputIterator = rightIterator = right->getConstIterator(inputAttrID - nLeftAttrs);
            } else {
                inputIterator = leftIterator = left->getConstIterator(inputAttrID);
                rightIterator = right->getConstIterator(rightEmptyTagPosition);
            }
        } else { // both input arrays are non-emptyable
            if (inputAttrID >= nLeftAttrs) {
                leftIterator = left->getConstIterator(0);
                if (inputAttrID == nLeftAttrs + nRightAttrs) {
                    rightIterator = right->getConstIterator(0);
                } else {
                    inputIterator = rightIterator = right->getConstIterator(inputAttrID - nLeftAttrs);
                }
            } else {
                inputIterator = leftIterator = left->getConstIterator(inputAttrID);
                rightIterator = right->getConstIterator(0);
            }
        }
        return shared_ptr<CrossJoinArrayIterator>(new CrossJoinArrayIterator(*this, attrID, leftIterator, rightIterator, inputIterator));
    }
}
