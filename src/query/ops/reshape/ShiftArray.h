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

#ifndef SHIFT_ARRAY_H
#define SHIFT_ARRAY_H

#include "array/DelegateArray.h"
#include "array/MemArray.h"

namespace scidb {

using namespace boost;
using namespace std;

class ShiftArray;
class ShiftArrayIterator;
class ShiftChunk;
class ShiftChunkIterator;

class ShiftChunkIterator : public DelegateChunkIterator
{  
    ShiftArray const& array;
    Coordinates outPos;
    Coordinates inPos;
  public:
    virtual bool   setPosition(Coordinates const& pos);
    virtual Coordinates const& getPosition();

    ShiftChunkIterator(ShiftArray const& array, DelegateChunk const* chunk, int iterationMode);
};

class ShiftChunk : public DelegateChunk
{
    ShiftArray const& array;
    Coordinates firstPos;
    Coordinates lastPos;
  public:
    void setInputChunk(ConstChunk const& inputChunk);
    Coordinates const& getFirstPosition(bool withOverlap) const;
    Coordinates const& getLastPosition(bool withOverlap) const;

    ShiftChunk(ShiftArray const& array, DelegateArrayIterator const& iterator, AttributeID attrID);
};

class ShiftArrayIterator : public DelegateArrayIterator
{
    ShiftArray const& array;
    Coordinates inPos;
    Coordinates outPos;

  public:
    virtual Coordinates const& getPosition();
    virtual bool setPosition(Coordinates const& pos);

	ShiftArrayIterator(ShiftArray const& array, AttributeID attrID, boost::shared_ptr<ConstArrayIterator> inputIterator);
};

class ShiftArray : public DelegateArray
{
    friend class ShiftChunk;
    friend class ShiftChunkIterator;
    friend class ShiftArrayIterator;

    Dimensions inDims;
    Dimensions outDims;

    void in2out(Coordinates const& inPos, Coordinates& outPos) const;
    void out2in(Coordinates const& outPos, Coordinates& inPos) const; 

  public:

    virtual DelegateChunk* createChunk(DelegateArrayIterator const* iterator, AttributeID id) const;
    virtual DelegateChunkIterator* createChunkIterator(DelegateChunk const* chunk, int iterationMode) const;
    virtual DelegateArrayIterator* createArrayIterator(AttributeID id) const;

    ShiftArray(ArrayDesc const& desc, boost::shared_ptr<Array> const& array);
};

}

#endif
