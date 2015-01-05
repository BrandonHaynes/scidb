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

#ifndef RESHAPE_ARRAY_H
#define RESHAPE_ARRAY_H

#include "array/DelegateArray.h"
#include "array/MemArray.h"

namespace scidb {

using namespace boost;
using namespace std;

class UnpackUnalignedArray;
class UnpackUnalignedArrayIterator;
class UnpackUnalignedChunk;
class UnpackUnalignedChunkIterator;

class UnpackUnalignedChunkIterator : public ConstChunkIterator
{
 private:
    UnpackUnalignedArray const& array;
    UnpackUnalignedChunk& chunk;
    Coordinates outPos;
    Coordinates inPos;
    Coordinates first;
    Coordinates last;
    boost::shared_ptr<ConstChunkIterator> inputIterator;
    boost::shared_ptr<ConstArrayIterator> arrayIterator;
    int mode;
    bool hasCurrent;
    AttributeID attrID;
    Value _value;
    boost::shared_ptr<Query> _query;

  public:
    virtual int    getMode();
    virtual bool   setPosition(Coordinates const& pos);
    virtual Coordinates const& getPosition();
    virtual void   operator++();
    virtual void   reset();
    virtual  Value& getItem();
    virtual bool   isEmpty();
    virtual bool   end();
    virtual ConstChunk const& getChunk();
    virtual boost::shared_ptr<Query> getQuery() { return _query; }

    UnpackUnalignedChunkIterator(UnpackUnalignedArray const& array, UnpackUnalignedChunk& chunk, int iterationMode);
};

class UnpackUnalignedChunk : public DelegateChunk
{
    friend class UnpackUnalignedChunkIterator;

    UnpackUnalignedArray const& array;
    MemChunk chunk;

  public:
    virtual bool isSparse() const;
    virtual boost::shared_ptr<ConstChunkIterator> getConstIterator(int iterationMode) const;
    void initialize(Coordinates const& pos);

    UnpackUnalignedChunk(UnpackUnalignedArray const& array, DelegateArrayIterator const& iterator, AttributeID attrID);
};

class UnpackUnalignedArrayIterator : public DelegateArrayIterator
{
    UnpackUnalignedArray const& array;
    Coordinates inPos;
    Coordinates outPos;
    bool hasCurrent;

  public:
    virtual ConstChunk const& getChunk();
    virtual Coordinates const& getPosition();
    virtual bool setPosition(Coordinates const& pos);
    virtual bool end();
    virtual void operator ++();
    virtual void reset();

	UnpackUnalignedArrayIterator(UnpackUnalignedArray const& array, AttributeID attrID, boost::shared_ptr<ConstArrayIterator> inputIterator);
};

class UnpackUnalignedArray : public DelegateArray
{
    friend class UnpackUnalignedChunk;
    friend class UnpackUnalignedChunkIterator;
    friend class UnpackUnalignedArrayIterator;

    Dimensions dims;

    Coordinate in2out(Coordinates const& inPos) const;
    void out2in(Coordinate outPos, Coordinates& inPos) const;

  public:

    virtual DelegateChunk* createChunk(DelegateArrayIterator const* iterator, AttributeID id) const;
    virtual DelegateArrayIterator* createArrayIterator(AttributeID id) const;

    UnpackUnalignedArray(ArrayDesc const& desc, boost::shared_ptr<Array> const& array, const boost::shared_ptr<Query>& query);
};

}

#endif
