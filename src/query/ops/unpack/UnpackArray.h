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
 * @file UnpackArray.cpp
 *
 * @brief Unpack array implementation
 *
 * @author Konstantin Knizhnik <knizhnik@garret.ru>
 */

#ifndef UNPACK_ARRAY_H
#define UNPACK_ARRAY_H

#include "array/DelegateArray.h"
#include "array/MemArray.h"

namespace scidb {

using namespace boost;
using namespace std;

class UnpackArray;
class UnpackArrayIterator;
class UnpackChunk;
class UnpackChunkIterator;

class UnpackChunkIterator : public ConstChunkIterator
{
private:
    UnpackArray const& array;
    UnpackChunk const& chunk;
    Coordinates inPos;
    Coordinates outPos;
    AttributeID attrID;
    boost::shared_ptr<ConstChunkIterator> inputIterator;
    int mode;
    Value _value;
    bool hasCurrent;
    Coordinate  first;
    Coordinate  last;
    Coordinate  base;
    boost::shared_ptr<Query> _query;

public:
    virtual bool   setPosition(Coordinates const& pos);
    virtual Coordinates const& getPosition();
    virtual void   operator++();
    virtual void   reset();
    virtual bool   isEmpty();
    virtual bool   end();
    virtual  Value& getItem();
    virtual int    getMode();
    virtual ConstChunk const& getChunk();
    virtual boost::shared_ptr<Query> getQuery() { return _query; }

    UnpackChunkIterator(UnpackArray const& array, UnpackChunk const& chunk, int iterationMode);
};

class UnpackChunk : public DelegateChunk
{
    friend class UnpackChunkIterator;
    friend class UnpackArrayIterator;
 private:
    UnpackArray const& array;
    ConstChunk const* inputChunk;
    MemChunk chunk;


  public:
    virtual boost::shared_ptr<ConstChunkIterator> getConstIterator(int iterationMode) const;
    virtual bool isSparse() const;
    void initialize(Coordinates const& pos);

    UnpackChunk(UnpackArray const& array, DelegateArrayIterator const& iterator, AttributeID attrID);
};

class UnpackArrayIterator : public DelegateArrayIterator
{
 private:
    UnpackArray const& array;
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

    UnpackArrayIterator(UnpackArray const& array, AttributeID attrID, boost::shared_ptr<ConstArrayIterator> inputIterator);
};

class UnpackArray : public DelegateArray
{
    friend class UnpackChunk;
    friend class UnpackChunkIterator;
    friend class UnpackArrayIterator;

    Dimensions dims;

    Coordinate in2out(Coordinates const& inPos) const;
    void out2in(Coordinate outPos, Coordinates& inPos) const;

  public:

    virtual DelegateChunk* createChunk(DelegateArrayIterator const* iterator, AttributeID id) const;
    virtual DelegateArrayIterator* createArrayIterator(AttributeID id) const;

    UnpackArray(ArrayDesc const& desc, boost::shared_ptr<Array> const& array, const boost::shared_ptr<Query>& query);
};

}

#endif
