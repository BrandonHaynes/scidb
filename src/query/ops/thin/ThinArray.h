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
 * @file ThinArray.cpp
 *
 * @brief Thin array implementation
 *
 * @author Konstantin Knizhnik <knizhnik@garret.ru>
 */

#ifndef THIN_ARRAY_H
#define THIN_ARRAY_H

#include "array/DelegateArray.h"
#include "array/MemArray.h"

namespace scidb {

using namespace boost;
using namespace std;

class ThinArray;
class ThinArrayIterator;
class ThinChunk;
class ThinChunkIterator;

Coordinate computeLastCoordinate(uint64_t dimensionLength,
                                 Coordinate dimensionStart,
                                 Coordinate thinStartingPoint,
                                 Coordinate stepInterval);

class ThinChunkIterator : public ConstChunkIterator
{  
    ThinArray const& array;
    ThinChunk const& chunk;
    Coordinates outPos;
    Coordinates inPos;
    Coordinates first;
    Coordinates last;
    boost::shared_ptr<ConstChunkIterator> inputIterator;
    int mode;
    bool hasCurrent;

  public:
    virtual int    getMode();
    virtual bool   setPosition(Coordinates const& pos);
    virtual Coordinates const& getPosition();
    virtual void   operator++();
    virtual void   reset();
    virtual Value& getItem();
    virtual bool   isEmpty();
    virtual bool   end();
    virtual ConstChunk const& getChunk();


    ThinChunkIterator(ThinArray const& array, ThinChunk const& chunk, int iterationMode);
};

class ThinChunk : public DelegateChunk
{
    friend class ThinChunkIterator;
    friend class ThinArrayIterator;

    ThinArray const& array;
    MemChunk chunk;
    ConstChunk const* srcChunk;

  public:
    virtual boost::shared_ptr<ConstChunkIterator> getConstIterator(int iterationMode) const;
    
    void initialize(Coordinates const& pos);

    ThinChunk(ThinArray const& array, DelegateArrayIterator const& iterator, AttributeID attrID);
};

class ThinArrayIterator : public DelegateArrayIterator
{
    ThinArray const& array;
    Coordinates inPos;
    Coordinates outPos;

  public:
    virtual ConstChunk const& getChunk();
    virtual Coordinates const& getPosition();
    virtual bool setPosition(Coordinates const& pos);
    virtual bool end();

	ThinArrayIterator(ThinArray const& array, AttributeID attrID, boost::shared_ptr<ConstArrayIterator> inputIterator);
};

class ThinArray : public DelegateArray
{
    friend class ThinChunk;
    friend class ThinChunkIterator;
    friend class ThinArrayIterator;

    Coordinates _from;
    Coordinates _step;

    void out2in(Coordinates const& outPos, Coordinates& inPos) const; 
    void in2out(Coordinates const& inPos, Coordinates& outPos) const; 

  public:

    virtual DelegateChunk* createChunk(DelegateArrayIterator const* iterator, AttributeID id) const;
    virtual DelegateArrayIterator* createArrayIterator(AttributeID id) const;

    ThinArray(ArrayDesc const& desc, boost::shared_ptr<Array> const& array, Coordinates const& from, Coordinates const& step);
};

}

#endif
