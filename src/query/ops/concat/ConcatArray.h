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
 * @file ConcatArray.h
 *
 * @brief The implementation of the array iterator for the concat operator
 *
 */

#ifndef CONCAT_ARRAY_H_
#define CONCAT_ARRAY_H_

#include <string>
#include <vector>
#include "array/MemArray.h"
#include "array/DelegateArray.h"
#include "array/Metadata.h"
#include "query/LogicalExpression.h"

namespace scidb
{

using namespace std;
using namespace boost;

class ConcatArray;
class ConcatArrayIterator;

class ConcatChunk : public DelegateChunk
{
    friend class ConcatArrayIterator;
    friend class ConcatArray;
  public:
	virtual Coordinates const& getFirstPosition(bool withOverlap) const;
	virtual Coordinates const& getLastPosition(bool withOverlap) const;
    virtual void setInputChunk(ConstChunk const& inputChunk);
    virtual void setProxy();
    ConcatChunk(ConcatArray const& array, ConcatArrayIterator const& arrayIterator, AttributeID attrID);

  private:
    MemChunk    shapeChunk;
    Coordinates firstPos; 
    Coordinates firstPosWithOverlap; 
    Coordinates lastPos; 
    Coordinates lastPosWithOverlap; 
    bool        direct;    
};

class ConcatDirectChunkIterator : public DelegateChunkIterator
{    
  public:
	virtual Coordinates const& getPosition();
	virtual bool setPosition(Coordinates const& pos);
    ConcatDirectChunkIterator(DelegateChunk const* chunk, int iterationMode);
    
  private:
    Coordinates currPos;
};

class ConcatChunkIterator : public DelegateChunkIterator
{  
    boost::shared_ptr<ConstChunkIterator> chunkIterator;
    Coordinates inPos;
    Coordinates outPos;
    Coordinates first;
    Coordinates last;
    int mode;
    bool hasCurrent;


  public:
    int    getMode();
    bool   end();
    bool   setPosition(Coordinates const& pos);
    Coordinates const& getPosition();
    void   operator++();
    void   reset();
     Value& getItem();
    bool   isEmpty();

    ConcatChunkIterator(DelegateChunk const* chunk, int iterationMode);
};

class ConcatArrayIterator : public DelegateArrayIterator
{
    friend class ConcatDirectChunkIterator;
    friend class ConcatChunkIterator;
    friend class ConcatChunk;
  public:
    virtual void operator ++();
    virtual void reset();
    virtual bool end();
	virtual bool setPosition(Coordinates const& pos);
	virtual Coordinates const& getPosition();
	ConstChunk const& getChunk();
    ConcatArrayIterator(ConcatArray const& array, AttributeID attrID);
                                                
  private:
    bool nextVisible();
    bool setInputPosition(size_t i);

    boost::shared_ptr< ConstArrayIterator > leftIterator;
    boost::shared_ptr< ConstArrayIterator > rightIterator;
    Coordinates outPos;
    Coordinates inPos;
    Coordinate  lastLeft;
    Coordinate  firstRight;
    size_t      concatChunkInterval;
    Coordinate  shift;
    bool        hasCurrent;
};

class ConcatArray : public DelegateArray
{
    friend class ConcatArrayIterator;
  public:
    virtual DelegateArrayIterator* createArrayIterator(AttributeID id) const;
    virtual DelegateChunk* createChunk(DelegateArrayIterator const* iterator, AttributeID id) const;
    virtual DelegateChunkIterator* createChunkIterator(DelegateChunk const* chunk, int iterationMode) const;

    /**
     * Get the least restrictive access mode that the array supports.
     * @return RANDOM in case of simpleAppend, NO_SET_POSITION otherwise
     */
    virtual Access getSupportedAccess() const
    {
        if(simpleAppend)
        {
            return RANDOM;
        }
        else
        {
            return MULTI_PASS;
        }
    }

    ConcatArray(ArrayDesc const& desc, boost::shared_ptr<Array> const& left, boost::shared_ptr<Array> const& right);

  private:
    Coordinate lastLeft;
    Coordinate firstRight;
    size_t     concatChunkInterval;
    boost::shared_ptr<Array> leftArray;
    boost::shared_ptr<Array> rightArray;
    bool simpleAppend;
    Dimensions const& dims;
};

}

#endif
