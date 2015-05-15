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
#ifndef _JOIN_ARRAY_H_
#define _JOIN_ARRAY_H_

#include "query/Operator.h"
#include "array/Metadata.h"
#include "array/DelegateArray.h"

namespace scidb {

class JoinEmptyableArray;
class JoinEmptyableArrayIterator;

class JoinChunkIterator : public DelegateChunkIterator
{    
  public:
    virtual void operator ++();
    virtual bool isEmpty();
    virtual bool end();
    virtual void reset();
	virtual bool setPosition(Coordinates const& pos);
    JoinChunkIterator(JoinEmptyableArrayIterator const& arrayIterator, DelegateChunk const* chunk, int iterationMode);
    
  protected:
    bool join();
    void alignIterators();

    boost::shared_ptr<ConstChunkIterator> joinIterator;
    int mode;
    bool hasCurrent;
};
   

class JoinBitmapChunkIterator : public JoinChunkIterator
{
  public:
    virtual  Value& getItem();

    JoinBitmapChunkIterator(JoinEmptyableArrayIterator const& arrayIterator, DelegateChunk const* chunk, int iterationMode);

  private:
     Value value;
};
     

class JoinEmptyableArrayIterator : public DelegateArrayIterator
{
    friend class JoinChunkIterator;
    friend class JoinEmptyableArray;
  public: 
    bool setPosition(Coordinates const& pos);
    virtual void reset();
    virtual void operator ++();
    virtual bool end();
	virtual ConstChunk const& getChunk();
    JoinEmptyableArrayIterator(JoinEmptyableArray const& array, AttributeID attrID, boost::shared_ptr<ConstArrayIterator> inputIterator, boost::shared_ptr<ConstArrayIterator> joinIterator, bool chunkLevelJoin);

  private:
    void alignIterators();
    
    boost::shared_ptr<ConstArrayIterator> _joinIterator;
    bool _hasCurrent;
    bool _chunkLevelJoin;
};

class JoinEmptyableArray : public DelegateArray
{
    friend class JoinEmptyableArrayIterator;
    friend class JoinChunkIterator;
  public:
    virtual DelegateChunkIterator* createChunkIterator(DelegateChunk const* chunk, int iterationMode) const;
    virtual DelegateArrayIterator* createArrayIterator(AttributeID id) const;

    JoinEmptyableArray(ArrayDesc const& desc, boost::shared_ptr<Array> left, boost::shared_ptr<Array> right);

  private:
    boost::shared_ptr<Array> left;
    boost::shared_ptr<Array> right;
    size_t nLeftAttributes;
    int    leftEmptyTagPosition;
    int    rightEmptyTagPosition;
    int    emptyTagPosition;
};

}

#endif
