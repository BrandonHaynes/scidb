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
 * @file ReverseArray.h
 *
 * @brief The implementation of the array iterator for the reverse operator
 *
 * The array iterator for the reverse maps incoming getChunks calls into the
 * appropriate getChunks calls for its input array. Then, if the requested chunk
 * fits in the reverse range, the entire chunk is returned as-is. Otherwise,
 * the appropriate piece of the chunk is carved out.
 *
 * NOTE: In the current implementation if the reverse window stretches beyond the
 * limits of the input array, the behavior of the operator is undefined.
 *
 * The top-level array object simply serves as a factory for the iterators.
 */

#ifndef REVERSE_ARRAY_H_
#define REVERSE_ARRAY_H_

#include <string>
#include "array/DelegateArray.h"
#include "array/Metadata.h"

namespace scidb
{

using namespace std;
using namespace boost;

class ReverseArray;
class ReverseArrayIterator;
class ReverseChunkIterator;

class ReverseChunk : public DelegateChunk
{
    friend class ReverseChunkIterator;
    friend class ReverseDirectChunkIterator;
  public:
    Coordinates const& getFirstPosition(bool withOverlap) const;
    Coordinates const& getLastPosition(bool withOverlap) const;
    boost::shared_ptr<ConstChunkIterator> getConstIterator(int iterationMode) const;

    void setPosition(Coordinates const& pos);

    ReverseChunk(ReverseArray const& array, DelegateArrayIterator const& iterator, AttributeID attrID);

  private:
    ReverseArray const& array;
    Coordinates firstPos;
    Coordinates firstPosWithOverlap;
    Coordinates lastPos;
    Coordinates lastPosWithOverlap;
};
    
class ReverseChunkIterator : public ConstChunkIterator
{
  public:
    int getMode();
     Value& getItem();
    bool isEmpty();
    bool end();
    void operator ++();
    Coordinates const& getPosition();
    bool setPosition(Coordinates const& pos);
    void reset();
    ConstChunk const& getChunk();

    ReverseChunkIterator(ReverseChunk const& chunk, int iterationMode);

  private:
    ReverseArray const& array;
    ReverseChunk const& chunk;
    ConstChunk const* inputChunk;
    boost::shared_ptr<ConstChunkIterator> inputIterator;
    Coordinates outPos; 
    Coordinates inPos; 
    bool hasCurrent;
    int mode;
};

class ReverseArrayIterator : public DelegateArrayIterator
{
    friend class ReverseChunkIterator;
  public:
	/***
	 * Constructor for the reverse iterator
	 * Here we initialize the current position vector to all zeros, and obtain an iterator for the appropriate
	 * attribute in the input array.
	 */
	ReverseArrayIterator(ReverseArray const& reverse, AttributeID attrID);

	/***
	 * The end call checks whether we're operating with the last chunk of the reverse
	 * window.
	 */
	virtual bool end();

	/***
	 * The ++ operator advances the current position to the next chunk of the reverse
	 * window.
	 */
	virtual void operator ++();

	/***
	 * Simply returns the current position
	 * Initial position is a vector of zeros of appropriate dimensionality
	 */
	virtual Coordinates const& getPosition();

	/***
	 * Here we only need to check that we're not moving beyond the bounds of the reverse window
	 */
	virtual bool setPosition(Coordinates const& pos);

	/***
	 * Reset simply changes the current position to all zeros
	 */
	virtual void reset();

    virtual ConstChunk const& getChunk();

  private:
    bool nextAvailable();
    bool setInputPosition(size_t i);

    ReverseArray const& array;	
	Coordinates outPos; 
	Coordinates inPos; 
    bool hasCurrent;
};

class ReverseArray : public DelegateArray
{
    friend class ReverseChunk;
    friend class ReverseChunkIterator;
    friend class ReverseArrayIterator;

  public:
	ReverseArray(ArrayDesc const& desc, boost::shared_ptr<Array>const& input);

    DelegateArrayIterator* createArrayIterator(AttributeID attrID) const;
    DelegateChunk* createChunk(DelegateArrayIterator const* iterator, AttributeID attrID) const;
 
    void revert(Coordinates const& src, Coordinates& dst) const;

  private:
	Dimensions const& dims;
};


} //namespace

#endif /* REVERSE_ARRAY_H_ */
