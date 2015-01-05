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
 * @file MemArray.h
 *
 * @brief In-Memory (temporary) array implementation
 */

#ifndef TUPLE_ARRAY_H_
#define TUPLE_ARRAY_H_

#include <boost/shared_ptr.hpp> 
#include <map>

#include "array/MemArray.h"
#include "query/Expression.h"
#include "query/FunctionDescription.h"

using namespace std;
using namespace boost;

namespace scidb
{

class TupleArray;
class TupleArrayIterator;
class TupleChunk;
class TupleChunkIterator;

class Tuple {
	friend class TupleArray;
  public:
	 Value& operator[](size_t pos) {
		return columns[pos];
	}
	 Value const& operator[](size_t pos) const{
		return columns[pos];
	}

	void resize(size_t size) {
		columns.resize(size);
	}

	// TODO: it would be good to specify column types here for value data allocation
	Tuple(size_t n = 0) : columns(n) {}
	Tuple(Tuple const& other) : columns(other.columns) {}

  private:
	vector<  Value > columns;
};

struct Key
{
	int  columnNo; // zero based
	bool ascent;
};

class TupleComparator
{
  private:
	vector<Key> _keys;
	ArrayDesc _arrayDesc;
	vector<FunctionPointer> _leFunctions;
	vector<FunctionPointer> _eqFunctions;

	// The types of each key are needed in compare() to call isNullOrNan().
	// The types are acquired in the constructor so that they don't need to be calculated again and again in compare().
	vector<DoubleFloatOther> _types;

  public:
	int operator()(boost::shared_ptr<Tuple> const& t1, boost::shared_ptr<Tuple> const& t2) {
		return compare(*t1, *t2);
	}

	/**
	 * Null < NaN < a regular double/float value.
	 */
	int compare(Tuple const& t1, Tuple const& t2);

	TupleComparator(vector<Key> const& keys, const ArrayDesc& arrayDesc);
};

struct SortContext
{
	vector<Key> keys;
};

class TupleArray : public Array
{
	friend class TupleChunk;
	friend class TupleChunkIterator;
	friend class TupleArrayIterator;
  public:
        void sort(boost::shared_ptr<TupleComparator> tcomp);

	virtual ArrayDesc const& getArrayDesc() const;

	virtual boost::shared_ptr<ConstArrayIterator> getConstIterator(AttributeID attId) const;

	TupleArray(ArrayDesc const& schema, vector< boost::shared_ptr<ConstArrayIterator> > const& arrayIterators, size_t nChunks);
	TupleArray(ArrayDesc const& schema, vector< boost::shared_ptr<ConstArrayIterator> > const& arrayIterators, size_t shift, size_t step);
	TupleArray(ArrayDesc const& schema, boost::shared_ptr<Array> inputArray);
	TupleArray(ArrayDesc const& schema, vector< boost::shared_ptr<Tuple> > const& tuples, Coordinate offset = 0);

	/*
	 * arrayIterators may have one less attribute (the empty tag) than the output schema does.
	 * TupleArray will then add the empty tag -- because the array is dense, every empty tag is 'true'.
	 */
	virtual void append(vector< boost::shared_ptr<ConstArrayIterator> > const& arrayIterators, size_t shift, size_t step);

	/*
	 * arrayIterators may have one less attribute (the empty tag) than the output schema does.
	 */
	virtual void append(vector< boost::shared_ptr<ConstArrayIterator> > const& arrayIterators, size_t nChunks);

	/**
	 * This version of append does not yet support inputArray having one less attribute (the empty tag).
	 */
	virtual void append(boost::shared_ptr<Array> inputArray);

	/**
	 * Truuncate size in array descriptor to real number of available tuples
	 */
	void truncate();

	size_t getNumberOfTuples() const {
		return tuples.size();
	}

	/**
	 * Compute the memory footprint of a single tuple. Useful for planning purposes.
	 * This is NOT equal to the size of a cell inside a structure like a MemArray. MemArrays use RLEPayloads which exhibit
	 * much smaller per-value footprint.
	 */
	static size_t getTupleFootprint(Attributes const& attrs);
	inline size_t getTupleFootprint() const
	{
		return getTupleFootprint(desc.getAttributes());
	}

  protected:
	ArrayDesc desc;
	Coordinate start;
	Coordinate end;
	vector< boost::shared_ptr<Tuple> > tuples;
	size_t chunkSize;

};

class TupleChunk : public ConstChunk
{
	friend class TupleChunkIterator;
	friend class TupleArrayIterator;
  public:
	const ArrayDesc& getArrayDesc() const;
	const AttributeDesc& getAttributeDesc() const;
	int getCompressionMethod() const;
	Coordinates const& getFirstPosition(bool withOverlap) const;
	Coordinates const& getLastPosition(bool withOverlap) const;
	boost::shared_ptr<ConstChunkIterator> getConstIterator(int iterationMode) const;

	Array const& getArray() const {
		return array;
	}

	TupleChunk(TupleArray const& array, AttributeID attrID);

 private:
	TupleArray const& array;
	AttributeID attrID;
	Coordinates firstPos;
	Coordinates lastPos;
};

class TupleArrayIterator : public ConstArrayIterator
{
  public:
	virtual ConstChunk const& getChunk();
	virtual bool end();
	virtual void operator ++();
	virtual Coordinates const& getPosition();
	virtual bool setPosition(Coordinates const& pos);
	virtual void reset();

	TupleArrayIterator(TupleArray const& array, AttributeID attrID);

  private:
	TupleArray const& array;
	AttributeID attrID;
	TupleChunk  chunk;
	Coordinates currPos;
	bool hasCurrent;
};

class TupleChunkIterator : public ConstChunkIterator
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

	TupleChunkIterator(TupleChunk const& chunk, int iterationMode);

  private:
	bool isVisible() const;

	TupleChunk const& chunk;
	TupleArray const& array;
	AttributeID attrID;
	Coordinates currPos;
	size_t last;
	int mode;
	size_t i;
};

}

#endif
