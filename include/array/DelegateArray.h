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
 * @file DelegateArray.h
 *
 * @brief The implementation of the array delegating all functionality to some other array
 */

#ifndef DELEGATE_ARRAY_H_
#define DELEGATE_ARRAY_H_

#include <string>
#include <boost/shared_array.hpp>
#include "array/MemArray.h"
#include "array/Metadata.h"

using namespace std;
using namespace boost;

namespace scidb
{

class DelegateArray;
class DelegateChunkIterator;
class DelegateArrayIterator;

class DelegateChunk : public ConstChunk
{
    friend class DelegateChunkIterator;
  public:
    const ArrayDesc& getArrayDesc() const;
    const AttributeDesc& getAttributeDesc() const;
    virtual int getCompressionMethod() const;
    virtual Coordinates const& getFirstPosition(bool withOverlap) const;
    virtual Coordinates const& getLastPosition(bool withOverlap) const;
    virtual boost::shared_ptr<ConstChunkIterator> getConstIterator(int iterationMode) const;

    virtual void setInputChunk(ConstChunk const& inputChunk);
    ConstChunk const& getInputChunk() const;
    DelegateArrayIterator const& getArrayIterator() const;
    bool isDirectMapping() const;

    size_t count() const;
    bool isCountKnown() const;
    bool isMaterialized() const;
    void* getData() const;
    size_t getSize() const;
    bool pin() const;
    void unPin() const;
    void compress(CompressedBuffer& buf, boost::shared_ptr<ConstRLEEmptyBitmap>& emptyBitmap) const;
    Array const& getArray() const;

    void overrideClone(bool clone = true) {
        isClone = clone;
    }

    virtual void overrideTileMode(bool enabled);
  
    bool inTileMode() const { 
        return tileMode;
    }

    DelegateChunk(DelegateArray const& array, DelegateArrayIterator const& iterator, AttributeID attrID, bool isClone);

    DelegateArray const& getDelegateArray() const { 
        return array;
    }

  protected:
    DelegateArray const& array;
    DelegateArrayIterator const& iterator;
    AttributeID attrID;
    ConstChunk const* chunk;
    bool isClone;
    bool tileMode;
};

class DelegateChunkIterator : public ConstChunkIterator
{
  public:
    virtual int getMode();
    virtual Value& getItem();
    virtual bool isEmpty();
    virtual bool end();
    virtual void operator ++();
    virtual Coordinates const& getPosition();
    virtual bool setPosition(Coordinates const& pos);
    virtual void reset();
    virtual ConstChunk const& getChunk();

    DelegateChunkIterator(DelegateChunk const* chunk, int iterationMode);
    virtual ~DelegateChunkIterator() {}

  protected:
    DelegateChunk const* chunk;
    boost::shared_ptr<ConstChunkIterator> inputIterator;
};

class DelegateArrayIterator : public ConstArrayIterator
{
  public:
	DelegateArrayIterator(DelegateArray const& delegate, AttributeID attrID, boost::shared_ptr<ConstArrayIterator> inputIterator);

	virtual ConstChunk const& getChunk();
        boost::shared_ptr<ConstArrayIterator> getInputIterator() const;

	virtual bool end();
	virtual void operator ++();
	virtual Coordinates const& getPosition();
	virtual bool setPosition(Coordinates const& pos);
	virtual void reset();

  protected:
    DelegateArray const& array;	
    AttributeID attr;
    boost::shared_ptr<ConstArrayIterator> inputIterator;
    boost::shared_ptr<DelegateChunk> chunk;
    bool chunkInitialized;
};

class DelegateArray : public Array
{
  public:
	DelegateArray(ArrayDesc const& desc, boost::shared_ptr<Array> input, bool isClone = false);

	virtual ~DelegateArray()
	{}

    /**
     * Get the least restrictive access mode that the array supports.
     * @return the least restrictive access mode supported by the input array.
     */
    virtual Access getSupportedAccess() const
    {
        return inputArray->getSupportedAccess();
    }

	virtual string const& getName() const;
	virtual ArrayID getHandle() const;
	virtual const ArrayDesc& getArrayDesc() const;
	virtual boost::shared_ptr<ConstArrayIterator> getConstIterator(AttributeID id) const;

    boost::shared_ptr<Array> getInputArray() const; 

    virtual DelegateChunk* createChunk(DelegateArrayIterator const* iterator, AttributeID id) const;
    virtual DelegateChunkIterator* createChunkIterator(DelegateChunk const* chunk, int iterationMode) const;
    virtual DelegateArrayIterator* createArrayIterator(AttributeID id) const;

  protected:
	ArrayDesc desc;
	boost::shared_ptr<Array> inputArray;
    bool isClone;
};

class ShallowDelegateArray : public DelegateArray
{
public:
    ShallowDelegateArray(ArrayDesc const& desc, boost::shared_ptr<Array> input):
        DelegateArray(desc,input)
    {}

    virtual ~ShallowDelegateArray()
    {}

    virtual boost::shared_ptr<ConstArrayIterator> getConstIterator(AttributeID id) const
    {
        return inputArray->getConstIterator(id);
    }
};

/**
 * Array with dummy empty-tag attribute - used to perform operations with 
 * emptyable and non-emptyable arrays
 */
class NonEmptyableArray : public DelegateArray
{
    class DummyBitmapChunkIterator : public DelegateChunkIterator
    {
      public:
        virtual Value& getItem();
        virtual bool isEmpty();

        DummyBitmapChunkIterator(DelegateChunk const* chunk, int iterationMode);

      private:
        Value _true;
    };
    class DummyBitmapArrayIterator : public DelegateArrayIterator
    {
      public:
        ConstChunk const& getChunk();
        DummyBitmapArrayIterator(DelegateArray const& delegate, AttributeID attrID, boost::shared_ptr<ConstArrayIterator> inputIterator);
      private:
        MemChunk shapeChunk;
    };
  public:
	NonEmptyableArray(boost::shared_ptr<Array> input);

    virtual DelegateArrayIterator* createArrayIterator(AttributeID id) const;
    virtual DelegateChunkIterator* createChunkIterator(DelegateChunk const* chunk, int iterationMode) const;
    virtual DelegateChunk* createChunk(DelegateArrayIterator const* iterator, AttributeID id) const;

  private:
    AttributeID emptyTagID;
};

/**
 * Array splitting C++ array into chunks
 */
class SplitArray : public DelegateArray 
{
  protected:
    class ArrayIterator : public DelegateArrayIterator
    {
      public:
        virtual ConstChunk const& getChunk();
        virtual bool end();
        virtual void operator ++();
        virtual Coordinates const& getPosition();
        virtual bool setPosition(Coordinates const& pos);
        virtual void reset();

        ArrayIterator(SplitArray const& array, AttributeID attrID);

      protected:
        MemChunk chunk;
        Address addr;
        Dimensions const& dims;
        SplitArray const& array;
        bool hasCurrent;
        bool chunkInitialized;
      private:
        size_t attrBitSize;
    };

  public:
    SplitArray(ArrayDesc const& desc,
               const boost::shared_array<char>& src,
               Coordinates const& from,
               Coordinates const& till,
               shared_ptr<Query>const& query);
    virtual ~SplitArray();

    /**
     * Get the least restrictive access mode that the array supports.
     * @return Array::RANDOM
     */
    virtual Access getSupportedAccess() const
    {
        return Array::RANDOM;
    }

    virtual DelegateArrayIterator* createArrayIterator(AttributeID id) const;
    const Coordinates& from() const { return _from; }
    const Coordinates& till() const { return _till; }
    const Coordinates& size() const { return _size; }
    const Coordinates& startingChunk() const { return _startingChunk; }
  private:
    Coordinates _startingChunk;
    Coordinates _from;
    Coordinates _till;
    Coordinates _size;
    boost::shared_array<char> _src;
    bool        _empty;
};

/**
 * Array materializing chunks
 */
class MaterializedArray : public DelegateArray
{
  public:
    enum MaterializeFormat { 
        PreserveFormat,
        RLEFormat,
        DenseFormat
    };
    MaterializeFormat _format;
    std::vector< std::map<Coordinates, boost::shared_ptr<MemChunk>, CoordinatesLess > > _chunkCache;
    std::map<Coordinates, boost::shared_ptr<ConstRLEEmptyBitmap>, CoordinatesLess > _bitmapCache;
    Mutex _mutex;
    size_t _cacheSize;

    static void materialize(const shared_ptr<Query>& query, MemChunk& materializedChunk, ConstChunk const& chunk, MaterializeFormat format);

    boost::shared_ptr<MemChunk> getMaterializedChunk(ConstChunk const& inputChunk);

    class ArrayIterator : public DelegateArrayIterator
    {
        MaterializedArray& _array;
        ConstChunk const* _chunkToReturn;
        boost::shared_ptr<MemChunk> _materializedChunk;

      public:
        virtual ConstChunk const& getChunk();
        virtual void operator ++();
        virtual bool setPosition(Coordinates const& pos);
        virtual void reset();

        ArrayIterator(MaterializedArray& arr, AttributeID attrID, boost::shared_ptr<ConstArrayIterator> input, MaterializeFormat chunkFormat);
    };

    MaterializedArray(boost::shared_ptr<Array> input, shared_ptr<Query>const& query, MaterializeFormat chunkFormat = PreserveFormat);

    virtual DelegateArrayIterator* createArrayIterator(AttributeID id) const;
};


} //namespace

#endif /* DELEGATE_ARRAY_H_ */
