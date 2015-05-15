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
 * @file Array.h
 *
 * @brief The Array interface of SciDB
 *
 * Arrays are accessed via chunk iterators, which in turn have item iterators.
 * We have constant and volatile iterators, for read-only or write-once access to the arrays.
 */

#ifndef ARRAY_H_
#define ARRAY_H_

#include <set>
#include <boost/shared_ptr.hpp>
#include <boost/weak_ptr.hpp>
#include <boost/noncopyable.hpp>
#include <util/CoordinatesMapper.h>
#include <array/Metadata.h>
#include <array/TileInterface.h>
#include <query/TypeSystem.h>
#include <query/Statistics.h>

namespace scidb
{
class Array;
class Aggregate;
class Query;
class Chunk;
class ConstArrayIterator;
class ConstRLEEmptyBitmap;

typedef std::set<Coordinates, CoordinatesLess> CoordinateSet;

/** \brief SharedBuffer is an abstract class for binary data holding
 *
 * It's used in network manager for holding binary data.
 * Before using object you should pin it and unpin after using.
 */
class SharedBuffer
{
public:
    virtual ~SharedBuffer() { }
    /**
     * @return pointer to binary buffer.
     * Note, data is available only during object is live.
     */
    virtual void* getData() const = 0;

    /**
     * @retrun a const pointer to binary buffer.  you may only read this data
     * Note, data is available only during object is live.
     */
    virtual void const* getConstData() const { return getData(); }

    /**
     * @return size of buffer in bytes
     */
    virtual size_t getSize() const = 0;

    /**
     * Method allocates memory for buffer inside implementation.
     * Implementor will manage this buffer itself.
     * @param size is a necessary size of memory to be allocated.
     */
    virtual void allocate(size_t size);

    /**
     * Method reallocates memory for buffer inside implementation.
     * Old buffer content is copuied to the new location.
     * Implementor will manage this buffer itself.
     * @param size is a necessary size of memory to be allocated.
     */
    virtual void reallocate(size_t size);

    /**
     * Free memory. After execution of this method getData() should return NULL.
     */
    virtual void free();

    /**
     * Tell to increase reference counter to hold buffer in memory
     * @return true if buffer is pinned (need to be unpinned) false otherwise
     */
    virtual bool pin() const = 0;

    /**
     * Tell to decrease reference counter to release buffer in memory
     * to know when it's not needed.
     */
    virtual void unPin() const = 0;
};

class MemoryBuffer : public SharedBuffer
{
  private:
    char*  data;
    size_t size;
    bool   copied;

  public:
    void* getData() const
    {
        return data;
    }

    size_t getSize() const
    {
        return size;
    }

    void free()
    {
        if (copied) {
            delete[] data;
        }
        data = NULL;
    }

    bool pin() const
    {
        return false;
    }

    void unPin() const
    {
    }

    ~MemoryBuffer()
    {
        free();
    }

    MemoryBuffer(const void* ptr, size_t len, bool copy = true) {
        if (copy) {
            data = new char[len];
            if (ptr != NULL) {
                memcpy(data, ptr, len);
            }
            copied = true;
            currentStatistics->allocatedSize += len;
            currentStatistics->allocatedChunks++;
        } else {
            data = (char*)ptr;
            copied = false;
        }
        size = len;
    }
};

/**
 * Buffer with compressed data
 */
class CompressedBuffer : public SharedBuffer
{
  private:
    size_t compressedSize;
    size_t decompressedSize;
    void*  data;
    int    compressionMethod;
  public:
    virtual void* getData() const;
    virtual size_t getSize() const;
    virtual void allocate(size_t size);
    virtual void reallocate(size_t size);
    virtual void free();
    virtual bool pin() const;
    virtual void unPin() const;

    int    getCompressionMethod() const;
    void   setCompressionMethod(int compressionMethod);

    size_t getDecompressedSize() const;
    void   setDecompressedSize(size_t size);

    CompressedBuffer(void* compressedData, int compressionMethod, size_t compressedSize, size_t decompressedSize);
    CompressedBuffer();
    ~CompressedBuffer();
};


/**
 * Macro to set coordinate in ChunkIterator::moveNext mask
 */
#define COORD(i) ((uint64_t)1 << (i))

class ConstChunk;
class MemChunk;

/**
 * Common const iterator interface
 */
class ConstIterator
{
public:
    /**
     * Check if end of chunk is reached
     * @return true if iterator reaches the end of the chunk
     */
    virtual bool end() = 0;

    /**
     * Position cursor to the next element (order of traversal depends on used iteration mode)
     */
    virtual void operator ++() = 0;

    /**
     * Get coordinates of the current element in the chunk
     */
    virtual Coordinates const& getPosition() = 0;

    /**
     * Set iterator's current positions
     * @return true if specified position is valid (belongs to the chunk and match current iteratation mode),
     * false otherwise
     */
    virtual bool setPosition(Coordinates const& pos) = 0;

    /**
     * Reset iterator to the first element
     */
    virtual void reset() = 0;

    virtual ~ConstIterator();

};


/**
 * Iterator over items in the chunk. The chunk consists of a number of Value entries with
 * positions in the coordinate space, as well as flags:
 *      NULL - the value is unknown
 *      core - the value is a core value managed by the current instance
 *      overlap - the value is an overlap value, it can only be used for computation, but
 *              its managed by some other site
 */
class ConstChunkIterator : public ConstIterator
{
  public:
    /**
     * Constants used to specify iteration mode mask
     */
    enum IterationMode {
        /**
         * Ignore components having null value
         */
        IGNORE_NULL_VALUES  = 1,
        /**
         * Ignore empty array elements
         */
        IGNORE_EMPTY_CELLS = 2,
        /**
         * Ignore overlaps
         */
        IGNORE_OVERLAPS = 4,
        /**
         * Do not check for empty cells event if there is empty attribute in array
         */
        NO_EMPTY_CHECK = 8,
        /**
         * When writing append empty bitmap to payload
         */
        APPEND_EMPTY_BITMAP = 16,
        /**
         * Append to the existed chunk
         */
        APPEND_CHUNK = 32,
        /**
         * Ignore default value in sparse array
         */
        IGNORE_DEFAULT_VALUES = 64,
        /**
         * Unused mode
         */
        UNUSED_VECTOR_MODE = 128,
        /**
         * Tile mode
         */
        TILE_MODE = 256,
        /**
         * Data is written in stride-major order
         */
        SEQUENTIAL_WRITE = 512,
        /**
         * Intended tile mode
         */
        INTENDED_TILE_MODE = 1024
    };

    /**
     * Get current iteration mode
     */
    virtual int getMode() = 0;

    /**
     * Get current element value
     */
    virtual Value& getItem() = 0;

    /**
     * Check if current array cell is empty (if iteration mode allows visiting of empty cells)
     */
    virtual bool isEmpty() = 0;

    /**
     * Move forward in the specified direction
     * @param direction bitmask of coordinates in which direction movement is performed,
     * for example in case of two dimensional matrix [I=1:10, J=1:100]
     * moveNext(COORD(0)) increments I coordinate, moveNext(COORD(1)) increments J coordinate and
     * moveNext(COORD(0)|COORD(1)) increments both coordinates
     * @return false if movement in the specified direction is not possible
     */
    virtual bool forward(uint64_t direction = COORD(0));

    /**
     * Move backward in the specified direction
     * @param direction bitmask of of coordinates in which direction movement is performed,
     * for example in case of two dimensional matrix [I=1:10, J=1:100]
     * moveNext(COORD(0)) decrements I coordinate, moveNext(COORD(1)) decrements J coordinate and
     * moveNext(COORD(0)|COORD(1)) decrements both coordinates
     * @return false if movement in the specified direction is not possible
     */
    virtual bool backward(uint64_t direction = COORD(0));

    /**
     * Get iterated chunk
     */
    virtual ConstChunk const& getChunk() = 0;

    /**
     * Get first position in the iterated chunk according to the iteration mode
     */
    virtual Coordinates const& getFirstPosition();

    /**
     * Get last position in the iterated chunk according to the iteration mode
     */
    virtual Coordinates const& getLastPosition();

    /**
     * Return a tile of at most maxValues starting at the logicalStart coordinates.
     * The logical position is advanced by the size of the returned tile.
     * @parm offset - [IN] array coordinates of the first data element
     *                      [OUT] Coordinates object to which the return value refers
     * @param maxValues   - max number of values in a tile
     * @param tileData    - output data tile
     * @param tileCoords  - output tile of array coordinates, one for each element in the data tile
     * @return scidb::Coordinates() if no data are found at the logicalStart coordinates
     *         (either at the end of chunk or at a logical "whole" in the serialized data);
     *         otherwise, the next set of array coordinates where data exist in row-major order.
     *         If the next position is at the end of the chunk, scidb::Coordinates() is returned and
     *         the output variables may contain data/coordinates.
     * @note a reference implementation is provided to avoid breaking (or rather fixing) all existing child implementations
     */
    virtual const Coordinates&
    getData(scidb::Coordinates& offset,
            size_t maxValues,
            boost::shared_ptr<BaseTile>& tileData,
            boost::shared_ptr<BaseTile>& tileCoords)
    {
        throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_UNREACHABLE_CODE)
        << "ConstChunkIterator::getData(const Coordinates)";
    }

    /**
     * Return a tile of at most maxValues starting at logicalStart.
     * The logical position is advanced by the size of the returned tile.
     * @parm logicalOffset - logical position (in row-major order) within a chunk of the first data element
     * @param maxValues   - max number of values in a tile
     * @param tileData    - output data tile
     * @param tileCoords  - output tile of logical position_t's, one for each element in the data tile
     * @return positon_t(-1) if no data is found at the logicalStart position
     *         (either at the end of chunk or at a logical "whole" in the serialized data);
     *         otherwise, the next position where data exist in row-major order.
     *         If the next position is at the end of the chunk, positon_t(-1) is returned and
     *         the output variables may contain data/coordinates.
     * @note a reference implementation is provided to avoid breaking (or rather fixing) all existing child implementations
     */
    virtual position_t
    getData(position_t logicalOffset,
            size_t maxValues,
            boost::shared_ptr<BaseTile>& tileData,
            boost::shared_ptr<BaseTile>& tileCoords)
    {
        throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_UNREACHABLE_CODE)
        << "ConstChunkIterator::getData(positon_t)";
    }

    /**
     * Return a tile of at most maxValues starting at the logicalStart coordinates.
     * The logical position is advanced by the size of the returned tile.
     * @parm offset - [IN] array coordinates of the first data element
     *                [OUT] Coordinates object to which the return value refers
     * @param maxValues   - max number of values in a tile
     * @param tileData    - output data tile
     * @return scidb::Coordinates() if no data are found at the logicalStart coordinates
     *         (either at the end of chunk or at a logical "whole" in the serialized data);
     *         otherwise, the next set of array coordinates where data exist in row-major order.
     *         If the next position is at the end of the chunk, scidb::Coordinates() is returned and
     *         the output variables may contain data/coordinates.
     * @note a reference implementation is provided to avoid breaking (or rather fixing) all existing child implementations
     */
    virtual const Coordinates&
    getData(scidb::Coordinates& offset,
            size_t maxValues,
            boost::shared_ptr<BaseTile>& tileData)
    {
        throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_UNREACHABLE_CODE)
        << "ConstChunkIterator::getData(const Coordinates, data)";
    }

    /**
     * Return a tile of at most maxValues starting at logicalStart.
     * The logical position is advanced by the size of the returned tile.
     * @parm logicalOffset - logical position (in row-major order) within a chunk of the first data element
     * @param maxValues   - max number of values in a tile
     * @param tileData    - output data tile
     * @return positon_t(-1) if no data is found at the logicalStart position
     *         (either at the end of chunk or at a logical "whole" in the serialized data);
     *         otherwise, the next position where data exist in row-major order.
     *         If the next position is at the end of the chunk, positon_t(-1) is returned and
     *         the output variables may contain data/coordinates.
     * @note a reference implementation is provided to avoid breaking (or rather fixing) all existing child implementations
     */
    virtual position_t
    getData(position_t logicalOffset,
            size_t maxValues,
            boost::shared_ptr<BaseTile>& tileData)
    {
        throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_UNREACHABLE_CODE)
        << "ConstChunkIterator::getData(positon_t,data)";
    }

    /**
     * @return a mapper capable of converting logical positions to/from array coordinates
     *         assuming row-major serialization order
     * @note a reference implementation is provided to avoid breaking (or rather fixing) all existing child implementations
     */
    virtual operator const CoordinatesMapper* () const
    {
        throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_UNREACHABLE_CODE)
        << "ConstChunkIterator::operator()(CoordinatesMapper*)";
    }

    /**
     * @return the iterator current logical position within a chunk
     *         assuming row-major serialization order
     * @note a reference implementation is provided to avoid breaking (or rather fixing) all existing child implementations
     */
    virtual position_t getLogicalPosition()
    {
        throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_UNREACHABLE_CODE)
        << "ConstChunkIterator::getLogicalPosition";
    }

    /**
     * Set the current iterator position corresponding to the logical position within a chunk
     *         assuming row-major serialization order
     * @return true if a cell with such position exist and its position is successfully recorded
     * @note a reference implementation is provided to avoid breaking (or rather fixing) all existing child implementations
     */
    using ConstIterator::setPosition;
    virtual bool setPosition(position_t pos)
    {
        throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_UNREACHABLE_CODE)
        << "ConstChunkIterator::setPosition";
    }
};


/**
 * The volatile iterator can also write items to the array
 */
class ChunkIterator : public ConstChunkIterator
{
public:
    /**
     * Update the current element value
     */
     virtual void writeItem(const  Value& item) = 0;

    /**
     * Save all changes done in the chunk
     */
    virtual void flush() = 0;

    /// Query context for this iterator
    virtual boost::shared_ptr<Query> getQuery() = 0;
};

/**
 * A read only chunk interface provides information on whether the chunk is:
 *   readonly - isReadOnly()
 *   positions:
 *      getFirstPosition(withOverlap) - provides the smallest position in stride-major order
 *      getLastPosition(withOverlap) - provides the largest position in stride-major order
 *      positions can be computed with or without taking overlap items into account
 *  Also the chunk can be:
 *  An iterator can be requested to access the items in the chunk:
 *      getConstIterator() - returns a read-only iterator to the items
 *      getIterator() - returns a volatile iterator to the items (the chunk cannot be read-only)
 */
class ConstChunk : public SharedBuffer
{
  public:
    /**
     * Check if this is MemChunk.
     */
    virtual bool isMemChunk() const
    {
        return false;
    }

   virtual bool isReadOnly() const;

   /**
    * Check if chunk data is stored somewhere (in memory on on disk)
    */
   virtual bool isMaterialized() const;

   size_t getBitmapSize() const;

   /**
    * Get array descriptor
    */
   virtual const ArrayDesc& getArrayDesc() const = 0;

   /**
    * Get chunk attribute descriptor
    */
   virtual const AttributeDesc& getAttributeDesc() const = 0;

   /**
    * Count number of present (non-empty) elements in the chunk.
    * Materialized subclasses that do not use the field materializedChunk might want to provide their own implementation.
    * @return the number of non-empty elements in the chunk.
    */
   virtual size_t count() const;

   /**
    * Check if count of non-empty elements in the chunk is known.
    * Materialized subclasses that do not use the field materializedChunk might want to provide their own implementation.
    * @return true if count() will run in constant time; false otherwise.
    */
   virtual bool isCountKnown() const;

   /**
    * Get numer of logical elements in the chunk.
    * @return the product of the chunk sizes in all dimensions.
    */
   size_t getNumberOfElements(bool withOverlap) const;

    /**
     * If chunk contains no gaps in its data: has no overlaps and fully belongs to non-emptyable array.
     */
   bool isSolid() const;

   virtual Coordinates const& getFirstPosition(bool withOverlap) const = 0;
   virtual Coordinates const& getLastPosition(bool withOverlap) const = 0;

   bool contains(Coordinates const& pos, bool withOverlap) const;

   virtual boost::shared_ptr<ConstChunkIterator> getConstIterator(int iterationMode = ConstChunkIterator::IGNORE_OVERLAPS|ConstChunkIterator::IGNORE_EMPTY_CELLS) const = 0;
   ConstChunkIterator* getConstIteratorPtr(int iterationMode = ConstChunkIterator::IGNORE_OVERLAPS|ConstChunkIterator::IGNORE_EMPTY_CELLS) {
      // TODO JHM ; temporary bridge to support concurrent development, to be removed by the end of RQ
#ifndef NO_SUPPPORT_FOR_SWIG_TARGETS_THAT_CANT_HANDLE_SHARED_PTRS
      return getConstIterator(iterationMode).operator->();
#else
      assert(false);
      return NULL;
#endif // NO_SUPPPORT_FOR_SWIG_TARGETS_THAT_CANT_HANDLE_SHARED_PTRS
   }

   virtual int getCompressionMethod() const = 0;

   /**
    * Compress chunk data info the specified buffer.
    * @param buf buffer where compressed data will be placed. It is intended to be initialized using default constructor and will be filled by this method.
    */
    virtual void compress(CompressedBuffer& buf, boost::shared_ptr<ConstRLEEmptyBitmap>& emptyBitmap) const;

    virtual void* getData() const;
    virtual size_t getSize() const;
    virtual bool pin() const;
    virtual void unPin() const;

    virtual Array const& getArray() const = 0;

    void makeClosure(Chunk& closure, boost::shared_ptr<ConstRLEEmptyBitmap> const& emptyBitmap) const;

    virtual boost::shared_ptr<ConstRLEEmptyBitmap> getEmptyBitmap() const;
    virtual ConstChunk const* getBitmapChunk() const;

    /**
     * Compute and place the chunk data in memory (if needed) and return a pointer to it.
     * @return a pointer to a chunk object that is materialized; may be a pointer to this.
     */
    virtual ConstChunk* materialize() const;

    virtual void overrideTileMode(bool) {}

    /**
     * @retun true if the chunk has no cells
     * @param withOverlap true if the overlap region(s) should be included (default)
     */
    bool isEmpty(bool withOverlap=true) const
    {
        int iterationMode = ConstChunkIterator::IGNORE_EMPTY_CELLS;
        if (!withOverlap) {
            iterationMode |= ConstChunkIterator::IGNORE_OVERLAPS;
        }
        boost::shared_ptr<ConstChunkIterator> ci = getConstIterator(iterationMode);
        assert(ci);
        return (ci->end());
    }

 protected:
    ConstChunk();
    virtual ~ConstChunk();

    /**
     * A pointer to a materialized copy of this chunk. Deallocated on destruction. Used as part of materialize() and other routines like count().
     * Note that not all subclasses use this field. Note also that PersistentChunk objects can exist indefinitely without being destroyed.
     */
    MemChunk* materializedChunk;
    boost::shared_ptr<ConstArrayIterator> emptyIterator;
};

/**
 * New (intialized) chunk implementation
 */
class Chunk : public ConstChunk
{
   double expectedDensity;

protected:
   Chunk() {
      expectedDensity = 0;
   }

public:

   /**
    * Allocate and memcpy from a raw byte array.
    */
   virtual void allocateAndCopy(char const* input, size_t byteSize, size_t count,
                                const boost::shared_ptr<Query>& query) {
       assert(getData()==NULL);
       assert(input!=NULL);

       allocate(byteSize);
       setCount(count);
       memcpy(getDataForLoad(), input, byteSize);

       write(query);
   }

   virtual bool isReadOnly() const {
      return false;
   }

   /**
    * Set expected sparse chunk density
    */
   void setExpectedDensity(double density) {
      expectedDensity = density;
   }

   /**
    * Get expected sparse chunk density
    */
   double getExpectedDensity() const {
      return expectedDensity;
   }

   /**
    * Decompress chunk from the specified buffer.
    * @param buf buffer containing compressed data.
    */
   virtual void decompress(const CompressedBuffer& buf);

   virtual boost::shared_ptr<ChunkIterator> getIterator(boost::shared_ptr<Query> const& query,
                                                        int iterationMode = ChunkIterator::NO_EMPTY_CHECK) = 0;

   virtual void merge(ConstChunk const& with, boost::shared_ptr<Query> const& query);

   /**
    * This function merges at the cell level. SLOW!
    * @param[in] with   the source chunk
    * @param[in] query
    *
    * @note The caller should call merge(), instead of directly calling this.
    */
   virtual void shallowMerge(ConstChunk const& with, boost::shared_ptr<Query> const& query);

   /**
    * This function tries to merge at the segment level. FAST!
    * Segment-level merging is performed if both chunks have empty-bitmap attached to the end.
    * Otherwise, shallowMerge is called.
    *
    * @param[in] with   the source chunk
    * @param[in] query
    *
    * @note The caller should call merge(), instead of directly calling this.
    * @pre The chunks must be MemChunks.
    * @pre The chunks must be in RLE format.
    */
   virtual void deepMerge(ConstChunk const& with, boost::shared_ptr<Query> const& query);

   /**
    * Perform a generic aggregate-merge of this with another chunk.
    * This is an older algorithm. Currently only used by aggregating redimension.
    * @param[in] with chunk to merge with. Must be filled out by an aggregating op.
    * @param[in] aggregate the aggregate to use
    * @param[in] query the query context
    */
   virtual void aggregateMerge(ConstChunk const& with,
                               boost::shared_ptr<Aggregate> const& aggregate,
                               boost::shared_ptr<Query> const& query);

   /**
    * Perform an aggregate-merge of this with another chunk.
    * This function is optimized for current group-by aggregates, which
    * are liable to produce sparse chunks with many nulls. This method does NOT work
    * if the intermediate aggregating array is emptyable (which is what redimension uses).
    * @param[in] with chunk to merge with. Must be filled out by an aggregating op.
    * @param[in] aggregate the aggregate to use
    * @param[in] query the query context
    */
   virtual void nonEmptyableAggregateMerge(ConstChunk const& with,
                                           boost::shared_ptr<Aggregate> const& aggregate,
                                           boost::shared_ptr<Query> const& query);

   virtual void write(const boost::shared_ptr<Query>& query) = 0;
   virtual void truncate(Coordinate lastCoord);
   virtual void setCount(size_t count);

    /* Get a reference to the data buffer of the chunk for the purposes of loading
       data directly to it.  getData() should be used for reading data only.  This
       interface can be overridden by classes that need to use getData() as a hook
       for loading data from some other source.
     */
   virtual void* getDataForLoad()
        { return getData(); }
};

/**
 * An array const iterator iterates over the chunks of the array available at the local instance.
 * Order of iteration is not specified.
 */
class ConstArrayIterator : public ConstIterator
{
public:
    /**
     * Select chunk which contains element with specified position in main (not overlapped) area
     * @param pos element position
     * @return true if chunk with containing specified position is present at the local instance, false otherwise
     */
    virtual bool setPosition(Coordinates const& pos);

    /**
     * Restart iterations from the beginning
     */
    virtual void reset();

    /**
     * Get current chunk
     */
    virtual ConstChunk const& getChunk() = 0;
};

/**
 * The volatile iterator can also write chunks to the array
 */
class ArrayIterator : public ConstArrayIterator
{
public:
    virtual Chunk& updateChunk();

    /**
     * Create new chunk at the local instance using default compression method for this attribute.
     * Only one chunk can be created and filled by iterator at each moment of time.
     * @param position of the first element in the created chunk (not including overlaps)
     */
    virtual Chunk& newChunk(Coordinates const& pos) = 0;

    /**
     * Create new chunk at the local instance.
     * Only one chunk can be created and filled by iterator at each moment of time.
     * @param position of the first element in the created chunk (not including overlaps)
     */
    virtual Chunk& newChunk(Coordinates const& pos, int compressionMethod) = 0;

    /**
     * Copy chunk
     * @param srcChunk source chunk
     */
    virtual Chunk& copyChunk(ConstChunk const& srcChunk, boost::shared_ptr<ConstRLEEmptyBitmap>& emptyBitmap);

    virtual Chunk& copyChunk(ConstChunk const& srcChunk) {
        boost::shared_ptr<ConstRLEEmptyBitmap> emptyBitmap;
        return copyChunk(srcChunk, emptyBitmap);
    }

    virtual void deleteChunk(Chunk& chunk);

    /// Query context for this iterator
    virtual boost::shared_ptr<Query> getQuery() = 0;
};

class Array;

/**
 * Iterator through all array elements. This iterator combines array and chunk iterators.
 * Please notice that using that using random positioning in array can cause very significant degradation of performance
 */
class ConstItemIterator : public ConstChunkIterator
{
  public:
    virtual int getMode();
    virtual  Value& getItem();
    virtual bool isEmpty();
    virtual ConstChunk const& getChunk() ;
    virtual bool end();
    virtual void operator ++();
    virtual Coordinates const& getPosition();
    virtual bool setPosition(Coordinates const& pos);
    virtual void reset();

    ConstItemIterator(Array const& array, AttributeID attrID, int iterationMode);

  private:
    boost::shared_ptr<ConstArrayIterator> arrayIterator;
    boost::shared_ptr<ConstChunkIterator> chunkIterator;
    int iterationMode;
};

/**
 * The array interface provides metadata about the array, including its handle, type and
 * array descriptors.
 * To access the data in the array, a constant (read-only) iterator can be requested, or a
 * volatile iterator can be used.
 */
class Array :
// TODO JHM ; temporary bridge to support concurrent development, to be removed by the end of RQ
#ifndef NO_SUPPPORT_FOR_SWIG_TARGETS_THAT_CANT_HANDLE_PROTECTED_BASE_CLASSES
    public SelfStatistics
#else
    protected SelfStatistics
#endif // NO_SUPPPORT_FOR_SWIG_TARGETS_THAT_CANT_ACCEPT_PROTECTED_BASE_CLASSES
{
public:

    /**
     * An enum that defines three levels of Array read access policy - ranging from most restrictive to least restrictive.
     */
    enum Access
    {
        /**
         * Most restrictive access policy wherein the array can only be iterated over one time.
         * If you need to read multiple attributes, you need to read all of the attributes horizontally, at the same time.
         * Imagine that reading the array is like scanning from a pipe - after a single scan, the data is no longer available.
         * This is the only supported access mode for InputArray and MergeSortArray.
         * Any SINGLE_PASS array must inherit from scidb::SinglePassArray if that array is ever returned from PhysicalOperator::execute().
         * The reason is that the sg() operator can handle only the SINGLE_PASS arrays conforming to the SinglePassArray interface.
         */
        SINGLE_PASS = 0,

        /**
         * A policy wherein the array can be iterated over several times, and various attributes can be scanned independently,
         * but the ArrayIterator::setPosition() function is not supported.
         * This is less restrictive than SINGLE_PASS.
         * This is the least restrictive access mode fsupported by ConcatArray
         */
        MULTI_PASS  = 1,

        /**
         * A policy wherein the client of the array can use the full functionality of the API.
         * This is the least restrictive access policy and it's supported by the vast majority of Array subtypes.
         */
        RANDOM      = 2
    };

    virtual ~Array() {}

    /**
     * Get array name
     */
    virtual std::string const& getName() const;

    /**
     * Get array identifier
     */
    virtual ArrayID getHandle() const;

    /**
     * Determine if this array has an easily accessible list of chunk positions. In fact, a set of chunk positions can
     * be generated from ANY array simply by iterating over all of the chunks once. However, this function will return true
     * if retrieving the chunk positions is a separate routine that is more efficient than iterating over all chunks.
     * All materialized arrays can and should implement this function.
     * @return true if this array supports calling getChunkPositions(). false otherwise.
     */
    virtual bool hasChunkPositions() const
    {
        return false;
    }

    /**
     * Build and return a list of the chunk positions. Only callable if hasChunkPositions() returns true, throws otherwise.
     * @return the sorted set of coordinates, containing the first coordinate of every chunk present in the array
     */
    virtual boost::shared_ptr<CoordinateSet> getChunkPositions() const;

    /**
     * If hasChunkPositions() is true, return getChunkPositions(); otherwise build a list of chunk positions manually
     * by iterating over the chunks of one of the array attributes. The attribute to iterate over is chosen according to a heuristic,
     * using empty_tag if available, otherwise picking the smallest fixed-size attribute. The array getSupportedAccess() must be
     * at least MULTI_PASS.
     * @return the sorted set of coordinates, containing the first coordinate of every chunk present in the array
     */
    virtual boost::shared_ptr<CoordinateSet> findChunkPositions() const;

    /**
     * Determine if the array is materialized; which means all chunks are populated either memory or on disk, and available on request.
     * This returns false by default as that is the case with all arrays. It returns true for MemArray, etc.
     * @return true if this is materialized; false otherwise
     */
    virtual bool isMaterialized() const
    {
        return false;
    }

    /**
     * Get the least restrictive access mode that the array supports. The default for the abstract superclass is RANDOM
     * as a matter of convenience, since the vast majority of our arrays support it. Subclasses that have access
     * restrictions are responsible for overriding this appropriately.
     * @return the least restrictive access mode
     */
    virtual Access getSupportedAccess() const
    {
        return RANDOM;
    }

    /**
     * Extract subarray between specified coordinates in the buffer.
     * @param attrID extracted attribute of the array (should be fixed size)
     * @param buf buffer preallocated by caller which should be preallocated by called and be large enough
     * to fit all data.
     * @param first minimal coordinates of extract box
     * @param last maximal coordinates of extract box
     * @param init EXTRACT_INIT_ZERO or EXTRACT_INIT_NAN
     *             if buf is floating-point, EXTRACT_INIT_NAN writes a NaN in
     *             buf for each cell that was empty; otherwise a zero.
     *             the second option is only meaningful for extracting to arrays of float or double
     * @param null EXTRACT_NULL_AS_EXCEPTION or EXTRACT_NULL_AS_NAN
     *             if buf is floating-point, EXTRACT_NULL_AS_NAN writes a NaN in
     *             buf for each null; otherwise a null is an exception.
     *             if a floating-point array, whether it should be extracted as a NaN
     * @return number of extracted chunks
     */
    enum extractInit_t { EXTRACT_INIT_ZERO=0, EXTRACT_INIT_NAN };
    enum extractNull_t { EXTRACT_NULL_AS_EXCEPTION=0, EXTRACT_NULL_AS_NAN };
    virtual size_t extractData(AttributeID attrID, void* buf, Coordinates const& first, Coordinates const& last,
                               extractInit_t init=EXTRACT_INIT_ZERO,
                               extractNull_t null=EXTRACT_NULL_AS_EXCEPTION) const;

    /**
     * Append data from the array
     * @param[in] input source array
     * @param[in] vertical the traversal order of appending: if true - append all chunks for attribute 0, then attribute 1...
     *            If false - append the first chunk for all attributes, then the second chunk...
     * @param[out] newChunkCoordinates if set - the method shall insert the coordinates of all appended chunks into the set pointed to.
     */
    virtual void append(boost::shared_ptr<Array>& input, bool const vertical = true,  std::set<Coordinates, CoordinatesLess>* newChunkCoordinates = NULL);

    /**
     * Get array descriptor
     */
    virtual ArrayDesc const& getArrayDesc() const = 0;

    /**
     * Get read-write iterator
     * @param attr attribute ID
     * @return iterator through chunks of spcified attribute
     */
    virtual boost::shared_ptr<ArrayIterator> getIterator(AttributeID attr);

    /**
     * Get read-only iterator
     * @param attr attribute ID
     * @return read-only iterator through chunks of spcified attribute
     */
    virtual boost::shared_ptr<ConstArrayIterator> getConstIterator(AttributeID attr) const = 0;

    ConstArrayIterator* getConstIteratorPtr(AttributeID attr) const {
// TODO JHM ; temporary bridge to support concurrent development, to be removed by the end of RQ
#ifndef NO_SUPPPORT_FOR_SWIG_TARGETS_THAT_CANT_HANDLE_SHARED_PTRS
        return getConstIterator(attr).operator->();
#else
        assert(false);
        return NULL;
#endif // NO_SUPPPORT_FOR_SWIG_TARGETS_THAT_CANT_HANDLE_SHARED_PTRS
    }

    /**
     * Get read-only iterator thtough all array elements
     * @param attr attribute ID
     * @param iterationMode chunk iteration mode
     */
    virtual boost::shared_ptr<ConstItemIterator> getItemIterator(AttributeID attr, int iterationMode = ConstChunkIterator::IGNORE_OVERLAPS|ConstChunkIterator::IGNORE_EMPTY_CELLS) const;

    /**
     * Scan entire array and print contents to logger
     * DEBUG build only.  Otherwise a nullop
     */
    void printArrayToLogger() const;

    void setQuery(boost::shared_ptr<Query> const& query) {_query = query;}


    /**
     * If count() can return its result in O(1) [or O(nExistingChunks) time if from a
     * fast in-memory index such as a chunkMap], then
     * this method should return true to indicate it is "reasonably" fast
     * Otherwse, this should return false, for example, when chunks themselves would return false
     * from their isCountKnown() method.  If there are gray areas in between these cases
     * O(1) or O(nExistingChunks) vs O(nNonNullCells), then the API of isCountKnown()
     * will either need definition refinement and/or API revision
     * @return true if count() is sufficiently fast by the above criteria
     */
    virtual bool isCountKnown() const;

    /**
     * While we would like all arrays to do this in O(1) time or O(nChunks) time,
     * some cases still require traversal of all the chunks of one attribute of the array.
     * If that expense is too much, then isCountKnown() should return false, and you
     * should avoid calling count().
     * @return the count of all non-empty cells in the array
     */
    virtual size_t count() const; // logically const, even if an result is cached.

 protected:
    /// The query context for this array
    boost::weak_ptr<Query> _query;
};

class PinBuffer {
    SharedBuffer const& buffer;
    bool pinned;
  public:
    PinBuffer(SharedBuffer const& buf) : buffer(buf) {
        pinned = buffer.pin();
    }

    bool isPinned() const {
        return pinned;
    }

    ~PinBuffer() {
        if (pinned) {
            buffer.unPin();
        }
    }
};

typedef PinBuffer Pinner;

/**
 * Constructed around a chunk pointer to automatically unpin the chunk on destruction.
 * May be initially constructed with NULL pointer, in which case the poiter may (or may not) be reset
 * to a valid chunk pointer.
 */
class UnPinner : public boost::noncopyable
{
private:
    Chunk* _buffer;

public:
    /**
     * Create an unpinner.
     * @param buffer the chunk pointer; can be NULL
     */
    UnPinner(Chunk* buffer) : _buffer(buffer)
    {}

    ~UnPinner()
    {
        if (_buffer)
        {
            _buffer->unPin();
        }
    }

    /**
     * Set or reset the unpinner pointer.
     * @param buf the chunk pointer; can be NULL
     */
    void set(Chunk* buf)
    {
        _buffer = buf;
    }

    Chunk* get()
    {
        return _buffer;
    }
};


}

#endif /* ARRAY_H_ */
