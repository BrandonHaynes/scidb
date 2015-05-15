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
 * @file MemChunk.h
 *
 * @brief In-Memory (temporary) chunk implementation
 */

#ifndef MEM_CHUNK_H_
#define MEM_CHUNK_H_

#include <array/Array.h>
#include <vector>
#include <map>
#include <assert.h>
#include <boost/shared_ptr.hpp>
#include <boost/scoped_ptr.hpp>
#include <boost/weak_ptr.hpp>
#include <boost/shared_array.hpp>
#include <util/Lru.h>
#include <util/CoordinatesMapper.h>
#include <array/Tile.h>

using namespace std;
using namespace boost;

namespace scidb
{
    class Query;
    /**
     * An Address is used to specify the location of a chunk inside an array.
     */
    struct Address
    {
        /*
         * Attribute identifier
         */
        AttributeID attId;
        /**
         * Chunk coordinates
         */
        Coordinates coords;

        /**
         * Default constructor
         */
        Address() : attId(~0)
        {}

        /**
         * Constructor
         * @param attId attribute identifier
         * @param coords element coordinates
         */
        Address(AttributeID attId, Coordinates const& coords) :
            attId(attId), coords(coords)
        {}

        /**
         * Copy constructor
         * @param addr the object to copy from
         */
        Address(const Address& addr)
        {
            this->attId = addr.attId;
            this->coords = addr.coords;
        }

        /**
         * Partial comparison function, used to implement std::map
         * @param other another aorgument of comparison
         * @return true if "this" preceeds "other" in partial order
         */
        bool operator <(const Address& other) const
        {
            if (attId != other.attId)
            {
                return attId < other.attId;
            }
            if (coords.size() != other.coords.size())
            {
                return coords.size() < other.coords.size();
            }
            for (size_t i = 0, n = coords.size(); i < n; i++)
            {
                if (coords[i] != other.coords[i])
                {
                    return coords[i] < other.coords[i];
                }
            }
            return false;
        }

        /**
         * Equality comparison
         * @param other another aorgument of comparison
         * @return true if "this" equals to "other"
         */
        bool operator ==(const Address& other) const
        {
            if (attId != other.attId)
            {
                return false;
            }
            assert(coords.size() == other.coords.size());
            for (size_t i = 0, n = coords.size(); i < n; i++)
            {
                if (coords[i] != other.coords[i])
                {
                    return false;
                }
            }
            return true;
        }

        /**
         * Inequality comparison
         * @param other another aorgument of comparison
         * @return true if "this" not equals to "other"
         */
        bool operator !=(const Address& other) const
        {
            return !(*this == other);
        }

        /**
         * Compute a hash of coordiantes
         * @return a 64-bit hash of the chunk coordinates.
         */
        uint64_t hash() const
        {
            uint64_t h = 0;
            for (int i = coords.size(); --i >= 0;)
            {
                h ^= coords[i];
            }
            return h;
        }
    };

    class MemArray;
#ifndef SCIDB_CLIENT
    class MemArrayIterator;
#endif
    /**
     * Chunk of temporary (in-memory) array
     */
    class MemChunk : public Chunk
    {
	#ifndef SCIDB_CLIENT
	friend class MemArray;
        friend class MemArrayIterator;
        friend class SharedMemCache;
	#endif
      protected:
        Address addr; // address of first chunk element
        void*   data; // uncompressed data (may be NULL if swapped out)
        mutable bool dirty; // true if dirty data might be present in buffer
        size_t  size;
        size_t  nElems;
        int     compressionMethod;
        Coordinates firstPos;
        Coordinates firstPosWithOverlaps;
        Coordinates lastPos;
        Coordinates lastPosWithOverlaps;
        ArrayDesc const* arrayDesc;
        Chunk* bitmapChunk;
        Array const* array;
        boost::shared_ptr<ConstRLEEmptyBitmap> emptyBitmap;
        boost::shared_ptr<ConstChunkIterator>
            getConstIterator(boost::shared_ptr<Query> const& query, int iterationMode) const;
      public:
        MemChunk();
        ~MemChunk();

        boost::shared_ptr<ConstRLEEmptyBitmap> getEmptyBitmap() const;
        void setEmptyBitmap(boost::shared_ptr<ConstRLEEmptyBitmap> const& bitmap);

        Address const& getAddress() const
        {
            return addr;
        }

        /**
         * @see ConstChunk::isMemChunk
         */
        virtual bool isMemChunk() const
        {
            return true;
        }

        size_t count() const;
        bool   isCountKnown() const;
        void setCount(size_t count);

        virtual bool isTemporary() const;

        /**
         * @see ConstChunk::isMaterialized
         */
        bool isMaterialized() const;

        /**
         * @see ConstChunk::materialize
         */
        ConstChunk* materialize() const
        {
            assert(materializedChunk == NULL);
            return const_cast<MemChunk*> (this);
        }

        /**
         * @see Chunk::write
         */
        virtual void write(const boost::shared_ptr<Query>& query);

        void fillRLEBitmap();

        virtual void initialize(Array const* array, ArrayDesc const* desc,
                                const Address& firstElem, int compressionMethod);
        virtual void initialize(ConstChunk const& srcChunk);

        void setBitmapChunk(Chunk* bitmapChunk);

        virtual bool isInitialized() const
        {
            return arrayDesc != NULL;
        }

        ConstChunk const* getBitmapChunk() const;

        Array const& getArray() const;
        const ArrayDesc& getArrayDesc() const;
        void setArrayDesc(const ArrayDesc* desc) { arrayDesc = desc; assert(desc); }
        const AttributeDesc& getAttributeDesc() const;
        int getCompressionMethod() const { return compressionMethod; }
        void* getData() const { dirty = true; return data; }
        void const* getConstData() const { return data; }
        bool isDirty() const { return dirty; }
        void markClean() { dirty = false; }
        /**
         * returns the amount of memory, in bytes, backing the chunk
         * @return size_t containing the number of bytes in memory
         */
        size_t getSize() const { return size; }
        void allocate(size_t size);
        void reallocate(size_t size);
        void free();
        Coordinates const& getFirstPosition(bool withOverlap) const;
        Coordinates const& getLastPosition(bool withOverlap) const;
        boost::shared_ptr<ChunkIterator> getIterator(boost::shared_ptr<Query> const& query, int iterationMode);
        boost::shared_ptr<ConstChunkIterator> getConstIterator(int iterationMode) const;
        bool pin() const;
        void unPin() const;

        void compress(CompressedBuffer& buf, boost::shared_ptr<ConstRLEEmptyBitmap>& emptyBitmap) const;
        void decompress(const CompressedBuffer& buf);

        static size_t getFootprint(size_t ndims)
        { return sizeof(MemChunk) + (4 * (ndims * sizeof(Coordinate))); }
    };



#ifndef SCIDB_CLIENT
    class LruMemChunk;
    typedef LRUSecondary<LruMemChunk*> MemChunkLru;
    typedef LRUSecondary<LruMemChunk*>::ListIterator MemChunkLruIterator;

    /**
     * Chunk of a temporary array whose body can be located either in memory either on disk.
     */
    class LruMemChunk : public MemChunk
    {
        friend class MemArray;
        friend class MemArrayIterator;
        friend class SharedMemCache;

    private:
        /**
         * Iterator indicating the position of the chunk in the LRU cache.
         */
        MemChunkLruIterator _whereInLru;

        /**
         * The offset into the datastore where the chunk has been written to.
         */
        int64_t      _dsOffset;

        /**
         * The difference between the number of times pin was called, minus the number of times unPin was called.
         */
        size_t       _accessCount;

        /**
         * The size of the allocated region in the datastore.
         */
        size_t       _dsAlloc;

        /**
         * The size of the chunk the last time we pinned or unPinned it. If you follow proper prodecure, the chunk size should
         * only change at unPin time; hence the name.
         */
        size_t       _sizeAtLastUnPin;

      public:
        /**
         * Create a new chunk, not in LRU with size 0.
         */
        LruMemChunk();

        ~LruMemChunk();

        /**
         * @see: MemChunk::pin
         */
        bool pin() const;

        /**
         * @see: MemChunk::unPin
         */
        void unPin() const;

        /**
         * @see: MemChunk::isTemporary
         */
        bool isTemporary() const;

        /**
         * Determine if the chunk is in the LRU.
         * @return true if the chunk is not in the LRU. False otherwise.
         */
        bool isEmpty() const;

        /**
         * Take a note that this LruMemChunk has been removed from the Lru.
         */
        void prune();

        /**
         * Remove the chunk from the LRU.
         */
        void removeFromLru();

        /**
         * Add the chunk to the LRU.
         */
        void pushToLru();

        /**
         * @see Chunk::write
         */
        virtual void write(const boost::shared_ptr<Query>& query);

        /**
         * Initialize chunk
         * @param array to which this chunk belongs
         * @param desc the array descriptor
         * @param firsElem chunk coords
         * @param compressionMethod
         */
        void initialize(MemArray const* array, ArrayDesc const* desc,
                        const Address& firstElem, int compressionMethod);
        virtual void initialize(Array const* array, ArrayDesc const* desc,
                                const Address& firstElem, int compressionMethod);
        virtual void initialize(ConstChunk const& srcChunk);
        boost::shared_ptr<ConstChunkIterator> getConstIterator(int iterationMode) const;
        boost::shared_ptr<ChunkIterator> getIterator(boost::shared_ptr<Query> const& query,
                                                     int iterationMode);

        /**
         * Calulate the overhead of an LruMemChunk based on the number of dimensions
         */
        static size_t getFootprint(size_t ndims)
        { return MemChunk::getFootprint(ndims) - sizeof(MemChunk) + sizeof(LruMemChunk); }
    };

#endif

    class BaseChunkIterator: public ChunkIterator, protected CoordinatesMapper
    {
      protected:
        ArrayDesc const& array;
        AttributeID attrID;
        AttributeDesc const& attr;
        Chunk* dataChunk;
        bool   dataChunkPinned;
        bool   hasCurrent;
        bool   hasOverlap;
        bool   isEmptyable;
        int    mode;
        boost::shared_ptr<ConstRLEEmptyBitmap> emptyBitmap;
        ConstRLEEmptyBitmap::iterator emptyBitmapIterator;

        Coordinates currPos;
        TypeId typeId;
        Type   type;
        Value const& defaultValue;
        uint64_t tilePos;
        uint64_t tileSize;
        bool isEmptyIndicator;
        boost::weak_ptr<Query> _query;

        BaseChunkIterator(ArrayDesc const& desc, AttributeID attr, Chunk* data, int iterationMode, boost::shared_ptr<Query> const& query);
        ~BaseChunkIterator();

      public:
        int  getMode();
        bool isEmpty();
        bool end();
        bool setPosition(Coordinates const& pos);
        void operator ++();
        ConstChunk const& getChunk();
        void reset();
        void writeItem(const Value& item);
        void flush();
        Coordinates const& getPosition();
        boost::shared_ptr<ConstRLEEmptyBitmap> getEmptyBitmap();
        boost::shared_ptr<Query> getQuery()
        {
            return _query.lock();
        }
    };

    class RLEConstChunkIterator : public BaseChunkIterator
    {
      public:
        RLEConstChunkIterator(ArrayDesc const& desc, AttributeID attr, Chunk* data, Chunk* bitmap, int iterationMode,
                              boost::shared_ptr<Query> const& query);

        Value& getItem();
        bool setPosition(Coordinates const& pos);
        void operator ++();
        void reset();

      private:
        ConstRLEPayload payload;
        ConstRLEPayload::iterator payloadIterator;

        Value value;
    };

    class RLEBitmapChunkIterator : public BaseChunkIterator
    {
      public:
        Value& getItem();

        RLEBitmapChunkIterator(ArrayDesc const& desc, AttributeID attr,
                               Chunk* data,
                               Chunk* bitmap,
                               int iterationMode,
                               boost::shared_ptr<Query> const& query);

      private:
        Value trueValue;
        Value value;
    };

    class RLEChunkIterator : public BaseChunkIterator
    {
      public:
        bool isEmpty();
        Value& getItem();
        void writeItem(const Value& item);
        void flush();
        bool setPosition(Coordinates const& pos);

        RLEChunkIterator(ArrayDesc const& desc, AttributeID attr,
                         Chunk* data,
                         Chunk* bitmap,
                         int iterationMode,
                         boost::shared_ptr<Query> const& query);
        virtual ~RLEChunkIterator();

      private:
        position_t getPos() {
            return isEmptyable ? emptyBitmapIterator.getLPos() : emptyBitmapIterator.getPPos();
        }
        arena::ArenaPtr const _arena;
        ValueMap  _values;
        size_t    _valuesFootprint;  // current memory footprint of elements in _values
        size_t    _initialFootprint; // memory footprint of original data payload
        Value     trueValue;
        Value     falseValue;
        Value     tmpValue;
        Value     tileValue;
        shared_ptr<ChunkIterator> emptyChunkIterator;
        RLEPayload payload;
        Chunk* bitmapChunk;
        RLEPayload::append_iterator appender;
        position_t prevPos;
        size_t    _sizeLimit;

        /**
         * Exception-safety control flag. This is checked by in the destructor of RLEChunkIterator,
         * to make sure unPin() is called upon destruction, unless flush() already executed.
         */
        bool _needsFlush;
    };

    /**
     * Abstract base chunk iterator providing functionality which keeps track of the iterator logical position within a chunk.
     * @note It uses an RLE empty bitmap extracted from a different materialized chunk (aka the empty bitmap chunk)
     */
    class BaseTileChunkIterator: public ConstChunkIterator, protected CoordinatesMapper
    {
      protected:
        ArrayDesc const& _array;
        AttributeID      _attrID;
        AttributeDesc const& _attr;
        Chunk* _dataChunk;
        bool   _hasCurrent;
        bool   _hasOverlap;
        int    _mode;
        boost::shared_ptr<ConstRLEEmptyBitmap> _emptyBitmap;
        ConstRLEEmptyBitmap::iterator _emptyBitmapIterator;
        Coordinates _currPos;
        boost::weak_ptr<Query> _query;

        BaseTileChunkIterator(ArrayDesc const& desc,
                              AttributeID attr,
                              Chunk* data,
                              int iterationMode,
                              boost::shared_ptr<Query> const& query);

        virtual ~BaseTileChunkIterator();

      public:
        int  getMode();
        bool isEmpty();
        bool end();
        virtual position_t getLogicalPosition();
      protected:
        virtual bool setPosition(position_t pos);
        bool setPosition(Coordinates const& pos);
        void operator ++();
        void reset();
        Coordinates const& getPosition();
    public:
        ConstChunk const& getChunk();
        boost::shared_ptr<Query> getQuery()
        {
            return _query.lock();
        }
    };

    class BaseTile;

    /**
     * Concrete chunk iterator class providing a tile-at-a-time access to chunk's data via getData()
     * as well as a value-at-a-time access via getItem().
     */
    class RLETileConstChunkIterator : public BaseTileChunkIterator
    {
    public:
        RLETileConstChunkIterator(ArrayDesc const& desc,
                                  AttributeID attr,
                                  Chunk* data,
                                  Chunk* bitmap,
                                  int iterationMode,
                                  boost::shared_ptr<Query> const& query);
        ~RLETileConstChunkIterator();
        Value& getItem();
        Coordinates const& getPosition();
        bool setPosition(Coordinates const& pos);
        virtual bool setPosition(position_t pos);
        void operator ++();
        void reset();

        /// @see ConstChunkIterator
        virtual const Coordinates& getData(scidb::Coordinates& logicalStart /*IN/OUT*/,
                                           size_t maxValues,
                                           boost::shared_ptr<BaseTile>& tileData,
                                           boost::shared_ptr<BaseTile>& tileCoords);

        /// @see ConstChunkIterator
        virtual position_t getData(position_t logicalStart,
                           size_t maxValues,
                           boost::shared_ptr<BaseTile>& tileData,
                           boost::shared_ptr<BaseTile>& tileCoords);

        /// @see ConstChunkIterator
        virtual const Coordinates& getData(scidb::Coordinates& logicalStart /*IN/OUT*/,
                                           size_t maxValues,
                                           boost::shared_ptr<BaseTile>& tileData);

        /// @see ConstChunkIterator
        virtual position_t getData(position_t logicalStart,
                                   size_t maxValues,
                                   boost::shared_ptr<BaseTile>& tileData);

        /// @see ConstChunkIterator
        operator const CoordinatesMapper* () const { return this; }
    private:
        /// Helper to pin the chunk and construct a payload iterator
        void prepare();
        void unPrepare();

        /// Helper to make sure that the payload iterator corresponds to the EBM iterator
        void alignIterators();

        /**
         * Return a tile of at most maxValues starting at logicalStart.
         * The logical position is advanced by the size of the returned tile.
         * @parm logicalStart - logical position (in row-major order) within a chunk of the first data element
         * @param maxValues   - max number of values in a tile
         * @param tileData    - output data tile
         * @param tileCoords  - output tile of logical position_t's, one for each element in the data tile
         * @param coordTileType - "scidb::Coordinates"
         * @param coordCtx      - any context necessary for constructing the tile of coordinates
         * @return positon_t(-1) if no data is found at the logicalStart position
         *         (either at the end of chunk or at a logical "whole" in the serialized data);
         *         otherwise, the next position where data exist in row-major order.
         *         If positon_t(-1) is returned, the output variables are not modified.
         */
        position_t
        getDataInternal(position_t logicalStart,
                        size_t maxValues,
                        boost::shared_ptr<BaseTile>& tileData,
                        boost::shared_ptr<BaseTile>& tileCoords,
                        const scidb::TypeId& coordTileType,
                        const BaseTile::Context* coordCtx);
        /**
         * Return a tile of at most maxValues starting at logicalStart.
         * The logical position is advanced by the size of the returned tile.
         * @parm logicalStart - logical position (in row-major order) within a chunk of the first data element
         * @param maxValues   - max number of values in a tile
         * @param tileData    - output data tile
         * @return positon_t(-1) if no data is found at the logicalStart position
         *         (either at the end of chunk or at a logical "whole" in the serialized data);
         *         otherwise, the next position where data exist in row-major order.
         *         If positon_t(-1) is returned, the output variables are not modified.
         */
        position_t
        getDataInternal(position_t logicalOffset,
                        size_t maxValues,
                        boost::shared_ptr<BaseTile>& tileData);

        class CoordinatesMapperWrapper : public CoordinatesMapperProvider
        {
        private:
            CoordinatesMapper* _mapper;
        public:
            CoordinatesMapperWrapper(CoordinatesMapper* mapper) : _mapper(mapper)
            { assert(_mapper); }
            virtual ~CoordinatesMapperWrapper() {}
            virtual operator const CoordinatesMapper* () const { return _mapper; }
        };

        class RLEPayloadDesc : public rle::RLEPayloadProvider
        {
        public:
            RLEPayloadDesc(ConstRLEPayload* rlePayload, position_t offset, size_t numElem);
            virtual const ConstRLEPayload* getPayload() const  { return _rlePayload; }
            virtual position_t getOffset() const { return _offset; }
            virtual size_t getNumElements() const { return _numElem; }
        private:
            const ConstRLEPayload* _rlePayload;
            const position_t _offset;
            const size_t _numElem;
        };

    private:
        /// data chunk RLE payload
        ConstRLEPayload _payload;
        ConstRLEPayload::iterator _payloadIterator;

        /// Current logical positon within a chunk,
        /// not needed as long as EBM is never unpinned/not in memory
        position_t _lPosition;

        /// cached singleton pointer
        TileFactory* _tileFactory;
        //// The data is non-bool, fixed size, and ebm is aligned with data
        mutable bool _fastTileInitialize;
        /// Whether the payload is always pinned
        bool  _isDataChunkPinned;
    protected:
        Value _value;
    };

} // scidb namespace
#endif // MEM_CHUNK_H_
