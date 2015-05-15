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

#ifndef MEM_ARRAY_H_
#define MEM_ARRAY_H_
#include <array/MemChunk.h>
#ifndef SCIDB_CLIENT
#include <array/Array.h>
#include <vector>
#include <map>
#include <assert.h>
#include <boost/shared_ptr.hpp>
#include <boost/weak_ptr.hpp>
#include <boost/shared_array.hpp>
#include <query/Query.h>
#include <util/FileIO.h>
#include <util/Lru.h>
#include <util/CoordinatesMapper.h>
#include <array/Tile.h>
#include <util/DataStore.h>

using namespace std;
using namespace boost;

namespace scidb
{
    /**
     * Structure to share mem chunks.
     */
    class SharedMemCache
    {
    private:
        // The LRU of LruMemChunk objects.
        MemChunkLru _theLru;

        uint64_t _usedMemSize;
        uint64_t _usedMemThreshold;
        Mutex _mutex;
        size_t _swapNum;
        size_t _loadsNum;
        size_t _dropsNum;
        uint64_t _genCount;
        DataStores _datastores;
        static SharedMemCache _sharedMemCache;

    public:
        SharedMemCache();
        void pinChunk(LruMemChunk& chunk);
        void unpinChunk(LruMemChunk& chunk);
        void swapOut();
        void deleteChunk(LruMemChunk& chunk);
        void cleanupArray(MemArray &array);
        static SharedMemCache& getInstance() {
            return _sharedMemCache;
        }

        /**
         * Get the LRU.
         * @return a reference to the LRU object.
         */
        static MemChunkLru& getLru() {
            return _sharedMemCache._theLru;
        }

        uint64_t getUsedMemSize() const {
            return _sharedMemCache._usedMemSize;
        }

        size_t getSwapNum() const {
            return _swapNum;
        }

        size_t getLoadsNum() const {
            return _loadsNum;
        }

        size_t getDropsNum() const {
            return _dropsNum;
        }

        /**
         * Initialize the datastores used for the temporary disk storage needed
         * by mem arrays.
         * @param memThreshold size of the in-memory cache
         * @param basePath directory where datastores for spilled data will live
         */
        void initSharedMemCache(uint64_t memThreshold, const char* basePath);

        /**
         * Update the memory threshold.
         */
        void setMemThreshold(uint64_t memThreshold)
        {
            _usedMemThreshold = memThreshold;
        }

        /**
         * Retrieve the current memory threhsold.
         * @return the mem threshold.
         */
        uint64_t getMemThreshold() const
        {
            return _usedMemThreshold;
        }

        /**
         * Debugging aid: compute the size of the chunks on the LRU list.
         * @return the sum of the sizes of the chunks in the LRU. Note: chunks that are currently pinned are not accounted for here.
         */
        uint64_t computeSizeOfLRU();

        /**
         * Debugging aid: compare computeSizeOfLRU with getUsedMemSize
         * @return true if computeSizeOfLRU is <= getUsedMemSize. This is an invariant that must always be true. False otherwise.
         */
        bool sizeCoherent();

    };

    /**
     * Temporary (in-memory) array implementation
     */
    class MemArray : public Array
    {
        friend class MemChunk;
        friend class LruMemChunk;
        friend class MemArrayIterator;
        friend class SharedMemCache;
      public:
        //
        // Sparse chunk iterator
        //
        virtual string const& getName() const;
        virtual ArrayID getHandle() const;

        virtual ArrayDesc const& getArrayDesc() const;

        virtual boost::shared_ptr<ArrayIterator> getIterator(AttributeID attId);
        virtual boost::shared_ptr<ConstArrayIterator> getConstIterator(AttributeID attId) const;

        MemArray(ArrayDesc const& arr, boost::shared_ptr<Query> const& query);

        /**
         * Construct by first creating an empty MemArray with the shape of input,
         * then append all data from input to this.
         * @param input the input array used for the array descriptor and the data
         * @param vertical the method to use when copying data. True by default,
         * meaning copy each attribute separately - first all chunks of attribute 1,
         * then all chunks of attribute 2 and so on... False means copy all attributes
         * at the same time - first all attributes for the first chunk, then all
         * attributes for the second chunk. The value false must be used when input
         * array does not support the independent scanning of attributes
         * (i.e. MergeSortArray).
         */
        MemArray(boost::shared_ptr<Array>& input, boost::shared_ptr<Query> const& query, bool vertical = true);
        ~MemArray();

        /**
         * @see Array::isMaterialized()
         */
        virtual bool isMaterialized() const
        {
            return true;
        }
      protected:
        ArrayDesc desc;
      private:
        Chunk& operator[](Address const& addr);
        void initLRU();
        void swapOut();
        void pinChunk(LruMemChunk& chunk);
        void unpinChunk(LruMemChunk& chunk);
        shared_ptr<DataStore> _datastore;
        map<Address, LruMemChunk> _chunks;
        Mutex _mutex;
    private:
        MemArray(const MemArray&);
    };

    /**
     * Temporary (in-memory) array iterator
     */
    class MemArrayIterator : public ArrayIterator
    {
      private:
        map<Address, LruMemChunk>::iterator curr;
        map<Address, LruMemChunk>::iterator last;
        MemArray& _array;
        Address addr;
        Chunk* currChunk;
        boost::shared_ptr<Array> parent;
        bool positioned;

        void position();

      public:
        void setParentArray(boost::shared_ptr<Array> arr) {
            parent = arr;
        }
        MemArrayIterator(MemArray& arr, AttributeID attId);
        ConstChunk const& getChunk();
        bool end();
        void operator ++();
        Coordinates const& getPosition();
        bool setPosition(Coordinates const& pos);
        void setCurrent();
        void reset();
        Chunk& newChunk(Coordinates const& pos);
        Chunk& newChunk(Coordinates const& pos, int compressionMethod);
        void deleteChunk(Chunk& chunk);
        virtual boost::shared_ptr<Query> getQuery()
        {
            return Query::getValidQueryPtr(_array._query);
        }
    };

} // scidb namespace
#endif
#endif
