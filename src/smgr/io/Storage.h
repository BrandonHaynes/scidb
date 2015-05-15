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
 * Storage.h
 *
 *  Created on: 06.01.2010
 *      Author: knizhnik@garret.ru
 *      Description: Storage manager interface
 */

#ifndef STORAGE_H_
#define STORAGE_H_

#include <stdlib.h>
#include <exception>
#include <limits>
#include <string>
#include <map>
#include <system/Cluster.h>
#include <array/MemArray.h>
#include <array/Compressor.h>
#include <query/Query.h>

namespace scidb
{
    /**
     * An extension of Address that specifies the chunk of a persistent array.
     *
     * Note: we do not use virtual methods here so there is no polymorphism.
     * We do not need virtual methods on these structures, so we opt for better performance.
     * Inheritance is only used to factor out similarity between two structures.
     *
     * Storage addresses have an interesting ordering scheme. They are ordered by
     * AttributeID, Coordinates, ArrayID (reverse).
     *
     * Internally the storage manager keeps all chunks for a given array name in the same subtree.
     * For a given array, you will see this kind of ordering:
     *
     * AttributeID = 0
     *   Coordinates = {0,0}
     *     ArrayID = 1 --> CHUNK (this chunk exists in all versions >= 1)
     *     ArrayID = 0 --> CHUNK (this chunk exists only in version 0)
     *   Coordinates = {0,10}
     *     ArrayID = 2 --> NULL (tombstone)
     *     ArrayID = 0 --> CHUNK (this chunk exists only in versions 0 and 1; there's a tombstone at 2)
     * AttributeID = 1
     *   ...
     *
     * The key methods that implement iteration over an array are findChunk and findNextChunk.
     * For those methods, the address with zero-sized-list coordinates is considered to be the start of
     * the array. In practice, to find the first chunk in the array - create an address with coordinates
     * {} and then find the first chunk greater than it. This is why our comparison function cares about
     * the size of the coordinate list.
     */
    struct StorageAddress: public Address
    {
        /**
         * Array Identifier for the Versioned Array ID wherein this chunk first appeared.
         */
        ArrayID arrId;

        /**
         * Default constructor
         */
        StorageAddress():
            Address(0, Coordinates()),
            arrId(0)
        {}

        /**
         * Constructor
         * @param arrId array identifier
         * @param attId attribute identifier
         * @param coords element coordinates
         */
        StorageAddress(ArrayID arrId, AttributeID attId, Coordinates const& coords):
            Address(attId, coords), arrId(arrId)
        {}

        /**
         * Copy constructor
         * @param addr the object to copy from
         */
        StorageAddress(StorageAddress const& addr):
            Address(addr.attId, addr.coords), arrId(addr.arrId)
        {}

        /**
         * Partial comparison function, used to implement std::map
         * @param other another aorgument of comparison
         * @return true if "this" preceeds "other" in partial order
         */
        inline bool operator < (StorageAddress const& other) const
        {
            if(attId != other.attId)
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
            if (arrId != other.arrId)
            {
                //note: reverse ordering to keep most-recent versions at the front of the map
                return arrId > other.arrId;
            }
            return false;
        }

        /**
         * Equality comparison
         * @param other another aorgument of comparison
         * @return true if "this" equals to "other"
         */
        inline bool operator == (StorageAddress const& other) const
        {
            if (arrId != other.arrId)
            {
                return false;
            }

            return Address::operator ==( static_cast<Address const&>(other));
        }

        /**
         * Inequality comparison
         * @param other another argument of comparison
         * @return true if "this" not equals to "other"
         */
        inline bool operator != (StorageAddress const& other) const
        {
            return !(*this == other);
        }

        /**
         * Check for same base Addr
         * @param other another argument of comparison
         * @return true if "this" is equal to "other" notwithstanding
         *         different arrId (version)
         */
        inline bool sameBaseAddr(StorageAddress const& other) const
        {
            if(attId != other.attId)
            {
                return false;
            }
            return (coords == other.coords);
        }
    };

    class ListChunkDescriptorsArrayBuilder;
    class ListChunkMapArrayBuilder;
    class PersistentChunk;
    class DataStores;

    /**
     * Storage manager interface
     */
    class Storage
    {
      public:
        virtual ~Storage() {}
        /**
         * Open storage manager at specified URL.
         * Format and semantic of storage URL depends in particular implementation of storage manager.
         * For local storage URL specifies path to the description file.
         * Description file has the following format:
         * ----------------------
         * <storage-header-path>
         * <log size limit> <storage-log-path>
         * ...
         * ----------------------
         * @param url implementation dependent database url
         * @param cacheSize chunk cache size: amount of memory in bytes which can be used to cache most frequently used
         */
        virtual void open(const string& url, size_t cacheSize) = 0;

        /**
         * Get write iterator through array chunks available in the storage.
         * This iterator is used to create some new version K (=arrDesc.getId()).
         * It will only return new chunks that have been created in version K.
         * For chunks from versions (0...K-1) - use getConstArrayIterator. Furthermore, if
         * this is the first call to getArrayIterator for K, the caller must ensure that
         * a version of K exists in the system catalog first before calling this.
         * @param arrDesc array descriptor
         * @param attId attribute identifier
         * @param query in the context of which the iterator is requeted
         * @return shared pointer to the array iterator
         */
        virtual boost::shared_ptr<ArrayIterator> getArrayIterator(boost::shared_ptr<const Array>& arr,
                                                                  AttributeID attId,
                                                                  boost::shared_ptr<Query>& query) = 0;

        /**
         * Get const array iterator through array chunks available in the storage.
         * @param arrDesc array descriptor
         * @param attId attribute identifier
         * @param query in the context of which the iterator is requeted
         * @return shared pointer to the array iterator
         */
        virtual boost::shared_ptr<ConstArrayIterator> getConstArrayIterator(boost::shared_ptr<const Array>& arr,
                                                                            AttributeID attId,
                                                                            boost::shared_ptr<Query>& query) = 0;

        /**
         * Flush all changes to the physical device(s) for the indicated array.  (optionally flush data
         * for all arrays if uaId == INVALID_ARRAY_ID). If power fault or system failure happens when there
         * is some unflushed data, then these changes can be lost
         */
        virtual void flush(ArrayUAID uaId = INVALID_ARRAY_ID) = 0;

        /**
         * Close storage manager
         */
        virtual void close() = 0;

        /**
         * Set this instance identifier
         */
        virtual void setInstanceId(InstanceID id) = 0;

        /**
         * Get this instance identifier
         */
        virtual InstanceID getInstanceId() const = 0;

        /**
         * Remove all versions prior to lastLiveArrId from the storage. If
         * lastLiveArrId is 0, removes all versions. Does nothing if the
         * specified array is not present.
         * @param uaId the Unversioned Array ID
         * @param lastLiveArrId the Versioned Array ID of last version to preserve
         */
        virtual void removeVersions(QueryID queryId,
                                    ArrayUAID uaId,
                                    ArrayID lastLiveArrId) = 0;

        /**
         * Remove a version of a persistent array from the in-memory
         * chunk-map.  Called for arrays that have been cleaned-up via
         * "rollback"
         * @param uaId the Unversioned Array ID
         * @param arrId the ID of the versioned array that should be removed
         */
        virtual void removeVersionFromMemory(ArrayUAID uaId, ArrayID arrId) = 0;

        /**
         * Rollback uncompleted updates
         * @param map of updated array which has to be rollbacked
         */
        virtual void rollback(std::map<ArrayID,VersionID> const& undoUpdates) = 0;

        struct DiskInfo
        {
            uint64_t used;
            uint64_t available;
            uint64_t clusterSize;
            uint64_t nFreeClusters;
            uint64_t nSegments;
        };

        virtual void getDiskInfo(DiskInfo& info) = 0;

        virtual uint64_t getCurrentTimestamp() const = 0;

        virtual uint64_t getUsedMemSize() const = 0;

        /**
         * Method for creating a list of chunk descriptors. Implemented by LocalStorage.
         * @param builder a class that creates a list array
         */
        virtual void listChunkDescriptors(ListChunkDescriptorsArrayBuilder& builder)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "chunk header retrieval is not supported by this storage type.";
        }

        /**
         * Method for creating a list of chunk map elements. Implemented by LocalStorage.
         * @param builder a class that creates a list array
         */
        virtual void listChunkMap(ListChunkMapArrayBuilder& builder)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "chunk map retrieval is not supported by this storage type.";
        }

        /**
         * Decompress chunk from the specified buffer
         * @param chunk destination chunk to receive decompressed data
         * @param buf buffer containing compressed data.
         */
        virtual void decompressChunk(ArrayDesc const& desc, PersistentChunk* chunk, CompressedBuffer const& buf) = 0;

        /**
         * Compress chunk to the specified buffer
         * @param chunk chunk to be compressed
         * @param buf buffer where compressed data will be placed. It is inteded to be initialized using default constructore and will be filled by this method.
         */
        virtual void compressChunk(ArrayDesc const& desc, PersistentChunk const* chunk, CompressedBuffer& buf) = 0;

        /**
         * Pin chunk in memory: prevent cache replacement algorithm to throw away this chunk from memory
         * @param chunk chunk to be pinned in memory
         */
        virtual void pinChunk(PersistentChunk const* chunk) = 0;

        /**
         * Unpin chunk in memory: decrement access couter for this chunk
         * @param chunk chunk to be unpinned
         */
        virtual void unpinChunk(PersistentChunk const* chunk) = 0;

        /**
         * Write new chunk in the storage.
         * @param chunk new chunk created by newChunk Method
         * @param query performing the operation
         */
        virtual void writeChunk(ArrayDesc const& desc, PersistentChunk* chunk, boost::shared_ptr<Query> const& query) = 0;

        /**
         * Find and fetch a chunk from a particular array. Throws exception if chunk does not exist.
         * @param desc the array descriptor
         * @param addr the address of the chunk
         * @param query performing the operation
         * @return the pointer to the chunk.
         */
        virtual boost::shared_ptr<PersistentChunk> readChunk(ArrayDesc const& desc,
                                                             StorageAddress const& addr,
                                                             const boost::shared_ptr<Query>& query) = 0;

        /**
         * Load chunk body from the storage.
         * @oaran desc the array descriptor
         * @param chunk loaded chunk
         */
        virtual void loadChunk(ArrayDesc const& desc, PersistentChunk* chunk) = 0;

        /**
         * Indicate to the storage module that a chunk is no
         * longer in use and the resources it requires can be freed
         * @param chunk to free
         */
        virtual void freeChunk(PersistentChunk* chunk) = 0;

        /**
         * Get latch for the specified chunk
         * @param chunk chunk to be locked
         */
        virtual RWLock& getChunkLatch(PersistentChunk* chunk) = 0;

        /**
         * Create new chunk in the storage. There should be no chunk with such address in the storage.
         * @param desc the array descriptor
         * @param addr chunk address
         * @param compressionMethod compression method for this chunk
         * @param query performing the operation
         * @throw SystemException if the chunk with such address already exists
         */
        virtual boost::shared_ptr<PersistentChunk> createChunk(ArrayDesc const& desc,
                                                               StorageAddress const& addr,
                                                               int compressionMethod,
                                                               const boost::shared_ptr<Query>& query) = 0;
        /**
         * Delete chunk
         * @param chunk chunk to be deleted
         */
        virtual void deleteChunk(ArrayDesc const& desc, PersistentChunk& chunk) = 0;

        virtual size_t getNumberOfInstances() const = 0;

        /**
         * Given an array descriptor and an address of a chunk - compute the InstanceID of the primary instance
         * that shall be responsible for this chunk. Currently this uses our primitive round-robin hashing
         * for all cases except replication. To be improved.
         * Note the same replication scheme is used for both - regular chunks and tombstones.
         * @param desc the array descriptor
         * @param address the address of the chunk (or tombstone entry)
         * @return the instance id responsible for this datum
         */
        virtual InstanceID getPrimaryInstanceId(ArrayDesc const& desc, StorageAddress const& address) const =0;

        /**
         * Get a list of the chunk positions for a particular persistent array. If the array is not found, no fields
         * shall be added to the chunks argument.
         * @param[in] desc the array descriptor. Must be for a persistent stored array with proper identifiers.
         * @param[in] query the query context.
         * @param[out] chunks the set of coordinates to which the chunk positions of the array shall be appended.
         */
        virtual void getChunkPositions(ArrayDesc const& desc, boost::shared_ptr<Query> const& query, CoordinateSet& chunks) = 0;

         /**
          * Given an array descriptor and a storage address for a chunk - find the storage address in the next chunk along the same attribute
          * in stride major order. The Array UAID and ID is taken from desc. The current coordinates and Attribute ID are taken from address.
          * The address whose coordinates are a zero-sized are considered to be the end of the array. If address.coords has size zero, then
          * the method shall attempt to find the first chunk for this array. Similarly if there is no next chunk, the method shall set
          * address.coords to a zero-sized list. Otherwise, the method shall set address.arrId and address.coords to the correct values
          * by which the next chunk can be retrieved.
          * @param desc the array descriptor for the desired array.
          * @param query the query context
          * @param address the address of the previous chunk
          * @return true if the chunk was found, false otherwise
          */
         virtual bool findNextChunk(ArrayDesc const& desc, boost::shared_ptr<Query> const& query, StorageAddress& address) =0;

         /**
          * Given and array descriptor and a desired storage address for the chunk, determine if there is a chunk at
          * address.attId, address.coords. If this version of this array (as described by desc) contains this chunk -
          * then set address.arrId to the proper value. Otherwise, set address.coords to a zero-sized list.
          * @param desc the array descriptor for the desired array
          * @param query the query context
          * @param address the address of the desired chunk
          * @return true if the chunk was found, false otherwise
          */
         virtual bool findChunk(ArrayDesc const& desc, boost::shared_ptr<Query> const& query, StorageAddress& address) =0;

         /**
          * Remove a previously existing chunk from existence in the given version on this instance.
          * This function alters the local storage only. This instance may or may not be responsible for this chunk and the caller bears
          * the responsibility of determining that. Note this method removes all attributes at once.
          * @param arrayDesc the array descriptor. The id field is used for version purposes.
          * @param coords the coordinates of the removed chunk
          * @param query the query context
          */
         virtual void removeLocalChunkVersion(ArrayDesc const& arrayDesc,
                                              Coordinates const& coord,
                                              boost::shared_ptr<Query> const& query) =0;

         /**
          * Remove a previously existing chunk from existence in the given version in the system.
          * Effectively this function sends the proper replica messages and then calls removeLocalChunkVersion on this instance.
          * Note this method removes all attributes at once.
          * @param arrayDesc the array descriptor
          * @param coords the coordinates of the tombstone
          * @param query the query context
          */
         virtual void removeChunkVersion(ArrayDesc const& arrayDesc,
                                         Coordinates const& coords,
                                         boost::shared_ptr<Query> const& query) =0;

         /**
          * Given an array descriptor desc and the coordinate set liveChunks - remove the chunk version for every
          * chunk that is in the array and NOT in liveChunks. This is used by overriding-storing ops to ensure that
          * new versions of arrays do not contain chunks from older versions unless explicitly added.
          * @param arrayDesc the array descriptor
          * @param liveChunks the set of chunks that should NOT be tombstoned
          * @param query the query context
          */
         virtual void removeDeadChunks(ArrayDesc const& arrayDesc,
                                       set<Coordinates, CoordinatesLess> const& liveChunks,
                                       boost::shared_ptr<Query> const& query) = 0;

         /**
          * Return DataStores object used by storage manager to store data
          */
         virtual DataStores& getDataStores() = 0;

    };

    /**
     * Storage factory.
     * By default it points to local storage manager implementation.
     * But it is possible to register any storae manager implementation using setInstance method
     */
    class StorageManager
    {
      public:
        /**
         * Set custom implementaiton of storage manager
         */
        static void setInstance(Storage& storage) {
            instance = &storage;
        }
        /**
         * Get instance of the storage (it is assumed that there can be only one storage in the application)
         */
        static Storage& getInstance() {
            return *instance;
        }
      private:
        static Storage* instance;
    };
}

#endif
