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
 * PersistentChunk.h
 *
 *  Created on: 10.23.2014
 *      Author: sfridella@paradigm4.com
 *      Description: Chunk which can be read to/from persistent storage
 */

#ifndef PERSISTENT_CHUNK_H_
#define PERSISTENT_CHUNK_H_

#include <util/DataStore.h>
#include <array/Metadata.h>
#include <smgr/io/Storage.h>

namespace scidb
{
    /**
     * If you are changing the format of the first three fields of the StorageHeader class (very rare), then you MUST change this number.
     * Illegal values are values that are very likely to occur in a corrupted file by accident, like:
     * 0x00000000
     * 0xFFFFFFFF
     *
     * Or values that have been used in the past:
     * 0xDDDDBBBB
     * 0x5C1DB123
     *
     * You must pick a value that is not equal to any of the values above - AND add it to the list.
     * Picking a new magic has the effect of storage file not being transferrable between scidb versions with different magic values.
     */
    const uint32_t SCIDB_STORAGE_HEADER_MAGIC = 0x5C1DB123;

    /**
     * If you are changing the format of the StorageHeader class (other than the first 3 fields), or any other structures that are saved to disk,
     * like ChunkHeader - you must increment this number.
     * When storage format versions are different, it is up to the scidb code to determine if an upgrade is possible. At the moment of this writing,
     * scidb with storage version X simply will refuse to read the metadata file created by storage version Y. Future behavior may be a lot more
     * sophisticated.
     *
     * Revision history:
     *
     * SCIDB_STORAGE_FORMAT_VERSION = 8:
     *    Author: Dave Gosselin
     *    Date: 8/21/14
     *    Ticket: 3672
     *    Note: Removing the dependency on sparsity and RLE checks means that uncompressed chunks will
     *          always be stored in RLE format.
     *
     * SCIDB_STORAGE_FORMAT_VERSION = 7:
     *    Author: Steve F.
     *    Date: 7/11/14
     *    Ticket: 3719
     *    Note: Now store storage version and datastore id in the tombstone chunk header
     *
     * SCIDB_STORAGE_FORMAT_VERSION = 6:
     *    Author: Steve F.
     *    Date: TBD
     *    Ticket: TBD
     *    Note: Changed data file format to use power-of-two allocation size with buddy blocks.
     *          Also, split data file into multiple files, one per array.
     *
     * SCIDB_STORAGE_FORMAT_VERSION = 5:
     *    Author: tigor
     *    Date: 10/31/2013
     *    Ticket: 3404
     *    Note: Removal of PersistentChunk clones
     *
     * SCIDB_STORAGE_FORMAT_VERSION = 4:
     *    Author: Alex P.
     *    Date: 5/28/2013
     *    Ticket: 2253
     *    Note: As a result of a long discussion, revamped and tested this behavior. Added min and max version to the storage header.
     *          Added a version number to each chunk header - to allow for future upgrade flexibility.
     *
     * SCIDB_STORAGE_FORMAT_VERSION = 3:
     *    Author: ??
     *    Date: ??
     *    Ticket: ??
     *    Note: Initial implementation dating back some time
     */
    const uint32_t SCIDB_STORAGE_FORMAT_VERSION = 8;

    /**
     * The beginning section of the storage header file.
     */
    struct StorageHeader
    {
        /**
         * A constant special value the header file must begin with.
         * If it's not equal to SCIDB_STORAGE_HEADER_MAGIC, then we know for sure the file is corrupted.
         */
        uint32_t magic;

        /**
         * The smallest version number among all the chunks that are currently stored.
         * Currently it's always equal to versionUpperBound; this is a placeholder for the future.
         */
        uint32_t versionLowerBound;

        /**
         * The largest version number among all the chunks that are currently stored.
         * Currently it's always equal to versionLowerBound; this is a placeholder for the future.
         */
        uint32_t versionUpperBound;

        /**
         * Current position in storage header (offset to where new chunk header will be written).
         */
        uint64_t currPos;

        /**
         * Number of chunks in local storage.
         */
        uint64_t nChunks;

        /**
         * This instance ID.
         */
        InstanceID   instanceId;
    };

    const size_t HEADER_SIZE = 4*KiB;  // align header on page boundary to allow aligned IO operations
    const size_t N_LATCHES = 101;      // XXX TODO: figure out if latching is still necessary after removing clone logic

    /**
     * Position of chunk in the storage
     */
    struct DiskPos
    {
        /**
         * Data store guid
         */
        DataStore::Guid dsGuid;

        /**
         * Position of chunk header in meta-data file
         */
        uint64_t hdrPos;

        /**
         * Offset of chunk within DataStore
         */
        uint64_t offs;

        bool operator < (DiskPos const& other) const {
            return (dsGuid != other.dsGuid)
                ? dsGuid < other.dsGuid
                : offs < other.offs;
        }

        DiskPos()
            : dsGuid(0),
              hdrPos(0),
              offs(0) {}

        std::string toString() const
        {
            std::stringstream ss;
            ss << "DiskPos:"
               << " dsGuid=" << dsGuid
               << ", (header off) hdrPos=" << hdrPos
               << ", (chunk off) offs=" << offs;
            return ss.str();
        }
    };

    /**
     * Chunk header as it is stored on the disk
     */
    struct ChunkHeader
    {
        /**
         * The version of the storage manager that produced this chunk.
         * Currently, this is always equal to SCIDB_STORAGE_VERSION.
         * Placeholder for the future.
         */
        uint32_t storageVersion;

        /**
         * The position of the chunk on disk
         */
        DiskPos  pos;

        /**
         * Versioned Array ID that contains this chunk.
         */
        ArrayID  arrId;

        /**
         * The Attribute ID the chunk belongs to.
         */
        AttributeID attId;

        /**
         * Size of the data after it has been compressed.
         */
        uint64_t compressedSize;

        /**
         * Size of the data prior to any compression.
         */
        uint64_t size;

        /**
         * The compression method used on this chunk.
         */
        int8_t   compressionMethod;

        /**
         * The special properties of this chunk.
         * @see enum Flags below
         */
        uint8_t  flags;

        /**
         * Number of coordinates the chunk has.
         * XXX: Somebody explain why this is stored per chunk? Seems wasteful.
         */
        uint16_t nCoordinates;

        /**
         * Actual size on disk: compressedSize + reserve.
         */
        uint64_t allocatedSize;

        /**
         * Number of non-empty cells in the chunk.
         */
        uint32_t nElems;

        /**
         * The instance ID this chunk must occupy;
         * not equal to current instance id if this is a replica.
         */
        uint32_t instanceId;

        enum Flags {
            DELTA_CHUNK = 2,
            TOMBSTONE = 8
        };

        /**
         * Check if a given flag is set.
         * Usage:
         * ChunkHeader hdr; bool isTombstone = hdr.is<TOMBSTONE>();
         * @return true if the template argument flag is set, false otherwise.
         */
        template<Flags FLAG>
        inline bool is() const
        {
            return flags & FLAG;
        }

        /**
         * Set one of the flags in the chunk header.
         * Usage:
         * ChunkHeader tombHdr; tombHdr.set<TOMBSTONE>(true);
         * @param[in] the value to set the flag to
         */
        template<Flags FLAG>
        inline void set(bool value)
        {
            if(value)
            {
                flags |= FLAG;
            }
            else
            {
                flags &= ~(FLAG);
            }
        }

        std::string toString() const
        {
            std::stringstream ss;
            ss << "ChunkHeader ["
               << " position=" << pos.toString()
               << ", arrId=" << arrId
               << ", attId=" << attId
               << ", instanceId=" << instanceId << ']';
            return ss.str();
        }

        ChunkHeader()
            : storageVersion(0),
              pos(),
              arrId(0),
              attId(0),
              compressedSize(0),
              size(0),
              compressionMethod(0),
              flags(0),
              nCoordinates(0),
              allocatedSize(0),
              nElems(0),
              instanceId(0) {}
    };

    inline ostream& operator<<(ostream& stream, ChunkHeader const& hdr)
    {
        stream << hdr.toString();
        return stream;
    }

    /**
     * Chunk header + coordinates
     */
    struct ChunkDescriptor
    {
        ChunkHeader hdr;
        Coordinate  coords[MAX_NUM_DIMS_SUPPORTED];

        void getAddress(StorageAddress& addr) const;
        std::string toString() const
        {
            std::stringstream ss;
            ss << "ChunkDesc ["
               << " hdr= " << hdr.toString()
               << ", coords=[ ";
            for (uint16_t i=0; (i < hdr.nCoordinates) && (i < MAX_NUM_DIMS_SUPPORTED); ++i) {
                ss << coords[i] << " ";
            }
            ss << "] ]";
            return ss.str();
        }

        ChunkDescriptor() {
            memset (coords, 0, sizeof(coords));
        }
    };

    /**
     * PersistentChunk is a container for a SciDB array chunk stored on disk.
     * PersistentChunk is an internal interface and should not be usable/visible
     * via the Array/Chunk/Iterator APIs.
     */
    class PersistentChunk : public boost::enable_shared_from_this<PersistentChunk>
    {
        friend class CachedStorage;
        friend class ListChunkMapArrayBuilder;

      public:

        /**
         * UnPinner/Pinner are helpers which manage pinning/unpinning
         * of a PersistentChunk within a scope
         */
        class UnPinner {
        protected:
            PersistentChunk* _pchunk;

        public:
            UnPinner(PersistentChunk* chunk) :
                _pchunk(chunk)
                {}
            ~UnPinner()
                {
                    if (_pchunk)
                    {
                        _pchunk->unPin();
                    }
                }
            void set(PersistentChunk* chunk)
                {
                    assert(!_pchunk);
                    _pchunk = chunk;
                }
            PersistentChunk* get()
                {
                    return _pchunk;
                }
        };

        class Pinner : public UnPinner {
        public:
            Pinner(PersistentChunk* chunk) :
                UnPinner(chunk)
            {
                assert(_pchunk);
                _pchunk->pin();
            }
        };

      private:
        PersistentChunk* _next; // L2-list to implement LRU
        PersistentChunk* _prev;
        StorageAddress _addr; // StorageAddress of first chunk element
        void*   _data; // uncompressed data (may be NULL if swapped out)
        ChunkHeader _hdr; // chunk header
        int     _accessCount; // number of active chunk accessors
        bool    _raw; // true if chunk is currently initialized or loaded from the disk
        bool    _waiting; // true if some thread is waiting completetion of chunk load from the disk
        uint64_t _timestamp;
        Coordinates _firstPosWithOverlaps;
        Coordinates _lastPos;
        Coordinates _lastPosWithOverlaps;
        Storage* _storage;

        void init();
        void calculateBoundaries(const ArrayDesc& ad);

        // -----------------------------------------
        // L2-List methods
        //
        bool isEmpty();
        void prune();
        void link(PersistentChunk* elem);
        void unlink();
        // -----------------------------------------
        void beginAccess();

      public:

        int getAccessCount() const { return _accessCount; }
        void setAddress(const ArrayDesc& ad, const ChunkDescriptor& desc);
        void setAddress(const ArrayDesc& ad, const StorageAddress& firstElem, int compressionMethod);

        RWLock& getLatch();

        ConstChunk const* getPersistentChunk() const;

        bool isDelta() const;

        size_t count() const;
        bool   isCountKnown() const;
        void   setCount(size_t count);

        int getCompressionMethod() const;
        void setCompressionMethod(int method);
        void* getDataForLoad();
        void* getData(const scidb::ArrayDesc&);
        size_t getSize() const;
        void allocate(size_t size);
        void reallocate(size_t size);
        void free();
        Coordinates const& getFirstPosition(bool withOverlap) const;
        Coordinates const& getLastPosition(bool withOverlap) const;
        bool pin() const;
        void unPin() const;

        virtual void truncate(Coordinate lastCoord);

        const StorageAddress& getAddress() const
        {
            return _addr;
        }

        const ChunkHeader& getHeader() const
        {
            return _hdr;
        }

        uint64_t getTimestamp() const
        {
            return _timestamp;
        }

        size_t getCompressedSize() const
        {
            return _hdr.compressedSize;
        }

        void setCompressedSize(size_t size)
        {
            _hdr.compressedSize = size;
        }

        bool isRaw() const
        {
            return _raw;
        }

        void setRaw(bool status)
        {
            _raw = status;
        }

        PersistentChunk();
        ~PersistentChunk();
    };
}

#endif
