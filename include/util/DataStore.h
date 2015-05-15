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
 * DataStore.h
 *
 *  Created on: 10/16/13
 *      Author: sfridella@pardigm4.com
 *      Description: Storage file interface
 */

#ifndef DATASTORE_H_
#define DATASTORE_H_

#include <dirent.h>
#include <map>
#include <set>
#include <util/FileIO.h>
#include <util/Mutex.h>
#include <boost/scoped_array.hpp>

namespace scidb
{

class DataStores;
class DataStoreFlusher;
class ListDataStoresArrayBuilder;

/**
 * @brief   Class which manages on-disk storage for an array.
 *
 * @details DataStore manages the on-disk storage for an array.
 *          To write data into a datastore, space must first be
 *          allocated using the allocateSpace() interface.  The
 *          resulting chunk (offset, len) can then be safely written
 *          to using writeData().  readData() is used to read data
 *          from the data store.  To ensure data and metadata
 *          associated with the datastore is stable on disk,
 *          flush must be called.
 */
class DataStore
{
public:

    typedef uint64_t Guid;

    /**
     * Find space for the chunk of indicated size in the DataStore.
     * @param requestedSize minimum required size
     * @param allocatedSize actual allocated size
     * @throws SystemException on error
     */
    off_t allocateSpace(size_t requestedSize, size_t& allocatedSize);

    /**
     * Write bytes to the DataStore, to a location that is already
     * allocated
     * @param off Location to write, must be allocated
     * @param buffer Data to write
     * @param len Number of bytes to write
     * @param allocatedSize Size of allocated region
     * @throws SystemException on error
     */
    void writeData(off_t off,
                   void const* buffer,
                   size_t len,
                   size_t allocatedSize);

    /**
     * Read a chunk from the DataStore
     * @param off Location of chunk to read
     * @param buffer Place to put data read
     * @param len Size of chunk to read
     * @throws SystemException on error
     */
    void readData(off_t off, void* buffer, size_t len);

    /**
     * Flush dirty data and metadata for the DataStore
     * @throws SystemException on error
     */
    void flush();

    /**
     * Mark chunk as free both in the free lists and on
     * disk
     * @param off Location of chunk to free
     * @param allocated Allocated size of chunk in file
     * @throws SystemException on error
     */
    void freeChunk(off_t off, size_t allocated);

    /**
     * Return the size of the data store
     * @param filesize Out param size of the store in bytes
     * @param blocks Out param # of 512b blocks used by store
     * @param reservedbytes Out param # of bytes reserved for use in store
     * @param freebytes Out param # of bytes free in store
     */
    void getSizes(off_t& filesize,
                  blkcnt_t& fileblocks,
                  off_t& reservedbytes,
                  off_t& freebytes) const;

    /**
     * Return the guid
     */
    Guid getGuid() const
        { return _guid; }

    /**
     * Destroy a DataStore object
     */
    ~DataStore();

    /**
     * Construct a new DataStore object
     */
    DataStore(char const* filename, Guid guid, DataStores& parent);

    /**
     * Accsessor: return the guid
     * @return the guid for this datastore
     */
    Guid getGuid()
        { return _guid; }

    /**
     * Return the amount of overhead used by DataStore per chunk
     */
    size_t getOverhead()
        { return sizeof(DiskChunkHeader); }

    /**
     * Verify the integrity of the free list and throw exception
     * if there is a problem
     */
    void verifyFreelist();

private:
    friend class DataStores;

    /* Round up size_t value to next power of two
     */
    static size_t roundUpPowerOf2(size_t size);

    /* Persist free lists to disk
       @pre caller has locked the DataStore
       @throws system exception on error
     */
    void persistFreelists();

    /* Initialize the free list and allocated size
       based on the size of the data store
     */
    void initializeFreelist();

    /* Read free lists from disk file
       @returns number of buckets successfully read
     */
    int readFreelistFromFile();

    /* Invalidate the free-list file on disk
       @pre caller has locked the DataStore
     */
    void invalidateFreelistFile();

    /* Remove the free-list file from disk
       @pre caller has locked the DataStore
     */
    void removeFreelistFile();

    /* Set the data store to be removed from disk on close
     */
    void removeOnClose()
        { _file->removeOnClose(); }

    /* Iterate the free lists and find a free chunk of the requested size
       @pre caller has locked the DataStore
     */
    off_t searchFreelist(size_t request);

    /* Add block to free list and try to consolidate buddy blocks
     */
    void addToFreelist(size_t bucket, off_t off);

    /* Verify the freelist, but with lock already held
     */
    void verifyFreelistInternal();

    /* Allocate more space into the data store to handle the requested chunk
     */
    void makeMoreSpace(size_t request);

    /* Update the largest free chunk member
     */
    void calcLargestFreeChunk();

    /* Check whether any parent blocks are on the freelist
     */
    bool isParentBlockFree(off_t off, size_t size);

    /* Dump the free list to the log for debug
     */
    void dumpFreelist();

    /* Free lists for data store
       power-of-two ---->  set of offsets
     */
    typedef std::map< size_t, std::set<off_t> > DataStoreFreelists;

    /* Header that prepends all chunks on disk
     */
    class DiskChunkHeader
    {
    public:
        static const size_t usedValue;    // special value to mark headers in use
        static const size_t freeValue;    // special value to mark free header
        size_t magic;                
        size_t size;
        DiskChunkHeader(bool free, size_t sz) : 
            magic(free ? freeValue : usedValue),
            size(sz)
            {}
        DiskChunkHeader() :
            magic(freeValue),
            size(0)
            {}
        bool isValid() { return (magic == usedValue) || (magic == freeValue); }
        bool isFree() { return magic == freeValue; }
    };

    /* Serialized free list bucket
     */
    class FreelistBucket
    {
        boost::scoped_array<char> _buf; // serialized data

        /* Pointers into _buf
         */
        size_t* _size;        // total size of the serialized data
        size_t* _key;         // size of blocks in bucket
        size_t* _nelements;   // number of free elements in bucket
        off_t* _offsets;      // offsets of free elements
        uint32_t* _crc;       // crc for entire serialized bucket

    public:
        /* Construct an flb from a free list bucket
         */
        FreelistBucket(size_t key, std::set<off_t>& bucket);
        /* Construct an flb by reading it from a file
         */
        FreelistBucket(File::FilePtr& f, off_t offset);

        /* Serialize the flb to a file
         */
        void write(File::FilePtr& f, off_t offset);
        
        /* Unserialize the flb into the freelist
         */
        void unload(DataStoreFreelists& fl);

        size_t size() { return *_size + sizeof(size_t); }
    };

    DataStores*                _dsm;              // DataStores manager
    mutable Mutex              _dslock;           // lock protects local state
    Guid                       _guid;             // unique id for this store
    File::FilePtr              _file;             // handle for data file
    mutable DataStoreFreelists _freelists;        // free blocks in the data file
    uint64_t                   _frees;            // counter used to track calls to free
    size_t                     _largestFreeChunk; // size of the biggest chunk in free list
    size_t                     _allocatedSize;    // size of the store including free blks
    bool                       _dirty;            // unflushed data is present
    bool                       _fldirty;          // fl data differs from fl data on-disk
};


/**
 * @brief   Periodically flushes dirty datastores in the background
 *
 * @details Runs a separate thread that wakes up at a configured interval
 *          and flushes any data stores that have been added to its list.
 */
class DataStoreFlusher
{
private:
    DataStores&_dsm;
    boost::shared_ptr<JobQueue> _queue;
    boost::shared_ptr<ThreadPool> _threadPool;
    bool _running;
    std::set <DataStore::Guid> _datastores;
    Mutex _lock;

    class FlushJob : public Job
    {
    private:
        int64_t _timeIntervalNanos;
        DataStoreFlusher *_flusher;
        
    public:
        FlushJob(int timeIntervalMSecs,
                 DataStoreFlusher* flusher):
            Job(boost::shared_ptr<Query>()),
            _timeIntervalNanos( (int64_t) timeIntervalMSecs * 1000000 ),
            _flusher(flusher)
            {}
        
        virtual void run();
    };

    boost::shared_ptr<FlushJob> _myJob;
    
public:
    DataStoreFlusher(DataStores& dsm):
        _dsm(dsm),
        _queue(boost::shared_ptr<JobQueue>(new JobQueue())),
        _threadPool(boost::shared_ptr<ThreadPool>(new ThreadPool(1, _queue))),
        _running(false),
        _datastores(),
        _lock(),
        _myJob()
        {}

    void start(int timeIntervalMSecs);
    void add(DataStore::Guid dsguid);
    void stop();
    
    ~DataStoreFlusher();
};


/**
 * @brief   Class which manages group of datastores
 *
 * @details DataStores is a class which manages and
 *          keeps track of a set of DataStore objects
 *          located at a common root in the file system.
 *          Callers obtain/close a DataStore by going through
 *          the DataStores interface.
 */
class DataStores
{
public:

    /**
     * Initialize the global DataStore state
     * @param basepath path to the root of the storage heirarchy
     */
    void initDataStores(char const* basepath);

    /**
     * Get a reference to a specific DataStore
     * @param guid unique identifier for desired datastore
     */
    boost::shared_ptr<DataStore> getDataStore(DataStore::Guid guid);

    /**
     * Remove a data store from memory and (if remove is true) from disk
     * @param guid unique identifier for target datastore
     */
    void closeDataStore(DataStore::Guid guid, bool remove);

    /**
     * Flush all DataStore objects
     * @throws user exception on error
     */
    void flushAllDataStores();

    /**
     * Clear all datastore files from the basepath
     */
    void clearAllDataStores();

    /**
     * List information about all datastores using the builder
     */
    void listDataStores(ListDataStoresArrayBuilder& builder);

    /**
     * Accessor, return the min allocation size
     */
    size_t getMinAllocSize()
        { return _minAllocSize; }

    /**
     * Accessor, return a ref to the error listener
     */
    InjectedErrorListener<DataStoreInjectedError>& getErrorListener()
        { return _listener; }

    /**
     * Accessor, return a ref to the flusher
     */
    DataStoreFlusher& getFlusher()
        { return _dsflusher; }

    /**
     * Constructor
     */
    DataStores() :
        _theDataStores(NULL),
        _basePath(""),
        _minAllocSize(0),
        _dsflusher(*this)
        {}

    /**
     * Destructor
     */
    ~DataStores();

private:
    
    typedef std::map< DataStore::Guid, boost::shared_ptr<DataStore> > DataStoreMap;

    /* Global map of all DataStores
     */
    DataStoreMap* _theDataStores;
    Mutex         _dataStoreLock;

    std::string _basePath;        // base path of data directory
    size_t      _minAllocSize;    // smallest allowed allocation

    /* Error listener for invalidate path
     */
    InjectedErrorListener<DataStoreInjectedError> _listener;

    /* Optional data store flusher
     */
    DataStoreFlusher _dsflusher;
};

inline static uint32_t calculateCRC32(void const* content, size_t content_length, uint32_t crc = ~0)
{
    static const uint32_t table [] = {
        0x00000000, 0x77073096, 0xEE0E612C, 0x990951BA,
        0x076DC419, 0x706AF48F, 0xE963A535, 0x9E6495A3,
        0x0EDB8832, 0x79DCB8A4, 0xE0D5E91E, 0x97D2D988,
        0x09B64C2B, 0x7EB17CBD, 0xE7B82D07, 0x90BF1D91,

        0x1DB71064, 0x6AB020F2, 0xF3B97148, 0x84BE41DE,
        0x1ADAD47D, 0x6DDDE4EB, 0xF4D4B551, 0x83D385C7,
        0x136C9856, 0x646BA8C0, 0xFD62F97A, 0x8A65C9EC,
        0x14015C4F, 0x63066CD9, 0xFA0F3D63, 0x8D080DF5,

        0x3B6E20C8, 0x4C69105E, 0xD56041E4, 0xA2677172,
        0x3C03E4D1, 0x4B04D447, 0xD20D85FD, 0xA50AB56B,
        0x35B5A8FA, 0x42B2986C, 0xDBBBC9D6, 0xACBCF940,
        0x32D86CE3, 0x45DF5C75, 0xDCD60DCF, 0xABD13D59,

        0x26D930AC, 0x51DE003A, 0xC8D75180, 0xBFD06116,
        0x21B4F4B5, 0x56B3C423, 0xCFBA9599, 0xB8BDA50F,
        0x2802B89E, 0x5F058808, 0xC60CD9B2, 0xB10BE924,
        0x2F6F7C87, 0x58684C11, 0xC1611DAB, 0xB6662D3D,

        0x76DC4190, 0x01DB7106, 0x98D220BC, 0xEFD5102A,
        0x71B18589, 0x06B6B51F, 0x9FBFE4A5, 0xE8B8D433,
        0x7807C9A2, 0x0F00F934, 0x9609A88E, 0xE10E9818,
        0x7F6A0DBB, 0x086D3D2D, 0x91646C97, 0xE6635C01,

        0x6B6B51F4, 0x1C6C6162, 0x856530D8, 0xF262004E,
        0x6C0695ED, 0x1B01A57B, 0x8208F4C1, 0xF50FC457,
        0x65B0D9C6, 0x12B7E950, 0x8BBEB8EA, 0xFCB9887C,
        0x62DD1DDF, 0x15DA2D49, 0x8CD37CF3, 0xFBD44C65,

        0x4DB26158, 0x3AB551CE, 0xA3BC0074, 0xD4BB30E2,
        0x4ADFA541, 0x3DD895D7, 0xA4D1C46D, 0xD3D6F4FB,
        0x4369E96A, 0x346ED9FC, 0xAD678846, 0xDA60B8D0,
        0x44042D73, 0x33031DE5, 0xAA0A4C5F, 0xDD0D7CC9,

        0x5005713C, 0x270241AA, 0xBE0B1010, 0xC90C2086,
        0x5768B525, 0x206F85B3, 0xB966D409, 0xCE61E49F,
        0x5EDEF90E, 0x29D9C998, 0xB0D09822, 0xC7D7A8B4,
        0x59B33D17, 0x2EB40D81, 0xB7BD5C3B, 0xC0BA6CAD,

        0xEDB88320, 0x9ABFB3B6, 0x03B6E20C, 0x74B1D29A,
        0xEAD54739, 0x9DD277AF, 0x04DB2615, 0x73DC1683,
        0xE3630B12, 0x94643B84, 0x0D6D6A3E, 0x7A6A5AA8,
        0xE40ECF0B, 0x9309FF9D, 0x0A00AE27, 0x7D079EB1,

        0xF00F9344, 0x8708A3D2, 0x1E01F268, 0x6906C2FE,
        0xF762575D, 0x806567CB, 0x196C3671, 0x6E6B06E7,
        0xFED41B76, 0x89D32BE0, 0x10DA7A5A, 0x67DD4ACC,
        0xF9B9DF6F, 0x8EBEEFF9, 0x17B7BE43, 0x60B08ED5,

        0xD6D6A3E8, 0xA1D1937E, 0x38D8C2C4, 0x4FDFF252,
        0xD1BB67F1, 0xA6BC5767, 0x3FB506DD, 0x48B2364B,
        0xD80D2BDA, 0xAF0A1B4C, 0x36034AF6, 0x41047A60,
        0xDF60EFC3, 0xA867DF55, 0x316E8EEF, 0x4669BE79,

        0xCB61B38C, 0xBC66831A, 0x256FD2A0, 0x5268E236,
        0xCC0C7795, 0xBB0B4703, 0x220216B9, 0x5505262F,
        0xC5BA3BBE, 0xB2BD0B28, 0x2BB45A92, 0x5CB36A04,
        0xC2D7FFA7, 0xB5D0CF31, 0x2CD99E8B, 0x5BDEAE1D,

        0x9B64C2B0, 0xEC63F226, 0x756AA39C, 0x026D930A,
        0x9C0906A9, 0xEB0E363F, 0x72076785, 0x05005713,
        0x95BF4A82, 0xE2B87A14, 0x7BB12BAE, 0x0CB61B38,
        0x92D28E9B, 0xE5D5BE0D, 0x7CDCEFB7, 0x0BDBDF21,

        0x86D3D2D4, 0xF1D4E242, 0x68DDB3F8, 0x1FDA836E,
        0x81BE16CD, 0xF6B9265B, 0x6FB077E1, 0x18B74777,
        0x88085AE6, 0xFF0F6A70, 0x66063BCA, 0x11010B5C,
        0x8F659EFF, 0xF862AE69, 0x616BFFD3, 0x166CCF45,

        0xA00AE278, 0xD70DD2EE, 0x4E048354, 0x3903B3C2,
        0xA7672661, 0xD06016F7, 0x4969474D, 0x3E6E77DB,
        0xAED16A4A, 0xD9D65ADC, 0x40DF0B66, 0x37D83BF0,
        0xA9BCAE53, 0xDEBB9EC5, 0x47B2CF7F, 0x30B5FFE9,

        0xBDBDF21C, 0xCABAC28A, 0x53B39330, 0x24B4A3A6,
        0xBAD03605, 0xCDD70693, 0x54DE5729, 0x23D967BF,
        0xB3667A2E, 0xC4614AB8, 0x5D681B02, 0x2A6F2B94,
        0xB40BBE37, 0xC30C8EA1, 0x5A05DF1B, 0x2D02EF8D
    };

    unsigned char* buffer = (unsigned char*) content;

    while (content_length-- != 0)
    {
        crc = (crc >> 8) ^ table[(crc & 0xFF) ^ *buffer++];
    }
    return crc;
}

}

#endif // DATASTORE_H_
