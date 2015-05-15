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
 * @file DataStore.cpp
 * @brief Implementation of array data file
 * @author sfridella@paradigm4.com
 */

/* Implementation notes:

   DataStore file is divided into power-of-two sized chunks. Some important
   invariants:

   1) If the file is non-zero length, then there are always valid chunks at
      offset 0, and offset filesize/2.  We never have a file with a single
      chunk that spans the entire file.
 */

#include <log4cxx/logger.h>
#include <util/DataStore.h>
#include <util/FileIO.h>
#include <util/Thread.h>
#include <system/Config.h>
#include <query/ops/list/ListArrayBuilder.h>

namespace scidb
{

using namespace boost;
using namespace std;

static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.smgr.datastore"));

/* ChunkHeader special values */
const size_t DataStore::DiskChunkHeader::usedValue = 0xfeedfacefeedface;
const size_t DataStore::DiskChunkHeader::freeValue = 0xdeadbeefdeadbeef;

/* Construct an flb structure from a bucket on the free list
 */
DataStore::FreelistBucket::FreelistBucket(size_t key, std::set<off_t>& bucket)
{
    size_t bucketsize = 
        (2 * sizeof(size_t)) +
        (bucket.size() * sizeof(off_t)) +
        sizeof(uint32_t);
    size_t bufsize = bucketsize + sizeof(size_t);

    _buf.reset(new char[bufsize]);

    /* Format of bucket:
       <size><key><nelements><offset 1>...<offset n><crc>
    */

    char* pos;
    
    pos = _buf.get();
    _size = reinterpret_cast<size_t*>(pos);
    pos += sizeof(size_t);
    _key = reinterpret_cast<size_t*>(pos);
    pos += sizeof(size_t);
    _nelements = reinterpret_cast<size_t*>(pos);
    pos += sizeof(size_t);
    _offsets = reinterpret_cast<off_t*>(pos);
    pos += (bucket.size() * sizeof(off_t));
    _crc = reinterpret_cast<uint32_t*>(pos);

    *_size = bucketsize;
    *_key = key;
    *_nelements = bucket.size();

    std::set<off_t>::iterator bucket_it;
    off_t offset = 0;

    for (bucket_it = bucket.begin();
         bucket_it != bucket.end();
         ++bucket_it)
    {
        
        _offsets[offset++] = *bucket_it;
    }

    *_crc = calculateCRC32((void*)_key, *_size - sizeof(uint32_t));
}

/* Construct an flb by reading it from a file
 */
DataStore::FreelistBucket::FreelistBucket(File::FilePtr& f, off_t offset)
{
    /* Format of bucket:
       <size><key><nelements><offset 1>...<offset n><crc>
    */

    size_t bufsize;
    size_t bucketsize;

    f->readAll(&bucketsize, sizeof(size_t), offset);
    bufsize = bucketsize + sizeof(size_t);
    offset += sizeof(size_t);

    _buf.reset(new char[bufsize]);

    f->readAll(_buf.get() + sizeof(size_t), bucketsize, offset);

    char* pos = _buf.get();

    _size = (size_t*) pos;
    *_size = bucketsize;
    pos += sizeof(size_t);
    _key = (size_t*) pos;
    pos += sizeof(size_t);
    _nelements = (size_t*) pos;
    pos += sizeof(size_t);
    _offsets = (off_t*) pos;
    pos += (*_nelements * sizeof(off_t));
    _crc = (uint32_t*) pos;

    if (*_crc != calculateCRC32(_key, *_size - sizeof(uint32_t)))
    {
        throw SYSTEM_EXCEPTION(SCIDB_SE_STORAGE, SCIDB_LE_DATASTORE_CORRUPT_FREELIST)
            << f->getPath();
    }
}

/* Serialize the flb to a file
 */
void
DataStore::FreelistBucket::write(File::FilePtr& f, off_t offset)
{
    /* Format of bucket:
       <size><key><nelements><offset 1>...<offset n><crc>
    */
    f->writeAll(_buf.get(), (*_size + sizeof(size_t)), offset);
}
        
/* Unserialize the flb into the freelist
 */
void
DataStore::FreelistBucket::unload(DataStoreFreelists& fl)
{
    LOG4CXX_TRACE(logger, "DataStore: uloading bucket with key " << *_key <<
                  " and size " << *_nelements);

    for (size_t offset = 0; offset < *_nelements; ++offset)
    {
        fl[*_key].insert(_offsets[offset]);
    } 
}

/* Find space for the chunk of indicated size in the DataStore.
 */
off_t
DataStore::allocateSpace(size_t requestedSize, size_t& allocatedSize)
{
    ScopedMutexLock sm(_dslock);
    off_t ret = 0;

    LOG4CXX_TRACE(logger, "datastore: allocate space " << requestedSize << " for " << 
                  _file->getPath());   

    invalidateFreelistFile();

    /* Round up required size to next power-of-two
     */
    size_t requiredSize = requestedSize + sizeof(DiskChunkHeader);
    if (requiredSize < _dsm->getMinAllocSize())
        requiredSize = _dsm->getMinAllocSize();
    requiredSize = roundUpPowerOf2(requiredSize);

    /* Check if the free lists have a chunk of the proper size
     */
    if (requiredSize > _largestFreeChunk)
    {
        makeMoreSpace(requiredSize);
    }
    SCIDB_ASSERT(requiredSize <= _largestFreeChunk);

    /* Look in the freelist to find a chunk to allocate.
     */
    ret = searchFreelist(requiredSize);
    allocatedSize = requiredSize;

    /* Update the largest free chunk
     */
    calcLargestFreeChunk();

    LOG4CXX_TRACE(logger, "datastore: allocate space " << requestedSize << " for " 
                  << _file->getPath() << " returned " << ret);   

    return ret;
}

/* Write bytes to the DataStore, to a location that is already
   allocated
 */
void
DataStore::writeData(off_t off,
                     void const* buffer,
                     size_t len,
                     size_t allocatedSize)
{
    ScopedMutexLock sm(_dslock);

    DiskChunkHeader hdr(false, allocatedSize);
    struct iovec iovs[2];

    /* Set up the iovecs
     */
    iovs[0].iov_base = (char*) &hdr;
    iovs[0].iov_len = sizeof(DiskChunkHeader);
    iovs[1].iov_base = (char*) buffer;
    iovs[1].iov_len = len;

    /* Issue the write
     */
    _file->writeAllv(iovs, 2, off);

    /* Update the dirty flag and schedule flush if necessary
     */
    if (!_dirty)
    {
        _dirty = true;
        _dsm->getFlusher().add(_guid);
    }
}

/* Read a chunk from the DataStore
 */
void
DataStore::readData(off_t off, void* buffer, size_t len)
{
    DiskChunkHeader hdr;
    struct iovec iovs[2];

    /* Set up the iovecs
     */
    iovs[0].iov_base = (char*) &hdr;
    iovs[0].iov_len = sizeof(DiskChunkHeader);
    iovs[1].iov_base = (char*) buffer;
    iovs[1].iov_len = len;

    /* Issue the read
     */
    _file->readAllv(iovs, 2, off);
    
    /* Check validity of header
     */
    if (!hdr.isValid())
    {
        throw SYSTEM_EXCEPTION(SCIDB_SE_STORAGE, SCIDB_LE_DATASTORE_CHUNK_CORRUPTED)
            << _file->getPath() << off;
    }
}

/* Flush dirty data and metadata for the DataStore
 */
void
DataStore::flush()
{
    LOG4CXX_TRACE(logger, "DataStore::flush for ds " << _file->getPath());
    ScopedMutexLock sm(_dslock);

    if (_dirty)
    {
        LOG4CXX_TRACE(logger, "DataStore::flushing data for ds " << _file->getPath());
        if (_file->fsync() != 0)
        {
            throw USER_EXCEPTION(SCIDB_SE_STORAGE, SCIDB_LE_OPERATION_FAILED) <<
                "fsync " + _file->getPath();
        }
        _dirty = false;
    }
    if (_fldirty)
    {
        LOG4CXX_TRACE(logger, "DataStore::flushing metadata for ds " << _file->getPath());
        persistFreelists();
    }
}

/* Mark chunk as free both in the free lists and on
   disk
 */
void 
DataStore::freeChunk(off_t off, size_t allocated)
{
    ScopedMutexLock sm(_dslock);

    LOG4CXX_TRACE(logger, "datastore: free chunk " << off << " for " << 
                  _file->getPath());   

    invalidateFreelistFile();

    /* Update the free list
     */
    addToFreelist(allocated, off);
    calcLargestFreeChunk();
}


/* Return size information about the data store
 */
void
DataStore::getSizes(off_t& filesize,
                    blkcnt_t& fileblocks,
                    off_t& reservedbytes,
                    off_t& freebytes) const
{
    ScopedMutexLock sm(_dslock);

    struct stat st;

    /* Get the file size information
     */
    int rc = _file->fstat(&st);
    if (rc != 0)
    {
        throw (SYSTEM_EXCEPTION(SCIDB_SE_STORAGE, SCIDB_LE_SYSCALL_ERROR)
               << "fstat" << rc << errno << ::strerror(errno) << _file->getPath());
    }

    filesize = st.st_size;
    fileblocks = st.st_blocks;
    freebytes = 0;

    /* Calc the number of free and reserved bytes
     */
    DataStoreFreelists::iterator dsit = _freelists.begin();
    while (dsit != _freelists.end())
    {
        set<off_t>::iterator bucketit = dsit->second.begin();
        while (bucketit != dsit->second.end())
        {
            freebytes += dsit->first;
            ++bucketit;
        }
        ++dsit;
    }
    reservedbytes = _allocatedSize - freebytes;
}

/* Persist free lists to disk
   @pre caller has locked the DataStore
 */
void
DataStore::persistFreelists()
{
    /* Open and truncate the freelist file
     */
    File::FilePtr flfile;
    std::string filename;

    filename = _file->getPath() + ".fl";
    flfile = FileManager::getInstance()->openFileObj(filename, O_CREAT | O_TRUNC | O_RDWR);
    if (!flfile)
    {
        throw SYSTEM_EXCEPTION(SCIDB_SE_STORAGE, SCIDB_LE_CANT_OPEN_PATH)
            << filename;
    }
    
    /* Iterate the freelists, writing as we go
       File format:
       <# of buckets><bucket 1>...<bucket n>
     */
    off_t fileoff = 0;
    size_t nbuckets = _freelists.size();
    DataStoreFreelists::iterator freelist_it;
    std::set<off_t>::iterator bucket_it;
    
    LOG4CXX_TRACE(logger, "datastore: persisting freelist for " << 
                  _file->getPath() << " buckets " << nbuckets);   
    
    flfile->writeAll((void*)&nbuckets, sizeof(size_t), fileoff);
    fileoff += sizeof(size_t);
   
    for (freelist_it = _freelists.begin(); 
         freelist_it != _freelists.end(); 
         ++freelist_it)
    {
        std::set<off_t>& bucket = freelist_it->second;
        if (bucket.size() == 0)
            continue;

        FreelistBucket flb(freelist_it->first, bucket);

        flb.write(flfile, fileoff);
        fileoff += flb.size();
    }

    flfile->fsync();
    _fldirty = false;
}

/* Initialize the free list and allocated size
   based on the size of the data store
*/
void
DataStore::initializeFreelist()
{
    /* Calc the correct new block size
     */
    struct stat st;
    size_t roundUpSize;
    
    _allocatedSize = _dsm->getMinAllocSize();
    _allocatedSize = roundUpPowerOf2(_allocatedSize);
    if (_file->fstat(&st) != 0)
    {
        throw SYSTEM_EXCEPTION(SCIDB_SE_STORAGE,
                               SCIDB_LE_SYSCALL_ERROR)
            << "fstat" << -1 << errno << ::strerror(errno) << _file->getPath(); 
    }    
    roundUpSize = roundUpPowerOf2(st.st_size);
    if (roundUpSize > _allocatedSize)
    {
        _allocatedSize = roundUpSize;
    }

    /* Try to read the freelist from the md file.
       If we fail, assume that the file is fully allocated,
       but we should at least mark the area between eof
       and allocated size as free.
     */
    if (readFreelistFromFile() == 0)
    {
        roundUpSize = _allocatedSize;
        while (roundUpSize > static_cast<size_t>(st.st_size))
        {
            size_t gap = roundUpSize - st.st_size;
            gap = roundUpPowerOf2(gap) / 2;
            if (gap < _dsm->getMinAllocSize())
            {
                break;
            }
            size_t off = roundUpSize - gap;
            _freelists[gap].insert(off);
            roundUpSize = off;
        }
        calcLargestFreeChunk();
    }
}

/* Read free lists from disk file
   @returns number of buckets successfully read
*/
int
DataStore::readFreelistFromFile()
{
    /* Try to open the freelist file
     */
    File::FilePtr flfile;
    std::string filename;

    filename = _file->getPath() + ".fl";
    flfile = FileManager::getInstance()->openFileObj(filename, O_RDONLY);
    if (!flfile)
    {
        return 0;
    }

    /* Sanity check:  make sure its not empty
     */
    struct stat st;

    if (flfile->fstat(&st) || (st.st_size == 0))
    {
        LOG4CXX_ERROR(logger, "DataStore: found empty freelist file for " <<
                      _file->getPath());
        return 0;
    }

    /* Try to parse the contents
       File format:
       <# of buckets><bucket 1>...<bucket n>
     */
    off_t fileoff = 0;
    size_t nbuckets = 0;
    size_t current;

    flfile->readAll(&nbuckets, sizeof(size_t), fileoff);
    fileoff += sizeof(size_t);

    LOG4CXX_TRACE(logger, "DataStore: reading " << nbuckets <<
                  " for freelist");

    for (current = 0; current < nbuckets; ++current)
    {
        try
        {
            FreelistBucket flb(flfile, fileoff);

            fileoff += flb.size();
            flb.unload(_freelists);
        }
        catch (SystemException const& x)
        {
            LOG4CXX_ERROR(logger, "DataStore: failed to read freelist for " <<
                          _file->getPath() << ", error (" << x.getErrorMessage() << ")");
            _freelists.clear();
            return 0;
        }
    }

    return nbuckets;
}

/* Invalidate the free-list file on disk
   @pre caller has locked the DataStore
 */
void
DataStore::invalidateFreelistFile()
{
    if (!_fldirty)
    {
        File::FilePtr flfile;
        std::string filename;
        size_t nbuckets = 0;

        LOG4CXX_TRACE(logger, "datastore: invalidating freelist for " << _file->getPath());   

        filename = _file->getPath() + ".fl";
        flfile = FileManager::getInstance()->openFileObj(filename, O_CREAT | O_TRUNC | O_RDWR);
        if (!flfile)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_STORAGE, SCIDB_LE_CANT_OPEN_PATH)
                << filename;
        }

        /* This is one vulnerable spot... after truncate, but before we write the zero byte
         */
        _dsm->getErrorListener().check();

        flfile->writeAll((void*)&nbuckets, sizeof(size_t), 0);
        flfile->fsync();
        _fldirty = true;
        _dsm->getFlusher().add(_guid);
    }
}

/* Dump the free list to the log for debug
 */
void
DataStore::dumpFreelist()
{
    DataStoreFreelists::iterator fl_it = _freelists.begin();

    LOG4CXX_DEBUG(logger, "Freelists for datastore " << 
                  _file->getPath() << ": ");

    while (fl_it != _freelists.end())
    {
        set<off_t>::iterator bucket_it = fl_it->second.begin();
        
        LOG4CXX_DEBUG(logger, "   bucket [ " <<
                      fl_it->first << " ] :");
        
        while (bucket_it != fl_it->second.end())
        {
            LOG4CXX_DEBUG(logger, "     offset : " << *bucket_it);
            ++bucket_it;
        }
        ++fl_it;
    }
}

/* Check whether any parent blocks are on the freelist
   pre: lock is held
 */
bool
DataStore::isParentBlockFree(off_t off, size_t size)
{
    DataStoreFreelists::iterator fl_it = 
        _freelists.upper_bound(size);

    while (fl_it != _freelists.end())
    {
        const size_t bucketSize = fl_it->first;
        const set<off_t>& bucket = fl_it->second;
        const off_t parentOff = off - (off % bucketSize);
        if (bucket.find(parentOff) != bucket.end())
        {
            return true;
        }
        ++fl_it;
    }
    return false;
}

/* Verify the integrity of the free list and throw exception
 * if there is a problem
 */
void
DataStore::verifyFreelist()
{
    ScopedMutexLock sm(_dslock);
    verifyFreelistInternal();
}

/* Verify the integrity of the free list when lock is already held
 */
void
DataStore::verifyFreelistInternal()
{
    DataStoreFreelists::iterator fl_it = _freelists.begin();

    while (fl_it != _freelists.end())
    {
        set<off_t>::iterator bucket_it = fl_it->second.begin();
        while (bucket_it != fl_it->second.end())
        {
            if (isParentBlockFree(*bucket_it, fl_it->first))
            {
                throw SYSTEM_EXCEPTION(SCIDB_SE_STORAGE, 
                                       SCIDB_LE_DATASTORE_CORRUPT_FREELIST)
                    << _file->getPath();
            }
            ++bucket_it;
        }
        ++fl_it;
    }  
}

/* Remove the free-list file from disk
   @pre caller has locked the DataStore
 */
void
DataStore::removeFreelistFile()
{
    /* Try to remove the freelist file
     */
    std::string filename = _file->getPath() + ".fl";
    File::remove(filename.c_str(), false);
}

/* Destroy a DataStore object
 */
DataStore::~DataStore()
{
}

/* Construct a new DataStore object
 */
DataStore::DataStore(char const* filename, Guid guid, DataStores& parent) :
    _dsm(&parent),
    _dslock(),
    _guid(guid),
    _frees(0),
    _largestFreeChunk(0),
    _dirty(false),
    _fldirty(false)
{
    /* Open the file
     */
    string filenamestr = filename;

    _file = FileManager::getInstance()->openFileObj(filenamestr.c_str(), O_LARGEFILE | O_RDWR | O_CREAT);
    if (_file.get() == NULL)
    {
        throw SYSTEM_EXCEPTION(SCIDB_SE_STORAGE,
                               SCIDB_LE_CANT_OPEN_FILE) 
            << filenamestr << ::strerror(errno) << errno;
    }

    LOG4CXX_TRACE(logger, "datastore: new ds opened file " << filenamestr);

    /* Try to initialize the free lists from the free-list file.
     */
    initializeFreelist();
}

/* Round up size_t value to next power of two (static)
 */
size_t
DataStore::roundUpPowerOf2(size_t size)
{
    size_t roundupSize = size;
    --roundupSize;
    roundupSize |= roundupSize >> 1;
    roundupSize |= roundupSize >> 2;
    roundupSize |= roundupSize >> 4;
    roundupSize |= roundupSize >> 8;
    roundupSize |= roundupSize >> 16;
    roundupSize |= roundupSize >> 32;
    ++roundupSize;

    return roundupSize;
}

/* Allocate more space into the data store to handle the requested chunk
 */
void
DataStore::makeMoreSpace(size_t request)
{
    SCIDB_ASSERT(request > _largestFreeChunk);
    SCIDB_ASSERT(_allocatedSize >= _largestFreeChunk);

    while (request > _largestFreeChunk)
    {
        _freelists[_allocatedSize].insert(_allocatedSize);
        _largestFreeChunk = _allocatedSize;
        _allocatedSize *= 2;
    }
}

/* Iterate the free lists and find a free chunk of the requested size
   @pre caller has locked the DataStore
 */
off_t
DataStore::searchFreelist(size_t request)
{
    off_t ret = 0;

    SCIDB_ASSERT(request <= _largestFreeChunk);

    /* Base case:  the target bucket contains a free chunk
     */
    DataStoreFreelists::iterator it =
        _freelists.find(request);
    if (it != _freelists.end())
    {
        assert(it->second.size() > 0);
        ret = *(it->second.begin());
        it->second.erase(ret);
        if (it->second.size() == 0)
        {
            _freelists.erase(it);
        }
    }
    /* Recursive case:  we have to get a free chunk by breaking
       up a larger free chunk
     */
    else
    {
        ret = searchFreelist(request * 2);
        _freelists[request].insert(ret + request);
    }
    
    return ret;
}

/* Add block to free list and try to consolidate buddy blocks
 */
void
DataStore::addToFreelist(size_t bucket, off_t off)
{
    SCIDB_ASSERT(roundUpPowerOf2(bucket) == bucket);
    SCIDB_ASSERT(off % bucket == 0);

    /* First check if a parent block is already on the freelist
       (possibly in crash recovery case).  If so, we don't
       need to do anything.
    */
    if (isParentBlockFree(off, bucket))
    {
        return;
    }

    /* Calc the buddy block
     */
    size_t parent = bucket * 2;
    off_t buddy;

    if (off % parent == 0)
    {
        buddy = off + bucket;
    }
    else
    {
        buddy = off - bucket;
    }

    /* Check if the buddy is free
     */
    DataStoreFreelists::iterator it =
        _freelists.find(bucket);
    if (it != _freelists.end())
    {
        std::set<off_t>::iterator bucket_it;

        bucket_it = (it->second).find(buddy);
        if (bucket_it != (it->second).end())
        {
            /* Merge with the buddy
             */
            off_t merged = (off < buddy) ? off : buddy;

            (it->second).erase(bucket_it);
            if ((it->second).size() == 0)
            {
                _freelists.erase(it);
            }
            addToFreelist(parent, merged);
            return;
        }
    }

    /* Buddy is not free, just insert into
       free list
     */
    _freelists[bucket].insert(off);

    /* Occaisionally we should check the integrity of
       the freelist (only in DEBUG)
     */
    if (isDebug())
    {
        if (++_frees % 1000 == 0)
        {
            verifyFreelistInternal();
        }
    }
}

/* Update the largest free chunk member
 */
void
DataStore::calcLargestFreeChunk()
{
    DataStoreFreelists::const_iterator it;

    it = _freelists.end();
    _largestFreeChunk =
        (it == _freelists.begin()) ?
        0 :
        (--it)->first;
}


/* Initialize the global DataStore state
 */
void
DataStores::initDataStores(char const* basepath)
{
    ScopedMutexLock sm(_dataStoreLock);

    if (_theDataStores == NULL)
    {
        _basePath = basepath;
        _basePath += "/";
        _minAllocSize = Config::getInstance()->getOption<int>(CONFIG_STORAGE_MIN_ALLOC_SIZE_BYTES);

        /* Create the datastore directory if necessary
         */
        if (!File::createDir(_basePath))
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_STORAGE, SCIDB_LE_CANT_CREATE_DIRECTORY)
                << _basePath;
        }
        
        /* Start background flusher
         */
        int syncMSeconds = Config::getInstance()->getOption<int> (CONFIG_SYNC_IO_INTERVAL);
        if (syncMSeconds > 0)
        {
            _dsflusher.start(syncMSeconds);
        }

        _theDataStores = new DataStoreMap();

        /* Start error listener
         */
        _listener.start();
    }
}

/* Get a reference to a specific DataStore
 */
shared_ptr<DataStore> 
DataStores::getDataStore(DataStore::Guid guid)
{
    DataStoreMap::iterator it;
    shared_ptr<DataStore> retval;
    ScopedMutexLock sm(_dataStoreLock);
    
    SCIDB_ASSERT(_theDataStores);

    /* Check the map
     */
    it = _theDataStores->find(guid);
    if (it != _theDataStores->end())
    {
        return it->second;
    }

    /* Not found, construct the object
     */
    stringstream filepath;
    filepath << _basePath << guid << ".data";
    retval = boost::make_shared<DataStore>(filepath.str().c_str(), 
                                           guid, 
                                           boost::ref(*this));
    (*_theDataStores)[guid] = retval;

    return retval;
}

/* Remove a data store from memory and disk
 */
void
DataStores::closeDataStore(DataStore::Guid guid, bool remove)
{
    DataStoreMap::iterator it;
    ScopedMutexLock sm(_dataStoreLock);
    
    SCIDB_ASSERT(_theDataStores);

    /* Check the map
     */
    it = _theDataStores->find(guid);
    if (it == _theDataStores->end())
    {
        /* It isn't in the map... maybe it hasn't been opened this time.  If remove
           is specified we need to open it so we can remove it from disk.
         */
        if (remove)
        {
            stringstream filepath;
            filepath << _basePath << guid << ".data";
            it =
                _theDataStores->insert(
                    make_pair(
                        guid,
                        boost::make_shared<DataStore>(filepath.str().c_str(),
                                                      guid,
                                                      boost::ref(*this))
                        )
                    ).first;
        }
        else
        {
            return;
        }
    }

    /* Remove it from the map
     */
    if (remove)
    {
        it->second->removeOnClose();
        it->second->removeFreelistFile();
    }
    _theDataStores->erase(it);
}

/* Flush all DataStore objects
 */
void
DataStores::flushAllDataStores()
{
    DataStoreMap::iterator it;
    shared_ptr<DataStore> current;
    DataStore::Guid lastGuid = 0;

    while (true)
    {
        {
            ScopedMutexLock sm(_dataStoreLock);

            SCIDB_ASSERT(_theDataStores);

            it = _theDataStores->upper_bound(lastGuid);
            if (it == _theDataStores->end())
            {
                break;
            }
            current = it->second;
            lastGuid = it->first;
        }

        current->flush();
        current.reset();
    }
}

/* Clear all datastore files from the basepath
 */
void
DataStores::clearAllDataStores()
{
    /* Try to open the base dir
     */
    DIR* dirp = ::opendir(_basePath.c_str());

    if (dirp == NULL)
    {
        LOG4CXX_ERROR(logger, "DataStores::clearAllDataStores: failed to open base dir, aborting clearAll");
        return;
    }

    boost::function<int()> f = boost::bind(&File::closeDir, _basePath.c_str(), dirp, false);
    scidb::Destructor<boost::function<int()> >  dirCloser(f);

    struct dirent entry;
    memset(&entry, 0, sizeof(entry));

    /* For each entry in the base dir
     */
    while (true)
    {
        struct dirent *result(NULL);

        int rc = ::readdir_r(dirp, &entry, &result);
        if (rc != 0 || result == NULL)
        {
            return;
        }
        assert(result == &entry);

        LOG4CXX_TRACE(logger, "DataStores::clearAllDataStores: found entry " << entry.d_name);

        /* If its a datastore or fl file, go ahead and try to remove it
         */
        size_t entrylen = strlen(entry.d_name);
        size_t fllen = strlen(".fl");
        size_t datalen = strlen(".data");
        const char* entryend = entry.d_name + entrylen;

        /* Check if entry ends in ".fl" or ".data"
         */
        if (((entrylen > fllen) && 
             (strcmp(entryend - fllen, ".fl") == 0)) ||
            ((entrylen > datalen) &&
             (strcmp(entryend - datalen, ".data") == 0))
            )
        {
            LOG4CXX_TRACE(logger, "DataStores::clearAllDataStores: deleting entry " << entry.d_name);
            std::string fullpath = _basePath + "/" + entry.d_name;
            File::remove(fullpath.c_str(), false);
        }
    }
}

/* List information about all datastores using the builder
 */
void
DataStores::listDataStores(ListDataStoresArrayBuilder& builder)
{
    ScopedMutexLock sm(_dataStoreLock);
    DataStoreMap::iterator it = _theDataStores->begin();
    
    while (it != _theDataStores->end())
    {
        builder.listElement(*(it->second));
        ++it;
    }
}

/* Destroy the DataStores object
 */
DataStores::~DataStores()
{
    _dsflusher.stop();
    _listener.stop();
}

/* Get current time in nanosecond resolution
 */
inline int64_t getTimeNanos()
{
    struct timeval tv;
    gettimeofday(&tv,0);
    return ((int64_t) tv.tv_sec) * 1000000000 + ((int64_t) tv.tv_usec) * 1000;
}

/* Main loop for DataStoreFlusher
 */
void
DataStoreFlusher::FlushJob::run()
{
    while (true)
    {
        int64_t totalSyncTime= 0;
        {
            /* Collect the datastores we need to flush
             */
            set<DataStore::Guid>::iterator it;
            set<DataStore::Guid> dss;
            {
                ScopedMutexLock cs(_flusher->_lock);
                if ( (_flusher->_running) == false)
                {
                    return;
                }

                for (it = _flusher->_datastores.begin();
                     it != _flusher->_datastores.end();
                     ++it)
                {
                    dss.insert(*it);
                }
                _flusher->_datastores.clear();
            }
            
            /* Flush the collected data stores
             */
            for (it = dss.begin(); it != dss.end(); ++it)
            {
                shared_ptr<DataStore> ds;

                int64_t t0 = getTimeNanos();
                ds = _flusher->_dsm.getDataStore(*it);
                ds->flush();
                int64_t t1 = getTimeNanos();
                totalSyncTime = totalSyncTime + t1 - t0;
            }
        }
        
        if ( totalSyncTime < _timeIntervalNanos )
        {
            uint64_t sleepTime = _timeIntervalNanos - totalSyncTime;
            struct timespec req;
            req.tv_sec= sleepTime / 1000000000;
            req.tv_nsec = sleepTime % 1000000000;
            while (::nanosleep(&req, &req) != 0)
            {
                if (errno != EINTR)
                {
                    LOG4CXX_ERROR(logger, "DataStoreFlusher: nanosleep fail errno "<<errno);
                }
            }
        }
    }
}

/* Start the data store flusher
 */
void 
DataStoreFlusher::start(int timeIntervalMSecs)
{
    ScopedMutexLock cs(_lock);
    if (_running)
    {
        throw SYSTEM_EXCEPTION(SCIDB_SE_STORAGE, SCIDB_LE_OPERATION_FAILED) << 
            "DataStoreFlusher: error on start; already running";
    }

    _running = true;

    if (!_threadPool->isStarted())
    {
        _threadPool->start();
    }
    _myJob.reset(new FlushJob(timeIntervalMSecs, this));
    _queue->pushJob(_myJob);
}

/* Shut down the data store flusher
 */
void
DataStoreFlusher::stop()
{
    {
        ScopedMutexLock cs(_lock);
        if (_running)
        {
            _running = false;
        }
        else
        {
            return;
        }
    }

    if(!_myJob->wait())
    {
        LOG4CXX_ERROR(logger, "DataStoreFlusher: error on stop.");
    }
    
    _datastores.clear();
}

/* Schedule a data store to be flushed
 */
void
DataStoreFlusher::add(DataStore::Guid ds)
{
    ScopedMutexLock cs(_lock);
    if(_running)
    {
        _datastores.insert(ds);
    }
}

/* Destroy the flusher
 */
DataStoreFlusher::~DataStoreFlusher()
{
    stop();
}

} // namespace scidb

