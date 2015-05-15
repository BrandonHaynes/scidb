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
 * @file
 *
 * @brief Storage implementation
 *
 * @author Konstantin Knizhnik <knizhnik@garret.ru>
 * @author poliocough@gmail.com
 * @author sfridella@paradigm4.com
 */


#include <sys/time.h>
#include <inttypes.h>
#include <map>
#include <boost/unordered_set.hpp>
#include <boost/tuple/tuple.hpp>
#include <boost/tuple/tuple_comparison.hpp>
#include <log4cxx/logger.h>
#include <network/NetworkManager.h>
#include <network/BaseConnection.h>
#include <network/MessageUtils.h>
#include <array/Metadata.h>
#include <query/Statistics.h>
#include <query/Operator.h>
#include <boost/make_shared.hpp>
#include <util/FileIO.h>
#include <query/ops/list/ListArrayBuilder.h>
#include <system/Cluster.h>
#include <system/Utils.h>
#include <system/Config.h>
#include <system/SciDBConfigOptions.h>
#include <system/Exceptions.h>
#include <system/SystemCatalog.h>
#include <util/Platform.h>
#include <array/TileIteratorAdaptors.h>
#include <smgr/io/InternalStorage.h>

namespace scidb
{

using namespace boost;
using namespace std;

///////////////////////////////////////////////////////////////////
/// Constants and #defines
///////////////////////////////////////////////////////////////////

static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.smgr"));

const size_t DEFAULT_TRANS_LOG_LIMIT = 1024; // default limit of transaction log file (in mebibytes)
const size_t MAX_CFG_LINE_LENGTH = 1*KiB;
const int MAX_REDUNDANCY = 8;
const int MAX_INSTANCE_BITS = 10; // 2^MAX_INSTANCE_BITS = max number of instances

///////////////////////////////////////////////////////////////////
/// Static helper functions
///////////////////////////////////////////////////////////////////

/**
 * Fibonacci hash for a 64 bit key
 * @param key to hash
 * @param fib_B = log2(max_num_of_buckets)
 * @return hash = bucket index
 */
static uint64_t fibHash64(const uint64_t key, const uint64_t fib_B)
{
    assert(fib_B < 64);
    const uint64_t fib_A64 = (uint64_t) 11400714819323198485U;
    return (key * fib_A64) >> (64 - fib_B);
}

inline static char* strtrim(char* buf)
{
    char* p = buf;
    char ch;
    while ((unsigned char) (ch = *p) <= ' ' && ch != '\0')
    {
        p += 1;
    }
    char* q = p + strlen(p);
    while (q > p && (unsigned char) q[-1] <= ' ')
    {
        q -= 1;
    }
    *q = '\0';
    return p;
}

inline static string relativePath(const string& dir, const string& file)
{
    return file[0] == '/' ? file : dir + file;
}

inline static double getTimeSecs()
{
    struct timeval tv;
    gettimeofday(&tv, 0);
    return (((double) tv.tv_sec) * 1000000 + ((double) tv.tv_usec)) / 1000000;
}

static void collectArraysToRollback(boost::shared_ptr<std::map<ArrayID, VersionID> >& arrsToRollback, const VersionID& lastVersion,
                                    const ArrayID& baseArrayId, const ArrayID& newArrayId)
{
    assert(arrsToRollback);
    assert(baseArrayId>0);
    (*arrsToRollback.get())[baseArrayId] = lastVersion;
}

VersionControl* VersionControl::instance;

///////////////////////////////////////////////////////////////////
/// ChunkInitializer
///////////////////////////////////////////////////////////////////

CachedStorage::ChunkInitializer::~ChunkInitializer()
{
    ScopedMutexLock cs(storage._mutex);
    storage.notifyChunkReady(chunk);
}

///////////////////////////////////////////////////////////////////
/// CachedStorage class
///////////////////////////////////////////////////////////////////

/* Constructor
 */
CachedStorage::CachedStorage() :
    _replicationManager(NULL)
{}

/* Types needed to track overlapping chunks
 */
typedef tuple<DataStore::Guid, uint64_t> CloneOffset;
struct CloneHash {
    size_t operator() (const CloneOffset clof) const {
        hash<uint64_t> myhash;
        return myhash(clof.get<0>()) + myhash(clof.get<1>());
    }
};

/* Initialize/read the Storage Description file on startup
 */
void
CachedStorage::initStorageDescriptionFile(const std::string& storageDescriptorFilePath)
{
    StatisticsScope sScope;
    InjectedErrorListener<WriteChunkInjectedError>::start();
    char buf[MAX_CFG_LINE_LENGTH];
    char const* descPath = storageDescriptorFilePath.c_str();
    size_t pathEnd = storageDescriptorFilePath.find_last_of('/');
    _databasePath = "";
    if (pathEnd != string::npos)
    {
        _databasePath = storageDescriptorFilePath.substr(0, pathEnd + 1);
    }
    FILE* f = fopen(descPath, "r");
    if (f == NULL)
    {
        f = fopen(descPath, "w");
        if (!f)
            throw SYSTEM_EXCEPTION(SCIDB_SE_STORAGE, SCIDB_LE_CANT_OPEN_FILE) << descPath << ferror(f);
        size_t fileNameBeg = (pathEnd == string::npos) ? 0 : pathEnd + 1;
        size_t fileNameEnd = storageDescriptorFilePath.find_last_of('.');
        if (fileNameEnd == string::npos || fileNameEnd < fileNameBeg)
        {
            fileNameEnd = storageDescriptorFilePath.size();
        }
        string databaseName = storageDescriptorFilePath.substr(fileNameBeg, fileNameEnd - fileNameBeg);
        _databaseHeader = _databasePath + databaseName + ".header";
        _databaseLog = _databasePath + databaseName + ".log";
        fprintf(f, "%s.header\n", databaseName.c_str());
        fprintf(f, "%ld %s.log\n", (long) DEFAULT_TRANS_LOG_LIMIT, databaseName.c_str());
        _logSizeLimit = (uint64_t) DEFAULT_TRANS_LOG_LIMIT * MiB;
    }
    else
    {
        int pos;
        long sizeMb;
        if (!fgets(buf, sizeof buf, f))
            throw SYSTEM_EXCEPTION(SCIDB_SE_STORAGE, SCIDB_LE_STORAGE_DESCRIPTOR_INVALID_FORMAT);
        _databaseHeader = relativePath(_databasePath, strtrim(buf));
        if (!fgets(buf, sizeof buf, f))
            throw SYSTEM_EXCEPTION(SCIDB_SE_STORAGE, SCIDB_LE_STORAGE_DESCRIPTOR_INVALID_FORMAT);
        if (sscanf(buf, "%ld%n", &sizeMb, &pos) != 1)
            throw SYSTEM_EXCEPTION(SCIDB_SE_STORAGE, SCIDB_LE_STORAGE_DESCRIPTOR_INVALID_FORMAT);
        _databaseLog = relativePath(_databasePath, strtrim(buf + pos));
        _logSizeLimit = (uint64_t) sizeMb * MiB;
    }
    fclose(f);
}

/* Initialize the chunk map from on-disk store
 */
void
CachedStorage::initChunkMap()
{
    LOG4CXX_TRACE(logger, "smgr open:  reading chunk map, nchunks " << _hdr.nChunks);

    _redundancy = Config::getInstance()->getOption<int> (CONFIG_REDUNDANCY);
    _syncReplication = !Config::getInstance()->getOption<bool> (CONFIG_ASYNC_REPLICATION);

    ChunkDescriptor desc;
    uint64_t chunkPos = HEADER_SIZE;
    StorageAddress addr;
    unordered_set<CloneOffset, CloneHash> clones;
    set<ArrayID> removedArrays;
    typedef map<ArrayID, ArrayID> ArrayMap;
    ArrayMap oldestVersions;
    typedef map<ArrayID, boost::shared_ptr<ArrayDesc> > ArrayDescCache;
    ArrayDescCache existentArrays;

    for (size_t i = 0; i < _hdr.nChunks; i++, chunkPos += sizeof(ChunkDescriptor))
    {
        size_t rc = _hd->read(&desc, sizeof(ChunkDescriptor), chunkPos);
        if (rc != sizeof(ChunkDescriptor))
        {
            LOG4CXX_ERROR(logger, "Inconsistency in storage header: rc="
                          << rc << ", chunkPos="
                          << chunkPos << ", i="
                          << i << ", hdr.nChunks="
                          << _hdr.nChunks << ", hdr.currPos="
                          << _hdr.currPos);
            _hdr.currPos = chunkPos;
            _hdr.nChunks = i;
            break;
        }
        if (desc.hdr.pos.hdrPos != chunkPos)
        {
            LOG4CXX_ERROR(logger, "Invalid chunk header " << i << " at position " << chunkPos
                          << " desc.hdr.pos.hdrPos=" << desc.hdr.pos.hdrPos
                          << " arrayID=" << desc.hdr.arrId
                          << " hdr.nChunks=" << _hdr.nChunks);
            _freeHeaders.insert(chunkPos);
        }
        else
        {
            assert(desc.hdr.nCoordinates < MAX_NUM_DIMS_SUPPORTED);

            LOG4CXX_TRACE(logger, "smgr open:  found chunk desc " << desc.toString());

            if (desc.hdr.arrId != 0)
            {
                /* Check if unversioned array exists
                 */
                ArrayDescCache::iterator it = existentArrays.find(desc.hdr.pos.dsGuid);
                if (it == existentArrays.end())
                {
                    if (removedArrays.count(desc.hdr.pos.dsGuid) == 0)
                    {
                        try
                        {
                            boost::shared_ptr<ArrayDesc> ad =
                                SystemCatalog::getInstance()->getArrayDesc(desc.hdr.pos.dsGuid);
                            it = existentArrays.insert(
                                ArrayDescCache::value_type(desc.hdr.pos.dsGuid, ad)
                                ).first;
                        }
                        catch (SystemException const& x)
                        {
                            if (x.getLongErrorCode() == SCIDB_LE_ARRAYID_DOESNT_EXIST)
                            {
                                /* Try to remove the datastore if it is there
                                 */
                                _datastores.closeDataStore(desc.hdr.pos.dsGuid,
                                                           true /* remove from disk */);
                                removedArrays.insert(desc.hdr.pos.dsGuid);
                            }
                            else
                            {
                                throw x;
                            }
                        }
                    }
                }

                /* If the unversioned array does not exist... wipe the chunk
                 */
                if (it == existentArrays.end())
                {
                    desc.hdr.arrId = 0;
                    LOG4CXX_TRACE(logger, "ChunkDesc: Remove chunk descriptor for non-existent "
                                  << "array at position " << chunkPos);
                    _hd->writeAll(&desc.hdr, sizeof(ChunkHeader), chunkPos);
                    assert(desc.hdr.nCoordinates < MAX_NUM_DIMS_SUPPORTED);
                    _freeHeaders.insert(chunkPos);
                    continue;
                }

                /* Else add chunk to map (if it is live)
                 */
                else
                {
                    /* Init array descriptor
                     */
                    ArrayDesc& adesc = *it->second;
                    assert(adesc.getUAId() == desc.hdr.pos.dsGuid);

                    /* Find/init the inner chunk map
                     */
                    ChunkMap::iterator iter = _chunkMap.find(adesc.getUAId());
                    if (iter == _chunkMap.end())
                    {
                        iter = _chunkMap.insert(make_pair(adesc.getUAId(),
                                                          make_shared <InnerChunkMap> ())).first;
                    }
                    shared_ptr<InnerChunkMap>& innerMap = iter->second;

                    /* Find the oldest version of array, and the storage address
                       of the chunk currently in use by this version
                    */
                    ArrayMap::iterator oldest_it = oldestVersions.find(adesc.getUAId());
                    if (oldest_it == oldestVersions.end())
                    {
                        oldestVersions[adesc.getUAId()] =
                            SystemCatalog::getInstance()->getOldestArrayVersion(adesc.getUAId());
                    }
                    desc.getAddress(addr);
                    StorageAddress oldestVersionAddr = addr;
                    oldestVersionAddr.arrId = oldestVersions[adesc.getUAId()];
                    StorageAddress oldestLiveChunkAddr;
                    InnerChunkMap::iterator oldestLiveChunk =
                        innerMap->lower_bound(oldestVersionAddr);
                    if (oldestLiveChunk == innerMap->end() ||
                        oldestLiveChunk->first.coords != oldestVersionAddr.coords ||
                        oldestLiveChunk->first.attId != oldestVersionAddr.attId)
                    {
                        oldestLiveChunkAddr = oldestVersionAddr;
                        oldestLiveChunkAddr.arrId = 0;
                    }
                    else
                    {
                        oldestLiveChunkAddr = oldestLiveChunk->first;
                    }

                    /* Chunk is live if and only if arrayID of chunk is > arrayID of chunk
                       currently pointed to by oldest version
                    */
                    if (desc.hdr.arrId > oldestLiveChunkAddr.arrId)
                    {
                        /* Chunk is live, put it in the map
                         */
                        shared_ptr<PersistentChunk>& chunk =(*innerMap)[addr].getChunk();
                        ASSERT_EXCEPTION((!chunk), "smgr open: NOT unique chunk");
                        if (!desc.hdr.is<ChunkHeader::TOMBSTONE>())
                        {
                            chunk.reset(new PersistentChunk());
                            chunk->setAddress(adesc, desc);
                            bool isUnique =
                                clones.insert(make_tuple(chunk->_hdr.pos.dsGuid,
                                                         chunk->_hdr.pos.offs)).second;
                            if (!isUnique) {
                                LOG4CXX_ERROR(logger, "smgr open: NOT unique chunk adesc= " << adesc
                                              << ", desc="<<desc.toString()
                                              << ", _hdr.pos="<<chunk->_hdr.pos.toString());
                                assert(false);
                                throw SYSTEM_EXCEPTION(SCIDB_SE_STORAGE,
                                                       SCIDB_LE_DATABASE_HEADER_CORRUPTED);
                            }
                        }
                        else
                        {
                            (*innerMap)[addr].setTombstonePos(desc.hdr.pos.hdrPos);
                        }

                        /* Now check if by inserting this chunk we made the previous one dead...
                         */
                        if (oldestLiveChunkAddr.arrId &&
                            desc.hdr.arrId <= oldestVersionAddr.arrId)
                        {
                            /* The oldestLiveChunk is now dead... wipe it out
                             */
                            shared_ptr<DataStore> ds =
                                _datastores.getDataStore(desc.hdr.pos.dsGuid);
                            markChunkAsFree(oldestLiveChunk->second, ds);
                            innerMap->erase(oldestLiveChunk);
                        }
                    }
                    else
                    {
                        /* Chunk is dead, wipe it out
                         */
                        shared_ptr<DataStore> ds =
                            _datastores.getDataStore(desc.hdr.pos.dsGuid);
                        desc.hdr.arrId = 0;
                        LOG4CXX_TRACE(logger, "ChunkDesc: Remove chunk descriptor for non-existent "
                                      << "array at position " << chunkPos);
                        _hd->writeAll(&desc.hdr, sizeof(ChunkHeader), chunkPos);
                        assert(desc.hdr.nCoordinates < MAX_NUM_DIMS_SUPPORTED);
                        _freeHeaders.insert(chunkPos);
                        ds->freeChunk(desc.hdr.pos.offs, desc.hdr.allocatedSize);
                    }
                }
            }
            else
            {
                _freeHeaders.insert(chunkPos);
            }
        }
    }

    if (chunkPos != _hdr.currPos)
    {
        LOG4CXX_ERROR(logger, "Storage header is not consistent: " << chunkPos << " vs. " << _hdr.currPos);
        // throw SYSTEM_EXCEPTION(SCIDB_SE_STORAGE, SCIDB_LE_DATABASE_HEADER_CORRUPTED);
        if (chunkPos > _hdr.currPos)
        {
            _hdr.currPos = chunkPos;
        }
    }
}

/* Read the storage description file to find path for chunk map file.
   Iterate the chunk map file and build the chunk map in memory.
 */
void
CachedStorage::open(const string& storageDescriptorFilePath, size_t cacheSizeBytes)
{
    /* read/create the storage description file
     */
    initStorageDescriptionFile(storageDescriptorFilePath);

    /* init cache
     */
    _cacheSize = cacheSizeBytes;
    _compressors = CompressorFactory::getInstance().getCompressors();
    _cacheUsed = 0;
    _strictCacheLimit = Config::getInstance()->getOption<bool> (CONFIG_STRICT_CACHE_LIMIT);
    _cacheOverflowFlag = false;
    _timestamp = 1;
    _lru.prune();

    /* Open metadata (chunk map) file and transcation log file
     */
    int flags = O_LARGEFILE | O_RDWR | O_CREAT;
    _hd = FileManager::getInstance()->openFileObj(_databaseHeader.c_str(), flags);
    if (!_hd) {
        throw SYSTEM_EXCEPTION(SCIDB_SE_STORAGE, SCIDB_LE_CANT_OPEN_FILE) <<
            _databaseHeader << ::strerror(errno) << errno;
    }

    struct flock flc;
    flc.l_type = F_WRLCK;
    flc.l_whence = SEEK_SET;
    flc.l_start = 0;
    flc.l_len = 1;

    if (_hd->fsetlock(&flc))
        throw SYSTEM_EXCEPTION(SCIDB_SE_STORAGE, SCIDB_LE_CANT_LOCK_DATABASE);

    _log[0] = FileManager::getInstance()->openFileObj((_databaseLog + "_1").c_str(),
                                                      O_LARGEFILE | O_SYNC | O_RDWR | O_CREAT);
    if (!_log[0]) {
        throw SYSTEM_EXCEPTION(SCIDB_SE_STORAGE, SCIDB_LE_CANT_OPEN_FILE) <<
            (_databaseLog + "_1") << ::strerror(errno) << errno;
    }

    _log[1] = FileManager::getInstance()->openFileObj((_databaseLog + "_2").c_str(),
                                                      O_LARGEFILE | O_SYNC | O_RDWR | O_CREAT);
    if (!_log[1]) {
        throw SYSTEM_EXCEPTION(SCIDB_SE_STORAGE, SCIDB_LE_CANT_OPEN_FILE) <<
            (_databaseLog + "_2") << ::strerror(errno) << errno;
    }

    _logSize = 0;
    _currLog = 0;

    /* Initialize the data stores
     */
    string dataStoresBase = _databasePath + "/datastores";
    _datastores.initDataStores(dataStoresBase.c_str());

    /* Read/initialize metadata header
     */
    size_t rc = _hd->read(&_hdr, sizeof(_hdr), 0);
    if (rc != 0 && rc != sizeof(_hdr)) {
        throw SYSTEM_EXCEPTION(SCIDB_SE_STORAGE, SCIDB_LE_OPERATION_FAILED_WITH_ERRNO)
            << "read" << ::strerror(errno) << errno;
    }

    _writeLogThreshold = Config::getInstance()->getOption<int> (CONFIG_IO_LOG_THRESHOLD);
    _enableDeltaEncoding = Config::getInstance()->getOption<bool> (CONFIG_ENABLE_DELTA_ENCODING);
    _nInstances = SystemCatalog::getInstance()->getNumberOfInstances();
    _redundancy = 0; // disable replication during rollback: each instance is perfroming rollback locally

    if (rc == 0 || (_hdr.magic == SCIDB_STORAGE_HEADER_MAGIC && _hdr.currPos < HEADER_SIZE))
    {
        LOG4CXX_TRACE(logger, "smgr open:  initializing storage header");

        /* Database is not initialized
         */
        ::memset(&_hdr, 0, sizeof(_hdr));
        _hdr.magic = SCIDB_STORAGE_HEADER_MAGIC;
        _hdr.versionLowerBound = SCIDB_STORAGE_FORMAT_VERSION;
        _hdr.versionUpperBound = SCIDB_STORAGE_FORMAT_VERSION;
        _hdr.currPos = HEADER_SIZE;
        _hdr.instanceId = INVALID_INSTANCE;
        _hdr.nChunks = 0;
    }
    else
    {
        LOG4CXX_TRACE(logger, "smgr open:  openinging storage header");

        /* Check for corrupted metadata file
         */
        if (_hdr.magic != SCIDB_STORAGE_HEADER_MAGIC)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_STORAGE, SCIDB_LE_INVALID_STORAGE_HEADER);
        }

        /* At the moment, both upper and lower bound versions in the file must equal to the
           current version in the code.
         */
        if (_hdr.versionLowerBound != SCIDB_STORAGE_FORMAT_VERSION ||
            _hdr.versionUpperBound != SCIDB_STORAGE_FORMAT_VERSION)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_STORAGE, SCIDB_LE_MISMATCHED_STORAGE_FORMAT_VERSION)
                  << _hdr.versionLowerBound
                  << _hdr.versionUpperBound
                  << SCIDB_STORAGE_FORMAT_VERSION;
        }

        /* Rollback uncommitted changes
         */
        doTxnRecoveryOnStartup();

        /* Database is initialized: read information about all locally available chunks in map
         */
        initChunkMap();

        /* Flush the datastores to capture freelist changes
         */
        _datastores.flushAllDataStores();
    }

    /* Start replication manager
     */
    _replicationManager = ReplicationManager::getInstance();
    assert(_replicationManager);
    assert(_replicationManager->isStarted());
}


/* Cleanup and close smgr
 */
void
CachedStorage::close()
{
    InjectedErrorListener<WriteChunkInjectedError>::stop();

    for (ChunkMap::iterator i = _chunkMap.begin(); i != _chunkMap.end(); ++i)
    {
        shared_ptr<InnerChunkMap> & innerMap = i->second;
        for (InnerChunkMap::iterator j = innerMap->begin(); j != innerMap->end(); ++j)
        {
            if (j->second.getChunk() && j->second.getChunk()->_accessCount != 0)
                throw SYSTEM_EXCEPTION(SCIDB_SE_STORAGE, SCIDB_LE_PIN_UNPIN_DISBALANCE);
        }
    }
    _chunkMap.clear();

    _hd.reset();
    _log[0].reset();
    _log[1].reset();
}

void CachedStorage::notifyChunkReady(PersistentChunk& chunk)
{
    // This method is invoked with storage mutex locked
    chunk._raw = false;
    if (chunk._waiting)
    {
        chunk._waiting = false;
        _loadEvent.signal(); // wakeup all threads waiting for this chunk
    }
}

void CachedStorage::pinChunk(PersistentChunk const* aChunk)
{
    ScopedMutexLock cs(_mutex);
    PersistentChunk& chunk = *const_cast<PersistentChunk*>(aChunk);
    LOG4CXX_TRACE(logger, "CachedStorage::pinChunk =" << &chunk << ", accessCount = "<<chunk._accessCount);
    chunk.beginAccess();
}

void CachedStorage::unpinChunk(PersistentChunk const* aChunk)
{
    ScopedMutexLock cs(_mutex);
    PersistentChunk& chunk = *const_cast<PersistentChunk*>(aChunk);
    LOG4CXX_TRACE(logger, "CachedStorage::unpinChunk =" << &chunk << ", accessCount = "<<chunk._accessCount);
    assert(chunk._accessCount > 0);
    if (--chunk._accessCount == 0)
    {
        // Chunk is not accessed any more by any thread, unpin it and include in LRU list
        _lru.link(&chunk);
    }
}

void CachedStorage::addChunkToCache(PersistentChunk& chunk)
{
    // Check amount of memory used by cached chunks and discard least recently used
    // chunks from the cache
    _mutex.checkForDeadlock();
    while (_cacheUsed + chunk.getSize() > _cacheSize)
    {
        if (_lru.isEmpty())
        {
            if (_strictCacheLimit && _cacheUsed != 0)
            {
                Event::ErrorChecker noopEc;
                _cacheOverflowFlag = true;
                _cacheOverflowEvent.wait(_mutex, noopEc);
            }
            else
            {
                break;
            }
        }
        internalFreeChunk(*_lru._prev);
    }

    LOG4CXX_TRACE(logger, "CachedStorage::addChunkToCache chunk=" << &chunk
                      << ", size = "<< chunk.getSize() << ", accessCount = "<<chunk._accessCount
                      << ", cacheUsed="<<_cacheUsed);

    _cacheUsed += chunk.getSize();
}

boost::shared_ptr<PersistentChunk>
CachedStorage::lookupChunk(ArrayDesc const& desc, StorageAddress const& addr)
{
    ScopedMutexLock cs(_mutex);
    ChunkMap::iterator iter = _chunkMap.find(desc.getUAId());
    if (iter != _chunkMap.end())
    {
        shared_ptr<InnerChunkMap>& innerMap = iter->second;
        InnerChunkMap::iterator innerIter = innerMap->find(addr);
        if (innerIter != innerMap->end())
        {
            shared_ptr<PersistentChunk>& chunk = innerIter->second.getChunk();
            if (chunk)
            {
                chunk->beginAccess();
                return chunk;
            }
        }
    }
    boost::shared_ptr<PersistentChunk> emptyChunk;
    return emptyChunk;
}

void CachedStorage::decompressChunk(ArrayDesc const& desc, PersistentChunk* chunk, CompressedBuffer const& buf)
{
    chunk->allocate(buf.getDecompressedSize());

    DBArrayChunkInternal intChunk(desc, chunk);
    if (buf.getSize() != buf.getDecompressedSize())
    {
        _compressors[buf.getCompressionMethod()]->decompress(buf.getData(), buf.getSize(), intChunk);
    }
    else
    {
        assert(chunk->getHeader().pos.hdrPos == 0);
        memcpy(intChunk.getDataForLoad(), buf.getData(), buf.getSize());
    }
}

void CachedStorage::compressChunk(ArrayDesc const& desc, PersistentChunk const* aChunk, CompressedBuffer& buf)
{
    assert(aChunk);
    PersistentChunk& chunk = *const_cast<PersistentChunk*>(aChunk);
    shared_ptr<DataStore> ds = _datastores.getDataStore(desc.getUAId());
    int compressionMethod = chunk.getCompressionMethod();
    if (compressionMethod < 0) {
        throw USER_EXCEPTION(SCIDB_SE_STORAGE, SCIDB_LE_COMPRESS_METHOD_NOT_DEFINED);
    }
    buf.setDecompressedSize(chunk.getSize());
    buf.setCompressionMethod(compressionMethod);
    {
        ScopedMutexLock cs(_mutex);
        if (!chunk.isRaw() && chunk._data != NULL)
        {
            PersistentChunk::Pinner scope(&chunk);
            buf.allocate(chunk.getCompressedSize() != 0 ? chunk.getCompressedSize() : chunk.getSize());
            DBArrayChunkInternal intChunk(desc, &chunk);
            size_t compressedSize = _compressors[compressionMethod]->compress(buf.getData(), intChunk);
            if (compressedSize == chunk.getSize())
            {
                memcpy(buf.getData(), chunk._data, compressedSize);
            }
            else if (compressedSize != buf.getSize())
            {
                buf.reallocate(compressedSize);
            }
        }
    }

    if (buf.getData() == NULL)
    { // chunk data is not present in the cache so read compressed data from the disk
        if (aChunk->_hdr.pos.hdrPos == 0)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_STORAGE, SCIDB_LE_ACCESS_TO_RAW_CHUNK) << aChunk->getHeader().arrId;
        }
        buf.allocate(aChunk->getCompressedSize());
        readChunkFromDataStore(*ds, *aChunk, buf.getData());
    }
}

inline bool CachedStorage::isResponsibleFor(ArrayDesc const& desc,
                                            PersistentChunk const& chunk,
                                            boost::shared_ptr<Query> const& query)
{
    ScopedMutexLock cs(_mutex);
    Query::validateQueryPtr(query);
    assert(chunk._hdr.instanceId < size_t(_nInstances));

    if (chunk._hdr.instanceId == _hdr.instanceId)
    {
        return true;
    }
    if (!query->isPhysicalInstanceDead(chunk._hdr.instanceId))
    {
        return false;
    }
    if (_redundancy == 1)
    {
        return true;
    }
    InstanceID replicas[MAX_REDUNDANCY + 1];
    getReplicasInstanceId(replicas, desc, chunk.getAddress());
    for (int i = 1; i <= _redundancy; i++)
    {
        if (replicas[i] == _hdr.instanceId)
        {
            return true;
        }
        if (!query->isPhysicalInstanceDead(replicas[i]))
        {
            // instance with this replica is alive
            return false;
        }
    }
    return false;
}

boost::shared_ptr<PersistentChunk> CachedStorage::createChunk(ArrayDesc const& desc,
                                                              StorageAddress const& addr,
                                                              int compressionMethod,
                                                              const boost::shared_ptr<Query>& query)
{
    ScopedMutexLock cs(_mutex);
    Query::validateQueryPtr(query);

    assert(desc.getUAId()!=0);
    ChunkMap::iterator iter = _chunkMap.find(desc.getUAId());
    if (iter == _chunkMap.end())
    {
        iter = _chunkMap.insert(make_pair(desc.getUAId(), make_shared <InnerChunkMap> ())).first;
    }
    else if (iter->second->find(addr) != iter->second->end())
    {
        throw SYSTEM_EXCEPTION(SCIDB_SE_STORAGE, SCIDB_LE_CHUNK_ALREADY_EXISTS)
        << CoordsToStr(addr.coords);
    }

    shared_ptr<PersistentChunk>& chunk = (*(iter->second))[addr].getChunk();
    chunk.reset(new PersistentChunk());
    chunk->setAddress(desc, addr, compressionMethod);
    LOG4CXX_TRACE(logger, "CachedStorage::createChunk =" << chunk.get() << ", accessCount = "<<chunk->_accessCount);
    chunk->_accessCount = 1; // newly created chunk is pinned
    chunk->_timestamp = ++_timestamp;
    return chunk;
}

void CachedStorage::deleteChunk(ArrayDesc const& desc, PersistentChunk& victim)
{
    ScopedMutexLock cs(_mutex);

    ChunkMap::const_iterator iter = _chunkMap.find(desc.getUAId());
    if (iter != _chunkMap.end())
    {
        iter->second->erase(victim._addr);
    }
}

void CachedStorage::freeChunk(PersistentChunk* victim)
{
    ScopedMutexLock cs(_mutex);
    internalFreeChunk(*victim);
}

void CachedStorage::internalFreeChunk(PersistentChunk& victim)
{
    if (victim._data != NULL && victim._hdr.pos.hdrPos != 0)
    {
        LOG4CXX_TRACE(logger, "CachedStorage::internalFreeChunk chunk=" << &victim
                      << ", size = "<< victim.getSize() << ", accessCount = "<<victim._accessCount
                      << ", cacheUsed="<<_cacheUsed);

        _cacheUsed -= victim.getSize();
        if (_cacheOverflowFlag)
        {
            _cacheOverflowFlag = false;
            _cacheOverflowEvent.signal();
        }
    }
    if (victim._next != NULL)
    {
        victim.unlink();
    }
    victim.free();
}

/* Remove all versions prior to arrId from the unversioned array uaId.
   If arrId is zero, remove everything
 */
void CachedStorage::removeVersions(QueryID queryId,
                                   ArrayUAID uaId,
                                   ArrayID lastLiveArrId)
{
    ScopedMutexLock cs(_mutex);
    shared_ptr<InnerChunkMap> innerMap;
    ChunkMap::const_iterator iter = _chunkMap.find(uaId);
    if (iter == _chunkMap.end())
    {
        return;
    }
    innerMap = iter->second;

    shared_ptr<DataStore> ds = _datastores.getDataStore(uaId);
    set<StorageAddress> victims;
    StorageAddress currentChunkAddr;
    bool currentChunkIsLive = true;
    for (InnerChunkMap::iterator i = innerMap->begin(); i != innerMap->end(); ++i)
    {
        StorageAddress const& address = i->first;

        /* If lastLiveArrId is non-zero, we must determine if the chunk is live.
           If lastLiveArrId is zero, then we proceed immediately to remove chunk.
        */
        if (lastLiveArrId)
        {
            if (!address.sameBaseAddr(currentChunkAddr))
            {
                /* Move on to next coordinate
                 */
                currentChunkAddr = address;
                currentChunkIsLive = true;
            }
            if (address.arrId > lastLiveArrId)
            {
                /* Chunk was added after oldest version
                   so it is still live
                */
                continue;
            }
            else if (address.arrId == lastLiveArrId)
            {
                /* Chunk was added in oldest version so it is
                   still live, but any older chunks are not
                */
                currentChunkIsLive = false;
                continue;
            }
            else if (address.arrId < lastLiveArrId)
            {
                /* Chunk was added prior to oldest version
                 */
                if (currentChunkIsLive)
                {
                    /* Chunk is still live, but older chunks are not
                     */
                    currentChunkIsLive = false;
                        continue;
                }
            }
        }

        /* Chunk should be removed
         */
        markChunkAsFree(i->second, ds);
        victims.insert(address);
    }
    _hd->writeAll(&_hdr, HEADER_SIZE, 0);
    for(set<StorageAddress>::iterator i = victims.begin(); i != victims.end(); ++i)
    {
        StorageAddress const& address = *i;
        innerMap->erase(address);
    }
    flush(uaId);
    if (!lastLiveArrId)
    {
        assert(innerMap->size() == 0);
        _chunkMap.erase(uaId);
        _datastores.closeDataStore(uaId, true /* remove from disk */);
    }
}

void CachedStorage::removeVersionFromMemory(ArrayUAID uaId, ArrayID arrId)
{
    ScopedMutexLock cs(_mutex);
    shared_ptr<InnerChunkMap> innerMap;
    ChunkMap::const_iterator iter = _chunkMap.find(uaId);
    if (iter == _chunkMap.end())
    {
        return;
    }
    else
    {
        innerMap = iter->second;
    }
    vector<StorageAddress> victims;
    for (InnerChunkMap::iterator i = innerMap->begin(); i != innerMap->end(); ++i)
    {
        StorageAddress const& addr = i->first;
        if (addr.arrId != arrId)
        {
            continue;
        }
        victims.push_back(addr);
    }
    for(vector<StorageAddress>::iterator i = victims.begin(); i != victims.end(); ++i)
    {
       StorageAddress const& address = *i;
       innerMap->erase(address);
    }
    if (innerMap->size() == 0)
    {
       _chunkMap.erase(uaId);
    }
}

InstanceID CachedStorage::getPrimaryInstanceId(ArrayDesc const& desc, StorageAddress const& address) const
{
    //in this context we have to be careful to use nInstances which was set at the beginning of system lifetime
    //this method must return the same value regardless of whether or not there were failures
    return desc.getHashedChunkNumber(address.coords) % _nInstances;
}

void CachedStorage::getReplicasInstanceId(InstanceID* replicas, ArrayDesc const& desc, StorageAddress const& address) const
{
    replicas[0] = getPrimaryInstanceId(desc, address);
    for (int i = 0; i < _redundancy; i++)
    {
        // A prime number can be used to smear the replicas as follows
        // InstanceID instanceId = (chunk.getArrayDesc().getHashedChunkNumber(chunk.addr.coords) + (i+1)) % PRIME_NUMBER % nInstances;
        // the PRIME_NUMBER needs to be just shy of the number of instances to work, so we would need a table.
        // For Fibonacci no table is required, and it seems to work OK.

        const uint64_t nReplicas = (_redundancy + 1);
        const uint64_t currReplica = (i + 1);
        const uint64_t chunkId = desc.getHashedChunkNumber(address.coords) * (nReplicas) + currReplica;
        InstanceID instanceId = fibHash64(chunkId, MAX_INSTANCE_BITS) % _nInstances;
        for (int j = 0; j <= i; j++)
        {
            if (replicas[j] == instanceId)
            {
                instanceId = (instanceId + 1) % _nInstances;
                j = -1;
            }
        }
        replicas[i + 1] = instanceId;
    }
}

void CachedStorage::replicate(ArrayDesc const& desc,
                              StorageAddress const& addr,
                              PersistentChunk* chunk,
                              void const* data,
                              size_t compressedSize,
                              size_t decompressedSize,
                              shared_ptr<Query> const& query,
                              vector<shared_ptr<ReplicationManager::Item> >& replicasVec)
{
    ScopedMutexLock cs(_mutex);
    Query::validateQueryPtr(query);

    if (_redundancy <= 0 || (chunk && !isPrimaryReplica(chunk)))
    { // self chunk
        return;
    }
    replicasVec.reserve(_redundancy);
    InstanceID replicas[MAX_REDUNDANCY + 1];
    getReplicasInstanceId(replicas, desc, addr);

    QueryID queryId = query->getQueryID();
    assert(queryId != 0);
    boost::shared_ptr<MessageDesc> chunkMsg;
    if (chunk && data)
    {
        boost::shared_ptr<CompressedBuffer> buffer = boost::make_shared<CompressedBuffer>();
        buffer->allocate(compressedSize);
        memcpy(buffer->getData(), data, compressedSize);
        chunkMsg = boost::make_shared<MessageDesc>(mtChunkReplica, buffer);
    }
    else
    {
        chunkMsg = boost::make_shared<MessageDesc>(mtChunkReplica);
    }
    chunkMsg->setQueryID(queryId);
    boost::shared_ptr<scidb_msg::Chunk> chunkRecord = chunkMsg->getRecord<scidb_msg::Chunk> ();
    chunkRecord->set_attribute_id(addr.attId);
    chunkRecord->set_array_id(addr.arrId);
    for (size_t k = 0; k < addr.coords.size(); k++)
    {
        chunkRecord->add_coordinates(addr.coords[k]);
    }
    chunkRecord->set_eof(false);

    if(chunk)
    {
        chunkRecord->set_compression_method(chunk->getCompressionMethod());
        chunkRecord->set_decompressed_size(decompressedSize);
        chunkRecord->set_count(0);
        LOG4CXX_TRACE(logger, "Replicate chunk of array ID=" << addr.arrId << " attribute ID=" << addr.attId);
        assert(data != NULL); //TODO: need an exception ?
    }
    else
    {
        chunkRecord->set_tombstone(true);
    }

    for (int i = 1; i <= _redundancy; i++)
    {
        boost::shared_ptr<ReplicationManager::Item> item = make_shared <ReplicationManager::Item>(replicas[i], chunkMsg, query);
        assert(_replicationManager);
        _replicationManager->send(item);
        replicasVec.push_back(item);
    }
}

void CachedStorage::abortReplicas(vector<boost::shared_ptr<ReplicationManager::Item> >* replicasVec)
{
    assert(replicasVec);
    for (size_t i = 0; i < replicasVec->size(); ++i)
    {
        const boost::shared_ptr<ReplicationManager::Item>& item = (*replicasVec)[i];
        assert(_replicationManager);
        _replicationManager->abort(item);
        assert(item->isDone());
    }
}

void CachedStorage::waitForReplicas(vector<boost::shared_ptr<ReplicationManager::Item> >& replicasVec)
{
    // _mutex must NOT be locked
    for (size_t i = 0; i < replicasVec.size(); ++i)
    {
        const boost::shared_ptr<ReplicationManager::Item>& item = replicasVec[i];
        assert(_replicationManager);
        _replicationManager->wait(item);
        assert(item->isDone());
        assert(item->validate(false));
    }
}

/* Write bytes to DataStore indicated by pos
 * @param pos DataStore and offset to which to write
 * @param data Bytes to write
 * @param len Number of bytes to write
 * @pre position in DataStore must be previously allocated
 * @throws userException if an error occurs
 */
void
CachedStorage::writeBytesToDataStore(DiskPos const& pos,
                                     void const* data,
                                     size_t len,
                                     size_t allocated)
{
    double t0 = 0, t1 = 0, writeTime = 0;
    shared_ptr<DataStore> ds = _datastores.getDataStore(pos.dsGuid);

    if (!ds)
    {
        throw USER_EXCEPTION(SCIDB_SE_STORAGE, SCIDB_LE_DATASTORE_NOT_FOUND);
    }

    if (_writeLogThreshold >= 0)
    {
        t0 = getTimeSecs();
    }
    ds->writeData(pos.offs, data, len, allocated);
    if (_writeLogThreshold >= 0)
    {
        t1 = getTimeSecs();
        writeTime = t1 - t0;
    }

    if (_writeLogThreshold >= 0 && writeTime * 1000 > _writeLogThreshold)
    {
        LOG4CXX_DEBUG(logger, "CWR: pwrite ds " << ds << " time " << writeTime);
    }
}

/* Force writing of chunk data to data store
   Exception is thrown if write failed
*/
void
CachedStorage::writeChunkToDataStore(DataStore& ds, PersistentChunk& chunk, void const* data)
{
    double t0 = 0, t1 = 0, writeTime = 0;

    if (_writeLogThreshold >= 0)
    {
        t0 = getTimeSecs();
    }
    ds.writeData(chunk._hdr.pos.offs,
                 data,
                 chunk._hdr.compressedSize,
                 chunk._hdr.allocatedSize);
    if (_writeLogThreshold >= 0)
    {
        t1 = getTimeSecs();
        writeTime = t1 - t0;
    }

    if (_writeLogThreshold >= 0 && writeTime * 1000 > _writeLogThreshold)
    {
        LOG4CXX_DEBUG(logger, "CWR: pwrite ds chunk "<< chunk.getHeader() <<" time "<< writeTime);
    }
}

/* Read chunk data from the disk
   Exception is thrown if read failed
*/
void
CachedStorage::readChunkFromDataStore(DataStore& ds, PersistentChunk const& chunk, void* data)
{
    double t0 = 0, t1 = 0, readTime = 0;
    if (_writeLogThreshold >= 0)
    {
        t0 = getTimeSecs();
    }
    ds.readData(chunk._hdr.pos.offs, data, chunk._hdr.compressedSize);
    if (_writeLogThreshold >= 0)
    {
        t1 = getTimeSecs();
        readTime = t1 - t0;
    }
    if (_writeLogThreshold >= 0 && readTime * 1000 > _writeLogThreshold)
    {
        LOG4CXX_DEBUG(logger, "CWR: pread ds chunk "<< chunk.getHeader() <<" time "<< readTime);
    }
}

RWLock& CachedStorage::getChunkLatch(PersistentChunk* chunk)
{
    return _latches[(size_t) chunk->_hdr.pos.offs % N_LATCHES];
}

void CachedStorage::getChunkPositions(ArrayDesc const& desc, boost::shared_ptr<Query> const& query, CoordinateSet& chunkPositions)
{
    StorageAddress readAddress (desc.getId(), 0, Coordinates());
    while(findNextChunk(desc, query, readAddress))
    {
        chunkPositions.insert(readAddress.coords);
    }
}

bool CachedStorage::findNextChunk(ArrayDesc const& desc,
                                  boost::shared_ptr<Query> const& query,
                                  StorageAddress& address)
{
    ScopedMutexLock cs(_mutex);
    assert(address.attId < desc.getAttributes().size() && address.arrId <= desc.getId());
    Query::validateQueryPtr(query);

    ChunkMap::iterator iter = _chunkMap.find(desc.getUAId());
    if (iter == _chunkMap.end())
    {
        address.coords.clear();
        return false;
    }
    shared_ptr<InnerChunkMap> const& innerMap = iter->second;
    if(address.coords.size())
    {
        address.coords[address.coords.size()-1] += desc.getDimensions()[desc.getDimensions().size() - 1].getChunkInterval();
    }
    address.arrId = desc.getId();
    InnerChunkMap::iterator innerIter = innerMap->lower_bound(address);
    while (true)
    {
        if (innerIter == innerMap->end() || innerIter->first.attId != address.attId)
        {
            address.coords.clear();
            return false;
        }
        if(innerIter->first.arrId <= desc.getId())
        {
            if(innerIter->second.getChunk() && isResponsibleFor( desc, *(innerIter->second.getChunk()), query))
            {
                address.arrId = innerIter->first.arrId;
                address.coords = innerIter->first.coords;
                return true;
            }
            else
            {
                address.arrId = desc.getId();
                address.coords = innerIter->first.coords;
                address.coords[address.coords.size()-1] += desc.getDimensions()[desc.getDimensions().size() - 1].getChunkInterval();
                innerIter = innerMap->lower_bound(address);
            }
        }
        while(innerIter != innerMap->end() && innerIter->first.arrId > address.arrId && innerIter->first.attId == address.attId)
        {
            ++innerIter;
        }
    }
}

bool CachedStorage::findChunk(ArrayDesc const& desc, boost::shared_ptr<Query> const& query, StorageAddress& address)
{
    ScopedMutexLock cs(_mutex);
    Query::validateQueryPtr(query);

    ChunkMap::iterator iter = _chunkMap.find(desc.getUAId());
    if (iter == _chunkMap.end())
    {
        address.coords.clear();
        return false;
    }
    shared_ptr<InnerChunkMap> const& innerMap = iter->second;
    address.arrId = desc.getId();
    InnerChunkMap::iterator innerIter = innerMap->lower_bound(address);
    if (innerIter == innerMap->end() || innerIter->first.coords != address.coords || innerIter->first.attId != address.attId)
    {
        address.coords.clear();
        return false;
    }

    assert(innerIter->first.arrId <= address.arrId && innerIter->first.coords == address.coords);
    // XXX empty query used? to represent what ? NID chunk ?
    if(innerIter->second.getChunk() && (!query || isResponsibleFor(desc, *(innerIter->second.getChunk()), query)))
    {
        address.arrId = innerIter->first.arrId;
        return true;
    }
    else
    {
        address.coords.clear();
        return false;
    }
}

void CachedStorage::cleanChunk(PersistentChunk* chunk)
{
    ScopedMutexLock cs(_mutex);
    LOG4CXX_TRACE(logger, "CachedStorage::cleanChunk =" << chunk << ", accessCount = "<<chunk->_accessCount);
    assert(chunk->_accessCount>0);
    --chunk->_accessCount;
    // Free the chunk regardless of _accessCount to avoid incorrect
    // _cacheUsed accounting done in internalFreeChunk()
    // (_accessCount can be >1 because we are double pinning sometimes,
    // e.g. in ArrayIterator::newChunk & ChunkIterator::ChunkIterator).
    // If we are here, we have failed to writeChunk() and the chunk is invalid
    chunk->free();
    notifyChunkReady(*chunk);
}

/* Write new chunk into the smgr.
 */
void
CachedStorage::writeChunk(ArrayDesc const& adesc,
                          PersistentChunk* newChunk,
                          const boost::shared_ptr<Query>& query)
{
    /* XXX TODO: consider locking mutex here to avoid writing replica chunks for a rolled-back query
     */
    PersistentChunk& chunk = *newChunk;

    /* To deal with exceptions: unpin and free
     */
    boost::function<void()> func = boost::bind(&CachedStorage::cleanChunk, this, &chunk);
    Destructor<boost::function<void()> > chunkCleaner(func);

    Query::validateQueryPtr(query);

    /* Update value count in Chunk Header
     */
    const AttributeDesc& attrDesc = adesc.getAttributes()[chunk.getAddress().attId];

    if (attrDesc.isEmptyIndicator()) {
        ConstRLEEmptyBitmap bitmap(static_cast<const char*>(chunk._data));
        chunk._hdr.nElems = bitmap.count();
    } else {
        ConstRLEPayload payload(static_cast<const char*>(chunk._data));
        chunk._hdr.nElems = payload.count();
    }

    /* Grab buffer to use for compressing chunk data and try to compress
     */
    const size_t bufSize = chunk.getSize();
    boost::scoped_array<char> buf(new char[bufSize]);
    if (!buf) {
        throw SYSTEM_EXCEPTION(SCIDB_SE_STORAGE, SCIDB_LE_CANT_ALLOCATE_MEMORY);
    }
    setToZeroInDebug(buf.get(), bufSize);

    currentStatistics->allocatedSize += bufSize;
    currentStatistics->allocatedChunks++;

    VersionID dstVersion = adesc.getVersionId();
    void const* deflated = buf.get();
    int nCoordinates = chunk._addr.coords.size();
    DBArrayChunkInternal intChunk(adesc, &chunk);
    size_t compressedSize = _compressors[chunk.getCompressionMethod()]->compress(buf.get(), intChunk);
    assert(compressedSize <= chunk.getSize());
    if (compressedSize == chunk.getSize())
    { // no compression
        deflated = chunk._data;
    }

    /* Replicate chunk data to other instances
     */
    vector<boost::shared_ptr<ReplicationManager::Item> > replicasVec;
    func = boost::bind(&CachedStorage::abortReplicas, this, &replicasVec);
    Destructor<boost::function<void()> > replicasCleaner(func);
    func.clear();
    replicate(adesc, chunk._addr, &chunk, deflated,
              compressedSize, chunk.getSize(), query, replicasVec);

    /* Write chunk locally into storage
     */
    {
        ScopedMutexLock cs(_mutex);
        assert(chunk.isRaw()); // new chunk is raw
        Query::validateQueryPtr(query);
        shared_ptr<DataStore> ds = _datastores.getDataStore(adesc.getUAId());

        /* Fill in the chunk descriptor
         */
        chunk._hdr.compressedSize = compressedSize;
        chunk._hdr.pos.dsGuid = adesc.getUAId();
        chunk._hdr.pos.offs = ds->allocateSpace(compressedSize,
                                                chunk._hdr.allocatedSize);

        /* Locate spot for chunk descriptor
         */
        if (_freeHeaders.empty())
        {
            chunk._hdr.pos.hdrPos = _hdr.currPos;
            _hdr.currPos += sizeof(ChunkDescriptor);
            _hdr.nChunks += 1;
        }
        else
        {
            set<uint64_t>::iterator i = _freeHeaders.begin();
            chunk._hdr.pos.hdrPos = *i;
            assert(chunk._hdr.pos.hdrPos != 0);
            _freeHeaders.erase(i);
        }

        /* Write ahead UNDO log
         */
        if (dstVersion != 0)
        {
            // Second entry in this array is the end-of-record sentinel.
            TransLogRecord transLogRecord[2];
            setToZeroInDebug(transLogRecord, sizeof(transLogRecord));

            transLogRecord->arrayUAID = adesc.getUAId();
            transLogRecord->arrayId = chunk._addr.arrId;
            transLogRecord->version = dstVersion;
            transLogRecord->hdr = chunk._hdr;
            transLogRecord->oldSize = 0;
            transLogRecord->hdrCRC = calculateCRC32(transLogRecord,
                                                    sizeof(TransLogRecordHeader));

            if (_logSize + sizeof(TransLogRecord) > _logSizeLimit)
            {
                _logSize = 0;
                _currLog ^= 1;
            }
            LOG4CXX_TRACE(logger, "ChunkDesc: Write in log chunk header " << transLogRecord->hdr.pos.offs << " at position " << _logSize);

            /* Write the transaction... log is opened O_SYNC so no flush is necessary
             */
            _log[_currLog]->writeAll(transLogRecord, sizeof(TransLogRecord) * 2, _logSize);
            _logSize += sizeof(TransLogRecord);
        }

        /* Write chunk data
         */
        writeChunkToDataStore(*ds, chunk, deflated);
        buf.reset();

        /* Write chunk descriptor in storage header
         */
        ChunkDescriptor cdesc;
        cdesc.hdr = chunk._hdr;
        for (int i = 0; i < nCoordinates; i++)
        {
            cdesc.coords[i] = chunk._addr.coords[i];
        }
        assert(chunk._hdr.pos.hdrPos != 0);

        LOG4CXX_TRACE(logger, "ChunkDesc: Write chunk descriptor at position " << chunk._hdr.pos.hdrPos);
        LOG4CXX_TRACE(logger, "Chunk descriptor to write: " << cdesc.toString());

        _hd->writeAll(&cdesc, sizeof(ChunkDescriptor), chunk._hdr.pos.hdrPos);

        /* Update storage header (for nchunks field)
         */
        _hd->writeAll(&_hdr, HEADER_SIZE, 0);

        InjectedErrorListener<WriteChunkInjectedError>::check();

        if (isPrimaryReplica(&chunk)) {
            chunkCleaner.disarm();
            chunk.unPin();
            notifyChunkReady(chunk);
            addChunkToCache(chunk);
        } // else chunkCleaner will dec accessCount and free
    }

    /* Wait for replication to complete
     */
    waitForReplicas(replicasVec);
    replicasCleaner.disarm();
}

/* Mark a chunk as free in the on-disk and in-memory chunk map.  Also mark it as free
   in the datastore.
 */
void CachedStorage::markChunkAsFree(InnerChunkMapEntry& entry, shared_ptr<DataStore>& ds)
{
    ChunkHeader header;
    shared_ptr<PersistentChunk>& chunk = entry.getChunk();

    if (!chunk)
    {
        /* Handle tombstone chunks
         */
        int rc = _hd->read(&header, sizeof(ChunkHeader), entry.getTombstonePos());
        if (rc != 0 && rc != sizeof(ChunkHeader)) {
            throw SYSTEM_EXCEPTION(SCIDB_SE_STORAGE,
                                   SCIDB_LE_OPERATION_FAILED_WITH_ERRNO)
                << "read" << ::strerror(errno) << errno;
        }
    }
    else
    {
        /* Handle live chunks
         */
        memcpy(&header, &(chunk->_hdr), sizeof(ChunkHeader));
        if (ds)
            ds->freeChunk(chunk->_hdr.pos.offs, chunk->_hdr.allocatedSize);
    }

    /* Update header as free and write back to storage header file
     */
    header.arrId = 0;
    LOG4CXX_TRACE(logger, "ChunkDesc: Free chunk descriptor at position " << header.pos.hdrPos);
    _hd->writeAll(&header, sizeof(ChunkHeader), header.pos.hdrPos);
    assert(header.nCoordinates < MAX_NUM_DIMS_SUPPORTED);
    _freeHeaders.insert(header.pos.hdrPos);
}

void CachedStorage::removeDeadChunks(ArrayDesc const& arrayDesc,
                                     set<Coordinates, CoordinatesLess> const& liveChunks,
                                     boost::shared_ptr<Query> const& query)
{
    typedef set<Coordinates, CoordinatesLess> DeadChunks;
    DeadChunks deadChunks;
    {
        ScopedMutexLock cs(_mutex);
        Query::validateQueryPtr(query);

        StorageAddress readAddress (arrayDesc.getId(), 0, Coordinates());
        while(findNextChunk(arrayDesc, query, readAddress))
        {
            if(liveChunks.count(readAddress.coords) == 0)
            {
                SCIDB_ASSERT( getPrimaryInstanceId(arrayDesc, readAddress) == _hdr.instanceId );
                deadChunks.insert(readAddress.coords);
            }
        }
    }
    for (DeadChunks::const_iterator i=deadChunks.begin(); i!=deadChunks.end(); ++i) {
        Coordinates const& coords = *i;
        // relication done inside removeChunkVersion() must be done with _mutex UNLOCKED
        removeChunkVersion(arrayDesc, coords, query);
    }
}

void CachedStorage::removeChunkVersion(ArrayDesc const& arrayDesc,
                                       Coordinates const& coords,
                                       shared_ptr<Query> const& query)
{
    vector<boost::shared_ptr<ReplicationManager::Item> > replicasVec;
    boost::function<void()> f = boost::bind(&CachedStorage::abortReplicas, this, &replicasVec);
    Destructor<boost::function<void()> > replicasCleaner(f);
    StorageAddress addr(arrayDesc.getId(), 0, coords);
    replicate(arrayDesc, addr, NULL, NULL, 0, 0, query, replicasVec);
    removeLocalChunkVersion(arrayDesc, coords, query);
    waitForReplicas(replicasVec);
    replicasCleaner.disarm();
}

void CachedStorage::removeLocalChunkVersion(ArrayDesc const& arrayDesc,
                                            Coordinates const& coords,
                                            shared_ptr<Query> const& query)
{
    ScopedMutexLock cs(_mutex);
    Query::validateQueryPtr(query);

    assert(arrayDesc.getUAId() != arrayDesc.getId()); //Immutable arrays NEVER have tombstones
    VersionID dstVersion = arrayDesc.getVersionId();
    ChunkDescriptor tombstoneDesc;
    setToZeroInDebug(&tombstoneDesc, sizeof(tombstoneDesc));

    tombstoneDesc.hdr.storageVersion = SCIDB_STORAGE_FORMAT_VERSION;
    tombstoneDesc.hdr.flags = 0;
    tombstoneDesc.hdr.set<ChunkHeader::TOMBSTONE>(true);
    tombstoneDesc.hdr.arrId = arrayDesc.getId();
    tombstoneDesc.hdr.nCoordinates = coords.size();
    tombstoneDesc.hdr.instanceId = getPrimaryInstanceId(arrayDesc, StorageAddress(arrayDesc.getId(), 0, coords));
    tombstoneDesc.hdr.allocatedSize = 0;
    tombstoneDesc.hdr.compressedSize = 0;
    tombstoneDesc.hdr.size = 0;
    tombstoneDesc.hdr.nElems = 0;
    tombstoneDesc.hdr.compressionMethod = 0;
    tombstoneDesc.hdr.pos.dsGuid = arrayDesc.getUAId();
    tombstoneDesc.hdr.pos.offs = 0;
    for (int i = 0; i <  tombstoneDesc.hdr.nCoordinates; i++)
    {
        tombstoneDesc.coords[i] = coords[i];
    }
    //WAL
    TransLogRecord transLogRecord[2];
    setToZeroInDebug(transLogRecord, sizeof(transLogRecord));
    transLogRecord->arrayUAID = arrayDesc.getUAId();
    transLogRecord->arrayId = arrayDesc.getId();
    transLogRecord->version = dstVersion;
    transLogRecord->oldSize = 0;
    ::memset(&transLogRecord[1], 0, sizeof(TransLogRecord)); // end of log marker
    ChunkMap::iterator iter = _chunkMap.find(arrayDesc.getUAId());
    if(iter == _chunkMap.end())
    {
        throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "Attempt to create tombstone for unexistent array";
    }
    shared_ptr<InnerChunkMap> inner = iter->second;
    for (AttributeID i =0; i<arrayDesc.getAttributes().size(); i++)
    {
        query->validate();

        tombstoneDesc.hdr.attId = i;
        StorageAddress addr (arrayDesc.getId(), i, coords);
        if( (*inner)[addr].getChunk() != NULL)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_STORAGE, SCIDB_LE_CHUNK_ALREADY_EXISTS)
            << CoordsToStr(addr.coords);
        }
        if (_freeHeaders.empty())
        {
            tombstoneDesc.hdr.pos.hdrPos = _hdr.currPos;
            _hdr.currPos += sizeof(ChunkDescriptor);
            _hdr.nChunks += 1;
        }
        else
        {
            set<uint64_t>::iterator i = _freeHeaders.begin();
            tombstoneDesc.hdr.pos.hdrPos = *i;
            assert( tombstoneDesc.hdr.pos.hdrPos != 0);
            _freeHeaders.erase(i);
        }
        (*inner)[addr].setTombstonePos(tombstoneDesc.hdr.pos.hdrPos);
        transLogRecord->hdr = tombstoneDesc.hdr;
        transLogRecord->hdrCRC = calculateCRC32(transLogRecord, sizeof(TransLogRecordHeader));
        if (_logSize + sizeof(TransLogRecord) > _logSizeLimit)
        {
            _logSize = 0;
            _currLog ^= 1;
        }
        LOG4CXX_TRACE(logger, "ChunkDesc: Write in log chunk tombstone header " << transLogRecord->hdr.pos.offs
                      << " at position " << _logSize);

        _log[_currLog]->writeAll(transLogRecord, sizeof(TransLogRecord) * 2, _logSize);
        _logSize += sizeof(TransLogRecord);

        LOG4CXX_TRACE(logger, "ChunkDesc: Write chunk tombstone descriptor at position " <<  tombstoneDesc.hdr.pos.hdrPos);
        LOG4CXX_TRACE(logger, "Chunk tombstone descriptor to write: " << tombstoneDesc.toString());

        _hd->writeAll(&tombstoneDesc, sizeof(ChunkDescriptor), tombstoneDesc.hdr.pos.hdrPos);
    }
    _hd->writeAll(&_hdr, HEADER_SIZE, 0);
    InjectedErrorListener<WriteChunkInjectedError>::check();
}

///
/// @note rollback must be called only when the query calling it is in error state
///       thus, before performing any updates under THE _mutex, the query context must be validated
///       to avoid leaving chunks behind
void CachedStorage::rollback(std::map<ArrayID, VersionID> const& undoUpdates)
{
    LOG4CXX_DEBUG(logger, "Performing rollback");

    ScopedMutexLock cs(_mutex);
    for (int i = 0; i < 2; i++)
    {
        uint64_t pos = 0;
        TransLogRecord transLogRecord;
        setToZeroInDebug(&transLogRecord, sizeof(transLogRecord));
        while (true)
        {
            // read txn log record
            size_t rc = _log[i]->read(&transLogRecord, sizeof(TransLogRecord), pos);
            if (rc != sizeof(TransLogRecord) || transLogRecord.arrayUAID == 0)
            {
                LOG4CXX_DEBUG(logger, "End of log at position " << pos << " rc=" << rc);
                break;
            }
            uint32_t crc = calculateCRC32(&transLogRecord, sizeof(TransLogRecordHeader));
            if (crc != transLogRecord.hdrCRC)
            {
                LOG4CXX_ERROR(logger, "CRC doesn't match for log record: "
                              << crc << " vs. expected " << transLogRecord.hdrCRC);
                break;
            }
            pos += sizeof(TransLogRecord);
            std::map<ArrayID, VersionID>::const_iterator it = undoUpdates.find(transLogRecord.arrayUAID);
            VersionID lastVersionID = -1;
            if (it != undoUpdates.end() && (lastVersionID = it->second) < transLogRecord.version)
            {
                // this version is to be un-done
                assert(transLogRecord.oldSize == 0);

                transLogRecord.hdr.arrId = 0; // mark chunk as free
                assert(transLogRecord.hdr.pos.hdrPos != 0);
                LOG4CXX_TRACE(logger, "ChunkDesc: Undo chunk descriptor creation at position "
                              << transLogRecord.hdr.pos.hdrPos);
                _hd->writeAll(&transLogRecord.hdr, sizeof(ChunkHeader), transLogRecord.hdr.pos.hdrPos);
                _freeHeaders.insert(transLogRecord.hdr.pos.hdrPos);

                /* Update the free list for the data store
                 */
                if (!transLogRecord.hdr.is<ChunkHeader::TOMBSTONE>() &&
                    lastVersionID > 0)
                {
                    shared_ptr<DataStore> ds =
                        _datastores.getDataStore(transLogRecord.hdr.pos.dsGuid);
                    ds->freeChunk(transLogRecord.hdr.pos.offs,
                                  transLogRecord.hdr.allocatedSize);
                }
            }
            pos += transLogRecord.oldSize;
        }
    }
    flush();

    for(std::map<ArrayID, VersionID>::const_iterator it = undoUpdates.begin();
        it != undoUpdates.end();
        ++it)
    {
        // If we rolled back the first version, delete the datastore
        if (it->second == 0)
        {
            _datastores.closeDataStore(it->first, true /* remove from disk */);
        }
        LOG4CXX_DEBUG(logger, "Rolling back arrId = "
                      << it->first << ", version = " << it->second);
    }

    LOG4CXX_DEBUG(logger, "Rollback complete");
}

void CachedStorage::doTxnRecoveryOnStartup()
{
    list<shared_ptr<SystemCatalog::LockDesc> > coordLocks;
    list<shared_ptr<SystemCatalog::LockDesc> > workerLocks;

    SystemCatalog::getInstance()->readArrayLocks(getInstanceId(), coordLocks, workerLocks);
    shared_ptr<map<ArrayID, VersionID> > arraysToRollback = make_shared <map<ArrayID, VersionID> > ();
    UpdateErrorHandler::RollbackWork collector = bind(&collectArraysToRollback, arraysToRollback, _1, _2, _3);

    { // Deal with the  SystemCatalog::LockDesc::COORD type locks first

        for (list<shared_ptr<SystemCatalog::LockDesc> >::const_iterator iter = coordLocks.begin(); iter != coordLocks.end(); ++iter)
        {
            const shared_ptr<SystemCatalog::LockDesc>& lock = *iter;

            if (lock->getLockMode() == SystemCatalog::LockDesc::RM)
            {
                const bool checkLock = false;
                RemoveErrorHandler::handleRemoveLock(lock, checkLock);
            }
            else if (lock->getLockMode() == SystemCatalog::LockDesc::CRT || lock->getLockMode() == SystemCatalog::LockDesc::WR)
            {
                UpdateErrorHandler::handleErrorOnCoordinator(lock, collector);
            }
            else
            {
                assert(lock->getLockMode() == SystemCatalog::LockDesc::RNF ||
                       lock->getLockMode() == SystemCatalog::LockDesc::RD);
            }
        }

        // Do the rollback
        rollback(*arraysToRollback.get());

        SystemCatalog::getInstance()->deleteCoordArrayLocks(getInstanceId());
    }

    { // Deal with the worker locks next

        arraysToRollback->clear();

        for (list<shared_ptr<SystemCatalog::LockDesc> >::const_iterator iter = workerLocks.begin(); iter != workerLocks.end(); ++iter)
        {
            const shared_ptr<SystemCatalog::LockDesc>& lock = *iter;

            if (lock->getLockMode() == SystemCatalog::LockDesc::CRT || lock->getLockMode() == SystemCatalog::LockDesc::WR)
            {
                const bool checkCoordinatorLock = true;
                UpdateErrorHandler::handleErrorOnWorker(lock, checkCoordinatorLock, collector);
            }
            else
            {
                assert(lock->getLockMode() == SystemCatalog::LockDesc::RNF);
            }
        }

        // Do the rollback
        rollback(*arraysToRollback.get());

        SystemCatalog::getInstance()->deleteWorkerArrayLocks(getInstanceId());
    }
}

/* Flush all changes to the physical device(s) for the indicated array.
   (optionally flush data for all arrays, if uaId == INVALID_ARRAY_ID).
*/
void
CachedStorage::flush(ArrayUAID uaId)
{
    int rc;

    /* flush the chunk map file
     */
    rc = _hd->fsync();
    if (rc != 0)
    {
        throw SYSTEM_EXCEPTION(SCIDB_SE_STORAGE, SCIDB_LE_OPERATION_FAILED_WITH_ERRNO)
            << "fsync" << ::strerror(errno) << errno;
    }

    /* flush the data store for the indicated array (or flush all datastores)
     */
    if (uaId != INVALID_ARRAY_ID)
    {
        shared_ptr<DataStore> ds = _datastores.getDataStore(uaId);
        ds->flush();
    }
    else
    {
        _datastores.flushAllDataStores();
    }
}

boost::shared_ptr<ArrayIterator> CachedStorage::getArrayIterator(boost::shared_ptr<const Array>& arr,
                                                                 AttributeID attId,
                                                                 boost::shared_ptr<Query>& query)
{
    return boost::shared_ptr<ArrayIterator>(new DBArrayIterator(this, arr, attId, query, true));
}

boost::shared_ptr<ConstArrayIterator> CachedStorage::getConstArrayIterator(boost::shared_ptr<const Array>& arr,
                                                                           AttributeID attId,
                                                                           boost::shared_ptr<Query>& query)
{
    return boost::shared_ptr<ConstArrayIterator>(new DBArrayIterator(this, arr, attId, query, false));
}

void CachedStorage::fetchChunk(ArrayDesc const& desc, PersistentChunk& chunk)
{
    ChunkInitializer guard(this, chunk);
    shared_ptr<DataStore> ds = _datastores.getDataStore(desc.getUAId());
    if (chunk._hdr.pos.hdrPos == 0)
    {
        throw SYSTEM_EXCEPTION(SCIDB_SE_STORAGE,
                               SCIDB_LE_ACCESS_TO_RAW_CHUNK) << chunk.getHeader().arrId;
    }
    size_t chunkSize = chunk.getSize();
    chunk.allocate(chunkSize);
    if (chunk.getCompressedSize() != chunkSize)
    {
        const size_t bufSize = chunk.getCompressedSize();
        boost::scoped_array<char> buf(new char[bufSize]);
        currentStatistics->allocatedSize += bufSize;
        currentStatistics->allocatedChunks++;
        if (!buf) {
            throw SYSTEM_EXCEPTION(SCIDB_SE_STORAGE, SCIDB_LE_CANT_ALLOCATE_MEMORY);
        }
        readChunkFromDataStore(*ds, chunk, buf.get());
        DBArrayChunkInternal intChunk(desc, &chunk);
        size_t rc = _compressors[chunk.getCompressionMethod()]->decompress(buf.get(), chunk.getCompressedSize(), intChunk);
        if (rc != chunk.getSize())
            throw SYSTEM_EXCEPTION(SCIDB_SE_STORAGE, SCIDB_LE_CANT_DECOMPRESS_CHUNK);
        buf.reset();
    }
    else
    {
        readChunkFromDataStore(*ds, chunk, chunk._data);
    }
}

void CachedStorage::loadChunk(ArrayDesc const& desc, PersistentChunk* aChunk)
{
    PersistentChunk& chunk = *aChunk;
    {
        ScopedMutexLock cs(_mutex);
        if (chunk._accessCount < 2)
        { // Access count>=2 means that this chunk is already pinned and loaded by some upper frame so access to it may not cause deadlock
            _mutex.checkForDeadlock();
        }
        if (chunk._raw)
        {
            // Some other thread is already loading the chunk: just wait until it completes
            do
            {
                chunk._waiting = true;
                Semaphore::ErrorChecker ec;
                boost::shared_ptr<Query> query = Query::getQueryByID(Query::getCurrentQueryID(), false);
                if (query)
                {
                    ec = bind(&Query::validate, query);
                }
                _loadEvent.wait(_mutex, ec);
            } while (chunk._raw);

            if (chunk._data == NULL)
            {
                chunk._raw = true;
            }
        }
        else
        {
            if (chunk._data == NULL)
            {
                _mutex.checkForDeadlock();
                chunk._raw = true;
                addChunkToCache(chunk);
            }
        }
    }

    if (chunk._raw)
    {
        fetchChunk(desc, chunk);
    }
}

boost::shared_ptr<PersistentChunk>
CachedStorage::readChunk(ArrayDesc const& desc,
                         StorageAddress const& addr,
                         const boost::shared_ptr<Query>& query)
{
    boost::shared_ptr<PersistentChunk> chunk = CachedStorage::lookupChunk(desc, addr);
    if (!chunk) {
        throw SYSTEM_EXCEPTION(SCIDB_SE_STORAGE, SCIDB_LE_CHUNK_NOT_FOUND);
    }
    loadChunk(desc, chunk.get());
    return chunk;
}

InstanceID CachedStorage::getInstanceId() const
{
    return _hdr.instanceId;
}

size_t CachedStorage::getNumberOfInstances() const
{
    return _nInstances;
}

void CachedStorage::setInstanceId(InstanceID id)
{
    _hdr.instanceId = id;
    _hd->writeAll(&_hdr, HEADER_SIZE, 0);
}

void CachedStorage::getDiskInfo(DiskInfo& info)
{
    ::memset(&info, 0, sizeof info);
}

void CachedStorage::listChunkDescriptors(ListChunkDescriptorsArrayBuilder& builder)
{
    ScopedMutexLock cs(_mutex);
    pair<ChunkDescriptor, bool> element;
    uint64_t chunkPos = HEADER_SIZE;
    for (size_t i = 0; i < _hdr.nChunks; i++, chunkPos += sizeof(ChunkDescriptor))
    {
        _hd->readAll(&element.first, sizeof(ChunkDescriptor), chunkPos);
        element.second = _freeHeaders.count(chunkPos);
        builder.listElement(element);
    }
}

void CachedStorage::listChunkMap(ListChunkMapArrayBuilder& builder)
{
    ScopedMutexLock cs(_mutex);
    for (ChunkMap::iterator i = _chunkMap.begin(); i != _chunkMap.end(); ++i)
    {
        ArrayUAID uaid = i->first;
        for (InnerChunkMap::iterator j = i->second->begin(); j != i->second->end(); ++j)
        {
            builder.listElement(ChunkMapEntry(uaid, j->first, j->second.getChunk().get()));
        }
    }
}

///////////////////////////////////////////////////////////////////
/// DBArrayIterator
///////////////////////////////////////////////////////////////////

CachedStorage::DBArrayIterator::DBArrayIterator(CachedStorage* storage,
                                                shared_ptr<const Array>& array,
                                                AttributeID attId, boost::shared_ptr<Query>& query,
                                                bool writeMode)
  : _currChunk(NULL),
    _storage(storage),
    _attrDesc(array->getArrayDesc().getAttributes()[attId]),
    _address(array->getArrayDesc().getId(), attId, Coordinates()),
    _query(query),
    _writeMode(writeMode),
    _array(array)
{
    reset();
}


CachedStorage::DBArrayIterator::~DBArrayIterator()
{}

CachedStorage::DBArrayChunk* CachedStorage::DBArrayIterator::getDBArrayChunk(boost::shared_ptr<PersistentChunk>& dbChunk)
{
    assert(dbChunk);
    DBArrayMap::iterator iter = _dbChunks.find(dbChunk);
    if (iter == _dbChunks.end()) {
        shared_ptr<DBArrayChunk> dbac(new DBArrayChunk(*this, dbChunk.get()));
        std::pair<DBArrayMap::iterator, bool> res = _dbChunks.insert(DBArrayMap::value_type(dbChunk, dbac));
        assert(res.second);
        iter = res.first;
    }
    assert(iter != _dbChunks.end());
    assert(iter->first == dbChunk);
    assert(iter->second->getPersistentChunk() == dbChunk.get());
    LOG4CXX_TRACE(logger, "DBArrayIterator::getDBArrayChunk this=" << this
                  << ", dbChunk=" << dbChunk.get()
                  << ", dbArrayChunk=" << iter->second.get());

    return iter->second.get();
}


ConstChunk const& CachedStorage::DBArrayIterator::getChunk()
{
    getQuery();
    if (end())
    {
        throw USER_EXCEPTION(SCIDB_SE_STORAGE, SCIDB_LE_NO_CURRENT_CHUNK);
    }
    if (_currChunk == NULL)
    {
        shared_ptr<PersistentChunk> chunk = _storage->lookupChunk(getArrayDesc(), _address);
        if (!chunk) {
            throw SYSTEM_EXCEPTION(SCIDB_SE_STORAGE, SCIDB_LE_CHUNK_NOT_FOUND);
        }
        PersistentChunk::UnPinner scope(chunk.get());
        DBArrayChunk *dbChunk = getDBArrayChunk(chunk);
        _currChunk = dbChunk;
        assert(_currChunk);
    }
    return *_currChunk;
}

bool CachedStorage::DBArrayIterator::end()
{
    return _address.coords.size() == 0;
}

void CachedStorage::DBArrayIterator::operator ++()
{
    shared_ptr<Query> query = getQuery();
    _currChunk = NULL;
    if (end())
    {
        throw USER_EXCEPTION(SCIDB_SE_STORAGE, SCIDB_LE_NO_CURRENT_CHUNK);
    }
    bool ret = _storage->findNextChunk(getArrayDesc(), query, _address);
    if (_writeMode)
    {   //in _writeMode we iterate only over chunks from this exact version
        while (ret && _address.arrId != getArrayDesc().getId())
        {
            ret = _storage->findNextChunk(getArrayDesc(), query, _address);
        }
    }
}

Coordinates const& CachedStorage::DBArrayIterator::getPosition()
{
    if (end())
    {
        throw USER_EXCEPTION(SCIDB_SE_STORAGE, SCIDB_LE_NO_CURRENT_CHUNK);
    }
    return _address.coords;
}

bool CachedStorage::DBArrayIterator::setPosition(Coordinates const& pos)
{
    shared_ptr<Query> query = getQuery();
    _currChunk = NULL;
    _address.coords = pos;
    getArrayDesc().getChunkPositionFor(_address.coords);

    bool ret = _storage->findChunk(getArrayDesc(), query, _address);
    if ( !ret || (_writeMode && _address.arrId != getArrayDesc().getId()))
    {
        _address.coords.clear();
        return false;
    }
    return true;
}

void CachedStorage::DBArrayIterator::reset()
{
    shared_ptr<Query> query = getQuery();
    _currChunk = NULL;
    _address.coords.clear();

    bool ret = _storage->findNextChunk(getArrayDesc(), query, _address);
    if (_writeMode)
    {   //in _writeMode we iterate only over chunks from this exact version
        while ( ret && _address.arrId != getArrayDesc().getId())
        {
            ret = _storage->findNextChunk(getArrayDesc(), query, _address);
        }
    }
}

Chunk& CachedStorage::DBArrayIterator::newChunk(Coordinates const& pos, int compressionMethod)
{
    ASSERT_EXCEPTION_FALSE("DBArrayIterator::newChunk(pos, compressionMethod)");
}

Chunk& CachedStorage::DBArrayIterator::newChunk(Coordinates const& pos)
{
    assert(_writeMode);

    int compressionMethod = getAttributeDesc().getDefaultCompressionMethod();
    shared_ptr<Query> query = getQuery();
    _currChunk = NULL;
    _address.coords = pos;
    if (!getArrayDesc().contains(_address.coords))
    {
        _address.coords.clear();
        throw USER_EXCEPTION(SCIDB_SE_STORAGE, SCIDB_LE_CHUNK_OUT_OF_BOUNDARIES);
    }
    getArrayDesc().getChunkPositionFor(_address.coords);

    bool ret = _storage->findChunk(getArrayDesc(), query, _address);
    if(ret && _address.arrId == getArrayDesc().getId())
    {
        stringstream ss; ss << CoordsToStr(_address.coords);
        _address.coords.clear();
        throw SYSTEM_EXCEPTION(SCIDB_SE_STORAGE, SCIDB_LE_CHUNK_ALREADY_EXISTS)
        << ss.str();
    }
    _address.arrId = getArrayDesc().getId();
    _address.coords = pos;
    getArrayDesc().getChunkPositionFor(_address.coords);
    shared_ptr<PersistentChunk> chunk =
        _storage->createChunk(getArrayDesc(), _address, compressionMethod, query);
    assert(chunk);
    DBArrayChunk *dbChunk = getDBArrayChunk(chunk);
    _currChunk = dbChunk;
    return *_currChunk;
}

void CachedStorage::DBArrayIterator::deleteChunk(Chunk& chunk) //XXX TODO: consider removing this method altogether
{
    DBArrayChunk* dbaChunk = dynamic_cast<DBArrayChunk*>(&chunk);
    if (dbaChunk==NULL || chunk.getArrayDesc() != getArrayDesc()) {
        throw (SYSTEM_EXCEPTION(SCIDB_SE_STORAGE, SCIDB_LE_INVALID_FUNCTION_ARGUMENT)
               << "chunk(not persistent)");
    }
    assert(_writeMode);
    _currChunk = NULL;
    _address.coords.clear();

    PersistentChunk* dbChunk = dbaChunk->getPersistentChunk();
    LOG4CXX_TRACE(logger, "DBArrayIterator::deleteChunk this="
                  << this << ", dbChunk=" << dbChunk << ", dbArrayChunk?=" << &chunk);
    _storage->deleteChunk(getArrayDesc(),*dbChunk);
    _dbChunks.erase(dbChunk->shared_from_this());
}

Chunk& CachedStorage::DBArrayIterator::copyChunk(ConstChunk const& srcChunk, boost::shared_ptr<ConstRLEEmptyBitmap>& emptyBitmap)
{
    assert(_writeMode);
    shared_ptr<Query> query = getQuery();
    _address.coords = srcChunk.getFirstPosition(false);
    if(getArrayDesc().getVersionId() > 1)
    {
        if(_storage->findChunk(getArrayDesc(), query, _address))
        {
            if(_address.arrId == getArrayDesc().getId())
            {
                stringstream ss; ss << CoordsToStr(_address.coords);
                _address.coords.clear();
                throw SYSTEM_EXCEPTION(SCIDB_SE_STORAGE, SCIDB_LE_CHUNK_ALREADY_EXISTS)
                << ss.str();
            }
            else
            {
                assert(_address.arrId < getArrayDesc().getId());
                shared_ptr<PersistentChunk> dstChunk = _storage->lookupChunk(getArrayDesc(), _address);
                if (!dstChunk) {
                    throw SYSTEM_EXCEPTION(SCIDB_SE_STORAGE, SCIDB_LE_CHUNK_NOT_FOUND);
                }
                PersistentChunk::UnPinner scope(dstChunk.get());
                DBArrayChunk const* dbaChunk = dynamic_cast<DBArrayChunk const*>(&srcChunk);
                if (dbaChunk && dbaChunk->getPersistentChunk() == dstChunk.get())
                {
                    // Original chunk was not changed: no need to do anything!
                    DBArrayChunk *dbChunk = getDBArrayChunk(dstChunk);
                    _currChunk = dbChunk;
                    assert(_currChunk);
                    return *_currChunk;
                }
                //else new delta code goes here!
            }
        }
    }
    boost::shared_ptr<ConstRLEEmptyBitmap> nullEmptyBitmap; // to avoid attaching EBM to the chunk
    _currChunk = &ArrayIterator::copyChunk(srcChunk, nullEmptyBitmap);

    assert(dynamic_cast<DBArrayChunk*>(_currChunk));
    _address.arrId = getArrayDesc().getId();

    return *_currChunk;
}

CachedStorage CachedStorage::instance;
Storage* StorageManager::instance = &CachedStorage::instance;

///////////////////////////////////////////////////////////////////
/// DBArrayChunk
///////////////////////////////////////////////////////////////////

CachedStorage::DBArrayChunk::DBArrayChunk(DBArrayIterator& iterator, PersistentChunk* chunk) :
DBArrayChunkBase(chunk), _arrayIter(iterator), _nWriters(0)
{
}

CachedStorage::DBArrayChunkBase::DBArrayChunkBase(PersistentChunk* chunk)
:_inputChunk(chunk)
{
    assert(chunk);
}

const Array& CachedStorage::DBArrayChunkBase::getArray() const
{
    ASSERT_EXCEPTION_FALSE("DBArrayChunkBase::getArray");
}

const Array& CachedStorage::DBArrayChunk::getArray() const
{
    return _arrayIter.getArray();
}

const ArrayDesc& CachedStorage::DBArrayChunkBase::getArrayDesc() const
{
    ASSERT_EXCEPTION_FALSE("DBArrayChunkBase::getArrayDesc");
}

const ArrayDesc& CachedStorage::DBArrayChunk::getArrayDesc() const
{
    return _arrayIter.getArrayDesc();
}

const AttributeDesc& CachedStorage::DBArrayChunkBase::getAttributeDesc() const
{
    ASSERT_EXCEPTION_FALSE("DBArrayChunkBase::getAttributeDesc");
}

const AttributeDesc& CachedStorage::DBArrayChunk::getAttributeDesc() const
{
    return _arrayIter.getAttributeDesc();
}

int CachedStorage::DBArrayChunkBase::getCompressionMethod() const
{
    return _inputChunk->getCompressionMethod();
}

bool CachedStorage::DBArrayChunkBase::pin() const
{
    LOG4CXX_TRACE(logger, "DBArrayChunkBase::pin() this=" << this << ", _inputChunk=" << _inputChunk);
    return _inputChunk->pin();
}

void CachedStorage::DBArrayChunkBase::unPin() const
{
    LOG4CXX_TRACE(logger, "DBArrayChunkBase::unPin() this=" << this << ", _inputChunk=" << _inputChunk);
    _inputChunk->unPin();
}

Coordinates const& CachedStorage::DBArrayChunkBase::getFirstPosition(bool withOverlap) const
{
    return _inputChunk->getFirstPosition(withOverlap);
}

Coordinates const& CachedStorage::DBArrayChunkBase::getLastPosition(bool withOverlap) const
{
    return _inputChunk->getLastPosition(withOverlap);
}

boost::shared_ptr<ConstChunkIterator> CachedStorage::DBArrayChunkBase::getConstIterator(int iterationMode) const
{
    ASSERT_EXCEPTION_FALSE("DBArrayChunkBase::getConstIterator");
}

boost::shared_ptr<ConstChunkIterator> CachedStorage::DBArrayChunk::getConstIterator(int iterationMode) const
{
    const AttributeDesc* bitmapAttr = getArrayDesc().getEmptyBitmapAttribute();
    Chunk* bitmap(NULL);
    PersistentChunk::UnPinner bitmapScope(NULL);
    shared_ptr<Query> query(_arrayIter.getQuery());

    if (bitmapAttr != NULL && bitmapAttr->getId() != DBArrayChunkBase::getAttributeId())
    {
        StorageAddress bitmapAddr(getArrayDesc().getId(), bitmapAttr->getId(), DBArrayChunkBase::getCoordinates());
        _arrayIter._storage->findChunk(getArrayDesc(), query, bitmapAddr);
        shared_ptr<PersistentChunk> bitmapChunk = _arrayIter._storage->readChunk(getArrayDesc(), bitmapAddr, query);
        bitmapScope.set(bitmapChunk.get());

        DBArrayChunk *dbChunk = _arrayIter.getDBArrayChunk(bitmapChunk);
        assert(dbChunk);

        bitmap = dbChunk;
    }

    PersistentChunk* dbChunk = getPersistentChunk();

    assert(dbChunk->getAddress().attId  == DBArrayChunkBase::getAttributeId());
    assert(dbChunk->getAddress().coords == DBArrayChunkBase::getCoordinates());

    dbChunk->pin();

    PersistentChunk::UnPinner selfScope(dbChunk);

    _arrayIter._storage->loadChunk(getArrayDesc(), dbChunk);
    if (getAttributeDesc().isEmptyIndicator()) {
        return boost::make_shared<RLEBitmapChunkIterator>(getArrayDesc(),
                                                          DBArrayChunkBase::getAttributeId(),
                                                          (Chunk*) this, bitmap, iterationMode, query);
    } else if ((iterationMode & ConstChunkIterator::INTENDED_TILE_MODE) ||
               (iterationMode & ConstChunkIterator::TILE_MODE)) { //old tile mode

        return boost::make_shared<RLEConstChunkIterator>(getArrayDesc(),
                                                         DBArrayChunkBase::getAttributeId(),
                                                         (Chunk*) this, bitmap, iterationMode, query);
    }

    // non-tile mode, but using the new tiles for read-ahead buffering
    boost::shared_ptr<RLETileConstChunkIterator> tiledIter =
        boost::make_shared<RLETileConstChunkIterator>(getArrayDesc(),
                                                      DBArrayChunkBase::getAttributeId(),
                                                      (Chunk*) this,
                                                      bitmap,
                                                      iterationMode,
                                                      query);
    return boost::make_shared< BufferedConstChunkIterator< boost::shared_ptr<RLETileConstChunkIterator> > >(tiledIter, query);
    // deprecated formats
}

boost::shared_ptr<ChunkIterator>
CachedStorage::DBArrayChunkBase::getIterator(boost::shared_ptr<Query> const& query,
                                             int iterationMode)
{
    ASSERT_EXCEPTION_FALSE("DBArrayChunkBase::getIterator");
}

boost::shared_ptr<ChunkIterator>
CachedStorage::DBArrayChunk::getIterator(boost::shared_ptr<Query> const& query,
                                         int iterationMode)
{
    if (query != _arrayIter.getQuery()) {
        throw (SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_INVALID_FUNCTION_ARGUMENT) << "invalid query");
    }
    const AttributeDesc* bitmapAttr = getArrayDesc().getEmptyBitmapAttribute();
    Chunk* bitmap(NULL);
    PersistentChunk::UnPinner bitmapScope(NULL);
    if (bitmapAttr != NULL && bitmapAttr->getId() != DBArrayChunkBase::getAttributeId()
        && !(iterationMode & ConstChunkIterator::NO_EMPTY_CHECK))
    {
        StorageAddress bitmapAddr(getArrayDesc().getId(), bitmapAttr->getId(),  DBArrayChunkBase::getCoordinates());
        shared_ptr<PersistentChunk> bitmapChunk = _arrayIter._storage->createChunk(getArrayDesc(),
                                                                                   bitmapAddr,
                                                                                   bitmapAttr->getDefaultCompressionMethod(),query);
        assert(bitmapChunk);
        bitmapScope.set(bitmapChunk.get());

        DBArrayChunk *dbChunk = _arrayIter.getDBArrayChunk(bitmapChunk);
        assert(dbChunk);
        bitmap = dbChunk;
    }
    _nWriters += 1;

    // we should not be storing in sparse format, but
    // there are operators that
    // still generate sparse chunks

    boost::shared_ptr<ChunkIterator> iterator =
        boost::shared_ptr<ChunkIterator>(new RLEChunkIterator(getArrayDesc(),
                                         DBArrayChunkBase::getAttributeId(),
                                                              this, bitmap,
                                                              iterationMode, query));
    return iterator;
}

boost::shared_ptr<ConstRLEEmptyBitmap> CachedStorage::DBArrayChunkBase::getEmptyBitmap() const
{
    ASSERT_EXCEPTION_FALSE("DBArrayChunkBase::getEmptyBitmap");
}

boost::shared_ptr<ConstRLEEmptyBitmap> CachedStorage::DBArrayChunk::getEmptyBitmap() const
{
    const AttributeDesc* bitmapAttr = getArrayDesc().getEmptyBitmapAttribute();
    boost::shared_ptr<ConstRLEEmptyBitmap> bitmap;
    if (bitmapAttr != NULL && bitmapAttr->getId() != DBArrayChunkBase::getAttributeId())
    {
        StorageAddress bitmapAddr(getArrayDesc().getId(), bitmapAttr->getId(), DBArrayChunkBase::getCoordinates());

        shared_ptr<Query> query(_arrayIter.getQuery());

        _arrayIter._storage->findChunk(getArrayDesc(), query, bitmapAddr);
        shared_ptr<scidb::PersistentChunk> bitmapChunk = _arrayIter._storage->readChunk(getArrayDesc(), bitmapAddr, query);

        PersistentChunk::UnPinner scope(bitmapChunk.get());

        DBArrayChunk *dbChunk = _arrayIter.getDBArrayChunk(bitmapChunk);
        assert(dbChunk);

        bitmap = make_shared<ConstRLEEmptyBitmap>(*dbChunk);
    }
    else
    {
        //XXX shouldn't we just return a NULL ptr ?
        bitmap = ConstChunk::getEmptyBitmap();
    }
    return bitmap;
}

size_t CachedStorage::DBArrayChunkBase::count() const
{
    assert(!materializedChunk);
    if (getArrayDesc().hasOverlap()) {
        // XXX HACK: It appears that the element count stored on disk includes the overlap region.
        // This violates(?) the ConstChunk::count() contract (inferred from implementation),
        // so we fall back to the "canonical" count() if the overlap is present.
        // ArrayIterator::copyChunk() might be the code to blame for incorrectly(?)
        // setting the count on persistent chunks
        return ConstChunk::count();
    }
    const size_t c = _inputChunk->count();

    return (c!=0) ? c : ConstChunk::count();
}

bool CachedStorage::DBArrayChunkBase::isCountKnown() const
{
    assert(!materializedChunk);
    if (!getArrayDesc().hasOverlap() && _inputChunk->isCountKnown()) {
        return true;
    }
    return ConstChunk::isCountKnown();
}

void CachedStorage::DBArrayChunkBase::setCount(size_t count)
{
    _inputChunk->setCount(count);
}

void CachedStorage::DBArrayChunkBase::truncate(Coordinate lastCoord)
{
    _inputChunk->truncate(lastCoord);
}

void CachedStorage::DBArrayChunkBase::merge(ConstChunk const& with, boost::shared_ptr<Query>& query)
{
    /* Trying to merge into a DB Chunk indicates an error
     */
    throw USER_EXCEPTION(SCIDB_SE_MERGE, SCIDB_LE_CHUNK_ALREADY_EXISTS)
        << CoordsToStr(getFirstPosition(false));
}

void CachedStorage::DBArrayChunkBase::aggregateMerge(ConstChunk const& with,
                                                     AggregatePtr const& aggregate,
                                                     boost::shared_ptr<Query>& query)
{
    /* Trying to merge into a DB Chunk indicates an error
     */
    throw USER_EXCEPTION(SCIDB_SE_MERGE, SCIDB_LE_CHUNK_ALREADY_EXISTS)
        << CoordsToStr(getFirstPosition(false));
}

void CachedStorage::DBArrayChunkBase::nonEmptyableAggregateMerge(ConstChunk const& with,
                                                                 AggregatePtr const& aggregate,
                                                                 boost::shared_ptr<Query>& query)
{
    /* Trying to merge into a DB Chunk indicates an error
     */
    throw USER_EXCEPTION(SCIDB_SE_MERGE, SCIDB_LE_CHUNK_ALREADY_EXISTS)
        << CoordsToStr(getFirstPosition(false));
}

void CachedStorage::DBArrayChunkBase::write(const boost::shared_ptr<Query>& query)
{
    ASSERT_EXCEPTION_FALSE("DBArrayChunkBase::write");
}

void CachedStorage::DBArrayChunk::write(const boost::shared_ptr<Query>& query)
{
    if (query != _arrayIter.getQuery()) {
        throw (SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_INVALID_FUNCTION_ARGUMENT) << "invalid query");
    }

    const size_t bitmapSize = getBitmapSize();
    if(bitmapSize != 0) {
        const size_t chunkSize = getSize();
        LOG4CXX_TRACE(logger, "CachedStorage::DBArrayChunk::write =" << this
                      << ", size = "<< chunkSize
                      << ", bitmapsize = "<< bitmapSize);
        assert(chunkSize>bitmapSize);
        reallocate(chunkSize-bitmapSize);
    }
    assert(getBitmapSize() == 0);

    PersistentChunk* dbChunk = getPersistentChunk();

    assert(dbChunk->getAddress().attId  == DBArrayChunkBase::getAttributeId());
    assert(dbChunk->getAddress().coords == DBArrayChunkBase::getCoordinates());

    if (--_nWriters <= 0)
    {
        _arrayIter._storage->writeChunk(getArrayDesc(), dbChunk, query);
        _nWriters = 0;
    }
}

void* CachedStorage::DBArrayChunkBase::getData() const
{
    return _inputChunk->getData(getArrayDesc());
}

void* CachedStorage::DBArrayChunkBase::getDataForLoad()
{
    return _inputChunk->getDataForLoad();
}

size_t CachedStorage::DBArrayChunkBase::getSize() const
{
    return _inputChunk->getSize();
}

void CachedStorage::DBArrayChunkBase::allocate(size_t size)
{
    _inputChunk->allocate(size);
}

void  CachedStorage::DBArrayChunkBase::reallocate(size_t size)
{
    _inputChunk->reallocate(size);
}

void CachedStorage::DBArrayChunkBase::free()
{
    _inputChunk->free();
}

void CachedStorage::DBArrayChunkBase::compress(CompressedBuffer& buf,
                                               boost::shared_ptr<ConstRLEEmptyBitmap>& emptyBitmap) const
{
    ASSERT_EXCEPTION_FALSE("DBArrayChunkBase::compress");
}

void CachedStorage::DBArrayChunk::compress(CompressedBuffer& buf,
                                           boost::shared_ptr<ConstRLEEmptyBitmap>& emptyBitmap) const
{
    if (emptyBitmap)
    {
        MemChunk closure;
        closure.initialize(*this);
        makeClosure(closure, emptyBitmap);
        closure.compress(buf, emptyBitmap);
    }
    else
    {
        PersistentChunk* dbChunk = getPersistentChunk();

        assert(dbChunk->getAddress().attId  == DBArrayChunkBase::getAttributeId());
        assert(dbChunk->getAddress().coords == DBArrayChunkBase::getCoordinates());

        PersistentChunk::Pinner scope(dbChunk);
        _arrayIter._storage->compressChunk(getArrayDesc(), dbChunk, buf);
    }
}

void CachedStorage::DBArrayChunk::decompress(CompressedBuffer const& buf)
{
    PersistentChunk* dbChunk = getPersistentChunk();

    assert(dbChunk->getAddress().attId  == DBArrayChunkBase::getAttributeId());
    assert(dbChunk->getAddress().coords == DBArrayChunkBase::getCoordinates());

    _arrayIter._storage->decompressChunk(getArrayDesc(), dbChunk, buf);
}

}
