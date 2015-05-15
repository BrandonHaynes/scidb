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
 * @file MemArray.cpp
 *
 * @brief Temporary (in-memory) array implementation
 *
 * @author Konstantin Knizhnik <knizhnik@garret.ru>
 * @author poliocough@gmail.com
 * @author others
 */

#include <log4cxx/logger.h>
#include <util/Platform.h>
#include <util/FileIO.h>
#include <util/Counter.h>
#include <array/MemArray.h>
#include <system/Exceptions.h>
#include <system/Config.h>
#include <array/Compressor.h>
#include <system/SciDBConfigOptions.h>
#include <query/Statistics.h>
#include <system/Utils.h>
#include <array/Tile.h>
#include <array/TileIteratorAdaptors.h>

namespace scidb
{
    using namespace boost;
    using namespace std;

    const size_t MAX_SPARSE_CHUNK_INIT_SIZE = 1*MiB;

    const bool _sDebug = false;

    // Logger for operator. static to prevent visibility of variable outside of file
    static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.array.memarray"));

    //
    // MemArray
    //

    MemArray::MemArray(ArrayDesc const& arr, boost::shared_ptr<Query> const& query)
    : desc(arr)
    {
        _query=query;
        initLRU();
    }

    MemArray::MemArray(boost::shared_ptr<Array>& input, boost::shared_ptr<Query> const& query, bool vertical)
    : desc(input->getArrayDesc())
    {
        _query=query;
        initLRU();
        append(input, vertical);
    }

    MemArray::~MemArray()
    {
        SharedMemCache::getInstance().cleanupArray(*this);
    }

    void MemArray::initLRU()
    {
    }

    void MemArray::pinChunk(LruMemChunk& chunk)
    {
        if (_sDebug) {
            LOG4CXX_TRACE(logger, "PIN: chunk="<<(void*)&chunk
                          << ",  accessCount is " << chunk._accessCount
                          << "Array="<<(void*)this << ",  name '" << chunk.arrayDesc->getName());
        }
        Query::getValidQueryPtr(_query);
        SharedMemCache::getInstance().pinChunk(chunk);
    }

    void MemArray::unpinChunk(LruMemChunk& chunk)
    {
        if (_sDebug) {
            LOG4CXX_TRACE(logger, "UNPIN: chunk="<<(void*)&chunk
                          << ",  accessCount is " << chunk._accessCount
                          << "Array="<<(void*)this << ",  name '" << chunk.arrayDesc->getName());
        }
        // unpin should always succeed because it is called during exception handling
        SharedMemCache::getInstance().unpinChunk(chunk);
    }

    string const& MemArray::getName() const
    {
        return desc.getName();
    }

    ArrayID MemArray::getHandle() const
    {
        return desc.getId();
    }

    ArrayDesc const& MemArray::getArrayDesc() const
    {
        return desc;
    }

    Chunk& MemArray::operator[](Address const& addr)
    {
        ScopedMutexLock cs(_mutex);
        LruMemChunk& chunk = _chunks[addr];
        if (!chunk.isInitialized()) {
            AttributeDesc const* bitmapAttr = desc.getEmptyBitmapAttribute();
            Chunk* bitmapChunk = NULL;
            if (bitmapAttr != NULL && bitmapAttr->getId() != addr.attId) {
                Address bitmapAddr(bitmapAttr->getId(), addr.coords);
                bitmapChunk = &(*this)[bitmapAddr];
            }
            chunk.initialize(this, &desc,
                             addr,
                             desc.getAttributes()[addr.attId].getDefaultCompressionMethod());
            chunk.setBitmapChunk(bitmapChunk);
            if (bitmapChunk != NULL) {
                bitmapChunk->unPin();
            }
            chunk.prune();
        }
        pinChunk(chunk);
        assert(chunk.isEmpty());
        return chunk;
    }

    boost::shared_ptr<ArrayIterator> MemArray::getIterator(AttributeID attId)
    {
        return boost::shared_ptr<ArrayIterator>(new MemArrayIterator(*this, attId));
    }

    boost::shared_ptr<ConstArrayIterator> MemArray::getConstIterator(AttributeID attId) const
    {
        return ((MemArray*)this)->getIterator(attId);
    }

    /**
     * @brief SharedMemCache::SharedMemCache
     */

    SharedMemCache::SharedMemCache() :
        _usedMemSize(0),
        _usedMemThreshold(DEFAULT_MEM_THRESHOLD * MiB), /*<< must be rewritten after config load */
        _swapNum(0),
        _loadsNum(0),
        _dropsNum(0),
        _genCount(0)
    {
    }

    /* Initialize the datastores used for the temporary disk storage needed
       by mem arrays.
     */
    void SharedMemCache::initSharedMemCache(uint64_t memThreshold, const char* basePath)
    {
        _usedMemThreshold = memThreshold;
        _datastores.initDataStores(basePath);
        _datastores.clearAllDataStores();
    }

    /*
     * Some notes:
     *  The LRU contains only chunks that are currently in-memory AND not pinned (access count 0).
     *  Invariant: if a chunk is on the LRU, its access count is 0; also if a chunk's access count is 0, it's on the LRU.
     *  Invariant: if a chunk is on the LRU, its size equals _sizeAtLastUnPin.
     *  If a chunk is pinned, it could be accessed, or modified. We know nothing about its real "size". We only know "_sizeAtLastUnPin".
     *  _usedMemSize is the sum of the sizes of all the pinned chunks AND all the chunks on the LRU.
     * -AP 1/30/13
     */

    void SharedMemCache::pinChunk(LruMemChunk &chunk)
    {
        ScopedMutexLock cs(_mutex);
        if (chunk._accessCount++ == 0) {
            chunk._sizeAtLastUnPin = chunk.size;  //mostly redundant. just in case someone is doing something clever
            if (chunk.getConstData() == NULL) {
                if (_usedMemSize > _usedMemThreshold) {
                    swapOut();
                }
                if (chunk.size != 0) {
                    assert(chunk._dsOffset >= 0);
                    chunk.reallocate(chunk.size);
                    assert(chunk.getConstData());
                    const MemArray* array = (const MemArray*)chunk.array;
                    assert(array->_datastore);
                    ++_loadsNum;
                    _usedMemSize += chunk.size;
                    array->_datastore->readData(chunk._dsOffset, chunk.getData(), chunk.size);
                    chunk.markClean();
                }
            } else {
                assert(!chunk.isEmpty());
                chunk.removeFromLru();
            }
        }
    }

    void SharedMemCache::unpinChunk(LruMemChunk &chunk)
    {
        ScopedMutexLock cs(_mutex);
        assert(chunk._accessCount > 0);
        if (--chunk._accessCount == 0) {
            //if chunk was changed, its size could be different
            //subtract OLD size and add NEW size to _usedMemSize to account for the delta
            assert(_usedMemSize >= chunk._sizeAtLastUnPin);
            _usedMemSize -= chunk._sizeAtLastUnPin;
            if (chunk.getConstData() == NULL)
            {
                assert(chunk.size == 0);
                chunk._sizeAtLastUnPin = 0;
            }
            else
            {
                _usedMemSize += chunk.size;
                chunk._sizeAtLastUnPin = chunk.size;
                assert(chunk.isEmpty());
                chunk.pushToLru();
                if (_usedMemSize > _usedMemThreshold) {
                    swapOut();
                }
            }
        }
    }

    void SharedMemCache::swapOut()
    {
        // this function must be called under _mutex lock
        while (!_theLru.empty() && _usedMemSize > _usedMemThreshold) {

            LruMemChunk* victim = NULL;
            bool popped = _theLru.pop(victim);
            SCIDB_ASSERT(popped);
            assert(victim!=NULL);
            assert(victim->_accessCount == 0);
            assert(victim->getConstData() != NULL);
            assert(!victim->isEmpty());
            victim->prune();
            _usedMemSize -= victim->size; //victim is not pinned, so the size is correct
            if (victim->isDirty())
            {
                MemArray* array = (MemArray*)victim->array;
                if (!array->_datastore) {
                    array->_datastore = _datastores.getDataStore(_genCount++);
                }
                size_t overhead = array->_datastore->getOverhead();
                if (victim->_dsOffset < 0 || (victim->_dsAlloc - overhead < victim->size)) {
                    if (victim->_dsOffset >= 0)
                    {
                        LOG4CXX_TRACE(logger, "SharedMemCache::swapOut : freeing chunk at offset " <<
                                      victim->_dsOffset);
                        array->_datastore->freeChunk(victim->_dsOffset, victim->_dsAlloc);
                    }
                    victim->_dsOffset = array->_datastore->allocateSpace(victim->size, victim->_dsAlloc);
                }
                array->_datastore->writeData(victim->_dsOffset,
                                             victim->getData(),
                                             victim->size,
                                             victim->_dsAlloc);
                ++_swapNum;
            }
            else
            {
                ++_dropsNum;
            }
            victim->free();
        }
        SCIDB_ASSERT(sizeCoherent());
    }

    void SharedMemCache::deleteChunk(LruMemChunk &chunk)
    {
        ScopedMutexLock cs(_mutex);
        assert(chunk._accessCount == 0);
        chunk.removeFromLru();
    }

    void SharedMemCache::cleanupArray(MemArray &array)
    {
        ScopedMutexLock cs(_mutex);
        for (map<Address, LruMemChunk>::iterator i = array._chunks.begin();
             i != array._chunks.end(); i++)
        {
            LruMemChunk &chunk = i->second;
            if (chunk.getConstData() != NULL) {
                //chunk could be pinned or just on the LRU.
                _usedMemSize -= chunk._sizeAtLastUnPin;
            }
            if (chunk._accessCount > 0) {
                LOG4CXX_DEBUG(logger, "Warning: accessCount is " << chunk._accessCount
                              << " due to MemArray cleanup '" << chunk.arrayDesc->getName());
            }
            if (!chunk.isEmpty()) {
                chunk.removeFromLru();
            }
        }
        SCIDB_ASSERT(sizeCoherent());

        /* Remove the data store for this array from disk (it will be unlinked when the
           array itself is destroyed
         */
        if (array._datastore)
        {
            DataStore::Guid guid = array._datastore->getGuid();
            _datastores.closeDataStore(guid, true /* and remove from disk */);
        }
    }

    uint64_t SharedMemCache::computeSizeOfLRU()
    {
        list<LruMemChunk*>::iterator iter = _theLru.begin();
        size_t res = 0;
        while (iter != _theLru.end())
        {
            LruMemChunk* ch = (*iter);
            res += ch->_sizeAtLastUnPin;
            ++iter;
        }
        return res;
    }

    bool SharedMemCache::sizeCoherent()
    {
        ScopedMutexLock cs(_mutex);
        size_t lruSize = computeSizeOfLRU();
        LOG4CXX_TRACE(logger, "SharedMemCache::sizeCoherent available bytes "<<lruSize<<" total bytes "<<_usedMemSize);
        //lruSize does not include chunks that are pinned and not on the LRU. So we can't always check for strict equality.
        return lruSize <= _usedMemSize;
    }

    SharedMemCache SharedMemCache::_sharedMemCache;


    //
    // Temporary (in-memory) array iterator
    //
    inline void MemArrayIterator::position()
    {
        if (!positioned) {
            reset();
        }
    }

    MemArrayIterator::MemArrayIterator(MemArray& arr, AttributeID attId)
    : _array(arr)
    {
        addr.attId = attId;
        currChunk = NULL;
        last = _array._chunks.end();
        positioned = false;
    }

    ConstChunk const& MemArrayIterator::getChunk()
    {
        position();
        if (!currChunk) {
            throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_NO_CURRENT_CHUNK);
        }
        return *currChunk;
    }

    bool MemArrayIterator::end()
    {
        position();
        return currChunk == NULL;
    }

    void MemArrayIterator::operator ++()
    {
        position();
        ++curr;
        setCurrent();
    }

    Coordinates const& MemArrayIterator::getPosition()
    {
        position();
        if (!currChunk) {
            throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_NO_CURRENT_CHUNK);
        }
        return currChunk->getFirstPosition(false);
    }

    bool MemArrayIterator::setPosition(Coordinates const& pos)
    {
        ScopedMutexLock cs(_array._mutex);
        currChunk = NULL;
        addr.coords = pos;
        _array.desc.getChunkPositionFor(addr.coords);
        curr = _array._chunks.find(addr);
        positioned = true;
        if (curr != last) {
            currChunk = &_array._chunks[addr];
            return true;
        } else {
            return false;
        }
    }

    void MemArrayIterator::setCurrent()
    {
        currChunk = (curr != last && curr->second.addr.attId == addr.attId) ? &curr->second : NULL;
    }

    void MemArrayIterator::reset()
    {
        positioned = true;
        ScopedMutexLock cs(_array._mutex);
        curr = _array._chunks.begin();
        while (curr != last && curr->second.addr.attId != addr.attId) {
            ++curr;
        }
        setCurrent();
    }

    void MemArrayIterator::deleteChunk(Chunk& aChunk)
    {
        LruMemChunk& chunk = (LruMemChunk&)aChunk;
        chunk._accessCount = 0;
        ScopedMutexLock cs(_array._mutex);
        SharedMemCache::getInstance().deleteChunk(chunk);
        _array._chunks.erase(chunk.addr);
    }

    Chunk& MemArrayIterator::newChunk(Coordinates const& pos)
    {
        if (!_array.desc.contains(pos)) {
            throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_CHUNK_OUT_OF_BOUNDARIES);
        }
        addr.coords = pos;
        _array.desc.getChunkPositionFor(addr.coords);

        ScopedMutexLock cs(_array._mutex);
        map<Address, LruMemChunk>::const_iterator chIter = _array._chunks.find(addr);
        if (chIter!=_array._chunks.end() &&
            // yet another hack to work around the fact that an emptytag chunk
            // is automatically created by MemArray
            addr.attId != _array.desc.getAttributes().size()-1) {
            throw SYSTEM_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_CHUNK_ALREADY_EXISTS)
            << CoordsToStr(addr.coords);
        }
        return _array[addr]; //pinned
    }

    Chunk& MemArrayIterator::newChunk(Coordinates const& pos, int compressionMethod)
    {
        Chunk& chunk = newChunk(pos);
        ((MemChunk&)chunk).compressionMethod = compressionMethod;
        return chunk;
    }

} // scidb namespace
