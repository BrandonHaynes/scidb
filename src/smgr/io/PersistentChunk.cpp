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
 * @brief Persistent Chunk implementation
 *
 * @author Konstantin Knizhnik <knizhnik@garret.ru>
 * @author poliocough@gmail.com
 * @author sfridella@paradigm4.com
 */

#include "PersistentChunk.h"

using namespace boost;
using namespace std;

namespace scidb
{

static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.pchunk"));

///////////////////////////////////////////////////////////////////
/// ChunkDescriptor
///////////////////////////////////////////////////////////////////

void ChunkDescriptor::getAddress(StorageAddress& addr) const
{
    addr.arrId = hdr.arrId;
    addr.attId = hdr.attId;
    addr.coords.resize(hdr.nCoordinates);
    for (size_t j = 0; j < hdr.nCoordinates; j++)
    {
        addr.coords[j] = coords[j];
    }
}


///////////////////////////////////////////////////////////////////
/// PersistentChunk
///////////////////////////////////////////////////////////////////

PersistentChunk::PersistentChunk()
    : _next(NULL),
      _prev(NULL),
      _addr(),
      _data(NULL),
      _hdr(),
      _accessCount(0),
      _raw(false),
      _waiting(false),
      _timestamp(1),
      _firstPosWithOverlaps(),
      _lastPos(),
      _lastPosWithOverlaps(),
      _storage(NULL)
{
}

PersistentChunk::~PersistentChunk()
{
    if (_accessCount != 0) {
        LOG4CXX_WARN(logger, "PersistentChunk::Destructor =" << this
                        << ", accessCount = "<<_accessCount << " is not 0");
    }
    if (_storage) {
        _storage->freeChunk(this);
    }
}

size_t PersistentChunk::count() const
{
    return _hdr.nElems ;
}

bool PersistentChunk::isCountKnown() const
{
    return (_hdr.nElems != 0);
}

void PersistentChunk::setCount(size_t count)
{
    _hdr.nElems = count;
}

bool PersistentChunk::isDelta() const
{
    return _hdr.is<ChunkHeader::DELTA_CHUNK> ();
}

void PersistentChunk::truncate(Coordinate lastCoord)
{
    _lastPos[0] = _lastPosWithOverlaps[0] = lastCoord;
}

void PersistentChunk::init()
{
    _data = NULL;
    LOG4CXX_TRACE(logger, "PersistentChunk::init =" << this << ", accessCount = "<<_accessCount);
    _accessCount = 0;
    _hdr.nElems = 0;
    _raw = false;
    _waiting = false;
    _next = _prev = NULL;
    _storage = &StorageManager::getInstance();
    _timestamp = 1;
}

RWLock& PersistentChunk::getLatch()
{
    return _storage->getChunkLatch(this);
}

void PersistentChunk::calculateBoundaries(const ArrayDesc& ad)
{
    _lastPos = _lastPosWithOverlaps = _firstPosWithOverlaps = _addr.coords;
    _hdr.instanceId = _storage->getPrimaryInstanceId(ad, _addr);
    const Dimensions& dims = ad.getDimensions();
    size_t n = dims.size();
    assert(_addr.coords.size() == n);
    for (size_t i = 0; i < n; i++)
    {
        if (_firstPosWithOverlaps[i] > dims[i].getStartMin())
        {
            _firstPosWithOverlaps[i] -= dims[i].getChunkOverlap();
        }
        _lastPos[i] = _lastPosWithOverlaps[i] += dims[i].getChunkInterval() - 1;
        if (_lastPos[i] > dims[i].getEndMax())
        {
            _lastPos[i] = dims[i].getEndMax();
        }
        if ((_lastPosWithOverlaps[i] += dims[i].getChunkOverlap()) > dims[i].getEndMax())
        {
            _lastPosWithOverlaps[i] = dims[i].getEndMax();
        }
    }
}

bool PersistentChunk::isEmpty()
{
    return _next == this;
}

void PersistentChunk::prune()
{
    _next = _prev = this;
}

void PersistentChunk::link(PersistentChunk* elem)
{
    assert((elem->_next == NULL && elem->_prev == NULL) || (elem->_next == elem && elem->_prev == elem));
    elem->_prev = this;
    elem->_next = _next;
    _next = _next->_prev = elem;
}

void PersistentChunk::unlink()
{
    _next->_prev = _prev;
    _prev->_next = _next;
    prune();
}

void PersistentChunk::beginAccess()
{
    LOG4CXX_TRACE(logger, "PersistentChunk::beginAccess =" << this << ", accessCount = "<<_accessCount);
    if (_accessCount++ == 0 && _next != NULL)
    {
        unlink();
    }
}

void PersistentChunk::setAddress(const ArrayDesc& ad, const StorageAddress& firstElem, int compressionMethod)
{
    init();
    _addr = firstElem;
    _raw = true; // new chunk is not yet initialized
    // initialize disk header of chunk
    _hdr.storageVersion = SCIDB_STORAGE_FORMAT_VERSION;
    _hdr.size = 0;
    _hdr.compressedSize = 0;
    _hdr.compressionMethod = compressionMethod;
    _hdr.arrId = _addr.arrId;
    _hdr.attId = _addr.attId;
    _hdr.nCoordinates = _addr.coords.size();
    _hdr.flags = 0;
    _hdr.pos.hdrPos = 0;
    calculateBoundaries(ad);
}

void PersistentChunk::setAddress(const ArrayDesc& ad, const ChunkDescriptor& desc)
{
    init();
    _hdr = desc.hdr;
    desc.getAddress(_addr);
    calculateBoundaries(ad);
}

int PersistentChunk::getCompressionMethod() const
{
    return _hdr.compressionMethod;
}

void PersistentChunk::setCompressionMethod(int method)
{
    assert(method>=0);
    _hdr.compressionMethod=method;
}

void* PersistentChunk::getData(const ArrayDesc& desc)
{
    if (!_accessCount) {
        throw SYSTEM_EXCEPTION(SCIDB_SE_STORAGE, SCIDB_LE_CHUNK_NOT_PINNED);
    }
    if (_hdr.pos.hdrPos != 0)
    {
        _storage->loadChunk(desc, this);
    }
    return _data;
}

void* PersistentChunk::getDataForLoad()
{
    return _data;
}

size_t PersistentChunk::getSize() const
{
    return _hdr.size;
}

size_t totalPersistentChunkAllocatedSize;

void PersistentChunk::allocate(size_t size)
{
    reallocate(size);
}

void PersistentChunk::reallocate(size_t size)
{
    assert(size>0);
    void* tmp = ::realloc(_data, size);
    if (!tmp) {
        throw SYSTEM_EXCEPTION(SCIDB_SE_STORAGE, SCIDB_LE_CANT_REALLOCATE_MEMORY);
    }
    _data = tmp;
    _hdr.size = size;
}

void PersistentChunk::free()
{
    if (isDebug() && _data) { memset(_data,0,_hdr.size); }
    ::free(_data);
    _data = NULL;
}

Coordinates const& PersistentChunk::getFirstPosition(bool withOverlap) const
{
    return withOverlap ? _firstPosWithOverlaps : _addr.coords;
}

Coordinates const& PersistentChunk::getLastPosition(bool withOverlap) const
{
    return withOverlap ? _lastPosWithOverlaps : _lastPos;
}

bool PersistentChunk::pin() const
{
    LOG4CXX_TRACE(logger, "PersistentChunk::pin() this=" << this);
    _storage->pinChunk(this);
    currentStatistics->pinnedSize += getSize();
    currentStatistics->pinnedChunks++;
    return true;
}

void PersistentChunk::unPin() const
{
    LOG4CXX_TRACE(logger, "PersistentChunk::unPin() this=" << this);
    _storage->unpinChunk(this);
}

}
