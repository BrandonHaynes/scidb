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
 * @file MemChunk.cpp
 *
 * @brief Temporary (in-memory) chunk implementation
 *
 * @author Konstantin Knizhnik <knizhnik@garret.ru>
 * @author poliocough@gmail.com
 * @author others
 */

#include <log4cxx/logger.h>
#include <util/Platform.h>
#include <array/MemArray.h>
#include <system/Exceptions.h>

#ifndef SCIDB_CLIENT
#include <system/Config.h>
#endif

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

    // Logger. static to prevent visibility of variable outside of file
    static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.array.memchunk"));

    //
    // MemChunk
    //
    MemChunk::MemChunk()
        : data(NULL),
          dirty(false),
          size(0),
          arrayDesc(NULL),
          bitmapChunk(NULL),
          array(NULL)
    {
    }

    Array const& MemChunk::getArray() const
    {
        assert(array);
        return *array;
    }

    boost::shared_ptr<ConstRLEEmptyBitmap> MemChunk::getEmptyBitmap() const
    {
        return (emptyBitmap ? emptyBitmap :
                (bitmapChunk ? bitmapChunk->getEmptyBitmap() :
                 ConstChunk::getEmptyBitmap()));
    }

    bool MemChunk::isMaterialized() const
    {
        return true;
    }

    bool MemChunk::isTemporary() const
    {
        return true;
    }

    size_t MemChunk::count() const
    {
        return nElems != 0 ? nElems : ConstChunk::count();
    }

    bool MemChunk::isCountKnown() const
    {
        return nElems != 0 || ConstChunk::isCountKnown();
    }

    void MemChunk::setCount(size_t count)
    {
        nElems = count;
    }

    ConstChunk const* MemChunk::getBitmapChunk() const
    {
        return (bitmapChunk != NULL ?
                bitmapChunk :
                (getAttributeDesc().isEmptyIndicator() ? this : NULL));
    }

    void MemChunk::setEmptyBitmap(boost::shared_ptr<ConstRLEEmptyBitmap> const& bitmap)
    {
        emptyBitmap = bitmap;
        if (bitmap) {
            bitmapChunk = this;
        }
    }

    void MemChunk::setBitmapChunk(Chunk* newBitmapChunk)
    {
        bitmapChunk = newBitmapChunk != NULL
           && (arrayDesc == NULL || !getAttributeDesc().isEmptyIndicator())
           && (newBitmapChunk->getAttributeDesc().isEmptyIndicator()
               || (arrayDesc->getEmptyBitmapAttribute() != NULL))
        ? (Chunk*)newBitmapChunk->getBitmapChunk() : NULL;
    }

  #ifndef SCIDB_CLIENT
    void LruMemChunk::initialize(ConstChunk const& srcChunk)
    {
        assert(false);
        throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_UNREACHABLE_CODE)
            << "LruMemChunk::initialize";
    }

    void LruMemChunk::initialize(MemArray const* arr, ArrayDesc const* desc, const Address& firstElem, int compMethod)
    {
        assert(arr);
        MemChunk::initialize(arr, desc, firstElem, compMethod);
    }

    void LruMemChunk::initialize(Array const* arr, ArrayDesc const* desc, const Address& firstElem, int compMethod)
    {
        assert(false);
        throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_UNREACHABLE_CODE) << "LruMemChunk::initialize";
    }

    boost::shared_ptr<ChunkIterator> LruMemChunk::getIterator(boost::shared_ptr<Query> const& query, int iterationMode)
    {
        if (Query::getValidQueryPtr(static_cast<const MemArray*>(array)->_query) != query) {
            throw (SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_INVALID_FUNCTION_ARGUMENT) << "invalid query");
        }
        return MemChunk::getIterator(query, iterationMode);
    }
    boost::shared_ptr<ConstChunkIterator> LruMemChunk::getConstIterator(int iterationMode) const
    {
        boost::shared_ptr<Query> query(Query::getValidQueryPtr(static_cast<const MemArray*>(array)->_query));
        return MemChunk::getConstIterator(query, iterationMode);
    }
  #endif

    void MemChunk::initialize(ConstChunk const& srcChunk)
    {
        Address addr(srcChunk.getAttributeDesc().getId(), srcChunk.getFirstPosition(false));
        initialize(&srcChunk.getArray(),
                   &srcChunk.getArrayDesc(),
                   addr,
                   srcChunk.getCompressionMethod());
    }

    void MemChunk::initialize(Array const* arr, ArrayDesc const* desc,
                              const Address& firstElem, int compMethod)
    {
        array = arr;
        arrayDesc = desc;
        nElems = 0;
        addr = firstElem;
        compressionMethod = compMethod;
        firstPos = lastPos = lastPosWithOverlaps = firstPosWithOverlaps = addr.coords;
        const Dimensions& dims = desc->getDimensions();
        for (size_t i = 0, n = dims.size(); i < n; i++) {
            if (firstPos[i] < dims[i].getStartMin() ||
                lastPos[i] > dims[i].getEndMax()) {
                throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_CHUNK_OUT_OF_BOUNDARIES);
            }
            if ((firstPosWithOverlaps[i] -= dims[i].getChunkOverlap()) < dims[i].getStartMin()) {
                firstPosWithOverlaps[i] = dims[i].getStartMin();
            }
            lastPos[i] = lastPosWithOverlaps[i] += dims[i].getChunkInterval() - 1;
            lastPosWithOverlaps[i] += dims[i].getChunkOverlap();

            if (lastPos[i] > dims[i].getEndMax()) {
                lastPos[i] = dims[i].getEndMax();
            }
            if (lastPosWithOverlaps[i] > dims[i].getEndMax()) {
                lastPosWithOverlaps[i] = dims[i].getEndMax();
            }
        }
    }

    const ArrayDesc& MemChunk::getArrayDesc() const
    {
        return *arrayDesc;
    }

    const AttributeDesc& MemChunk::getAttributeDesc() const
    {
        return arrayDesc->getAttributes()[addr.attId];
    }

    void MemChunk::allocate(size_t size)
    {
        reallocate(size);
    }

    void MemChunk::reallocate(size_t newSize)
    {
        assert(newSize>0);
        void* tmp = ::realloc(data, newSize);
        if (!tmp) {
            throw SYSTEM_EXCEPTION(SCIDB_SE_NO_MEMORY, SCIDB_LE_CANT_REALLOCATE_MEMORY);
        }
        data = tmp;
        size = newSize;
    }

    void MemChunk::free()
    {
        if (isDebug() && data) {
            memset(data, 0, size);
        }
        ::free(data);
        data = NULL;
    }

    MemChunk::~MemChunk()
    {
        free();
    }

    Coordinates const& MemChunk::getFirstPosition(bool withOverlap) const
    {
        return withOverlap ? firstPosWithOverlaps : firstPos;
    }

    Coordinates const& MemChunk::getLastPosition(bool withOverlap) const
    {
        return withOverlap ? lastPosWithOverlaps : lastPos;
    }

    void MemChunk::fillRLEBitmap()
    {
        boost::shared_ptr<Query> emptyQuery;
        RLEChunkIterator iterator(*arrayDesc, addr.attId, this, NULL, ChunkIterator::NO_EMPTY_CHECK, emptyQuery);
        boost::shared_ptr<ConstRLEEmptyBitmap> emptyBitmap = iterator.getEmptyBitmap();
        allocate(emptyBitmap->packedSize());
        emptyBitmap->pack(static_cast<char*>(getData()));
    }

    boost::shared_ptr<ChunkIterator> MemChunk::getIterator(boost::shared_ptr<Query> const& query, int iterationMode)
    {
        return boost::shared_ptr<ChunkIterator>(new RLEChunkIterator(*arrayDesc, addr.attId, this, bitmapChunk, iterationMode, query));

    }
    boost::shared_ptr<ConstChunkIterator> MemChunk::getConstIterator(int iterationMode) const
    {
        boost::shared_ptr<Query> emptyQuery;
        return MemChunk::getConstIterator(emptyQuery, iterationMode);
    }
    boost::shared_ptr<ConstChunkIterator> MemChunk::getConstIterator(boost::shared_ptr<Query> const& query, int iterationMode) const
    {
        PinBuffer scope(*this);
        if (getAttributeDesc().isEmptyIndicator() || getConstData() == NULL) {
            return boost::make_shared<RLEBitmapChunkIterator>(*arrayDesc, addr.attId,
                                                              (Chunk*)this, bitmapChunk,
                                                              iterationMode, query);
        } else if ((iterationMode & ConstChunkIterator::INTENDED_TILE_MODE) ||
                   (iterationMode & ConstChunkIterator::TILE_MODE)) { //old tile mode

            return boost::make_shared<RLEConstChunkIterator>(*arrayDesc, addr.attId,
                                                             (Chunk*)this, bitmapChunk,
                                                             iterationMode, query);
        }

        // non-tile mode, but using the new tiles for read ahead buffering
        boost::shared_ptr<RLETileConstChunkIterator> tiledIter =

        boost::make_shared<RLETileConstChunkIterator>(*arrayDesc,
                                                      addr.attId,
                                                      (Chunk*)this,
                                                      bitmapChunk,
                                                      iterationMode,
                                                      query);
        return boost::make_shared< BufferedConstChunkIterator< boost::shared_ptr<RLETileConstChunkIterator> > >(tiledIter, query);
    }

    bool MemChunk::pin() const
    {
        return false;
    }

    void MemChunk::unPin() const
    {
    }

    void MemChunk::write(const boost::shared_ptr<Query>& query)
    {
        // MemChunks can be stand-alone and not always have the query context,
        // dont validate query (yet?)
    }

    void MemChunk::compress(CompressedBuffer& buf,
                            boost::shared_ptr<ConstRLEEmptyBitmap>& emptyBitmap) const
    {
        ConstChunk const* src = this;
        MemChunk closure;
        if (emptyBitmap && getBitmapSize() == 0) {
            closure.initialize(*this);
            makeClosure(closure, emptyBitmap);
            src = &closure;
        }
        PinBuffer scope(*src);
        size_t decompressedSize = src->getSize();
        if (!emptyBitmap) {
            decompressedSize -= src->getBitmapSize();
        }
        buf.allocate(decompressedSize);
        size_t compressedSize = CompressorFactory::getInstance().getCompressors()[compressionMethod]->compress(buf.getData(), *src, decompressedSize);
        if (compressedSize == decompressedSize) {
            memcpy(buf.getData(), src->getData(), decompressedSize);
        } else {
            buf.reallocate(compressedSize);
        }
        buf.setDecompressedSize(decompressedSize);
        buf.setCompressionMethod(compressionMethod);
    }

    void MemChunk::decompress(CompressedBuffer const& buf)
    {
        allocate(buf.getDecompressedSize());
        if (buf.getSize() == buf.getDecompressedSize()) {
            memcpy(getData(), buf.getData(), buf.getSize());
        } else {
            CompressorFactory::getInstance().getCompressors()[buf.getCompressionMethod()]->decompress(buf.getData(), buf.getSize(), *this);
        }
    }

#ifndef SCIDB_CLIENT

    //
    // LruMemChunk
    //
    LruMemChunk::LruMemChunk(): _whereInLru(SharedMemCache::getLru().end())
    {
        _dsOffset = -1;
        _dsAlloc = 0;
        _accessCount = 0;
        _sizeAtLastUnPin = 0;
    }

    LruMemChunk::~LruMemChunk()
    {
        // If exception is raised during update of array, then access counter may be non zero
        //assert(accessCount == 0);
        removeFromLru();
    }

    bool LruMemChunk::isEmpty() const {
        return _whereInLru == SharedMemCache::getLru().end();
    }

    /**
     * Take a note that this LruMemChunk has been removed from the Lru.
     */
    void LruMemChunk::prune() {
        _whereInLru = SharedMemCache::getLru().end();
    }

    void LruMemChunk::removeFromLru() {
        if (!isEmpty()) {
            SharedMemCache::getLru().erase(_whereInLru);
            prune();
        }
    }

    void LruMemChunk::pushToLru() {
        assert(isEmpty());
        _whereInLru = SharedMemCache::getLru().push(this);
    }

    bool LruMemChunk::isTemporary() const
    {
        return false;
    }

    bool LruMemChunk::pin() const
    {
        if (currentStatistics) {
            currentStatistics->pinnedSize += size;
            currentStatistics->pinnedChunks++;
        }
        ((MemArray*)array)->pinChunk(*(LruMemChunk*)this);
        return true;
    }

    void LruMemChunk::unPin() const
    {
        ((MemArray*)array)->unpinChunk(*(LruMemChunk*)this);
    }

    void LruMemChunk::write(const boost::shared_ptr<Query>& query)
    {
        if (Query::getValidQueryPtr(static_cast<const MemArray*>(array)->_query) != query) {
            throw (SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_INVALID_FUNCTION_ARGUMENT) << "invalid query");
        }
        unPin();
    }

#endif

    //
    // RLEChunkIterator
    //
    //
    boost::shared_ptr<ConstRLEEmptyBitmap> BaseChunkIterator::getEmptyBitmap()
    {
        return emptyBitmap;
    }

    int BaseChunkIterator::getMode()
    {
        return mode;
    }

    bool BaseChunkIterator::isEmpty()
    {
        return false;
    }

    bool BaseChunkIterator::end()
    {
        return !hasCurrent;
    }

    ConstChunk const& BaseChunkIterator::getChunk()
    {
        return *dataChunk;
    }

    void BaseChunkIterator::reset()
    {
        emptyBitmapIterator.reset();
        hasCurrent = !emptyBitmapIterator.end();
        tilePos = 0;
    }

    Coordinates const& BaseChunkIterator::getPosition()
    {
        if (!hasCurrent) {
            throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_NO_CURRENT_ELEMENT);
        }
        pos2coord((mode & TILE_MODE) ? tilePos : emptyBitmapIterator.getLPos(), currPos);
        return currPos;
    }

    void BaseChunkIterator::operator ++()
    {
        if (!hasCurrent)
            throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_NO_CURRENT_ELEMENT);
        if (mode & TILE_MODE) {
            tilePos += tileSize;
            hasCurrent = tilePos < _logicalChunkSize;
        } else {
            ++emptyBitmapIterator;
            hasCurrent = !emptyBitmapIterator.end();
        }
    }

    bool BaseChunkIterator::setPosition(Coordinates const& coord)
    {
        if (mode & TILE_MODE) {
            tilePos = coord2pos(coord);
            if (tilePos % tileSize)
                throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_TILE_NOT_ALIGNED);
            return hasCurrent = tilePos < _logicalChunkSize;
        } else {
            if (!dataChunk->contains(coord, !(mode & IGNORE_OVERLAPS))) {
                return hasCurrent = false;
            }
            position_t pos = coord2pos(coord);
            bool res = (hasCurrent = emptyBitmapIterator.setPosition(pos));
            return res;
        }
        return false;
    }

    BaseChunkIterator::~BaseChunkIterator()
    {
        if (_sDebug) {
            LOG4CXX_TRACE(logger, "~BCI this=" << this
                          << ", chunk=" << dataChunk
                          << ", pinned=" << dataChunkPinned);
        }
        if (dataChunkPinned) {
            dataChunk->unPin();
        }
    }
    void BaseChunkIterator::flush()
    {
        throw USER_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "BaseChunkIterator::flush";
    }

    void BaseChunkIterator::writeItem(const  Value& item)
    {
        throw USER_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "BaseChunkIterator::writeItem";
    }

    BaseChunkIterator::BaseChunkIterator(ArrayDesc const& desc,
                                         AttributeID aid,
                                         Chunk* data,
                                         int iterationMode,
                                         boost::shared_ptr<Query> const& query)
    : CoordinatesMapper(*data),
      array(desc),
      attrID(aid),
      attr(array.getAttributes()[aid]),
      dataChunk(data),
      hasCurrent(false),
      mode(iterationMode),
      currPos(array.getDimensions().size()),
      typeId(attr.getType()),
      type(TypeLibrary::getType(typeId)),
      defaultValue(attr.getDefaultValue()),
      isEmptyIndicator(attr.isEmptyIndicator()),
      _query(query)
    {
        const Dimensions& dim = array.getDimensions();
        size_t nDims = dim.size();
        dataChunkPinned = dataChunk->pin();

        if (_sDebug) {
            LOG4CXX_TRACE(logger, "BCI::BCI this=" << this
                          << ", chunk=" << dataChunk
                          << ", pinned=" << dataChunkPinned);
        }
        position_t nElems = 1;
        hasOverlap = false;
        Coordinates const& firstPos(data->getFirstPosition(true));
        Coordinates const& lastPos(data->getLastPosition(true));

        for (size_t i = 0; i < nDims; i++) {
            nElems *= lastPos[i] - firstPos[i] + 1;
            hasOverlap = hasOverlap || (dim[i].getChunkOverlap() != 0);
        }

        tileSize = 1;
#ifndef SCIDB_CLIENT
        if (Config::getInstance()->getOption<int>(CONFIG_TILE_SIZE) > 0) {
            if ((iterationMode & INTENDED_TILE_MODE) && !attr.isNullable() && type.bitSize() >= 8) {
                mode |= TILE_MODE;
            }
            tileSize = Config::getInstance()->getOption<int>(CONFIG_TILE_SIZE);
            if (Config::getInstance()->getOption<int>(CONFIG_TILES_PER_CHUNK) > 0) {
                size_t tilesPerChunk = Config::getInstance()->getOption<int>(CONFIG_TILES_PER_CHUNK);
                tileSize = max(tileSize, _logicalChunkSize/tilesPerChunk);
            }
        }
#endif

        isEmptyable = array.getEmptyBitmapAttribute() != NULL;
        tilePos = 0;
    }

    //
    // Constant RLE chunk
    //

RLEConstChunkIterator::RLEConstChunkIterator(ArrayDesc const& desc,
                                             AttributeID attr, Chunk* data, Chunk* bitmap, int iterationMode,
                                             boost::shared_ptr<Query> const& query)
    : BaseChunkIterator(desc, attr, data, iterationMode, query),
      payload((char const*)data->getConstData()),
      payloadIterator(&payload),
      value((mode & TILE_MODE) ? Value(type,Value::asTile) : Value(type))
    {
        if (_sDebug) {
            LOG4CXX_TRACE(logger, "RLEConstChunkIterator::RLEConstChunkIterator this="<<this
                          <<" data="<<data
                          <<" bitmap "<<bitmap
                          <<" attr "<<attr
                          <<" payload data="
                          << data->getData()
                          << " #segments=" << payload.nSegments());
        }

        if (((iterationMode & APPEND_CHUNK) || bitmap == NULL) && payload.packedSize() < data->getSize()) {
            emptyBitmap = boost::shared_ptr<ConstRLEEmptyBitmap>(new ConstRLEEmptyBitmap((char*)data->getData() + payload.packedSize()));
        } else if (bitmap != NULL) {
            emptyBitmap = bitmap->getEmptyBitmap();
        }
        if (!emptyBitmap) {
            emptyBitmap = shared_ptr<RLEEmptyBitmap>(new RLEEmptyBitmap(_logicalChunkSize));
        }
        if (hasOverlap && (iterationMode & IGNORE_OVERLAPS)) {
            emptyBitmap = emptyBitmap->cut(data->getFirstPosition(true),
                                           data->getLastPosition(true),
                                           data->getFirstPosition(false),
                                           data->getLastPosition(false));
        }
        if (_sDebug) {
            LOG4CXX_TRACE(logger, "RLEConstChunkIterator::RLEConstChunkIterator this="<<this
                          <<" ebmCount="<< emptyBitmap->count()
                          <<" pCount="<<payload.count());
        }
        assert(emptyBitmap->count() <= payload.count());

        emptyBitmapIterator = emptyBitmap->getIterator();
        reset();
        if (_sDebug) {
            LOG4CXX_TRACE(logger, "RLEConstChunkIterator::RLEConstChunkIterator this="<<this
                          <<" data="<<data
                          <<" bitmap "<<bitmap
                          <<" attr "<<attr
                          <<" payload data="
                          << data->getData()
                          << " #segments=" << payload.nSegments()
                          <<" pCount="<<payload.count());
        }
    }

    void RLEConstChunkIterator::reset()
    {
        BaseChunkIterator::reset();
        if (hasCurrent) {
            while (true)
            {
                if (!payloadIterator.setPosition(emptyBitmapIterator.getPPos()))
                    throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_OPERATION_FAILED) << "setPosition";
                break;
            }
        }
    }

    Value& RLEConstChunkIterator::getItem()
    {
        if (!hasCurrent)
            throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_NO_CURRENT_ELEMENT);
        if (mode & TILE_MODE) {
            position_t end = min(tilePos + tileSize, _logicalChunkSize);
            value.getTile()->unPackTile(payload, *emptyBitmap, tilePos, end);
        } else {
            payloadIterator.getItem(value);
        }
        return value;
    }

    void RLEConstChunkIterator::operator ++()
    {
        if (mode & TILE_MODE) {
            tilePos += tileSize;
            hasCurrent = tilePos < _logicalChunkSize;
        } else {
            if (!hasCurrent)
                throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_NO_CURRENT_ELEMENT);
            ++emptyBitmapIterator;
            if (!emptyBitmapIterator.end()) {
                if (!payloadIterator.end()) {
                    ++payloadIterator;
                }
                while (true) {
                    position_t pos = emptyBitmapIterator.getPPos();
                    if (payloadIterator.end() || payloadIterator.getPPos() != pos)
                    {
                        if (!payloadIterator.setPosition(pos))
                            throw SYSTEM_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_OPERATION_FAILED) << "setPosition";
                    }
                    return;
                }
            }
            hasCurrent = false;
        }
    }

    bool RLEConstChunkIterator::setPosition(Coordinates const& coord)
    {
        if (!BaseChunkIterator::setPosition(coord)) {
            return false;
        }
        if (!(mode & TILE_MODE) && !payloadIterator.setPosition(emptyBitmapIterator.getPPos())) {
            throw SYSTEM_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_OPERATION_FAILED) << "setPosition";
        }
        return true;
    }

    //
    // RLE Bitmap chunk iterator
    //

    RLEBitmapChunkIterator::RLEBitmapChunkIterator(ArrayDesc const& desc, AttributeID attr,
                                                   Chunk* data, Chunk* bitmap, int iterationMode,
                                                   boost::shared_ptr<Query> const& query)
    : BaseChunkIterator(desc, attr, data, iterationMode, query),
      value((mode & TILE_MODE) ? Value(type,Value::asTile) : Value(type))
    {
        UnPinner dataUP(dataChunk);
        dataChunkPinned=false; // bitmap is copied out

        if (data->getData() == NULL) {
            emptyBitmap = shared_ptr<RLEEmptyBitmap>(new RLEEmptyBitmap(_logicalChunkSize));
        } else {
            emptyBitmap = data->getEmptyBitmap();
            if (hasOverlap && (iterationMode & IGNORE_OVERLAPS)) {
                emptyBitmap = emptyBitmap->cut(data->getFirstPosition(true),
                                               data->getLastPosition(true),
                                               data->getFirstPosition(false),
                                               data->getLastPosition(false));
            }
        }
        emptyBitmapIterator = emptyBitmap->getIterator();
        hasCurrent = !emptyBitmapIterator.end();
        trueValue.setBool(true);
    }

    Value& RLEBitmapChunkIterator::getItem()
    {
        if (!hasCurrent)
            throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_NO_CURRENT_ELEMENT);
        if (mode & TILE_MODE) {
            position_t end = min(tilePos + tileSize, _logicalChunkSize);
            value.getTile()->unPackTile(*emptyBitmap, tilePos, end);
            return value;
        } else {
            return trueValue;
        }
    }


    //
    // RLE write chunk iterator
    //
    RLEChunkIterator::RLEChunkIterator(ArrayDesc const& desc,
                                       AttributeID attrID,
                                       Chunk* data,
                                       Chunk* bitmap,
                                       int iterationMode,
                                       boost::shared_ptr<Query> const& query)
    : BaseChunkIterator(desc, attrID, data, iterationMode, query),
      _arena (arena::newArena(arena::Options("RLEValueMap").pagesize(64*KiB).resetting(true))),
      _values(_arena),
      _valuesFootprint(0),
      _initialFootprint(0),
      tileValue(type,Value::asTile),
      payload(type),
      bitmapChunk(bitmap),
      appender(&payload),
      prevPos(0),
      _sizeLimit(0),
      _needsFlush(true)
    {
#ifndef SCIDB_CLIENT
        _sizeLimit = Config::getInstance()->getOption<size_t>(CONFIG_CHUNK_SIZE_LIMIT);
#endif
        emptyBitmap = make_shared<RLEEmptyBitmap>(_logicalChunkSize);
        emptyBitmapIterator = emptyBitmap->getIterator();
        hasCurrent = !emptyBitmapIterator.end();

        if (iterationMode & ConstChunkIterator::APPEND_CHUNK) {
            if (!(iterationMode &
                  (ConstChunkIterator::SEQUENTIAL_WRITE|ConstChunkIterator::TILE_MODE))) {

                // use ValueMap, suck all the existing data into a ValueMap first

                if (isEmptyable) {
                    shared_ptr<ConstChunkIterator> it =
                        data->getConstIterator(ConstChunkIterator::APPEND_CHUNK|
                                               ConstChunkIterator::IGNORE_EMPTY_CELLS);
                    while (!it->end()) {
                        _values[coord2pos(it->getPosition())] = it->getItem();
                        _valuesFootprint += Value::getFootprint(it->getItem().size());
                        ++(*it);
                    }
                } else {
                    assert(!isEmptyIndicator);
                    ConstRLEPayload payload((char*)data->getData());
                    ConstRLEPayload::iterator it(&payload);
                    while (!it.end()) {
                        if (it.isDefaultValue(defaultValue)) {
                            it.toNextSegment();
                        } else {
                            Value v;
                            pair<ValueMap::iterator, bool> result =
                                _values.insert(make_pair(it.getPPos(),v));
                            assert(result.second);
                            ValueMap::iterator& vm_it = result.first;
                            it.getItem(vm_it->second);
                            _valuesFootprint += Value::getFootprint(vm_it->second.size());
                            ++it;
                        }
                    }
                }
            } else {
                if (dataChunk->getData() != NULL) {
                    if (isEmptyIndicator) {
                        ConstRLEEmptyBitmap initialEbm(reinterpret_cast<char*>(dataChunk->getData()));
                        _initialFootprint = initialEbm.packedSize();
                    } else {
                        ConstRLEPayload initialPayload(reinterpret_cast<char*>(dataChunk->getData()));
                        _initialFootprint = initialPayload.packedSize();
                    }
                }
            }
        }

        if ( (iterationMode & ConstChunkIterator::APPEND_EMPTY_BITMAP) && isEmptyable && !isEmptyIndicator &&
             (iterationMode & (ConstChunkIterator::SEQUENTIAL_WRITE|ConstChunkIterator::TILE_MODE)) &&
             (!bitmapChunk || !bitmapChunk->getEmptyBitmap()) ) {
            // we are asked to append an empty bitmap, but are not given one
            // note that our own empty bitmap is useless even if we are constructing a completely new chunk
            // because it is created fully dense (to accommodate the setPosition() calls i guess)
            assert(false);
            throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_NO_ASSOCIATED_BITMAP_CHUNK);
        }

        falseValue.setBool(false);
        if (bitmap != NULL && !(iterationMode & NO_EMPTY_CHECK)) {
            trueValue.setBool(true);
            bitmap->pin();
            mode &= ~TILE_MODE;
            emptyChunkIterator = bitmap->getIterator(query, mode);
        }
    }

    RLEChunkIterator::~RLEChunkIterator()
    {
        if (_needsFlush)
        {
            if (dataChunk) {
                dataChunk->unPin();
            }
        }
    }

    bool RLEChunkIterator::setPosition(Coordinates const& pos)
    {
        position_t prevTilePos = tilePos;
        if (BaseChunkIterator::setPosition(pos))
        {
            assert(tilePos <= static_cast<uint64_t>(std::numeric_limits<int64_t>::max()));
            if ((mode & TILE_MODE) && payload.nSegments() && prevTilePos >= static_cast<position_t>(tilePos))
                throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_TILE_MODE_EXPECTED_STRIDE_MAJOR_ORDER);
            return true;
        }
        return false;
    }

    bool RLEChunkIterator::isEmpty()
    {
        return _values.find(getPos()) == _values.end();
    }

    Value& RLEChunkIterator::getItem()
    {
        if (!hasCurrent)
            throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_NO_CURRENT_ELEMENT);
        if (mode & TILE_MODE) {
            position_t end = min(tilePos + tileSize, _logicalChunkSize);
            tileValue.getTile()->unPackTile(payload, *emptyBitmap, tilePos, end);
            return tileValue;
        } else {

            ValueMap::iterator iter = _values.find(getPos());
            if (iter == _values.end()) {
                // XXX TODO: this seems like a wrong a result, there should be NO value here
                tmpValue = defaultValue;
                return tmpValue;
            }
            return (*iter).second;
        }
    }

namespace {
void addToPayload(const position_t curPos,
                  const Value& val,
                  const Value* fillVal,
                  RLEPayload::append_iterator& appender,
                  position_t& lastPos)

{
    assert(lastPos>=0);
    assert(curPos>=0);
    ASSERT_EXCEPTION(curPos>=lastPos, "It is an internal bug in the system that the SEQUENTIAL_WRITE rule is violated.");
    if (fillVal && curPos != lastPos) {
        appender.add(*fillVal,curPos-lastPos);
    }
    appender.add(val);
    lastPos = curPos + 1;
}
}
    void RLEChunkIterator::writeItem(const Value& item)
    {
        size_t newSize = 0;

        if (!hasCurrent) {
            throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_NO_CURRENT_ELEMENT);
        }
        if (mode & TILE_MODE) {
            RLEPayload* tile = item.getTile();
            if (tile->count() == INFINITE_LENGTH) {
                position_t end = min(tilePos + tileSize, _logicalChunkSize);
                tile->trim(end - tilePos);
            }
            payload.append(*tile);
            if (_sizeLimit) { newSize = _initialFootprint + payload.packedSize(); }
        } else {
            if (item.isNull() && !attr.isNullable()) {
                throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_ASSIGNING_NULL_TO_NON_NULLABLE);
            }
            if (mode & SEQUENTIAL_WRITE) {
                // Before pending the item, do two things:
                //   - Make sure the new position is not smaller than the previous position.
                //   - If there are gaps to fill, fill them.
                //
                // Note that the code is using the pattern of:
                //   - assert(pos >= prevPos)
                //   - prevPos = pos + 1
                // instead of
                //   - assert(pos > prevPos)
                //   - prevPos = pos
                // because the initial value of prevPos is 0 and the first pos to write to may be 0.
                //
                if (isEmptyIndicator) {
                    const position_t pos = emptyBitmapIterator.getLPos();
                    addToPayload(pos, item, &falseValue, appender, prevPos);
                } else if (!isEmptyable) {
                    const position_t pos = emptyBitmapIterator.getPPos();
                    addToPayload(pos, item, &defaultValue, appender, prevPos);
                } else {
                    // Note from DZ: this sanity check is important to avoid wrong-result bugs such as #4127.
                    // Basically, if a chunk is in SEQUENTIAL_WRITE mode, items must be appended in increasing coordinates.
                    //
                    const position_t pos = emptyBitmapIterator.getLPos();
                    addToPayload(pos, item, NULL, appender, prevPos);
                }
                // appender.flush() should not be necessary
                if (_sizeLimit) { newSize = _initialFootprint + payload.packedSize(); }
            } else {
                if (!type.variableSize() && item.size() > type.byteSize()) {
                    throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_TRUNCATION) << item.size() << type.byteSize();
                }
                ValueMap::value_type valuePair(getPos(), item);
                pair<ValueMap::iterator, bool> result =
                    _values.insert(valuePair);
                    if (result.second) {
                        /* new element was inserted
                         */
                        _valuesFootprint += Value::getFootprint(item.size());
                    } else {
                        /* old element was found, need to copy new item in
                         */
                        _valuesFootprint -= Value::getFootprint(result.first->second.size());
                        result.first->second = item;
                        _valuesFootprint += Value::getFootprint(item.size());
                    }
                    if (_sizeLimit) { newSize = _valuesFootprint; }
            }
            if (emptyChunkIterator) {
                if (!emptyChunkIterator->setPosition(getPosition())) {
                    throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_OPERATION_FAILED) << "setPosition";
                }
                emptyChunkIterator->writeItem(trueValue);
            }
        }

        if (newSize > _sizeLimit * MiB) {
            throw USER_EXCEPTION(SCIDB_SE_OPERATOR,
                                 SCIDB_LE_CHUNK_TOO_LARGE)
            << newSize << _sizeLimit;
        }
    }

    void RLEChunkIterator::flush()
    {
        if (!_needsFlush) {
            return;
        }
        if (!(mode & (SEQUENTIAL_WRITE|TILE_MODE))) {
            // in case we used ValueMap

            if (isEmptyIndicator) {
                RLEEmptyBitmap bitmap(_values);
                dataChunk->allocate(bitmap.packedSize());
                bitmap.pack((char*)dataChunk->getData());
            } else {
                RLEPayload payload(_values, emptyBitmap->count(), type.byteSize(), attr.getDefaultValue(), type.bitSize()==1, isEmptyable);
                if (isEmptyable && (mode & APPEND_EMPTY_BITMAP)) {
                    RLEEmptyBitmap bitmap(_values, true);
                    dataChunk->allocate(payload.packedSize() + bitmap.packedSize());
                    payload.pack((char*)dataChunk->getData());
                    bitmap.pack((char*)dataChunk->getData() + payload.packedSize());
                } else {
                    dataChunk->allocate(payload.packedSize());
                    payload.pack((char*)dataChunk->getData());
                }
            }

        } else { // otherwise we appended directly to the payload

            // [dzhang Note:] the 'if' statement is introduced to avoid a bug that,
            // in the particular case, the ending segment (of default values) failed to be added.
            // However, the whole RLEChunkIterator logic is extremely complex and should be rewritten.
            if ((mode & TILE_MODE) && (mode & SEQUENTIAL_WRITE) && !isEmptyable) {
                assert(!isEmptyIndicator);
                size_t logicalChunkSize = static_cast<size_t>(_logicalChunkSize);
                if ((mode & SEQUENTIAL_WRITE) && payload.count() != logicalChunkSize) {
                    RLEPayload tile(attr.getDefaultValue(), logicalChunkSize-payload.count(), type.byteSize(), type.bitSize()==1);
                    payload.append(tile);
                }

            } else if ((mode & (TILE_MODE|SEQUENTIAL_WRITE)) == SEQUENTIAL_WRITE) {
                if (!isEmptyable) {
                    position_t count = emptyBitmap->count();
                    if (count != prevPos) {
                        assert(count > prevPos);
                        appender.add(defaultValue, count - prevPos);
                    }
                }
                appender.flush();
            }

            if (emptyChunkIterator) {
                if (mode & TILE_MODE) {
                    throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_CANT_UPDATE_BITMAP_IN_TILE_MODE);
                }
                // flush it in case it updates bitmapChunk,
                // which may be used in the APPEND_EMPTY_BITMAP case
                emptyChunkIterator->flush();
            }

            RLEPayload origPayload;
            RLEPayload *resultPayload = &payload;

            if (mode & APPEND_CHUNK) {
                // append new content to the original content
                //XXX TODO: this path needs testing
                if (isEmptyIndicator) {

                    RLEPayload::append_iterator origAppender(&origPayload);
                    ConstRLEEmptyBitmap constEbm(reinterpret_cast<char*>(dataChunk->getData()));

                    position_t lastLPos = 0;
                    for (ConstRLEEmptyBitmap::iterator ebmIterator = constEbm.getIterator();
                         !ebmIterator.end() ; ++ebmIterator) {
                        const position_t curLPos = ebmIterator.getLPos();
                        addToPayload(curLPos, trueValue, &falseValue, origAppender, lastLPos);
                    }
                    origAppender.flush();
                } else {
                    ConstRLEPayload constPayload(reinterpret_cast<char*>(dataChunk->getData()));
                    origPayload = constPayload;
                }
                origPayload.append(*resultPayload);
                resultPayload = &origPayload;
            }

            if (isEmptyIndicator) {
                RLEEmptyBitmap bitmap(*resultPayload);
                dataChunk->allocate(bitmap.packedSize());
                bitmap.pack((char*)dataChunk->getData());

            } else if (isEmptyable && (mode & APPEND_EMPTY_BITMAP)) {
                assert(bitmapChunk);
                boost::shared_ptr<ConstRLEEmptyBitmap> bitmap = bitmapChunk->getEmptyBitmap();
                if (bitmap) {
                    dataChunk->allocate(resultPayload->packedSize() + bitmap->packedSize());
                    resultPayload->pack((char*)dataChunk->getData());
                    bitmap->pack((char*)dataChunk->getData() + resultPayload->packedSize());
                } else {
                    assert(false);
                    dataChunk->allocate(resultPayload->packedSize());
                    resultPayload->pack((char*)dataChunk->getData());
                }
            } else {
                dataChunk->allocate(resultPayload->packedSize());
                resultPayload->pack((char*)dataChunk->getData());
            }
        }
        boost::shared_ptr<Query> query(getQuery());
        /* Once _needsFlush is unset, the following may happen:
           1. write() is successful & accessCount is decremented
           2. LruMemChunk::write() fails and the chunk will be kicked out by the ~MemArray()
           3. PersistentChunk::write() fails and accessCount is decremented
           4. PersistentChunk::write() fails and the chunk is kicked out by the rollback logic.
        */
        _needsFlush = false;
        dataChunk->write(query);

        if (emptyChunkIterator) {
            emptyChunkIterator->flush();
        }
    }

    // New Tile based iterators

    int BaseTileChunkIterator::getMode()
    {
        return _mode;
    }

    bool BaseTileChunkIterator::isEmpty()
    {
        return false;
    }

    bool BaseTileChunkIterator::end()
    {
        return !_hasCurrent;
    }

    ConstChunk const& BaseTileChunkIterator::getChunk()
    {
        return *_dataChunk;
    }

    void BaseTileChunkIterator::reset()
    {
        _emptyBitmapIterator.reset();
        _hasCurrent = !_emptyBitmapIterator.end();
    }

    Coordinates const& BaseTileChunkIterator::getPosition()
    {
        assert(!(_mode & TILE_MODE));
        if (!_hasCurrent) {
            throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_NO_CURRENT_ELEMENT);
        }
        pos2coord(_emptyBitmapIterator.getLPos(), _currPos);
        return _currPos;
    }

    position_t BaseTileChunkIterator::getLogicalPosition()
    {
        assert(!(_mode & TILE_MODE));
        if (!_hasCurrent) {
            throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_NO_CURRENT_ELEMENT);
        }
        return _emptyBitmapIterator.getLPos();
    }

    void BaseTileChunkIterator::operator ++()
    {
        assert(!(_mode & TILE_MODE));

        if (!_hasCurrent) {
            throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_NO_CURRENT_ELEMENT);
        }
        ++_emptyBitmapIterator;
        _hasCurrent = !_emptyBitmapIterator.end();
    }

    bool BaseTileChunkIterator::setPosition(Coordinates const& coord)
    {
        assert(!(_mode & TILE_MODE));
        assert(!coord.empty());

        if (!_dataChunk->contains(coord, !(_mode & IGNORE_OVERLAPS))) { //XXX necessary ?
            return (_hasCurrent = false);
        }
        position_t pos = coord2pos(coord);
        bool res = (_hasCurrent = _emptyBitmapIterator.setPosition(pos));
        return res;
    }

    bool BaseTileChunkIterator::setPosition(position_t lPos)
    {
        assert(!(_mode & TILE_MODE));
        if (lPos < 0) {
            return _hasCurrent = false;
        }
        Coordinates coord;
        pos2coord(lPos, coord);
        if (!_dataChunk->contains(coord, !(_mode & IGNORE_OVERLAPS))) { //XXX necessary ?
            return (_hasCurrent = false);
        }
        bool res = ( _hasCurrent = _emptyBitmapIterator.setPosition(lPos));
        return res;
    }

    BaseTileChunkIterator::~BaseTileChunkIterator()
    {
        assert(!(_mode & TILE_MODE));
        if (_sDebug) {
            LOG4CXX_TRACE(logger, "~BTileCI this=" << this
                          << ", chunk=" << _dataChunk);
        }
    }

BaseTileChunkIterator::BaseTileChunkIterator(ArrayDesc const& desc,
                                             AttributeID aid,
                                             Chunk* data,
                                             int iterationMode,
                                             boost::shared_ptr<Query> const& query)
: CoordinatesMapper(*data),
  _array(desc),
  _attrID(aid),
  _attr(_array.getAttributes()[aid]),
  _dataChunk(data),
  _hasCurrent(false),
  _mode(iterationMode),
  _currPos(_array.getDimensions().size()),
  _query(query)
{
    if (_sDebug) {
        LOG4CXX_TRACE(logger, "BTileCI this=" << this
                      << ", chunk=" << _dataChunk);
    }
    const Dimensions& dim = _array.getDimensions();
    size_t nDims = dim.size();

    _hasOverlap = false;

    if (iterationMode & INTENDED_TILE_MODE ||
        iterationMode & TILE_MODE)
    {
        assert(false);
        throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_UNREACHABLE_CODE)
        << "unsupported TILE_MODE";
    }

    for (size_t i = 0; i < nDims; i++) {
        _hasOverlap = _hasOverlap || (dim[i].getChunkOverlap() != 0);
    }
}

/**
 * Constant RLE chunk iterator providing the getData() interface
 */
RLETileConstChunkIterator::RLETileConstChunkIterator(ArrayDesc const& desc,
                                                     AttributeID attr,
                                                     Chunk* data,
                                                     Chunk* bitmap,
                                                     int iterationMode,
                                                     shared_ptr<Query> const& query)
  : BaseTileChunkIterator(desc, attr, data, iterationMode, query),
    _payload(NULL),
    _lPosition(-1),
    _tileFactory(TileFactory::getInstance()),
    _fastTileInitialize(false),
    _isDataChunkPinned(false),
    _value(TypeLibrary::getType(_attr.getType()))
{
    const char * func = "RLETileConstChunkIterator";
    assert(! (_mode & TILE_MODE));

    if (_sDebug) {
        LOG4CXX_TRACE(logger, func << " this="<<this
                      <<" data="<<data
                      <<" bitmap "<<bitmap
                      <<" attr "<<attr
                      <<" payload data="
                      << data->getConstData());
    }
    assert(_tileFactory);

    prepare();
    UnPinner dataUP(_dataChunk);
    bool isEmptyBitmapAttached = ((_mode & APPEND_CHUNK) || bitmap == NULL) &&
                                 _payload.packedSize() < data->getSize();

    if (isEmptyBitmapAttached) {
        _emptyBitmap = make_shared<ConstRLEEmptyBitmap>(static_cast<char const*>(data->getConstData())
                                                        + _payload.packedSize());
    } else if (bitmap != NULL) {
        _emptyBitmap = bitmap->getEmptyBitmap();
    }
    if (!_emptyBitmap) {
        _emptyBitmap = make_shared<RLEEmptyBitmap>(_logicalChunkSize);
    }
    if (_hasOverlap && (_mode & IGNORE_OVERLAPS)) {
        _emptyBitmap = _emptyBitmap->cut(data->getFirstPosition(true),
                                         data->getLastPosition(true),
                                         data->getFirstPosition(false),
                                         data->getLastPosition(false));
        isEmptyBitmapAttached = false;
    }

    if (isEmptyBitmapAttached) {
        // empty bitmap is attached and the chunk requires pinning
        assert(data->getSize()>_payload.packedSize());
        const size_t payloadSize = _payload.packedSize();
        const size_t ebmSize = data->getSize()-_payload.packedSize();
        if (2*ebmSize <= payloadSize) {
            // copy out EBM
            _emptyBitmap = make_shared<RLEEmptyBitmap>(*_emptyBitmap);
        } else {
            // keep the chunk pinned
            dataUP.set(NULL);
            _isDataChunkPinned=true;
        }
    }

    _emptyBitmapIterator = _emptyBitmap->getIterator();
    _hasCurrent = !_emptyBitmapIterator.end();

    if (_sDebug) {
        LOG4CXX_TRACE(logger, func << " this="<<this
                      <<" ebmCount="<< _emptyBitmap->count()
                      <<" pCount="<<   _payload.count());
    }

    assert(_emptyBitmap->count() <= _payload.count());

    _fastTileInitialize = (_emptyBitmap->count() == _payload.count()) &&
                          (!_payload.isBool()) &&
                          (_payload.elementSize() > 0) ;
    if (_sDebug) {
        LOG4CXX_TRACE(logger, func << " this="<<this
                      <<" data="<<data
                      <<" bitmap "<<bitmap
                      <<" attr "<<_attr
                      <<" payload data="
                      << data->getData()
                      << " #segments=" << _payload.nSegments()
                      <<" pCount="     << _payload.count());
    }

    if (_hasCurrent) {
        _lPosition = _emptyBitmapIterator.getLPos();
    }
    unPrepare();
}

RLETileConstChunkIterator::RLEPayloadDesc::RLEPayloadDesc(ConstRLEPayload* rlePayload,
                                                          position_t offset,
                                                          size_t numElem)
: _rlePayload(rlePayload),
  _offset(offset),
  _numElem(numElem)
{
    assert(rlePayload);
    assert(offset>=0);
    assert(numElem>0);
}

RLETileConstChunkIterator::~RLETileConstChunkIterator()
{
}
void RLETileConstChunkIterator::unPrepare()
{
    if (isDebug() && !_isDataChunkPinned ) {
        assert(_emptyBitmap);
        // avoid heap alloaction
        _payload.~ConstRLEPayload();
        new(&_payload) ConstRLEPayload(NULL);
    }
}
void RLETileConstChunkIterator::prepare()
{
    if (_isDataChunkPinned ) { return; }

    assert(_dataChunk);
    _dataChunk->pin();

    UnPinner dataUP(_dataChunk);

    assert(_payload.count()==0);

    // avoid heap alloaction
    _payload.~ConstRLEPayload();
    new(&_payload) ConstRLEPayload(static_cast<char const*>(_dataChunk->getConstData()));

    _payloadIterator = _payload.getIterator();

    dataUP.set(NULL); // keep it pinned
}

void RLETileConstChunkIterator::reset()
{
    assert(! (_mode & TILE_MODE));

    BaseTileChunkIterator::reset();

    if (_hasCurrent) {
        _lPosition = _emptyBitmapIterator.getLPos();
    } else {
        _lPosition = -1;
    }
}

Value& RLETileConstChunkIterator::getItem()
{
    assert(! (_mode & TILE_MODE));
    if (!_hasCurrent) {
        throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_NO_CURRENT_ELEMENT);
    }

    prepare();
    UnPinner dataUP(_dataChunk);

    assert(_lPosition >= 0);

    assert(_emptyBitmapIterator.getLPos() == _lPosition);
    alignIterators();

    _payloadIterator.getItem(_value);
    unPrepare();
    return _value;
}

const Coordinates&
RLETileConstChunkIterator::getData(scidb::Coordinates& offset,
                                   size_t maxValues,
                                   boost::shared_ptr<BaseTile>& tileData,
                                   boost::shared_ptr<BaseTile>& tileCoords)
{
    if (offset.empty()) {
        return offset;
    }
    const CoordinatesMapperWrapper coordMapper(this);
    position_t logicalStartPos = coord2pos(offset);
    logicalStartPos = getDataInternal(logicalStartPos, maxValues,
                                      tileData, tileCoords,
                                      "scidb::Coordinates", &coordMapper);
    if (logicalStartPos < 0) {
        offset.clear();
    } else {
        pos2coord(logicalStartPos, offset);
    }
    return offset;
}

position_t
RLETileConstChunkIterator::getData(position_t logicalOffset,
                                   size_t maxValues,
                                   boost::shared_ptr<BaseTile>& tileData,
                                   boost::shared_ptr<BaseTile>& tileCoords)
{
    const CoordinatesMapperWrapper coordMapper(this);
    return getDataInternal(logicalOffset, maxValues, tileData, tileCoords,
                           "scidb::Coordinates", &coordMapper);
}

const Coordinates&
RLETileConstChunkIterator::getData(scidb::Coordinates& offset,
                                   size_t maxValues,
                                   boost::shared_ptr<BaseTile>& tileData)
{
    if (offset.empty()) {
        return offset;
    }
    position_t logicalStartPos = coord2pos(offset);
    logicalStartPos = getDataInternal(logicalStartPos, maxValues, tileData);
    if (logicalStartPos < 0) {
        offset.clear();
    } else {
        pos2coord(logicalStartPos, offset);
    }
    return offset;
}

position_t
RLETileConstChunkIterator::getData(position_t logicalOffset,
                                   size_t maxValues,
                                   boost::shared_ptr<BaseTile>& tileData)
{
    return getDataInternal(logicalOffset, maxValues, tileData);
}


/// @todo XXX re-factor
position_t
RLETileConstChunkIterator::getDataInternal(position_t logicalOffset,
                                           size_t maxValues,
                                           boost::shared_ptr<BaseTile>& tileData)
{
    const char * func = "RLETileConstChunkIterator::getDataInternal(data)";
    assert(! (_mode & TILE_MODE));

    prepare();
    UnPinner dataUP(_dataChunk);

    bool needSetPosition = (_emptyBitmapIterator.end() ||
                            (logicalOffset != _emptyBitmapIterator.getLPos()));

    if ( needSetPosition &&
        !BaseTileChunkIterator::setPosition(logicalOffset)) {
        assert(!_hasCurrent);
        _lPosition = -1;
        unPrepare();
        assert(!_hasCurrent);
        return _lPosition;
    }

    shared_ptr<BaseTile> dataTile  = _tileFactory->construct(_attr.getType(), BaseEncoding::RLE);

    if (_fastTileInitialize) {
        position_t pPosition = _emptyBitmapIterator.getPPos();
        assert(pPosition>=0);

        RLEPayloadDesc rlePDesc(&_payload, pPosition, maxValues);
        dataTile->getEncoding()->initialize(&rlePDesc);

        assert(dataTile->size()>0);
        bool ret = _emptyBitmapIterator.skip(dataTile->size()-1);
        assert(ret); ret=ret;
        assert(!_emptyBitmapIterator.end());
        ++_emptyBitmapIterator;

    } else {

        LOG4CXX_TRACE(logger, func << " SLOW tile init: "
                      << " isBool="<<_payload.isBool()
                      << " elemSize="<<_payload.elementSize()
                      << " attrType="<<_attr.getType());

        dataTile->initialize();
        dataTile->reserve(maxValues);

        for (size_t n=0; !_payloadIterator.end() &&
             !_emptyBitmapIterator.end() &&
             n < maxValues; ++_payloadIterator, ++_emptyBitmapIterator, ++n) {

            alignIterators();

            _payloadIterator.getItem(_value);
            dataTile->push_back(_value);
        }
        dataTile->finalize();
    }

    if (!_emptyBitmapIterator.end()) {
        _lPosition = _emptyBitmapIterator.getLPos();
        assert(_lPosition >=0 &&
               _lPosition > logicalOffset);
        _hasCurrent = true;
    } else {
        // end of chunk data
        _hasCurrent = false;
        _lPosition = -1;
    }
    tileData.swap(dataTile);
    unPrepare();
    return _lPosition;
}

position_t
RLETileConstChunkIterator::getDataInternal(position_t logicalOffset,
                                           size_t maxValues,
                                           boost::shared_ptr<BaseTile>& tileData,
                                           boost::shared_ptr<BaseTile>& tileCoords,
                                           const scidb::TypeId& coordTileType,
                                           const BaseTile::Context* coordCtx)
{
    assert(! (_mode & TILE_MODE));

    prepare();
    UnPinner dataUP(_dataChunk);

    bool needSetPosition = (_emptyBitmapIterator.end() ||
                            (logicalOffset != _emptyBitmapIterator.getLPos()));

    if ( needSetPosition &&
        !BaseTileChunkIterator::setPosition(logicalOffset)) {
        assert(!_hasCurrent);
        _lPosition = -1;
        unPrepare();
        assert(!_hasCurrent);
        return _lPosition;
    }

    shared_ptr<BaseTile> dataTile = _tileFactory->construct(_attr.getType(), BaseEncoding::RLE);

    if (_fastTileInitialize) {

        position_t pPosition = _emptyBitmapIterator.getPPos();
        assert(pPosition>=0);

        RLEPayloadDesc rlePDesc(&_payload, pPosition, maxValues);
        dataTile->getEncoding()->initialize(&rlePDesc);

        assert(dataTile->size()>0);
    } else {
        dataTile->initialize();
        dataTile->reserve(maxValues);
    }

    shared_ptr<BaseTile> coordTile = _tileFactory->construct(coordTileType, BaseEncoding::ARRAY, coordCtx);
    coordTile->initialize();
    coordTile->reserve(maxValues);

    ArrayEncoding<position_t>* coordEncoding = safe_dynamic_cast<ArrayEncoding<position_t>* >( coordTile->getEncoding() );

    for (size_t n=0; !_payloadIterator.end() &&
         !_emptyBitmapIterator.end() &&
         n < maxValues; ++_payloadIterator,
         ++_emptyBitmapIterator, ++n) {

        if (!_fastTileInitialize) {
            alignIterators();
            _payloadIterator.getItem(_value);
            dataTile->push_back(_value);
        }
        coordEncoding->push_back(_emptyBitmapIterator.getLPos());
    }

    if (!_fastTileInitialize) {
        dataTile->finalize();
    }
    coordTile->finalize();

    assert(dataTile->size() == coordTile->size());

    if (!_emptyBitmapIterator.end()) {
        _lPosition = _emptyBitmapIterator.getLPos();
        assert(_lPosition >=0 &&
               _lPosition > logicalOffset);
        _hasCurrent = true;
    } else {
        // end of chunk data
        _hasCurrent = false;
        _lPosition = -1;
    }

    tileData.swap(dataTile);
    tileCoords.swap(coordTile);
    unPrepare();
    return _lPosition;
}

void RLETileConstChunkIterator::alignIterators()
{
    // emptybitmap can (apparently) have fewer elements than the payload
    // (as a result of filtering *just* the emptybitmap?)
    // here we make sure to skip the elements not in the emptybitmap
    position_t pPosition = _emptyBitmapIterator.getPPos();
    if (_payloadIterator.getPPos() != pPosition)
    {
        if (!_payloadIterator.setPosition(pPosition)) {
            throw SYSTEM_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_OPERATION_FAILED) << "setPosition";
        }
    }
}

void RLETileConstChunkIterator::operator ++()
{
    assert(! (_mode & TILE_MODE));
    if (!_hasCurrent) {
        throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_NO_CURRENT_ELEMENT);
    }
    assert(_emptyBitmap);
    assert(_emptyBitmapIterator.getLPos() == _lPosition);

    ++_emptyBitmapIterator;
    if (_emptyBitmapIterator.end()) {
        _hasCurrent = false;
        return;
    }

    _lPosition = _emptyBitmapIterator.getLPos();
}

Coordinates const& RLETileConstChunkIterator::getPosition()
{
    assert(! (_mode & TILE_MODE));
    assert(_emptyBitmap);
    assert(_emptyBitmapIterator.getLPos() == _lPosition);

    Coordinates const& coords = BaseTileChunkIterator::getPosition();
    assert((&coords) == (&_currPos));
    return coords;
}

bool RLETileConstChunkIterator::setPosition(Coordinates const& coord)
{
    assert(! (_mode & TILE_MODE));
    assert(_emptyBitmap);

    if (!BaseTileChunkIterator::setPosition(coord)) {
        assert(!_hasCurrent);
        return false;
    }

    _lPosition = _emptyBitmapIterator.getLPos();
    return true;
}

bool RLETileConstChunkIterator::setPosition(position_t lPos)
{
    assert(! (_mode & TILE_MODE));
    assert(lPos>=0);
    assert(_emptyBitmap);

    if (!BaseTileChunkIterator::setPosition(lPos)) {
        assert(!_hasCurrent);
        return false;
    }

    _lPosition = _emptyBitmapIterator.getLPos();
    assert(lPos == _lPosition);
    return true;
}

}
