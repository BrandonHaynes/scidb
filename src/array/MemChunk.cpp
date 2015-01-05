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
    {
        arrayDesc = NULL;
        bitmapChunk = NULL;
        size = 0;
        sparse = false;
        array = NULL;
    }

    Array const& MemChunk::getArray() const
    {
        return *array;
    }

    boost::shared_ptr<ConstRLEEmptyBitmap> MemChunk::getEmptyBitmap() const
    {
        return emptyBitmap ? emptyBitmap : bitmapChunk ? bitmapChunk->getEmptyBitmap() : ConstChunk::getEmptyBitmap();
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
        return bitmapChunk != NULL ? bitmapChunk : getAttributeDesc().isEmptyIndicator() ? this : NULL;
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
                || (isRLE() && arrayDesc->getEmptyBitmapAttribute() != NULL))
            ? (Chunk*)newBitmapChunk->getBitmapChunk() : NULL;
    }

  #ifndef SCIDB_CLIENT
    void LruMemChunk::initialize(ConstChunk const& srcChunk)
    {
        assert(false);
        throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_UNREACHABLE_CODE) << "LruMemChunk::initialize";
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
        setSparse(srcChunk.isSparse());
#ifndef SCIDB_CLIENT
        setRLE(srcChunk.getAttributeDesc().isEmptyIndicator() || srcChunk.isRLE());
#else
        setRLE(srcChunk.isRLE());
#endif
    }

    void MemChunk::initialize(Array const* arr, ArrayDesc const* desc, const Address& firstElem, int compMethod)
    {
        array = arr;
        arrayDesc = desc;
        sparse = false;
#ifndef SCIDB_CLIENT
        rle = true;
#else
        rle = false;
#endif
        nElems = 0;
        addr = firstElem;
        compressionMethod = compMethod;
        firstPos = lastPos = lastPosWithOverlaps = firstPosWithOverlaps = addr.coords;
        const Dimensions& dims = desc->getDimensions();
        for (size_t i = 0, n = dims.size(); i < n; i++) {
            if (firstPos[i] < dims[i].getStart() ||
                lastPos[i] > dims[i].getEndMax()) {
                throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_CHUNK_OUT_OF_BOUNDARIES);
            }
            if ((firstPosWithOverlaps[i] -= dims[i].getChunkOverlap()) < dims[i].getStart()) {
                firstPosWithOverlaps[i] = dims[i].getStart();
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

    int MemChunk::getCompressionMethod() const
    {
        return compressionMethod;
    }

    void* MemChunk::getData() const
    {
        return data.get();
    }

    size_t MemChunk::getSize() const
    {
        return size;
    }

    void MemChunk::allocate(size_t size)
    {
        if (this->size != size || getData()==NULL) {
            reallocate(size);
        }
    }

    void MemChunk::reallocate(size_t newSize)
    {
        char* newData = new char[newSize];
        if (!newData)
            throw SYSTEM_EXCEPTION(SCIDB_SE_NO_MEMORY, SCIDB_LE_CANT_ALLOCATE_MEMORY);
        size_t minSize = newSize<size ? newSize : size;
        if (data.get()!=NULL) {
            memcpy(newData, data.get(), minSize);
        }
        data.reset(newData);
        size = newSize;
        if (currentStatistics) {
            currentStatistics->allocatedSize += newSize;
            currentStatistics->allocatedChunks++;
        }
    }

    void MemChunk::free()
    {
        if (isDebug() && data && size) {
            memset(data.get(), 0, size);
        }
        data.reset();
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

    bool MemChunk::isSparse() const
    {
        return sparse;
    }

    bool MemChunk::isRLE() const
    {
        return rle;
    }

    void MemChunk::setSparse(bool sparse)
    {
        this->sparse = sparse;
    }

    void MemChunk::setRLE(bool rle)
    {
        this->rle = rle;
    }

    void MemChunk::fillRLEBitmap()
    {
        boost::shared_ptr<Query> emptyQuery;
        RLEChunkIterator iterator(*arrayDesc, addr.attId, this, NULL, ChunkIterator::NO_EMPTY_CHECK, emptyQuery);
        boost::shared_ptr<ConstRLEEmptyBitmap> emptyBitmap = iterator.getEmptyBitmap();
        allocate(emptyBitmap->packedSize());
        emptyBitmap->pack(static_cast<char*>(getData()));
        rle = true;
    }

    boost::shared_ptr<ChunkIterator> MemChunk::getIterator(boost::shared_ptr<Query> const& query, int iterationMode)
    {
        return boost::shared_ptr<ChunkIterator>(
            isRLE()
            ? new RLEChunkIterator(*arrayDesc, addr.attId, this, bitmapChunk, iterationMode, query)
            : ((iterationMode & ChunkIterator::SPARSE_CHUNK) || isSparse())
                ? (ChunkIterator*)new SparseChunkIterator(*arrayDesc, addr.attId, this, bitmapChunk,
                                                          !(iterationMode & ChunkIterator::APPEND_CHUNK), iterationMode, query)
                : (ChunkIterator*)new MemChunkIterator(*arrayDesc, addr.attId, this, bitmapChunk,
                                                       !(iterationMode & ChunkIterator::APPEND_CHUNK), iterationMode, query));
    }
    boost::shared_ptr<ConstChunkIterator> MemChunk::getConstIterator(int iterationMode) const
    {
        boost::shared_ptr<Query> emptyQuery;
        return MemChunk::getConstIterator(emptyQuery, iterationMode);
    }
    boost::shared_ptr<ConstChunkIterator> MemChunk::getConstIterator(boost::shared_ptr<Query> const& query, int iterationMode) const
    {
        PinBuffer scope(*this);
        if (isRLE()) {
            if (getAttributeDesc().isEmptyIndicator() || getData() == NULL) {
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
        // deprecated formats
        if (isSparse()) {
            return boost::make_shared<SparseChunkIterator>(*arrayDesc, addr.attId,
                                                           (Chunk*)this, bitmapChunk,
                                                           false, iterationMode, query);
        }
        return boost::make_shared<MemChunkIterator>(*arrayDesc, addr.attId,
                                                    (Chunk*)this, bitmapChunk,
                                                    false, iterationMode, query);
    }
    bool MemChunk::pin() const
    {
        return false;
    }

    void MemChunk::unPin() const
    {
    }

    void MemChunk::write(boost::shared_ptr<Query>& query)
    {
        // MemChunks can be stand-alone and not always have the query context - dont validate query (yet?)
    }

    void MemChunk::compress(CompressedBuffer& buf, boost::shared_ptr<ConstRLEEmptyBitmap>& emptyBitmap) const
    {
        ConstChunk const* src = this;
        MemChunk closure;
        if (emptyBitmap && rle && getBitmapSize() == 0) {
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
        PinBuffer scope(buf);
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

    void LruMemChunk::write(boost::shared_ptr<Query>& query)
    {
        if (Query::getValidQueryPtr(static_cast<const MemArray*>(array)->_query) != query) {
            throw (SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_INVALID_FUNCTION_ARGUMENT) << "invalid query");
        }
        unPin();
    }

#endif

    //
    // Temporary (in-memory) array chunk iterator
    //
    int MemChunkIterator::getMode()
    {
        return mode;
    }

    bool MemChunkIterator::supportsVectorMode() const
    {
        return isPlain;
    }

    void MemChunkIterator::setVectorMode(bool enabled)
    {
        if (enabled) {
            // TODO: destination array may have bitmap attribute and so chunk is not plain
            //USER_CHECK(SCIDB_E_INVALID_OPERAND, supportsVectorMode(), "Vector mode not supported");
            mode |= VECTOR_MODE;
            currElem = firstElem;
            bufPos = buf;
            hasCurrent = currElem < lastElem;
            moveToNextAvailable = false;
        } else {
            mode &= ~VECTOR_MODE;
        }
    }


    inline bool MemChunkIterator::isEmptyCell()
    {
        return (emptyBitmap != NULL && (emptyBitmap[currElem >> 3] & (1 << (currElem & 7))) == 0)
            || (emptyBitmapIterator && !emptyBitmapIterator->setPosition(currPos));
    }


    inline void MemChunkIterator::findNextAvailable()
    {
        if (moveToNextAvailable) {
            moveToNextAvailable = false;
            ++(*this);
        }
    }

    Value& MemChunkIterator::getItem()
    {
        findNextAvailable();
        if (currElem >= lastElem)
            throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_NO_CURRENT_ELEMENT);
        if (mode & TILE_MODE) {
            size_t tileSize = lastElem - currElem;
            if (tileSize > maxTileSize) {
                tileSize = maxTileSize;
            }
            size_t rawSize = (elemSize == 0) ? (tileSize + 7) >> 3 : tileSize * elemSize;
            RLEPayload* tile = value.getTile(attr->getType());
            tile->unpackRawData(bufPos, rawSize, 0, elemSize, tileSize, elemSize == 0);
            if (varyingOffs) {
                tile->setVarPart(buf + varyingOffs, dataChunk->getSize() - varyingOffs - nullBitmapSize);
            }
            return value;
        }
        if (mode & VECTOR_MODE) {
            size_t strideSize = lastElem - currElem;
            if (elemSize == 0) {
                strideSize = (strideSize + 7) >> 3;
            } else {
                strideSize *= elemSize;
            }
            if (strideSize > STRIDE_SIZE) {
                strideSize = STRIDE_SIZE;
            }
            value.linkData(bufPos, strideSize);
            return value;
        }
        if (isEmptyCell())
            throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_ACCESS_TO_EMPTY_CELL);
        if (nullBitmap != NULL && (nullBitmap[currElem >> 3] & (1 << (currElem & 7)))) {
            if (elemSize >= sizeof(int)) {
                value.setNull(*(int*)bufPos);
            } else if (elemSize != 0) {
                value.setNull(*bufPos);
            } else {
                value.setNull((*bufPos & (1 << (currElem & 7))) != 0);
            }
        } else {
            if (elemSize == 0) { // bit vector
                value.setBool((*bufPos & (1 << (currElem & 7))) != 0);
            } else {
                if (varyingOffs) {
                    const int dataOffset = *(int*)bufPos;
                    uint8_t* src = (uint8_t*)(buf + dataOffset + varyingOffs);
                    size_t itemSize;
                    if (*src != 0) {
                        itemSize = *src++;
                    } else {
                        itemSize = (src[1] << 24) | (src[2] << 16) | (src[3] << 8) | src[4];
                        src += 5;
                    }
                    value.setData(src, itemSize);
                } else {
                    value.setData(bufPos, elemSize);
                }
            }
        }
        return value;
    }

    bool MemChunkIterator::isEmpty()
    {
        findNextAvailable();
        return isEmptyCell();
    }

    bool MemChunkIterator::end()
    {
        findNextAvailable();
        return !hasCurrent;
    }

    ConstChunk const& MemChunkIterator::getChunk()
    {
        return *dataChunk;
    }

    void MemChunkIterator::operator ++()
    {
        findNextAvailable();
        if (mode & VECTOR_MODE) {
            currElem += nElemsPerStride;
            bufPos += STRIDE_SIZE;
            hasCurrent = currElem < lastElem;
            return;
        }
        if (mode & TILE_MODE) {
            currElem += maxTileSize;
            if (elemSize == 0) {
                bufPos += (maxTileSize+7) >> 3;
            } else {
                bufPos += maxTileSize*elemSize;
            }
            hasCurrent = currElem < lastElem;
            return;
        }
        while (++currElem < lastElem) {
            size_t i = currPos.size()-1;
            while (++currPos[i] > lastPos[i])
            {
                currPos[i] = firstPos[i];
                assert(i != 0);
                i -= 1;
            }
            if (!checkBounds || i == currPos.size()-1) { // continue sequential traversal of chunk
                if (!(((mode & IGNORE_NULL_VALUES) && nullBitmap != NULL && (nullBitmap[currElem >> 3] & (1 << (currElem & 7))))
                      || ((mode & IGNORE_EMPTY_CELLS) && isEmptyCell())))
                {
                    bufPos = buf + (elemSize == 0 ? (currElem >> 3) : (currElem * elemSize));
                    hasCurrent = true;
                    return;
                }
            } else { // skip overlaps region
                if (setPosition(currPos)) {
                    return;
                }
            }
        }
        hasCurrent = false;
    }

    Coordinates const& MemChunkIterator::getPosition()
    {
        findNextAvailable();
        if (mode & (TILE_MODE|VECTOR_MODE)) {
            size_t offset = currElem;
            const Dimensions& dim = array.getDimensions();
            for (int i = dim.size(); --i >= 0;) {
                size_t length = dim[i].getChunkInterval() + dim[i].getChunkOverlap()*2;
                currPos[i] = origin[i] + (offset % length);
                offset /= length;
            }
        }
        return currPos;
    }

    bool MemChunkIterator::setPosition(Coordinates const& pos)
    {
        moveToNextAvailable = false;
        size_t offset = 0;
        const Dimensions& dim = array.getDimensions();
        for (size_t i = 0, n = dim.size(); i < n; i++) {
            if (pos[i] < firstPos[i] || pos[i] > lastPos[i]) {
                return hasCurrent = false;
            }
            offset *= dim[i].getChunkInterval() + dim[i].getChunkOverlap()*2;
            offset += pos[i] - origin[i];
        }
        assert(offset >= firstElem && offset < lastElem);
        currElem = 0;
        bufPos = buf;
        seek(offset);
        if (&pos != &currPos) {
            currPos = pos;
        }
        if ((mode & IGNORE_EMPTY_CELLS) && isEmptyCell()) {
            return hasCurrent = false;
        }
        if ((mode & IGNORE_NULL_VALUES) && nullBitmap != NULL && (nullBitmap[offset >> 3] & (1 << (offset & 7)))) {
            return hasCurrent = false;
        }
        return hasCurrent = true;
    }

    void MemChunkIterator::seek(size_t offset)
    {
        currElem += offset;
        bufPos = buf + (elemSize == 0 ? (currElem >> 3) : (currElem * elemSize));
    }

    void MemChunkIterator::reset()
    {
        currPos = firstPos = dataChunk->getFirstPosition(!(mode & IGNORE_OVERLAPS));
        lastPos = dataChunk->getLastPosition(!(mode & IGNORE_OVERLAPS));

        const Dimensions& dim = array.getDimensions();
        size_t nDims = dim.size();

        size_t offset = 0;
        for (size_t i = 0; i < nDims; i++) {
            offset *= dim[i].getChunkInterval() + dim[i].getChunkOverlap()*2;
            offset += firstPos[i] - origin[i];
        }
        firstElem = offset;

        offset = 0;
        for (size_t i = 0; i < nDims; i++) {
            offset *= dim[i].getChunkInterval() + dim[i].getChunkOverlap()*2;
            offset += lastPos[i] - origin[i];
        }
        lastElem = offset + 1;
        if (mode & (TILE_MODE|VECTOR_MODE)) {
            currElem = firstElem;
            bufPos = buf;
            moveToNextAvailable = false;
            hasCurrent = currElem < lastElem;
        } else {
            currElem = firstElem - 1;
            currPos[nDims-1] -= 1;
            moveToNextAvailable = true;
            hasCurrent = false;
        }
    }

    void MemChunkIterator::writeItem(const  Value& item)
    {
        findNextAvailable();
        if (currElem >= lastElem)
            throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_NO_CURRENT_ELEMENT);
        if (mode & TILE_MODE) {
            RLEPayload* tile = item.getTile();
            char* dst = bufPos;
            for (size_t i = 0, n = tile->nSegments(); i < n; i++) {
                RLEPayload::Segment const& s = tile->getSegment(i);
                assert(!s._null);
                char* src = tile->getRawValue(s._valueIndex);
                size_t len = (size_t)s.length();
                if (s._same) {
                    for (size_t j = 0; j < len; j++) {
                        memcpy(dst, src, elemSize);
                        dst += elemSize;
                    }
                } else {
                    memcpy(dst, src, elemSize*len);
                    dst += elemSize*len;
                }
            }
            return;
        }
        if (mode & VECTOR_MODE) {
            assert(bufPos + item.size() <= (char*)dataChunk->getData() + dataChunk->getSize());
            memcpy(bufPos, item.data(), item.size());
            return;
        }
        if (item.isNull()) {
            if (!nullBitmap)
                throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_ASSIGNING_NULL_TO_NON_NULLABLE);
            nullBitmap[currElem >> 3] |= 1 << (currElem & 7);
            if (elemSize >= sizeof(int)) {
                *(int*)bufPos = item.getMissingReason();
            } else if (elemSize != 0) {
                *bufPos = item.getMissingReason();
            } else {
                if (item.getMissingReason() != 0) {
                    *bufPos |= 1 << (currElem & 7);
                } else {
                    *bufPos &= ~(1 << (currElem & 7));
                }
            }
        } else {
            if (nullBitmap != NULL) {
                nullBitmap[currElem >> 3] &= ~(1 << (currElem & 7));
            }
            if (varyingOffs) {
                if (item != defaultValue) { // space for varying size elements with zero length is preserved at the begining of the chunk
                    size_t itemSize = item.size();
                    int bodyOffs = *(int*)bufPos;
                    if (bodyOffs != 0) {
                        uint8_t* src = (uint8_t*)(buf + varyingOffs + bodyOffs);
                        size_t oldSize = src[0] ? src[0] : (src[1] << 24) | (src[2] << 16) | (src[3] << 8) | src[4];
                        if (oldSize < itemSize) {
                            bodyOffs = 0;
                        }
                    }
                    if (bodyOffs == 0) {
                        bodyOffs = (int)used;
                    size_t size = dataChunk->getSize();

                    used += itemSize;
                    if (itemSize-1 >= 0xFF) {
                        used += 5;
                    } else {
                        used += 1;
                    }
                    if (nullBitmapSize + varyingOffs + used > size) {
                        size_t newSize = nullBitmapSize + varyingOffs + used > size*2
                            ? nullBitmapSize + varyingOffs + used : size*2;
                        dataChunk->reallocate(newSize);
                        size_t bufOffs = bufPos - buf;
                        buf = (char*)dataChunk->getData();
                        memset(buf + size, 0, newSize - size);
                        if (nullBitmap != NULL) {
                            nullBitmap = buf;
                            buf += nullBitmapSize;
                        }
                        bufPos = buf + bufOffs;
                    }
                    *(int*)bufPos = bodyOffs;
                    }
                    char* dst = buf + varyingOffs + bodyOffs;
                    if (itemSize-1 >= 0xFF) {
                        *dst++ = '\0';
                        *dst++ = char(itemSize >> 24);
                        *dst++ = char(itemSize >> 16);
                        *dst++ = char(itemSize >> 8);
                    }
                    *dst++ = char(itemSize);
                    memcpy(dst, item.data(), itemSize);
                }
            } else if (elemSize == 0) {
                assert(bufPos < (char*)dataChunk->getData() + dataChunk->getSize());
                if (item.getBool()) {
                    *bufPos |= 1 << (currElem & 7);
                } else {
                    *bufPos &= ~(1 << (currElem & 7));
                }
            } else {
                if (item.size() > elemSize) {
                    throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_TRUNCATION) << item.size() << elemSize;
                }
                assert(bufPos + elemSize <= (char*)dataChunk->getData() + dataChunk->getSize());
                memcpy(bufPos, item.data(), item.size());
            }
        }
        if (emptyBitmap != NULL && emptyBitmap != buf) {
            emptyBitmap[currElem >> 3] |= (1 << (currElem & 7));
        } else if (emptyBitmapIterator) {
            if (!((boost::shared_ptr<ChunkIterator>&)emptyBitmapIterator)->setPosition(currPos))
                throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_OPERATION_FAILED) << "setPosition";
            ((boost::shared_ptr<ChunkIterator>&)emptyBitmapIterator)->writeItem(trueValue);
        }
    }

    void MemChunkIterator::flush()
    {
        _needsFlush = false;
        DimensionDesc const& dim = array.getDimensions()[0];
        if ((Coordinate)dim.getLength() == MAX_COORDINATE
            && !(mode & ChunkIterator::SPARSE_CHUNK)
            && bitmapChunk == NULL
            && currPos[0] < lastPos[0])
        {
            dataChunk->truncate(currPos[0]);
        }
        if (varyingOffs != 0) {
            dataChunk->reallocate(nullBitmapSize + varyingOffs + used);
        }
        boost::shared_ptr<Query> query(getQuery());
        dataChunk->write(query);
        if (bitmapChunk != NULL) {
            if (emptyBitmapIterator) {
                ((boost::shared_ptr<ChunkIterator>&)emptyBitmapIterator)->flush();
            } else {
               bitmapChunk->write(query);
            }
        }
    }

    MemChunkIterator::MemChunkIterator(ArrayDesc const& desc,
                                       AttributeID attId,
                                       Chunk* dataChunk,
                                       Chunk* bitmapChunk,
                                       bool newChunk,
                                       int iterationMode,
                                       boost::shared_ptr<Query> const& query)
    : array(desc),
      _query(query)
    {
        //This class is used for both - const and non-const iterators. We only use flush if we are open for writing.
        _needsFlush = newChunk || (iterationMode & ChunkIterator::APPEND_CHUNK);
        attr = &array.getAttributes()[attId];
        type =  TypeLibrary::getType(attr->getType());
        this->dataChunk = dataChunk;
        dataChunkPinned = dataChunk->pin();

        mode = iterationMode & ~VECTOR_MODE;
        value =  Value(type);
        trueValue.setBool(true);
        defaultValue = attr->getDefaultValue();

        // Calculate number of elements in the chunk
        const Dimensions& dim = array.getDimensions();
        size_t n = 1;
        origin = dataChunk->getFirstPosition(false);
        for (int i = dim.size(); --i >= 0;) {
            n *= dim[i].getChunkInterval() + dim[i].getChunkOverlap()*2;
            origin[i] -= dim[i].getChunkOverlap();
        }
        nElems = n;
#ifndef SCIDB_CLIENT
        maxTileSize = Config::getInstance()->getOption<int>(CONFIG_TILE_SIZE);
        size_t tilesPerChunk = Config::getInstance()->getOption<int>(CONFIG_TILES_PER_CHUNK);
        if (tilesPerChunk != 0) {
            maxTileSize = max(maxTileSize, n/tilesPerChunk);
        }
#else
        maxTileSize = 1;
#endif
        checkBounds = n != dataChunk->getNumberOfElements(!(iterationMode & IGNORE_OVERLAPS));
        isPlain = !checkBounds && dataChunk->isPlain();
        // Convert size of element in bits to bytes
        elemSize = type.bitSize() != 0 ? type.bitSize() : desc.getAttributes()[attId].getVarSize() * 8;
        if (elemSize == 0) { // varying size type
            elemSize = sizeof(int); // int used to store offset to the body
            varyingOffs = nElems*elemSize;
        } else {
            varyingOffs = 0;
            elemSize >>= 3; // in case of boolean type (size = 1 bit), elemSize becomes equal to zero
            nElemsPerStride = (elemSize == 0) ? STRIDE_SIZE * 8 : STRIDE_SIZE / elemSize;
        }

        // Get or allocate chunk data buffer
        size_t bitmapSize = (nElems + 7) >> 3;
        if (newChunk) {
            size_t dataSize = elemSize == 0 ? bitmapSize : (varyingOffs ? nElems*2 : nElems)*elemSize;
            if (attr->isNullable()) {
                dataSize += bitmapSize;
            }
            if (varyingOffs) {
                dataSize += (defaultValue.size()-1 >= 0xFF ? 5 : 1) + defaultValue.size();
            }
            dataChunk->allocate(dataSize);
            buf = (char*)dataChunk->getData();
            memset(buf, 0, dataSize);
        } else {
            buf = (char*)dataChunk->getData();
        }

        // Set empty bitmap
        emptyBitmap = NULL;
        if (!(iterationMode & NO_EMPTY_CHECK) && bitmapChunk != NULL) {
            this->bitmapChunk = bitmapChunk;
            bitmapChunkPinned = bitmapChunk->pin();
            if (newChunk) {
                if (bitmapChunk->isSparse()) {
                    emptyBitmapIterator = bitmapChunk->getIterator(query, SPARSE_CHUNK);
                } else {
                    bitmapChunk->allocate(bitmapSize);
                    emptyBitmap = (char*)bitmapChunk->getData();
                    memset(emptyBitmap, 0, bitmapSize);
                }
                bitmapChunk->pin();
            } else {
                if (bitmapChunk->isSparse() || bitmapChunk->isRLE()) {
                    emptyBitmapIterator = bitmapChunk->getConstIterator((iterationMode & IGNORE_OVERLAPS)|((iterationMode & APPEND_CHUNK) ? 0 : IGNORE_EMPTY_CELLS|IGNORE_DEFAULT_VALUES)|SPARSE_CHUNK);
                } else {
                    emptyBitmap = (char*)bitmapChunk->getData();
                }
            }
        } else {
            this->bitmapChunk = NULL;
            bitmapChunkPinned = false;
            if (attr->isEmptyIndicator()) {
                emptyBitmap = (char*)buf;
            }
        }

        // Set null bitmap
        if (attr->isNullable()) {
            nullBitmap = buf;
            nullBitmapSize = bitmapSize;
            buf += bitmapSize;
            if (newChunk) {
                if (defaultValue.isNull()) {
                    memset(nullBitmap, 0xFF, bitmapSize);
                }
            }
        } else {
            nullBitmap = NULL;
            nullBitmapSize = 0;
        }
        if (varyingOffs) {
            if (iterationMode & APPEND_CHUNK) {
                used = dataChunk->getSize() - nullBitmapSize - varyingOffs;
            } else if (newChunk) {
                used = 0;
                if (!defaultValue.isNull()) {
                    size_t defaultValueSize = defaultValue.size();
                    char* data = buf + varyingOffs;
                    char* dst = data;
                    if (defaultValueSize-1 >= 0xFF) {
                        *dst++ = '\0';
                        *dst++ = char(defaultValueSize >> 24);
                        *dst++ = char(defaultValueSize >> 16);
                        *dst++ = char(defaultValueSize >> 8);
                    }
                    *dst++ = char(defaultValueSize);
                    memcpy(dst, defaultValue.data(), defaultValueSize);
                    used = (dst - data) + defaultValueSize;
                } else if (defaultValue.getMissingReason() != 0) {
                    int missingReason = defaultValue.getMissingReason();
                    for (int* mp = (int*)buf; n != 0; mp++, n--) {
                        *mp = missingReason;
                    }
                }
            }
        } else if (newChunk) {
            if (defaultValue.isNull()) {
                int missingReason = defaultValue.getMissingReason();
                if (missingReason != 0) {
                    if (elemSize >= sizeof(int)) {
                        for (char* p = buf; n != 0; p += elemSize, n--) {
                            *(int*)p = missingReason;
                        }
                    } else if (elemSize != 0) {
                        for (char* p = buf; n != 0; p += elemSize, n--) {
                            *p = (char)missingReason;
                        }
                    } else {
                        memset(buf, 0xFF, bitmapSize);
                    }
                }
            } else if (!defaultValue.isDefault(attr->getType())) {
                if (elemSize == 0) { // boolean attribute with default value true
                    memset(buf, 0xFF, bitmapSize);
                } else {
                    void const* defaultValueData =  defaultValue.data();
                    if (defaultValue.size() > elemSize) {
                        throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_TRUNCATION) << defaultValue.size() << elemSize;
                    }
                    for (char* p = buf; n != 0; p += elemSize, n--) {
                        memcpy(p, defaultValueData, elemSize);
                    }
                }
            }
        }
        reset();
    }

    MemChunkIterator::~MemChunkIterator()
    {
        if (_needsFlush)
        {
            if (dataChunk) {
                dataChunk->unPin();
            }
            if (bitmapChunk) {
                bitmapChunk->unPin();
            }
        }

        if (dataChunkPinned) {
            dataChunk->unPin();
        }
        if (bitmapChunkPinned) {
            bitmapChunk->unPin();
        }
    }


    //
    // Sparse chunk iterator
    //

    inline uint32_t SparseChunkIterator::binarySearch(uint64_t pos) {
        uint32_t l = 0;
        uint32_t r = nNonDefaultElems;
        if (_logicalChunkSize != (uint32_t)_logicalChunkSize) {
            SparseElem64* arr = elemsList64;
            while (l < r) {
                uint32_t m = (l + r) >> 1;
                if (arr[m].position < pos) {
                    l = m + 1;
                } else {
                    r = m;
                }
            }
        } else {
            SparseElem* arr = elemsList;
            while (l < r) {
                uint32_t m = (l + r) >> 1;
                if (arr[m].position < pos) {
                    l = m + 1;
                } else {
                    r = m;
                }
            }
        }
        return r;
    }

    inline void SparseChunkIterator::setCurrPosition()
    {
        if (elemsList != NULL) {
            currElemIndex = binarySearch(currElem);
            if (currElemIndex < nNonDefaultElems) {
                if (_logicalChunkSize != (uint32_t)_logicalChunkSize) {
                    nextNonDefaultElem = elemsList64[currElemIndex].position;
                    currElemOffs = elemsList64[currElemIndex].offset;
                    isNull = nextNonDefaultElem == currElem ? elemsList64[currElemIndex].isNull : isNullDefault;
                } else {
                    nextNonDefaultElem = elemsList[currElemIndex].position;
                    currElemOffs = elemsList[currElemIndex].offset;
                    isNull = nextNonDefaultElem == currElem ? elemsList[currElemIndex].isNull : isNullDefault;
                }
            } else {
                nextNonDefaultElem = ~0;
                isNull = isNullDefault;
            }
        } else {
            curr = elemsMap.lower_bound(currElem);
            if (curr != elemsMap.end()) {
                nextNonDefaultElem = curr->first;
                currElemOffs = curr->second.offset;
                isNull = nextNonDefaultElem == currElem ? curr->second.isNull : isNullDefault;
            } else {
                nextNonDefaultElem = ~0;
                isNull = isNullDefault;
            }
        }
    }

    inline void SparseChunkIterator::findNextAvailable()
    {
        if (moveToNextAvailable) {
            moveToNextAvailable = false;
            ++(*this);
        }
    }


    int SparseChunkIterator::getMode()
    {
        return mode;
    }

    inline bool SparseChunkIterator::isEmptyCell()
    {
        return isEmptyIndicator
            ? nextNonDefaultElem != currElem || !(buf[currElemOffs >> 3] & (1 << (currElemOffs & 7)))
            : ((emptyBitmap != NULL && (emptyBitmap[currElem >> 3] & (1 << (currElem & 7))) == 0)
               || (emptyBitmapIterator && !emptyBitmapIterator->setPosition(currPos)));
    }

    Value& SparseChunkIterator::getItem()
    {
        findNextAvailable();
        if (!hasCurrent)
            throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_NO_CURRENT_ELEMENT);
        if (nextNonDefaultElem != currElem) {
            return defaultValue;
        }
        uint32_t offs = currElemOffs;
        if (isNull) {
            value.setNull(offs);
        } else {
            if (elemSize == 1) { // bit vector
                value.setBool((buf[(offs >> 3)] & (1 << (offs & 7))) != 0);
            } else {
                size_t itemSize;
                uint8_t* src = (uint8_t*)(buf + offs);
                if (elemSize == 0) {
                    if (*src != 0) {
                        itemSize = *src++;
                    } else {
                        itemSize = (src[1] << 24) | (src[2] << 16) | (src[3] << 8) | src[4];
                        src += 5;
                    }
                } else {
                    itemSize = elemSize >> 3;
                }
                value.setData(src, itemSize);
            }
        }
        return value;
    }

    bool SparseChunkIterator::isEmpty()
    {
        findNextAvailable();
        return !(mode & IGNORE_EMPTY_CELLS) && isEmptyCell();
    }

    bool SparseChunkIterator::end()
    {
        findNextAvailable();
        return !hasCurrent;
    }

    inline bool SparseChunkIterator::isOutOfBounds()
    {
        for (size_t i = 0, n = currPos.size(); i < n; i++) {
            if (currPos[i] < firstPos[i] || currPos[i] > lastPos[i]) {
                return true;
            }
        }
        return false;
    }



    void SparseChunkIterator::operator ++()
    {
        findNextAvailable();
        if (skipDefaults) {
            if (elemsList != NULL) {
                if (hasCurrent) {
                    currElemIndex += 1;
                }
                if (_logicalChunkSize != (uint32_t)_logicalChunkSize) {
                    while (currElemIndex < nNonDefaultElems) {
                        if (!(mode & IGNORE_NULL_VALUES) || !elemsList64[currElemIndex].isNull)
                        {
                            currElem = nextNonDefaultElem = elemsList64[currElemIndex].position;
                            pos2coord(currElem, currPos);
                            if (!checkBounds || !isOutOfBounds()) {
                                currElemOffs = elemsList64[currElemIndex].offset;
                                isNull = elemsList64[currElemIndex].isNull;
                                hasCurrent = true;
                                return;
                            }
                        }
                        currElemIndex += 1;
                    }
                } else {
                    while (currElemIndex < nNonDefaultElems) {
                        if (!(mode & IGNORE_NULL_VALUES) || !elemsList[currElemIndex].isNull)
                        {
                            currElem = nextNonDefaultElem = elemsList[currElemIndex].position;
                            pos2coord(currElem, currPos);
                            if (!checkBounds || !isOutOfBounds()) {
                                currElemOffs = elemsList[currElemIndex].offset;
                                isNull = elemsList[currElemIndex].isNull;
                                hasCurrent = true;
                                return;
                            }
                        }
                        currElemIndex += 1;
                    }
                }
            } else {
                if (hasCurrent) {
                    ++curr;
                }
                while (curr != elemsMap.end()) {
                    if (!(mode & IGNORE_NULL_VALUES) || !curr->second.isNull)
                    {
                        currElem = nextNonDefaultElem = curr->first;
                        pos2coord(currElem, currPos);
                        if (!checkBounds || !isOutOfBounds()) {
                            currElemOffs = curr->second.offset;
                            isNull = curr->second.isNull;
                            hasCurrent = true;
                            return;
                        }
                    }
                    ++curr;
                }
            }
        } else if ((mode & IGNORE_EMPTY_CELLS) && emptyBitmapIterator) {
            if (hasCurrent) {
                ++(*emptyBitmapIterator);
            }
            while (!emptyBitmapIterator->end()) {
                currPos = emptyBitmapIterator->getPosition();
                currElem = coord2pos(currPos);
                setCurrPosition();
                if (!(mode & IGNORE_NULL_VALUES) || !isNull) { // out of bounds check is already perfromed by emopty iterator
                    hasCurrent = true;
                    return;
                }
                ++(*emptyBitmapIterator);
            }
        } else {
            if (hasCurrent) {
                currElem += 1;
            }
            while (currElem < _logicalChunkSize) {
                setCurrPosition();
                if ((!(mode & IGNORE_NULL_VALUES) || !isNull)
                    && (!(mode & IGNORE_EMPTY_CELLS) || !isEmptyCell()))
                {
                    pos2coord(currElem, currPos);
                    if (!checkBounds || !isOutOfBounds()) {
                        hasCurrent = true;
                        return;
                    }
                }
                currElem += 1;
            }
        }
        hasCurrent = false;
    }

    ConstChunk const& SparseChunkIterator::getChunk()
    {
        return *dataChunk;
    }

    Coordinates const& SparseChunkIterator::getPosition()
    {
        findNextAvailable();
        if (!hasCurrent)
            throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_NO_CURRENT_ELEMENT);
        return currPos;
    }

    bool SparseChunkIterator::setPosition(Coordinates const& pos)
    {
        moveToNextAvailable = false;
        currPos = pos;
        currElem = coord2pos(currPos);
        setCurrPosition();
        return hasCurrent =
            (!(mode & IGNORE_DEFAULT_VALUES) || nextNonDefaultElem == currElem)
            && (!(mode & IGNORE_NULL_VALUES) || !isNull)
            && (!(mode & IGNORE_EMPTY_CELLS) || !isEmptyCell())
            && !isOutOfBounds();
    }


    void SparseChunkIterator::reset()
    {
        currPos = firstPos;
        currElem = coord2pos(currPos);
        setCurrPosition();
        if (emptyBitmapIterator) {
            emptyBitmapIterator->reset();
        }
        hasCurrent = false;
        moveToNextAvailable = true;
    }

    void SparseChunkIterator::writeItem(const  Value& item)
    {
        findNextAvailable();
        if (!hasCurrent)
            throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_NO_CURRENT_ELEMENT);
        if (item != defaultValue) {
            SparseMapValue& val = elemsMap[currElem];
            if (val.offset == 0 || val.isNull) {
                if (!item.isNull()) {
                    val.offset = used;
                    if (elemSize == 1) {
                        if (((7 + used++) >> 3) >= allocated) {
                            dataChunk->reallocate(allocated *= 2);
                            buf = (char*)dataChunk->getData();
                        }
                    } else {
                        size_t size;
                        if (elemSize == 0) {
                            size = item.size();
                            if (size-1 >= 0xFF) {
                                size += 5;
                            } else {
                                size += 1;
                            }
                        } else {
                            size = elemSize >> 3;
                         }
                        used += size;
                        if (used > allocated) {
                            while (used > (allocated *= 2));
                            dataChunk->reallocate(allocated);
                            buf = (char*)dataChunk->getData();
                        }
                    }
                }
            }
            else
            {
                if (!elemSize)
                    throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_NOT_IMPLEMENTED) << "update of varying size type";
            }
            if (item.isNull())
            {
                if (!isNullable)
                    throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_ASSIGNING_NULL_TO_NON_NULLABLE);
                val.isNull = true;
                val.offset = item.getMissingReason();
            } else {
                uint32_t offs = val.offset;
                val.isNull = false;
                if (elemSize == 1) {
                    if (item.getBool()) {
                        buf[offs >> 3] |= 1 << (offs & 7);
                    } else {
                        buf[offs >> 3] &= ~(1 << (offs & 7));
                    }
                } else {
                    char* dst = buf + offs;
                    size_t itemSize;
                    if (elemSize == 0) {
                        itemSize = item.size();
                        if (itemSize-1 >= 0xFF) {
                            *dst++ = '\0';
                            *dst++ = char(itemSize >> 24);
                            *dst++ = char(itemSize >> 16);
                            *dst++ = char(itemSize >> 8);
                        }
                        *dst++ = char(itemSize);
                    } else {
                        itemSize = elemSize >> 3;
                        if (item.size() > itemSize) {
                            throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_TRUNCATION) << item.size() << itemSize;
                        }
                    }
                    memcpy(dst, item.data(), itemSize);
                }
            }
        }
        if (emptyBitmap != NULL) {
            emptyBitmap[currElem >> 3] |= (1 << (currElem & 7));
        } else if (emptyBitmapIterator)
        {
            if (!((boost::shared_ptr<ChunkIterator>&)emptyBitmapIterator)->setPosition(currPos))
                throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_OPERATION_FAILED) << "setPosition";
            ((boost::shared_ptr<ChunkIterator>&)emptyBitmapIterator)->writeItem(trueValue);
        }
    }

    void SparseChunkIterator::flush()
    {
       _needsFlush = false;
       boost::shared_ptr<Query> query(getQuery());
       size_t nElems = elemsMap.size();
       if (nElems > size_t(
#ifndef SCIDB_CLIENT
            Config::getInstance()->getOption<double>(CONFIG_DENSE_CHUNK_THRESHOLD)*dataChunk->getNumberOfElements(true)
#else
            DEFAULT_DENSE_CHUNK_THRESHOLD*dataChunk->getNumberOfElements(true)
#endif
            ))
        {
            MemChunk denseChunk;
            Address addr(attrID, firstPos);
            denseChunk.initialize(&dataChunk->getArray(), &array, addr, dataChunk->getCompressionMethod());
            {
                boost::shared_ptr<ChunkIterator> dst = denseChunk.getIterator(query, NO_EMPTY_CHECK);
                while (!end()) {
                    dst->setPosition(getPosition());
                    dst->writeItem(getItem());
                    ++(*this);
                }
                dst->flush();
            }
            dataChunk->reallocate(denseChunk.getSize());
            memcpy(dataChunk->getData(), denseChunk.getData(), denseChunk.getSize());
            dataChunk->setSparse(false);
        } else {
            size_t usedSize = elemSize == 1 ? ((used + 7) >> 3) : used;
            usedSize = (usedSize + 7) & ~7;
            dataChunk->reallocate(nElems*((_logicalChunkSize != (uint32_t)_logicalChunkSize) ? sizeof(SparseElem64) : sizeof(SparseElem)) + usedSize);
            SparseChunkHeader* hdr = (SparseChunkHeader*)dataChunk->getData();
            hdr->nElems = nElems;
            hdr->used = usedSize;
            if (_logicalChunkSize != (uint32_t)_logicalChunkSize) {
                SparseElem64* se = (SparseElem64*)((char*)hdr + usedSize);
                for (map<uint64_t, SparseMapValue>::iterator i = elemsMap.begin(); i != elemsMap.end(); i++, se++) {
                    se->position = i->first;
                    se->isNull = i->second.isNull;
                    se->offset = i->second.offset;
                }
            } else {
                SparseElem* se = (SparseElem*)((char*)hdr + usedSize);
                for (map<uint64_t, SparseMapValue>::iterator i = elemsMap.begin(); i != elemsMap.end(); i++, se++) {
                    se->position = (uint32_t)i->first;
                    se->isNull = i->second.isNull;
                    se->offset = i->second.offset;
                }
            }
            dataChunk->setSparse(true);
        }
        dataChunk->write(query);
        if (bitmapChunk != NULL) {
            if (emptyBitmapIterator) {
                ((boost::shared_ptr<ChunkIterator>&)emptyBitmapIterator)->flush();
            } else {
               bitmapChunk->write(query);
            }
        }
    }

    SparseChunkIterator::SparseChunkIterator(ArrayDesc const& desc, AttributeID attr, Chunk* dataChunk, Chunk* bitmapChunk, bool newChunk, int iterationMode, boost::shared_ptr<Query> const& query)
    : CoordinatesMapper(*dataChunk),
      array(desc),
      attrDesc(array.getAttributes()[attr]),
      attrID(attr),
      type( TypeLibrary::getType(attrDesc.getType())),
      value(type),
      trueValue(TypeLibrary::getType(TID_BOOL)),
      defaultValue(attrDesc.getDefaultValue()),
      firstPos(dataChunk->getFirstPosition(!(iterationMode & IGNORE_OVERLAPS))),
      lastPos(dataChunk->getLastPosition(!(iterationMode & IGNORE_OVERLAPS))),
      currPos(_nDims),
      isEmptyIndicator(attrDesc.isEmptyIndicator()),
      isNullDefault(defaultValue.isNull()),
      isNullable(attrDesc.isNullable()),
      skipDefaults((isNullDefault && (iterationMode & IGNORE_NULL_VALUES)) || (iterationMode & IGNORE_DEFAULT_VALUES) || ((iterationMode & IGNORE_EMPTY_CELLS) && isEmptyIndicator)),
      _query(query)
    {
        //This class is used for both - const and non-const iterators. We only use flush if we are open for writing.
        _needsFlush = newChunk || (iterationMode & ChunkIterator::APPEND_CHUNK);
        const Dimensions& dim = array.getDimensions();
        size_t nDims = dim.size();
        this->dataChunk = dataChunk;
        dataChunkPinned = dataChunk->pin();

        trueValue.setBool(true);
        mode = iterationMode;

        elemSize = type.bitSize() != 0 ? type.bitSize() : attrDesc.getVarSize() * 8;

        uint64_t visibleElems = 1;
        for (size_t i = 0; i < nDims; i++) {
            visibleElems *= lastPos[i] - firstPos[i] + 1;
        }
        checkBounds = visibleElems != _logicalChunkSize;

        if (newChunk) {
            double expectedDensity = dataChunk->getExpectedDensity();
            if (expectedDensity == 0) {
                expectedDensity =
#ifndef SCIDB_CLIENT
                    Config::getInstance()->getOption<double>(CONFIG_SPARSE_CHUNK_INIT_SIZE);
#else
                    DEFAULT_SPARSE_CHUNK_INIT_SIZE;
#endif
            }
            size_t initElems = size_t(_logicalChunkSize * expectedDensity);
            if (initElems == 0) {
                initElems = 1;
            }
            allocated = sizeof(SparseChunkHeader) +
                (  elemSize == 0 ?
#ifndef SCIDB_CLIENT
                    Config::getInstance()->getOption<int>(CONFIG_STRING_SIZE_ESTIMATION)*initElems
#else
                    DEFAULT_STRING_SIZE_ESTIMATION*initElems
#endif
                   : elemSize == 1 ? ((initElems + 7) >> 3)
                   : initElems*(elemSize >> 3));
            if (allocated > MAX_SPARSE_CHUNK_INIT_SIZE) {
                allocated = MAX_SPARSE_CHUNK_INIT_SIZE;
            }
            dataChunk->allocate(allocated);
            buf = (char*)dataChunk->getData();
            memset(buf, 0, allocated);
            used = sizeof(SparseChunkHeader);
            elemsList = NULL;
        } else {
            buf = (char*)dataChunk->getData();
            SparseChunkHeader* hdr = (SparseChunkHeader*)buf;
            allocated = dataChunk->getSize();
            used = hdr->used;
            nNonDefaultElems = hdr->nElems;
            if (iterationMode & APPEND_CHUNK) {
                if (_logicalChunkSize != (uint32_t)_logicalChunkSize) {
                    SparseElem64* se = (SparseElem64*)(buf + used);
                    for (size_t i = 0, n = nNonDefaultElems; i < n; i++, se++) {
                        SparseMapValue& val = elemsMap[se->position];
                        val.offset = se->offset;
                        val.isNull = se->isNull;
                    }
                } else {
                    SparseElem* se = (SparseElem*)(buf + used);
                    for (size_t i = 0, n = nNonDefaultElems; i < n; i++, se++) {
                        SparseMapValue& val = elemsMap[se->position];
                        val.offset = se->offset;
                        val.isNull = se->isNull;
                    }
                }
                elemsList = NULL;
            } else {
                elemsList = (SparseElem*)(buf + used);
                elemsList64 = (SparseElem64*)elemsList;
            }
        }
        // Set empty bitmap
        emptyBitmap = NULL;
        if (!(iterationMode & NO_EMPTY_CHECK) && bitmapChunk != NULL) {
            this->bitmapChunk = bitmapChunk;
            bitmapChunkPinned = bitmapChunk->pin();
            if (newChunk) {
                if (bitmapChunk->isSparse()) {
                    emptyBitmapIterator = bitmapChunk->getIterator(query, SPARSE_CHUNK);
                } else {
                    size_t bitmapSize = (_logicalChunkSize + 7) >> 3;
                    assert(!bitmapChunk->isSparse());
                    bitmapChunk->allocate(bitmapSize);
                    emptyBitmap = (char*)bitmapChunk->getData();
                    memset(emptyBitmap, 0, bitmapSize);
                }
                bitmapChunk->pin();
            } else {
                if (bitmapChunk->isSparse()) {
                    emptyBitmapIterator = bitmapChunk->getConstIterator((iterationMode & IGNORE_OVERLAPS)|((iterationMode & APPEND_CHUNK) ? 0 : IGNORE_EMPTY_CELLS|IGNORE_DEFAULT_VALUES)|SPARSE_CHUNK);
                } else {
                    emptyBitmap = (char*)bitmapChunk->getData();
                }
            }
        } else {
            bitmapChunkPinned = false;
            this->bitmapChunk = NULL;
        }
        if (elemSize == 1) {
            used <<= 3; // in bits
        }
        reset();
    }

    SparseChunkIterator::~SparseChunkIterator()
    {
        if (_needsFlush)
        {
            if (dataChunk) {
                dataChunk->unPin();
            }
            if (bitmapChunk) {
                bitmapChunk->unPin();
            }
        }

        if (dataChunkPinned) {
            dataChunk->unPin();
        }
        if (bitmapChunkPinned) {
            bitmapChunk->unPin();
        }
    }

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

    bool BaseChunkIterator::supportsVectorMode() const
    {
        return false; // not now
    }

    void BaseChunkIterator::setVectorMode(bool enabled)
    {
        if (enabled) {
            throw USER_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "BaseChunkIterator::setVectorMode";
        }
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
        if (!hasCurrent)
            throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_NO_CURRENT_ELEMENT);
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
            return hasCurrent = emptyBitmapIterator.setPosition(pos);
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
            hasOverlap |= dim[i].getChunkOverlap() != 0;
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
      payload((char*)data->getData()),
      payloadIterator(&payload),
      value(type)
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
            value.getTile(typeId)->unPackTile(payload, *emptyBitmap, tilePos, end);
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
      value(type)
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
            value.getTile(typeId)->unPackTile(*emptyBitmap, tilePos, end);
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
      tileValue(type, true),
      payload(type),
      bitmapChunk(bitmap),
      appender(&payload),
      prevPos(0)
    {
        emptyBitmap = shared_ptr<RLEEmptyBitmap>(new RLEEmptyBitmap(_logicalChunkSize));
        emptyBitmapIterator = emptyBitmap->getIterator();
        hasCurrent = !emptyBitmapIterator.end();

        if (iterationMode & ConstChunkIterator::APPEND_CHUNK) {
            if (iterationMode & ConstChunkIterator::SEQUENTIAL_WRITE) {
                throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_INVALID_OPERATION_FOR_SEQUENTIAL_MODE);
            }
            if (isEmptyable) {
                shared_ptr<ConstChunkIterator> it = data->getConstIterator(ConstChunkIterator::APPEND_CHUNK|ConstChunkIterator::IGNORE_EMPTY_CELLS);
                while (!it->end()) {
                    _values[coord2pos(it->getPosition())] = it->getItem();
                    ++(*it);
                }
            } else {
                ConstRLEPayload payload((char*)data->getData());
                ConstRLEPayload::iterator it(&payload);
                while (!it.end()) {
                    if (it.isDefaultValue(defaultValue)) {
                        it.toNextSegment();
                    } else {
                        it.getItem(_values[it.getPPos()]);
                        ++it;
                    }
                }
            }
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
            if (isEmpty()) {
                tmpValue = defaultValue;
                return tmpValue;
            }
            return _values[getPos()];
        }
    }

    void RLEChunkIterator::writeItem(const Value& item)
    {
        if (!hasCurrent)
            throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_NO_CURRENT_ELEMENT);
        if (mode & TILE_MODE) {
            RLEPayload* tile = item.getTile();
            if (tile->count() == INFINITE_LENGTH) {
                position_t end = min(tilePos + tileSize, _logicalChunkSize);
                tile->trim(end - tilePos);
            }
            payload.append(*tile);
        } else {
            if (item.isNull() && !attr.isNullable())
                throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_ASSIGNING_NULL_TO_NON_NULLABLE);

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
                    position_t pos = emptyBitmapIterator.getLPos();
                    ASSERT_EXCEPTION(pos>=prevPos, "It is an internal bug in the system that the SEQUENTIAL_WRITE rule is violated.");
                    if (pos != prevPos) {
                        appender.add(falseValue, pos - prevPos);
                    }
                    prevPos = pos+1;
                } else if (!isEmptyable) {
                    position_t pos = emptyBitmapIterator.getPPos();
                    ASSERT_EXCEPTION(pos>=prevPos, "It is an internal bug in the system that the SEQUENTIAL_WRITE rule is violated.");
                    if (pos != prevPos) {
                        appender.add(defaultValue, pos - prevPos);
                    }
                    prevPos = pos+1;
                } else {
                    // Note from DZ: this sanity check is important to avoid wrong-result bugs such as #4127.
                    // Basically, if a chunk is in SEQUENTIAL_WRITE mode, items must be appended in increasing coordinates.
                    //
                    position_t pos = emptyBitmapIterator.getLPos();
                    ASSERT_EXCEPTION(pos>=prevPos, "It is an internal bug in the system that the SEQUENTIAL_WRITE rule is violated.");
                    prevPos = pos+1;
                }
                appender.add(item);
            } else {
                if (!type.variableSize() && item.size() > type.byteSize()) {
                    throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_TRUNCATION) << item.size() << type.byteSize();
                }
                _values[getPos()] = item;
            }
            if (emptyChunkIterator) {
                if (!emptyChunkIterator->setPosition(getPosition()))
                    throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_OPERATION_FAILED) << "setPosition";
                emptyChunkIterator->writeItem(trueValue);
            }
        }
    }

    void RLEChunkIterator::flush()
    {
        _needsFlush = false;
        if (!(mode & (SEQUENTIAL_WRITE|TILE_MODE))) {
            if (isEmptyIndicator) {
                RLEEmptyBitmap bitmap(_values);
                dataChunk->allocate(bitmap.packedSize());
                bitmap.pack((char*)dataChunk->getData());
            } else {
                RLEPayload payload(_values, emptyBitmap->count(), type.byteSize(), attr.getDefaultValue(), type.bitSize()==1, isEmptyable);
                if (isEmptyable && (mode & APPEND_CHUNK)) {
                    RLEEmptyBitmap bitmap(_values, true);
                    dataChunk->allocate(payload.packedSize() + bitmap.packedSize());
                    payload.pack((char*)dataChunk->getData());
                    bitmap.pack((char*)dataChunk->getData() + payload.packedSize());
                } else {
                    dataChunk->allocate(payload.packedSize());
                    payload.pack((char*)dataChunk->getData());
                }
            }
        } else {
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
            }

            else {
                if ((mode & (TILE_MODE|SEQUENTIAL_WRITE)) == SEQUENTIAL_WRITE) {
                    if (!isEmptyable) {
                        position_t count = emptyBitmap->count();
                        if (count != prevPos) {
                            assert(count > prevPos);
                            appender.add(defaultValue, count - prevPos);
                        }
                    }
                    appender.flush();
                }
            }

            if (emptyChunkIterator && (mode & TILE_MODE))
                throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_CANT_UPDATE_BITMAP_IN_TILE_MODE);
            if (isEmptyIndicator) {
                RLEEmptyBitmap bitmap(payload);
                dataChunk->allocate(bitmap.packedSize());
                bitmap.pack((char*)dataChunk->getData());
            } else {
                if (isEmptyable && (mode & APPEND_CHUNK))
                {
                    if (!bitmapChunk)
                        throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_NO_ASSOCIATED_BITMAP_CHUNK);
                    boost::shared_ptr<ConstRLEEmptyBitmap> bitmap = bitmapChunk->getEmptyBitmap();
                    if (bitmap) {
                        dataChunk->allocate(payload.packedSize() + bitmap->packedSize());
                        payload.pack((char*)dataChunk->getData());
                        bitmap->pack((char*)dataChunk->getData() + payload.packedSize());
                    } else {
                        dataChunk->allocate(payload.packedSize());
                        payload.pack((char*)dataChunk->getData());
                    }
                } else {
                    dataChunk->allocate(payload.packedSize());
                    payload.pack((char*)dataChunk->getData());
                }
            }
        }
        if (mode & SPARSE_CHUNK) {
            dataChunk->setSparse(true);
        }
        boost::shared_ptr<Query> query(getQuery());
        dataChunk->write(query);
        if (emptyChunkIterator) {
            emptyChunkIterator->flush();
        }
    }

    // New Tile based iterators

    boost::shared_ptr<ConstRLEEmptyBitmap> BaseTileChunkIterator::getEmptyBitmap()
    {
        return _emptyBitmap;
    }

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
            return _hasCurrent = false;
        }
        position_t pos = coord2pos(coord);
        return _hasCurrent = _emptyBitmapIterator.setPosition(pos);
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
            return _hasCurrent = false;
        }
        return _hasCurrent = _emptyBitmapIterator.setPosition(lPos);
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
        _hasOverlap |= dim[i].getChunkOverlap() != 0;
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
    _lPosition(-1),
    _tileFactory(TileFactory::getInstance()),
    _fastTileInitialize(false),
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
                      << data->getData());
    }
    assert(_tileFactory);

    prepare();
    UnPinner dataUP(_dataChunk);

    if (((_mode & APPEND_CHUNK) || bitmap == NULL) && _payload->packedSize() < data->getSize()) {
        _emptyBitmap = make_shared<ConstRLEEmptyBitmap>(static_cast<char*>(data->getData())
                                                        + _payload->packedSize());
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
    }
    _emptyBitmapIterator = _emptyBitmap->getIterator();
    _hasCurrent = !_emptyBitmapIterator.end();

    if (_sDebug) {
        LOG4CXX_TRACE(logger, func << " this="<<this
                      <<" ebmCount="<< _emptyBitmap->count()
                      <<" pCount="<<   _payload->count());
    }

    assert(_emptyBitmap->count() <= _payload->count());

    _fastTileInitialize = (_emptyBitmap->count() == _payload->count()) &&
                          (!_payload->isBool()) &&
                          (_payload->elementSize() > 0) ;
    if (_sDebug) {
        LOG4CXX_TRACE(logger, func << " this="<<this
                      <<" data="<<data
                      <<" bitmap "<<bitmap
                      <<" attr "<<_attr
                      <<" payload data="
                      << data->getData()
                      << " #segments=" << _payload->nSegments()
                      <<" pCount="     << _payload->count());
    }

    if (_hasCurrent) {
        _lPosition = _emptyBitmapIterator.getLPos();
    }
    _payload.reset();
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

void RLETileConstChunkIterator::prepare()
{
    assert(_dataChunk);
    _dataChunk->pin();

    UnPinner dataUP(_dataChunk);

    _payload = make_shared<ConstRLEPayload>(static_cast<char*>(_dataChunk->getData()));
    _payloadIterator = _payload->getIterator();

    dataUP.set(NULL); // keep it pinned
}

void RLETileConstChunkIterator::reset()
{
    assert(! (_mode & TILE_MODE));
    assert(_emptyBitmap);
    assert(!_payload);

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
    assert(!_payload);
    assert(_emptyBitmap);

    prepare();
    UnPinner dataUP(_dataChunk);
    assert(_payload);
    assert(_lPosition >= 0);

    assert(_emptyBitmapIterator.getLPos() == _lPosition);
    alignIterators();

    _payloadIterator.getItem(_value);
    _payload.reset();
    return _value;
}

const Coordinates&
RLETileConstChunkIterator::getData(scidb::Coordinates& offset,
                                   size_t maxValues,
                                   boost::shared_ptr<BaseTile>& tileData,
                                   boost::shared_ptr<BaseTile>& tileCoords)
{
    const scidb::TypeId& coordinatesType = "scidb::Coordinates";
    const CoordinatesMapperWrapper coordMapper(this);

    if (offset.empty()) {
        return offset;
    }
    position_t logicalStartPos = coord2pos(offset);
    logicalStartPos = getDataInternal(logicalStartPos, maxValues,
                                      tileData, tileCoords,
                                      coordinatesType, &coordMapper);
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
    const scidb::TypeId& coordinatesType = "scidb::Coordinates";
    const CoordinatesMapperWrapper coordMapper(this);
    return getDataInternal(logicalOffset, maxValues, tileData, tileCoords,
                           coordinatesType, &coordMapper);
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
    assert(!_payload);
    assert(_emptyBitmap);

    prepare();
    UnPinner dataUP(_dataChunk);
    assert(_payload);
    bool needSetPosition = (_emptyBitmapIterator.end() ||
                            (logicalOffset != _emptyBitmapIterator.getLPos()));

    if ( needSetPosition &&
        !BaseTileChunkIterator::setPosition(logicalOffset)) {
        assert(!_hasCurrent);
        _lPosition = -1;
        _payload.reset();
        assert(!_hasCurrent);
        return _lPosition;
    }

    shared_ptr<BaseTile> dataTile  = _tileFactory->construct(_attr.getType(), BaseEncoding::RLE);

    if (_fastTileInitialize) {
        position_t pPosition = _emptyBitmapIterator.getPPos();
        assert(pPosition>=0);

        RLEPayloadDesc rlePDesc(_payload.get(), pPosition, maxValues);
        dataTile->getEncoding()->initialize(&rlePDesc);

        assert(dataTile->size()>0);
        bool ret = _emptyBitmapIterator.skip(dataTile->size()-1);
        assert(ret); ret=ret;
        assert(!_emptyBitmapIterator.end());
        ++_emptyBitmapIterator;

    } else {

        LOG4CXX_TRACE(logger, func << " SLOW tile init: "
                      << " isBool="<<_payload->isBool()
                      << " elemSize="<<_payload->elementSize()
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

    _payload.reset(); // dont need payload any more

    if (!_emptyBitmapIterator.end()) {
        _lPosition = _emptyBitmapIterator.getLPos();
        assert(_lPosition >=0 &&
               _lPosition > logicalOffset);
        _hasCurrent = true;
    } else {
        // end of chuk data
        _hasCurrent = false;
        _lPosition = -1;
    }
    tileData.swap(dataTile);
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
    assert(!_payload);
    assert(_emptyBitmap);

    prepare();
    UnPinner dataUP(_dataChunk);
    assert(_payload);
    bool needSetPosition = (_emptyBitmapIterator.end() ||
                            (logicalOffset != _emptyBitmapIterator.getLPos()));

    if ( needSetPosition &&
        !BaseTileChunkIterator::setPosition(logicalOffset)) {
        assert(!_hasCurrent);
        _lPosition = -1;
        _payload.reset();
        assert(!_hasCurrent);
        return _lPosition;
    }

    shared_ptr<BaseTile> dataTile = _tileFactory->construct(_attr.getType(), BaseEncoding::RLE);

    if (_fastTileInitialize) {

        position_t pPosition = _emptyBitmapIterator.getPPos();
        assert(pPosition>=0);

        RLEPayloadDesc rlePDesc(_payload.get(), pPosition, maxValues);
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
    _payload.reset(); // dont need payload any more

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
    assert(!_payload);
    assert(_emptyBitmap);

    assert(_emptyBitmapIterator.getLPos() == _lPosition);

    ++_emptyBitmapIterator;
    if (_emptyBitmapIterator.end()) {
        _hasCurrent = false;
        _payload.reset();
        return;
    }

    _lPosition = _emptyBitmapIterator.getLPos();
    _payload.reset();
}

Coordinates const& RLETileConstChunkIterator::getPosition()
{
    assert(! (_mode & TILE_MODE));
    assert(!_payload);
    assert(_emptyBitmap);

    assert(_emptyBitmapIterator.getLPos() == _lPosition);

    Coordinates const& coords = BaseTileChunkIterator::getPosition();
    assert((&coords) == (&_currPos));
    return coords;
}

bool RLETileConstChunkIterator::setPosition(Coordinates const& coord)
{
    assert(! (_mode & TILE_MODE));
    assert(!_payload);
    assert(_emptyBitmap);

    if (!BaseTileChunkIterator::setPosition(coord)) {
        assert(!_hasCurrent);
        _payload.reset();
        return false;
    }

    _lPosition = _emptyBitmapIterator.getLPos();
    return true;
}

bool RLETileConstChunkIterator::setPosition(position_t lPos)
{
    assert(! (_mode & TILE_MODE));
    assert(!_payload);
    assert(_emptyBitmap);
    assert(lPos>=0);

    if (!BaseTileChunkIterator::setPosition(lPos)) {
        assert(!_hasCurrent);
        _payload.reset();
        return false;
    }

    _lPosition = _emptyBitmapIterator.getLPos();
    assert(lPos == _lPosition);
    return true;
}

}
