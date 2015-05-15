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
 * @file Array.cpp
 *
 * @brief Array API
 *
 * @author Konstantin Knizhnik <knizhnik@garret.ru>
 */

#include <vector>
#include <string.h>
#include <boost/assign.hpp>

#include <util/Platform.h>
#include <array/MemArray.h>
#include <array/RLE.h>
#include <array/AllocationBuffer.h>
#include <system/Exceptions.h>
#include <query/FunctionDescription.h>
#include <query/TypeSystem.h>
#include <query/Statistics.h>
#include <query/Query.h>

#ifndef SCIDB_CLIENT
#include "system/Config.h"
#endif

#include <system/SciDBConfigOptions.h>
#include <log4cxx/logger.h>
#include <query/Operator.h>
#include <array/DeepChunkMerger.h>

using namespace boost::assign;

namespace scidb
{
    // Logger for operator. static to prevent visibility of variable outside of file
    static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.array.Array"));

    void SharedBuffer::free()
    {
        assert(false);
        throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_UNREACHABLE_CODE) << "SharedBuffer::free";
    }

    void SharedBuffer::allocate(size_t size)
    {
        assert(false);
        throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_UNREACHABLE_CODE) << "SharedBuffer::allocate";
    }

    void SharedBuffer::reallocate(size_t size)
    {
        assert(false);
        throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_UNREACHABLE_CODE) << "SharedBuffer::reallocate";
    }

    void* CompressedBuffer::getData() const
    {
        return data;
    }

    size_t CompressedBuffer::getSize() const
    {
        return compressedSize;
    }

    void CompressedBuffer::allocate(size_t size)
    {
        data = ::malloc(size);
        if (data == NULL) {
            throw SYSTEM_EXCEPTION(SCIDB_SE_NO_MEMORY, SCIDB_LE_CANT_ALLOCATE_MEMORY);
        }
        compressedSize = size;
        currentStatistics->allocatedSize += size;
        currentStatistics->allocatedChunks++;
    }

    void CompressedBuffer::reallocate(size_t size)
    {
        void *tmp = ::realloc(data, size);
        if (tmp == NULL) {
            throw SYSTEM_EXCEPTION(SCIDB_SE_NO_MEMORY, SCIDB_LE_CANT_ALLOCATE_MEMORY);
        }
        data = tmp;
        compressedSize = size;
        currentStatistics->allocatedSize += size;
        currentStatistics->allocatedChunks++;
    }

    void CompressedBuffer::free()
    {
        if (isDebug() && data && compressedSize) {
            memset(data, 0, compressedSize);
        }
       ::free(data);
       data = NULL;
    }

    bool CompressedBuffer::pin() const
    {
        ASSERT_EXCEPTION(false, "CompressedBuffer::pin: ");
        return false;
    }

    void CompressedBuffer::unPin() const
    {
        ASSERT_EXCEPTION(false, "CompressedBuffer::unpin: ");
    }


    int CompressedBuffer::getCompressionMethod() const
    {
        return compressionMethod;
    }

    void CompressedBuffer::setCompressionMethod(int m)
    {
        compressionMethod = m;
    }

    size_t CompressedBuffer::getDecompressedSize() const
    {
        return decompressedSize;
    }

    void CompressedBuffer::setDecompressedSize(size_t size)
    {
        decompressedSize = size;
    }

    CompressedBuffer::CompressedBuffer(void* compressedData, int compressionMethod, size_t compressedSize, size_t decompressedSize)
    {
        data = compressedData;
        this->compressionMethod = compressionMethod;
        this->compressedSize = compressedSize;
        this->decompressedSize = decompressedSize;
    }

    CompressedBuffer::CompressedBuffer()
    {
        data = NULL;
        compressionMethod = 0;
        compressedSize = 0;
        decompressedSize = 0;
    }

    CompressedBuffer::~CompressedBuffer()
    {
        free();
    }

    AllocationBuffer::AllocationBuffer(const arena::ArenaPtr& arena)
                    : _arena(arena),
                      _data(0),
                      _size(0)
    {}

    AllocationBuffer::~AllocationBuffer()
    {
        this->free();
    }

    void* AllocationBuffer::getData() const
    {
        return _data;
    }

    size_t AllocationBuffer::getSize() const
    {
        return _size;
    }

    bool AllocationBuffer::pin() const
    {
        return false;
    }

    void AllocationBuffer::unPin() const
    {}

    void AllocationBuffer::allocate(size_t n)
    {
        _data = _arena->allocate(_size = n);
    }

    void AllocationBuffer::free()
    {
        _arena->recycle(_data);
        _data = 0;
    }

    size_t ConstChunk::getBitmapSize() const
    {
        if (isMaterialized() && !getAttributeDesc().isEmptyIndicator()) {
            PinBuffer scope(*this);
            ConstRLEPayload payload((char*)getData());
            return getSize() - payload.packedSize();
        }
        return 0;
    }

    ConstChunk const* ConstChunk::getBitmapChunk() const
    {
        return this;
    }

    void ConstChunk::makeClosure(Chunk& closure, boost::shared_ptr<ConstRLEEmptyBitmap> const& emptyBitmap) const
    {
        PinBuffer scope(*this);
        closure.allocate(getSize() + emptyBitmap->packedSize());
        memcpy(closure.getDataForLoad(), getData(), getSize());
        emptyBitmap->pack((char*)closure.getDataForLoad() + getSize());
    }

    ConstChunk* ConstChunk::materialize() const
    {
        if (materializedChunk == NULL || materializedChunk->getFirstPosition(false) != getFirstPosition(false)) {
            if (materializedChunk == NULL) {
                ((ConstChunk*)this)->materializedChunk = new MemChunk();
            }
            materializedChunk->initialize(*this);
            materializedChunk->setBitmapChunk((Chunk*)getBitmapChunk());
            boost::shared_ptr<ConstChunkIterator> src
                = getConstIterator((getArrayDesc().getEmptyBitmapAttribute() == NULL ?  ChunkIterator::IGNORE_DEFAULT_VALUES : 0 )|ChunkIterator::IGNORE_EMPTY_CELLS|ChunkIterator::INTENDED_TILE_MODE|(materializedChunk->getArrayDesc().hasOverlap() ? 0 : ChunkIterator::IGNORE_OVERLAPS));

            shared_ptr<Query> emptyQuery;
            boost::shared_ptr<ChunkIterator> dst
                = materializedChunk->getIterator(emptyQuery,
                                                 (src->getMode() & ChunkIterator::TILE_MODE)|ChunkIterator::ChunkIterator::NO_EMPTY_CHECK|ChunkIterator::SEQUENTIAL_WRITE);
            size_t count = 0;
            while (!src->end()) {
                if (!dst->setPosition(src->getPosition())) {
                    Coordinates const& pos = src->getPosition();
                    dst->setPosition(pos);
                    throw SYSTEM_EXCEPTION(SCIDB_SE_MERGE, SCIDB_LE_OPERATION_FAILED) << "setPosition";
                }
                dst->writeItem(src->getItem());
                count += 1;
                ++(*src);
            }
            if (!getArrayDesc().hasOverlap()) {
                materializedChunk->setCount(count);
            }
            dst->flush();
        }
        return materializedChunk;
    }

    void ConstChunk::compress(CompressedBuffer& buf, boost::shared_ptr<ConstRLEEmptyBitmap>& emptyBitmap) const
    {
        materialize()->compress(buf, emptyBitmap);
    }

    void* ConstChunk::getData() const
    {
        return materialize()->getData();
    }

    size_t ConstChunk::getSize() const
    {
        return materialize()->getSize();
    }

    bool ConstChunk::pin() const
    {
        return false;
    }
    void ConstChunk::unPin() const
    {
        assert(typeid(*this) != typeid(ConstChunk));
    }

    void Chunk::decompress(CompressedBuffer const& buf)
    {
        throw USER_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "Chunk::decompress";
    }

    void Chunk::merge(ConstChunk const& with, boost::shared_ptr<Query> const& query)
    {
        Query::validateQueryPtr(query);

        setCount(0); // unknown
        char* dst = (char*)getDataForLoad();

        // If dst already has data, merge; otherwise, copy.
        if ( dst != NULL )
        {
            if (isMemChunk() && with.isMemChunk()) {
                deepMerge(with, query);
            } else {
                shallowMerge(with, query);
            }
        }
        else {
            PinBuffer scope(with);
            char* src = (char*)with.getData();
            allocateAndCopy(src, with.getSize(), with.count(), query);
        }
    }

    void Chunk::deepMerge(ConstChunk const& with, boost::shared_ptr<Query> const& query)
    {
        assert(isMemChunk() && with.isMemChunk());

        Query::validateQueryPtr(query);

        DeepChunkMerger deepChunkMerger((MemChunk&)*this, (MemChunk const&)with, query);
        deepChunkMerger.merge();
    }

    void Chunk::shallowMerge(ConstChunk const& with, boost::shared_ptr<Query> const& query)
    {
        Query::validateQueryPtr(query);

        boost::shared_ptr<ChunkIterator> dstIterator =
            getIterator(query,
                        ChunkIterator::APPEND_CHUNK |
                        ChunkIterator::APPEND_EMPTY_BITMAP |
                        ChunkIterator::NO_EMPTY_CHECK);
        boost::shared_ptr<ConstChunkIterator> srcIterator = with.getConstIterator(ChunkIterator::IGNORE_EMPTY_CELLS|ChunkIterator::IGNORE_DEFAULT_VALUES);
        if (getArrayDesc().getEmptyBitmapAttribute() != NULL) {
            while (!srcIterator->end()) {
                if (!dstIterator->setPosition(srcIterator->getPosition()))
                    throw SYSTEM_EXCEPTION(SCIDB_SE_MERGE, SCIDB_LE_OPERATION_FAILED) << "setPosition";
                Value const& value = srcIterator->getItem();
                dstIterator->writeItem(value);
                ++(*srcIterator);
            }
        } else { // ignore default values
            Value const& defaultValue = getAttributeDesc().getDefaultValue();
            while (!srcIterator->end()) {
                Value const& value = srcIterator->getItem();
                if (value != defaultValue) {
                    if (!dstIterator->setPosition(srcIterator->getPosition()))
                        throw SYSTEM_EXCEPTION(SCIDB_SE_MERGE, SCIDB_LE_OPERATION_FAILED) << "setPosition";
                    dstIterator->writeItem(value);
                }
                ++(*srcIterator);
            }
        }
        dstIterator->flush();
    }

    void Chunk::aggregateMerge(ConstChunk const& with,
                               AggregatePtr const& aggregate,
                               boost::shared_ptr<Query> const& query)
    {
        Query::validateQueryPtr(query);

        if (isReadOnly())
            throw USER_EXCEPTION(SCIDB_SE_MERGE, SCIDB_LE_CANT_UPDATE_READ_ONLY_CHUNK);

        AttributeDesc const& attr = getAttributeDesc();

        if (aggregate->getStateType().typeId() != attr.getType())
            throw SYSTEM_EXCEPTION(SCIDB_SE_MERGE, SCIDB_LE_TYPE_MISMATCH_BETWEEN_AGGREGATE_AND_CHUNK);

        if (!attr.isNullable())
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_AGGREGATE_STATE_MUST_BE_NULLABLE);//enforce equivalency w above merge()

        setCount(0);
        char* dst = (char*)getDataForLoad();
        if (dst != NULL)
        {
            boost::shared_ptr<ChunkIterator> dstIterator =
                getIterator(query,
                            ChunkIterator::APPEND_CHUNK |
                            ChunkIterator::APPEND_EMPTY_BITMAP |
                            ChunkIterator::NO_EMPTY_CHECK);
            boost::shared_ptr<ConstChunkIterator> srcIterator = with.getConstIterator(ChunkIterator::IGNORE_NULL_VALUES);
            while (!srcIterator->end())
            {
                Value val = srcIterator->getItem();  // We need to make a copy here, because the mergeIfNeeded() call below may change it.

                if (!dstIterator->setPosition(srcIterator->getPosition())) {
                    throw SYSTEM_EXCEPTION(SCIDB_SE_MERGE, SCIDB_LE_OPERATION_FAILED) << "setPosition";
                }
                Value& val2 = dstIterator->getItem();
                aggregate->mergeIfNeeded(val, val2);
                dstIterator->writeItem(val);

                ++(*srcIterator);
            }
            dstIterator->flush();
        }
        else
        {
            PinBuffer scope(with);
            allocateAndCopy((char*)with.getData(), with.getSize(), with.count(), query);
        }
    }

    void Chunk::nonEmptyableAggregateMerge(ConstChunk const& with,
                                           AggregatePtr const& aggregate,
                                           boost::shared_ptr<Query> const& query)
    {
        Query::validateQueryPtr(query);

        if (isReadOnly())
        {
            throw USER_EXCEPTION(SCIDB_SE_MERGE, SCIDB_LE_CANT_UPDATE_READ_ONLY_CHUNK);
        }

        AttributeDesc const& attr = getAttributeDesc();

        if (aggregate->getStateType().typeId() != attr.getType())
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_MERGE, SCIDB_LE_TYPE_MISMATCH_BETWEEN_AGGREGATE_AND_CHUNK);
        }

        if (!attr.isNullable())
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_AGGREGATE_STATE_MUST_BE_NULLABLE);//enforce equivalency w above merge()
        }

        char* dst = static_cast<char*>(getDataForLoad());
        PinBuffer scope(with);
        if (dst != NULL)
        {
            boost::shared_ptr<ChunkIterator>dstIterator = getIterator(query,
                                                                      ChunkIterator::APPEND_CHUNK |
                                                                      ChunkIterator::APPEND_EMPTY_BITMAP |
                                                                      ChunkIterator::NO_EMPTY_CHECK);
            CoordinatesMapper mapper(with);
            ConstRLEPayload inputPayload( static_cast<char*>(with.getData()));
            ConstRLEPayload::iterator inputIter(&inputPayload);
            Value val;
            position_t lpos;
            Coordinates cpos(mapper.getNumDims());

            while (!inputIter.end())
            {
                //Missing Reason 0 is reserved by the system meaning "group does not exist".
                //All other Missing Reasons may be used by the aggregate if needed.
                if (inputIter.isNull() && inputIter.getMissingReason() == 0)
                {
                    inputIter.toNextSegment();
                }
                else
                {
                    inputIter.getItem(val);
                    lpos = inputIter.getPPos();
                    mapper.pos2coord(lpos, cpos);
                    if (!dstIterator->setPosition(cpos))
                        throw SYSTEM_EXCEPTION(SCIDB_SE_MERGE, SCIDB_LE_OPERATION_FAILED) << "setPosition";
                    Value& val2 = dstIterator->getItem();
                    aggregate->mergeIfNeeded(val, val2);
                    dstIterator->writeItem(val);
                    ++inputIter;
                }
            }
            dstIterator->flush();
        }
        else {
            allocateAndCopy(static_cast<char*>(with.getData()), with.getSize(), with.count(), query);
        }
    }

    void Chunk::setCount(size_t)
    {
    }

    void Chunk::truncate(Coordinate lastCoord)
    {
    }

    bool ConstChunk::contains(Coordinates const& pos, bool withOverlap) const
    {
        Coordinates const& first = getFirstPosition(withOverlap);
        Coordinates const& last = getLastPosition(withOverlap);
        for (size_t i = 0, n = first.size(); i < n; i++) {
            if (pos[i] < first[i] || pos[i] > last[i]) {
                return false;
            }
        }
        return true;
    }

    bool ConstChunk::isCountKnown() const
    {
        return getArrayDesc().getEmptyBitmapAttribute() == NULL
            || (materializedChunk && materializedChunk->isCountKnown());
    }

    size_t ConstChunk::count() const
    {
        if (getArrayDesc().getEmptyBitmapAttribute() == NULL) {
            return getNumberOfElements(false);
        }
        if (materializedChunk) {
            return materializedChunk->count();
        }
        shared_ptr<ConstChunkIterator> i = getConstIterator();
        size_t n = 0;
        while (!i->end()) {
            ++(*i);
            n += 1;
        }
        return n;
    }

    size_t ConstChunk::getNumberOfElements(bool withOverlap) const
    {
        Coordinates low = getFirstPosition(withOverlap);
        Coordinates high = getLastPosition(withOverlap);
        return getChunkNumberOfElements(low, high);
    }

    ConstIterator::~ConstIterator() {}

    Coordinates const& ConstChunkIterator::getFirstPosition()
    {
        return getChunk().getFirstPosition((getMode() & IGNORE_OVERLAPS) == 0);
    }

    Coordinates const& ConstChunkIterator::getLastPosition()
    {
        return getChunk().getLastPosition((getMode() & IGNORE_OVERLAPS) == 0);
    }

        bool ConstChunkIterator::forward(uint64_t direction)
    {
        Coordinates pos = getPosition();
        Coordinates const& last = getLastPosition();
        do {
            for (size_t i = 0; direction != 0; i++, direction >>= 1) {
                if (direction & 1) {
                    if (++pos[i] > last[i]) {
                        return false;
                    }
                }
            }
        } while (!setPosition(pos));
        return true;
    }

        bool ConstChunkIterator::backward(uint64_t direction)
    {
        Coordinates pos = getPosition();
        Coordinates const& first = getFirstPosition();
        do {
            for (size_t i = 0; direction != 0; i++, direction >>= 1) {
                if (direction & 1) {
                    if (--pos[i] < first[i]) {
                        return false;
                    }
                }
            }
        } while (!setPosition(pos));
        return true;
    }

    bool ConstChunk::isSolid() const
    {
        Dimensions const& dims = getArrayDesc().getDimensions();
        Coordinates const& first = getFirstPosition(false);
        Coordinates const& last = getLastPosition(false);
        for (size_t i = 0, n = dims.size(); i < n; i++) {
            if (dims[i].getChunkOverlap() != 0 || (last[i] - first[i] + 1) != dims[i].getChunkInterval()) {
                return false;
            }
        }
        return !getAttributeDesc().isNullable()
            && !TypeLibrary::getType(getAttributeDesc().getType()).variableSize()
            && getArrayDesc().getEmptyBitmapAttribute() == NULL;
    }

    bool ConstChunk::isReadOnly() const
    {
        return true;
    }

    bool ConstChunk::isMaterialized() const
    {
        return false;
    }

    boost::shared_ptr<ConstRLEEmptyBitmap> ConstChunk::getEmptyBitmap() const
    {
        if (getAttributeDesc().isEmptyIndicator()/* && isMaterialized()*/) {
            PinBuffer scope(*this);
            return boost::shared_ptr<ConstRLEEmptyBitmap>(scope.isPinned() ? new ConstRLEEmptyBitmap(*this) : new RLEEmptyBitmap(ConstRLEEmptyBitmap(*this)));
        }
        AttributeDesc const* emptyAttr = getArrayDesc().getEmptyBitmapAttribute();
        if (emptyAttr != NULL) {
            if (!emptyIterator) {
                ((ConstChunk*)this)->emptyIterator = getArray().getConstIterator(emptyAttr->getId());
            }
            if (!emptyIterator->setPosition(getFirstPosition(false))) {
                throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_OPERATION_FAILED) << "setPosition";
            }
            ConstChunk const& bitmapChunk = emptyIterator->getChunk();
            PinBuffer scope(bitmapChunk);
            return boost::shared_ptr<ConstRLEEmptyBitmap>(new RLEEmptyBitmap(ConstRLEEmptyBitmap((char*)bitmapChunk.getData())));
        }
        return boost::shared_ptr<ConstRLEEmptyBitmap>();
    }


    ConstChunk::ConstChunk() : materializedChunk(NULL)
    {
    }

    ConstChunk::~ConstChunk()
    {
        delete materializedChunk;
    }

    std::string const& Array::getName() const
    {
        return getArrayDesc().getName();
    }

    ArrayID Array::getHandle() const
    {
        return getArrayDesc().getId();
    }

    void Array::append(boost::shared_ptr<Array>& input,
                       bool const vertical,
                       set<Coordinates, CoordinatesLess>* newChunkCoordinates)
    {
        if (vertical)
        {
            assert(input->getSupportedAccess() >= MULTI_PASS);

            for (size_t i = 0, n = getArrayDesc().getAttributes().size(); i < n; i++) {
                boost::shared_ptr<ArrayIterator> dst = getIterator(i);
                boost::shared_ptr<ConstArrayIterator> src = input->getConstIterator(i);
                while (!src->end())
                {
                    if(newChunkCoordinates && i == 0)
                    {
                        newChunkCoordinates->insert(src->getPosition());
                    }
                    dst->copyChunk(src->getChunk());
                    ++(*src);
                }
            }
        }
        else
        {
            size_t nAttrs = getArrayDesc().getAttributes().size();
            std::vector< boost::shared_ptr<ArrayIterator> > dstIterators(nAttrs);
            std::vector< boost::shared_ptr<ConstArrayIterator> > srcIterators(nAttrs);
            for (size_t i = 0; i < nAttrs; i++)
            {
                dstIterators[i] = getIterator(i);
                srcIterators[i] = input->getConstIterator(i);
            }
            while (!srcIterators[0]->end())
            {
                if(newChunkCoordinates)
                {
                    newChunkCoordinates->insert(srcIterators[0]->getPosition());
                }
                for (size_t i = 0; i < nAttrs; i++)
                {
                    boost::shared_ptr<ArrayIterator> dst = dstIterators[i];
                    boost::shared_ptr<ConstArrayIterator> src = srcIterators[i];
                    dst->copyChunk(src->getChunk());
                    ++(*src);
                }
            }
        }
    }

    boost::shared_ptr<CoordinateSet> Array::getChunkPositions() const
    {
        throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_UNKNOWN_ERROR) << "calling getChunkPositions on an invalid array";
    }

    shared_ptr<CoordinateSet> Array::findChunkPositions() const
    {
        if (hasChunkPositions())
        {
            return getChunkPositions();
        }
        if (getSupportedAccess() == SINGLE_PASS)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_OPERATOR, SCIDB_LE_UNSUPPORTED_INPUT_ARRAY) << "findChunkPositions";
        }
        //Heuristic: make an effort to scan the empty tag. If there is no empty tag - scan the smallest fixed-sized attribute.
        //If an attribute is small in size (bool or uint8), chances are it takes less disk scan time and/or less compute time to pass over it.
        ArrayDesc const& schema = getArrayDesc();
        AttributeDesc const* attributeToScan = schema.getEmptyBitmapAttribute();
        if (attributeToScan == NULL)
        {
            //The array doesn't have an empty tag. Let's pick the smallest fixed-size attribute.
            attributeToScan = &schema.getAttributes()[0];
            size_t scannedAttributeSize = attributeToScan->getSize();
            for(size_t i = 1, n = schema.getAttributes().size(); i < n; i++)
            {
                AttributeDesc const& candidate = schema.getAttributes()[i];
                size_t candidateSize = candidate.getSize();
                if ( candidateSize != 0 && (candidateSize < scannedAttributeSize || scannedAttributeSize == 0))
                {
                    attributeToScan = &candidate;
                    scannedAttributeSize = candidateSize;
                }
            }
        }
        assert(attributeToScan != NULL);
        AttributeID victimId = attributeToScan->getId();
        boost::shared_ptr<CoordinateSet> result(new CoordinateSet());
        //Iterate over the target attribute, find the position of each chunk, add all chunk positions to result
        boost::shared_ptr<ConstArrayIterator> iter = getConstIterator(victimId);
        while( ! iter->end() )
        {
            result->insert(iter->getPosition());
            ++(*iter);
        }
        return result;
    }


    static char* copyStride(char* dst, char* src, Coordinates const& first, Coordinates const& last, Dimensions const& dims, size_t step, size_t attrSize, size_t c)
    {
        size_t n = dims[c].getChunkInterval();
        if (c+1 == dims.size()) {
            memcpy(dst, src, n*attrSize);
            src += n*attrSize;
        } else {
            step /= last[c] - first[c] + 1;
            for (size_t i = 0; i < n; i++) {
                src = copyStride(dst, src, first, last, dims, step, attrSize, c+1);
                dst += step*attrSize;
            }
        }
        return src;
    }

    size_t Array::extractData(AttributeID attrID, void* buf,
                              Coordinates const& first, Coordinates const& last,
                              Array::extractInit_t init,
                              extractNull_t null) const
    {
        ArrayDesc const& arrDesc = getArrayDesc();
        AttributeDesc const& attrDesc = arrDesc.getAttributes()[attrID];
        Type attrType( TypeLibrary::getType(attrDesc.getType()));
        Dimensions const& dims = arrDesc.getDimensions();
        size_t nDims = dims.size();
        if (attrType.variableSize()) {
            throw USER_EXCEPTION(SCIDB_SE_EXECUTION,
                                 SCIDB_LE_EXTRACT_EXPECTED_FIXED_SIZE_ATTRIBUTE);
        }

        if (attrType.bitSize() < 8) {
            throw USER_EXCEPTION(SCIDB_SE_EXECUTION,
                                 SCIDB_LE_EXTRACT_UNEXPECTED_BOOLEAN_ATTRIBUTE);
        }

        if (first.size() != nDims || last.size() != nDims) {
            throw USER_EXCEPTION(SCIDB_SE_EXECUTION,
                                 SCIDB_LE_WRONG_NUMBER_OF_DIMENSIONS);
        }

        size_t bufSize = 1;
        for (size_t j = 0; j < nDims; j++) {
            if (last[j] < first[j] ||
                (first[j] - dims[j].getStartMin()) % dims[j].getChunkInterval() != 0) {
                throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_UNALIGNED_COORDINATES);
            }
            bufSize *= last[j] - first[j] + 1;
        }

        size_t attrSize = attrType.byteSize();

        switch(init) {
        case EXTRACT_INIT_ZERO:
            memset(buf, 0, bufSize*attrSize);
            break;
        case EXTRACT_INIT_NAN:
            {
                TypeEnum typeEnum = typeId2TypeEnum(attrType.typeId());
                if (typeEnum == TE_FLOAT) {
                    float * bufFlt = reinterpret_cast<float*>(buf);
                    std::fill(bufFlt, bufFlt+bufSize, NAN);
                } else if (typeEnum == TE_DOUBLE) {
                    double * bufDbl = reinterpret_cast<double*>(buf);
                    std::fill(bufDbl, bufDbl+bufSize, NAN);
                } else {
                    assert(false); // there is no such thing as NAN for these types.  The calling programmer made a serious error.
                    SCIDB_UNREACHABLE();
                }
            }
            break;
        default:
            SCIDB_UNREACHABLE(); // all cases should have been enumerated.;
        }

        size_t nExtracted = 0;
        for (boost::shared_ptr<ConstArrayIterator> i = getConstIterator(attrID);
             !i->end(); ++(*i)) {
            size_t j, chunkOffs = 0;
            ConstChunk const& chunk = i->getChunk();
            Coordinates const& chunkPos = i->getPosition();
            for (j = 0; j < nDims; j++) {
                if (chunkPos[j] < first[j] || chunkPos[j] > last[j]) {
                    break;
                }
                chunkOffs *= last[j] - first[j] + 1;
                chunkOffs += chunkPos[j] - first[j];
            }
            if (j == nDims) {
                for (boost::shared_ptr<ConstChunkIterator> ci =
                         chunk.getConstIterator(ChunkIterator::IGNORE_OVERLAPS |
                                                ChunkIterator::IGNORE_EMPTY_CELLS |
                                                ChunkIterator::IGNORE_NULL_VALUES);
                     !ci->end(); ++(*ci)) {
                    Value& v = ci->getItem();
                    if (!v.isNull()) {
                        Coordinates const& itemPos = ci->getPosition();
                        size_t itemOffs = 0;
                        for (j = 0; j < nDims; j++) {
                            itemOffs *= last[j] - first[j] + 1;
                            itemOffs += itemPos[j] - first[j];
                        }
                        memcpy((char*)buf + itemOffs*attrSize,
                               ci->getItem().data(), attrSize);
                    } else if (null==EXTRACT_NULL_AS_NAN) {
                        Coordinates const& itemPos = ci->getPosition();
                        size_t itemOffs = 0;
                        for (j = 0; j < nDims; j++) {
                            itemOffs *= last[j] - first[j] + 1;
                            itemOffs += itemPos[j] - first[j];
                        }
                        TypeEnum typeEnum = typeId2TypeEnum(attrType.typeId());
                        // historically, no alignment guarantee on buf
                        char * itemAddr = (char*)buf + itemOffs*attrSize;
                        if (typeEnum == TE_FLOAT) {
                            float nan=NAN;
                            std::copy((char*)&nan, (char*)&nan + sizeof(nan), itemAddr);
                        } else if (typeEnum == TE_DOUBLE) {
                            double nan=NAN;
                            std::copy((char*)&nan, (char*)&nan + sizeof(nan), itemAddr);
                        } else {
                            SCIDB_UNREACHABLE(); // there is no such thing as NaN for other types.  The calling programmer made a serious error.
                        }
                    } else { // EXTRACT_NULL_AS_EXCEPTION
                        throw USER_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "NULL to non-nullable operator";
                    }
                }
                nExtracted += 1;
            }
        }
        return nExtracted;
    }

    boost::shared_ptr<ArrayIterator> Array::getIterator(AttributeID attr)
    {
        throw USER_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "Array::getIterator";
    }


    boost::shared_ptr<ConstItemIterator> Array::getItemIterator(AttributeID attrID, int iterationMode) const
    {
        return boost::shared_ptr<ConstItemIterator>(new ConstItemIterator(*this, attrID, iterationMode));
    }

    bool Array::isCountKnown() const
    {
        // if an array keeps track of the count in substantially less time than traversing
        // all chunks of an attribute, then that array should override this method
        // and return true, and override count() [below] with a faster method.
        return false;
    }

    size_t Array::count() const
    {
        // if there is a way to get the count in O(1) time, or without traversing
        // a set of chunks, then that should be done here, when that becomes possible

        if (getSupportedAccess() == SINGLE_PASS)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_OPERATOR, SCIDB_LE_UNSUPPORTED_INPUT_ARRAY) << "findChunkElements";
        }

        //Heuristic: make an effort to scan the empty tag. If there is no empty tag - scan the smallest fixed-sized attribute.
        //If an attribute is small in size (bool or uint8), chances are it takes less disk scan time and/or less compute time to pass over it.
        ArrayDesc const& schema = getArrayDesc();
        AttributeDesc const* attributeToScan = schema.getEmptyBitmapAttribute();
        if (attributeToScan == NULL)
        {
            //The array doesn't have an empty tag. Let's pick the smallest fixed-size attribute.
            attributeToScan = &schema.getAttributes()[0];
            size_t scannedAttributeSize = attributeToScan->getSize();
            for(size_t i = 1, n = schema.getAttributes().size(); i < n; i++)
            {
                AttributeDesc const& candidate = schema.getAttributes()[i];
                size_t candidateSize = candidate.getSize();
                if ( candidateSize != 0 && (candidateSize < scannedAttributeSize || scannedAttributeSize == 0))
                {
                    attributeToScan = &candidate;
                    scannedAttributeSize = candidateSize;
                }
            }
        }
        assert(attributeToScan != NULL);
        AttributeID victimId = attributeToScan->getId();
        size_t result = 0;
        //Iterate over the target attribute, find nElements of each chunk, add such values to the result
        boost::shared_ptr<ConstArrayIterator> iter = getConstIterator(victimId);
        while( ! iter->end() )
        {
            ConstChunk const& curChunk = iter->getChunk();
            result += curChunk.count();
            ++(*iter);
        }
        return result;
    }


    void Array::printArrayToLogger() const
    {
        // This function is only usable in debug builds, otherwise it is a no-op
#ifndef NDEBUG
        size_t nattrs = this->getArrayDesc().getAttributes(true).size();

        vector< shared_ptr<ConstArrayIterator> > arrayIters(nattrs);
        vector< shared_ptr<ConstChunkIterator> > chunkIters(nattrs);
        vector<TypeId> attrTypes(nattrs);

        LOG4CXX_DEBUG(logger, "[printArray] name (" << this->getName() << ")");

        for (size_t i = 0; i < nattrs; i++)
        {
            arrayIters[i] = this->getConstIterator(i);
            attrTypes[i] = this->getArrayDesc().getAttributes(true)[i].getType();
        }

        while (!arrayIters[0]->end())
        {
            for (size_t i = 0; i < nattrs; i++)
            {
                chunkIters[i] = arrayIters[i]->getChunk().getConstIterator();
            }

            while (!chunkIters[0]->end())
            {
                vector<Value> item(nattrs);
                stringstream ssvalue;
                stringstream sspos;

                ssvalue << "( ";
                for (size_t i = 0; i < nattrs; i++)
                {
                    item[i] = chunkIters[i]->getItem();
                    ssvalue << ValueToString(attrTypes[i], item[i]) << " ";
                }
                ssvalue << ")";

                sspos << "( ";
                for (size_t i = 0; i < chunkIters[0]->getPosition().size(); i++)
                {
                    sspos << chunkIters[0]->getPosition()[i] << " ";
                }
                sspos << ")";

                LOG4CXX_DEBUG(logger, "[PrintArray] pos " << sspos.str() << " val " << ssvalue.str());

                for (size_t i = 0; i < nattrs; i++)
                {
                    ++(*chunkIters[i]);
                }
            }

            for (size_t i = 0; i < nattrs; i++)
            {
                ++(*arrayIters[i]);
            }
        }
#endif
    }

    bool ConstArrayIterator::setPosition(Coordinates const& pos)
    {
        ASSERT_EXCEPTION(false,"ConstArrayIterator::setPosition");
        return false;
    }

    void ConstArrayIterator::reset()
    {
        ASSERT_EXCEPTION(false,"ConstArrayIterator::reset");
    }

    void ArrayIterator::deleteChunk(Chunk& chunk)
    {
        assert(false);
    }
    Chunk& ArrayIterator::updateChunk()
    {
        ConstChunk const& constChunk = getChunk();
        if (constChunk.isReadOnly()) {
            throw USER_EXCEPTION(SCIDB_SE_MERGE, SCIDB_LE_CANT_UPDATE_READ_ONLY_CHUNK);
        }
        Chunk& chunk = const_cast<Chunk&>(dynamic_cast<const Chunk&>(constChunk));
        chunk.pin();
        return chunk;
    }

    Chunk& ArrayIterator::copyChunk(ConstChunk const& chunk, boost::shared_ptr<ConstRLEEmptyBitmap>& emptyBitmap)
    {
        const Coordinates& pos = chunk.getFirstPosition(false);
        Chunk& outChunk = newChunk(pos);

        //verify that the declared chunk intervals match. Otherwise the copy - could still work - but would either be an implicit reshape or outright dangerous
        SCIDB_ASSERT(chunk.getArrayDesc().getDimensions().size() == outChunk.getArrayDesc().getDimensions().size());
        for(size_t i = 0, n = chunk.getArrayDesc().getDimensions().size(); i < n; i++)
        {
            SCIDB_ASSERT(chunk.getArrayDesc().getDimensions()[i].getChunkInterval() == outChunk.getArrayDesc().getDimensions()[i].getChunkInterval());
        }

        try {
            boost::shared_ptr<Query> query(getQuery());

            // If copying from an emptyable array to an non-emptyable array, we need to fill in the default values.
            size_t nAttrsChunk = chunk.getArrayDesc().getAttributes().size();
            size_t nAttrsOutChunk = outChunk.getArrayDesc().getAttributes().size();
            assert(nAttrsChunk >= nAttrsOutChunk);
            assert(nAttrsOutChunk+1 >= nAttrsChunk);
            bool emptyableToNonEmptyable = (nAttrsOutChunk+1 == nAttrsChunk);
            if (chunk.isMaterialized()
                    && chunk.getArrayDesc().hasOverlap() == outChunk.getArrayDesc().hasOverlap()
                    && chunk.getAttributeDesc().isNullable() == outChunk.getAttributeDesc().isNullable()
                    // if emptyableToNonEmptyable, we cannot use memcpy because we need to insert defaultvalues
                    && !emptyableToNonEmptyable
                    && chunk.getNumberOfElements(true) == outChunk.getNumberOfElements(true)
                )
            {
                PinBuffer scope(chunk);
                if (emptyBitmap && chunk.getBitmapSize() == 0) {
                    size_t size = chunk.getSize() + emptyBitmap->packedSize();
                    outChunk.allocate(size);
                    memcpy(outChunk.getDataForLoad(), chunk.getData(), chunk.getSize());
                    emptyBitmap->pack((char*)outChunk.getData() + chunk.getSize());
                } else {
                    size_t size = emptyBitmap ? chunk.getSize() : chunk.getSize() - chunk.getBitmapSize();
                    outChunk.allocate(size);
                    memcpy(outChunk.getDataForLoad(), chunk.getData(), size);
                }
                outChunk.setCount(chunk.isCountKnown() ? chunk.count() : 0);
                outChunk.write(query);
            } else {
                if (emptyBitmap) {
                    chunk.makeClosure(outChunk, emptyBitmap);
                    outChunk.write(query);
                } else {
                    boost::shared_ptr<ConstChunkIterator> src = chunk.getConstIterator(
                            ChunkIterator::IGNORE_EMPTY_CELLS|ChunkIterator::INTENDED_TILE_MODE|(outChunk.getArrayDesc().hasOverlap() ? 0 : ChunkIterator::IGNORE_OVERLAPS));
                    boost::shared_ptr<ChunkIterator> dst =
                        outChunk.getIterator(query,
                                             (src->getMode() & ChunkIterator::TILE_MODE)|ChunkIterator::NO_EMPTY_CHECK|ChunkIterator::SEQUENTIAL_WRITE);
                    size_t count = 0;
                    while (!src->end()) {
                        if (!emptyableToNonEmptyable) {
                            count += 1;
                        }
                        dst->setPosition(src->getPosition());
                        dst->writeItem(src->getItem());
                        ++(*src);
                    }
                    if (!(src->getMode() & ChunkIterator::TILE_MODE) &&
                        !chunk.getArrayDesc().hasOverlap()) {
                        if (emptyableToNonEmptyable) {
                            count = outChunk.getNumberOfElements(false); // false = no overlap
                        }
                        outChunk.setCount(count);
                    }
                    dst->flush();
                }
            }
        } catch (...) {
            deleteChunk(outChunk);
            throw;
        }
        return outChunk;
    }

    int ConstItemIterator::getMode()
    {
        return iterationMode;
    }

     Value& ConstItemIterator::getItem()
    {
        return chunkIterator->getItem();
    }

    bool ConstItemIterator::isEmpty()
    {
        return chunkIterator->isEmpty();
    }

    ConstChunk const& ConstItemIterator::getChunk()
    {
        return chunkIterator->getChunk();
    }

    bool ConstItemIterator::end()
    {
        return !chunkIterator || chunkIterator->end();
    }

    void ConstItemIterator::operator ++()
    {
        ++(*chunkIterator);
        while (chunkIterator->end()) {
            chunkIterator.reset();
            ++(*arrayIterator);
            if (arrayIterator->end()) {
                return;
            }
            chunkIterator = arrayIterator->getChunk().getConstIterator(iterationMode);
        }
    }

    Coordinates const& ConstItemIterator::getPosition()
    {
        return chunkIterator->getPosition();
    }

    bool ConstItemIterator::setPosition(Coordinates const& pos)
    {
        if (!chunkIterator || !chunkIterator->setPosition(pos)) {
            chunkIterator.reset();
            if (arrayIterator->setPosition(pos)) {
                chunkIterator = arrayIterator->getChunk().getConstIterator(iterationMode);
                return chunkIterator->setPosition(pos);
            }
            return false;
        }
        return true;
    }

    void ConstItemIterator::reset()
    {
        chunkIterator.reset();
        arrayIterator->reset();
        if (!arrayIterator->end()) {
            chunkIterator = arrayIterator->getChunk().getConstIterator(iterationMode);
        }
    }

    ConstItemIterator::ConstItemIterator(Array const& array, AttributeID attrID, int mode)
    : arrayIterator(array.getConstIterator(attrID)),
      iterationMode(mode)
    {
        if (!arrayIterator->end()) {
            chunkIterator = arrayIterator->getChunk().getConstIterator(mode);
        }
    }


    static void dummyFunction(const Value** args, Value* res, void*) {}

    class UserDefinedRegistrator {
      public:
        UserDefinedRegistrator() {}

        void foo();
    };

    void UserDefinedRegistrator::foo()
    {
        REGISTER_FUNCTION(length, list_of(TID_STRING)(TID_STRING), TypeId(TID_INT64), dummyFunction);
        REGISTER_CONVERTER(string, char, TRUNCATE_CONVERSION_COST, dummyFunction);
        REGISTER_TYPE(decimal, 16);
    }

    UserDefinedRegistrator userDefinedRegistrator;

}
