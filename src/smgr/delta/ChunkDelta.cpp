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


#include "smgr/delta/ChunkDelta.h"
#include "array/MemArray.h"

#include <stdlib.h>

#include <boost/shared_ptr.hpp>
using boost::shared_ptr;

namespace scidb
{

TypeId getTypeIdForIntSize(int size)
{
    if (size == 0) {
        return TID_VOID;
    }
    stringstream ss;
    ss << "$int" << (size<<3);
    const TypeId& type = ss.str();
    TypeLibrary::registerType(Type(type, size<<3));
    return type;
}


InvalidDeltaException::InvalidDeltaException(int excNum) : _excNum(excNum)
{
        // No-op
}

const char* InvalidDeltaException::what() const throw()
{
        stringstream ss;
        ss << "Invalid delta data.  Error #" << _excNum;
        return ss.str().c_str();
}


const Value ChunkDelta::ValueDifference(const Value& v1, const Value& v2)
{
        if (v1.size() > 8 || v2.size() > 8) {
            throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_TRUNCATION) << max(v1.size(), v2.size()) << 8;
        }
     
        int64_t v1_val = 0, v2_val = 0;
        memcpy(&v1_val, v1.data(), v1.size());
        memcpy(&v2_val, v2.data(), v2.size());
        int64_t deltaVal = v1_val - v2_val;
        Value vOut(v1);
        vOut.setData(&deltaVal, v2.size());
        return vOut;
}

/// TODO: Get the semantics right for differentiating between null and zero values
MemChunk& ChunkDelta::subtractChunks(MemChunk& deltaChunk, const ConstChunk& srcChunk, const ConstChunk& targetChunk)
{
        // First, create the delta
        shared_ptr<ConstChunkIterator> src = srcChunk.getConstIterator();
        shared_ptr<ConstChunkIterator> trg = targetChunk.getConstIterator();

        Coordinates srcCoord, trgCoord;
        // Temporary storage for the sparse difference
        deltaChunk.initialize(targetChunk);
        //deltaChunk.setSparse(srcChunk.isSparse() && targetChunk.isSparse());
        shared_ptr<Query> emptyQuery;
        shared_ptr<ChunkIterator> deltaIter = deltaChunk.getIterator(emptyQuery,
                                                                     ChunkIterator::NO_EMPTY_CHECK);

        // Make sure we're differencing two arrays that are the same type
        AttributeDesc attrDesc = srcChunk.getAttributeDesc();
        assert(attrDesc.getType() == targetChunk.getAttributeDesc().getType());

        // Pre-generate a "zero", to make differencing simpler
        Type attrType = TypeLibrary::getType(attrDesc.getType());
        Value zeroValue(attrType);
        memset(zeroValue.data(), 0, zeroValue.size());

        // Difference the two arrays
        // Assume that the iterators iterate in the same order,
        // so that we can perform a simple merge diff.

        CoordinatesLess cl;

        while (!src->end() && !trg->end())
        {
                // The two chunks are treated as sparse arrays,
                // and combined using a merge operation.
                srcCoord = src->getPosition();
                trgCoord = trg->getPosition();

                if (cl(srcCoord, trgCoord)) {
                        // trg doesn't have a value at this location.
                        // Add the negation of src's value at this location to the delta
                        // (so that when added to src, the result is 0),
                        // and move src forward to catch up.
                        if (!deltaIter->setPosition(srcCoord))
                            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_OPERATION_FAILED) << "setPosition";
                        deltaIter->writeItem(ValueDifference(zeroValue, src->getItem()));
                        ++(*src);
                } else if (cl(trgCoord, srcCoord)) {
                        // src doesn't have a value at this location.
                        // Add trg's value at this location to the delta, and move trg forward to catch up.
                        if (!deltaIter->setPosition(trgCoord))
                            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_OPERATION_FAILED) << "setPosition";
                        deltaIter->writeItem(trg->getItem());
                        ++(*trg);
                } else {
                        // Both src and trg have a value here.
                        // Store the difference.
                        if (!deltaIter->setPosition(srcCoord))
                            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_OPERATION_FAILED) << "setPosition";
                        deltaIter->writeItem(ValueDifference(trg->getItem(), src->getItem()));
                        ++(*src);
                        ++(*trg);
                }
        }

        // Grab any remaining values (src may extend past trg, or vice versa)
        while (!src->end()) {
                if (!deltaIter->setPosition(src->getPosition()))
                    throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_OPERATION_FAILED) << "setPosition";
                deltaIter->writeItem(ValueDifference(zeroValue, src->getItem()));
                ++(*src);
        }

        while (!trg->end()) {
                if (!deltaIter->setPosition(trg->getPosition()))
                    throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_OPERATION_FAILED) << "setPosition";
                deltaIter->writeItem(trg->getItem());
                ++(*trg);
        }

        deltaIter->flush();

        return deltaChunk;
}

static inline int64_t signExtend(int64_t val, uint8_t numBytes)
{
        // Relies on the fact that the ">>" operator on signed integers
        // implements bit extension.
        // Also, on the assumption that subtraction and bit shifts are fast.
        numBytes = (8 - numBytes) << 3;
        return ((val << numBytes) >> numBytes);
}

/// TODO: Get the semantics right for differentiating between null and zero values
static void addChunks(MemChunk& outChunk, const ConstChunk& addend)
{
   shared_ptr<ConstChunkIterator> addendIter = addend.getConstIterator(ChunkIterator::NO_EMPTY_CHECK | ChunkIterator::APPEND_CHUNK | ChunkIterator::IGNORE_DEFAULT_VALUES);
   shared_ptr<Query> emptyQuery;
   shared_ptr<ChunkIterator> outIter = outChunk.getIterator(emptyQuery,
                                                            ChunkIterator::NO_EMPTY_CHECK | ChunkIterator::APPEND_CHUNK | ChunkIterator::IGNORE_DEFAULT_VALUES);
        Value val(TypeLibrary::getType(outChunk.getAttributeDesc().getType()));

        int64_t lhs, rhs, sum;
        if (addend.getAttributeDesc().getType() == outChunk.getAttributeDesc().getType()) {
                if (!addendIter->end()) {
                        do {
                                lhs = 0;
                                rhs = 0;
                                sum = 0;

                                if (!outIter->setPosition(addendIter->getPosition()))
                                    throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_OPERATION_FAILED) << "setPosition";

                                memcpy(&lhs, addendIter->getItem().data(), addendIter->getItem().size());
                                memcpy(&rhs, outIter->getItem().data(), outIter->getItem().size());

                                sum = lhs + rhs;

                                memcpy(val.data(), &sum, min(val.size(), sizeof(sum)));

                                outIter->writeItem(val);
                                ++(*addendIter);
                        } while (!addendIter->end());
                }
        } else {
                // We're using signed integers and "class Value" does not do sign extension properly
                // (This code will work in the above case as well, but it's slower)

                const uint8_t addendBitDepth = TypeLibrary::getType(addend.getAttributeDesc().getType()).byteSize();
                const uint8_t outBitDepth = TypeLibrary::getType(outChunk.getAttributeDesc().getType()).byteSize();

                if (!addendIter->end()) {
                        do {
                                lhs = 0;
                                rhs = 0;
                                sum = 0;

                                if (!outIter->setPosition(addendIter->getPosition()))
                                    throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_OPERATION_FAILED) << "setPosition";

                                memcpy(&lhs, addendIter->getItem().data(), addendIter->getItem().size());
                                memcpy(&rhs, outIter->getItem().data(), outIter->getItem().size());

                                sum = signExtend(lhs, addendBitDepth) + signExtend(rhs, outBitDepth);

                                memcpy(val.data(), &sum, min(val.size(), sizeof(sum)));

                                outIter->writeItem(val);
                                ++(*addendIter);
                        } while (!addendIter->end());
                }
        }

        outIter->flush();
}

static uint64_t findOptimalBitDepth(const ConstChunk& chunk) {
        // First, gather some statistics.  Then calculate the optimal bit depth.
        const unsigned int MAX_BYTES = 8;
        vector<uint64_t> counts(MAX_BYTES, 0);
        int64_t tmpVal;
        uint64_t totalElts = 0;

        shared_ptr<ConstChunkIterator> deltaIter = chunk.getConstIterator();
        if (!deltaIter->end()) {
                int64_t threshold;
                do
                {
                        totalElts++;

                        // Skip empty cells; they take zero space, they'll fit anywhere
                        if (deltaIter->isEmpty()) continue;

                        // For each entry in this array, test all possible cell bit-depths
                        // (ie., int8_t, int16_t, int32_t, int64_t),
                        // and increment a counter for each one that is too small
                        // to store the given value.
                        // This will later be used to determine a cutoff for "unusually large" values.
                        tmpVal = deltaIter->getItem().getInt64();
                        if (tmpVal != 0) {
                                counts[0]++;  // Special-case 0; 0 really means "this value isn't stored at all"
                        }
                        for (unsigned int i = 1; i < MAX_BYTES; i++) {
                                threshold = ((int64_t)1 << (i*8 - 1));
                                if (tmpVal >= threshold || -tmpVal > threshold) counts[i]++;
                        }
                        ++(*deltaIter);
                }
                while (!deltaIter->end());
        }

        // Cost for a dense array at this bit depth is i * totalElts.
        // Cost for a sparse array with this many elements (assuming coordinate form) is
        //      counts[i]*(deltaRank*sizeof(Coordinate) + deltaTypeSize).
        // Find the minimum size for i=0..MAX_BYTES

        const uint64_t deltaRank = chunk.getArrayDesc().getDimensions().size();
        const uint64_t deltaTypeSize = (TypeLibrary::getType(chunk.getAttributeDesc().getType())).byteSize();


        uint64_t minSoFar = 0xffffffffffffffffULL;
        uint64_t minIndex = -1;
        uint64_t currMin;
        for (unsigned int i = 0; i < MAX_BYTES; i++) {
                currMin = (i * totalElts) + counts[i]*(deltaRank*sizeof(Coordinate) + deltaTypeSize);
                if (currMin <= minSoFar) {
                        minSoFar = currMin;
                        minIndex = i;
                }
        }

        return minIndex;
}

static void splitDelta(MemChunk& sparseData, MemChunk& denseData, const ConstChunk& deltaChunk, uint64_t bitDepth)
{
   shared_ptr<Query> emptyQuery;
   shared_ptr<ChunkIterator> sparseIter = sparseData.getIterator(emptyQuery, ChunkIterator::NO_EMPTY_CHECK);
   shared_ptr<ChunkIterator> denseIter = denseData.getIterator(emptyQuery, ChunkIterator::NO_EMPTY_CHECK);

        shared_ptr<ConstChunkIterator> deltaIter = deltaChunk.getConstIterator();

        int64_t cutoffThreshold = (int64_t)1 << (8*bitDepth - 1);

        Coordinates coords;

        Value val(TypeLibrary::getType(deltaChunk.getAttributeDesc().getType()));
        Value denseVal(TypeLibrary::getType(denseData.getAttributeDesc().getType()));
        if (!deltaIter->end()) {
                do
                {
                        // For each value in deltaChunk,
                        // if it is larger than the specified threshold, store it in the sparse array;
                        // if it is smaller, store it in the dense array.
                        // The assumption is that the threshold is the maximum size of data that can fit into
                        // the dense array; but that the sparse array allocates more space per value and so
                        // can fit larger values.

                        val = deltaIter->getItem();

                        coords = deltaIter->getPosition();

                        if (abs(val.getInt64()) >= cutoffThreshold ) {
                            if (!sparseIter->setPosition(deltaIter->getPosition()))
                                throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_OPERATION_FAILED) << "setPosition";
                            sparseIter->writeItem(val);
                        } else {
                            if (!denseIter->setPosition(deltaIter->getPosition()))
                                throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_OPERATION_FAILED) << "setPosition";
                            denseVal.setData(val.data(), denseVal.size());
                            denseIter->writeItem(denseVal);
                        }
                        ++(*deltaIter);
                } while (!deltaIter->end());
        }

        denseIter->flush();
        sparseIter->flush();
}

void ChunkDelta::writeDeltaData(const uint8_t bitDepth, const MemChunk& sparseData, const MemChunk& denseData)
{
        // Build a header
        SubtractionDeltaHeader header;
        header.sparseDataLength = sparseData.isInitialized() ? sparseData.getSize() : 0;
        header.denseDataLength = (denseData.isInitialized() && bitDepth > 0) ? denseData.getSize() : 0;
        header.denseBitDepth = bitDepth;

        // Allocate memory; play pointer games to get `deltaData` and `type` set properly
        fromDeltaBufSize = sizeof(type) + sizeof(header) + header.sparseDataLength + header.denseDataLength;
        fromDeltaBuffer = malloc(fromDeltaBufSize);

        deltaData = (uint8_t*)fromDeltaBuffer + sizeof(type);
        deltaBufSize = fromDeltaBufSize - sizeof(type);

        ((uint8_t*)fromDeltaBuffer)[0] = SUBTRACTIVE;

        // Write the header to the buffer
        memcpy(deltaData, &header, sizeof(header));

        // Write the sparse-array data, if it exists
        if (header.sparseDataLength > 0) {
                memcpy((uint8_t*)deltaData + sizeof(header), sparseData.getData(), header.sparseDataLength);
        }

        // Write the dense-array data, if it exists
        if (bitDepth > 0) {
                memcpy((uint8_t*)deltaData + sizeof(header) + header.sparseDataLength, denseData.getData(), header.denseDataLength);
        }
}

void ChunkDelta::createDelta_Subtractive(const ConstChunk& srcChunk, const ConstChunk& targetChunk)
{
        MemChunk deltaChunk;
        subtractChunks(deltaChunk, srcChunk, targetChunk);

        // Now, we have a delta.
        // If it's a sparse delta, just use it as-is.
        // If not, split it into components.

        if (!deltaChunk.isSparse()) {
                // Calculate the delta threshold, the number of bytes of dense data to store
                // (any value that requires more bytes than this must be stored in a separate sparse array)
                uint64_t bitDepth = findOptimalBitDepth(deltaChunk);

                // Create and initialize the sparse and dense array-pair that this data will be split into
                MemChunk sparseData;
                MemChunk denseData;

                sparseData.initialize(deltaChunk);
                sparseData.setSparse(true);

                // Hack time!
                // In order to specify a custom Type for a chunk,
                // I have to construct fake array metadata for it.

                AttributeDesc deltaChunkAD = srcChunk.getAttributeDesc();
                AttributeDesc attrDesc(deltaChunkAD.getId(),
                                                           deltaChunkAD.getName(),
                               getTypeIdForIntSize(bitDepth),   // Custom type; otherwise same as the delta chunk
                                                           deltaChunkAD.getFlags(),
                                                           deltaChunkAD.getDefaultCompressionMethod(),
                                                           deltaChunkAD.getAliases(),
                                                           deltaChunkAD.getReserve());
                Attributes attrs(1, attrDesc);
                ArrayDesc denseDesc("Temporary Array", attrs, srcChunk.getArrayDesc().getDimensions(), srcChunk.getArrayDesc().getFlags());
                Address address(0, deltaChunk.getAddress().coords);
                denseData.initialize(&deltaChunk.getArray(), 
                                     &denseDesc,
                                     address,
                                     deltaChunk.getCompressionMethod());
                denseData.setSparse(false);

                // Split the delta data into dense and sparse parts
                splitDelta(sparseData, denseData, deltaChunk, bitDepth);

                // Allocate a buffer and write these deltas to it
                writeDeltaData(bitDepth, sparseData, denseData);
        }
        else
        {
                // Fake dense data
                MemChunk denseData;

                // No need to split into dense and sparse.
                // We already have sparse; just store the difference.
                writeDeltaData(0, deltaChunk, denseData);
        }

        type = SUBTRACTIVE;
        ((uint8_t*)fromDeltaBuffer)[0] = type;
}

void ChunkDelta::createDelta_BSDiff(const ConstChunk& srcChunk, const ConstChunk& targetChunk)
{
        // Allocate some space to put the delta into, and for our "type" header field.
        // The delta can be arbitrarily large in principle (BSDiff
        // doesn't necessarily produce small deltas, just highly
        // compressible ones), so just make up a big size to allocate.
        fromDeltaBufSize = (srcChunk.getSize() + targetChunk.getSize() + 1000) * 2 + sizeof(type);
        fromDeltaBuffer = malloc(fromDeltaBufSize);

        // We're playing some slightly-silly pointer games here:
        // BSDiff wants its own buffer, but we want to prepend a "type" field.
        // So, update BSDiff's buffer pointers to point to after "type".
        deltaData = (uint8_t*)fromDeltaBuffer + sizeof(type);
        deltaBufSize = fromDeltaBufSize - sizeof(type);

        type = BSDIFF;
        ((uint8_t*)fromDeltaBuffer)[0] = type;

        // If the patch would be even bigger than the allocated space
        // (or if BSDiff fails to create a patch for any other reason),
        // mark this as an invalid delta so that we don't use it.
        int ret;
        if ((ret = bsdiff_nocompress((u_char*)srcChunk.getData(), srcChunk.getSize(),
                        (u_char*)targetChunk.getData(), targetChunk.getSize(),
                        (u_char*)deltaData, deltaBufSize, &deltaBufSize)) != 0) {
                validDelta = false;
        }
}

static MemChunk& cloneChunk(MemChunk& outChunk, const ConstChunk& srcChunk, const size_t size)
{
        // Initialize outChunk as a copy of srcChunk
        outChunk.allocate(size);
        memcpy(outChunk.getData(), srcChunk.getData(), outChunk.getSize());

        outChunk.initialize(srcChunk);

        return outChunk;
}


void ChunkDelta::applyDeltas_Subtractive(const ConstChunk& srcChunk, SharedBuffer& out)
{
        MemChunk outChunk;
        cloneChunk(outChunk, srcChunk, out.getSize());

        // Apply all of the deltas that we know about, in order
        // The goal here is to avoid copying outChunk O(delta-chain length) times.
        // In applications with small updates, deltas can be very small and
        // very numerous, so we want to go through and apply them by
        // mutating outChunk in place.
        for (vector<ChunkDelta*>::reverse_iterator patchIter = deltasToApply.rbegin();
                        patchIter != deltasToApply.rend(); patchIter++)
        {
                // All deltas in here must be valid; otherwise we can't use them.
                // Complain loudly if this isn't the case.
                if (!(*patchIter)->isValidDelta()) {
                        throw new InvalidDeltaException(1337);
                }

                SubtractionDeltaHeader header;
                memcpy(&header, (*patchIter)->deltaData, sizeof(header));

                MemChunk sparsePart;

                // If we have sparse data in this delta, go add it to outChunk
                if (header.sparseDataLength > 0) {
                        MemChunk sparsePart;

                        sparsePart.allocate(header.sparseDataLength);
                        memcpy(sparsePart.getData(), (uint8_t*)(*patchIter)->deltaData + sizeof(header), header.sparseDataLength);

                        sparsePart.initialize(srcChunk);
                        sparsePart.setSparse(true);

                        addChunks(outChunk, sparsePart);
                }

                // If we have dense data in this delta, go add it to outChunk
                if (header.denseDataLength > 0) {
                        MemChunk densePart;

                        densePart.allocate(header.denseDataLength);
                        memcpy(densePart.getData(), (uint8_t*)(*patchIter)->deltaData + sizeof(header) + header.sparseDataLength, header.denseDataLength);

                        // The dense array is intentionally at a funky/reduced bit depth, to save space
                        // Hack time!
                        // In order to specify a custom Type for a chunk,
                        // I have to construct fake array metadata for it.
                        AttributeDesc deltaChunkAD = srcChunk.getAttributeDesc();
                        AttributeDesc attrDesc(deltaChunkAD.getId(),
                                                                   deltaChunkAD.getName(),
                                   getTypeIdForIntSize(header.denseBitDepth),   // Custom type; otherwise same as the delta chunk
                                                                   deltaChunkAD.getFlags(),
                                                                   deltaChunkAD.getDefaultCompressionMethod(),
                                                                   deltaChunkAD.getAliases(),
                                                                   deltaChunkAD.getReserve());
                        Attributes attrs(1, attrDesc);
                        ArrayDesc arrDesc("Temporary Array", attrs, srcChunk.getArrayDesc().getDimensions(), srcChunk.getArrayDesc().getFlags());
                        Address tmpAddr(0, srcChunk.getFirstPosition(false));
                        densePart.initialize(&srcChunk.getArray(),
                                             &arrDesc,
                                             tmpAddr,
                                             srcChunk.getCompressionMethod());
                        densePart.setSparse(false);                        
                        addChunks(outChunk, densePart);
                }
        }

        // Now, we have a nice chunk with the data that we want.
        // Copy that data into the output buffer.
        memcpy(out.getData(), outChunk.getData(), out.getSize());
}

void ChunkDelta::applyDeltas_BSDiff(const ConstChunk& srcChunk, SharedBuffer& out)
{
        int ret;
        const void *initialData = srcChunk.getData();  // Keeping this around for later comparisons
        void *currData = (void*)initialData;
        off_t currDataSize = srcChunk.getSize();
        void *newData;
        off_t newDataSize;
        for (vector<ChunkDelta*>::reverse_iterator patchIter = deltasToApply.rbegin();
                        patchIter != deltasToApply.rend(); patchIter++)
        {

                // Shouldn't be strictly necessary, but, good defensive programming
                newData = NULL;
                newDataSize = 0;

                // Go through and apply each patch, one after the other
                // PERFORMANCE: Don't bother trying to not copy the data each time;
                // bsdiff isn't smart enough to help us here anyway.
                if ((ret = bspatch_nocompress((u_char*)currData, currDataSize,
                                        (u_char*)(*patchIter)->deltaData, (*patchIter)->deltaBufSize,
                                        (u_char**)&newData, &newDataSize)) != 0)
                {
                        throw InvalidDeltaException(-ret);
                }

                if (currData != initialData) {
                        // bspatch_nocompress() gives us newly-allocated buffers.
                        // Don't free our original buffer!, but free all other
                        // intermediate buffers.
                        free(currData);
                }

                currData = newData;
                currDataSize = newDataSize;
        }

        // Copy the new data into the output buffer.
        // It would be nice to avoid this copy, but doing so would
        // involve changing the SharedBuffer interface.
        out.allocate(currDataSize);
        memcpy(out.getData(), currData, currDataSize);
        free(currData);
}


ChunkDelta::ChunkDelta(void * _fromDeltaBuffer, off_t _bufSize)
        : fromDeltaBuffer(_fromDeltaBuffer), fromDeltaBufSize(_bufSize),
          needToFreeBuffer(false), validDelta(true)
{
        deltaData = (uint8_t*)fromDeltaBuffer + 1;
        deltaBufSize = fromDeltaBufSize - 1;
        type = ((uint8_t*)fromDeltaBuffer)[0];

        deltasToApply.push_back(this);
}

ChunkDelta::ChunkDelta(const ConstChunk& srcChunk, const ConstChunk& targetChunk)
        : fromDeltaBuffer(NULL), needToFreeBuffer(true), validDelta(true)
{
        // Produces a delta, then forgets the src and target chunk.
        // Allocates memory internally to store the delta.

        uint64_t srcChunkBitDepth = TypeLibrary::getType(srcChunk.getAttributeDesc().getType()).bitSize();
        uint64_t targetChunkBitDepth = TypeLibrary::getType(targetChunk.getAttributeDesc().getType()).bitSize();

        if (!srcChunk.isRLE() && !targetChunk.isRLE() && srcChunkBitDepth == targetChunkBitDepth &&
            (srcChunkBitDepth == 8 || srcChunkBitDepth == 16 || srcChunkBitDepth == 32 || srcChunkBitDepth == 64)) // If delta's
        {
                createDelta_Subtractive(srcChunk, targetChunk);
        } else {
                createDelta_BSDiff(srcChunk, targetChunk);
        }

        deltasToApply.push_back(this);
}

ChunkDelta::~ChunkDelta()
{
        if (needToFreeBuffer) {
                free(fromDeltaBuffer);
        }
}

bool ChunkDelta::isValidDelta()
{
        return validDelta;
}

bool ChunkDelta::applyDelta(const ConstChunk& srcChunk, SharedBuffer& out)
{
        switch (type)
        {
        case BSDIFF:
                applyDeltas_BSDiff(srcChunk, out);
                return true;
        case SUBTRACTIVE:
                applyDeltas_Subtractive(srcChunk, out);
                return true;
        default:
                 throw InvalidDeltaException(type);
        }
}

void ChunkDelta::pushDelta(ChunkDelta& d)
{
        assert(d.type == type);  // Don't support chained deltas of different types yet
        deltasToApply.push_back(&d);
}


void* ChunkDelta::getData()
{
        return fromDeltaBuffer;
}

size_t ChunkDelta::getSize()
{
        return fromDeltaBufSize;
}

}

