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
#include "FITSInputArray.h"

#include <log4cxx/logger.h>
#include <log4cxx/basicconfigurator.h>
#include <log4cxx/helpers/exception.h>

/*
 * @author miguel@spacebase.org
 *
 * Supported features:
 *
 * - Only FITS multidimensional arrays (ie. images) are supported. Therefore,
 *   ASCII tables and Binary tables are (currently) not supported.
 *
 * - The following data types are supported: int16, int32, float32.
 *   (It should be trivial to add support for other data types so just ping me!)
 *
 * Workflow:
 *
 * - We spawn an iterator per attribute as needed. Then, as each iterator
 *   requests its next "chunk", we go and read the relevant part of the
 *   file, doing all necessary conversions and building up a MemChunk.
 *
 * - A set of most recent 'kWindowSize' MemChunks are kept in memory. At
 *   least 2 chunks are always kept in memory, since the iterator for
 *   Attribute1 may be requesting chunk N while iterator for Attribute2 may
 *   still be reading chunk N-1.
 *
 * - Since FITS files uses Fortran order for arrays - and not row-major order
 *   like SciDB - this code will never be very fast :-( For now I simply jump
 *   around the file as needed to build the current SciDB chunk. Smarter
 *   tricks are possible... maybe... I think. In any case, FITS files are also
 *   big-endian so there's plenty of conversions happening there anyway...
 */

namespace scidb
{
using namespace std;

static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.ops.impl_fits_input"));

/* FITSInputArray */

FITSInputArray::FITSInputArray(ArrayDesc const& array, string const& filePath, uint32_t hdu, boost::shared_ptr<Query>& query)
    : parser(filePath),
      hdu(hdu),
      desc(array),
      dims(array.getDimensions()),
      nDims(dims.size()),
      nAttrs(array.getAttributes(true).size()),
      values(nAttrs),
      chunks(nAttrs),
      chunkIterators(nAttrs),
      chunkIndex(0),
      chunkPos(nDims),
      query(query)
{
    initValueHolders();

    // Most initialization steps are only done later, when the first
    // chunk is requested by an iterator. See getChunkByIndex()
}

/**
 * Initialize value holders.
 *
 * NOTE: For FITS arrays there's always a single attribute, so the fact
 * that values is a vector is useless. Nonetheless, I left it as a vector
 * since the FITS tables do include multiple attributes and they might be
 * supported by this operator in the future.
 */
void FITSInputArray::initValueHolders()
{
    Attributes const& attrs = desc.getAttributes();
    for (size_t i = 0; i < nAttrs; i++) {
        values[i] = Value(TypeLibrary::getType(attrs[i].getType()));
    }
}

ArrayDesc const& FITSInputArray::getArrayDesc() const
{
    return desc;
}

boost::shared_ptr<ConstArrayIterator> FITSInputArray::getConstIterator(AttributeID attr) const
{
    return boost::shared_ptr<ConstArrayIterator>(new FITSInputArrayIterator(*(FITSInputArray*) this, attr));
}

ConstChunk* FITSInputArray::getChunkByIndex(size_t index, AttributeID attr)
{
    if (chunkIndex > 0) {
        while (index > chunkIndex) {    // Keep reading until we reach desired chunk
            if (!advanceChunkPos()) {   // Finished reading entire array
                return NULL;
            }
            readChunk();
        }
    } else {                            // First time a chunk is read
        string error;
        if (!parser.moveToHDU(hdu, error)) {
            LOG4CXX_ERROR(logger, error);
            throw USER_EXCEPTION(SCIDB_SE_IMPORT_ERROR, SCIDB_LE_OP_INPUT_ERROR10);
        }

        if (!validSchema()) {
            LOG4CXX_ERROR(logger, "Array schema does not match schema in file");
            throw USER_EXCEPTION(SCIDB_SE_IMPORT_ERROR, SCIDB_LE_OP_INPUT_ERROR10);
        }
        if (!validDimensions()) {
            LOG4CXX_ERROR(logger, "Array dimensions do not match dimensions in file");
            throw USER_EXCEPTION(SCIDB_SE_IMPORT_ERROR, SCIDB_LE_OP_INPUT_ERROR10);
        }

        calculateLength();
        initChunkPos();
        readChunk();
    }
    // If within window, return chunk
    if (index < kWindowSize || (index > (chunkIndex - kWindowSize) && index <= chunkIndex)) {
         return &chunks[attr].chunks[index % kWindowSize];
    }

    // Otherwise, reset and start over
    chunkIndex = 0;
    return getChunkByIndex(index, attr);
}

bool FITSInputArray::validSchema()
{
    if (nAttrs != 1) {
        return false;
    }

    Attributes const& attrs = desc.getAttributes();
    const TypeId attrType = attrs[0].getType();

    switch (parser.getBitPixType()) {
    case FITSParser::INT16:
        if (attrType != TID_INT16) {
            return false;
        }
        break;
    case FITSParser::INT32:
        if (attrType != TID_INT32) {
            return false;
        }
        break;
    case FITSParser::INT16_SCALED:
    case FITSParser::INT32_SCALED:
    case FITSParser::FLOAT32_SCALED:
        if (attrType != TID_FLOAT) {
            return false;
        }
        break;
    default:
        assert(false);

    }
    return true;
}

bool FITSInputArray::validDimensions()
{
    vector<int> const& axisSizes = parser.getAxisSizes();

    for (size_t i = 0; i < nDims; i++) {
        if (static_cast<uint64_t>(axisSizes[i]) != dims[i].getLength()) {
            return false;
        }
    }
    return true;
}

/**
 * Initialize chunk position.
 */
void FITSInputArray::initChunkPos()
{
    chunkIndex = 1;

    for (size_t i = 0; i < nDims; i++) {
        chunkPos[i] = dims[i].getStartMin();
    }
}

/**
 * Advance chunk position to the start of the next chunk.
 *
 * @return False if out-of-bounds while advancing.
 */
bool FITSInputArray::advanceChunkPos()
{
    ++chunkIndex;

    for (size_t i = nDims - 1; ; i--) {
        chunkPos[i] += dims[i].getChunkInterval();
        if (chunkPos[i] <= dims[i].getEndMax()) {
            return true;
        } else {
            chunkPos[i] = dims[i].getStartMin();
            if (i == 0) {
                return false;
            }
        }
    }
    assert(false);
}

void FITSInputArray::calculateLength()
{
    // Number of elements to read consecutively (ie. size of the inner dimension)
    nConsecutive = dims[nDims - 1].getChunkInterval();

    // Number of elements in all but inner dimension
    nOuter = 1;
    for (size_t i = 0; i < (nDims - 1); i++) {
        nOuter *= dims[i].getChunkInterval();
    }
}

/**
 * Read next chunk from the file. It jumps around the file as needed, to cope
 * with Fortran array order.
 */
void FITSInputArray::readChunk()
{
    boost::shared_ptr<Query> queryLock(Query::getValidQueryPtr(query));

    initMemChunks(queryLock);

    Coordinates pos(nDims); // Coordinates within a chunk, starting at (0, ..., 0)

    for (size_t i = 0; i < nOuter; i++) {

        // Calculate cell number from 'pos' and move to it
        int cell = 0;
        int k = 1;
        for (int j = nDims - 1; j >= 0; j--) {
            cell += k * (pos[j] + chunkPos[j] - dims[j].getStartMin());
            k *= dims[j].getLength();
        }
        parser.moveToCell(cell);

        // Read consecutive values
        switch (parser.getBitPixType()) {
            case FITSParser::INT16:
                readShortInts(nConsecutive);
                break;
            case FITSParser::INT16_SCALED:
                readShortIntsAndScale(nConsecutive);
                break;
            case FITSParser::INT32:
                readInts(nConsecutive);
                break;
            case FITSParser::INT32_SCALED:
                readIntsAndScale(nConsecutive);
                break;
            case FITSParser::FLOAT32_SCALED:
                readFloats(nConsecutive);
                break;
            default:
                LOG4CXX_ERROR(logger, "Unsupported BITPIX value");
                throw USER_EXCEPTION(SCIDB_SE_IMPORT_ERROR, SCIDB_LE_OP_INPUT_ERROR10);
        }

        // Advance position in chunk
        int j;
        for (j = nDims - 2; j >= 0; j--) {
            ++pos[j];
            if (pos[j] < dims[j].getChunkInterval()) {
                break;
            } else {
                pos[j] = 0;
            }
        }
    }

    flushMemChunks();
}

/**
 * Initialize chunks and iterators.
 */
void FITSInputArray::initMemChunks(boost::shared_ptr<Query>& query)
{
    for (size_t i = 0; i < nAttrs; i++) {
        Address addr(i, chunkPos);

        MemChunk& chunk = chunks[i].chunks[chunkIndex % kWindowSize];
        chunk.initialize(this, &desc,
                         addr, desc.getAttributes()[i].getDefaultCompressionMethod());
        chunkIterators[i] =
            chunk.getIterator(query,
                              ChunkIterator::NO_EMPTY_CHECK |
                              ChunkIterator::IGNORE_OVERLAPS);
    }
}

/**
 * Flush iterators.
 */
void FITSInputArray::flushMemChunks()
{
    for (size_t i = 0; i < nAttrs; i++) {
        chunkIterators[i]->flush();
    }
}

void FITSInputArray::readShortInts(size_t n)
{
    for (size_t i = 0; i < n; i++) {
        values[0].setInt16(parser.readInt16());
        chunkIterators[0]->writeItem(values[0]);
        ++(*chunkIterators[0]);
    }
}

void FITSInputArray::readShortIntsAndScale(size_t n)
{
    for (size_t j = 0; j < n; j++) {
        values[0].setFloat( parser.getBZero() + parser.getBScale() * parser.readInt16() );
        chunkIterators[0]->writeItem(values[0]);
        ++(*chunkIterators[0]);
    }
}

void FITSInputArray::readInts(size_t n)
{
    for (size_t i = 0; i < n; i++) {
        values[0].setInt32(parser.readInt32());
        chunkIterators[0]->writeItem(values[0]);
        ++(*chunkIterators[0]);
    }
}

void FITSInputArray::readIntsAndScale(size_t n)
{
    for (size_t i = 0; i < n; i++) {
        values[0].setFloat( parser.getBZero() + parser.getBScale() * parser.readInt32() );
        chunkIterators[0]->writeItem(values[0]);
        ++(*chunkIterators[0]);
    }
}

void FITSInputArray::readFloats(size_t n)
{
    for (size_t i = 0; i < n; i++) {
        values[0].setFloat( parser.getBZero() + parser.getBScale() * parser.readFloat32() );
        chunkIterators[0]->writeItem(values[0]);
        ++(*chunkIterators[0]);
    }
}

/* FITSInputArrayIterator */

FITSInputArrayIterator::FITSInputArrayIterator(FITSInputArray& array, AttributeID attr)
    : array(array),
      attr(attr),
      chunk(NULL),
      chunkIndex(1),
      chunkRead(false)
{
}

bool FITSInputArrayIterator::end()
{
    if (!chunkRead) {
        chunk = array.getChunkByIndex(chunkIndex, attr);
        chunkRead = true;
    }
    return (chunk == NULL);
}

void FITSInputArrayIterator::operator ++()
{
    if (end()) {
        throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_NO_CURRENT_ELEMENT);
    }
    chunkIndex++;
    chunkRead = false;
}

Coordinates const& FITSInputArrayIterator::getPosition()
{
    if (end()) {
        throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_NO_CURRENT_ELEMENT);
    }
    return chunk->getFirstPosition(false);
}

bool FITSInputArrayIterator::setPosition(Coordinates const& pos)
{
    throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_NOT_IMPLEMENTED) << "setPosition";
}

void FITSInputArrayIterator::reset()
{
    chunk = NULL;
    chunkIndex = 1;
    chunkRead = false;
}

ConstChunk const& FITSInputArrayIterator::getChunk()
{
    if (end()) {
        throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_NO_CURRENT_ELEMENT);
    }
    return (*chunk);
}

}
