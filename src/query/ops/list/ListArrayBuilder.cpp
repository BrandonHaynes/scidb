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
 * ListArrayBuilder.cpp
 *
 *  Created on: May 25, 2012
 *      Author: poliocough@gmail.com
 */

#include "ListArrayBuilder.h"

using namespace boost;

namespace scidb
{

template <typename T>
Dimensions ListArrayBuilder<T>::getDimensions(shared_ptr<Query> const& query) const
{
    shared_ptr<const InstanceLiveness> queryLiveness(query->getCoordinatorLiveness());
    size_t numInstances = queryLiveness->getNumInstances();

    Dimensions dims(LIST_NUM_DIMS);
    dims[0] = DimensionDesc("inst", 0, 0, numInstances-1, numInstances-1, 1, 0);
    dims[1] = DimensionDesc("n", 0, 0, MAX_COORDINATE, MAX_COORDINATE, LIST_CHUNK_SIZE, 0);
    return dims;
}

template <typename T>
ArrayDesc ListArrayBuilder<T>::getSchema(shared_ptr<Query> const& query) const
{
    return ArrayDesc("list", getAttributes(), getDimensions(query));
}

template <typename T>
void ListArrayBuilder<T>::initialize(shared_ptr<Query> const& query)
{
    _query = query;
    ArrayDesc schema = getSchema(_query);
    _nAttrs = schema.getAttributes().size() - 1;
    _array = shared_ptr<MemArray>(new MemArray(schema, query));
    Coordinate myInstance = query->getInstanceID();
    _currPos = Coordinates(LIST_NUM_DIMS,0);
    _currPos[0] = myInstance;
    _outAIters.reserve(_nAttrs);
    for(size_t i =0; i<_nAttrs; ++i)
    {
        _outAIters.push_back(_array->getIterator(i));
    }
    _outCIters.reserve(_nAttrs);
    for(AttributeID i=0; i<_nAttrs; ++i)
    {
        Chunk& ch = _outAIters[i]->newChunk(_currPos);
        _outCIters.push_back(ch.getIterator(_query, i == 0 ? ChunkIterator::SEQUENTIAL_WRITE :
                                                             ChunkIterator::SEQUENTIAL_WRITE | ChunkIterator::NO_EMPTY_CHECK));
    }
    _nextChunkPos = _currPos;
    _nextChunkPos[1] += LIST_CHUNK_SIZE;
    _initialized = true;
}

template <typename T>
void ListArrayBuilder<T>::listElement(T const& element)
{
    assert(_initialized);
    if(_currPos[1]==_nextChunkPos[1])
    {
        for(AttributeID i=0; i<_nAttrs; ++i)
        {
            _outCIters[i]->flush();
            Chunk& ch = _outAIters[i]->newChunk(_currPos);
            _outCIters[i] = ch.getIterator(_query,i == 0 ? ChunkIterator::SEQUENTIAL_WRITE :
                                                           ChunkIterator::SEQUENTIAL_WRITE | ChunkIterator::NO_EMPTY_CHECK);
        }
        _nextChunkPos[1] += LIST_CHUNK_SIZE;
    }
    for(AttributeID i=0; i<_nAttrs; ++i)
    {
        _outCIters[i]->setPosition(_currPos);
    }
    addToArray(element);
    ++_currPos[1];
}

template <typename T>
shared_ptr<MemArray> ListArrayBuilder<T>::getArray()
{
    assert(_initialized);
    for(AttributeID i=0; i<_nAttrs; ++i)
    {
        _outCIters[i]->flush();
    }
    return _array;
}

Attributes ListChunkDescriptorsArrayBuilder::getAttributes() const
{
    Attributes attrs(NUM_ATTRIBUTES);
    attrs[STORAGE_VERSION]    = AttributeDesc(STORAGE_VERSION,   "svrsn",           TID_UINT32, 0, 0);
    attrs[INSTANCE_ID]        = AttributeDesc(INSTANCE_ID,       "insn",            TID_UINT32, 0, 0);
    attrs[DATASTORE_GUID]     = AttributeDesc(DATASTORE_GUID,    "dguid",           TID_UINT32, 0, 0);
    attrs[DISK_HEADER_POS]    = AttributeDesc(DISK_HEADER_POS,   "dhdrp",           TID_UINT64, 0, 0);
    attrs[DISK_OFFSET]        = AttributeDesc(DISK_OFFSET,       "doffs",           TID_UINT64, 0, 0);
    attrs[V_ARRAY_ID]         = AttributeDesc(V_ARRAY_ID,        "arrid",           TID_UINT64, 0, 0);
    attrs[ATTRIBUTE_ID]       = AttributeDesc(ATTRIBUTE_ID,      "attid",           TID_UINT64, 0, 0);
    attrs[COORDINATES]        = AttributeDesc(COORDINATES,       "coord",           TID_STRING, 0, 0);
    attrs[COMPRESSION]        = AttributeDesc(COMPRESSION,       "comp",            TID_INT8,   0, 0);
    attrs[FLAGS]              = AttributeDesc(FLAGS,             "flags",           TID_UINT8,  0, 0);
    attrs[NUM_ELEMENTS]       = AttributeDesc(NUM_ELEMENTS,      "nelem",           TID_UINT32, 0, 0);
    attrs[COMPRESSED_SIZE]    = AttributeDesc(COMPRESSED_SIZE,   "csize",           TID_UINT32, 0, 0);
    attrs[UNCOMPRESSED_SIZE]  = AttributeDesc(UNCOMPRESSED_SIZE, "usize",           TID_UINT32, 0, 0);
    attrs[ALLOCATED_SIZE]     = AttributeDesc(ALLOCATED_SIZE,    "asize",           TID_UINT32, 0, 0);
    attrs[FREE]               = AttributeDesc(FREE,              "free",            TID_BOOL,   0, 0);
    attrs[EMPTY_INDICATOR]    = AttributeDesc(EMPTY_INDICATOR,   DEFAULT_EMPTY_TAG_ATTRIBUTE_NAME, TID_INDICATOR, AttributeDesc::IS_EMPTY_INDICATOR, 0);
    return attrs;
}

void ListChunkDescriptorsArrayBuilder::addToArray(pair<ChunkDescriptor, bool> const& value)
{
    Value v;
    ChunkDescriptor const& desc = value.first;
    v.setUint32(desc.hdr.storageVersion);
    _outCIters[STORAGE_VERSION]->writeItem(v);
    v.setUint32(desc.hdr.instanceId);
    _outCIters[INSTANCE_ID]->writeItem(v);
    v.setUint32(desc.hdr.pos.dsGuid);
    _outCIters[DATASTORE_GUID]->writeItem(v);
    v.setUint64(desc.hdr.pos.hdrPos);
    _outCIters[DISK_HEADER_POS]->writeItem(v);
    v.setUint64(desc.hdr.pos.offs);
    _outCIters[DISK_OFFSET]->writeItem(v);
    v.setUint64(desc.hdr.arrId);
    _outCIters[V_ARRAY_ID]->writeItem(v);
    v.setUint64(desc.hdr.attId);
    _outCIters[ATTRIBUTE_ID]->writeItem(v);
    Coordinates coords(desc.coords,  desc.coords + desc.hdr.nCoordinates);
    std::ostringstream str;
    str<<CoordsToStr(coords);
    v.setString(str.str().c_str());
    _outCIters[COORDINATES]->writeItem(v);
    v.setInt8(desc.hdr.compressionMethod);
    _outCIters[COMPRESSION]->writeItem(v);
    v.setUint8(desc.hdr.flags);
    _outCIters[FLAGS]->writeItem(v);
    v.setUint32(desc.hdr.nElems);
    _outCIters[NUM_ELEMENTS]->writeItem(v);
    v.setUint32(desc.hdr.compressedSize);
    _outCIters[COMPRESSED_SIZE]->writeItem(v);
    v.setUint32(desc.hdr.size);
    _outCIters[UNCOMPRESSED_SIZE]->writeItem(v);
    v.setUint32(desc.hdr.allocatedSize);
    _outCIters[ALLOCATED_SIZE]->writeItem(v);
    v.setBool(value.second);
    _outCIters[FREE]->writeItem(v);
}

Attributes ListChunkMapArrayBuilder::getAttributes() const
{
    Attributes attrs(NUM_ATTRIBUTES);
    attrs[STORAGE_VERSION]     = AttributeDesc(STORAGE_VERSION,   "svrsn",           TID_UINT32, 0, 0);
    attrs[INSTANCE_ID]         = AttributeDesc(INSTANCE_ID,       "instn",           TID_UINT32, 0, 0);
    attrs[DATASTORE_GUID]      = AttributeDesc(DATASTORE_GUID,    "dguid",           TID_UINT32, 0, 0);
    attrs[DISK_HEADER_POS]     = AttributeDesc(DISK_HEADER_POS,   "dhdrp",           TID_UINT64, 0, 0);
    attrs[DISK_OFFSET]         = AttributeDesc(DISK_OFFSET,       "doffs",           TID_UINT64, 0, 0);
    attrs[U_ARRAY_ID]          = AttributeDesc(U_ARRAY_ID,        "uaid",            TID_UINT64, 0, 0);
    attrs[V_ARRAY_ID]          = AttributeDesc(V_ARRAY_ID,        "arrid",           TID_UINT64, 0, 0);
    attrs[ATTRIBUTE_ID]        = AttributeDesc(ATTRIBUTE_ID,      "attid",           TID_UINT64, 0, 0);
    attrs[COORDINATES]         = AttributeDesc(COORDINATES,       "coord",           TID_STRING, 0, 0);
    attrs[COMPRESSION]         = AttributeDesc(COMPRESSION,       "comp",            TID_INT8,   0, 0);
    attrs[FLAGS]               = AttributeDesc(FLAGS,             "flags",           TID_UINT8,  0, 0);
    attrs[NUM_ELEMENTS]        = AttributeDesc(NUM_ELEMENTS,      "nelem",           TID_UINT32, 0, 0);
    attrs[COMPRESSED_SIZE]     = AttributeDesc(COMPRESSED_SIZE,   "csize",           TID_UINT32, 0, 0);
    attrs[UNCOMPRESSED_SIZE]   = AttributeDesc(UNCOMPRESSED_SIZE, "usize",           TID_UINT32, 0, 0);
    attrs[ALLOCATED_SIZE]      = AttributeDesc(ALLOCATED_SIZE,    "asize",           TID_UINT32, 0, 0);
    attrs[ADDRESS]             = AttributeDesc(ADDRESS,           "addrs",           TID_UINT64, 0, 0);
    attrs[CLONE_OF]            = AttributeDesc(CLONE_OF,          "clnof",           TID_UINT64, 0, 0);
    attrs[CLONES]              = AttributeDesc(CLONES,            "clons",           TID_STRING, 0, 0);
    attrs[NEXT]                = AttributeDesc(NEXT,              "next",            TID_UINT64, 0, 0);
    attrs[PREV]                = AttributeDesc(PREV,              "prev",            TID_UINT64, 0, 0);
    attrs[DATA]                = AttributeDesc(DATA,              "data",            TID_UINT64, 0, 0);
    attrs[ACCESS_COUNT]        = AttributeDesc(ACCESS_COUNT,      "accnt",           TID_INT32,  0, 0);
    attrs[N_WRITERS]           = AttributeDesc(N_WRITERS,         "nwrit",           TID_INT32,  0, 0);
    attrs[TIMESTAMP]           = AttributeDesc(TIMESTAMP,         "tstmp",           TID_UINT64, 0, 0);
    attrs[RAW]                 = AttributeDesc(RAW,               "raw",             TID_BOOL,   0, 0);
    attrs[WAITING]             = AttributeDesc(WAITING,           "waitn",           TID_BOOL,   0, 0);
    attrs[LAST_POS]            = AttributeDesc(LAST_POS,          "lpos",            TID_STRING, 0, 0);
    attrs[FIRST_POS_OVERLAP]   = AttributeDesc(FIRST_POS_OVERLAP, "fposo",           TID_STRING, 0, 0);
    attrs[LAST_POS_OVERLAP]    = AttributeDesc(LAST_POS_OVERLAP,  "lposo",           TID_STRING, 0, 0);
    attrs[STORAGE]             = AttributeDesc(STORAGE,           "strge",           TID_UINT64, 0, 0);
    attrs[EMPTY_INDICATOR]     = AttributeDesc(EMPTY_INDICATOR,   DEFAULT_EMPTY_TAG_ATTRIBUTE_NAME, TID_INDICATOR, AttributeDesc::IS_EMPTY_INDICATOR, 0);
    return attrs;
}

void ListChunkMapArrayBuilder::addToArray(ChunkMapEntry const& value)
{
    Value v;
    PersistentChunk const* chunk = value._chunk;
    v.setUint32(chunk == NULL ? -1 : chunk->_hdr.storageVersion);
    _outCIters[STORAGE_VERSION]->writeItem(v);
    v.setUint32(chunk == NULL ? -1 : chunk->_hdr.instanceId);
    _outCIters[INSTANCE_ID]->writeItem(v);
    v.setUint32(chunk == NULL ? -1 : chunk->_hdr.pos.dsGuid);
    _outCIters[DATASTORE_GUID]->writeItem(v);
    v.setUint64(chunk == 0 ? -1 : chunk->_hdr.pos.hdrPos);
    _outCIters[DISK_HEADER_POS]->writeItem(v);
    v.setUint64(chunk == 0 ? -1 : chunk->_hdr.pos.offs);
    _outCIters[DISK_OFFSET]->writeItem(v);
    v.setUint64(value._uaid);
    _outCIters[U_ARRAY_ID]->writeItem(v);
    v.setUint64(value._addr.arrId);
    _outCIters[V_ARRAY_ID]->writeItem(v);
    v.setUint64(value._addr.attId);
    _outCIters[ATTRIBUTE_ID]->writeItem(v);
    std::ostringstream str;
    str<<CoordsToStr(value._addr.coords);
    v.setString(str.str().c_str());
    _outCIters[COORDINATES]->writeItem(v);
    v.setInt8(chunk == 0 ? -1 : chunk->_hdr.compressionMethod);
    _outCIters[COMPRESSION]->writeItem(v);
    v.setUint8(chunk == 0 ? -1 : chunk->_hdr.flags);
    _outCIters[FLAGS]->writeItem(v);
    v.setUint32(chunk == 0 ? -1 : chunk->_hdr.nElems);
    _outCIters[NUM_ELEMENTS]->writeItem(v);
    v.setUint32(chunk == 0 ? -1 : chunk->_hdr.compressedSize);
    _outCIters[COMPRESSED_SIZE]->writeItem(v);
    v.setUint32(chunk == 0 ? -1 : chunk->_hdr.size);
    _outCIters[UNCOMPRESSED_SIZE]->writeItem(v);
    v.setUint32(chunk == 0 ? -1 : chunk->_hdr.allocatedSize);
    _outCIters[ALLOCATED_SIZE]->writeItem(v);
    v.setUint64((uint64_t)(chunk));
    _outCIters[ADDRESS]->writeItem(v);
    //XXX TODO: remove this field (used to be _cloneOf)
    v.setUint64(-1) ;
    _outCIters[CLONE_OF]->writeItem(v);
    //XXX TODO: remove this field (used to be _clones)
    str.str("");
    str<<"[]";
    v.setString(str.str().c_str());
    _outCIters[CLONES]->writeItem(v);
    v.setUint64(chunk == 0 ? -1 : (uint64_t)chunk->_next);
    _outCIters[NEXT]->writeItem(v);
    v.setUint64(chunk == 0 ? -1 : (uint64_t)chunk->_prev);
    _outCIters[PREV]->writeItem(v);
    v.setUint64(chunk == 0 ? -1 : (uint64_t)chunk->_data);
    _outCIters[DATA]->writeItem(v);
    v.setInt32(chunk == 0 ? -1 : chunk->_accessCount);
    _outCIters[ACCESS_COUNT]->writeItem(v);
    //XXX tigor TODO: remove _nWrite from the schema
    v.setInt32(chunk == 0 ? -1 : -1);
    _outCIters[N_WRITERS]->writeItem(v);
    v.setUint64(chunk == 0 ? -1 : chunk->_timestamp);
    _outCIters[TIMESTAMP]->writeItem(v);
    v.setBool(chunk == 0 ? false : chunk->_raw);
    _outCIters[RAW]->writeItem(v);
    v.setBool(chunk == 0 ? false : chunk->_waiting);
    _outCIters[WAITING]->writeItem(v);
    str.str("");
    if ( chunk )
    {
        str<<CoordsToStr(chunk->_lastPos);
    }
    v.setString(str.str().c_str());
    _outCIters[LAST_POS]->writeItem(v);
    str.str("");
    if ( chunk )
    {
        str<< CoordsToStr(chunk->_firstPosWithOverlaps);
    }
    v.setString(str.str().c_str());
    _outCIters[FIRST_POS_OVERLAP]->writeItem(v);
    str.str("");
    if (chunk)
    {
        str<< CoordsToStr(chunk->_lastPosWithOverlaps);
    }
    v.setString(str.str().c_str());
    _outCIters[LAST_POS_OVERLAP]->writeItem(v);
    v.setUint64(chunk == 0 ? -1 : (uint64_t)chunk->_storage);
    _outCIters[STORAGE]->writeItem(v);
}

Attributes ListLibrariesArrayBuilder::getAttributes() const
{
    Attributes attrs(NUM_ATTRIBUTES);
    attrs[PLUGIN_NAME]         = AttributeDesc(PLUGIN_NAME,       "name",            TID_STRING, 0, 0);
    attrs[MAJOR]               = AttributeDesc(MAJOR,             "major",           TID_UINT32, 0, 0);
    attrs[MINOR]               = AttributeDesc(MINOR,             "minor",           TID_UINT32, 0, 0);
    attrs[PATCH]               = AttributeDesc(PATCH,             "patch",           TID_UINT32, 0, 0);
    attrs[BUILD]               = AttributeDesc(BUILD,             "build",           TID_UINT32, 0, 0);
    attrs[BUILD_TYPE]          = AttributeDesc(BUILD_TYPE,        "build_type",      TID_STRING, AttributeDesc::IS_NULLABLE, 0);
    attrs[EMPTY_INDICATOR]     = AttributeDesc(EMPTY_INDICATOR,   DEFAULT_EMPTY_TAG_ATTRIBUTE_NAME, TID_INDICATOR, AttributeDesc::IS_EMPTY_INDICATOR, 0);
    return attrs;
}

void ListLibrariesArrayBuilder::addToArray(LibraryInformation const& item)
{
    Value v;
    v.setString(item.pluginName.c_str());
    _outCIters[PLUGIN_NAME]->writeItem(v);
    v.setUint32(item.majorVersion);
    _outCIters[MAJOR]->writeItem(v);
    v.setUint32(item.minorVersion);
    _outCIters[MINOR]->writeItem(v);
    v.setUint32(item.buildNumber);
    _outCIters[BUILD]->writeItem(v);
    v.setUint32(item.patchVersion);
    _outCIters[PATCH]->writeItem(v);
    if (item.buildType.size())
    {
        v.setString(item.buildType.c_str());
    }
    else
    {
        v.setNull();
    }
    _outCIters[BUILD_TYPE]->writeItem(v);
}

Attributes ListQueriesArrayBuilder::getAttributes() const
{
    Attributes attrs(NUM_ATTRIBUTES);
    attrs[QUERY_ID]      = AttributeDesc(QUERY_ID,       "query_id",       TID_UINT64, 0, 0);
    attrs[COORDINATOR]   = AttributeDesc(COORDINATOR,    "coordinator",    TID_UINT64, 0, 0);
    attrs[QUERY_STR]     = AttributeDesc(QUERY_STR,      "query_string",   TID_STRING, 0, 0);
    attrs[CREATION_TIME] = AttributeDesc(CREATION_TIME,  "creation_time",  TID_DATETIME, 0, 0);
    attrs[ERROR_CODE]    = AttributeDesc(ERROR_CODE,     "error_code",     TID_INT32, 0, 0);
    attrs[ERROR]         = AttributeDesc(ERROR,          "error",          TID_STRING, 0, 0);
    attrs[IDLE]          = AttributeDesc(IDLE,           "idle",           TID_BOOL, 0, 0);
    attrs[EMPTY_INDICATOR] = AttributeDesc(EMPTY_INDICATOR,
                                           DEFAULT_EMPTY_TAG_ATTRIBUTE_NAME,
                                           TID_INDICATOR,
                                           AttributeDesc::IS_EMPTY_INDICATOR, 0);
    return attrs;
}

void ListQueriesArrayBuilder::addToArray(shared_ptr<Query> const& query)
{
    Value v;
    v.setString(query->queryString.c_str());
    _outCIters[QUERY_STR]->writeItem(v);
    v.setUint64(query->getQueryID());
    _outCIters[QUERY_ID]->writeItem(v);
    v.setUint64(query->getCoordinatorPhysicalInstanceID());
    _outCIters[COORDINATOR]->writeItem(v);
    v.setDateTime(query->getCreationTime());
    _outCIters[CREATION_TIME]->writeItem(v);
    v.setInt32(query->getErrorCode());
    _outCIters[ERROR_CODE]->writeItem(v);
    v.setString(query->getErrorDescription().c_str());
    _outCIters[ERROR]->writeItem(v);
    v.setBool(query->idle());
    _outCIters[IDLE]->writeItem(v);
}

}
