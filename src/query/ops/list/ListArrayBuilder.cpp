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
 *      Author: poliocough@gmail.com and friends
 */

#include <malloc.h>
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
    _dimIdOff = 0;
    _query = query;
    ArrayDesc schema = getSchema(_query);
    _nAttrs = schema.getAttributes().size() - 1;
    _array = shared_ptr<MemArray>(new MemArray(schema, query));
    const size_t numDims = schema.getDimensions().size();
    _currPos = Coordinates(numDims,0);

    if (numDims > 1) {
        // adding the instance coordinate
        Coordinate myInstance = query->getInstanceID();
        _currPos[0] = myInstance;
        _dimIdOff = 1;
    }
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
    _nextChunkPos[_dimIdOff] += LIST_CHUNK_SIZE;
    _initialized = true;
}

template <typename T>
void ListArrayBuilder<T>::listElement(T const& element)
{
    assert(_initialized);
    if(_currPos[_dimIdOff]==_nextChunkPos[_dimIdOff])
    {
        for(AttributeID i=0; i<_nAttrs; ++i)
        {
            _outCIters[i]->flush();
            Chunk& ch = _outAIters[i]->newChunk(_currPos);
            _outCIters[i] = ch.getIterator(_query,i == 0 ? ChunkIterator::SEQUENTIAL_WRITE :
                                                           ChunkIterator::SEQUENTIAL_WRITE | ChunkIterator::NO_EMPTY_CHECK);
        }
        _nextChunkPos[_dimIdOff] += LIST_CHUNK_SIZE;
    }
    for(AttributeID i=0; i<_nAttrs; ++i)
    {
        _outCIters[i]->setPosition(_currPos);
    }
    addToArray(element);
    ++_currPos[_dimIdOff];
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

template<typename T>
template<typename type>
void ListArrayBuilder<T>::write(size_t a,const type& t)
{// might want to specialize this for tyoe std:string?
    _outCIters[a]->writeItem(Value(t,Value::asData));
}

Attributes ListChunkDescriptorsArrayBuilder::getAttributes() const
{
    Attributes attrs(NUM_ATTRIBUTES);
    attrs[STORAGE_VERSION]    = AttributeDesc(STORAGE_VERSION,   "svrsn",           TID_UINT32, 0, 0);
    attrs[INSTANCE_ID]        = AttributeDesc(INSTANCE_ID,       "insn",            TID_UINT32, 0, 0);
    attrs[DATASTORE_GUID]     = AttributeDesc(DATASTORE_GUID,    "dguid",           TID_UINT64, 0, 0);
    attrs[DISK_HEADER_POS]    = AttributeDesc(DISK_HEADER_POS,   "dhdrp",           TID_UINT64, 0, 0);
    attrs[DISK_OFFSET]        = AttributeDesc(DISK_OFFSET,       "doffs",           TID_UINT64, 0, 0);
    attrs[V_ARRAY_ID]         = AttributeDesc(V_ARRAY_ID,        "arrid",           TID_UINT64, 0, 0);
    attrs[ATTRIBUTE_ID]       = AttributeDesc(ATTRIBUTE_ID,      "attid",           TID_UINT64, 0, 0);
    attrs[COORDINATES]        = AttributeDesc(COORDINATES,       "coord",           TID_STRING, 0, 0);
    attrs[COMPRESSION]        = AttributeDesc(COMPRESSION,       "comp",            TID_INT8,   0, 0);
    attrs[FLAGS]              = AttributeDesc(FLAGS,             "flags",           TID_UINT8,  0, 0);
    attrs[NUM_ELEMENTS]       = AttributeDesc(NUM_ELEMENTS,      "nelem",           TID_UINT32, 0, 0);
    attrs[COMPRESSED_SIZE]    = AttributeDesc(COMPRESSED_SIZE,   "csize",           TID_UINT64, 0, 0);
    attrs[UNCOMPRESSED_SIZE]  = AttributeDesc(UNCOMPRESSED_SIZE, "usize",           TID_UINT64, 0, 0);
    attrs[ALLOCATED_SIZE]     = AttributeDesc(ALLOCATED_SIZE,    "asize",           TID_UINT64, 0, 0);
    attrs[FREE]               = AttributeDesc(FREE,              "free",            TID_BOOL,   0, 0);
    attrs[EMPTY_INDICATOR]    = AttributeDesc(EMPTY_INDICATOR,   DEFAULT_EMPTY_TAG_ATTRIBUTE_NAME, TID_INDICATOR, AttributeDesc::IS_EMPTY_INDICATOR, 0);
    return attrs;
}

void ListChunkDescriptorsArrayBuilder::addToArray(pair<ChunkDescriptor, bool> const& value)
{
    Value v;
    ChunkDescriptor const& desc = value.first;
    v.reset(desc.hdr.storageVersion);
    _outCIters[STORAGE_VERSION]->writeItem(v);
    v.reset(desc.hdr.instanceId);
    _outCIters[INSTANCE_ID]->writeItem(v);
    v.reset(desc.hdr.pos.dsGuid);
    _outCIters[DATASTORE_GUID]->writeItem(v);
    v.reset(desc.hdr.pos.hdrPos);
    _outCIters[DISK_HEADER_POS]->writeItem(v);
    v.reset(desc.hdr.pos.offs);
    _outCIters[DISK_OFFSET]->writeItem(v);
    v.reset(desc.hdr.arrId);
    _outCIters[V_ARRAY_ID]->writeItem(v);
    v.reset(desc.hdr.attId);
    _outCIters[ATTRIBUTE_ID]->writeItem(v);
    Coordinates coords(desc.coords,  desc.coords + desc.hdr.nCoordinates);
    std::ostringstream str;
    str<<CoordsToStr(coords);
    v.setString(str.str());
    _outCIters[COORDINATES]->writeItem(v);
    v.reset(desc.hdr.compressionMethod);
    _outCIters[COMPRESSION]->writeItem(v);
    v.reset(desc.hdr.flags);
    _outCIters[FLAGS]->writeItem(v);
    v.reset(desc.hdr.nElems);
    _outCIters[NUM_ELEMENTS]->writeItem(v);
    v.reset(desc.hdr.compressedSize);
    _outCIters[COMPRESSED_SIZE]->writeItem(v);
    v.reset(desc.hdr.size);
    _outCIters[UNCOMPRESSED_SIZE]->writeItem(v);
    v.reset(desc.hdr.allocatedSize);
    _outCIters[ALLOCATED_SIZE]->writeItem(v);
    v.reset(value.second);
    _outCIters[FREE]->writeItem(v);
}

Attributes ListChunkMapArrayBuilder::getAttributes() const
{
    Attributes attrs(NUM_ATTRIBUTES);
    attrs[STORAGE_VERSION]     = AttributeDesc(STORAGE_VERSION,   "svrsn",           TID_UINT32, 0, 0);
    attrs[INSTANCE_ID]         = AttributeDesc(INSTANCE_ID,       "instn",           TID_UINT32, 0, 0);
    attrs[DATASTORE_GUID]      = AttributeDesc(DATASTORE_GUID,    "dguid",           TID_UINT64, 0, 0);
    attrs[DISK_HEADER_POS]     = AttributeDesc(DISK_HEADER_POS,   "dhdrp",           TID_UINT64, 0, 0);
    attrs[DISK_OFFSET]         = AttributeDesc(DISK_OFFSET,       "doffs",           TID_UINT64, 0, 0);
    attrs[U_ARRAY_ID]          = AttributeDesc(U_ARRAY_ID,        "uaid",            TID_UINT64, 0, 0);
    attrs[V_ARRAY_ID]          = AttributeDesc(V_ARRAY_ID,        "arrid",           TID_UINT64, 0, 0);
    attrs[ATTRIBUTE_ID]        = AttributeDesc(ATTRIBUTE_ID,      "attid",           TID_UINT64, 0, 0);
    attrs[COORDINATES]         = AttributeDesc(COORDINATES,       "coord",           TID_STRING, 0, 0);
    attrs[COMPRESSION]         = AttributeDesc(COMPRESSION,       "comp",            TID_INT8,   0, 0);
    attrs[FLAGS]               = AttributeDesc(FLAGS,             "flags",           TID_UINT8,  0, 0);
    attrs[NUM_ELEMENTS]        = AttributeDesc(NUM_ELEMENTS,      "nelem",           TID_UINT32, 0, 0);
    attrs[COMPRESSED_SIZE]     = AttributeDesc(COMPRESSED_SIZE,   "csize",           TID_UINT64, 0, 0);
    attrs[UNCOMPRESSED_SIZE]   = AttributeDesc(UNCOMPRESSED_SIZE, "usize",           TID_UINT64, 0, 0);
    attrs[ALLOCATED_SIZE]      = AttributeDesc(ALLOCATED_SIZE,    "asize",           TID_UINT64, 0, 0);
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
    v.reset(chunk == NULL ? -1 : chunk->_hdr.storageVersion);
    _outCIters[STORAGE_VERSION]->writeItem(v);
    v.reset(chunk == NULL ? -1 : chunk->_hdr.instanceId);
    _outCIters[INSTANCE_ID]->writeItem(v);
    v.reset(chunk == NULL ? -1 : chunk->_hdr.pos.dsGuid);
    _outCIters[DATASTORE_GUID]->writeItem(v);
    v.reset(chunk == 0 ? -1 : chunk->_hdr.pos.hdrPos);
    _outCIters[DISK_HEADER_POS]->writeItem(v);
    v.reset(chunk == 0 ? -1 : chunk->_hdr.pos.offs);
    _outCIters[DISK_OFFSET]->writeItem(v);
    v.reset(value._uaid);
    _outCIters[U_ARRAY_ID]->writeItem(v);
    v.reset(value._addr.arrId);
    _outCIters[V_ARRAY_ID]->writeItem(v);
    v.reset(value._addr.attId);
    _outCIters[ATTRIBUTE_ID]->writeItem(v);
    std::ostringstream str;
    str<<CoordsToStr(value._addr.coords);
    v.setString(str.str());
    _outCIters[COORDINATES]->writeItem(v);
    v.reset(chunk == 0 ? int8_t(-1) : chunk->_hdr.compressionMethod);
    _outCIters[COMPRESSION]->writeItem(v);
    v.reset(chunk == 0 ? uint8_t(-1) : chunk->_hdr.flags);
    _outCIters[FLAGS]->writeItem(v);
    v.reset(chunk == 0 ? -1 : chunk->_hdr.nElems);
    _outCIters[NUM_ELEMENTS]->writeItem(v);
    v.reset(chunk == 0 ? -1 : chunk->_hdr.compressedSize);
    _outCIters[COMPRESSED_SIZE]->writeItem(v);
    v.reset(chunk == 0 ? -1 : chunk->_hdr.size);
    _outCIters[UNCOMPRESSED_SIZE]->writeItem(v);
    v.reset(chunk == 0 ? -1 : chunk->_hdr.allocatedSize);
    _outCIters[ALLOCATED_SIZE]->writeItem(v);
    v.reset((uint64_t)(chunk));
    _outCIters[ADDRESS]->writeItem(v);
    //XXX TODO: remove this field (used to be _cloneOf)
    v.reset(-1) ;
    _outCIters[CLONE_OF]->writeItem(v);
    //XXX TODO: remove this field (used to be _clones)
    v.setString("[]");
    _outCIters[CLONES]->writeItem(v);
    v.reset(chunk == 0 ? -1 : (uint64_t)chunk->_next);
    _outCIters[NEXT]->writeItem(v);
    v.reset(chunk == 0 ? -1 : (uint64_t)chunk->_prev);
    _outCIters[PREV]->writeItem(v);
    v.reset(chunk == 0 ? -1 : (uint64_t)chunk->_data);
    _outCIters[DATA]->writeItem(v);
    v.reset(chunk == 0 ? -1 : chunk->_accessCount);
    _outCIters[ACCESS_COUNT]->writeItem(v);
    //XXX tigor TODO: remove _nWrite from the schema
    v.reset(chunk == 0 ? -1 : -1);
    _outCIters[N_WRITERS]->writeItem(v);
    v.reset(chunk == 0 ? -1 : chunk->_timestamp);
    _outCIters[TIMESTAMP]->writeItem(v);
    v.reset(chunk == 0 ? false : chunk->_raw);
    _outCIters[RAW]->writeItem(v);
    v.reset(chunk == 0 ? false : chunk->_waiting);
    _outCIters[WAITING]->writeItem(v);
    str.str("");
    if ( chunk )
    {
        str<<CoordsToStr(chunk->_lastPos);
    }
    v.setString(str.str());
    _outCIters[LAST_POS]->writeItem(v);
    str.str("");
    if ( chunk )
    {
        str<< CoordsToStr(chunk->_firstPosWithOverlaps);
    }
    v.setString(str.str());
    _outCIters[FIRST_POS_OVERLAP]->writeItem(v);
    str.str("");
    if (chunk)
    {
        str<< CoordsToStr(chunk->_lastPosWithOverlaps);
    }
    v.setString(str.str());
    _outCIters[LAST_POS_OVERLAP]->writeItem(v);
    v.reset(chunk == 0 ? -1 : (uint64_t)chunk->_storage);
    _outCIters[STORAGE]->writeItem(v);
}

Attributes ListMeminfoArrayBuilder::getAttributes() const
{
    Attributes a(NUM_ATTRIBUTES);
    a[ARENA   ]        = AttributeDesc(ARENA          ,"arena",   TID_INT32,0,0);
    a[ORDBLKS ]        = AttributeDesc(ORDBLKS        ,"ordblks", TID_INT32,0,0);
    a[SMBLKS  ]        = AttributeDesc(SMBLKS         ,"smblks",  TID_INT32,0,0);
    a[HBLKS   ]        = AttributeDesc(HBLKS          ,"hblks",   TID_INT32,0,0);
    a[HBLKHD  ]        = AttributeDesc(HBLKHD         ,"hblkhd",  TID_INT32,0,0);
    a[USMBLKS ]        = AttributeDesc(USMBLKS        ,"usmblks", TID_INT32,0,0);
    a[FSMBLKS ]        = AttributeDesc(FSMBLKS        ,"fsmblks", TID_INT32,0,0);
    a[UORDBLKS]        = AttributeDesc(UORDBLKS       ,"uordblks",TID_INT32,0,0);
    a[FORDBLKS]        = AttributeDesc(FORDBLKS       ,"fordblks",TID_INT32,0,0);
    a[KEEPCOST]        = AttributeDesc(KEEPCOST       ,"keepcost",TID_INT32,0,0);
    a[EMPTY_INDICATOR] = AttributeDesc(EMPTY_INDICATOR,DEFAULT_EMPTY_TAG_ATTRIBUTE_NAME,TID_INDICATOR,AttributeDesc::IS_EMPTY_INDICATOR,0);
    return a;
}

void ListMeminfoArrayBuilder::addToArray(struct mallinfo const& mi)
{
    write(ARENA   ,mi.arena   );
    write(ORDBLKS ,mi.ordblks );
    write(SMBLKS  ,mi.smblks  );
    write(HBLKS   ,mi.hblks   );
    write(HBLKHD  ,mi.hblkhd  );
    write(USMBLKS ,mi.usmblks );
    write(FSMBLKS ,mi.fsmblks );
    write(UORDBLKS,mi.uordblks);
    write(FORDBLKS,mi.fordblks);
    write(KEEPCOST,mi.keepcost);
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
    v.setString(item.pluginName);
    _outCIters[PLUGIN_NAME]->writeItem(v);
    v.reset(item.majorVersion);
    _outCIters[MAJOR]->writeItem(v);
    v.reset(item.minorVersion);
    _outCIters[MINOR]->writeItem(v);
    v.reset(item.buildNumber);
    _outCIters[BUILD]->writeItem(v);
    v.reset(item.patchVersion);
    _outCIters[PATCH]->writeItem(v);
    if (item.buildType.size())
    {
        v.setString(item.buildType);
    }
    else
    {
        v.setNull();
    }
    _outCIters[BUILD_TYPE]->writeItem(v);
}

Attributes ListDataStoresArrayBuilder::getAttributes() const
{
    Attributes attrs(NUM_ATTRIBUTES);
    attrs[GUID]                = AttributeDesc(GUID,              "uaid",            TID_UINT64, 0, 0);
    attrs[FILE_BYTES]          = AttributeDesc(FILE_BYTES,        "file_bytes",      TID_UINT64, 0, 0);
    attrs[FILE_BLOCKS_512]     = AttributeDesc(FILE_BLOCKS_512,   "file_blocks_512", TID_UINT64, 0, 0);
    attrs[RESERVED_BYTES]      = AttributeDesc(RESERVED_BYTES,    "log_resv_bytes",  TID_UINT64, 0, 0);
    attrs[FREE_BYTES]          = AttributeDesc(FREE_BYTES,        "log_free_bytes",  TID_UINT64, 0, 0);
    attrs[EMPTY_INDICATOR]     = AttributeDesc(EMPTY_INDICATOR,
                                               DEFAULT_EMPTY_TAG_ATTRIBUTE_NAME,
                                               TID_INDICATOR,
                                               AttributeDesc::IS_EMPTY_INDICATOR,
                                               0);
    return attrs;
}

void ListDataStoresArrayBuilder::addToArray(DataStore const& item)
{
    Value v;
    off_t filesize = 0;
    blkcnt_t fileblks = 0;
    off_t reservedbytes = 0;
    off_t freebytes = 0;

    item.getSizes(filesize, fileblks, reservedbytes, freebytes);
    v.reset(item.getGuid());
    _outCIters[GUID]->writeItem(v);
    v.reset(filesize);
    _outCIters[FILE_BYTES]->writeItem(v);
    v.reset(fileblks);
    _outCIters[FILE_BLOCKS_512]->writeItem(v);
    v.reset(reservedbytes);
    _outCIters[RESERVED_BYTES]->writeItem(v);
    v.reset(freebytes);
    _outCIters[FREE_BYTES]->writeItem(v);
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
    v.setString(query->queryString);
    _outCIters[QUERY_STR]->writeItem(v);
    v.reset(query->getQueryID());
    _outCIters[QUERY_ID]->writeItem(v);
    const bool resolveLocalInstanceID=true;
    v.reset(query->getPhysicalCoordinatorID(resolveLocalInstanceID));
    _outCIters[COORDINATOR]->writeItem(v);
    v.reset(query->getCreationTime());
    _outCIters[CREATION_TIME]->writeItem(v);
    v.reset(query->getErrorCode());
    _outCIters[ERROR_CODE]->writeItem(v);
    v.setString(query->getErrorDescription());
    _outCIters[ERROR]->writeItem(v);
    v.reset(query->idle());
    _outCIters[IDLE]->writeItem(v);
}

Attributes ListCounterArrayBuilder::getAttributes() const
{
    Attributes attrs(NUM_ATTRIBUTES);
    attrs[NAME]             = AttributeDesc(NAME,           "name",        TID_STRING, 0, 0);
    attrs[TOTAL]            = AttributeDesc(TOTAL,          "total",       TID_UINT64, 0, 0);
    attrs[TOTAL_MSECS]      = AttributeDesc(TOTAL_MSECS,    "total_msecs", TID_UINT64, 0, 0);
    attrs[AVG_MSECS]        = AttributeDesc(AVG_MSECS,      "avg_msecs",   TID_FLOAT, 0, 0);
    attrs[EMPTY_INDICATOR]  = AttributeDesc(EMPTY_INDICATOR,
                                            DEFAULT_EMPTY_TAG_ATTRIBUTE_NAME,
                                            TID_INDICATOR,
                                            AttributeDesc::IS_EMPTY_INDICATOR,
                                            0);
    return attrs;
}

void ListCounterArrayBuilder::addToArray(CounterState::Entry const& item)
{
    Value v;
    float avg_msecs =
      item._num ?
      static_cast<float>(item._msecs) / static_cast<float>(item._num) :
      0;

    v.setString(CounterState::getInstance()->getName(item._id));
    _outCIters[NAME]->writeItem(v);
    v.reset(item._num);
    _outCIters[TOTAL]->writeItem(v);
    v.reset(item._msecs);
    _outCIters[TOTAL_MSECS]->writeItem(v);
    v.reset(avg_msecs);
    _outCIters[AVG_MSECS]->writeItem(v);
}


Attributes ListArraysArrayBuilder::getAttributes() const
{
    Attributes attrs(NUM_ATTRIBUTES);
    attrs[ARRAY_NAME] = AttributeDesc(ARRAY_NAME, "name", TID_STRING,0,0);
    attrs[ARRAY_UAID] = AttributeDesc(ARRAY_UAID, "uaid", TID_INT64,0,0);
    attrs[ARRAY_ID] = AttributeDesc(ARRAY_ID, "aid", TID_INT64,0,0);
    attrs[ARRAY_SCHEMA] = AttributeDesc(ARRAY_SCHEMA,  "schema", TID_STRING,0,0);
    attrs[ARRAY_IS_AVAILABLE] = AttributeDesc(ARRAY_IS_AVAILABLE, "availability", TID_BOOL,0,0);
    attrs[ARRAY_IS_TRANSIENT] = AttributeDesc(ARRAY_IS_TRANSIENT,  "temporary",    TID_BOOL,0,0);
    attrs[EMPTY_INDICATOR] = AttributeDesc(EMPTY_INDICATOR,
                                           DEFAULT_EMPTY_TAG_ATTRIBUTE_NAME,
                                           TID_INDICATOR,
                                           AttributeDesc::IS_EMPTY_INDICATOR,
                                           0);
    return attrs;
}


Dimensions ListArraysArrayBuilder::getDimensions(shared_ptr<Query> const& query) const
{
    Dimensions dims(1, DimensionDesc("No", 0, 0, MAX_COORDINATE, MAX_COORDINATE, LIST_CHUNK_SIZE, 0));
    return dims;
}

void ListArraysArrayBuilder::addToArray(ArrayDesc const& desc)
{
    Value v;
    v.setString(desc.getName());
    _outCIters[ARRAY_NAME]->writeItem(v);

    v.reset(desc.getUAId());
    _outCIters[ARRAY_UAID]->writeItem(v);

    v.reset(desc.getId());
    _outCIters[ARRAY_ID]->writeItem(v);

    stringstream ss;
    printSchema(ss, desc);
    v.setString(ss.str());
    _outCIters[ARRAY_SCHEMA]->writeItem(v);

    v.reset(!desc.isInvalid());
    _outCIters[ARRAY_IS_AVAILABLE]->writeItem(v);

    v.reset(desc.isTransient());
    _outCIters[ARRAY_IS_TRANSIENT]->writeItem(v);
}

}
