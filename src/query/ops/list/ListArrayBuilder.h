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
 * ListArrayBuilder.h
 *
 *  Created on: May 25, 2012
 *      Author: poliocough@gmail.com and friends
 */

#ifndef LISTARRAYBUILDER_H_
#define LISTARRAYBUILDER_H_

#include <array/MemArray.h>
#include <smgr/io/InternalStorage.h>
#include <util/DataStore.h>
#include <util/Counter.h>

struct mallinfo;

/****************************************************************************/
namespace scidb {
/****************************************************************************/

/**
 * Abstract class to build a per-instance MemArray that contains a list of arbitrary elements.
 * Every MemArray built with this class contains two dimensions:
 * [inst=0:numInstances-1,1,0, n=0:*,LIST_CHUNK_SIZE,0]
 * Where n is the zero-based number of the object at that particular instance (0,1,2...).
 * This allows us to create a list of an arbitrary number of objects, on every instance,
 * and present this list seamlessly as a single array.
 *
 * Ideally, every subclass just needs to provide two things:
 * - a getAttributes() function which returns the list of the K attributes for the resulting array.
 *   K must include the emtpy tag.
 * - an addToArray() function which takes an object and splits it into the K-1 attribute values.
 */
template<typename T>
class ListArrayBuilder
{
protected:
    static const uint64_t LIST_CHUNK_SIZE=1000000;
    static const size_t LIST_NUM_DIMS=2;
    typedef T value_type;
    bool _initialized;
    shared_ptr<Query> _query;
    boost::shared_ptr<MemArray> _array;
    Coordinates _currPos;
    Coordinates _nextChunkPos;
    vector<shared_ptr<ArrayIterator> > _outAIters;
    vector<shared_ptr<ChunkIterator> > _outCIters;
    size_t _nAttrs;
    size_t _dimIdOff;

    /**
     * Add one element to the array
     * @param value the element to add.
     */
    virtual void addToArray(T const& value) =0;

    ListArrayBuilder()
    : _initialized(false), _nAttrs(0), _dimIdOff(0) {}

    /**
     *  Write the element 't' out as the value of attribute 'a'.
     */
    template<class type>
    void write(size_t a,const type& t);

    /**
     * Construct and return the dimensions of the array.
     * @param query the query context
     * @return dimensions as described above.
     */
    virtual Dimensions getDimensions(boost::shared_ptr<Query> const& query) const;

    /**
     * Construct and return the attributes of the array. The attributes must include the empty tag.
     * @return the attributes that the result array contains.
     */
    virtual Attributes getAttributes() const = 0;

public:
    virtual ~ListArrayBuilder() {}

    /**
     * Construct and return the schema of the array
     * @return the array named "list" using getDimensions and getAttributes
     */
    virtual ArrayDesc getSchema(boost::shared_ptr<Query> const& query) const;

    /**
     * Perform initialization and reset of internal fields. Must be called prior to calling listElement or getArray.
     * @param query the query context
     */
    virtual void initialize(boost::shared_ptr<Query> const& query);

    /**
     * Add information about one element to the array. Initialize must be called prior to this.
     * @param value the element to add
     */
    virtual void listElement(T const& value);

    /**
     * Get the result array. Initialize must be called prior to this.
     * @return a well-formed MemArray that contains information.
     */
    virtual boost::shared_ptr<MemArray> getArray();
};

/**
 * A ListArrayBuilder for listing ChunkDescriptor objects.
 * The second element in the pair is an indicator whether the Chunk Descriptor is "free" (true) or "occupied" (false).
 */
struct ListChunkDescriptorsArrayBuilder : ListArrayBuilder <pair<ChunkDescriptor,bool> >
{
    enum
    {
        STORAGE_VERSION  ,
        INSTANCE_ID      ,
        DATASTORE_GUID   ,
        DISK_HEADER_POS  ,
        DISK_OFFSET      ,
        V_ARRAY_ID       ,
        ATTRIBUTE_ID     ,
        COORDINATES      ,
        COMPRESSION      ,
        FLAGS            ,
        NUM_ELEMENTS     ,
        COMPRESSED_SIZE  ,
        UNCOMPRESSED_SIZE,
        ALLOCATED_SIZE   ,
        FREE             ,
        EMPTY_INDICATOR  ,
        NUM_ATTRIBUTES
    };

    virtual void       addToArray(value_type const&);
    virtual Attributes getAttributes() const;
};

struct ChunkMapEntry
{
    ArrayUAID               _uaid;
    StorageAddress          _addr;
    PersistentChunk const*  _chunk;

    ChunkMapEntry(ArrayUAID const uaid, StorageAddress const& addr, PersistentChunk const* const chunk):
        _uaid(uaid),
        _addr(addr),
        _chunk(chunk)
    {}
};

/**
 * A ListArrayBuilder for listing PersistentChunk objects.
 * Technically, we could take the ArrayUAID from the PersistentChunk. That value should be the same as the ArrayUAID that
 * points to the node in the tree. But we are taking the value from the tree to be extra defensive.
 */
struct ListChunkMapArrayBuilder : ListArrayBuilder<ChunkMapEntry>
{
    enum
    {
        STORAGE_VERSION     ,
        INSTANCE_ID         ,
        DATASTORE_GUID      ,
        DISK_HEADER_POS     ,
        DISK_OFFSET         ,
        U_ARRAY_ID          ,
        V_ARRAY_ID          ,
        ATTRIBUTE_ID        ,
        COORDINATES         ,
        COMPRESSION         ,
        FLAGS               ,
        NUM_ELEMENTS        ,
        COMPRESSED_SIZE     ,
        UNCOMPRESSED_SIZE   ,
        ALLOCATED_SIZE      ,
        ADDRESS             ,
        CLONE_OF            ,
        CLONES              ,
        NEXT                ,
        PREV                ,
        DATA                ,
        ACCESS_COUNT        ,
        N_WRITERS           ,
        TIMESTAMP           ,
        RAW                 ,
        WAITING             ,
        LAST_POS            ,
        FIRST_POS_OVERLAP   ,
        LAST_POS_OVERLAP    ,
        STORAGE             ,
        EMPTY_INDICATOR     ,
        NUM_ATTRIBUTES
    };

    virtual void       addToArray(value_type const&);
    virtual Attributes getAttributes() const;
};

/**
 *  A ListArrayBuilder for listing 'mallinfo' structures, one per instance.
 */
struct ListMeminfoArrayBuilder : ListArrayBuilder<struct mallinfo>
{
    enum
    {
        ARENA,              /* non-mmapped space allocated from system      */
        ORDBLKS,            /* number of free chunks                        */
        SMBLKS,             /* number of fastbin blocks                     */
        HBLKS,              /* number of mmapped regions                    */
        HBLKHD,             /* space in mmapped regions                     */
        USMBLKS,            /* maximum total allocated space                */
        FSMBLKS,            /* space available in freed fastbin blocks      */
        UORDBLKS,           /* total allocated space                        */
        FORDBLKS,           /* total free space                             */
        KEEPCOST,           /* top-most, releasable (via malloc_trim) space */
        EMPTY_INDICATOR,
        NUM_ATTRIBUTES
    };

    virtual Attributes      getAttributes() const;
    virtual void            addToArray(const value_type&);
};

/**
 * An array-listable summary of a library plugin.
 */
struct LibraryInformation
{
    /**
     * Name of the plugin (or "scidb" for core).
     */
    string pluginName;

    /**
     * Major version number.
     */
    uint32_t majorVersion;

    /**
     * Minor version number.
     */
    uint32_t minorVersion;

    /**
     * Patch number.
     */
    uint32_t patchVersion;

    /**
     * Build number.
     */
    uint32_t buildNumber;

    /**
     * The build type of the plugin. Currently used for SciDB only.
     * Sadly we don't currently store plugin build types.
     */
    string buildType;

    LibraryInformation(string const& name,
                       uint32_t maV,
                       uint32_t miV,
                       uint32_t pV,
                       uint32_t bN,
                       string const& buildType = string()):
        pluginName(name),
        majorVersion(maV),
        minorVersion(miV),
        patchVersion(pV),
        buildNumber(bN),
        buildType(buildType)
    {}
};

/**
 *  A ListArrayBuilder for listing loaded library information.
 */
struct ListLibrariesArrayBuilder : ListArrayBuilder<LibraryInformation>
{
    enum
    {
        PLUGIN_NAME     ,
        MAJOR           ,
        MINOR           ,
        PATCH           ,
        BUILD           ,
        BUILD_TYPE      ,
        EMPTY_INDICATOR ,
        NUM_ATTRIBUTES
    };

    virtual void       addToArray(value_type const&);
    virtual Attributes getAttributes() const;
};

/**
 *  A ListArrayBuilder for listing datastore information.
 */
struct ListDataStoresArrayBuilder : ListArrayBuilder<DataStore>
{
    enum
    {
        GUID            ,
        FILE_BYTES      ,
        FILE_BLOCKS_512 ,
        RESERVED_BYTES  ,
        FREE_BYTES      ,
        EMPTY_INDICATOR ,
        NUM_ATTRIBUTES
    };

    virtual void       addToArray(value_type const&);
    virtual Attributes getAttributes() const;
};

/**
 *  A ListArrayBuilder for listing Query objects.
 */
struct ListQueriesArrayBuilder : ListArrayBuilder<boost::shared_ptr<Query> >
{
    enum
    {
        QUERY_ID,
        COORDINATOR,
        QUERY_STR,
        CREATION_TIME,
        ERROR_CODE,
        ERROR,
        IDLE,
        EMPTY_INDICATOR,
        NUM_ATTRIBUTES
    };

    virtual void       addToArray(value_type const&);
    virtual Attributes getAttributes() const;
};


/**
 *  A ListArrayBuilder for listing counter values.
 */
struct ListCounterArrayBuilder : ListArrayBuilder<CounterState::Entry>
{
    enum
    {
        NAME,
        TOTAL,
        TOTAL_MSECS,
        AVG_MSECS,
        EMPTY_INDICATOR,
        NUM_ATTRIBUTES
    };

    virtual void       addToArray(value_type const&);
    virtual Attributes getAttributes() const;
};

/**
 *  A ListArrayBuilder for listing array information.
 */
class ListArraysArrayBuilder : public ListArrayBuilder<ArrayDesc>
{
public:
    enum
    {
    ARRAY_NAME,
    ARRAY_UAID,
    ARRAY_ID,
    ARRAY_SCHEMA,
    ARRAY_IS_AVAILABLE,
    ARRAY_IS_TRANSIENT,
    EMPTY_INDICATOR,
    NUM_ATTRIBUTES
    };
    virtual void addToArray(value_type const&);
    virtual Attributes getAttributes() const;
    virtual Dimensions getDimensions(boost::shared_ptr<Query> const& query) const;
};

/****************************************************************************/
}
/****************************************************************************/
#endif
/****************************************************************************/
