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
 *      Author: poliocough@gmail.com
 */

#ifndef LISTARRAYBUILDER_H_
#define LISTARRAYBUILDER_H_

#include <array/MemArray.h>
#include <smgr/io/InternalStorage.h>


namespace scidb
{

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
template <typename T>
class ListArrayBuilder
{
protected:
    static const uint64_t LIST_CHUNK_SIZE=1000000;
    static const size_t LIST_NUM_DIMS=2;

    bool _initialized;
    shared_ptr<Query> _query;
    boost::shared_ptr<MemArray> _array;
    Coordinates _currPos;
    Coordinates _nextChunkPos;
    vector<shared_ptr<ArrayIterator> > _outAIters;
    vector<shared_ptr<ChunkIterator> > _outCIters;
    size_t _nAttrs;

    /**
     * Add one element to the array
     * @param value the element to add.
     */
    virtual void addToArray(T const& value) =0;

    ListArrayBuilder(): _initialized(false) {}

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
class ListChunkDescriptorsArrayBuilder : public ListArrayBuilder < pair<ChunkDescriptor, bool> >
{
private:
    /**
     * Verbose names of all the attributes output by list('chunk descriptors') for internal consistency and dev readability.
     */
    enum Attrs
    {
        STORAGE_VERSION     =0,
        INSTANCE_ID         =1,
        DATASTORE_GUID      =2,
        DISK_HEADER_POS     =3,
        DISK_OFFSET         =4,
        V_ARRAY_ID          =5,
        ATTRIBUTE_ID        =6,
        COORDINATES         =7,
        COMPRESSION         =8,
        FLAGS               =9,
        NUM_ELEMENTS        =10,
        COMPRESSED_SIZE     =11,
        UNCOMPRESSED_SIZE   =12,
        ALLOCATED_SIZE      =13,
        FREE                =14,
        EMPTY_INDICATOR     =15,
        NUM_ATTRIBUTES      =16
    };

    /**
     * Add information abotu a ChunkDescriptor to the array.
     * @param value the first element is the descriptor, the second element is true if descriptor is free.
     */
    virtual void addToArray(pair<ChunkDescriptor, bool> const& value);

public:
    /**
     * Get the attributes of the array.
     * @return the attribute descriptors
     */
    virtual Attributes getAttributes() const;
};

struct ChunkMapEntry
{
    ArrayUAID _uaid;
    StorageAddress _addr;
    PersistentChunk const* _chunk;

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
class ListChunkMapArrayBuilder : public ListArrayBuilder <ChunkMapEntry>
{
private:
    /**
     * Verbose names of all the attributes output by list('chunk map') for internal consistency and dev readability.
     */
    enum Attrs
    {
        STORAGE_VERSION     =0,
        INSTANCE_ID         =1,
        DATASTORE_GUID      =2,
        DISK_HEADER_POS     =3,
        DISK_OFFSET         =4,
        U_ARRAY_ID          =5,
        V_ARRAY_ID          =6,
        ATTRIBUTE_ID        =7,
        COORDINATES         =8,
        COMPRESSION         =9,
        FLAGS               =10,
        NUM_ELEMENTS        =11,
        COMPRESSED_SIZE     =12,
        UNCOMPRESSED_SIZE   =13,
        ALLOCATED_SIZE      =14,
        ADDRESS             =15,
        CLONE_OF            =16,
        CLONES              =17,
        NEXT                =18,
        PREV                =19,
        DATA                =20,
        ACCESS_COUNT        =21,
        N_WRITERS           =22,
        TIMESTAMP           =23,
        RAW                 =24,
        WAITING             =25,
        LAST_POS            =26,
        FIRST_POS_OVERLAP   =27,
        LAST_POS_OVERLAP    =28,
        STORAGE             =29,
        EMPTY_INDICATOR     =30,
        NUM_ATTRIBUTES      =31
    };

    /**
     * Add information about a PersistentChunk to the array.
     * @param value - a pair of the Unversioned Array ID and PersistentChunk to list
     */
    virtual void addToArray(ChunkMapEntry const& value);

public:
    /**
     * Get the attributes of the array
     * @return the attribute descriptors
     */
    virtual Attributes getAttributes() const;
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
                       uint32_t const maV,
                       uint32_t const miV,
                       uint32_t const pV,
                       uint32_t const bN,
                       string const buildType = ""):
        pluginName(name),
        majorVersion(maV),
        minorVersion(miV),
        patchVersion(pV),
        buildNumber(bN),
        buildType(buildType)
    {}
};

/**
 * A ListArrayBuilder for listing PersistentChunk objects.
 * Technically, we could take the ArrayUAID from the PersistentChunk. That value should be the same as the ArrayUAID that
 * points to the node in the tree. But we are taking the value from the tree to be extra defensive.
 */
class ListLibrariesArrayBuilder : public ListArrayBuilder <LibraryInformation>
{
private:
    /**
     * Verbose names of all the attributes output by list('chunk map') for internal consistency and dev readability.
     */
    enum Attrs
    {
        PLUGIN_NAME     =0,
        MAJOR           =1,
        MINOR           =2,
        PATCH           =3,
        BUILD           =4,
        BUILD_TYPE      =5,
        EMPTY_INDICATOR =6,
        NUM_ATTRIBUTES  =7
    };

    /**
     * Add information about a PersistentChunk to the array.
     * @param value - a pair of the Unversioned Array ID and PersistentChunk to list
     */
    virtual void addToArray(LibraryInformation const& item);

public:
    /**
     * Get the attributes of the array
     * @return the attribute descriptors
     */
    virtual Attributes getAttributes() const;
};

/**
 * A ListArrayBuilder for listing Query objects.
 */
class ListQueriesArrayBuilder : public ListArrayBuilder < boost::shared_ptr<Query> >
{
private:
    /**
     * Verbose names of all the attributes output by list('queries') for internal consistency and dev readability.
     */
    enum Attrs
    {
    QUERY_ID=0,
    COORDINATOR,
    QUERY_STR,
    CREATION_TIME,
    ERROR_CODE,
    ERROR,
    IDLE,
    EMPTY_INDICATOR,
    NUM_ATTRIBUTES // must be last
    };

    /**
     * Add information about a Query to the array.
     * @param item query to add
     */
    virtual void addToArray(boost::shared_ptr<Query> const& item);

public:
    /**
     * Get the attributes of the array
     * @return the attribute descriptors
     */
    virtual Attributes getAttributes() const;
};

}

#endif /* LISTARRAYBUILDER_H_ */
