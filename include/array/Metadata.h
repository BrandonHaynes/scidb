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
 * @file Metadata.h
 *
 * @brief Structures for fetching and updating metadata of cluster.
 *
 * @author Artyom Smirnov <smirnoffjr@gmail.com>
 * @author poliocough@gmail.com
 */

#ifndef METADATA_H_
#define METADATA_H_

#include <stdint.h>
#include <string>
#include <vector>
#include <iosfwd>
#include <assert.h>
#include <set>

#include <boost/operators.hpp>
#include <boost/serialization/set.hpp>
#include <boost/serialization/map.hpp>

#include <array/Coordinate.h>
#include <query/TypeSystem.h>

namespace scidb
{

class AttributeDesc;
class DimensionDesc;
class InstanceDesc;
class LogicalOpDesc;
class ObjectNames;
class PhysicalOpDesc;
class Value;

/**
 * Vector of AttributeDesc type
 */
typedef std::vector<AttributeDesc> Attributes;

/**
 * Vector of DimensionDesc type
 */
typedef std::vector<DimensionDesc> Dimensions;

/**
 * Vector of InstanceDesc type
 */
typedef std::vector<InstanceDesc> Instances;

typedef std::vector<LogicalOpDesc> LogicalOps;

typedef std::vector<PhysicalOpDesc> PhysicalOps;

/**
 * Instance identifier
 */
typedef uint64_t InstanceID;

/**
 * Array identifier
 */
typedef uint64_t ArrayID;

/**
 * Unversioned Array identifier
 */
typedef uint64_t ArrayUAID;

/**
 * Identifier of array version
 */
typedef uint64_t VersionID;

/**
 * Attribute identifier (attribute number in array description)
 */
typedef uint32_t AttributeID;

/**
 * Note: this id is used in messages serialized by GPB and be careful with changing this type.
 */
typedef uint64_t QueryID;

typedef uint64_t OpID;

const VersionID   LAST_VERSION          = (VersionID)-1;
const VersionID   ALL_VERSIONS          = (VersionID)-2;
const InstanceID  CLIENT_INSTANCE       = ~0;  // Connection with this instance id is client connection
const InstanceID  INVALID_INSTANCE      = ~0;  // Invalid instanceID for checking that it's not registered
const QueryID     INVALID_QUERY_ID      = ~0;
const ArrayID     INVALID_ARRAY_ID      = ~0;
const AttributeID INVALID_ATTRIBUTE_ID  = ~0;
const size_t      INVALID_DIMENSION_ID  = ~0;
const std::string DEFAULT_EMPTY_TAG_ATTRIBUTE_NAME = "EmptyTag";

/**
 * Partitioning schema shows how an array is distributed among the SciDB instances.
 *
 * Guidelines for introducing a new partitioning schema:
 *   - Add to enum PartitioningSchema (right above psMax).
 *   - Modify the doxygen comments in LogicalSG.cpp.
 *   - Modify redistribute() to handle the new partitioning schema.
 *   - Modify std::ostream& operator<<(std::ostream& stream, const ArrayDistribution& dist). (See Operator.cpp)
 *   - If the partitioning schema uses extra data:
 *   -    Modify doesPartitioningSchemaHaveOptionalData.
 *   -    Derive a class from PartitioningSchemaData.
 *   -    When modifying redistribute(), consider the extra data for the new partitioning schema.
 */
enum PartitioningSchema
{
    psReplication = 0,
    psHashPartitioned,
    psLocalInstance,
    psByRow,
    psByCol,
    psUndefined,
    psGroupby,
    psScaLAPACK,

    // A newly introduced partitioning schema should be added before this line.
    psMAX
};

/**
 * Whether a partitioning schema has optional data.
 */
inline bool doesPartitioningSchemaHaveData(PartitioningSchema ps)
{
    return ps==psGroupby || ps==psScaLAPACK;
}

/**
 * Whether an uint32_t is a valid partitioning schema.
 */
inline bool isValidPartitioningSchema(uint32_t ps, bool allowOptionalData=true)
{
    if (ps >= (uint32_t)psMAX)
    {
        return false;
    }

    if (!allowOptionalData && doesPartitioningSchemaHaveData((PartitioningSchema)ps))
    {
        return false;
    }

    return true;
}

/**
 * The base class for optional data for certain PartitioningSchema.
 */
class PartitioningSchemaData
{
public:
    virtual ~PartitioningSchemaData() {}

    /**
     * return which partitioning schema this type of data is for.
     */
    virtual PartitioningSchema getID() = 0;
};

/**
 * The class for the optional data for psGroupby.
 */
struct PartitioningSchemaDataGroupby : PartitioningSchemaData
{
    /**
     * Whether each dimension is a groupby dim.
     */
    std::vector<bool> _arrIsGroupbyDim;

    virtual PartitioningSchema getID()
    {
        return psGroupby;
    }
};

/**
 * Coordinates mapping mode
 */
enum CoordinateMappingMode
{
    cmUpperBound,
    cmLowerBound,
    cmExact,
    cmTest,
    cmLowerCount,
    cmUpperCount
};

/**
 * @brief Class containing all possible object names
 *
 * During array processing schemas can be merged in many ways. For example NATURAL JOIN contain all
 * attributes from both arrays and dimensions combined. Attributes in such example received same names
 * as from original schema and also aliases from original schema name if present, so it can be used
 * later for resolving ambiguity. Dimensions in output schema received not only aliases, but also
 * additional names, so same dimension in output schema can be referenced by old name from input schema.
 *
 * Despite object using many names and aliases catalog storing only one name - base name. This name
 * will be used also for returning in result schema. So query processor handling all names but storage
 * and user API using only one.
 *
 * @note Alias this is not full name of object! Basically it prefix received from schema name or user
 * defined alias name.
 */
class ObjectNames : boost::equality_comparable<ObjectNames>
{
public:
    typedef std::set<std::string>               AliasesType;
    typedef std::map<std::string,AliasesType>   NamesType;
    typedef std::pair<std::string,AliasesType>  NamesPairType;

    ObjectNames();

    /**
     * Constructing initial name without aliases and/or additional names. This name will be later
     * used for returning to user or storing to catalog.
     *
     * @param baseName base object name
     */
    ObjectNames(const std::string &baseName);

    /**
     * Constructing full name
     *
     * @param baseName base object name
     * @param names other names and aliases
     */
    ObjectNames(const std::string &baseName, const NamesType &names);

    /**
     * Add new object name
     *
     * @param name object name
     */
    void addName(const std::string &name);

    /**
     * Add new alias name to object name
     *
     * @param alias alias name
     * @param name object name
     */
    void addAlias(const std::string &alias, const std::string &name);

    /**
     * Add new alias name to all object names
     *
     * @param alias alias name
     */
    void addAlias(const std::string &alias);

    /**
     * Check if object has such name and alias (if given).
     *
     * @param name object name
     * @param alias alias name
     * @return true if has
     */
    bool hasNameAndAlias(const std::string &name, const std::string &alias = "") const;

    /**
     * Get all names and aliases of object
     *
     * @return names and aliases map
     */
    const NamesType& getNamesAndAliases() const;

    /**
     * Get base name of object.
     *
     * @return base name of object
     */
    const std::string& getBaseName() const;

    bool operator==(const ObjectNames &o) const;

    friend std::ostream& operator<<(std::ostream& stream, const ObjectNames &ob);
    friend void printSchema (std::ostream& stream, const ObjectNames &ob);

    template<class Archive>
    void serialize(Archive& ar, const unsigned int version)
    {
        ar & _baseName;
        ar & _names;
    }

protected:
    NamesType   _names;
    std::string _baseName;
};

/**
 * Syntactic sugar to represent an n-dimensional vector.
 */
class DimensionVector : boost::equality_comparable<DimensionVector>,
                        boost::additive<DimensionVector>
{
public:
    /**
     * Create a zero-length vector in numDims dimensions.
     * @param[in] numDims number of dimensions
     */
    DimensionVector(size_t numDims = 0)
        : _data(numDims,0)
    {}

    /**
     * Create a vector based on values.
     * @param[in] vector values
     */
    DimensionVector(const Coordinates& values)
        : _data(values)
    {}

    /**
     * Check if this is a "NULL" vector.
     * @return true iff the vector has 0 dimensions.
     */
    bool isEmpty() const
    {
        return _data.empty();
    }

    /**
     * Get the number of dimensions.
     * @return the number of dimensions
     */
    size_t numDimensions() const
    {
        return _data.size();
    }

    Coordinate& operator[] (size_t index)
    {
        return isDebug() ? _data.at(index) : _data[index];
    }

    const Coordinate& operator[] (size_t index) const
    {
        return isDebug() ? _data.at(index) : _data[index];
    }

    DimensionVector& operator+= (const DimensionVector&);
    DimensionVector& operator-= (const DimensionVector&);

    friend bool operator== (const DimensionVector & a, const DimensionVector & b)
    {
        return a._data == b._data;
    }

    void clear()
    {
        _data.clear();
    }

    operator const Coordinates& () const
    {
        return _data;
    }

    /**
     * Retrieve a human-readable description.
     * Append a human-readable description of this onto str. Description takes up
     * one or more lines. Append indent spacer characters to the beginning of
     * each line.
     * @param[out] str buffer to write to
     * @param[in] indent number of spacer characters to start every line with.
     */
    void toString(std::ostringstream &,int indent = 0) const;

    template<class Archive>
    void serialize(Archive& a,unsigned version)
    {
        a & _data;
    }

private:
    Coordinates _data;
};


/**
 * Descriptor of array. Used for getting metadata of array from catalog.
 */
class ArrayDesc : boost::equality_comparable<ArrayDesc>
{
    friend class DimensionDesc;
public:
    /**
     * Various array qualifiers
     */
    enum ArrayFlags
    {
        TRANSIENT    = 0x10,    ///< Represented as a MemArray held in the transient array cache: see TransientCache.h for details.
        INVALID      = 0x20     ///< The array is no longer in a consinstent state and should be removed from the database.
    };

    /**
     * Construct empty array descriptor (for receiving metadata)
     */
    ArrayDesc();

    /**
     * Construct partial array descriptor (without id, for adding to catalog)
     *
     * @param name array name
     * @param attributes vector of attributes
     * @param dimensions vector of dimensions
     * @param flags array flags from ArrayDesc::ArrayFlags
     */
    ArrayDesc(const std::string &name, const Attributes& attributes, const Dimensions &dimensions,
              int32_t flags = 0);

    /**
     * Construct full descriptor (for returning metadata from catalog)
     *
     * @param arrId the unique array ID
     * @param uAId the unversioned array ID
     * @param vId the version number
     * @param name array name
     * @param attributes vector of attributes
     * @param dimensions vector of dimensions
     * @param flags array flags from ArrayDesc::ArrayFlags
     * @param documentation comment
     */
    ArrayDesc(ArrayID arrId, ArrayUAID uAId, VersionID vId, const std::string &name, const Attributes& attributes, const Dimensions &dimensions,
              int32_t flags = 0);

    /**
     * Copy constructor
     */
    ArrayDesc(ArrayDesc const& other);

   ~ArrayDesc() { }

    /**
     * Assignment operator
     */
    ArrayDesc& operator = (ArrayDesc const&);

    /**
     * Get the unversioned array id (id of parent array)
     * @return unversioned array id
     */
    ArrayUAID getUAId() const
    {
        return _uAId;
    }

    /**
     * Get the unique versioned array id.
     * @return the versioned array id
     */
    ArrayID getId() const
    {
        return _arrId;
    }

    /**
     * Get the array version number.
     * @return the version number
     */
    VersionID getVersionId() const
    {
        return _versionId;
    }

    /**
     * Set array identifiers
     * @param [in] arrId the versioned array id
     * @param [in] uAId the unversioned array id
     * @param [in] vId the version number
     */
    void setIds(ArrayID arrId, ArrayUAID uAId, VersionID vId)
    {
        _arrId = arrId;
        _uAId = uAId;
        _versionId = vId;
    }

    /**
     * Get name of array
     * @return array name
     */
    const std::string& getName() const
    {
        return _name;
    }

    /**
     * Set name of array
     * @param name array name
     */
    void setName(const std::string& name)
    {
        _name = name;
    }

    /**
     * Find out if an array name is for a versioned array.
     * In our current naming scheme, in order to be versioned, the name
     * must contain the "@" symbol, as in "myarray@3". However, NID
     * array names have the form "myarray@3:dimension1" and those arrays
     * are actually NOT versioned.
     * @param[in] name the name to check. A nonempty string.
     * @return true if name contains '@' at position 1 or greater and does not contain ':'.
     *         false otherwise.
     */
    static bool isNameVersioned(std::string const& name);

    /**
     * Find out if an array name is for an unversioned array - not a NID and not a version.
     * @param[in] the name to check. A nonempty string.
     * @return true if the name contains neither ':' nor '@'. False otherwise.
     */
    static bool isNameUnversioned(std::string const& name);

    /**
     * Given the versioned array name, extract the corresponing name for the unversioned array.
     * In other words, compute the name of the "parent" array. Or, simply put, given "foo@3" produce "foo".
     * @param[in] name
     * @return a substring of name up to and excluding '@', if isNameVersioned(name) is true.
     *         name otherwise.
     */
    static std::string makeUnversionedName(std::string const& name)
    {
        if (isNameVersioned(name))
        {
            size_t const locationOfAt = name.find('@');
            return name.substr(0, locationOfAt);
        }
        return name;
    }

    /**
    * Given the versioned array name, extract the version id.
    * Or, simply put, given "foo@3" produce 3.
    * @param[in] name
    * @return a substring of name after and excluding '@', converted to a VersionID, if
    *         isVersionedName(name) is true.
    *         0 otherwise.
    */
    static VersionID getVersionFromName(std::string const& name)
    {
        if(isNameVersioned(name))
        {
           size_t locationOfAt = name.find('@');
           return atol(&name[locationOfAt+1]);
        }
        return 0;
    }

    /**
     * Given an unversioned array name and a version ID, stitch the two together.
     * In other words, given "foo", 3 produce "foo@3".
     * @param[in] name must be a nonempty unversioned name
     * @param[in] version the version number
     * @return the concatenation of name, "@" and version
     */
    static std::string makeVersionedName(std::string const& name, VersionID const version)
    {
        assert(!isNameVersioned(name));
        std::stringstream ss;
        ss << name << "@" << version;
        return ss.str();
    }

    /**
     * Get static array size (number of elements within static boundaries)
     * @return array size
     */
    uint64_t getSize() const;

    /**
     * Get actual array size (number of elements within actual boundaries)
     * @return array size
     */
    uint64_t getCurrSize() const;

    /**
     * Get array size in bytes (works only for arrays with fixed size dimensions and fixed size types)
     * @return array size in bytes
     */
    uint64_t getUsedSpace() const;

    /**
     * Get number of chunks in array
     * @return number of chunks in array
     */
    uint64_t getNumberOfChunks() const;

    /**
     * Get bitmap attribute used to mark empty cells
     * @return descriptor of the empty indicator attribute or NULL is array is regular
     */
    AttributeDesc const* getEmptyBitmapAttribute() const
    {
        return _bitmapAttr;
    }

    /**
     * Get vector of array attributes
     * @return array attributes
     */
    Attributes const& getAttributes(bool excludeEmptyBitmap = false) const
    {
        return excludeEmptyBitmap ? _attributesWithoutBitmap : _attributes;
    }

    /**
     * Get vector of array dimensions
     * @return array dimensions
     */
    Dimensions const& getDimensions() const {return _dimensions;}
    Dimensions&       getDimensions()       {return _dimensions;}

    /** Set vector of array dimensions */
    ArrayDesc& setDimensions(Dimensions const& dims)
    {
        _dimensions = dims;
        initializeDimensions();
        return *this;
    }

    /**
     * Find the index of a DimensionDesc by name and alias.
     * @return index of desired dimension or -1 if not found
     */
    ssize_t findDimension(const std::string& name, const std::string& alias) const;

    /**
     * Check if position belongs to the array boundaries
     */
    bool contains(Coordinates const& pos) const;

    /**
     * Get position of the chunk for the given coordinates
     * @param[inout] pos  an element position goes in, a chunk position goes out (not including overlap).
     */
    void getChunkPositionFor(Coordinates& pos) const;

    /**
     * @return whether a given position is a chunk position.
     * @param[in] pos  a cell position.
     */
    bool isAChunkPosition(Coordinates const& pos) const;

    /**
     * @return whether a cellPos belongs to a chunk specified with a chunkPos.
     * @param[in] cellPos  a cell position.
     * @param[in] chunkPos a chunk position.
     */
    bool isCellPosInChunk(Coordinates const& cellPos, Coordinates const& chunkPos) const;

    /**
      * Get boundaries of the chunk
      * @param chunkPosition - position of the chunk (should be aligned (for example, by getChunkPositionFor)
      * @param withOverlap - include or not include chunk overlap to result
      * @param lowerBound - lower bound of chunk area
      * @param upperBound - upper bound of chunk area
      */
    void getChunkBoundaries(Coordinates const& chunkPosition,
                            bool withOverlap,
                            Coordinates& lowerBound,
                            Coordinates& upperBound) const;
   /**
     * Get hashed position of the chunk for the given coordinates
     * @param pos in: element position
     * @param box in: bounding box for offset distributions
     */
    uint64_t getHashedChunkNumber(Coordinates const& pos) const;

    /**
     * Get flags associated with array
     * @return flags
     */
    int32_t getFlags() const
    {
        return _flags;
    }

    /**
     * Trim unbounded array to its actual boundaries
     */
    void trim();

    /**
     * Checks if array has non-zero overlap in any dimension
     */
    bool hasOverlap() const;

    /**
     * Return true if the array is marked as being 'transient'. See proposal
     * 'TransientArrays' for more details.
     */
    bool isTransient() const
    {
        return _flags & TRANSIENT;
    }

    /**
     * Mark or unmark the array as being 'transient'. See proposal
     * 'TransientArrays' for more details.
     */
    ArrayDesc& setTransient(bool transient)
    {
        if (transient)
        {
            _flags |= TRANSIENT;
        }
        else
        {
            _flags &= (~TRANSIENT);
        }

        return *this;
    }

    /**
     * Return true if the array is marked as being 'invalid', that is,
     * is pending removal from the database.
     */
    bool isInvalid() const
    {
        return _flags & INVALID;
    }

    /**
     * Get partitioning schema
     */
    PartitioningSchema getPartitioningSchema() const
    {
        return _ps;
    }

    /**
     * Set partitioning schema
     */
    void setPartitioningSchema(PartitioningSchema ps)
    {
       _ps = ps;
    }

    /**
     * Add alias to all objects of schema
     *
     * @param alias alias name
     */
    void addAlias(const std::string &alias);

    template<class Archive>
    void serialize(Archive& ar,unsigned version)
    {
        ar & _arrId;
        ar & _uAId;
        ar & _versionId;
        ar & _name;
        ar & _attributes;
        ar & _dimensions;
        ar & _flags;
        //XXX tigor TODO: add _ps as well

        if (Archive::is_loading::value)
        {
            locateBitmapAttribute();
        }
    }

    bool operator ==(ArrayDesc const& other) const;

    void cutOverlap();
    Dimensions grabDimensions(VersionID version) const;

    bool coordsAreAtChunkStart(Coordinates const& coords) const;
    bool coordsAreAtChunkEnd(Coordinates const& coords) const;

    void addAttribute(AttributeDesc const& newAttribute);

    double getNumChunksAlongDimension(size_t dimension, Coordinate start = MAX_COORDINATE, Coordinate end = MIN_COORDINATE) const;

private:
    void locateBitmapAttribute();
    void initializeDimensions();


    /**
     * The Versioned Array Identifier - unique ID for every different version of a named array.
     * This is the most important number, returned by ArrayDesc::getId(). It is used all over the system -
     * to map chunks to arrays, for transaction semantics, etc.
     */
    ArrayID _arrId;

    /**
     * The Unversioned Array Identifier - unique ID for every different named array.
     * Used to relate individual array versions to the "parent" array. Some arrays are
     * not versioned. Examples are IMMUTABLE arrays as well as NID arrays.
     * For those arrays, _arrId is is equal to _uAId (and _versionId is 0)
     */
    ArrayUAID _uAId;

    /**
     * The Array Version Number - simple, aka the number 3 in "myarray@3".
     */
    VersionID _versionId;

    std::string _name;
    Attributes _attributes;
    Attributes _attributesWithoutBitmap;
    Dimensions _dimensions;
    AttributeDesc* _bitmapAttr;
    int32_t _flags;
    PartitioningSchema _ps;
};

/**
 * Attribute descriptor
 */
class AttributeDesc
{
public:
    enum AttributeFlags
    {
        IS_NULLABLE        = 1,
        IS_EMPTY_INDICATOR = 2
    };

    /**
     * Construct empty attribute descriptor (for receiving metadata)
     */
    AttributeDesc();
    virtual ~AttributeDesc() {}

    /**
     * Construct full attribute descriptor
     *
     * @param id attribute identifier
     * @param name attribute name
     * @param type attribute type
     * @param flags attribute flags from AttributeDesc::AttributeFlags
     * @param defaultCompressionMethod default compression method for this attribute
     * @param aliases attribute aliases
     * @param defaultValue default attribute value (if NULL, then use predefined default value: zero for scalar types, empty for strings,...)
     * @param comment documentation comment
     * @param varSize size of variable size type
     */
    AttributeDesc(AttributeID id, const std::string &name, TypeId type, int16_t flags,
                  uint16_t defaultCompressionMethod,
                  const std::set<std::string> &aliases = std::set<std::string>(),
                  Value const* defaultValue = NULL,
                  const std::string &defaultValueExpr = std::string(),
                  size_t varSize = 0);


    /**
     * Construct full attribute descriptor
     *
     * @param id attribute identifier
     * @param name attribute name
     * @param type attribute type
     * @param flags attribute flags from AttributeDesc::AttributeFlags
     * @param defaultCompressionMethod default compression method for this attribute
     * @param aliases attribute aliases
     * @param reserve percent of chunk space reserved for future updates
     * @param defaultValue default attribute value (if NULL, then use predefined default value: zero for scalar types, empty for strings,...)
     * @param comment documentation comment
     * @param varSize size of variable size type
     */
    AttributeDesc(AttributeID id, const std::string &name, TypeId type, int16_t flags,
                  uint16_t defaultCompressionMethod,
                  const std::set<std::string> &aliases,
                  int16_t reserve, Value const* defaultValue = NULL,
                  const std::string &defaultValueExpr = std::string(),
                  size_t varSize = 0);

    bool operator == (AttributeDesc const& other) const;
    bool operator != (AttributeDesc const& other) const
    {
        return !(*this == other);
    }

    /**
     * Get attribute identifier
     * @return attribute identifier
     */
    AttributeID getId() const;

    /**
     * Get attribute name
     * @return attribute name
     */
    const std::string& getName() const;

    /**
     * Get attribute aliases
     * @return attribute aliases
     */
    const std::set<std::string>& getAliases() const;

    /**
     * Assign new alias to attribute
     * @alias alias name
     */
    void addAlias(const std::string& alias);

    /**
     * Check if such alias present in aliases
     * @alias alias name
     * @return true if such alias present
     */
    bool hasAlias(const std::string& alias) const;

    /**
     * Get chunk reserved space percent
     * @return reserved percent of chunk size
     */
    int16_t getReserve() const;

    /**
     * Get attribute type
     * @return attribute type
     */
    TypeId getType() const;

    /**
     * Check if this attribute can have NULL values
     */
    bool isNullable() const;

    /**
     * Check if this arttribute is empty cell indicator
     */
    bool isEmptyIndicator() const;

    /**
     * Get default compression method for this attribute: it is possible to specify explictely different
     * compression methods for each chunk, but by default one returned by this method is used
     */
    uint16_t getDefaultCompressionMethod() const;

    /**
     * Get default attribute value
     */
    Value const& getDefaultValue() const;

    /**
     * Get attribute flags
     * @return attribute flags
     */
    int getFlags() const;

    /**
     * Return type size or var size (in bytes) or 0 for truly variable size.
     */
    size_t getSize() const;

    /**
     * Get the optional variable size.v
     */
    size_t getVarSize() const;

    /**
     * Retrieve a human-readable description.
     * Append a human-readable description of this onto str. Description takes up
     * one or more lines. Append indent spacer characters to the beginning of
     * each line. Call toString on interesting children. Terminate with newline.
     * @param[out] str buffer to write to
     * @param[in] indent number of spacer characters to start every line with.
     */
    virtual void toString (std::ostringstream&,int indent = 0) const;

    template<class Archive>
    void serialize(Archive& ar, const unsigned int version)
    {
        ar & _id;
        ar & _name;
        ar & _aliases;
        ar & _type;
        ar & _flags;
        ar & _defaultCompressionMethod;
        ar & _reserve;
        ar & _defaultValue;
        ar & _varSize;
        ar & _defaultValueExpr;
    }

    /**
     * Return expression string which used for default value.
     *
     * @return expression string
     */
    const std::string& getDefaultValueExpr() const;

private:
    AttributeID _id;
    std::string _name;
    std::set<std::string> _aliases;
    TypeId _type;
    int16_t _flags;
    uint16_t _defaultCompressionMethod;
    int16_t _reserve;
    Value _defaultValue;
    size_t _varSize;

    /**
     * Compiled and serialized expression for evaluating default value. Used only for storing/retrieving
     * to/from system catalog. Default value evaluated once after fetching metadata or during schema
     * construction in parser. Later only Value field passed between schemas.
     *
     * We not using Expression object because this class used on client.
     * actual value.
     */
    //TODO: May be good to have separate Metadata interface for client library
    std::string _defaultValueExpr;
};

/**
 * Descriptor of dimension
 */
class DimensionDesc : public ObjectNames, boost::equality_comparable<DimensionDesc>
{
public:
    /**
     * Construct empty dimension descriptor (for receiving metadata)
     */
    DimensionDesc();

    virtual ~DimensionDesc() {}

    /**
     * Construct full descriptor (for returning metadata from catalog)
     *
     * @param name dimension name
     * @param start dimension start
     * @param end dimension end
     * @param chunkInterval chunk size in this dimension
     * @param chunkOverlap chunk overlay in this dimension
     */
    DimensionDesc(const std::string &name,
                  Coordinate start, Coordinate end,
                  int64_t chunkInterval, int64_t chunkOverlap);

    /**
     *
     * @param baseName name of dimension derived from catalog
     * @param names dimension names and/ aliases collected during query compilation
     * @param start dimension start
     * @param end dimension end
     * @param chunkInterval chunk size in this dimension
     * @param chunkOverlap chunk overlay in this dimension
     */
    DimensionDesc(const std::string &baseName, const NamesType &names,
                  Coordinate start, Coordinate end,
                  int64_t chunkInterval, int64_t chunkOverlap);

    /**
     * Construct full descriptor (for returning metadata from catalog)
     *
     * @param name dimension name
     * @param startMin dimension minimum start
     * @param currSart dimension current start
     * @param currMax dimension current end
     * @param endMax dimension maximum end
     * @param chunkInterval chunk size in this dimension
     * @param chunkOverlap chunk overlay in this dimension
     */
    DimensionDesc(const std::string &name,
                  Coordinate startMin, Coordinate currStart,
                  Coordinate currEnd, Coordinate endMax,
                  int64_t chunkInterval, int64_t chunkOverlap);

    /**
     * Construct full descriptor (for returning metadata from catalog)
     *
     * @param baseName dimension name derived from catalog
     * @param name dimension names and/ aliases collected during query compilation
     * @param startMin dimension minimum start
     * @param currStart dimension current start
     * @param currEnd dimension current end
     * @param endMax dimension maximum end
     * @param chunkInterval chunk size in this dimension
     * @param chunkOverlap chunk overlay in this dimension
     */
    DimensionDesc(const std::string &baseName, const NamesType &names,
                  Coordinate startMin, Coordinate currStart,
                  Coordinate currEnd, Coordinate endMax,
                  int64_t chunkInterval, int64_t chunkOverlap);

    bool operator == (DimensionDesc const&) const;

    /**
     * @return minimum start coordinate.
     * @note This is reliable. The value is independent of the data in the array.
     */
    Coordinate getStartMin() const
    {
        return _startMin;
    }

    /**
     * @return current start coordinate.
     * @note In an array with no data, getCurrStart()=MAX_COORDINATE and getCurrEnd()=MIN_COORDINATE.
     * @note This is NOT reliable.
     *       The only time the value can be trusted is right after the array schema is generated by scan().
     *       As soon as we add other ops into the mix (e.g. filter(scan()), the value is not trustworthy anymore.
     */
    Coordinate getCurrStart() const
    {
        return _currStart;
    }

    /**
     * @return current end coordinate.
     * @note This is NOT reliable. @see getCurrStart().
     */
    Coordinate getCurrEnd() const
    {
        return _currEnd;
    }

    /**
     * @return maximum end coordinate.
     * @note This is reliable. @see getStartMin().
     */
    Coordinate getEndMax() const
    {
        return _endMax;
    }

    /**
     * @return dimension length, or INFINITE_LENGTH in case getStartMin() is MIN_COORDINATE or getEndMax() is MAX_COORDINATE.
     * @note This is reliable. @see getStartMin().
     */
    uint64_t getLength() const;

    /**
     * @return current dimension length.
     * @note This is NOT reliable. @see getCurrStart().
     * @note This may read from the system catalog.
     */
    uint64_t getCurrLength() const;

    /**
     * @return the chunk interval in this dimension, not including overlap.
     */
    int64_t getChunkInterval() const
    {
        return _chunkInterval;
    }

    /**
     * @return chunk overlap in this dimension.
     * @note Given base coordinate Xi, a chunk stores data with coordinates in [Xi-getChunkOverlap(), Xi+getChunkInterval()+getChunkOverlap()].
     */
    int64_t getChunkOverlap() const
    {
        return _chunkOverlap;
    }

    /**
     * Retrieve a human-readable description.
     * Append a human-readable description of this onto str. Description takes up
     * one or more lines. Append indent spacer characters to the beginning of
     * each line. Call toString on interesting children. Terminate with newline.
     * @param[out] str buffer to write to
     * @param[in] indent number of spacer characters to start every line with.
     */
    virtual void toString (std::ostringstream&,int indent = 0) const;

    template<class Archive>
    void serialize(Archive& ar, const unsigned int version)
    {
        ar & boost::serialization::base_object<ObjectNames>(*this);
        ar & _startMin;
        ar & _currStart;
        ar & _currEnd;
        ar & _endMax;
        ar & _chunkInterval;
        ar & _chunkOverlap;
    }

    void setCurrStart(Coordinate currStart)
    {
        _currStart = currStart;
    }

    void setCurrEnd(Coordinate currEnd)
    {
        _currEnd = currEnd;
    }

    void setStartMin(Coordinate startMin)
    {
        _startMin = startMin;
    }

    void setEndMax(Coordinate endMax)
    {
        _endMax = endMax;
    }

    void setChunkInterval(int64_t i)
    {
        assert(i >= 1);

        _chunkInterval = i;
    }

    void setChunkOverlap(int64_t i)
    {
        assert(i >= 0);

        _chunkOverlap = i;
    }

private:
    void validate() const;

private:
    friend class ArrayDesc;

    Coordinate _startMin;
    Coordinate _currStart;

    Coordinate _currEnd;
    Coordinate _endMax;

    /**
     * The length of the chunk along this dimension, excluding overlap.
     *
     * Chunk Interval is often used as part of coordinate math and coordinates are signed int64. To make life easier
     * for everyone, chunk interval is also signed for the moment. Same with position_t in RLE.h.
     */
    int64_t _chunkInterval;

    /**
     * The length of just the chunk overlap along this dimension.
     * Signed to make coordinate math easier.
     */
    int64_t _chunkOverlap;

    ArrayDesc* _array;
};

/**
 * Descriptor of instance
 */
class InstanceDesc
{
public:
    /**
     * Construct empty instance descriptor (for receiving metadata)
     */
    InstanceDesc();

    /**
     * Construct partial instance descriptor (without id, for adding to catalog)
     *
     * @param host ip or hostname where instance running
     * @param port listening port
     * @param path to the binary
     */
    InstanceDesc(const std::string &host, uint16_t port, const std::string &path);

    /**
     * Construct full instance descriptor
     *
     * @param instance_id instance identifier
     * @param host ip or hostname where instance running
     * @param port listening port
     * @param online instance status (online or offline)
     */
    InstanceDesc(uint64_t instance_id, const std::string &host,
                 uint16_t port,
                 uint64_t onlineTs,
                 const std::string &path);

    /**
     * Get instance identifier
     * @return instance identifier
     */
    uint64_t getInstanceId() const
    {
        return _instance_id;
    }

    /**
     * Get instance hostname or ip
     * @return instance host
     */
    const std::string& getHost() const
    {
        return _host;
    }

    /**
     * Get instance listening port number
     * @return port number
     */
    uint16_t getPort() const
    {
        return _port;
    }

    /**
     * @return time when the instance marked itself online
     */
    uint64_t getOnlineSince() const
    {
        return _online;
    }

    /**
     * Get instance binary path
     * @return path to the instance's binary
     */
    const std::string& getPath() const
    {
        return _path;
    }

private:
    uint64_t    _instance_id;
    std::string _host;
    uint16_t    _port;
    uint64_t    _online;
    std::string _path;
};

/**
 * Descriptor of pluggable logical operator
 */
class LogicalOpDesc
{
public:
    /**
     * Default constructor
     */
    LogicalOpDesc()
    {}

    /**
     * Construct descriptor for adding to catalog
     *
     * @param name Operator name
     * @param module Operator module
     * @param entry Operator entry in module
     */
    LogicalOpDesc(const std::string& name, const std::string& module, const std::string& entry) :
        _name(name),
        _module(module),
        _entry(entry)
    {}

    /**
     * Construct full descriptor
     *
     * @param logicalOpId Logical operator identifier
     * @param name Operator name
     * @param module Operator module
     * @param entry Operator entry in module
     */
    LogicalOpDesc(OpID logicalOpId, const std::string& name, const std::string& module,
                    const std::string& entry) :
        _logicalOpId(logicalOpId),
        _name(name),
        _module(module),
        _entry(entry)
    {}

    /**
     * Get logical operator identifier
     *
     * @return Operator identifier
     */
    OpID getLogicalOpId() const
    {
        return _logicalOpId;
    }

    /**
     * Get logical operator name
     *
     * @return Operator name
     */
    const std::string& getName() const
    {
        return _name;
    }

    /**
     * Get logical operator module
     *
     * @return Operator module
     */
    const std::string& getModule() const
    {
        return _module;
    }

    /**
     * Get logical operator entry in module
     *
     * @return Operator entry
     */
    const std::string& getEntry() const
    {
        return _entry;
    }

private:
    OpID        _logicalOpId;
    std::string _name;
    std::string _module;
    std::string _entry;
};

class PhysicalOpDesc
{
public:
    /**
     * Default constructor
     */
    PhysicalOpDesc()
    {}

    PhysicalOpDesc(const std::string& logicalOpName, const std::string& name,
                const std::string& module, const std::string& entry) :
        _logicalOpName(logicalOpName),
        _name(name),
        _module(module),
        _entry(entry)
    {}

    /**
     * Construct full descriptor
     *
     * @param physicalOpId Operator identifier
     * @param logicalOpName Logical operator name
     * @param name Physical operator name
     * @param module Operator module
     * @param entry Operator entry in module
     * @return
     */
    PhysicalOpDesc(OpID physicalOpId, const std::string& logicalOpName,
                const std::string& name, const std::string& module, const std::string& entry) :
        _physicalOpId(physicalOpId),
        _logicalOpName(logicalOpName),
        _name(name),
        _module(module),
        _entry(entry)
    {}

    /**
     * Get physical operator identifier
     *
     * @return Operator identifier
     */
    OpID getId() const
    {
        return _physicalOpId;
    }

    /**
     * Get logical operator name
     *
     * @return Operator name
     */
    const std::string& getLogicalName() const
    {
        return _logicalOpName;
    }

    /**
     * Get physical operator name
     *
     * @return Operator name
     */
    const std::string& getName() const
    {
        return _name;
    }

    /**
     * Get physical operator module
     *
     * @return Operator module
     */
    const std::string& getModule() const
    {
        return _module;
    }

    /**
     * Get physical operator entry in module
     *
     * @return Operator entry
     */
    const std::string& getEntry() const
    {
        return _entry;
    }

  private:
    OpID        _physicalOpId;
    std::string _logicalOpName;
    std::string _name;
    std::string _module;
    std::string _entry;
};

class VersionDesc
{
  public:
    VersionDesc(ArrayID a = 0,VersionID v = 0,time_t t = 0)
        : _arrayId(a),
          _versionId(v),
          _timestamp(t)
    {}

    ArrayID getArrayID() const
    {
        return _arrayId;
    }

    VersionID getVersionID() const
    {
        return _versionId;
    }

    time_t getTimeStamp() const
    {
        return _timestamp;
    }

  private:
    ArrayID   _arrayId;
    VersionID _versionId;
    time_t    _timestamp;
};


/**
 * Helper function to add the empty tag attribute to Attributes,
 * if the empty tag attribute did not already exist.
 *
 * @param   attributes  the original Attributes
 * @return  the new Attributes
 */
inline Attributes addEmptyTagAttribute(const Attributes& attributes) {
    size_t size = attributes.size();
    assert(size>0);
    if (attributes[size-1].isEmptyIndicator()) {
        return attributes;
    }
    Attributes newAttributes = attributes;
    newAttributes.push_back(AttributeDesc((AttributeID)newAttributes.size(),
            DEFAULT_EMPTY_TAG_ATTRIBUTE_NAME,  TID_INDICATOR, AttributeDesc::IS_EMPTY_INDICATOR, 0));
    return newAttributes;
}

/**
 * Helper function to add the empty tag attribute to ArrayDesc,
 * if the empty tag attribute did not already exist.
 *
 * @param   desc    the original ArrayDesc
 * @return  the new ArrayDesc
 */
inline ArrayDesc addEmptyTagAttribute(ArrayDesc const& desc)
{
    //XXX: This does not check to see if some other attribute does not already have the same name
    //     and it would be faster to mutate the structure, not copy. See also ArrayDesc::addAttribute
    return ArrayDesc(desc.getName(), addEmptyTagAttribute(desc.getAttributes()), desc.getDimensions());
}

/**
 * Compute the first position of a chunk, given the chunk position and the dimensions info.
 * @param[in]  chunkPos      The chunk position (not including overlap)
 * @param[in]  dims          The dimensions.
 * @param[in]  withOverlap   Whether overlap is respected.
 * @return the first chunk position
 */
inline Coordinates computeFirstChunkPosition(Coordinates const& chunkPos, Dimensions const& dims, bool withOverlap = true)
{
    assert(chunkPos.size() == dims.size());
    if (!withOverlap) {
        return chunkPos;
    }

    Coordinates firstPos = chunkPos;
    for (size_t i=0; i<dims.size(); ++i) {
        assert(chunkPos[i]>=dims[i].getStartMin());
        assert(chunkPos[i]<=dims[i].getEndMax());

        firstPos[i] -= dims[i].getChunkOverlap();
        if (firstPos[i] < dims[i].getStartMin()) {
            firstPos[i] = dims[i].getStartMin();
        }
    }
    return firstPos;
}

/**
 * Compute the last position of a chunk, given the chunk position and the dimensions info.
 * @param[in]  chunkPos      The chunk position (not including overlap)
 * @param[in]  dims          The dimensions.
 * @param[in]  withOverlap   Whether overlap is respected.
 * @return the last chunk position
 */
inline Coordinates computeLastChunkPosition(Coordinates const& chunkPos, Dimensions const& dims, bool withOverlap = true)
{
    assert(chunkPos.size() == dims.size());

    Coordinates lastPos = chunkPos;
    for (size_t i=0; i<dims.size(); ++i) {
        assert(chunkPos[i]>=dims[i].getStartMin());
        assert(chunkPos[i]<=dims[i].getEndMax());

        lastPos[i] += dims[i].getChunkInterval()-1;
        if (withOverlap) {
            lastPos[i] += dims[i].getChunkOverlap();
        }
        if (lastPos[i] > dims[i].getEndMax()) {
            lastPos[i] = dims[i].getEndMax();
        }
    }
    return lastPos;
}

/**
 * Get the logical space size of a chunk.
 * @param[in]  low   the low position of the chunk
 * @param[in]  high  the high position of the chunk
 * @return     #cells in the space that the chunk covers
 * @throw      SYSTEM_EXCEPTION(SCIDB_SE_METADATA, SCIDB_LE_LOGICAL_CHUNK_SIZE_TOO_LARGE)
 */
size_t getChunkNumberOfElements(Coordinates const& low, Coordinates const& high);

/**
 * Get the logical space size of a chunk.
 * @param[in]  chunkPos      The chunk position (not including overlap)
 * @param[in]  dims          The dimensions.
 * @param[in]  withOverlap   Whether overlap is respected.
 * @return     #cells in the space the cell covers
 */
inline size_t getChunkNumberOfElements(Coordinates const& chunkPos, Dimensions const& dims, bool withOverlap = true)
{
    Coordinates lo(computeFirstChunkPosition(chunkPos,dims,withOverlap));
    Coordinates hi(computeLastChunkPosition (chunkPos,dims,withOverlap));
    return getChunkNumberOfElements(lo,hi);
}

/**
 * Determine whether two arrays have the same partitioning.
 * @returns true IFF all dimensions have same chunk sizes and overlaps
 * @throws internal error if dimension sizes do not match.
 */
bool samePartitioning(ArrayDesc const& a1, ArrayDesc const& a2);

/**
 * Print only the pertinent part of the relevant object.
 */
void printDimNames(std::ostream&, const Dimensions&);
void printSchema(std::ostream&,const Dimensions&);
void printSchema(std::ostream&,const DimensionDesc&);
void printSchema(std::ostream&,const ArrayDesc&);
void printNames (std::ostream&,const ObjectNames::NamesType&);
std::ostream& operator<<(std::ostream&,const Attributes&);
std::ostream& operator<<(std::ostream&,const ArrayDesc&);
std::ostream& operator<<(std::ostream&,const AttributeDesc&);
std::ostream& operator<<(std::ostream&,const DimensionDesc&);
std::ostream& operator<<(std::ostream&,const Dimensions&);
std::ostream& operator<<(std::ostream&,const InstanceDesc&);
std::ostream& operator<<(std::ostream&,const ObjectNames::NamesType&);

} // namespace

#endif /* METADATA_H_ */
