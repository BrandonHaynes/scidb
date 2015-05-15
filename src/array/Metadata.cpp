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
 * @file Metadata.cpp
 *
 * @brief Structures for fetching and updating cluster metadata.
 *
 * @author Artyom Smirnov <smirnoffjr@gmail.com>
 */
#include <sstream>
#include <boost/foreach.hpp>
#include <boost/algorithm/string.hpp>
#include <boost/filesystem/path.hpp>

#ifndef SCIDB_CLIENT
#include <system/Config.h>
#endif

#include <util/PointerRange.h>
#include <system/SciDBConfigOptions.h>
#include <query/TypeSystem.h>
#include <array/Metadata.h>
#include <system/SystemCatalog.h>
#include <system/Utils.h>
#include <smgr/io/Storage.h>
#include <array/Compressor.h>

using namespace std;

namespace scidb
{
ObjectNames::ObjectNames()
{}

ObjectNames::ObjectNames(const std::string &baseName):
    _baseName(baseName)
{
    addName(baseName);
}

ObjectNames::ObjectNames(const std::string &baseName, const NamesType &names):
    _names(names),
    _baseName(baseName)
{}

void ObjectNames::addName(const std::string &name)
{
    string trimmedName = name;
    trim(trimmedName);
    assert(trimmedName != "");

    if (hasNameAndAlias(name))
        return;

    _names[name] = set<string>();
}

void ObjectNames::addAlias(const std::string &alias, const std::string &name)
{
    if (!alias.empty())
    {
        string trimmedAlias = alias;
        trim(trimmedAlias);
        assert(trimmedAlias != "");

        string trimmedName = name;
        trim(trimmedName);
        assert(trimmedName != "");

        _names[name].insert(alias);
    }
}

void ObjectNames::addAlias(const std::string &alias)
{
    if (!alias.empty())
    {
        string trimmedAlias = alias;
        trim(trimmedAlias);
        assert(trimmedAlias != "");

        BOOST_FOREACH(const NamesPairType &nameAlias, _names)
        {
            _names[nameAlias.first].insert(alias);
        }
    }
}

bool ObjectNames::hasNameAndAlias(const std::string &name, const std::string &alias) const
{
    NamesType::const_iterator nameIt = _names.find(name);

    if (nameIt != _names.end())
    {
        if (alias.empty())
            return true;
        else
            return ( (*nameIt).second.find(alias) != (*nameIt).second.end() );
    }

    return false;
}

const ObjectNames::NamesType& ObjectNames::getNamesAndAliases() const
{
    return _names;
}

const std::string& ObjectNames::getBaseName() const
{
    return _baseName;
}

bool ObjectNames::operator==(const ObjectNames &o) const
{
    return _names == o._names;
}

std::ostream& operator<<(std::ostream& stream,const ObjectNames::NamesType::value_type& pair)
{
    stream << pair.first;
    return insertRange(stream,pair.second,", ");
}

std::ostream& operator<<(std::ostream& stream, const ObjectNames::NamesType &ob)
{
    return insertRange(stream,ob,", ");
}

void printNames (std::ostream& stream, const ObjectNames::NamesType &ob)
{
    for (ObjectNames::NamesType::const_iterator nameIt = ob.begin(); nameIt != ob.end(); ++nameIt)
    {
        if (nameIt != ob.begin())
        {
            stream << ", ";
        }
        stream << (*nameIt).first;
    }
}

/*
 * Class DimensionVector
 */

DimensionVector& DimensionVector::operator+= (const DimensionVector& rhs)
{
    if (isEmpty())
    {
        _data = rhs._data;
    }
    else
    if (!rhs.isEmpty())
    {
        assert(numDimensions() == rhs.numDimensions());

        for (size_t i=0, n=numDimensions(); i!=n; ++i)
        {
            _data[i] += rhs._data[i];
        }
    }

    return *this;
}

DimensionVector& DimensionVector::operator-= (const DimensionVector& rhs)
{
    if (!isEmpty() && !rhs.isEmpty())
    {
        assert(numDimensions() == rhs.numDimensions());

        for (size_t i=0, n=numDimensions(); i!=n; ++i)
        {
            _data[i] -= rhs._data[i];
        }
    }

    return *this;
}
/**
 * Retrieve a human-readable description.
 * Append a human-readable description of this onto str. Description takes up
 * one or more lines. Append indent spacer characters to the beginning of
 * each line.
 * @param[out] str buffer to write to
 * @param[in] indent number of spacer characters to start every line with.
 */
void DimensionVector::toString (std::ostringstream &str, int indent) const
{
    if (indent > 0)
    {
        str << std::string(indent,' ');
    }

    if (isEmpty())
    {
        str << "[empty]";
    }
    else
    {
        str << '[';
        insertRange(str,_data,' ');
        str << ']';
    }
}

/*
 * Class ArrayDesc
 */
ArrayDesc::ArrayDesc() :
    _arrId(0),
    _uAId(0),
    _versionId(0),
    _bitmapAttr(NULL),
    _flags(0),
    _ps(psUndefined)
{}

ArrayDesc::ArrayDesc(const std::string &name,
                     const Attributes& attributes,
                     const Dimensions &dimensions,
                     int32_t flags) :
    _arrId(0),
    _uAId(0),
    _versionId(0),
    _name(name),
    _attributes(attributes),
    _dimensions(dimensions),
    _flags(flags),
    _ps(psUndefined)
{
    locateBitmapAttribute();
    initializeDimensions();
}

ArrayDesc::ArrayDesc(ArrayID arrId, ArrayUAID uAId, VersionID vId,
                     const std::string &name,
                     const Attributes& attributes,
                     const Dimensions &dimensions,
                     int32_t flags) :
    _arrId(arrId),
    _uAId(uAId),
    _versionId(vId),
    _name(name),
    _attributes(attributes),
    _dimensions(dimensions),
    _flags(flags),
    _ps(psUndefined)
{
    //either both 0 or not...
    assert(arrId == 0 || uAId != 0);

    locateBitmapAttribute();
    initializeDimensions();
}

ArrayDesc::ArrayDesc(ArrayDesc const& other) :
    _arrId(other._arrId),
    _uAId(other._uAId),
    _versionId(other._versionId),
    _name(other._name),
    _attributes(other._attributes),
    _attributesWithoutBitmap(other._attributesWithoutBitmap),
    _dimensions(other._dimensions),
    _bitmapAttr(other._bitmapAttr != NULL ? &_attributes[other._bitmapAttr->getId()] : NULL),
    _flags(other._flags),
    _ps(other._ps)
{
    initializeDimensions();
}

bool ArrayDesc::operator ==(ArrayDesc const& other) const
{
    return
        _name == other._name &&
        _attributes == other._attributes &&
        _dimensions == other._dimensions &&
        _flags == other._flags;
}

ArrayDesc& ArrayDesc::operator = (ArrayDesc const& other)
{
    _arrId = other._arrId;
    _uAId = other._uAId;
    _versionId = other._versionId;
    _name = other._name;
    _attributes = other._attributes;
    _attributesWithoutBitmap = other._attributesWithoutBitmap;
    _dimensions = other._dimensions;
    _bitmapAttr = (other._bitmapAttr != NULL) ? &_attributes[other._bitmapAttr->getId()] : NULL;
    _flags = other._flags;
    initializeDimensions();
    _ps = other._ps;
    return *this;
}

bool ArrayDesc::isNameVersioned(std::string const& name)
{
    if (name.empty())
    {
        throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "calling isNameVersioned on an empty string";
    }

    size_t const locationOfAt = name.find('@');
    size_t const locationOfColon = name.find(':');
    return locationOfAt > 0 && locationOfAt < name.size() && locationOfColon == std::string::npos;
}

bool ArrayDesc::isNameUnversioned(std::string const& name)
{
    if (name.empty())
    {
        throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "calling isNameUnversioned on an empty string";
    }

    size_t const locationOfAt = name.find('@');
    size_t const locationOfColon = name.find(':');
    return locationOfAt == std::string::npos && locationOfColon == std::string::npos;
}

void ArrayDesc::initializeDimensions()
{
    Coordinate logicalChunkSize = 1;
    for (size_t i = 0, n = _dimensions.size(); i < n; i++) {
        _dimensions[i]._array = this;
        Coordinate chunkLength = _dimensions[i].getChunkInterval();
        Coordinate t = chunkLength + _dimensions[i].getChunkOverlap();
        if ( t < chunkLength ) //overflow check
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_METADATA, SCIDB_LE_LOGICAL_CHUNK_SIZE_TOO_LARGE);
        }
        chunkLength = t;
        t = chunkLength + _dimensions[i].getChunkOverlap();
        if ( t < chunkLength) //ooverflow check again
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_METADATA, SCIDB_LE_LOGICAL_CHUNK_SIZE_TOO_LARGE);
        }

        t = logicalChunkSize * chunkLength;
        if (chunkLength != 0 && t / chunkLength != logicalChunkSize) //overflow check again
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_METADATA, SCIDB_LE_LOGICAL_CHUNK_SIZE_TOO_LARGE);
        }
        logicalChunkSize = t;
    }
}

void ArrayDesc::trim()
{
    for (size_t i = 0, n = _dimensions.size(); i < n; i++) {
        DimensionDesc& dim = _dimensions[i];
        if (dim._startMin == MIN_COORDINATE && dim._currStart != MAX_COORDINATE) {
            dim._startMin = dim._currStart;
        }
        if (dim._endMax == MAX_COORDINATE && dim._currEnd != MIN_COORDINATE) {
            dim._endMax = (dim._startMin + (dim._currEnd - dim._startMin + dim._chunkInterval) / dim._chunkInterval * dim._chunkInterval + dim._chunkOverlap - 1);
        }
    }
}

uint64_t ArrayDesc::getHashedChunkNumber(Coordinates const& pos) const
{
    Dimensions const& dims = _dimensions;
    uint64_t no = 0;
    /// The goal here is to produce a good hash function without using array
    /// dimension sizes (which can be changed in case of unboundary arrays)
    for (size_t i = 0, n = pos.size(); i < n; i++)
    {
        // 1013 is prime number close to 1024. 1024*1024 is assumed to be optimal chunk size for 2-d array.
        // For 1-d arrays value of this constant is not important, because we are multiplying it on 0.
        // 3-d arrays and arrays with more dimensions are less common and using prime number and XOR should provide
        // well enough (uniform) mixing of bits.
        no = (no * 1013) ^ ((pos[i] - dims[i].getStartMin()) / dims[i].getChunkInterval());
    }
    return no;
}

ssize_t ArrayDesc::findDimension(const std::string& name, const std::string& alias) const
{
    const ssize_t N_DIMS = _dimensions.size();
    for (ssize_t i = 0; i < N_DIMS; ++i) {
        if (_dimensions[i].hasNameAndAlias(name, alias)) {
            return i;
        }
    }
    return -1;
}

bool ArrayDesc::contains(Coordinates const& pos) const
{
    Dimensions const& dims = _dimensions;
    for (size_t i = 0, n = pos.size(); i < n; i++) {
        if (pos[i] < dims[i].getStartMin() || pos[i] > dims[i].getEndMax()) {
            return false;
        }
    }
    return true;
}

void ArrayDesc::getChunkPositionFor(Coordinates& pos) const
{
    Dimensions const& dims = _dimensions;
    for (size_t i = 0, n = pos.size(); i < n; i++) {
        if ( dims[i].getChunkInterval() != 0) {
            Coordinate diff = (pos[i] - dims[i].getStartMin()) % dims[i].getChunkInterval();

            // The code below ensures the correctness of this code, in case pos[i] < dims[i].getStartMin().
            // Example:
            //   - Suppose dimStart=0, chunkInterval=5. All chunkPos should be a multiple of 5.
            //   - Given pos[i]=-9, we desire to reduce it to -10.
            //   - The calculated diff = -4.
            //   - The step below changes diff to a non-negative number of 1, bedore using it to decrease pos[i].
            if (diff < 0) {
                diff += dims[i].getChunkInterval();
            }

            pos[i] -= diff;
        }
    }
}

bool ArrayDesc::isAChunkPosition(Coordinates const& pos) const
{
    Coordinates chunkPos = pos;
    getChunkPositionFor(chunkPos);
    return coordinatesCompare(pos, chunkPos) == 0;
}

bool ArrayDesc::isCellPosInChunk(Coordinates const& cellPos, Coordinates const& chunkPos) const
{
    Coordinates chunkPosForCell = cellPos;
    getChunkPositionFor(chunkPosForCell);
    return coordinatesCompare(chunkPosForCell, chunkPos) == 0;
}

void ArrayDesc::getChunkBoundaries(Coordinates const& chunkPosition,
                                   bool withOverlap,
                                   Coordinates& lowerBound,
                                   Coordinates& upperBound) const
{
#ifndef NDEBUG
    do
    {
        Coordinates alignedChunkPosition = chunkPosition;
        getChunkPositionFor(alignedChunkPosition);
        SCIDB_ASSERT(alignedChunkPosition == chunkPosition);
    }
    while(false);
#endif /* NDEBUG */
    Dimensions const& d = getDimensions();
    Dimensions::size_type const n = d.size();
    SCIDB_ASSERT(n == chunkPosition.size());
    lowerBound = chunkPosition;
    upperBound = chunkPosition;
    for (size_t i = 0; i < n; i++) {
        upperBound[i] += d[i].getChunkInterval() - 1;
    }
    if (withOverlap) {
        for (size_t i = 0; i < n; i++) {
            lowerBound[i] -= d[i].getChunkOverlap();
            upperBound[i] += d[i].getChunkOverlap();
        }
    }
    for (size_t i = 0; i < n; ++i) {
        lowerBound[i] = std::max(lowerBound[i], d[i].getStartMin());
        upperBound[i] = std::min(upperBound[i], d[i].getEndMax());
    }
}

void ArrayDesc::locateBitmapAttribute()
{
    _bitmapAttr = NULL;
    _attributesWithoutBitmap = _attributes;
    for (size_t i = 0, n = _attributes.size(); i < n; i++) {
        if (_attributes[i].getType() ==  TID_INDICATOR) {
            _bitmapAttr = &_attributes[i];
            _attributesWithoutBitmap.erase(_attributesWithoutBitmap.begin() + i);
        }
    }
}

uint64_t ArrayDesc::getSize() const
{
    uint64_t size = 1;
    uint64_t max = std::numeric_limits<uint64_t>::max();
    for (size_t i = 0, n = _dimensions.size(); i < n; i++)
    {
        uint64_t length = _dimensions[i].getLength();
        //check for uint64_t overflow
        if (length >= INFINITE_LENGTH || length > max / size)
        {
            return INFINITE_LENGTH;
        }
        size *= length;
    }
    return size;
}

uint64_t ArrayDesc::getCurrSize() const
{
    uint64_t size = 1;
    for (size_t i = 0, n = _dimensions.size(); i < n; i++) {
        uint64_t length = _dimensions[i].getCurrLength();
        if (length == INFINITE_LENGTH) {
            return INFINITE_LENGTH;
        }
        size *= length;
    }
    return size;
}

uint64_t ArrayDesc::getUsedSpace() const
{
    uint64_t nElems = getCurrSize();
    if (nElems == INFINITE_LENGTH) {
        return INFINITE_LENGTH;
    }
    size_t totalBitSize = 0;
    for (size_t i = 0, n = _attributes.size(); i < n; i++) {
        totalBitSize +=  TypeLibrary::getType(_attributes[i].getType()).bitSize();
        if (_attributes[i].isNullable()) {
            totalBitSize += 1;
        }
    }
    return (nElems*totalBitSize + 7)/8;
}

uint64_t ArrayDesc::getNumberOfChunks() const
{
    uint64_t nChunks = 1;
    for (size_t i = 0, n = _dimensions.size(); i < n; i++) {
        uint64_t length = _dimensions[i].getLength();
        if (length == INFINITE_LENGTH) {
            return INFINITE_LENGTH;
        }
        nChunks *= (length + _dimensions[i].getChunkInterval() - 1) / _dimensions[i].getChunkInterval();
    }
    return nChunks*_attributes.size();
}

void ArrayDesc::cutOverlap()
{
    for (size_t i = 0, n = _dimensions.size(); i < n; i++) {
        _dimensions[i]._chunkOverlap = 0;
    }
}

bool ArrayDesc::hasOverlap() const
{
    for (size_t i = 0, n = _dimensions.size(); i < n; i++) {
        if (_dimensions[i].getChunkOverlap() != 0) {
            return true;
        }
    }
    return false;
}

Dimensions ArrayDesc::grabDimensions(VersionID version) const
{
    Dimensions dims(_dimensions.size());
    for (size_t i = 0; i < dims.size(); i++) {
        DimensionDesc const& dim = _dimensions[i];
        dims[i] = dim;
    }
    return dims;
}

void ArrayDesc::addAlias(const std::string &alias)
{
    BOOST_FOREACH(AttributeDesc &attr, _attributes)
    {
        attr.addAlias(alias);
    }

    BOOST_FOREACH(AttributeDesc &attr, _attributesWithoutBitmap)
    {
        attr.addAlias(alias);
    }

    BOOST_FOREACH(DimensionDesc &dim, _dimensions)
    {
        dim.addAlias(alias);
    }
}

bool ArrayDesc::coordsAreAtChunkStart(Coordinates const& coords) const
{
    if (coords.size() != _dimensions.size())
        throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_DIMENSIONS_MISMATCH);

    for (size_t i = 0; i < coords.size(); i++ )
    {
       if ( coords[i] < _dimensions[i].getStartMin() ||
            coords[i] > _dimensions[i].getEndMax() ||
            (coords[i] - _dimensions[i].getStartMin()) % _dimensions[i].getChunkInterval() != 0 )
       {
           return false;
       }
    }
    return true;
}

bool ArrayDesc::coordsAreAtChunkEnd(Coordinates const& coords) const
{
    if (coords.size() != _dimensions.size())
        throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_DIMENSIONS_MISMATCH);

    for (size_t i = 0; i < coords.size(); i++ )
    {
        if ( coords[i] != _dimensions[i].getEndMax() &&
             (coords[i] < _dimensions[i].getStartMin() ||
              coords[i] > _dimensions[i].getEndMax() ||
              (coords[i] + 1 - _dimensions[i].getStartMin()) % _dimensions[i].getChunkInterval() != 0 ))
        {
            return false;
        }
    }
    return true;
}

void ArrayDesc::addAttribute(AttributeDesc const& newAttribute)
{
    assert(newAttribute.getId() == _attributes.size());
    for (size_t i = 0; i< _dimensions.size(); i++)
    {
        if (_dimensions[i].getBaseName() == newAttribute.getName() || newAttribute.hasAlias(_dimensions[i].getBaseName()))
        {
            throw USER_EXCEPTION(SCIDB_SE_METADATA, SCIDB_LE_DUPLICATE_ATTRIBUTE_NAME) << newAttribute.getName();
        }
    }

    for (size_t i = 0; i < _attributes.size(); i++)
    {
        if (_attributes[i].getName() == newAttribute.getName())
        {
            throw USER_EXCEPTION(SCIDB_SE_METADATA, SCIDB_LE_DUPLICATE_ATTRIBUTE_NAME) << newAttribute.getName();
        }
    }
    _attributes.push_back(newAttribute);
    if (newAttribute.getType() == TID_INDICATOR)
    {
        assert(_bitmapAttr == NULL);
        _bitmapAttr = &_attributes[_attributes.size()-1];
    }
    else
    {
        _attributesWithoutBitmap.push_back(newAttribute);
    }
}

double ArrayDesc::getNumChunksAlongDimension(size_t dimension, Coordinate start, Coordinate end) const
{
    assert(dimension < _dimensions.size());
    if(start==MAX_COORDINATE && end ==MIN_COORDINATE)
    {
        start = _dimensions[dimension].getStartMin();
        end = _dimensions[dimension].getEndMax();
    }
    return ceil((end * 1.0 - start + 1.0) / _dimensions[dimension].getChunkInterval());
}

size_t getChunkNumberOfElements(Coordinates const& low, Coordinates const& high)
{
    size_t M = size_t(-1);
    size_t ret = 1;
    assert(low.size()==high.size());
    for (size_t i=0; i<low.size(); ++i) {
        assert(high[i] >= low[i]);
        size_t interval = high[i] - low[i] + 1;
        if (M/ret < interval) {
            throw SYSTEM_EXCEPTION(SCIDB_SE_METADATA, SCIDB_LE_LOGICAL_CHUNK_SIZE_TOO_LARGE);
        }
        ret *= interval;
    }
    return ret;
}

bool samePartitioning(ArrayDesc const& a1, ArrayDesc const& a2)
{
    Dimensions const& dims1 = a1.getDimensions();
    Dimensions const& dims2 = a2.getDimensions();

    // Same dimension count, or else some inferSchema() method failed to forbid this!
    SCIDB_ASSERT(dims1.size() == dims2.size());

    for (size_t i = 0; i < dims1.size(); ++i) {
        DimensionDesc const& dim1 = dims1[i];
        DimensionDesc const& dim2 = dims2[i];
        if (dim1.getChunkInterval() != dim2.getChunkInterval()) {
            return false;
        }
        if (dim1.getChunkOverlap() != dim2.getChunkOverlap()) {
            return false;
        }
    }

    return true;
}

void printSchema(std::ostream& stream,const ArrayDesc& ob)
{
#ifndef SCIDB_CLIENT
    if (Config::getInstance()->getOption<bool>(CONFIG_ARRAY_EMPTYABLE_BY_DEFAULT)) {
        if (ob.getEmptyBitmapAttribute() == NULL) {
            stream << "not empty ";
        }
    } else {
        if (ob.getEmptyBitmapAttribute() != NULL) {
            stream << "empty ";
        }
    }
#endif
    stream << ob.getName()
           << '<' << ob.getAttributes(true)
           << "> [";
    printSchema(stream, ob.getDimensions());
    stream << ']';
}

std::ostream& operator<<(std::ostream& stream,const ArrayDesc& ob)
{
#ifndef SCIDB_CLIENT
    if (Config::getInstance()->getOption<bool>(CONFIG_ARRAY_EMPTYABLE_BY_DEFAULT)) {
        if (ob.getEmptyBitmapAttribute() == NULL) {
            stream << "not empty ";
        }
    } else {
        if (ob.getEmptyBitmapAttribute() != NULL) {
            stream << "empty ";
        }
    }
#endif
    stream << ob.getName()
           << '<' << ob.getAttributes(true)
           << "> [" << ob.getDimensions() << ']';
    return stream;
}

/*
 * Class AttributeDesc
 */
AttributeDesc::AttributeDesc() :
    _id(0),
    _type( TypeId( TID_VOID)),
    _flags(0),
    _defaultCompressionMethod(0),
    _reserve(
#ifndef SCIDB_CLIENT
        Config::getInstance()->getOption<int>(CONFIG_CHUNK_RESERVE)
#else
        0
#endif
     ),
    _varSize(0)
{}

AttributeDesc::AttributeDesc(AttributeID id,
                             const std::string &name,
                             TypeId type,
                             int16_t flags,
                             uint16_t defaultCompressionMethod,
                             const std::set<std::string> &aliases,
                             int16_t reserve,
                             Value const* defaultValue,
                             const string &defaultValueExpr,
                             size_t varSize):
    _id(id),
    _name(name),
    _aliases(aliases),
    _type(type),
    _flags(flags | (type ==  TID_INDICATOR ? IS_EMPTY_INDICATOR : 0)),
    _defaultCompressionMethod(defaultCompressionMethod),
    _reserve(reserve),
    _varSize(varSize),
    _defaultValueExpr(defaultValueExpr)
{
    if (defaultValue != NULL) {
        _defaultValue = *defaultValue;
    } else {
        _defaultValue = Value(TypeLibrary::getType(type));
        if (flags & IS_NULLABLE) {
            _defaultValue.setNull();
        } else {
            _defaultValue = TypeLibrary::getDefaultValue(type);
        }
    }
    if(name == DEFAULT_EMPTY_TAG_ATTRIBUTE_NAME &&  (_flags & AttributeDesc::IS_EMPTY_INDICATOR) == 0)
    {
        throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "Empty tag attribute name misuse";
    }
    if(name != DEFAULT_EMPTY_TAG_ATTRIBUTE_NAME && (_flags & AttributeDesc::IS_EMPTY_INDICATOR))
    {
        throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "Empty tag attribute not named properly";
    }
}

AttributeDesc::AttributeDesc(AttributeID id, const std::string &name,  TypeId type, int16_t flags,
        uint16_t defaultCompressionMethod, const std::set<std::string> &aliases,
        Value const* defaultValue,
        const string &defaultValueExpr,
        size_t varSize) :
    _id(id),
    _name(name),
    _aliases(aliases),
    _type(type),
    _flags(flags | (type ==  TID_INDICATOR ? IS_EMPTY_INDICATOR : 0)),
    _defaultCompressionMethod(defaultCompressionMethod),
    _reserve(
#ifndef SCIDB_CLIENT
        Config::getInstance()->getOption<int>(CONFIG_CHUNK_RESERVE)
#else
        0
#endif
    ),
    _varSize(varSize),
    _defaultValueExpr(defaultValueExpr)
{
    if (defaultValue != NULL) {
        _defaultValue = *defaultValue;
    } else {
        _defaultValue = Value(TypeLibrary::getType(type));
        if (flags & IS_NULLABLE) {
            _defaultValue.setNull();
        } else {
            _defaultValue = TypeLibrary::getDefaultValue(type);
        }
    }
    if(name == DEFAULT_EMPTY_TAG_ATTRIBUTE_NAME &&  (_flags & AttributeDesc::IS_EMPTY_INDICATOR) == 0)
    {
        throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "Empty tag attribute name misuse";
    }
    if(name != DEFAULT_EMPTY_TAG_ATTRIBUTE_NAME && (_flags & AttributeDesc::IS_EMPTY_INDICATOR))
    {
        throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "Empty tag attribute not named properly";
    }
}

bool AttributeDesc::operator ==(AttributeDesc const& other) const
{
    return
        _id == other._id &&
        _name == other._name &&
        _aliases == other._aliases &&
        _type == other._type &&
        _flags == other._flags &&
        _defaultCompressionMethod == other._defaultCompressionMethod &&
        _reserve == other._reserve &&
        _defaultValue == other._defaultValue &&
        _varSize == other._varSize &&
        _defaultValueExpr == other._defaultValueExpr;
}

AttributeID AttributeDesc::getId() const
{
    return _id;
}

const std::string& AttributeDesc::getName() const
{
    return _name;
}

const std::set<std::string>& AttributeDesc::getAliases() const
{
    return _aliases;
}

void AttributeDesc::addAlias(const string& alias)
{
    string trimmedAlias = alias;
    trim(trimmedAlias);
    _aliases.insert(trimmedAlias);
}

bool AttributeDesc::hasAlias(const std::string& alias) const
{
    if (alias.empty())
        return true;
    else
        return (_aliases.find(alias) != _aliases.end());
}

TypeId AttributeDesc::getType() const
{
    return _type;
}

int AttributeDesc::getFlags() const
{
    return _flags;
}

bool AttributeDesc::isNullable() const
{
    return (_flags & IS_NULLABLE) != 0;
}

bool AttributeDesc::isEmptyIndicator() const
{
    return (_flags & IS_EMPTY_INDICATOR) != 0;
}

uint16_t AttributeDesc::getDefaultCompressionMethod() const
{
    return _defaultCompressionMethod;
}

Value const& AttributeDesc::getDefaultValue() const
{
    return _defaultValue;
}

int16_t AttributeDesc::getReserve() const
{
    return _reserve;
}

size_t AttributeDesc::getSize() const
{
    Type const& type = TypeLibrary::getType(_type);
    return type.byteSize() > 0 ? type.byteSize() : getVarSize();
}

size_t AttributeDesc::getVarSize() const
{
    return _varSize;
}

const std::string& AttributeDesc::getDefaultValueExpr() const
{
    return _defaultValueExpr;
}

/**
 * Retrieve a human-readable description.
 * Append a human-readable description of this onto str. Description takes up
 * one or more lines. Append indent spacer characters to the beginning of
 * each line. Call toString on interesting children. Terminate with newline.
 * @param[out] str buffer to write to
 * @param[in] indent number of spacer characters to start every line with.
 */
void AttributeDesc::toString(std::ostringstream &str, int indent) const
{
    if (indent > 0)
    {
        str<<std::string(indent,' ');
    }

    str<< "[attDesc] id " << _id
       << " name " << _name
       << " aliases {";

    BOOST_FOREACH(const std::string& alias,_aliases)
    {
        str << _name << "." << alias << ", ";
    }

    str<< "} type " << _type
       << " flags " << _flags
       << " compression " << _defaultCompressionMethod
       << " reserve " << _reserve
       << " default " << ValueToString(_type,_defaultValue);
}

std::ostream& operator<<(std::ostream& stream,const Attributes& atts)
{
    return insertRange(stream,atts,',');
}

std::ostream& operator<<(std::ostream& stream, const AttributeDesc& att)
{
    //don't print NOT NULL because it default behaviour
    stream << att.getName() << ':' << att.getType()
           << (att.getFlags() & AttributeDesc::IS_NULLABLE ? " NULL" : "");
    try
    {
        if (!isDefaultFor(att.getDefaultValue(),att.getType()))
        {
            stream << " DEFAULT " << ValueToString(att.getType(), att.getDefaultValue());
        }
    }
    catch (const SystemException &e)
    {
        if (e.getLongErrorCode() != SCIDB_LE_TYPE_NOT_REGISTERED)
        {
            e.raise();
        }

        stream << " DEFAULT UNKNOWN";
    }
    if (att.getDefaultCompressionMethod() != CompressorFactory::NO_COMPRESSION)
    {
        stream << " COMPRESSION '" << CompressorFactory::getInstance().getCompressors()[att.getDefaultCompressionMethod()]->getName() << "'";
    }
    return stream;
}

/*
 * Class DimensionDesc
 */

DimensionDesc::DimensionDesc() :
    ObjectNames(),

    _startMin(0),
    _currStart(0),
    _currEnd(0),
    _endMax(0),

    _chunkInterval(0),
    _chunkOverlap(0)
{
    validate();
}

DimensionDesc::DimensionDesc(const std::string &name, Coordinate start, Coordinate end, int64_t chunkInterval,
                             int64_t chunkOverlap) :
    ObjectNames(name),

    _startMin(start),
    _currStart(MAX_COORDINATE),
    _currEnd(MIN_COORDINATE),
    _endMax(end),

    _chunkInterval(chunkInterval),
    _chunkOverlap(chunkOverlap)
{
    validate();
}

DimensionDesc::DimensionDesc(const std::string &baseName, const NamesType &names, Coordinate start, Coordinate end,
                             int64_t chunkInterval, int64_t chunkOverlap) :
    ObjectNames(baseName, names),

    _startMin(start),
    _currStart(MAX_COORDINATE),
    _currEnd(MIN_COORDINATE),
    _endMax(end),

    _chunkInterval(chunkInterval),
    _chunkOverlap(chunkOverlap)
{
    validate();
}

DimensionDesc::DimensionDesc(const std::string &name, Coordinate startMin, Coordinate currStart, Coordinate currEnd,
                             Coordinate endMax, int64_t chunkInterval, int64_t chunkOverlap) :
    ObjectNames(name),

    _startMin(startMin),
    _currStart(currStart),
    _currEnd(currEnd),
    _endMax(endMax),
    _chunkInterval(chunkInterval),
    _chunkOverlap(chunkOverlap)
{
    validate();
}

DimensionDesc::DimensionDesc(const std::string &baseName, const NamesType &names, Coordinate startMin,
                             Coordinate currStart, Coordinate currEnd, Coordinate endMax, int64_t chunkInterval, int64_t chunkOverlap) :
    ObjectNames(baseName, names),

    _startMin(startMin),
    _currStart(currStart),
    _currEnd(currEnd),
    _endMax(endMax),
    _chunkInterval(chunkInterval),
    _chunkOverlap(chunkOverlap)
{
    validate();
}

bool DimensionDesc::operator == (DimensionDesc const& other) const
{
    return
        _names == other._names &&
        _startMin == other._startMin &&
        _endMax == other._endMax &&
        _chunkInterval == other._chunkInterval &&
        _chunkOverlap == other._chunkOverlap;
}

uint64_t DimensionDesc::getLength() const
{
    return _startMin == MIN_COORDINATE || _endMax == MAX_COORDINATE ? INFINITE_LENGTH : (_endMax - _startMin + 1);
}

uint64_t DimensionDesc::getCurrLength() const
{
    Coordinate low = _startMin;
    Coordinate high = _endMax;
#ifndef SCIDB_CLIENT
    if (_startMin == MIN_COORDINATE || _endMax == MAX_COORDINATE) {
        if (_array->getId() != 0) {
            size_t index = this - &_array->_dimensions[0];
            if (_startMin == MIN_COORDINATE) {
                low = SystemCatalog::getInstance()->getLowBoundary(_array->getId())[index];
            }
            if (_endMax == MAX_COORDINATE) {
                high = SystemCatalog::getInstance()->getHighBoundary(_array->getId())[index];
            }
        } else {
            low = _currStart;
            high = _currEnd;
        }
    }
#endif
    /*
     * check for empty array - according to informal agreement,
     * high boundary for empty array is MAX_COORDINATE
     */
    if (low == MAX_COORDINATE || high == MIN_COORDINATE) {
        return 0;
    } else {
        return high - low + 1;
    }
}

/**
 * Retrieve a human-readable description.
 * Append a human-readable description of this onto str. Description takes up
 * one or more lines. Append indent spacer characters to the beginning of
 * each line. Call toString on interesting children. Terminate with newline.
 * @param[out] str buffer to write to
 * @param[in] indent number of spacer characters to start every line with.
 */
void DimensionDesc::toString (std::ostringstream &str,int indent) const
{
    if (indent > 0)
    {
        str<<std::string(indent,' ');
    }

    str<<"[dimDesc] names "<<_names
       <<" startMin "<<_startMin
       <<" currStart "<<_currStart
       <<" currEnd "<<_currEnd
       <<" endMax "<<_endMax
       <<" chnkInterval "<<_chunkInterval
       <<" chnkOverlap "<<_chunkOverlap
       << "\n";
}

void DimensionDesc::validate() const
{
    if (_startMin > _endMax)
    {
        throw SYSTEM_EXCEPTION(SCIDB_SE_SYNTAX, SCIDB_LE_HIGH_SHOULDNT_BE_LESS_LOW);
    }
}

void printSchema(std::ostream& stream,const Dimensions& dims)
{
    for (size_t i=0,n=dims.size(); i<n; i++)
    {
        printSchema(stream, dims[i]);
        if (i != n-1)
        {
            stream << ',';
        }
    }
}

std::ostream& operator<<(std::ostream& stream,const Dimensions& dims)
{
    return insertRange(stream,dims,',');
}

std::ostream& operator<<(std::ostream& stream,const DimensionDesc& dim)
{
    Coordinate start = dim.getStartMin();
    stringstream ssstart;
    ssstart << start;

    Coordinate end = dim.getEndMax();
    stringstream ssend;
    ssend << end;

    stream << dim.getNamesAndAliases() << '=' << (start == MIN_COORDINATE ? "*" : ssstart.str()) << ':'
           << (end == MAX_COORDINATE ? "*" : ssend.str()) << ","
           << dim.getChunkInterval() << "," << dim.getChunkOverlap();
    return stream;
}

void printSchema(std::ostream& stream,const DimensionDesc& dim)
{
    Coordinate start = dim.getStartMin();
    stringstream ssstart;
    ssstart << start;

    Coordinate end = dim.getEndMax();
    stringstream ssend;
    ssend << end;

    printNames(stream, dim.getNamesAndAliases());
    stream << '=' << (start == MIN_COORDINATE ? "*" : ssstart.str()) << ':'
           << (end == MAX_COORDINATE ? "*" : ssend.str()) << ","
           << dim.getChunkInterval() << "," << dim.getChunkOverlap();
}

void printDimNames(std::ostream& os, Dimensions const& dims)
{
    const size_t N = dims.size();
    for (size_t i = 0; i < N; ++i) {
        if (i) {
            // Alias separator used by printNames is a comma, so use semi-colon.
            os << ';';
        }
        printNames(os, dims[i].getNamesAndAliases());
    }
}

/*
 * Class InstanceDesc
 */
InstanceDesc::InstanceDesc() :
    _instance_id(0),
    _port(0),
    _online(~0)
{}

InstanceDesc::InstanceDesc(const std::string &host, uint16_t port, const std::string &path) :
        _host(host),
        _port(port),
        _online(~0)
{
     boost::filesystem::path p(path);
     _path = p.normalize().string();
}

InstanceDesc::InstanceDesc(uint64_t instance_id, const std::string &host,
                           uint16_t port, uint64_t online, const std::string &path) :
        _instance_id(instance_id),
        _host(host),
        _port(port),
        _online(online)
{
    boost::filesystem::path p(path);
    _path = p.normalize().string();
}

std::ostream& operator<<(std::ostream& stream,const InstanceDesc& instance)
{
    stream << "instance { id = " << instance.getInstanceId()
           << ", host = " << instance.getHost()
           << ", port = " << instance.getPort()
           << ", has been on-line since " << instance.getOnlineSince()
           << ", path = " << instance.getPath();
    return stream;
}

} // namespace
