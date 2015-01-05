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
 * @file TypeSystem.h
 *
 * @author roman.simakov@gmail.com
 *
 * @brief Type system of SciDB base of inline version of Value and looper
 * expression evaluator.
 */

#ifndef TYPESYSTEM_H_
#define TYPESYSTEM_H_

#include <stdlib.h>
#include <stdint.h>
#include <vector>
#include <map>
#include <boost/shared_ptr.hpp>
#include <assert.h>
#include <cstring>
#include <cmath>
#include "limits.h"

#include "system/Constants.h"
#include "system/Exceptions.h"
#include "util/Singleton.h"
#include "util/Mutex.h"
#include "util/StringUtil.h"
#include "util/PluginObjects.h"
#include <array/RLE.h>

namespace scidb
{

class LogicalExpression;

const size_t STRIDE_SIZE = 64*KiB;

// Type identifier
typedef std::string TypeId;

const char TID_INVALID[] = "InvalidType";
const char TID_INDICATOR[] = "indicator";
const char TID_CHAR[] = "char";
const char TID_INT8[] = "int8";
const char TID_INT16[] = "int16";
const char TID_INT32[] = "int32";
const char TID_INT64[] = "int64";
const char TID_UINT8[] = "uint8";
const char TID_UINT16[] = "uint16";
const char TID_UINT32[] = "uint32";
const char TID_UINT64[] = "uint64";
const char TID_FLOAT[] = "float";
const char TID_DOUBLE[] = "double";
const char TID_BOOL[] = "bool";
const char TID_STRING[] = "string";
const char TID_DATETIME[] = "datetime";
const char TID_DATETIMETZ[] = "datetimetz";
const char TID_VOID[] = "void";
const char TID_BINARY[] = "binary";

/**
 * TypeEnum is provided to have efficient type comparison.
 * In an inner loop, comparing TypeId is much slower than comparing TypeEnum.
 */
enum TypeEnum
{
    TE_INVALID = -1,
    TE_INDICATOR = 0,
    TE_CHAR,
    TE_INT8,
    TE_INT16,
    TE_INT32,
    TE_INT64,
    TE_UINT8,
    TE_UINT16,
    TE_UINT32,
    TE_UINT64,
    TE_FLOAT,
    TE_DOUBLE,
    TE_BOOL,
    TE_STRING,
    TE_DATETIME,
    TE_DATETIMETZ,
    TE_VOID,
    TE_BINARY
};

/**
 * Convert TypeId to TypeEnum.
 */
inline TypeEnum typeId2TypeEnum(TypeId tid, bool noThrow = false)
{
    if (tid==TID_INDICATOR) {
        return TE_INDICATOR;
    }
    else if (tid==TID_CHAR) {
        return TE_CHAR;
    }
    else if (tid==TID_INT8) {
        return TE_INT8;
    }
    else if (tid==TID_INT16) {
        return TE_INT16;
    }
    else if (tid==TID_INT32) {
        return TE_INT32;
    }
    else if (tid==TID_INT64) {
        return TE_INT64;
    }
    else if (tid==TID_UINT8) {
        return TE_UINT8;
    }
    else if (tid==TID_UINT16) {
        return TE_UINT16;
    }
    else if (tid==TID_UINT32) {
        return TE_UINT32;
    }
    else if (tid==TID_UINT64) {
        return TE_UINT64;
    }
    else if (tid==TID_FLOAT) {
        return TE_FLOAT;
    }
    else if (tid==TID_DOUBLE) {
        return TE_DOUBLE;
    }
    else if (tid==TID_BOOL) {
        return TE_BOOL;
    }
    else if (tid==TID_STRING) {
        return TE_STRING;
    }
    else if (tid==TID_DATETIME) {
        return TE_DATETIME;
    }
    else if (tid==TID_DATETIMETZ) {
        return TE_DATETIMETZ;
    }
    else if (tid==TID_VOID) {
        return TE_VOID;
    }
    else if (tid==TID_BINARY) {
        return TE_BINARY;
    }
    else if (tid==TID_INVALID) {
        return TE_INVALID;
    }

    // Probably a user-defined type of some kind.  XXX We need to do a
    // better job of supporting those here!
    if (noThrow) {
        return TE_INVALID;
    }
    throw USER_EXCEPTION(SCIDB_SE_TYPE, SCIDB_LE_TYPE_NOT_REGISTERED) << tid;
}

/**
 * Convert TypeEnum to TypeId.
 */
inline TypeId typeEnum2TypeId(TypeEnum te)
{
    switch (te)
    {
    case TE_INDICATOR:
        return TID_INDICATOR;
    case TE_CHAR:
        return TID_CHAR;
    case TE_INT8:
        return TID_INT8;
    case TE_INT16:
        return TID_INT16;
    case TE_INT32:
        return TID_INT32;
    case TE_INT64:
        return TID_INT64;
    case TE_UINT8:
        return TID_UINT8;
    case TE_UINT16:
        return TID_UINT16;
    case TE_UINT32:
        return TID_UINT32;
    case TE_UINT64:
        return TID_UINT64;
    case TE_FLOAT:
        return TID_FLOAT;
    case TE_DOUBLE:
        return TID_DOUBLE;
    case TE_BOOL:
        return TID_BOOL;
    case TE_STRING:
        return TID_STRING;
    case TE_DATETIME:
        return TID_DATETIME;
    case TE_DATETIMETZ:
        return TID_DATETIMETZ;
    case TE_VOID:
        return TID_VOID;
    case TE_BINARY:
        return TID_BINARY;
    default:
        assert(false);
    }
    return TID_VOID;
}

template<typename Type_tt>
TypeId type2TypeId()
{
    assert(false);
    throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_UNREACHABLE_CODE)
    << typeid(Type_tt).name();
    return TID_VOID;
}

template<>  TypeId type2TypeId<char>();
template<>  TypeId type2TypeId<int8_t>();
template<>  TypeId type2TypeId<int16_t>();
template<>  TypeId type2TypeId<int32_t>();
template<>  TypeId type2TypeId<int64_t>();
template<>  TypeId type2TypeId<uint8_t>();
template<>  TypeId type2TypeId<uint16_t>();
template<>  TypeId type2TypeId<uint32_t>();
template<>  TypeId type2TypeId<uint64_t>();
template<>  TypeId type2TypeId<float>();
template<>  TypeId type2TypeId<double>();

inline bool IS_VARLEN(const TypeId& tid)
{
    return tid == TID_STRING || tid == TID_BINARY;
}

inline bool IS_REAL(const TypeId& tid)
{
    return tid == TID_FLOAT || tid == TID_DOUBLE;
}

inline bool IS_INTEGRAL(const TypeId& tid)
{
    return tid == TID_INT8
        || tid == TID_INT16
        || tid == TID_INT32
        || tid == TID_INT64
        || tid == TID_UINT8
        || tid == TID_UINT16
        || tid == TID_UINT32
        || tid == TID_UINT64;
}

inline bool IS_NUMERIC(const TypeId& tid)
{
    return IS_INTEGRAL(tid) || IS_REAL(tid);
}

inline bool IS_SIGNED(const TypeId& tid)
{
    return tid == TID_INT8
        || tid == TID_INT16
        || tid == TID_INT32
        || tid == TID_INT64
        || IS_REAL(tid);
}

const size_t STRFTIME_BUF_LEN = 256;
const char* const DEFAULT_STRFTIME_FORMAT = "%F %T";

/**
 * Class to present description of a type of a value.
 * The class knows about size of data.
 * Objects of this class can be compared with each other.
 */
class Type
{
private:
    TypeId _typeId;     /**< type identificator */
    uint32_t _bitSize;  /**< bit size is used in storage manager. 0 - for variable size data */
    TypeId _baseType;

public:

    Type(const TypeId& typeId, const uint32_t bitSize, const TypeId baseType = TID_VOID):
    _typeId(typeId), _bitSize(bitSize), _baseType(baseType)
    {
    }

    Type(): _typeId(TypeId(TID_VOID)), _bitSize(0), _baseType(TypeId(TID_VOID))
    {
    }

    /**
     * @return type id
     */
    TypeId typeId() const {
        return _typeId;
    }

    /**
     * @return base type id
     */
    TypeId baseType() const {
        return _baseType;
    }


     /**
     * Check if this supertype is base type for subtype
     * return true if subtype is direct or indirect subtype of supertype
     */
   static bool isSubtype(TypeId const& subtype, TypeId const& supertype);

    /**
     * Check if this type is subtype of the specified type
     * return true if this type is direct or indirect subtype of the specified type
     */
    bool isSubtypeOf(TypeId const& type) const {
        return _baseType != TID_VOID && (_baseType == type || isSubtype(_baseType, type));
    }

    /**
     * @return a name of type
     */
    const std::string& name() const
    {
        return _typeId;
    }

    /**
     * @return size of data in bits
     */
    uint32_t bitSize() const {
        return _bitSize;
    }

    /**
     * @return size of data in bytes
     */
    uint32_t byteSize() const {
        return (_bitSize + 7) >> 3;
    }

    bool variableSize() const {
        return _bitSize == 0;
    }

    bool isVoid() const {
                return (0 ==  _typeId.compare(TID_VOID));
        }

    bool operator<(const Type& ob) const {
        return (0 > _typeId.compare(ob.typeId()));
    }

    bool operator==(const Type& ob) const {
        return (0 == _typeId.compare(ob.typeId()));
    }

    bool operator==(const std::string& ob) const {
        return (0 == _typeId.compare(ob));
    }

    bool operator!=(const Type& ob) const {
        return (0 != _typeId.compare(ob.typeId()));
    }

    bool operator!=(const std::string& ob) const {
        return (0 != _typeId.compare(ob));
    }
};

struct Type_less {
    bool operator()(const Type &f1, const Type &f2) const
    {
        return ( f1 < f2 );
    }
};

std::ostream& operator<<(std::ostream& stream, const Type& ob );
std::ostream& operator<<(std::ostream& stream, const std::vector<Type>& ob );
std::ostream& operator<<(std::ostream& stream, const std::vector<TypeId>& ob );

/**
 * The Value class is data storage for type Type. It has only data, as the
 * type descriptor will be stored separately.
 *
 * The main goal of this class implementing is keep all methods of it inline.
 */
#define BUILTIN_METHODS(TYPE_NAME, METHOD_NAME) \
    TYPE_NAME get##METHOD_NAME() const { return *static_cast<TYPE_NAME*>((void*)&_builtinBuf); } \
    void set##METHOD_NAME(TYPE_NAME val) { _missingReason = MR_DATUM; _size = sizeof(TYPE_NAME); *static_cast<TYPE_NAME*>((void*)&_builtinBuf) = val; }

class Value
{
private:
    typedef int64_t builtinbuf_t ;

    /**
     *  _missingReason is an overloaded element. It contains information
     *  related to the 'missing' code for data, but also details about
     *  cases where the data is stored in a buffer allocated outside the
     *  class instance, and merely linked to it here.
     *
     * _missingReason >= 0 means value is NULL and _missingReason has a code
     *  of reason.
     * _missingReason == MR_DATUM (-1) means value is not NULL and data()
     *  returns relevant buffer with value.
     * _missingReason == MR_VECTOR (-2) means _data contains linked vector data
     *  that should not be freed. Methods changing *data() are disallowed.
     * _missingReason == MR_TILE (-3) means _tile is an RLEPayload.
     */
    int32_t _missingReason;

    enum MissingReason {
        MR_NULL = 0,
        MR_DATUM = -1,
        MR_VECTOR = -2,
        MR_TILE = -3
    };

    /*
    ** For variable length data, the size of this data in bytes.
    */
    uint32_t _size;

    /*
    ** A union type. If _missingReason is MR_VECTOR, or the _size > 8,
    ** then the data is found in a buffer pointed to out of
    ** _data. Otherwise, the data associated with this instance of the
    ** Value class is found in the 8-byte _builtinBuf.
    */
    union {
        void*   _data;
        builtinbuf_t _builtinBuf;
    };

    /**
     * This is RLEEncoded payload of data which should be processed.
     * Payload data do not include empty elements.
     * Empty bitmask should be applied separately while unpacking data.
     */
    RLEPayload* _tile;

    inline void init (bool allocate = true)
    {
        if (allocate && _size > sizeof(_builtinBuf))
        {
            _data = malloc(_size);
            if (!_data) {
                throw SYSTEM_EXCEPTION(SCIDB_SE_TYPESYSTEM, SCIDB_LE_NO_MEMORY_FOR_VALUE);
            }
            memset(_data, 0, _size);
        }
    }

    void destroy()
    {
        if (_size > sizeof(_builtinBuf) && _missingReason != MR_VECTOR) {
            free(_data);
            _data = NULL;
        }
        delete _tile;
    }

public:
    explicit Value()
        : _missingReason(MR_NULL), _size(0), _builtinBuf(0), _tile(NULL)
    {}

    /**
     * Construct Value for some size.
     */
    explicit Value(size_t size):
        _missingReason(MR_DATUM), _size(size), _builtinBuf(0), _tile(NULL)
    {
        init();
    }

    /**
     * Construct Value for some Type.
     */
    explicit Value(const Type& type):
        _missingReason(MR_DATUM), _size(type.byteSize()), _builtinBuf(0), _tile(NULL)
    {
        init(type.typeId()!=TID_VOID);
    }

    /**
     * Construct Value for some Type with tile mode support.
     */
    explicit Value(const Type& type, bool tile)
        : _missingReason(tile ? MR_TILE : MR_DATUM)
        , _size(type.byteSize())
        , _builtinBuf(0)
        , _tile(tile ? new RLEPayload(type) : NULL)
    {
        init(type.typeId() != TID_VOID);
    }

    /**
     * Construct value with linked data
     * @param data a pointer to linked data
     * @param size a size of linked data buffer
     */
    explicit Value(void* data, size_t size, bool isVector = true)
    : _size(size), _builtinBuf(0), _tile(NULL)
    {
        if (isVector) {
            _missingReason = MR_VECTOR;
            _data = data;
        } else {
            _missingReason = MR_DATUM;
            void* ptr;
            if (size > sizeof(_builtinBuf)) {
                _data = ptr = malloc(_size);
                if (!ptr) {
                    throw SYSTEM_EXCEPTION(SCIDB_SE_TYPESYSTEM, SCIDB_LE_NO_MEMORY_FOR_VALUE);
                }
            } else {
                ptr = (void*)&_builtinBuf;
            }
            memcpy(ptr, data, size);
        }
    }

    /**
     * Copy constructor.
     * @param Value object to be copied.
     */
    Value(const Value& val):
    _missingReason(MR_DATUM), _size(0), _builtinBuf(0), _tile(NULL)
    {
        *this = val;
    }

    void clear()
    {
        destroy();
        _missingReason = MR_NULL;
        _size = 0;
        _builtinBuf = 0;
        _tile = NULL;
    }

    ~Value()
    {
        destroy();
    }

    /**
     * Get the total in-memory footprint of the Value.
     * @param dataSize the size of the stored data, in bytes
     * @return the total in-memory footprint that would be occupied by allocating new Value(size)
     */
    static size_t getFootprint(size_t dataSize)
    {
        //if the datatype is smaller than _builtinBuf, it's stored inside _builtinBuf.
        if (dataSize > sizeof(builtinbuf_t ))
        {
            return sizeof(Value) + dataSize;
        }
        else
        {
            return sizeof(Value);
        }
    }

    /**
     * Link data buffer of some size to the Value object.
     * @param pointer to data buffer
     * @param size of data buffer in bytes
     */
    void linkData(void* data, size_t size)
    {
        if (((NULL != data) || (0 != size)) &&
            ((NULL == data) || (0 == size)))
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_CANT_LINK_DATA_TO_ZERO_BUFFER);
        }

        if (_size > sizeof(_builtinBuf) && _missingReason != MR_VECTOR) {
            free(_data);
            // _data overwritten below, no need to set it to NULL
        }
        _missingReason = MR_VECTOR;
        _size = size;
        _data = data;
    }

    /**
     * Get (void *) to data contents of Value object.
     * @return (void *) to data
     */
    void* data() const
    {
        return _missingReason != MR_VECTOR && _size <= sizeof(_builtinBuf) ? (void*)&_builtinBuf : _data;
    }

    /**
     * Get size of data contents in bytes.
     * @return size_t length of data in bytes
     */
    size_t size() const
    {
        return _size;
    }
    /**
     * Equality operator
     * Very basic, byte-wise equality.
     */
    bool operator == (Value const& other) const
    {
        return _missingReason == other. _missingReason
               && (_missingReason >= 0
                || (_size == other._size && (_size > sizeof(_builtinBuf)
                                             ? memcmp(_data, other._data, _size) == 0
                                             : _builtinBuf == other._builtinBuf)));
    }

    bool operator != (Value const& other) const
    {
        return !(*this == other);
    }

    /*
     * Check if data buffer is equal to type's default value
     */
    bool isDefault(const TypeId& typeId) const;

    /**
     * Check if data buffer is filled with 0
     * TODO: (RS) It can be optimized by using comparing of DWORDs
     */
    bool isZero() const
    {
        char* ptr = (char*)data();
        size_t n = size();
        while (n-- != 0)
        {
            if (*ptr++ != 0)
            {
                return false;
            }
        }
        return true;
    }

    /**
     * Set the Value to zero.
     */
    void setZero()
    {
        if (_missingReason == MR_VECTOR)
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_CANT_MODIFY_VALUE_WITH_LINKED_DATA);

                // TODO: Fix this. Should be able to set a Vector of
                //       values to default.
        if (_missingReason == MR_TILE)
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_CANT_SET_VALUE_VECTOR_TO_DEFAULT);

        _missingReason = MR_DATUM;
        if (_size > sizeof(_builtinBuf)) {
            memset(_data, 0, _size);
        } else {
            _builtinBuf = 0;
        }
    }

    /**
     * Set up memory to hold a vector of data values in this Value.
     * @param size of the memory to hold the vector in bytes.
     */
    void setVector(const size_t size)
    {
        if (_missingReason == MR_VECTOR)
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_CANT_MODIFY_VALUE_WITH_LINKED_DATA);

        if (size > sizeof(_builtinBuf)) {
            _data = (_size <= sizeof(_builtinBuf)) ? malloc(size) : realloc(_data, size);
            if (_data == NULL) {
                throw SYSTEM_EXCEPTION(SCIDB_SE_TYPESYSTEM, SCIDB_LE_NO_MEMORY_FOR_VALUE);
            }
        } else
        {
            if (_size > sizeof(_builtinBuf))
            {
                free(_data);
                _data = NULL;
            }
        }
        _size = size;
//        _missingReason = MR_TILE;
    }

    /**
     * Allocate space for value
     * @param size in bytes of the data buffer.
     */
    void setSize(const size_t size)
    {
        if (_missingReason == MR_VECTOR)
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_CANT_MODIFY_VALUE_WITH_LINKED_DATA);

        if (size > sizeof(_builtinBuf)) {
            _data = (_size <= sizeof(_builtinBuf)) ? malloc(size) : realloc(_data, size);
            if (_data == NULL) {
                throw SYSTEM_EXCEPTION(SCIDB_SE_TYPESYSTEM, SCIDB_LE_NO_MEMORY_FOR_VALUE);
            }
        } else {
            if (_size > sizeof(_builtinBuf)) {
                free(_data);
                _data = NULL;
            }
        }
        _size = size;
        _missingReason = MR_DATUM;
    }

    /**
     * Copy data buffer into the value object.
     * @param (void *) to data buffer to be copied.
     * @param size in bytes of the data buffer.
     */
    void setData(const void* data, size_t size)
    {
        void* ptr;

        if (_missingReason == MR_VECTOR) {
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_CANT_MODIFY_VALUE_WITH_LINKED_DATA);
        }
        if (size > sizeof(_builtinBuf)) {
            ptr = (_size <= sizeof(_builtinBuf)) ? malloc(size) : realloc(_data, size);
            if (ptr == NULL) {
                throw SYSTEM_EXCEPTION(SCIDB_SE_TYPESYSTEM, SCIDB_LE_NO_MEMORY_FOR_VALUE);
            }
            _data = ptr;
        } else {
            if (_size > sizeof(_builtinBuf)) {
                free(_data);
                // assignment to _builtinBuf below will zero _data too
            }
            ptr = (void*)&_builtinBuf;
            _builtinBuf = 0;
        }
        _size = size;
        memcpy(ptr, data, size);
        _missingReason = MR_DATUM;
    }

    /**
     * Check if value represents 'missing' value
     */
    bool isNull() const
    {
        return _missingReason >= 0;
    }

    /**
     * Set 'missing value with optional explanation of value missing reason.
     */
    void setNull(int reason = 0)
    {
        _missingReason = reason;
    }

    /**
     * Get reason of missing value (if it is previously set by setNull method)
     */
    int32_t getMissingReason() const
    {
        return _missingReason;
    }

    bool isVector() const
    {
        return _missingReason == MR_VECTOR
            || _missingReason == MR_TILE;
    }

    /**
     * Assignment operator.
     */
    Value& operator=(const Value& val)
    {
        if (this == &val)
        {
            return *this;
        }

        if (val._missingReason != MR_VECTOR) {
            // TODO: It's better to have special indicator of using tile mode in vector.
            // I will add it later.
            if (val._tile != NULL) {
                if (_tile == NULL) {
                    _tile = new RLEPayload();
                }
                *_tile = *val._tile;
            } else {
                delete _tile;
                _tile = NULL;
            }
            if (_missingReason == MR_VECTOR)
                throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_CANT_MODIFY_VALUE_WITH_LINKED_DATA);

            if (val.isNull()) {
                setNull(val.getMissingReason());
            } else {
                if (val._size <= sizeof(_builtinBuf)) {
                    if (_size > sizeof(_builtinBuf)) {
                        free(_data);
                        // assignment to _builtnBuf below clobbers _data
                    }
                    _builtinBuf = val._builtinBuf;
                    _size = val._size;
                } else {
                    setData(val._data, val._size);
                }
                _missingReason = val._missingReason;
            }
        } else {
            // Here we have source with linked data buffer and just copy
                        // pointer and size to this.
            //
            // PGB: Woah. This is a bit risky, no? If you free the first
            //      Value object, and free the memory associated with it,
            //      then this second Value will have dodgy data. Can we
            //      make the _Data a boost::shared_ptr<void> ?
            if (_size > sizeof(_builtinBuf) && _missingReason != MR_VECTOR) {
                free(_data);
            }
            _data = val._data;
            _size = val._size;
            _missingReason = MR_VECTOR;
        }
        return *this;
    }

    void swap(Value& val)
    {
        std::swap(_missingReason,val._missingReason);
        std::swap(_size,val._size);
        std::swap(_data,val._data);
        std::swap(_tile,val._tile);

        assert(isNull() || _missingReason != MR_TILE);
        assert((_size <= sizeof(_builtinBuf)) || _data != NULL);
    }

    void makeTileConstant(const TypeId& typeId);

        /*
        ** Serialization of the Value object for I/O
        */
    template<class Archive>
    void serialize(Archive& ar, const unsigned int version)
    {
        ar & _size;
        ar & _missingReason;

        // Serialization data
        if (!isNull()) {
                char* ptr = NULL;
            if (Archive::is_loading::value) {
                ptr = _size > sizeof(_builtinBuf)
                    ? (char*)(_data = malloc(_size))
                    : (char*)&_builtinBuf;
            } else {
                ptr = (char*) data();
            }
            for (size_t i = 0, n = _size; i < n; i++) {
                ar & ptr[i];
            }
        }

        // Serialization payload
        if (Archive::is_loading::value) {
            bool hasTile;
            ar & hasTile;
            if (hasTile) {
                _tile = new RLEPayload();
                ar & (*_tile);
            }
        } else {
            bool hasTile = _tile;
            ar & hasTile;
            if (hasTile) {
                ar & (*_tile);
            }
        }
    }

    // Methods for manipulating data content with casting to required C++ type
    BUILTIN_METHODS(char, Char)
    BUILTIN_METHODS(int8_t, Int8)
    BUILTIN_METHODS(int16_t, Int16)
    BUILTIN_METHODS(int32_t, Int32)
    BUILTIN_METHODS(int64_t, Int64)
    BUILTIN_METHODS(uint8_t, Uint8)
    BUILTIN_METHODS(uint16_t, Uint16)
    BUILTIN_METHODS(uint32_t, Uint32)
    BUILTIN_METHODS(uint64_t, Uint64)
    BUILTIN_METHODS(uint64_t, DateTime)
    BUILTIN_METHODS(float, Float)
    BUILTIN_METHODS(double, Double)
    BUILTIN_METHODS(bool, Bool)

    /// @return this value as a given type
    template<typename Type>
    const Type& get() const
    {
        assert(sizeof(Type) == _size);
        return *reinterpret_cast<const Type*>(&_builtinBuf);
    }

    /// set this value as a given type
    template<typename Type>
    void set(const Type& val)
    {
        if (_size > sizeof(_builtinBuf) && _missingReason != MR_VECTOR) {
            assert(false);
            free(_data);
            _data=0;
        }
        _missingReason = MR_DATUM;
        _size = sizeof(Type);
        if (_size > sizeof(_builtinBuf)) {
            assert(false);
            throw std::runtime_error("size of type too big");
        }
        (*reinterpret_cast<Type*>(&_builtinBuf)) = val;
    }

    /**
     * Setter of a scidb::Value, given its built-in value.
     */
    template<class T>
    static void setBuiltInValue(Value& v, T t, TypeEnum te)
    {
        switch(te)
        {
        case TE_CHAR:
            v.setChar(t); break;
        case TE_INT8:
            v.setInt8(t); break;
        case TE_INT16:
            v.setInt16(t); break;
        case TE_INT32:
            v.setInt32(t); break;
        case TE_INT64:
            v.setInt64(t); break;
        case TE_UINT8:
            v.setUint8(t); break;
        case TE_UINT16:
            v.setUint16(t); break;
        case TE_UINT32:
            v.setUint32(t); break;
        case TE_UINT64:
            v.setUint64(t); break;
        case TE_DATETIME:
            v.setDateTime(t); break;
        case TE_FLOAT:
            v.setFloat(t); break;
        case TE_DOUBLE:
            v.setDouble(t); break;
        case TE_BOOL:
            v.setBool(t); break;
        default:
            assert(false);
        }
    }

    /**
     * Getter of a scidb::Value, given its built-in value.
     */
    template<class T>
    static T getBuiltInValue(Value& v, TypeEnum te)
    {
        switch(te)
        {
        case TE_CHAR:
            return v.getChar();
        case TE_INT8:
            return v.getInt8();
        case TE_INT16:
            return v.getInt16();
        case TE_INT32:
            return v.getInt32();
        case TE_INT64:
            return v.getInt64();
        case TE_UINT8:
            return v.getUint8();
        case TE_UINT16:
            return v.getUint16();
        case TE_UINT32:
            return v.getUint32();
        case TE_UINT64:
            return v.getUint64();
        case TE_DATETIME:
            return v.getDateTime();
        case TE_FLOAT:
            return v.getFloat();
        case TE_DOUBLE:
            return v.getDouble();
        case TE_BOOL:
            return v.getBool();
        default:
            assert(false);
        }
        return T();
    }

    /**
    * Return (char *) to to the contents of the Value object
    */
    const char* getString() const
    {
        return _size==0 ? "" : (const char*)data();
    }

    /**
    * Set the data contents of the
    * @param Pointer to null terminated 'C' string
    */
    void setString(const char* str)
    {
        //
        // PGB: No, no, no. A string "" is *not* a missing string.
        // if (str && *str) { }
        if (str)
        {
            setData(str, strlen(str) + 1);
        }
        else
        {
            setNull();
        }
    }

    void setString(const std::string& s)
    {
        setData(s.c_str(),s.size() + 1);
    }

/**
 * New RLE fields of Value
 */
public:
    RLEPayload* getTile(TypeId const& type);

    RLEPayload* getTile() const
    {
        //XXX disable assert(_tile != NULL);
        //XXX for the hack to enable vectorized execution
//TODO:        assert(_missingReason == MR_TILE);
        return _tile;
    }

    void setTile(RLEPayload* payload)
    {
        _size = payload->elementSize();
        delete _tile;
        _tile = payload;
        _missingReason = MR_TILE;
    }
};

std::ostream& operator<<(std::ostream& stream, const Value& ob );

/**
 * TypeLibrary is a container to registered types in the engine.
 */
class TypeLibrary
{
private:
    static TypeLibrary _instance;
    std::map<TypeId, Type, __lesscasecmp> _typesById;
    std::map<TypeId, Type, __lesscasecmp> _builtinTypesById;
    std::map<TypeId, Value, __lesscasecmp> _defaultValuesById;
    PluginObjects _typeLibraries;
    Mutex mutex;

    const Type& _getType(TypeId typeId);
    void _registerType(const Type& type);
    bool _hasType(TypeId typeId);
    size_t _typesCount();
    std::vector<TypeId> _typeIds();
    const Value& _getDefaultValue(TypeId typeId);

public:
    TypeLibrary();

    static void registerBuiltInTypes();

    static const Type& getType(TypeId typeId)
    {
        return _instance._getType(typeId);
    }

    static bool hasType(const std::string& typeId)
    {
        return _instance._hasType(typeId);
    }

    static const std::vector<Type> getTypes(const std::vector<TypeId> typeIds)
    {
        std::vector<Type> result;
        for( size_t i = 0, l = typeIds.size(); i < l; i++ )
            result.push_back(_instance._getType(typeIds[i]));

        return result;
    }

    static void registerType(const Type& type)
    {
        _instance._registerType(type);
    }

    /**
     * Return the number of types currently registered in the TypeLibrary.
     */
    static size_t typesCount() {
        return _instance._typesCount();
    }

    /**
     * Return a vector of typeIds registered in the library.
     */
    static std::vector<TypeId> typeIds() {
        return _instance._typeIds();
    }

    static const PluginObjects& getTypeLibraries() {
        return _instance._typeLibraries;
    }

    static const Value& getDefaultValue(TypeId typeId)
    {
        return _instance._getDefaultValue(typeId);
    }
};

inline RLEPayload* Value::getTile(TypeId const& type)
{
    if (_tile == NULL)
    {
        _tile = new RLEPayload(TypeLibrary::getType(type));
    }
    return _tile;
}

inline bool Value::isDefault(const TypeId& typeId) const
{
    return *this == TypeLibrary::getDefaultValue(typeId);
}

/**
 * Helper Value functions
 */
/**
 * @param type a type of input value
 * @param value a value to be converted
 * @return string with value
 */
std::string ValueToString(const TypeId type, const Value& value, int precision = 6);


/**
 * @param type a type of output value
 * @param str a string to be converted
 * @param [out] value a value in which string will be converted
  */
void StringToValue(const TypeId type, const std::string& str, Value& value);

/**
 * @param type a type of input value
 * @param value a value to be converted
 * @return double value
 */
double ValueToDouble(const TypeId type, const Value& value);

/**
 * @param type a type of output value
 * @param d a double value to be converted
 * @param [out] value a value in which double will be converted
  */
void DoubleToValue(const TypeId type, double d, Value& value);

bool isBuiltinType(const TypeId type);

TypeId propagateType(const TypeId type);
TypeId propagateTypeToReal(const TypeId type);

/**
 * Convert string to date time
 * @param str string woth data/time to be parsed
 * @return Unix time (time_t)
 */
time_t parseDateTime(std::string const& str);

void parseDateTimeTz(std::string const& str, Value& result);

/**
 * The three-value logic is introduced to improve efficiency for calls to isNan.
 * If isNan takes as input a TypeId, every time isNan is called on a value, string comparisions would be needed
 * to check if the type is equal to TID_DOUBLE and/or TID_FLOAT.
 * With the introduction of DoubleFloatOther, the caller can do the string comparison once for a collection of values.
 */
enum DoubleFloatOther
{
    DOUBLE_TYPE,
    FLOAT_TYPE,
    OTHER_TYPE
};

/**
 * Given a TypeId, tell whether it is double, float, or other.
 * @param[in] type   a string type
 * @return one constant in DoubleFloatOther
 */
inline DoubleFloatOther getDoubleFloatOther(TypeId const& type)
{
    if (type==TID_DOUBLE) {
        return DOUBLE_TYPE;
    } else if (type==TID_FLOAT) {
        return FLOAT_TYPE;
    }
    return OTHER_TYPE;
}

/**
 * A value can be in one of below, assuming null < nan < regular
 */
enum NullNanRegular
{
    NULL_VALUE,
    NAN_VALUE,
    REGULAR_VALUE
};

/**
 * Given a value, tell whether it is Null, Nan, or a regular value.
 * @param[in] v      a value
 * @param[in] type   an enum DoubleFloatOther
 * @return one constant in NullNanRegular
 */
inline NullNanRegular getNullNanRegular(Value const& v, DoubleFloatOther type)
{
    if (v.isNull()) {
        return NULL_VALUE;
    }
    if (type==DOUBLE_TYPE) {
        double d = v.getDouble();
        if (std::isnan(d)) {
            return NAN_VALUE;
        }
    } else if (type==FLOAT_TYPE) {
        float d = v.getFloat();
        if (std::isnan(d)) {
            return NAN_VALUE;
        }
    }
    return REGULAR_VALUE;
}

/**
 * Check if a value is NaN.
 * @param[in] v     a value
 * @param[in] type  an enum DoubleFloatOther
 * @return    true iff the value is Nan
 *
 */
inline bool isNan(const Value& v, DoubleFloatOther type)
{
    if (type==DOUBLE_TYPE) {
        return std::isnan(v.getDouble());
    } else if (type==FLOAT_TYPE) {
        return std::isnan(v.getFloat());
    }
    return false;
}

/**
 * Check if a value is Null or NaN.
 * @param[in] v     a value
 * @param[in] type  an enum DoubleFloatOther
 * @return    true iff the value is either Null or Nan
 */
inline bool isNullOrNan(const Value& v, DoubleFloatOther type)
{
    return v.isNull() || isNan(v, type);
}

} //namespace

#endif /* TYPESYSTEM_H_ */
