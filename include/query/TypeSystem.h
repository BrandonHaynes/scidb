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

#ifndef TYPESYSTEM_H_
#define TYPESYSTEM_H_

/****************************************************************************/

#include <map>                                           // For std::map
#include <cmath>                                         // For std:isnan()
#include <boost/operators.hpp>                           // For op overloads
#include <util/Mutex.h>                                  // For Mutex
#include <util/PointerRange.h>                           // For PointerRange
#include <util/PluginObjects.h>                          // For PluginObjects
#include <array/RLE.h>                                   // For RLEPayload
#include <query/Value.h>                                 // For Value

/****************************************************************************/
namespace scidb {
/****************************************************************************/

typedef std::string TypeId;

const char TID_INVALID[]    = "InvalidType";
const char TID_INDICATOR[]  = "indicator";
const char TID_CHAR[]       = "char";
const char TID_INT8[]       = "int8";
const char TID_INT16[]      = "int16";
const char TID_INT32[]      = "int32";
const char TID_INT64[]      = "int64";
const char TID_UINT8[]      = "uint8";
const char TID_UINT16[]     = "uint16";
const char TID_UINT32[]     = "uint32";
const char TID_UINT64[]     = "uint64";
const char TID_FLOAT[]      = "float";
const char TID_DOUBLE[]     = "double";
const char TID_BOOL[]       = "bool";
const char TID_STRING[]     = "string";
const char TID_DATETIME[]   = "datetime";
const char TID_DATETIMETZ[] = "datetimetz";
const char TID_VOID[]       = "void";
const char TID_BINARY[]     = "binary";

/**
 *  TypeEnum is provided to support efficient type comparisons.
 */
enum TypeEnum
{
    TE_INVALID   = -1,
    TE_INDICATOR =  0,
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
 *  Map the given TypeId to its corresponding TypeEnum.
 */
TypeEnum typeId2TypeEnum(const TypeId&,bool noThrow = false);

/**
 *  Map the given TypeEnum to its corresponding TypeId.
 */
inline TypeId typeEnum2TypeId(TypeEnum te)
{
    switch (te)
    {
        default:            SCIDB_ASSUME(false);
        case TE_INDICATOR:  return TID_INDICATOR;
        case TE_CHAR:       return TID_CHAR;
        case TE_INT8:       return TID_INT8;
        case TE_INT16:      return TID_INT16;
        case TE_INT32:      return TID_INT32;
        case TE_INT64:      return TID_INT64;
        case TE_UINT8:      return TID_UINT8;
        case TE_UINT16:     return TID_UINT16;
        case TE_UINT32:     return TID_UINT32;
        case TE_UINT64:     return TID_UINT64;
        case TE_FLOAT:      return TID_FLOAT;
        case TE_DOUBLE:     return TID_DOUBLE;
        case TE_BOOL:       return TID_BOOL;
        case TE_STRING:     return TID_STRING;
        case TE_DATETIME:   return TID_DATETIME;
        case TE_DATETIMETZ: return TID_DATETIMETZ;
        case TE_VOID:       return TID_VOID;
        case TE_BINARY:     return TID_BINARY;
    }

    return TID_VOID;
}

template<typename t>
            TypeId type2TypeId();
template<>  TypeId type2TypeId<char>    ();
template<>  TypeId type2TypeId<int8_t>  ();
template<>  TypeId type2TypeId<int16_t> ();
template<>  TypeId type2TypeId<int32_t> ();
template<>  TypeId type2TypeId<int64_t> ();
template<>  TypeId type2TypeId<uint8_t> ();
template<>  TypeId type2TypeId<uint16_t>();
template<>  TypeId type2TypeId<uint32_t>();
template<>  TypeId type2TypeId<uint64_t>();
template<>  TypeId type2TypeId<float>   ();
template<>  TypeId type2TypeId<double>  ();

inline bool IS_VARLEN(const TypeId& t)
{
    return t==TID_STRING || t==TID_BINARY;
}

inline bool IS_REAL(const TypeId& t)
{
    return t==TID_FLOAT || t==TID_DOUBLE;
}

inline bool IS_INTEGRAL(const TypeId& t)
{
    return t == TID_INT8
        || t == TID_INT16
        || t == TID_INT32
        || t == TID_INT64
        || t == TID_UINT8
        || t == TID_UINT16
        || t == TID_UINT32
        || t == TID_UINT64;
}

inline bool IS_NUMERIC(const TypeId& t)
{
    return IS_INTEGRAL(t) || IS_REAL(t);
}

inline bool IS_SIGNED(const TypeId& t)
{
    return t == TID_INT8
        || t == TID_INT16
        || t == TID_INT32
        || t == TID_INT64
        || IS_REAL(t);
}

/****************************************************************************/

const size_t      STRFTIME_BUF_LEN        = 256;
const char* const DEFAULT_STRFTIME_FORMAT = "%F %T";

/****************************************************************************/

/**
 *  Describes the size and behaviour of a set of values.
 */
class Type : boost::totally_ordered<Type>,
             boost::totally_ordered<TypeId>
{
 public:                  // Construction
                              Type()
                               : _typeId  (TID_VOID),
                                 _bitSize (0),
                                 _baseType(TID_VOID)     {}
                              Type(const TypeId& i,uint32_t n,const TypeId& b = TID_VOID)
                               : _typeId  (i),
                                 _bitSize (n),
                                 _baseType(b)            {}

 public:                   // Operations
      const TypeId&           name()               const {return _typeId;}
      const TypeId&           typeId()             const {return _typeId;}
      const TypeId&           baseType()           const {return _baseType;}
            uint32_t          bitSize()            const {return _bitSize;}
            uint32_t          byteSize()           const {return (_bitSize + 7) >> 3;}
            bool              variableSize()       const {return _bitSize == 0;}
            bool              isVoid()             const {return _typeId.compare(TID_VOID) == 0;}

 public:                   // Operations
    static  bool              isSubtype  (TypeId const& sub,TypeId const& sup);
            bool              isSubtypeOf(const TypeId& t)const {return _baseType!=TID_VOID && (_baseType==t || isSubtype(_baseType,t));}

  private:                 // Representation
            TypeId            _typeId;                  // type identificator
            uint32_t          _bitSize;                 // bit size is used in storage manager. 0 - for variable size data
            TypeId            _baseType;
};

/****************************************************************************/

std::ostream& operator<<(std::ostream&,const Type&);
std::ostream& operator<<(std::ostream&,const std::vector<Type>&);
std::ostream& operator<<(std::ostream&,const std::vector<TypeId>&);

/****************************************************************************/

inline bool operator <(const Type& a,const Type&   b) {return a.typeId().compare(b.typeId())< 0;}
inline bool operator==(const Type& a,const Type&   b) {return a.typeId().compare(b.typeId())==0;}
inline bool operator <(const Type& a,const TypeId& b) {return a.typeId().compare(b)         < 0;}
inline bool operator==(const Type& a,const TypeId& b) {return a.typeId().compare(b)         ==0;}

/****************************************************************************/

/**
 *  Construct a value to represent a null with a missing reason code of '0'.
 */
inline Value::Value()
            : _code(0),
              _size(0),
              _data(0)
{
    assert(consistent());                                // Check consistency
}

/**
 *  Construct a value to represent an object of length 'n', and initialize its
 *  internal storage with zeros.
 */
inline Value::Value(size_t n)
            : _code(MR_DATUM),
              _size(n),
              _data(0)
{
    if (large(n))                                        // Expect large data?
    {
        _data = calloc(n);                               // ...allocate it now
    }

    assert(consistent());                                // Check consistency
}

/**
 *  Construct a value to represent an object of the given type, and initialize
 *  its internal storage with zeros.
 */
inline Value::Value(const Type& t)
            : _code(MR_DATUM),
              _size(t.byteSize()),
              _data(0)
{
    if (large(_size))                                    // Expect large data?
    {
        _data = calloc(_size);                           // ...allocate it now
    }

    assert(consistent());                                // Check consistency
}

/**
 *  Construct a value to represent a copy of the native value 'v' of type 't'.
 */
template<class t>
inline Value::Value(const t& v,asData_t)
            : _code(MR_DATUM),
              _size(sizeof(t))
{
    if (large(sizeof(t)))                                // Expect large data?
    {
        _data = malloc(_size);                           // ...allocate it now

        new(_data) t(v);                                 // ...copy construct
    }
    else                                                 // It fits in '_data'
    {
        new(&_data) t(v);                                // ...copy construct
    }

    assert(consistent());                                // Check consistency
}

/**
 *  Construct a value to represent an old-style 'tile' of values of type 't'.
 */
inline Value::Value(const Type& t,asTile_t)
            : _code(MR_TILE),
              _size(0),
              _tile(new RLEPayload(t))
{
    assert(consistent());                                // Check consistency
}

/**
 *  Construct a value to represent a copy of the 'n' bytes of data that start
 *  at the address 'v'.
 */
inline Value::Value(void* v,size_t n)
            : _code(MR_DATUM),
              _size(n),
              _data(0)
{
    assert(implies(n!=0,v!=0));                          // Validate arguments

    if (large(n))                                        // Expect large data?
    {
        _data = malloc(n);                               // ...allocate it now
        memcpy(_data,v,n);                               // ...then copy it in
    }
    else                                                 // It fits in '_data'
    {
        memcpy(&_data,v,n);                              // ...so just copy it
    }

    assert(consistent());                                // Check consistency
}

/**
 *  Construct a value to represent a copy of the value 'v'.
 */
inline Value::Value(const Value& v)
            : _code(v._code),
              _size(v._size),
              _data(v._data)
 {
    assert(v.consistent());                              // Validate arguments

    if (v.isTile())                                      // Source has a tile?
    {
        _tile = new RLEPayload(*v._tile);                // ...so copy payload
    }
    else                                                 // No, could be datum
    if (large(_size))                                    // ...heap allocated?
    {
        _data = malloc(_size);                           // ...allocate a copy

        memcpy(_data,v._data,_size);                     // ...copy data over
    }

    assert(consistent());                                // Check consistency
}

/**
 *  Destroy the value by freeing any tile or memory we may be holding on to.
 */
inline Value::~Value()
{
    if (isTile())                                        // Holding onto tile?
    {
        delete _tile;                                    // ...good riddance!
    }
    else
    {
        reset();                                         // ...free the datum
    }
}

/**
 *  Return the missing reason,  an optional (small) non-negative integer that
 *  qualifies the reason for this value being marked as null, or return -1 if
 *  the value is not in fact missing at all.
 */
inline int32_t Value::getMissingReason() const
{
    return isNull() ? _code : -1;                        // Return the reason
}

/**
 *  Mark the value as being 'null' (or 'missing') with an optional reason code
 *  as given.
 *
 *  A null is a well defined value of an array at some point in its domain and
 *  is not to be confused with a point at which the array is simply undefined,
 *  or 'empty'.
 *
 *  Technically, class Value represents the Disjoint Union of some native type
 *  't' with a set of (small) non-negative integers that are distinct from all
 *  elements of this set 't';  at any time the Value can tell you which of the
 *  the two it happens to be holding at that moment.
 *
 *  It is an error to call this method for a value that is carrying a 'tile'.
 */
inline void Value::setNull(reason reason)
{
    assert(!isTile());                                   // Once tile, always

    _code = reason;                                      // Record the reason

    assert(consistent());                                // Check consistency
}

/**
 *  Return the value of type 't' that we are currently representing.
 *
 *  It is an error to call this method for a value that is carrying a 'tile'.
 */
template<class t>
inline const t& Value::get() const
{
    return const_cast<Value*>(this)->get<t>();           // Call mutable get<>
}

/**
 *  Return the value of type 't' that we are currently representing.
 *
 *  It is an error to call this method for a value that is carrying a 'tile'.
 */
template<class t>
inline t& Value::get()
{
    assert(!isTile());                                   // Must carry a datum
    assert(sizeof(t) <= _size);                          // That is no smaller
    assert(iff(small(sizeof(t)),small(_size)));          // And similar layout

 /* It would be more general to test the 'small'-ness of '_size' here,  rather
    than looking at the size of the target type 't':  doing so would enable us
    to handle a fetch of the first (small) element from an embedded array that
    does not itself fit within the buffer, for example; but this would require
    a *run time* check, thus slowing down the much more common case of a fetch
    of a single small value from a Value that  holds exactly one such element;
    understand that the test below involves a size known to the compiler, thus
    is folded away at compile time, and so incurs no overhead whatsoever...*/

    if (small(sizeof(t)))                                // Fits in the buffer
    {
        return reinterpret_cast<t&>(_data);              // ...no dereference
    }
    else
    {
        return *static_cast<t*>(_data);                  // ...ok, dereference
    }
}

/**
 *  Assign this object the value 'v' of type 't'.
 *
 *  It is an error to call this method for a value that is carrying a 'tile'.
 */
template<class t>
inline void Value::set(const t& v)
{
    assert(!isTile());                                   // Not a tile pointer
    assert(boost::has_trivial_destructor<t>());          // Check simple value
    assert(small(sizeof(t)) && small(_size));            // No need to realloc

    _code = MR_DATUM;                                    // Now holds a datum
    _size = sizeof(t);                                   // So record its size
    new(&_data) t(v);                                    // Copy the datum in

    assert(consistent());                                // Check consistency
}

/**
 *  Assign this object the value 'v' of type 't', after first clearing up any
 *  existing allocation we may happen to be carrying.
 *
 *  It is an error to call this method for a value that is carrying a 'tile'.
 */
template<class t>
inline void Value::reset(const t& v)
{
    assert(!isTile());                                   // Not a tile pointer
    assert(boost::has_trivial_destructor<t>());          // Check simple value

    new(setSize(sizeof(t))) t(v);                        // Resize, then copy

    assert(consistent());                                // Check consistency
}

/**
 *  Return a pointer to the start of the size() bytes of storage being managed
 *  by this object.
 *
 *  It is an error to call this method for a value that is carrying a 'tile'.
 *
 *  The orginal author incorrectly typed this method as returning a *mutable*
 *  pointer - there are now hundreds of call sites relying on this fact. Sigh.
 */
inline void* Value::data() const
{
    assert(!isTile());                                   // Don't slice tile*

    return large(_size) ? _data : (void*)&_data;         // Return the buffer
}

/**
 *  Resize our internal buffer to hold 'n' bytes of storage for the datum, and
 *  return a pointer to the start of this new storage area.
 *
 *  It is an error to call this method for a value that is carrying a 'tile'.
 */
inline void* Value::setSize(size_t n)
{
    assert(!isTile());                                   // Once tile, always

    _code = MR_DATUM;                                    // Now carrying datum

    if (large(n))                                        // New size is large?
    {
        if (_size != n)                                  // ...and different?
        {
            if (large(_size))                            // ....got allocation?
            {
                _data = realloc(_data,n);                // .....reallocate it
            }
            else                                         // ....no allocation
            {
                _data = malloc(n);                       // .....allocate now
            }

            _size = n;                                   // ....store new size
        }

        assert(consistent());                            // ...all looks good
        return _data;
    }
    else                                                 // No, datum is small
    {
        reset();                                         // ...free the buffer
        _data = 0;                                       // ...reset the datum
        _size = n;                                       // ...record new size

        assert(consistent());                            // ...all looks good
        return  &_data;                                  // ...return pointer
    }
}

/**
 *  Assign to the value a copy of the 'n' bytes of storage starting at address
 *  'v'.
 */
inline void Value::setData(const void* v,size_t n)
{
    assert(implies(n!=0,v!=0));                          // Validate arguments

    memcpy(setSize(n),v,n);                              // Resize, then copy

    assert(consistent());                                // Check consistency
}

/**
 *  Return true if this value can safely be interpreted as representing a null
 *  terminated string; that is, it carries a datum whose final byte is a zero.
 */
inline bool Value::isString() const
{
    assert(consistent());                                // Check consistency

    return _code==MR_DATUM && (_size==0 || getData<char>()[_size-1]==0);
}

/**
 *  Return a pointer to the null terminated sequence of characters that reside
 *  within our data buffer.
 */
inline const char* Value::getString() const
{
    assert(isString());                                  // Ensure *is* string

    return getData<char>();                              // The string pointer
}

/**
 *  Assign to this value a copy of the string 's'.
 */
inline void Value::setString(const char* s)
{
    assert(s != 0);                                      // Validate arguments

    setData(s,strlen(s) + 1);                            // Copy characters in

    assert(consistent() && isString());                  // Check consistency
}

/**
 *  Assign to this value a copy of the string 's'.
 */
inline void Value::setString(const std::string& s)
{
    setData(s.c_str(),s.size() + 1);                     // Copy characters in

    assert(consistent() && isString());                  // Check consistency
}

/**
 *  Swap this value with the value 'v'.
 */
inline void Value::swap(Value& v)
{
    std::swap(_code,v._code);                            // Swap status codes
    std::swap(_size,v._size);                            // Swap buffer sizes
    std::swap(_data,v._data);                            // Swap data buffers

    assert(consistent() && v.consistent());              // Check consistency
}

/**
 *  Assign to this value a copy of the value 'v'.
 *
 *  Note the trick for reusing the copy constructor, which is justified by the
 *  fact that the Value class is conceptually 'sealed'; that is, no class will
 *  ever inherit from it.
 */
inline Value& Value::operator=(const Value& v)
{
    if (this != &v)                                      // Non trivial assign?
    {
        this->~Value();                                  // ...destroy current

        new(this) Value(v);                              // ...re-init overtop
    }

    assert(consistent());                                // Check consistency
    return *this;                                        // Permit a = b = c
}

/**
 *  Return true if 'a' and 'b' both represent the same value.
 *
 *  It is an error to call this method if either value is carrying a 'tile'.
 */
inline bool operator==(const Value& a,const Value& b)
{
    assert(!a.isTile() && !b.isTile());                  // Can't handle tiles

    if (a._code != b._code)                              // Their codes differ?
    {
        return false;                                    //  ...so they differ
    }

    if (a.isNull())                                      // Are both missing?
    {
        return true;                                     // ...so are the same
    }

    if (a._size != b._size)                              // Their sizes differ?
    {
        return false;                                    //  ...so they differ
    }

    if (Value::large(a._size))                           // Are large values?
    {
        return memcmp(a._data,b._data,a._size) == 0;     // ...compare buffers
    }
    else
    {
        return a._data == b._data;                       // ...compare buffers
    }

    return false;                                        //  The values differ
}

/**
 *  Return the value to the state of a newly constructed missing value.
 */
inline void Value::clear()
{
    operator=(Value());                                  // Assign the default
}

/**
 *  Serialize the value to or from the serialization archive 'ar'.
 */
template<class Archive>
void Value::serialize(Archive& ar,unsigned int)
{
    ar & _size;                                          // Serialize the size
    ar & _code;                                          // Serialize the code

    if (isDatum())                                       // Carries real data?
    {
        if (Archive::is_loading::value && large(_size))  // ...and it's large?
        {
            _data = malloc(_size);                       // ....so allocate it
        }

        for (char* p=getData<char>(), *e = p+_size; p!=e; ++p)
        {
            ar & *p;                                     // ....serialize it
        }
    }
    else                                                 // No, a null or tile
    if (Archive::is_loading::value)                      // Just read size in?
    {
        _size = 0;                                       // ...so no real data
    }

 /* We don't actually need to serialize the 'hasTile' fag any more, but still
    do so as to remain compatable with the previous serialization format...*/

    bool _ = false; ar & _;                              // Serialize boolean

    if (isTile())                                        // Holding onto tile?
    {
        if (Archive::is_loading::value)                  // ...and reading it?
        {
            _tile = new RLEPayload();                    // ....so allocate it
        }

        ar & *_tile;                                     // ...serialize tile
    }

    assert(consistent());                                // Check consistency
}

/**
 *  Return the number of bytes needed to represent this value.
 *
 *  Does not currently take into account any internal block header that may be
 *  placed on the front of the heap allocation by the system allocator.
 */
inline size_t Value::getFootprint(size_t n)
{
    if (large(n))                                        // Is the data large?
    {
        return sizeof(Value) + n;                        // ...add the pointer
    }
    else                                                 // No, stored locally
    {
        return sizeof(Value);                            // ...so no overhead
    }
}

/**
 *  Check the call to calloc() and throw an exception if out of memory.
 */
inline void* Value::calloc(size_t n)
{
    assert(large(n));                                    // Data must be large

    void* p = ::calloc(n,1);                             // Delegate to calloc

    if (p == 0)                                          // Allocation failed?
    {
        fail(SCIDB_LE_NO_MEMORY_FOR_VALUE);              // ...throw exception
    }

    return p;                                            // The new allocation
}

/**
 *  Check the call to malloc() and throw an exception if out of memory.
 */
inline void* Value::malloc(size_t n)
{
    assert(large(n));                                    // Data must be large

    void* p = ::malloc(n);                               // Delegate to malloc

    if (p == 0)                                          // Allocation failed?
    {
        fail(SCIDB_LE_NO_MEMORY_FOR_VALUE);              // ...throw exception
    }

    return p;                                            // The new allocation
}

/**
 *  Check the call to realloc() and throw an exception if out of memory.
 */
inline void* Value::realloc(void* p,size_t n)
{
    assert(large(n) && p!=0);                            // Data must be large

    void* v = ::realloc(p,n);                            // Delegate to realloc

    if (v == 0)                                          // Allocation failed?
    {
        fail(SCIDB_LE_NO_MEMORY_FOR_VALUE);              // ...throw exception
    }

    return v;                                            // The new allocation
}

/**
 *  Free any heap allocated data we may currently be holding on to, but do not
 *  overwrite the buffer - the caller will take care of this themselves.
 *
 *  Be careful here: this function is inherently unsafe and does not leave the
 *  value in a consistent state; instead, it is up to the caller to ensure the
 *  '_data' and '_size' fields are correctly synchronized afterward.
 */
inline void Value::reset()
{
    if (large(_size))                                    // Has an allocation?
    {
        ::free(_data);                                   // ...so free it now
    }
}

/****************************************************************************/

/**
 * TypeLibrary is a container to registered types in the engine.
 */
class TypeLibrary
{
private:
    static TypeLibrary                      _instance;
    std::map<TypeId, Type,  __lesscasecmp>  _typesById;
    std::map<TypeId, Type,  __lesscasecmp>  _builtinTypesById;
    std::map<TypeId, Value, __lesscasecmp>  _defaultValuesById;
    PluginObjects                           _typeLibraries;
    Mutex                           mutable _mutex;

private:
    bool                _hasType        (const TypeId&) const;
    const Type&         _getType        (const TypeId&);
    const Value&        _getDefaultValue(const TypeId&);
    size_t              _typesCount     ()              const;
    std::vector<TypeId> _typeIds        ()              const;
    void                _registerType   (const Type&);

public:
    TypeLibrary();

    static void registerBuiltInTypes();

    static bool hasType(const TypeId& t)
    {
        return _instance._hasType(t);
    }

    static const Type& getType(const TypeId& t)
    {
        return _instance._getType(t);
    }

    static std::vector<Type> getTypes(PointerRange<TypeId>);

    static void registerType(const Type& t)
    {
        _instance._registerType(t);
    }

    /**
     * Return the number of types currently registered in the TypeLibrary.
     */
    static size_t typesCount()
    {
        return _instance._typesCount();
    }

    /**
     * Return a vector of typeIds registered in the library.
     */
    static std::vector<TypeId> typeIds()
    {
        return _instance._typeIds();
    }

    static const PluginObjects& getTypeLibraries()
    {
        return _instance._typeLibraries;
    }

    static const Value& getDefaultValue(const TypeId& t)
    {
        return _instance._getDefaultValue(t);
    }
};

/**
 *  Return true if the value 'v' is the default value for the type 't'.
 */
inline bool isDefaultFor(const Value& v,const TypeId& t)
{
    return TypeLibrary::getDefaultValue(t) == v;
}

/**
 * @param type a type of input value
 * @param value a value to be converted
 * @return string with value
 */
std::string ValueToString(const TypeId& type, const Value& value, int precision = 6);

/**
 * @param type a type of output value
 * @param str a string to be converted
 * @param [out] value a value in which string will be converted
  */
void StringToValue(const TypeId& type, const std::string& str, Value& value);

/**
 * @param type a type of input value
 * @param value a value to be converted
 * @return double value
 */
double ValueToDouble(const TypeId& type, const Value& value);

/**
 * @param type a type of output value
 * @param d a double value to be converted
 * @param [out] value a value in which double will be converted
  */
void DoubleToValue(const TypeId& type, double d, Value& value);

/**
 * Convert string to integral type T, backward compatibly with sscanf(3).
 *
 * @description We know we have an integer type here, so sscanf(3) is
 * overkill: we can call strtoimax(3)/strtoumax(3) with less overhead.
 * Also disallow octal input: the string is either base 10 or (with
 * leading 0x or 0X) base 16.
 *
 * @param s null-terminated string
 * @param type TypeId used to generate exception messages
 * @return the T integer value parsed from the string @c s
 * @throws USER_EXCEPTION(SCIDB_SE_TYPE_CONVERSION,SCIDB_LE_FAILED_PARSE_STRING)
 */
template <typename T>
T StringToInteger(const char *s, const TypeId& tid);

bool isBuiltinType(const TypeId& type);
TypeId propagateType(const TypeId& type);
TypeId propagateTypeToReal(const TypeId& type);

/**
 * Convert string to date time
 * @param str string woth data/time to be parsed
 * @return Unix time (time_t)
 */
time_t parseDateTime(std::string const& str);

void parseDateTimeTz(std::string const& str, Value& result);

/**
 * The three-value logic is introduced to improve efficiency for calls to isNan.
 *
 * If isNan takes as input a TypeId, every time isNan is called on a value,
 * string comparisions would be needed to check if the type is equal to TID_DOUBLE
 * and/or TID_FLOAT.
 *
 * With the introduction of DoubleFloatOther, the caller can do the string
 * comparison once for a collection of values.
 */
enum DoubleFloatOther
{
    DOUBLE_TYPE,
    FLOAT_TYPE,
    OTHER_TYPE
};

/**
 * Given a TypeId, tell whether it is double, float, or other.
 *
 * @param[in] type   a string type
 * @return one constant in DoubleFloatOther
 */
inline DoubleFloatOther getDoubleFloatOther(TypeId const& type)
{
    if (type == TID_DOUBLE)
    {
        return DOUBLE_TYPE;
    }
    else
    if (type == TID_FLOAT)
    {
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
 *
 * @param[in] v      a value
 * @param[in] type   an enum DoubleFloatOther
 * @return one constant in NullNanRegular
 */
inline NullNanRegular getNullNanRegular(Value const& v,DoubleFloatOther type)
{
    if (v.isNull())
    {
        return NULL_VALUE;
    }

    if (type == DOUBLE_TYPE)
    {
        if (std::isnan(v.get<double>()))
        {
            return NAN_VALUE;
        }
    }
    else
    if (type == FLOAT_TYPE)
    {
        if (std::isnan(v.get<float>()))
        {
            return NAN_VALUE;
        }
    }

    return REGULAR_VALUE;
}

/**
 * Check if a value is NaN.
 *
 * @param[in] v     a value
 * @param[in] type  an enum DoubleFloatOther
 * @return    true iff the value is Nan
 */
inline bool isNan(const Value& v,DoubleFloatOther type)
{
    if (type == DOUBLE_TYPE)
    {
        return std::isnan(v.get<double>());
    }
    else
    if (type == FLOAT_TYPE)
    {
        return std::isnan(v.get<float>());
    }

    return false;
}

/**
 * Check if a value is Null or NaN.
 *
 * @param[in] v     a value
 * @param[in] type  an enum DoubleFloatOther
 * @return    true iff the value is either Null or Nan
 */
inline bool isNullOrNan(const Value& v,DoubleFloatOther type)
{
    return v.isNull() || isNan(v,type);
}

Value makeTileConstant(const TypeId&,const Value&);

/****************************************************************************/
}
/****************************************************************************/
#endif
/****************************************************************************/
