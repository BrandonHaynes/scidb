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

#ifndef VALUE_H_
#define VALUE_H_

/****************************************************************************/

#include <boost/operators.hpp>                           // For op overloads
#include <util/PointerRange.h>                           // For PointerRange
#include <util/PluginObjects.h>                          // For PluginObjects

/****************************************************************************/
namespace scidb {
/****************************************************************************/

class Type;
class RLEPayload;

/**
 *  Represents a single data value.
 *
 *  Can also represent an old-style 'tile' of values, a representation that we
 *  wish to move away from, however.
 */
class Value : boost::equality_comparable<Value>
{
 public:                   // Supporting Types
    enum    asData_t          {asData};                  // Has actual datum
    enum    asTile_t          {asTile};                  // Has an RLEPayload*
    typedef uint8_t           reason;                    // Why is it missing?

 public:                   // Construction
    explicit                  Value();
    explicit                  Value(size_t);
    explicit                  Value(const Type&);
    template<class type>      Value(const type&,asData_t);
                              Value(const Type&,asTile_t);
                              Value(void*,size_t);
                              Value(const Value&);
                             ~Value();

 public:                   // Operations
            Value&            operator= (const Value&);
    friend  bool              operator==(const Value&, const Value&);
    friend  std::ostream&     operator<<(std::ostream&,const Value&);

 public:                   // Operations
            bool              isNull()             const {return _code >= 0;}
            bool              isTile()             const {return _code == MR_TILE;}
            bool              isDatum()            const {return _code == MR_DATUM;}
            bool              isSmall()            const {return _code == MR_DATUM && small(_size);}
            bool              isLarge()            const {return _code == MR_DATUM && large(_size);}
            bool              isString()           const;
            bool              getBool()            const {return get<bool>    ();}
            char              getChar()            const {return get<char>    ();}
            int8_t            getInt8()            const {return get<int8_t>  ();}
            int16_t           getInt16()           const {return get<int16_t> ();}
            int32_t           getInt32()           const {return get<int32_t> ();}
            int64_t           getInt64()           const {return get<int64_t> ();}
            uint8_t           getUint8()           const {return get<uint8_t> ();}
            uint16_t          getUint16()          const {return get<uint16_t>();}
            uint32_t          getUint32()          const {return get<uint32_t>();}
            uint64_t          getUint64()          const {return get<uint64_t>();}
            uint64_t          getDateTime()        const {return get<uint64_t>();}
            float             getFloat()           const {return get<float>   ();}
            double            getDouble()          const {return get<double>  ();}
            size_t            size()               const {return _size;}
            void*             data()               const;
            RLEPayload*       getTile()            const {assert(_code==MR_TILE);return _tile;}
      const char*             getString()          const;
            int32_t           getMissingReason()   const;

 public:                   // Operations
    static  size_t            getFootprint(size_t);

 public:                   // Operations
            void              setNull    (reason = 0);
            void              setBool    (bool     v)    {set<bool>    (v);}
            void              setChar    (char     v)    {set<char>    (v);}
            void              setInt8    (int8_t   v)    {set<int8_t>  (v);}
            void              setInt16   (int16_t  v)    {set<int16_t> (v);}
            void              setInt32   (int32_t  v)    {set<int32_t> (v);}
            void              setInt64   (int64_t  v)    {set<int64_t> (v);}
            void              setUint8   (uint8_t  v)    {set<uint8_t> (v);}
            void              setUint16  (uint16_t v)    {set<uint16_t>(v);}
            void              setUint32  (uint32_t v)    {set<uint32_t>(v);}
            void              setUint64  (uint64_t v)    {set<uint64_t>(v);}
            void              setDateTime(uint64_t v)    {set<uint64_t>(v);}
            void              setFloat   (float    v)    {set<float>   (v);}
            void              setDouble  (double   v)    {set<double>  (v);}
            void*             setSize    (size_t)        SCIDB_FORCEINLINE;
            void              setData    (const void*,size_t);
            void              setString  (const char*);
            void              setString  (const std::string&);
            void              clear      ();
            void              swap       (Value&);

 public:                   // Operations
    template<class t>t const& get        ()        const;
    template<class t>t&       get        ();
    template<class t>void     set        (const t&);
    template<class t>void     reset      (const t&);
    template<class a>void     serialize  (a&,unsigned int);

 public:                   // Operations
    template<class t>t const* getData    ()        const {return static_cast<t const*>(data());}
    template<class t>t*       getData    ()              {return static_cast<t*>      (data());}

 private:                  // Implementation
            bool              consistent ()        const;// Assert consistency
            void*             resize     (size_t);       // Resize data buffer
            void              reset      ();             // Clear  data buffer

 private:                  // Implementation
    static  bool              small      (size_t n)      {return n<=sizeof(_data);}
    static  bool              large      (size_t n)      {return n> sizeof(_data);}
    static  void*             calloc     (size_t);       // Wraps calloc()
    static  void*             malloc     (size_t);       // Wraps malloc()
    static  void*             realloc    (void*,size_t); // Wraps realloc()
    static  void              fail       (int);          // Throw exception

 private:                  // Representation
            enum
            {
                MR_DATUM   = -1,                         // Represents a datum
                MR_TILE    = -2                          // Represents a tile
            };

 private:                  // Representation
            int32_t           _code;                     // >=0 for 'missing'
            uint32_t          _size;                     // Size of the buffer
    union { void*             _data;                     // The data buffer
            RLEPayload*       _tile; };                  // The tile pointer
};

/****************************************************************************/
}
/****************************************************************************/
#endif
/****************************************************************************/
