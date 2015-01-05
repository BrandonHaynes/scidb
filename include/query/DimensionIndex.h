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

#ifndef DIMENSION_INDEX_H_
#define DIMENSION_INDEX_H_


#include <set>
#include <map>
#include <boost/make_shared.hpp>
#include "system/Exceptions.h"
#include "query/TypeSystem.h"
#include "query/FunctionLibrary.h"

namespace scidb
{

using namespace std;
using namespace boost;

// Compare attribute values using "<" function for attribute type
class AttributeComparator
{
  public:
    AttributeComparator()
     : _less(0)
    {}

    AttributeComparator(TypeId tid)
     : _less(getComparison(tid))
    {}

  public:
    bool operator()(const Value& v1, const Value& v2) const
    {
        const Value* operands[2] = {&v1,&v2};
        Value result;
        _less(operands, &result, NULL);
        return result.getBool();
    }

  private:
    static FunctionPointer getComparison(TypeId tid)
    {
        vector<TypeId>          inputTypes(2,tid);
        FunctionDescription     functionDesc;
        vector<FunctionPointer> converters;

        if (!FunctionLibrary::getInstance()->findFunction("<", inputTypes, functionDesc, converters, false) || !converters.empty())
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_EXECUTION,SCIDB_LE_OPERATION_NOT_FOUND) << "<" << tid;
        }

        return functionDesc.getFuncPtr();
    }

  private:
    FunctionPointer _less;
};

// Set of attribute values.
// Tempalte is used to support both classical set and multiset
// Double value is handled in special way because it is expected to be the most frequently used type for user defined coordinates
template<typename DoubleSet, typename ValueSet>
class __Attribute_XSet : AttributeComparator
{
    typedef typename DoubleSet::const_iterator DoubleIterator;
    typedef typename ValueSet::const_iterator ValueIterator;

  public:
    __Attribute_XSet(TypeId tid) : AttributeComparator(tid), totalSize(0), valueSet(*this)
    {
    }
    size_t size() const
    {
        return (typeID == TID_DOUBLE) ? doubleSet.size() : valueSet.size();
    }

    // Add single value
    void add(Value const& item)
    {
        if (typeID == TID_DOUBLE)
        {
            size_t origSize = doubleSet.size();
            doubleSet.insert(item.getDouble());
            if (doubleSet.size() > origSize )
            {
                totalSize += sizeof(double);
            }
        } else {
            size_t origSize = valueSet.size();
            valueSet.insert(item);
            if (valueSet.size() > origSize)
            {
                totalSize += item.size();
                if (type.variableSize()) {
                    totalSize += item.size()-1 >= 0xFF ? 5 : 1;
                }
            }
        }
    }

    // Add data from buffer
    void add(void const* data, size_t size)
    {
        if (typeID == TID_DOUBLE) {
            double* src = (double*)data;
            double* end = src + size/sizeof(double);
            while (src < end)
            {
                size_t origSize = doubleSet.size();
                doubleSet.insert(*src++);
                if (doubleSet.size()>origSize)
                {
                    totalSize += sizeof(double);
                }
            }
        } else {
            Value value;
            uint8_t* src = (uint8_t*)data;
            uint8_t* end = src + size;
            size_t attrSize = type.byteSize();
            if (attrSize == 0) {
                while (src < end) {
                    if (*src == 0) {
                        attrSize = (src[1] << 24) | (src[2] << 16) | (src[3] << 8) | src[4];
                        src += 5;
                    } else {
                        attrSize = *src++;
                    }
                    value.setData(src, attrSize);

                    size_t origSize = valueSet.size();
                    valueSet.insert(value);
                    if(valueSet.size()>origSize)
                    {
                        totalSize += attrSize + (attrSize-1 >= 0xFF ? 5 : 1);
                    }
                    src += attrSize;
                }
            } else {
                while (src < end) {
                    value.setData(src, attrSize);

                    size_t origSize = valueSet.size();
                    valueSet.insert(value);
                    if(valueSet.size()>origSize)
                    {
                        totalSize += attrSize;
                    }
                    src += attrSize;
                }
            }
        }
    }

    // Add data from shared buffer
    void add(const shared_ptr<SharedBuffer>& buf, InstanceID instance = 0)
    {
        if (buf) {
            add(buf->getData(), buf->getSize());
        }
    }

    //
    // Perform sort of data and return pointer to the buffer with sorted data.
    // Format of the buffer depends on whether it is used at first stage (partial sort) or at second stage (merge of partially sorted data)
    shared_ptr<SharedBuffer> sort(bool partial)
    {
        if (!partial)
        {
            // for final sort we store number of value (coordiantes) and offsets to bodies for varying size type
            if (type.variableSize())
            {
                totalSize += valueSet.size()*sizeof(int);
            }
            totalSize += sizeof(size_t); // number of coordinates
        }

        MemoryBuffer* buf = new MemoryBuffer(NULL, totalSize);
        char* dst = (char*)buf->getData();
        if (typeID == TID_DOUBLE) {
            double* dp = (double*)dst;
            for (DoubleIterator i = doubleSet.begin(); i != doubleSet.end(); ++i) {
                *dp++ = *i;
            }
            dst = (char*)dp;
        } else {
            size_t attrSize = type.byteSize();
            if (attrSize == 0) { // varying size type
                int* offsPtr = (int*)dst;
                char* base = NULL;
                if (!partial) {
                    base = (char*)(offsPtr + valueSet.size());
                    dst = base;
                }
                for (ValueIterator i = valueSet.begin(); i != valueSet.end(); ++i) {
                    attrSize = i->size();
                    if (!partial) {
                        *offsPtr++ = (int)(dst - base); // offset to body
                    }
                    // Simple encoding for length of varying size types: use one byte for values with size < 255 and 5 bytes for others
                    if (attrSize-1 >= 0xFF) {
                        *dst++ = '\0';
                        *dst++ = char(attrSize >> 24);
                        *dst++ = char(attrSize >> 16);
                        *dst++ = char(attrSize >> 8);
                    }
                    *dst++ = char(attrSize);
                    memcpy(dst, i->data(), attrSize);
                    dst += attrSize;
                }
            } else { // fixed size type
                for (ValueIterator i = valueSet.begin(); i != valueSet.end(); ++i) {
                    memcpy(dst, i->data(), attrSize);
                    dst += attrSize;
                }
            }
        }
        if (!partial)
        {
            *(size_t*)dst = size(); // number of written values
            dst += sizeof(size_t);
        }
        assert(dst == (char*)buf->getData() + buf->getSize());
        return shared_ptr<SharedBuffer>(buf);
    }

    TypeId getType()
    {
        return typeID;
    }

    TypeId const typeID;
    Type const type;
    DoubleSet doubleSet;
    size_t totalSize;
    ValueSet valueSet;
};

struct DoubleKey
{
    double key;
    InstanceID instance;

    bool operator <(DoubleKey const& other) const
    {
        return key < other.key;
    }

    DoubleKey(double k, InstanceID n) : key(k), instance(n) {}
    DoubleKey() {}
};

struct ValueKey
{
    Value  key;
    InstanceID instance;

    ValueKey(Value const& k, InstanceID n) : key(k), instance(n) {}
    ValueKey() {}
};

// Comparator of Values
class ValueKeyComparator : public AttributeComparator
{
  public:
    bool operator()(const ValueKey& v1, const ValueKey& v2) const
    {
        return AttributeComparator::operator()(v1.key, v2.key);
    }

    ValueKeyComparator()
    {
    }

    ValueKeyComparator(TypeId tid) : AttributeComparator(tid) {}
};


// Bag of values of non-unique attributes. The main difference with AttributeMultiSet (__Attribute_XSet< multiset<double>, multiset<Value, AttributeComparator> >)
// is that we have to store extra information about instance at which this value is located.
// It is needed to correctly merge of bags from different instances.
// Methods are the same as in __Attribute_XSet
class AttributeBag : ValueKeyComparator
{
  public:
    AttributeBag(TypeId tid) : ValueKeyComparator(tid), typeID(tid), type(TypeLibrary::getType(tid)), totalSize(0), valueSet(*this)
    {
    }

    size_t size() const
    {
        return (typeID == TID_DOUBLE) ? doubleSet.size() : valueSet.size();
    }

    void add(Value const& item, InstanceID instance = 0)
    {
        if (typeID == TID_DOUBLE) {
            doubleSet.insert(DoubleKey(item.getDouble(), instance));
            totalSize += sizeof(ValueKey);
        } else {
            valueSet.insert(ValueKey(item, instance));
            totalSize += item.size();
            if (type.variableSize()) {
                totalSize += item.size()-1 >= 0xFF ? 5 : 1;
            }
        }
    }

    void add(void const* data, size_t size, InstanceID instance)
    {
        totalSize += size;
        if (typeID == TID_DOUBLE) {
            DoubleKey dk;
            dk.instance = instance;
            double* src = (double*)data;
            double* end = src + size/sizeof(double);
            while (src < end)
            {
                dk.key = *src++;
                doubleSet.insert(dk);
            }
        } else {
            ValueKey vk;
            uint8_t* src = (uint8_t*)data;
            uint8_t* end = src + size;
            size_t attrSize = type.byteSize();
            vk.instance = instance;

            if (attrSize == 0) {
                while (src < end) {
                    if (*src == 0) {
                        attrSize = (src[1] << 24) | (src[2] << 16) | (src[3] << 8) | src[4];
                        src += 5;
                    } else {
                        attrSize = *src++;
                    }
                    vk.key.setData(src, attrSize);
                    valueSet.insert(vk);
                    src += attrSize;
                }
            } else {
                while (src < end) {
                    vk.key.setData(src, attrSize);
                    valueSet.insert(vk);
                    src += attrSize;
                }
            }
        }
    }

    void add(shared_ptr<SharedBuffer> buf, InstanceID instance)
    {
        if (buf) {
            add(buf->getData(), buf->getSize(), instance);
        }
    }

    shared_ptr<SharedBuffer> sort(bool partial)
    {
        if (!partial)
        {
            if (type.variableSize())
            {
                totalSize += valueSet.size()*sizeof(int); // string offsets
            }
            totalSize += size()*sizeof(uint16_t); // instance IDs
            totalSize += sizeof(size_t); // number of coordinates
        }

        shared_ptr<MemoryBuffer> buf(boost::make_shared<MemoryBuffer>((void*)NULL, totalSize));
        char* dst = (char*)buf->getData();
        uint16_t* np = (uint16_t*)(dst + totalSize - sizeof(size_t) - size()*sizeof(uint16_t));
        if (typeID == TID_DOUBLE) {
            double* dp = (double*)dst;
            for (multiset<DoubleKey>::iterator i = doubleSet.begin(); i != doubleSet.end(); ++i) {
                *dp++ = i->key;
                if (!partial) {
                    *np++ = i->instance;
                }
            }
            dst = (char*)dp;
        } else {
            size_t attrSize = type.byteSize();
            if (attrSize == 0) {
                int* offsPtr = (int*)dst;
                char* base = NULL;
                if (!partial) {
                    base = (char*)(offsPtr + valueSet.size());
                    dst = base;
                }
                for (multiset<ValueKey, ValueKeyComparator>::const_iterator i = valueSet.begin(); i != valueSet.end(); ++i) {
                    attrSize = i->key.size();
                    if (!partial) {
                        *offsPtr++ = (int)(dst - base);
                    }
                    if (attrSize-1 >= 0xFF) {
                        *dst++ = '\0';
                        *dst++ = char(attrSize >> 24);
                        *dst++ = char(attrSize >> 16);
                        *dst++ = char(attrSize >> 8);
                    }
                    *dst++ = char(attrSize);
                    memcpy(dst, i->key.data(), attrSize);
                    dst += attrSize;
                    if (!partial) {
                        *np++ = i->instance;
                    }
                }
            } else {
                for (multiset<ValueKey, ValueKeyComparator>::const_iterator i = valueSet.begin(); i != valueSet.end(); ++i) {
                    memcpy(dst, i->key.data(), attrSize);
                    dst += attrSize;
                    if (!partial) {
                        *np++ = i->instance;
                    }
                }
            }
        }
        if (!partial)
        {
            dst = (char*)np;
            *(size_t *)dst = size();
            dst += sizeof(size_t);
        }
        assert(dst == (char*)buf->getData() + buf->getSize());
        return buf;
    }

    TypeId getType()
    {
        return typeID;
    }

  private:
    TypeId const typeID;
    Type const type;
    multiset<DoubleKey> doubleSet;
    size_t totalSize;
    multiset<ValueKey, ValueKeyComparator> valueSet;
};

typedef __Attribute_XSet< set<double>, set<Value, AttributeComparator> > AttributeSet;
typedef __Attribute_XSet< multiset<double>, multiset<Value, AttributeComparator> > AttributeMultiSet;

// Class used to map original coordinate value (user defined coordinate) into ordinal (integer) coordinate and visa versa.
// Current implementation use sorted array and binary search.
// Template is used to support both unique and non-unique attributes.
// Double value is handled in special way because it is expected to be the most frequently used type for user defined coordinates
template<typename DoubleMap, typename ValueMap>
class __Attribute_XMap : AttributeComparator
{
    typedef typename DoubleMap::const_iterator DoubleIterator;
    typedef typename ValueMap::const_iterator ValueIterator;

  public:
    __Attribute_XMap(DimensionDesc const& dim, FunctionPointer to, FunctionPointer from)
    :
        _toOrdinal(to),
        _fromOrdinal(from),
        _start(dim.getStart()),
        _length(dim.getLength())
    {}

    __Attribute_XMap(TypeId tid, Coordinate start, size_t nCoords, void const* data, size_t size)
    : AttributeComparator(tid), typeID(tid), type(TypeLibrary::getType(tid)), valueMap(*this), _start(start), _toOrdinal(NULL), _fromOrdinal(NULL)
    {
        if (typeID == TID_DOUBLE) {
            double* src = (double*)data;
            double* end = src + size/sizeof(double);
            while (src < end) {
                doubleMap.insert(make_pair(*src++, start++));
            }
        } else {
            Value value;
            uint8_t* src = (uint8_t*)data;
            uint8_t* end = src + size;
            size_t attrSize = type.byteSize();
            if (attrSize == 0) {
                int* offsPtr = (int*)src;
                uint8_t* base = src + nCoords*sizeof(int);
                for (size_t i = 0; i < nCoords; i++) {
                    src = base + offsPtr[i];
                    if (*src == 0) {
                        attrSize = (src[1] << 24) | (src[2] << 16) | (src[3] << 8) | src[4];
                        src += 5;
                    } else {
                        attrSize = *src++;
                    }
                    value.setData(src, attrSize);
                    valueMap.insert(make_pair(value,start++));
                }
            } else {
                while (src < end) {
                    value.setData(src, attrSize);
                    valueMap.insert(make_pair(value,start++));
                    src += attrSize;
                }
            }
        }
    }

    __Attribute_XMap(TypeId tid, Coordinate start, size_t nCoords, void const* data, size_t size, InstanceID myInstance)
    : AttributeComparator(tid), typeID(tid), type(TypeLibrary::getType(tid)), valueMap(*this), _start(start), _toOrdinal(NULL), _fromOrdinal(NULL)
    {
        Coordinate coord = start;
        duplicates.resize(nCoords); // collect number of duplicates at each instance
        if (typeID == TID_DOUBLE) {
            double* dp = (double*)data;
            uint16_t* np = (uint16_t*)(dp + nCoords);
            for (size_t i = 0, j = 0; i < nCoords; i++) {
                doubleMap.insert(make_pair(dp[i], start + i));
                if (dp[i] != dp[j]) {
                    j = i;
                }
                if (InstanceID(np[i]) < myInstance) {
                    duplicates[j] += 1;
                }
            }
        } else {
            Value prev;
            Value value;
            uint8_t* src = (uint8_t*)data;
            uint16_t* np = (uint16_t*)(src + size) - nCoords;
            size_t attrSize = type.byteSize();
            if (attrSize == 0) {
                int* offsPtr = (int*)src;
                uint8_t* base = src + nCoords*sizeof(int);
                for (size_t i = 0, j = 0; i < nCoords; i++) {
                    src = base + offsPtr[i];
                    // Simple encoding for length of varying size types: use one byte for values with size < 255 and 5 bytes for other
                    if (*src == 0) {
                        attrSize = (src[1] << 24) | (src[2] << 16) | (src[3] << 8) | src[4];
                        src += 5;
                    } else {
                        attrSize = *src++;
                    }
                    value.setData(src, attrSize);
                    valueMap.insert(make_pair(value, coord + i));
                    src += attrSize;
                    if (value != prev) {
                        prev = value;
                        j = i;
                    }
                    if (InstanceID(np[i]) < myInstance) {
                        duplicates[j] += 1;
                    }
                }
            } else {
                for (size_t i = 0, j = 0; i < nCoords; i++) {
                    value.setData(src, attrSize);
                    src += attrSize;
                    valueMap.insert(make_pair(value, coord + i));
                    if (value != prev) {
                        prev = value;
                        j = i;
                    }
                    if (InstanceID(np[i]) < myInstance) {
                        duplicates[j] += 1;
                    }
                }
                assert(src == (uint8_t*)np);
            }
        }
    }

    void getOriginalCoordinate(Value& value, Coordinate pos, bool throwException = true) const
    {
        assert(_fromOrdinal != NULL);
        Value params[3];
        params[0].setInt64(pos);
        params[1].setInt64(_start);
        params[2].setInt64(_length);
        const Value* pParams[3] = {&params[0], &params[1], &params[2]};
        _fromOrdinal(pParams, &value, NULL);
        if (throwException) {
            if (value.isNull())
                throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_NO_MAPPING_FOR_COORDINATE);
        }
    }

    Coordinate get(Value const& value, CoordinateMappingMode mode = cmExact)
    {
        if (value.isNull()) {
            if (mode != cmTest)
                throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_NO_MAPPING_FOR_COORDINATE);
            return MIN_COORDINATE-1;
        }
        if (_toOrdinal != NULL) { // functional mapping
            const Value* params[3];
            Value result;
            params[0] = &value;
            Value p1;
            p1.setInt64(_start);
            params[1] = &p1;
            Value p2;
            p2.setInt64(_length);
            params[2] = &p2;
            _toOrdinal(params, &result, NULL);
            if (result.isNull())
                throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_NO_MAPPING_FOR_COORDINATE);
            return result.getInt64();
        }
        Coordinate coord;
        if (typeID == TID_DOUBLE) {
            DoubleIterator i;
            double d = value.getDouble();
            switch (mode) {
              case cmTest:
                i = doubleMap.find(d);
                return i == doubleMap.end() ? MIN_COORDINATE-1 : i->second;
              case cmExact:
                i = doubleMap.find(d);
                break;
              case cmLowerBound:
                //i = doubleMap.lower_bound(d);
                //break;
              case cmLowerCount:
                i = doubleMap.lower_bound(d);
                return i == doubleMap.end() ? _start + doubleMap.size() : i->second;
              case cmUpperCount:
                i = doubleMap.upper_bound(d);
                return  i == doubleMap.end() ? _start + doubleMap.size() : i->second;
              case cmUpperBound:
                i = doubleMap.upper_bound(d);
                if (i == doubleMap.begin()) {
                    return i->second-1;
                }
                --i;
                return i == doubleMap.end() ? _start - 1 : i->second;
              default:
                assert(false);
            }
            if (i == doubleMap.end())
                throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_NO_MAPPING_FOR_COORDINATE);
            coord = i->second;
        } else {
            ValueIterator i;
            switch (mode) {
              case cmTest:
                i = valueMap.find(value);
                return i == valueMap.end() ? MIN_COORDINATE-1 : i->second;
              case cmExact:
                i = valueMap.find(value);
                break;
              case cmLowerBound:
                //i = valueMap.lower_bound(value);
                //break;
              case cmLowerCount:
                i = valueMap.lower_bound(value);
                return i == valueMap.end() ? _start + valueMap.size() : i->second;
              case cmUpperCount:
                i = valueMap.upper_bound(value);
                return  i == valueMap.end() ? _start + valueMap.size() : i->second;
              case cmUpperBound:
                i = valueMap.upper_bound(value);
                if (i == valueMap.begin()) {
                    return i->second-1;
                }
                --i;
                return i == valueMap.end() ? _start - 1 : i->second;
              default:
                assert(false);
            }
            if (i == valueMap.end())
                throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_NO_MAPPING_FOR_COORDINATE);
            coord = i->second;
        }
        if (!duplicates.empty()) {
            coord += duplicates[coord - _start]++;
        }
        return coord;
    }

    bool hasFunctionMapping() const
    {
        return _toOrdinal != NULL;
    }

    size_t size() const
    {
        return (typeID == TID_DOUBLE) ? doubleMap.size() : valueMap.size();
    }

  private:
    TypeId const typeID;
    Type const type;
    DoubleMap doubleMap;
    ValueMap  valueMap;
    Coordinate _start;
    Coordinate _length;
    vector<size_t> duplicates;
    FunctionPointer _toOrdinal;
    FunctionPointer _fromOrdinal;
};

typedef __Attribute_XMap< map<double,Coordinate>, map<Value,Coordinate,AttributeComparator> > AttributeMap;
typedef __Attribute_XMap< multimap<double,Coordinate>, multimap<Value,Coordinate,AttributeComparator> > AttributeMultiMap;

}

#endif
