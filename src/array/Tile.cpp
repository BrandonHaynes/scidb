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
 * @file Tile.cpp
 *
 * @brief Implementation of Tile related functionality
 *
 */
#include <iostream>
#include <log4cxx/logger.h>
#include <util/Platform.h>
#include <util/PointerRange.h>
#include <system/Exceptions.h>
#include <array/Tile.h>

namespace scidb
{
static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.array.tile"));

namespace rle {
std::ostream& operator << ( std::ostream& out,
                            const scidb::rle::Segment& segment )
{
    out << "Segment { start position = " << segment.getStartPosition()
        << ", _dataIndex = " << segment.getDataIndex()
        << ", _isrun = "     << segment.isRun()
        << ", _isNull = "   << segment.isNull() << " }";

    return out;
}
}  //namespace rle


void TileFactory::registerConstructor(const scidb::TypeId tID,
                                      const BaseEncoding::EncodingID eID,
                                      const TileConstructor& constructor)
{
    assert(constructor);
    assert(eID>0);
    assert(!tID.empty());
    KeyType tfKey(eID, tID);

    std::pair<TileFactoryMap::iterator, bool> res =
       _factories.insert(TileFactoryMap::value_type(tfKey, constructor));
    if (!res.second) {
        assert(false);
        std::stringstream ss;
        ss << "TileFactory::registerConstructor(" << tID << "," << eID <<")";
        throw SYSTEM_EXCEPTION(SCIDB_SE_TYPESYSTEM, SCIDB_LE_OPERATION_FAILED) << ss.str();
    }
}

boost::shared_ptr<BaseTile>
TileFactory::construct(const scidb::TypeId tID,
                       const BaseEncoding::EncodingID eID,
                       const BaseTile::Context* ctx)
{
    assert (!_factories.empty());

    KeyType tfKey(eID, tID);
    TileFactoryMap::iterator iter = _factories.find(tfKey);
    if (iter == _factories.end()) {
        scidb::TypeId bufTypeId("scidb::Value");
        KeyType bufTypeKey(eID, bufTypeId);
        iter = _factories.find(bufTypeKey);
    }
    if (iter == _factories.end()) {
        assert(false);
        throw std::runtime_error("unknown type for tile");
        return boost::shared_ptr<BaseTile>();
    }
    return (iter->second)(tID, eID, ctx);
}

std::ostream& operator<< ( std::ostream& out,
                           const std::vector<char>& vec )
{
    out << "std::vector<char> [";
    insertRange(out, vec.begin(), vec.end(), " ");
    out << "] ";
    return out;
}

void TileFactory::registerBuiltinTypes()
{
    registerBuiltin<char,    RLEEncoding>(TID_CHAR,  BaseEncoding::RLE);
    registerBuiltin<int8_t,  RLEEncoding>(TID_INT8,  BaseEncoding::RLE);
    registerBuiltin<int16_t, RLEEncoding>(TID_INT16, BaseEncoding::RLE);
    registerBuiltin<int32_t, RLEEncoding>(TID_INT32, BaseEncoding::RLE);
    registerBuiltin<int64_t, RLEEncoding>(TID_INT64, BaseEncoding::RLE);

    registerBuiltin<uint8_t,  RLEEncoding>(TID_UINT8,  BaseEncoding::RLE);
    registerBuiltin<uint16_t, RLEEncoding>(TID_UINT16, BaseEncoding::RLE);
    registerBuiltin<uint32_t, RLEEncoding>(TID_UINT32, BaseEncoding::RLE);
    registerBuiltin<uint64_t, RLEEncoding>(TID_UINT64, BaseEncoding::RLE);

    registerBuiltin<float,  RLEEncoding>(TID_FLOAT,  BaseEncoding::RLE);
    registerBuiltin<double, RLEEncoding>(TID_DOUBLE, BaseEncoding::RLE);
    //XXX vector<bool>::iterator is causing trouble, needs a specialization
    // registerBuiltin<bool,   RLEEncoding>(TID_BOOL,   BaseEncoding::RLE);

    registerBuiltin<scidb::Value, RLEEncoding>("scidb::Value", BaseEncoding::RLE);
    registerBuiltin<scidb::Coordinates, ArrayEncoding>("scidb::Coordinates", BaseEncoding::ARRAY);
}

template<>
void RLEEncoding<scidb::Value>::initializeInternalData(const char* startData,
                                                       const char* endData,
                                                       size_t elemSize)
{
    const char * func = "RLEEncoding<scidb::Value>::initializeInternalData";
    for (std::vector<scidb::Value>::iterator iter = _data.begin();
         startData != endData;
         ++iter, startData +=elemSize) {
        assert(iter != _data.end());
        scidb::Value& el = *iter;
        assert(startData <= (endData-elemSize));
        el.setData(startData, elemSize);

        LOG4CXX_TRACE(logger, func << " this = "<< this <<" next value = "<<el);
    }
}

} //namespace scidb
