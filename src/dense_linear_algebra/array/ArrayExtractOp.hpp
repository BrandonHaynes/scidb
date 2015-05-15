#ifndef ARRAYEXTRACTOP_HPP_
#define ARRAYEXTRACTOP_HPP_

///
/// ArrayExtractOp.hpp
///

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

// C++
#include <limits>
#include <sstream>

// boost
#include <boost/shared_ptr.hpp>

// scidb
#include <array/Array.h>
#include <array/MemArray.h>
#include <array/Metadata.h>
#include <array/Tile.h>
#include <array/TileIteratorAdaptors.h>
#include <log4cxx/logger.h>
#include <system/Exceptions.h>
#include <system/Sysinfo.h>
#include <util/Platform.h>


namespace scidb
{
static log4cxx::LoggerPtr extractOpLogger(log4cxx::Logger::getLogger("scidb.libdense_linear_algebra.array.extractOp"));

/// The following is a similar calling sequence to Array::extractData()
/// but adds a template parameter abstracting what is to be done with the data, and drops the memory pointer.
/// In theory, Array::extractData() could be implemented using this template.
///
/// This implementation uses the new tile iterator paradigm, rather than directly accessing the many
/// possible formats of chunk that could be passed to the function.

template<class ExtractOp_tt>
void extractDataToOp(shared_ptr<scidb::Array> array, scidb::AttributeID attrID,
                     scidb::Coordinates const& first, scidb::Coordinates const& last,
                     ExtractOp_tt& extractOp, const shared_ptr<Query>& query)
{
    typedef double Value_t ;   // likely future template parameter

    const bool DBG=false ;

    scidb::ArrayDesc const& arrayDesc =  array->getArrayDesc();
    scidb::AttributeDesc const& attrDesc = arrayDesc.getAttributes()[attrID];

    scidb::Dimensions const& dims = arrayDesc.getDimensions();
    size_t nDims = dims.size();
    assert(nDims == 2); // matrix operations only, currently

    scidb::Type attrType(scidb::TypeLibrary::getType(attrDesc.getType()));
    if (attrType.variableSize())
        throw USER_EXCEPTION(scidb::SCIDB_SE_EXECUTION, scidb::SCIDB_LE_EXTRACT_EXPECTED_FIXED_SIZE_ATTRIBUTE);

    if (attrType.bitSize() < 8)
        throw USER_EXCEPTION(scidb::SCIDB_SE_EXECUTION, scidb::SCIDB_LE_EXTRACT_UNEXPECTED_BOOLEAN_ATTRIBUTE);

    if (first.size() != nDims || last.size() != nDims)
        throw USER_EXCEPTION(scidb::SCIDB_SE_EXECUTION, scidb::SCIDB_LE_WRONG_NUMBER_OF_DIMENSIONS);

    size_t attrSize = attrType.byteSize();
    if (attrSize != sizeof(Value_t)) {
        throw USER_EXCEPTION(scidb::SCIDB_SE_EXECUTION, scidb::SCIDB_LE_WRONG_ATTRIBUTE_TYPE); // TODO: really WRONG_ATTRIBUTE_SIZE
    }



    boost::shared_ptr<scidb::ConstArrayIterator> chunksIt;
    for(chunksIt = array->getConstIterator(/*attrid*/0); ! chunksIt->end(); ++(*chunksIt) ) {
        if(DBG) std::cerr << "extractDataToOp: next chunksIt" << std::endl ;

        scidb::ConstChunk const& chunk = chunksIt->getChunk();
        scidb::Coordinates chunkOrigin(2); chunkOrigin = chunk.getFirstPosition(false);
        scidb::Coordinates chunkLast(2); chunkLast = chunk.getLastPosition(false);

        shared_ptr<ConstChunkIterator> itChunk = chunk.getConstIterator();
        if( !dynamic_cast<RLETileConstChunkIterator*>(itChunk.get()) &&
            !dynamic_cast<BufferedConstChunkIterator< boost::shared_ptr<RLETileConstChunkIterator> >* >(itChunk.get()) ) {
            // these iterators have functioning getData() implementations
            // see Tigor for more details
            // XXX TODO: can these checks be moved "inside" the[an?] emulation layer?
            itChunk = make_shared<
                         TileConstChunkIterator<
                            shared_ptr<ConstChunkIterator> > >(itChunk, query);
        }
        assert(itChunk->getLogicalPosition()>=0);

        // use about 1/2 of L1, the other half is for the destination
        const size_t MAX_VALUES_TO_GET = Sysinfo::getCPUCacheSize(Sysinfo::CPU_CACHE_L1)/2/sizeof(Value_t);

        extractOp.blockBegin();
        // for all non-zeros in chunk (memory is already zeroed)
        Coordinates coords(2);
        for (position_t offset = itChunk->getLogicalPosition(); offset >= 0; ) {
            boost::shared_ptr<BaseTile> tileData;
            boost::shared_ptr<BaseTile> tileCoords;
            offset = itChunk->getData(offset, MAX_VALUES_TO_GET, tileData, tileCoords);
            if (!tileData) {
                assert(!tileCoords);
                break;
            }

            // Tigor: seems like these asserts should be inside the getData() call above, not the caller
            assert(tileData);
            assert(tileCoords);
            assert(tileData->size() == tileCoords->size());
            assert(tileData->size() > 0);

            // XXX TODO: should provide Tile<>::iterators instead of dealing with the encodings etc.
            Tile<Coordinates, ArrayEncoding >* coordTileTyped =
               safe_dynamic_cast< Tile<Coordinates, ArrayEncoding >* >(tileCoords.get());
            assert(coordTileTyped);

            RLEEncoding<Value_t>* dataTyped = safe_dynamic_cast< RLEEncoding <Value_t>* >(tileData->getEncoding());
            typename RLEEncoding<Value_t>::const_iterator dataIter = dataTyped->begin();
            assert(coordTileTyped->size() == tileCoords->size());
            assert(dataTyped->size() == tileData->size());

            for (size_t i=0, n=coordTileTyped->size(); i < n; ++i, ++dataIter) {
                assert(dataIter != dataTyped->end());
                if(dataIter.isNull()) {
                    throw USER_EXCEPTION(scidb::SCIDB_SE_EXECUTION, scidb::SCIDB_LE_CANT_CONVERT_NULL);
                }

                const Value_t val = (*dataIter);

                coordTileTyped->at(i,coords);
                assert(coords.size()==2);
                extractOp(val, coords[0], coords[1]);
            }
        }
        extractOp.blockEnd();
    }
}


} //namespace

#endif // ARRAYEXTRACTOP_HPP
