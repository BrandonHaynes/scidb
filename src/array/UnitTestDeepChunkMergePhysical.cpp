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

#include <query/Operator.h>
#include <array/Metadata.h>
#include <system/Cluster.h>
#include <query/Query.h>
#include <boost/make_shared.hpp>
#include <system/Exceptions.h>
#include <system/Utils.h>
#include <log4cxx/logger.h>
#include <util/NetworkMessage.h>
#include <array/RLE.h>

#include "DeepChunkMerger.h"
using namespace boost;
using namespace std;

namespace scidb
{
static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.unittest"));

class UnitTestDeepChunkMergePhysical: public PhysicalOperator
{
    typedef map<Coordinate, Value> CoordValueMap;
    typedef std::pair<Coordinate, Value> CoordValueMapEntry;
public:

    UnitTestDeepChunkMergePhysical(const string& logicalName, const string& physicalName,
                    const Parameters& parameters, const ArrayDesc& schema)
    : PhysicalOperator(logicalName, physicalName, parameters, schema)
    {
    }

    void preSingleExecute(shared_ptr<Query> query)
    {
    }

    /**
     * Generate a random value.
     * The function should be extended to cover all types and all special values such as NaN, and then be moved to a public header file.
     * @param[in]    type        the type of the value
     * @param[inout] value       the value to be filled
     * @param[in]    percentNull a number from 0 to 100, where 0 means never generate null, and 100 means always generate null
     * @return       the value from the parameter
     */
    Value& genRandomValue(TypeId const& type, Value& value, int percentNull, int nullReason)
    {
        assert(percentNull>=0 && percentNull<=100);

        if (percentNull>0 && rand()%100<percentNull) {
            value.setNull(nullReason);
        } else if (type==TID_INT64) {
            value.setInt64(rand());
        } else if (type==TID_BOOL) {
            value.setBool(rand()%100<50);
        } else if (type==TID_STRING) {
            vector<char> str;
            const size_t maxLength = 300;
            const size_t minLength = 1;
            assert(minLength>0);
            size_t length = rand()%(maxLength-minLength) + minLength;
            str.resize(length + 1);
            for (size_t i=0; i<length; ++i) {
                int c;
                do {
                    c = rand()%128;
                } while (! isalnum(c));
                str[i] = (char)c;
            }
            str[length-1] = 0;
            value.setString(&str[0]);
        } else {
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_UNITTEST_FAILED)
                << "UnitTestDeepChunkMergePhysical" << "genRandomValue";
        }
        return value;
    }

    /**
     * Given a value, return a human-readable string for its value.
     * @note This should eventually be factored out to the include/ directory.
     * @see ArrayWriter
     */
   inline string valueToString(Value const& value, TypeId const& type)
    {
        std::stringstream ss;

        if (value.isNull()) {
            ss << "?(" << value.getMissingReason() << ")";
        } else if (type==TID_INT64) {
            ss << value.getInt64();
        } else if (type==TID_BOOL) {
            ss << (value.getBool() ? "true" : "false");
        } else if (type==TID_STRING) {
            ss << value.getString();
        } else {
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_UNITTEST_FAILED)
                << "UnitTestDeepChunkMergePhysical" << "value2string";
        }
        return ss.str();
    }

    /**
     * Insert data from a map to an array.
     * @param[in]    query
     * @param[inout] array  the array to receive data
     * @param[in]    m      the map of Coordinate --> Value
     * @param[in]    whetherAttachBitmap  whether the bitmap itself should be attached to the end of the data chunk
     */
    void insertMapDataIntoArray(shared_ptr<Query>& query, MemArray& array, CoordValueMap const& m, bool whetherAttachBitmap)
    {
        shared_ptr<ArrayIterator> arrayIter = array.getIterator(0);
        Coordinates coord(1), coordZero(1);
        coordZero[0] = 0;
        MemChunk& chunk = (MemChunk&)arrayIter->newChunk(coordZero);
        shared_ptr<ChunkIterator> chunkIter = chunk.getIterator(query, ChunkIterator::SEQUENTIAL_WRITE);
        BOOST_FOREACH(CoordValueMapEntry const& p, m) {
            coord[0] = p.first;
            chunkIter->setPosition(coord);
            chunkIter->writeItem(p.second);
        }
        chunkIter->flush();

        // The code segment below will attach the empty bitmap to the end of the data chunk.
        if (whetherAttachBitmap) {
            MemChunk tmpChunk;
            tmpChunk.initialize(chunk);
            chunk.makeClosure(tmpChunk, chunk.getEmptyBitmap());
            chunk.reallocate(tmpChunk.getSize());
            memcpy(chunk.getData(), tmpChunk.getData(), tmpChunk.getSize());
        }
    }

    /**
     * Test deep-chunk merge once.
     * The method generates two one-chunk arrays, randomly fill the data in the chunks, merge the two chunks, and check correctness.
     * For each chunk, there is 90% possibility that the bitmap is attached to the end of it.
     * For each cell, there is 20% possibility that it is empty.
     * For each value, there is 10% possibility that it is null.
     *
     * @param[in]   query
     * @param[in]   type     the value type
     * @param[in]   start    the start coordinate of the dim
     * @param[in]   end      the end coordinate of the dim
     * @param[in]   chunkInterval  the chunk interval
     *
     * @throw SCIDB_SE_INTERNAL::SCIDB_LE_UNITTEST_FAILED
     */
    void testOnce_DeepChunkMerge(boost::shared_ptr<Query>& query, TypeId const& type,
            Coordinate start, Coordinate end, int64_t chunkInterval)
    {
        const int percentAttachBitmap = 90;
        const int percentEmpty = 20;
        const int percentNullValue = 10;
        const int missingReason = 0;

        // Array schema
        vector<AttributeDesc> attributes(1);
        attributes[0] = AttributeDesc((AttributeID)0, "dummy_attribute",  type, AttributeDesc::IS_NULLABLE, 0);
        vector<DimensionDesc> dimensions(1);
        dimensions[0] = DimensionDesc(string("dummy_dimension"), start, end, chunkInterval, 0);
        ArrayDesc schema("dummy_array", addEmptyTagAttribute(attributes), dimensions);

        // Define two one-chunk arrays, simulating fragments appearing in different instances.
        MemArray arrayInstOne(schema,query), arrayInstTwo(schema,query);

        // Generate source data in the form of maps.
        CoordValueMap mapInstOne, mapInstTwo;
        Value value;
        for (Coordinate i=start; i<(min(end+1, start+chunkInterval)); ++i) {
            if (! rand()%100<percentEmpty) {
                mapInstOne[i] = genRandomValue(type, value, percentNullValue, missingReason);
            }
            if (! rand()%100<percentEmpty) {
                mapInstTwo[i] = genRandomValue(type, value, percentNullValue, missingReason);
            }
        }

        // Insert the map data into the array chunks.
        insertMapDataIntoArray(query, arrayInstOne, mapInstOne, rand()%100<percentAttachBitmap);
        insertMapDataIntoArray(query, arrayInstTwo, mapInstTwo, rand()%100<percentAttachBitmap);

        // Merge
        // After merging attrId=0 (the real attribute), the empty bitmap will be out-of-date temporarily.
        for (AttributeID attrId=0; attrId<2; ++attrId ) {
            Coordinates coord(1);
            coord[0] = 0;
            shared_ptr<ArrayIterator> arrayIterInstOne = arrayInstOne.getIterator(attrId);
            arrayIterInstOne->setPosition(coord);
            MemChunk& chunkInstOne = (MemChunk&)arrayIterInstOne->updateChunk();
            shared_ptr<ConstArrayIterator> constArrayIterInstTwo = arrayInstTwo.getConstIterator(attrId);
            constArrayIterInstTwo->setPosition(coord);
            MemChunk const& chunkInstTwo = (MemChunk const&)constArrayIterInstTwo->getChunk();

            chunkInstOne.deepMerge(chunkInstTwo, query);
        }

        // Check correctness.
        // - Copy data from mapInstTwo to mapInstOne. This is the truth.
        // - Retrieve all data from the first array. This is the merged result.
        // - Make sure the truth and the merged result are equal.
        CoordValueMap mergedResult, expectedResult;
        shared_ptr<ConstArrayIterator> constArrayIterInstOne = arrayInstOne.getConstIterator(0);
        constArrayIterInstOne->reset();
        MemChunk& chunkInstOne = (MemChunk&)constArrayIterInstOne->getChunk();
        shared_ptr<ConstChunkIterator> const& constChunkIterInstOne = chunkInstOne.getConstIterator(ChunkIterator::IGNORE_EMPTY_CELLS);
        while (!constChunkIterInstOne->end()) {
            Coordinates const& coord = constChunkIterInstOne->getPosition();
            Value& v = constChunkIterInstOne->getItem();
            mergedResult[coord[0]] = v;
            ++(*constChunkIterInstOne);
        }
        BOOST_FOREACH(CoordValueMapEntry const& p, mapInstOne) {
            expectedResult[p.first] = p.second;
        }
        BOOST_FOREACH(CoordValueMapEntry const& p, mapInstTwo) {
            expectedResult[p.first] = p.second;
        }

        try {
            BOOST_FOREACH(CoordValueMapEntry const& p, expectedResult) {
                CoordValueMap::const_iterator mapIter = mergedResult.find(p.first);
                if (mapIter==mergedResult.end()) {
                    throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_UNITTEST_FAILED) << "UnitTestDeepChunkMergePhysical" << "merge result has too few data";
                }
                if (p.second!=mapIter->second) {
                    throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_UNITTEST_FAILED) << "UnitTestDeepChunkMergePhysical" << "merge result has incorrect data";
                }
            }
            BOOST_FOREACH(CoordValueMapEntry const& p, mergedResult) {
                CoordValueMap::const_iterator mapIter = expectedResult.find(p.first);
                if (mapIter==expectedResult.end()) {
                    throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_UNITTEST_FAILED) << "UnitTestDeepChunkMergePhysical" << "merge result has too much data";
                }
                if (p.second!=mapIter->second) {
                    throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_UNITTEST_FAILED) << "UnitTestDeepChunkMergePhysical" << "merge result has incorrect data";
                }
            }
        } catch (Exception& e) {
            LOG4CXX_DEBUG(logger, "[Failure details] type=" << type << ", end=" << end << ", interval=" << chunkInterval);

            LOG4CXX_DEBUG(logger, "[Failure details] Dst array original:");
            BOOST_FOREACH(CoordValueMapEntry const& p, mapInstOne) {
                LOG4CXX_DEBUG(logger, "[" << p.first << "]: " << valueToString(p.second, type));
            }

            LOG4CXX_DEBUG(logger, "[Failure details] With array original:");
            BOOST_FOREACH(CoordValueMapEntry const& p, mapInstTwo) {
                LOG4CXX_DEBUG(logger, "[" << p.first << "]: " << valueToString(p.second, type));
            }

            LOG4CXX_DEBUG(logger, "[Failure details] Expected merged result:");
            BOOST_FOREACH(CoordValueMapEntry const& p, expectedResult) {
                LOG4CXX_DEBUG(logger, "[" << p.first << "]: " << valueToString(p.second, type));
            }

            LOG4CXX_DEBUG(logger, "[Failure details] Actual merged result:");
            BOOST_FOREACH(CoordValueMapEntry const& p, mergedResult) {
                LOG4CXX_DEBUG(logger, "[" << p.first << "]: " << valueToString(p.second, type));
            }

            throw;
        }
     }

    boost::shared_ptr<Array> execute(vector< boost::shared_ptr<Array> >& inputArrays, boost::shared_ptr<Query> query)
    {
        srand(time(NULL));

        for (Coordinate end=1; end<10; ++end) {
            for (int64_t interval=1; interval<15; ++interval) {
                testOnce_DeepChunkMerge(query, TID_INT64, 0, end, interval);
                testOnce_DeepChunkMerge(query, TID_BOOL, 0, end, interval);
                testOnce_DeepChunkMerge(query, TID_STRING, 0, end, interval);
            }
        }

        return shared_ptr<Array> (new MemArray(_schema,query));
    }

};

REGISTER_PHYSICAL_OPERATOR_FACTORY(UnitTestDeepChunkMergePhysical, "test_deep_chunk_merge", "UnitTestDeepChunkMergePhysical");
}
