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
#include <boost/foreach.hpp>
#include <system/Exceptions.h>
#include <system/Utils.h>
#include <log4cxx/logger.h>
#include <util/NetworkMessage.h>
#include <array/RLE.h>
#include <array/MemArray.h>

using namespace boost;
using namespace std;

namespace scidb
{
static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.unittest"));

class UnitTestMemArrayPhysical: public PhysicalOperator
{
    typedef map<Coordinate, Value> CoordValueMap;
    typedef std::pair<Coordinate, Value> CoordValueMapEntry;
public:

    UnitTestMemArrayPhysical(const string& logicalName, const string& physicalName,
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
                << "UnitTestMemArrayPhysical" << "genRandomValue";
        }
        return value;
    }

    /**
     * Given a value, return a human-readable string for its value.
     * @note This should eventually be factored out to the include/ directory.
     * @see ArrayWriter
     */
    string valueToString(Value const& value, TypeId const& type)
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
                << "UnitTestMemArrayPhysical" << "value2string";
        }
        return ss.str();
    }

    /**
     * Insert data from a map to an array.
     * @param[in]    query
     * @param[inout] array  the array to receive data
     * @param[in]    m      the map of Coordinate --> Value
     */
    void insertMapDataIntoArray(shared_ptr<Query>& query, MemArray& array, CoordValueMap const& m)
    {
        Coordinates coord(1);
        coord[0] = 0;
        vector< shared_ptr<ArrayIterator> > arrayIters(array.getArrayDesc().getAttributes(true).size());
        vector< shared_ptr<ChunkIterator> > chunkIters(arrayIters.size());

        for (size_t i = 0; i < arrayIters.size(); i++)
        {
            arrayIters[i] = array.getIterator(i);
            chunkIters[i] =
                ((MemChunk&)arrayIters[i]->newChunk(coord)).getIterator(query,
                                                                        ChunkIterator::SEQUENTIAL_WRITE);
        }

        BOOST_FOREACH(CoordValueMapEntry const& p, m) {
            coord[0] = p.first;
            for (size_t i = 0; i < chunkIters.size(); i++)
            {
                if (!chunkIters[i]->setPosition(coord))
                {
                    chunkIters[i]->flush();
                    chunkIters[i].reset();
                    chunkIters[i] =
                        ((MemChunk&)arrayIters[i]->newChunk(coord)).getIterator(query,
                                                                                ChunkIterator::SEQUENTIAL_WRITE);
                    chunkIters[i]->setPosition(coord);
                }
                chunkIters[i]->writeItem(p.second);
            }
        }

        for (size_t i = 0; i < chunkIters.size(); i++)
        {
            chunkIters[i]->flush();
        }
    }

    /**
     * Test memarray once.
     * First this method sets the mem array threshold to 
     * something small.  Then it generates a large 1-d array of
     * random values.  Finally it scans the values once.
     * If the number of swapouts is not equal to the number
     * of reads, we assert.
     *
     * @param[in]   query
     * @param[in]   type     the value type
     * @param[in]   start    the start coordinate of the dim
     * @param[in]   end      the end coordinate of the dim
     * @param[in]   chunkInterval  the chunk interval
     * @param[in]   threshold the mem-array threshold in mb
     *
     * @throw SCIDB_SE_INTERNAL::SCIDB_LE_UNITTEST_FAILED
     */
    void testOnce_MemArray(boost::shared_ptr<Query>& query,
                           TypeId const& type,
                           Coordinate start,
                           Coordinate end,
                           uint32_t chunkInterval,
                           uint64_t threshold)
    {
        const int percentNullValue = 0;
        const int missingReason = 0;
        size_t currentSwaps = 0;
        size_t currentLoads = 0;
        size_t swaps = 0;
        size_t loads = 0;

        LOG4CXX_DEBUG(logger, "MemArray UnitTest Attempt [type=" << type << "][start=" << start << "][end=" << end <<
                      "][chunkInterval=" << chunkInterval << "][threshold=" << threshold << "]");

        // Set the memarray threshold
        uint64_t currentThreshold = SharedMemCache::getInstance().getMemThreshold();
        SharedMemCache::getInstance().setMemThreshold(threshold * MiB);

        try
        {

            // Array schema
            vector<AttributeDesc> attributes(1);
            attributes[0] = AttributeDesc((AttributeID)0, "X",  type, AttributeDesc::IS_NULLABLE, 0);

            vector<DimensionDesc> dimensions(1);
            dimensions[0] = DimensionDesc(string("dummy_dimension"), start, end, chunkInterval, 0);
            ArrayDesc schema("dummy_array", addEmptyTagAttribute(attributes), dimensions);

            // Define the array
            shared_ptr<MemArray> arrayInst(new MemArray(schema,query));
            shared_ptr<Array> baseArrayInst = static_pointer_cast<MemArray, Array>(arrayInst);

            // Generate source data
            CoordValueMap mapInst;
            Value value;
            for (Coordinate i=start; i<end+1; ++i)
            {
                mapInst[i] = genRandomValue(type, value, percentNullValue, missingReason);
            }

            // Record the current swaps and reads
            currentSwaps = SharedMemCache::getInstance().getSwapNum();
            currentLoads = SharedMemCache::getInstance().getLoadsNum();

            // Insert the map data into the array.
            insertMapDataIntoArray(query, *arrayInst, mapInst);

            // Scan the array
            // - Retrieve all data from the array.
            Value t;
            size_t itemCount = 0;
            shared_ptr<ConstArrayIterator> constArrayIter = arrayInst->getConstIterator(0);
            constArrayIter->reset();
            while (!constArrayIter->end())
            {
                shared_ptr<ConstChunkIterator> constChunkIter =
                    constArrayIter->getChunk().getConstIterator(ChunkIterator::IGNORE_EMPTY_CELLS);
                while (!constChunkIter->end())
                {
                    itemCount++;
                    Value& v = constChunkIter->getItem();
                    t = v;
                    ++(*constChunkIter);
                }
                ++(*constArrayIter);
            }
            if (itemCount != mapInst.size())
            {
                stringstream ss;
                
                ss << "wrong # of elements in array, expected: " << mapInst.size() <<
                    " got: " << itemCount;
                throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_UNITTEST_FAILED) <<
                    "UnitTestMemArray" << ss.str();
            }

            // Check the swap and read numbers
            swaps = SharedMemCache::getInstance().getSwapNum() - currentSwaps;
            loads = SharedMemCache::getInstance().getLoadsNum() - currentLoads;

            if (swaps != loads)
            {
                throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_UNITTEST_FAILED) <<
                    "UnitTestMemArray" << "mismatch between swaps and loads";
            }
        }
        catch (...)
        {
            SharedMemCache::getInstance().setMemThreshold(currentThreshold);
            throw;
        }

        SharedMemCache::getInstance().setMemThreshold(currentThreshold);

        LOG4CXX_DEBUG(logger, "MemArray UnitTest Success [type=" << type << "][start=" <<
                      start << "][end=" << end << "][chunkInterval=" << chunkInterval <<
                      "][threshold=" << threshold << "]" << "][loads=" << loads <<
                      "][swaps=" << swaps << "]");
    }

    boost::shared_ptr<Array> execute(vector< boost::shared_ptr<Array> >& inputArrays, boost::shared_ptr<Query> query)
    {
        srand(time(NULL));

        testOnce_MemArray(query, TID_INT64, 0, 500000, 10000, 2);

        return shared_ptr<Array> (new MemArray(_schema,query));
    }

};

REGISTER_PHYSICAL_OPERATOR_FACTORY(UnitTestMemArrayPhysical, "test_memarray", "UnitTestMemArrayPhysical");
}
