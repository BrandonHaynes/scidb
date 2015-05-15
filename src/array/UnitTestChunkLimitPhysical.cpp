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
#include <array/SortArray.h>

using namespace boost;
using namespace std;

namespace scidb
{
static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.unittest"));

class UnitTestChunkLimitPhysical: public PhysicalOperator
{
    typedef map<Coordinate, Value> CoordValueMap;
    typedef std::pair<Coordinate, Value> CoordValueMapEntry;
public:

    UnitTestChunkLimitPhysical(const string& logicalName, const string& physicalName,
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
                << "UnitTestChunkLimitPhysical" << "genRandomValue";
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
                << "UnitTestChunkLimitPhysical" << "value2string";
        }
        return ss.str();
    }

    /**
     * Build array chunk with indicated number of random values of the specified type, using indicated
     * iteration mode.
     * @param[in]    query
     * @param[inout] array  the array to receive data
     * @param[in]    type   the type of values to put into the chunk
     * @param[in]    count  the number of values to put into the chunk
     * @param[in]    mode   the iteration mode for the chunk iterator
     */
    void buildRandomArrayChunk(shared_ptr<Query>& query,
                               MemArray& array,
                               TypeId type,
                               int count,
                               int mode)
    {
        Coordinates coord(1);
        coord[0] = 0;
        shared_ptr<ArrayIterator> arrayIter = array.getIterator(0);
        shared_ptr<ChunkIterator> chunkIter = 
            ((MemChunk&)arrayIter->newChunk(coord)).getIterator(query, mode);

        if (!chunkIter->setPosition(coord))
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_UNITTEST_FAILED) <<
                "UnitTestChunkLimit" << "Failed to set position in chunk";
        }
        
        for (int j = 0; j < count; j++)
        {
            Value v;

            genRandomValue(type, v, 0, 0);
            chunkIter->writeItem(v);
            ++(*chunkIter);
        }
        chunkIter->flush();
    }


    /**
     * Test sort array once.
     * The method sets the chunk limit to the indicated number,
     * then tries to create a chunk of the inidicated size and
     * type, using the indicated mode.  If "expectFail" is true
     * then the method looks for the "CHUNK_TOO_LARGE" exception,
     * and fails if it does not see it.  If "expectFail" is false,
     * the method does the opposite.  Before exiting, the method
     * always resets the chunk limit to the original value.
     *
     * @param[in]   query
     * @param[in]   limit      the desired chunk limit (as a string)
     * @param[in]   type       the value type
     * @param[in]   count      how many values
     * @param[in]   mode       iteration mode
     * @param[in]   expectFail is an error expected?
     *
     * @throw SCIDB_SE_INTERNAL::SCIDB_LE_UNITTEST_FAILED
     */
    void testOnce_ChunkLimit(boost::shared_ptr<Query>& query,
                             string const& limit,
                             TypeId const& type,
                             int count,
                             int mode,
                             bool expectFail)
    {
        bool failed = false;

        LOG4CXX_DEBUG(logger, "ChunkLimit UnitTest Attempt [type=" << type << "][count=" << count 
                      << "][mode=" << mode << "][expectFail=" << expectFail << "]");

        // Array schema
        vector<AttributeDesc> attributes(1);
        attributes[0] = AttributeDesc((AttributeID)0, "X",  type, AttributeDesc::IS_NULLABLE, 0);

        vector<DimensionDesc> dimensions(1);
        dimensions[0] = DimensionDesc(string("dummy_dimension"), 0, count, count, 0);
        ArrayDesc schema("dummy_array", addEmptyTagAttribute(attributes), dimensions);

        // Test array
        shared_ptr<MemArray> array(new MemArray(schema, query));

        // set the chunk size limit
        std::string oldLimit;
        try 
        {
            oldLimit = Config::getInstance()->setOptionValue("chunk-size-limit-mb",
                                                             limit);
        }
        catch (Exception const& e)
        {
            LOG4CXX_DEBUG(logger, "ChunkLimit UnitTest unexpected exception: "
                          << e.getStringifiedLongErrorCode());
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_UNITTEST_FAILED)
                << "UnitTestChunkLimitPhysical" << "setOptionValue";
        }

        // try to create the chunk
        try 
        {
            buildRandomArrayChunk(query, *array, type, count, mode);
        }
        catch (Exception const& x)
        {
            if (!expectFail)
            {
                LOG4CXX_DEBUG(logger, "ChunkLimit UnitTest unexpected exception: "
                              << x.getStringifiedLongErrorCode());
                failed = true;
            }
            else if (x.getLongErrorCode() != SCIDB_LE_CHUNK_TOO_LARGE)
            {
                LOG4CXX_DEBUG(logger, "ChunkLimit UnitTest incorrect exception: "
                              << x.getStringifiedLongErrorCode());
                failed = true;
            }
        }
        
        // set the chunk size limit back
        try 
        {
            Config::getInstance()->setOptionValue("chunk-size-limit-mb",
                                                  oldLimit);
        }
        catch (Exception const& e)
        {
            LOG4CXX_DEBUG(logger, "ChunkLimit UnitTest unexpected exception: "
                          << e.getStringifiedLongErrorCode());
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_UNITTEST_FAILED)
                << "UnitTestChunkLimitPhysical" << "setOptionValue2";
        }

        if (failed)
        {
            LOG4CXX_DEBUG(logger, "ChunkLimit UnitTest Failed [type=" << type << "][count=" << count 
                          << "][mode=" << mode << "][expectFail=" << expectFail << "]");
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_UNITTEST_FAILED)
                << "UnitTestChunkLimitPhysical" << "unexpected status";
        }
        else
        {
            LOG4CXX_DEBUG(logger, "ChunkLimit UnitTest Success [type=" << type << "][count=" << count 
                          << "][mode=" << mode << "][expectFail=" << expectFail << "]");
        }
    }

    boost::shared_ptr<Array> execute(vector< boost::shared_ptr<Array> >& inputArrays,
                                     boost::shared_ptr<Query> query)
    {
        srand(time(NULL));

        testOnce_ChunkLimit(query, "2", TID_INT64, 100000, 0, false);
        testOnce_ChunkLimit(query, "2", TID_INT64, 100000, ChunkIterator::SEQUENTIAL_WRITE, false);
        testOnce_ChunkLimit(query, "2", TID_INT64, 500000, 0, true);
        testOnce_ChunkLimit(query, "2", TID_INT64, 500000, ChunkIterator::SEQUENTIAL_WRITE, true);
        testOnce_ChunkLimit(query, "2", TID_STRING, 10000, 0, false);
        testOnce_ChunkLimit(query, "2", TID_STRING, 10000, ChunkIterator::SEQUENTIAL_WRITE, false);
        testOnce_ChunkLimit(query, "2", TID_STRING, 500000, 0, true);
        testOnce_ChunkLimit(query, "2", TID_STRING, 500000, ChunkIterator::SEQUENTIAL_WRITE, true);

        return shared_ptr<Array> (new MemArray(_schema,query));
    }

};

REGISTER_PHYSICAL_OPERATOR_FACTORY(UnitTestChunkLimitPhysical, "test_chunk_limit", "UnitTestChunkLimitPhysical");
}
