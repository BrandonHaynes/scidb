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

using namespace std;
using namespace boost;

namespace scidb
{

static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("test_cache"));

class PhysicalTestCache : public PhysicalOperator
{
private:
    size_t _defaultChunkSize;

public:
    /**
     * Standard issue operator constructor.
     */
    PhysicalTestCache(std::string const& logicalName,
                  std::string const& physicalName,
                  Parameters const& parameters,
                  ArrayDesc const& schema):
        PhysicalOperator(logicalName, physicalName, parameters, schema),
        _defaultChunkSize( 1000000 )
    {}

    virtual ~PhysicalTestCache()
    {}

    void testLRUSize(size_t expectedSize)
    {
        size_t usedSize = SharedMemCache::getInstance().getUsedMemSize();
        if (usedSize != expectedSize)
        {
            ostringstream out;
            out<<"LRU size of "<<usedSize<<" does not match expected size of "<<expectedSize;
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL,SCIDB_LE_ILLEGAL_OPERATION)<<out.str();
        }
    }

    void testLRUSizeLT(size_t expectedSize)
    {
        size_t usedSize = SharedMemCache::getInstance().getUsedMemSize();
        if (usedSize > expectedSize)
        {
            ostringstream out;
            out<<"LRU size of "<<usedSize<<" is above the expected upper bound of "<<expectedSize;
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL,SCIDB_LE_ILLEGAL_OPERATION)<<out.str();
        }
    }

    /**
     * Create a MemArray with a single attribute of numElements consecutive integers (to prevent RLE encoding).
     */
    shared_ptr<Array> makeInt64Array(int64_t numElements, shared_ptr<Query>& query)
    {
        Attributes attrs;
        attrs.push_back(AttributeDesc(0, "att", TID_INT64, AttributeDesc::IS_NULLABLE, 0));
        Dimensions dims;
        dims.push_back(DimensionDesc("i",0,MAX_COORDINATE,_defaultChunkSize,0));
        ArrayDesc schema("arr", attrs, dims);
        shared_ptr<Array> array(new MemArray(schema,query));
        Coordinates pos(1,0);
        shared_ptr<ArrayIterator> aiter = array->getIterator(0);
        shared_ptr<ChunkIterator> citer;
        Value v;
        while (pos[0] < numElements)
        {
            if(pos[0] % _defaultChunkSize == 0)
            {
                if(citer)
                {
                    citer->flush();
                }
                citer = aiter->newChunk(pos).getIterator(query, ChunkIterator::NO_EMPTY_CHECK | ChunkIterator::SEQUENTIAL_WRITE);
            }
            citer->setPosition(pos);
            v.setInt64(pos[0]);
            citer->writeItem(v);
            ++(pos[0]);
        }
        if(citer)
        {
            citer->flush();
        }
        return array;
    }

    /**
     * Add more consecutive integers to an existing arr. whereToStart must be a coordinate in the last filled chunk.
     */
    void addToInt64Array(shared_ptr<Array>& array, size_t whereToStart, int64_t numElements, shared_ptr<Query>& query)
    {
        Coordinates pos(1,whereToStart);
        shared_ptr<ArrayIterator> aiter = array->getIterator(0);
        aiter->setPosition(pos);
        shared_ptr<ChunkIterator> citer = aiter->updateChunk().getIterator(query, ChunkIterator::NO_EMPTY_CHECK |
                                                                           ChunkIterator::APPEND_EMPTY_BITMAP |
                                                                           ChunkIterator::APPEND_CHUNK);
        Value v;
        while (pos[0] < numElements)
        {
            if(pos[0] % _defaultChunkSize == 0)
            {
                citer->flush();
                citer = aiter->newChunk(pos).getIterator(query, ChunkIterator::NO_EMPTY_CHECK | ChunkIterator::SEQUENTIAL_WRITE);
            }
            citer->setPosition(pos);
            v.setInt64(pos[0]);
            citer->writeItem(v);
            ++(pos[0]);
        }
        citer->flush();
    }

    /**
     * Iterate over all chunks of an array and add what their getSize() says.
     */
    size_t computeArraySize(shared_ptr<Array> &inputArray)
    {
        size_t res =0;
        ArrayDesc const& schema = inputArray->getArrayDesc();
        size_t nAttrs = schema.getAttributes().size();
        vector<shared_ptr<ConstArrayIterator> > arrayIters(nAttrs);
        for (AttributeID i=0; i<nAttrs; ++i)
        {
            arrayIters[i] = inputArray->getConstIterator(i);
        }
        while (!arrayIters[0]->end())
        {
            for (AttributeID i=0; i<nAttrs; ++i)
            {
                res += arrayIters[i]->getChunk().getSize();
                ++(*arrayIters[i]);
            }
        }
        return res;
    }

    /**
     * Iterate over all chunks of an array and pin/unpin them.
     */
    void iterateOverArray(shared_ptr<Array> &inputArray)
    {
        ArrayDesc const& schema = inputArray->getArrayDesc();
        size_t nAttrs = schema.getAttributes().size();
        vector<shared_ptr<ConstArrayIterator> > arrayIters(nAttrs);
        vector<shared_ptr<ConstChunkIterator> > chunkIters(nAttrs);
        for (AttributeID i=0; i<nAttrs; ++i)
        {
            arrayIters[i] = inputArray->getConstIterator(i);
        }
        while (!arrayIters[0]->end())
        {
            for (AttributeID i=0; i<nAttrs; ++i)
            {
                chunkIters[i] = arrayIters[i]->getChunk().getConstIterator();
                while (!chunkIters[i]->end())
                {
                    ++(*(chunkIters[i]));
                }
                ++(*arrayIters[i]);
            }
        }
    }

    shared_ptr< Array> execute(vector< shared_ptr< Array> >& inputArrays, shared_ptr<Query> query)
    {
        size_t maxArraySize = SharedMemCache::getInstance().getMemThreshold();

        testLRUSize(0);

        shared_ptr<Array> arr = makeInt64Array(2123456, query);
        size_t arraySize = computeArraySize(arr);
        if (arraySize > maxArraySize)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "The test is invalidated by a low MEM_ARRAY_THRESHOLD";
        }
        testLRUSize(arraySize);

        shared_ptr<Array> arr2 = makeInt64Array(1123456, query);
        size_t arraySize2 = computeArraySize(arr2);
        if (arraySize2 > maxArraySize)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "The test is invalidated by a low MEM_ARRAY_THRESHOLD";
        }
        testLRUSize(arraySize + arraySize2);

        //iterating over the array should not change cache size. That's the problem we were hitting before!
        iterateOverArray(arr);
        iterateOverArray(arr2);
        testLRUSize(arraySize + arraySize2);

        //adding data to existing array (existing chunk only or existing chunk + new chunks) should keep sizes correct.
        addToInt64Array(arr, 2123456, 12345, query);
        addToInt64Array(arr2, 1123456, 1000000, query);
        arraySize = computeArraySize(arr);
        arraySize2 = computeArraySize(arr2);
        if (arraySize > maxArraySize || arraySize2 > maxArraySize)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "The test is invalidated by a low MEM_ARRAY_THRESHOLD";
        }
        testLRUSize(arraySize + arraySize2);

        arr.reset();
        arr2.reset();
        testLRUSize(0);

        try
        {
            size_t newMaxSize = 20 * MiB;
            SharedMemCache::getInstance().setMemThreshold(newMaxSize);
            //blow the cache by creating a larger array. ensure the cache size is still under limit
            arr = makeInt64Array((newMaxSize+ 10 * sizeof(int64_t))/ sizeof(int64_t), query);
            testLRUSizeLT(newMaxSize);
            SharedMemCache::getInstance().setMemThreshold(maxArraySize);
        }
        catch (...)
        {
            //try to set the threshold back to where it was
            SharedMemCache::getInstance().setMemThreshold(maxArraySize);
            throw;
        }
        return shared_ptr<Array> (new MemArray(_schema,query));
    }
};

REGISTER_PHYSICAL_OPERATOR_FACTORY(PhysicalTestCache, "test_cache", "PhysicalTestCache");


} //namespace scidb
