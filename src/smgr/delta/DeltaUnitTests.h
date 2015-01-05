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
 * @file DeltaUnitTests.h
 *
 * @author Adam Seering <adam@seering.org>
 */

#ifndef DELTA_UNIT_TESTS_H_
#define DELTA_UNIT_TESTS_H_

#include <string.h>

#include <cppunit/TestFixture.h>
#include <cppunit/extensions/HelperMacros.h>

#include <boost/shared_ptr.hpp>
using boost::shared_ptr;

#include <boost/assign/list_of.hpp>
using boost::assign::list_of;

#include <exception>
//#include <iostream>  // Used for debugging
//using std::cout;

#include "smgr/delta/ChunkDelta.h"
#include "smgr/delta/Delta.h"
#include "array/MemArray.h"

using namespace scidb;


class DeltaTests: public CppUnit::TestFixture
{

CPPUNIT_TEST_SUITE(DeltaTests);
CPPUNIT_TEST(testBasicSanity);
CPPUNIT_TEST(testIntegerTypeLibrary);
CPPUNIT_TEST(testDenseChunkDeltaCreation);
CPPUNIT_TEST(testLargeDenseChunkDeltaCreation);
CPPUNIT_TEST(testSparseChunkDeltaCreation);
CPPUNIT_TEST(testStringDeltaCreation);
CPPUNIT_TEST(testDeltaChainCreation);
CPPUNIT_TEST(testDeltaVersionControlAPI);
CPPUNIT_TEST_SUITE_END();

private:

        ArrayDesc defaultArrayDesc;

        const static size_t xDim = 10;
        const static size_t yDim = 10;

        void basicInit(MemChunk& chunk, bool sparse, bool preallocate = true)
        {
                // Lots of boilerplate to cook up a fake array with a fake chunk in it
                // Only run this once; make the variables static so they persist out-of-scope
                static AttributeDesc attrDesc(0, "test attribute", "uint64", 0, 0,
                        std::set<std::string>(), 0, NULL);
                attrDesc.addAlias("testattr");
                static Attributes attr = list_of<AttributeDesc>(attrDesc);
                static DimensionDesc X_d("X axis", 0, 0, xDim, xDim, xDim, 0);
                static DimensionDesc Y_d("Y axis", 0, 0, yDim, yDim, yDim, 0);
                static Dimensions dims = list_of<DimensionDesc>(X_d)(Y_d);
                static Address addr(0, 0, list_of<Coordinate>(0)(0));
                static ArrayDesc arrayDesc(10001, "test array", attr, dims, 0);

                // Ok, now we can finally instantiate a new version
                chunk.initialize(&arrayDesc, addr, 0);
                chunk.setSparse(sparse);
                if (preallocate) {
                        chunk.allocate(xDim*yDim*sizeof(uint64_t));
                }
        }

        void stringInit(MemChunk& chunk, bool sparse, bool preallocate = true)
        {
                // Lots of boilerplate to cook up a fake array with a fake chunk in it
                // Only run this once; make the variables static so they persist out-of-scope
                static AttributeDesc attrDesc(0, "test attribute", "string", 0, 0,
                        std::set<std::string>(), 0, NULL);
                attrDesc.addAlias("testattr");
                static Attributes attr = list_of<AttributeDesc>(attrDesc);
                static DimensionDesc X_d("X axis", 0, 0, xDim/10, xDim/10, xDim/10, 0);
                static DimensionDesc Y_d("Y axis", 0, 0, yDim/10, yDim/10, yDim/10, 0);
                static Dimensions dims = list_of<DimensionDesc>(X_d)(Y_d);
                static Address addr(0, 0, list_of<Coordinate>(0)(0));
                static ArrayDesc arrayDesc(0, "test array", attr, dims, 0);

                // Ok, now we can finally instantiate a new version
                chunk.initialize(&arrayDesc, addr, 0);
                chunk.setSparse(sparse);
                if (preallocate) {
                        chunk.allocate(xDim*yDim*sizeof(uint64_t)/100);
                }
        }

        // Per-test statistics
        StatisticsScope statScope;
        Statistics s;

public:
        DeltaTests() : statScope(&s) {}

        void testModifyMemChunk()
        {
                MemChunk cDense, cSparse;
                basicInit(cDense, false);
                basicInit(cSparse, true);
                boost::shared_ptr<Query> emptyQuery;
                shared_ptr<ChunkIterator> ciDense = cDense.getIterator(emptyQuery, ChunkIterator::IGNORE_DEFAULT_VALUES);
                shared_ptr<ChunkIterator> ciSparse = cSparse.getIterator(emptyQuery, ChunkIterator::IGNORE_DEFAULT_VALUES);

        }

        void testBasicSanity()
        {
                //cout<<"testBasicSanity()" <<endl;
                // This is really overkill to test, but, gotta start somewhere...
                // Consider it a useful CPPUnit refresher.

                // Make sure we can construct/throw/catch the new exception types that we introduce.
                CPPUNIT_ASSERT_THROW(throw InvalidVersionException();, InvalidVersionException);
                CPPUNIT_ASSERT_THROW(throw InvalidDeltaException(1);, InvalidDeltaException);

                // Make sure we aren't mangling DeltaType's
                CPPUNIT_ASSERT(SUBTRACTIVE != BSDIFF);

                // Can we construct struct's/enum's?
                DeltaChunkHeader dch;
                dch.chunkEnd = 0;
                DeltaBlockHeader dbh;
                dbh.numChunks = 0;
                SubtractionDeltaHeader sdh;
                sdh.denseBitDepth = 0;
                DeltaType d;
                d = SUBTRACTIVE;
        }

        void testIntegerTypeLibrary()
        {
                //cout<<"testIntegerTypeLibrary()" <<endl;
                // Make sure we're actually adding types as we go
                size_t initialTypesCount = TypeLibrary::typesCount();
                size_t createdTypesCount = 0;

                TypeId tid;

        tid = getTypeIdForIntSize(4);   // int32_t should be a builtin
                CPPUNIT_ASSERT_EQUAL(tid, TypeId("$int32"));
                CPPUNIT_ASSERT_EQUAL(initialTypesCount+createdTypesCount, TypeLibrary::typesCount());

        tid = getTypeIdForIntSize(7);   // int56_t really shouldn't be a builtin...
                CPPUNIT_ASSERT_EQUAL(tid, TypeId("$int56"));
                CPPUNIT_ASSERT_EQUAL(initialTypesCount+createdTypesCount, TypeLibrary::typesCount());
        }

        void testDenseChunkDeltaCreation()
        {
                //cout<<"testDenseChunkDeltaCreation()" <<endl;

                MemChunk firstVersion;
                basicInit(firstVersion, false);

                Value val(TypeLibrary::getType("uint64"));
                val.setUint64(200);

                boost::shared_ptr<Query> emptyQuery;

                // Go through and zero the array
                shared_ptr<ChunkIterator> firstIter = firstVersion.getIterator(emptyQuery, ChunkIterator::NO_EMPTY_CHECK);
                do {
                        firstIter->writeItem(val);
                        ++(*firstIter);
                } while (!firstIter->end());

                // The differencing code makes this assumption about Chunk::size(),
                // that for a dense array the raw data size will be exactly the
                // product of the total number of elements and the size of each element
                CPPUNIT_ASSERT_EQUAL((size_t)(sizeof(uint64_t)*xDim*yDim), firstVersion.getSize());


                // Now, create and zero a second array
                MemChunk secondVersion;
                basicInit(secondVersion, false);

                val.setUint64(283);

                shared_ptr<ChunkIterator> secondIter = secondVersion.getIterator(emptyQuery, ChunkIterator::NO_EMPTY_CHECK);
                do {
                        secondIter->writeItem(val);
                        ++(*secondIter);
                } while (!secondIter->end());
                // And set one value, so that there is some difference:

                val.setUint64((uint64_t)314159265358979);  // Arbitrary number that fits into a uint64 but not any smaller type
                secondIter->setPosition(secondIter->getFirstPosition());
                secondIter->writeItem(val);

                // Make sure we didn't mess up setting Value sizes somewhere
                CPPUNIT_ASSERT_EQUAL(val.getUint64(), secondIter->getItem().getUint64());

                // Double-check, just for good measure
                CPPUNIT_ASSERT_EQUAL((size_t)(sizeof(uint64_t)*xDim*yDim), secondVersion.getSize());


                // Now, create a delta; see what happens
                ChunkDelta chunkDelta(firstVersion, secondVersion);
                CPPUNIT_ASSERT(chunkDelta.isValidDelta());

                // Make sure we actually got data that's different
                CPPUNIT_ASSERT((chunkDelta.getSize() != secondVersion.getSize())
                                        || (!memcmp(chunkDelta.getData(), secondVersion.getData(), chunkDelta.getSize())));

                // And try applying it; see if we get our raw data back
                MemChunk buf;
                buf.allocate(firstVersion.getSize());
                chunkDelta.applyDelta(firstVersion, buf);
                basicInit(buf, false, false);

                shared_ptr<ChunkIterator> bufIter = buf.getIterator(emptyQuery, ChunkIterator::NO_EMPTY_CHECK | ChunkIterator::APPEND_CHUNK);
                secondIter->reset();
                do {
                        CPPUNIT_ASSERT_EQUAL(bufIter->getItem().getUint64(), secondIter->getItem().getUint64());
                        ++(*secondIter);
                        ++(*bufIter);
                } while (!secondIter->end());

                CPPUNIT_ASSERT_EQUAL(secondVersion.getSize(), buf.getSize());
                CPPUNIT_ASSERT_EQUAL(0, memcmp(secondVersion.getData(), buf.getData(), buf.getSize()));

                // Allow a dense array, but should only use one byte per value,
                // and so be relatively small.
                // The current format should use exactly 161 bytes of data,
                // mostly headers of various sorts, but this may change.
                CPPUNIT_ASSERT(chunkDelta.getSize() < secondVersion.getSize());
        }

        void testLargeDenseChunkDeltaCreation()
        {
                //cout<<"testLargeDenseChunkDeltaCreation()" <<endl;

                MemChunk firstVersion;
                basicInit(firstVersion, false);

                Value val(TypeLibrary::getType("uint64"));
                val.setUint64(0xefffffffffff);

                boost::shared_ptr<Query> emptyQuery;
                // Go through and zero the array
                shared_ptr<ChunkIterator> firstIter = firstVersion.getIterator(emptyQuery, ChunkIterator::NO_EMPTY_CHECK);
                do {
                        firstIter->writeItem(val);
                        ++(*firstIter);
                } while (!firstIter->end());

                // The differencing code makes this assumption about Chunk::size(),
                // that for a dense array the raw data size will be exactly the
                // product of the total number of elements and the size of each element
                CPPUNIT_ASSERT_EQUAL((size_t)(sizeof(uint64_t)*xDim*yDim), firstVersion.getSize());


                // Now, create and zero a second array
                MemChunk secondVersion;
                basicInit(secondVersion, false);

                val.setUint64(0xffffffffffff);

                shared_ptr<ChunkIterator> secondIter = secondVersion.getIterator(emptyQuery, ChunkIterator::NO_EMPTY_CHECK);
                do {
                        secondIter->writeItem(val);
                        ++(*secondIter);
                } while (!secondIter->end());
                // And set one value, so that there is some difference:

                val.setUint64((uint64_t)314159265358979);  // Arbitrary number that fits into a uint64 but not any smaller type
                secondIter->setPosition(secondIter->getFirstPosition());
                secondIter->writeItem(val);

                // Make sure we didn't mess up setting Value sizes somewhere
                CPPUNIT_ASSERT_EQUAL(val.getUint64(), secondIter->getItem().getUint64());

                // Double-check, just for good measure
                CPPUNIT_ASSERT_EQUAL((size_t)(sizeof(uint64_t)*xDim*yDim), secondVersion.getSize());


                // Now, create a delta; see what happens
                ChunkDelta chunkDelta(firstVersion, secondVersion);
                CPPUNIT_ASSERT(chunkDelta.isValidDelta());

                // Make sure we actually got data that's different
                CPPUNIT_ASSERT((chunkDelta.getSize() != secondVersion.getSize())
                                        || (!memcmp(chunkDelta.getData(), secondVersion.getData(), chunkDelta.getSize())));

                // And try applying it; see if we get our raw data back
                MemChunk buf;
                buf.allocate(firstVersion.getSize());
                chunkDelta.applyDelta(firstVersion, buf);
                basicInit(buf, false, false);

                shared_ptr<ChunkIterator> bufIter = buf.getIterator(emptyQuery, ChunkIterator::NO_EMPTY_CHECK | ChunkIterator::APPEND_CHUNK);
                secondIter->reset();
                do {
                        CPPUNIT_ASSERT_EQUAL(bufIter->getItem().getUint64(), secondIter->getItem().getUint64());
                        ++(*secondIter);
                        ++(*bufIter);
                } while (!secondIter->end());

                CPPUNIT_ASSERT_EQUAL(secondVersion.getSize(), buf.getSize());
                CPPUNIT_ASSERT_EQUAL(0, memcmp(secondVersion.getData(), buf.getData(), buf.getSize()));

                // Allow a dense array, but should only use one byte per value,
                // and so be relatively small.
                // The current format should use exactly 161 bytes of data,
                // mostly headers of various sorts, but this may change.
                CPPUNIT_ASSERT(chunkDelta.getSize() < secondVersion.getSize());
        }


        void testSparseChunkDeltaCreation()
        {
                //cout<<"testSparseChunkDeltaCreation()" <<endl;

                MemChunk firstVersion;
                basicInit(firstVersion, false);

                Value val(TypeLibrary::getType("uint64"));
                val.setUint64(1000);

                boost::shared_ptr<Query> emptyQuery;

                // Set a value
                shared_ptr<ChunkIterator> firstIter = firstVersion.getIterator(emptyQuery, ChunkIterator::NO_EMPTY_CHECK);
                firstIter->writeItem(val);
                ++(*firstIter);
                ++(*firstIter);

                val.setUint64(27);
                firstIter->writeItem(val);
                ++(*firstIter);

                // The differencing code makes this assumption about Chunk::size(),
                // that for a dense array the raw data size will be exactly the
                // product of the total number of elements and the size of each element
                CPPUNIT_ASSERT_EQUAL((size_t)(sizeof(uint64_t)*xDim*yDim), firstVersion.getSize());


                // Now, create and zero a second array
                MemChunk secondVersion;
                basicInit(secondVersion, false);

                shared_ptr<ChunkIterator> secondIter = secondVersion.getIterator(emptyQuery, ChunkIterator::NO_EMPTY_CHECK);

                // And set some more values
                val.setUint64((uint64_t)314159265358979);  // Arbitrary number that fits into a uint64 but not any smaller type
                secondIter->writeItem(val);
                ++(*secondIter);

                val.setUint64(2);
                secondIter->writeItem(val);
                ++(*secondIter);

                ++(*secondIter);

                val.setUint64(0);
                secondIter->writeItem(val);
                ++(*secondIter);

                // Make sure we didn't mess up setting Value sizes somewhere
                CPPUNIT_ASSERT_EQUAL(val.getUint64(), secondIter->getItem().getUint64());

                // Double-check, just for good measure
                CPPUNIT_ASSERT_EQUAL((size_t)(sizeof(uint64_t)*xDim*yDim), secondVersion.getSize());


                // Now, create a delta; see what happens
                ChunkDelta chunkDelta(firstVersion, secondVersion);
                CPPUNIT_ASSERT(chunkDelta.isValidDelta());

                // Make sure we actually got data that's different
                CPPUNIT_ASSERT((chunkDelta.getSize() != secondVersion.getSize())
                                        || (!memcmp(chunkDelta.getData(), secondVersion.getData(), chunkDelta.getSize())));

                // And try applying it; see if we get our raw data back
                MemChunk buf;
                buf.allocate(secondVersion.getSize());
                chunkDelta.applyDelta(firstVersion, buf);
                basicInit(buf, false, false);

                CPPUNIT_ASSERT_EQUAL(secondVersion.getSize(), buf.getSize());

                shared_ptr<ChunkIterator> bufIter = buf.getIterator(emptyQuery, ChunkIterator::NO_EMPTY_CHECK | ChunkIterator::APPEND_CHUNK);
                secondIter->reset();
                do {
                        CPPUNIT_ASSERT_EQUAL(bufIter->getPosition(), secondIter->getPosition());
                        CPPUNIT_ASSERT_EQUAL(bufIter->getItem().getUint64(), secondIter->getItem().getUint64());
                        ++(*secondIter);
                        ++(*bufIter);
                } while (!secondIter->end());

                CPPUNIT_ASSERT_EQUAL(0, memcmp(secondVersion.getData(), buf.getData(), buf.getSize()));

                // No dense data allowed here; not even 1 byte/cell.
                // The current format should use exactly 117 bytes of data,
                // mostly headers of various sorts, but this may change.
                CPPUNIT_ASSERT(chunkDelta.getSize() < secondVersion.getSize());
        }

        void testStringDeltaCreation()
        {
                //cout<<"testStringDeltaCreation()" <<endl;

                // Create a big string to work with
                const size_t strBufSize = 300;
                char strBuf[strBufSize];
                memset(&strBuf[0], 'a', strBufSize-1);
                strBuf[strBufSize-1] = '\0';

                MemChunk firstVersion;
                stringInit(firstVersion, false);

                Value val(TypeLibrary::getType("string"));
                val.setString(&strBuf[0]);

                boost::shared_ptr<Query> emptyQuery;

                // Go through and zero the array
                shared_ptr<ChunkIterator> firstIter = firstVersion.getIterator(emptyQuery, ChunkIterator::NO_EMPTY_CHECK);
                do {
                        firstIter->writeItem(val);
                        ++(*firstIter);
                } while (!firstIter->end());

                // Now, create and zero a second array
                MemChunk secondVersion;
                stringInit(secondVersion, false);

                Value val2(TypeLibrary::getType("string"));
                memset(&strBuf[0], 'b', strBufSize-1);
                strBuf[strBufSize-1] = '\0';  // Should be redundant; oh well
                val2.setString(&strBuf[0]);

                shared_ptr<ChunkIterator> secondIter = secondVersion.getIterator(emptyQuery, ChunkIterator::NO_EMPTY_CHECK);
                do {
                        secondIter->writeItem(val);
                        ++(*secondIter);
                        if (!secondIter->end()) {
                                secondIter->writeItem(val2);
                                ++(*secondIter);
                        }
                } while (!secondIter->end());
                // And set one value, so that there is some difference:

                memset(&strBuf[0], 'c', strBufSize-1);
                strBuf[strBufSize-1] = '\0';  // Should be redundant; oh well
                val.setString(&strBuf[0]);
                secondIter->setPosition(secondIter->getFirstPosition());
                secondIter->writeItem(val);

                // Now, create a delta; see what happens
                ChunkDelta chunkDelta(firstVersion, secondVersion);
                CPPUNIT_ASSERT(chunkDelta.isValidDelta());

                // Make sure we actually got data that's different
                CPPUNIT_ASSERT((chunkDelta.getSize() != secondVersion.getSize())
                                        || (!memcmp(chunkDelta.getData(), secondVersion.getData(), chunkDelta.getSize())));

                // And try applying it; see if we get our raw data back
                MemChunk buf;
                buf.allocate(secondVersion.getSize());
                chunkDelta.applyDelta(firstVersion, buf);
                stringInit(buf, false, false);

                CPPUNIT_ASSERT_EQUAL(secondVersion.getSize(), buf.getSize());
                CPPUNIT_ASSERT_EQUAL(0, memcmp(secondVersion.getData(), buf.getData(), buf.getSize()));
        }

        void testDeltaChainCreation()
        {
                //cout<<"testDeltaChainCreation()" <<endl;

                MemChunk firstVersion;
                basicInit(firstVersion, false);

                Value val(TypeLibrary::getType("uint64"));
                val.setUint64(200);

                boost::shared_ptr<Query> emptyQuery;

                // Go through and zero the array
                shared_ptr<ChunkIterator> firstIter = firstVersion.getIterator(emptyQuery, ChunkIterator::NO_EMPTY_CHECK);
                do {
                        firstIter->writeItem(val);
                        ++(*firstIter);
                } while (!firstIter->end());

                // The differencing code makes this assumption about Chunk::size(),
                // that for a dense array the raw data size will be exactly the
                // product of the total number of elements and the size of each element
                CPPUNIT_ASSERT_EQUAL((size_t)(sizeof(uint64_t)*xDim*yDim), firstVersion.getSize());


                // Now, create and zero a second array
                MemChunk secondVersion;
                basicInit(secondVersion, false);

                val.setUint64(283);

                shared_ptr<ChunkIterator> secondIter = secondVersion.getIterator(emptyQuery, ChunkIterator::NO_EMPTY_CHECK);
                do {
                        secondIter->writeItem(val);
                        ++(*secondIter);
                } while (!secondIter->end());
                // And set one value, so that there is some difference:

                val.setUint64((uint64_t)314159265358979);  // Arbitrary number that fits into a uint64 but not any smaller type
                secondIter->setPosition(secondIter->getFirstPosition());
                secondIter->writeItem(val);

                // Make sure we didn't mess up setting Value sizes somewhere
                CPPUNIT_ASSERT_EQUAL(val.getUint64(), secondIter->getItem().getUint64());

                // Double-check, just for good measure
                CPPUNIT_ASSERT_EQUAL((size_t)(sizeof(uint64_t)*xDim*yDim), secondVersion.getSize());


                // Now, create a delta; see what happens
                ChunkDelta chunkDelta(firstVersion, secondVersion);
                CPPUNIT_ASSERT(chunkDelta.isValidDelta());

                // Make sure we actually got data that's different
                CPPUNIT_ASSERT((chunkDelta.getSize() != secondVersion.getSize())
                                        || (!memcmp(chunkDelta.getData(), secondVersion.getData(), chunkDelta.getSize())));

                // Now, create a second delta; see what happens
                ChunkDelta chunkDelta2(secondVersion, firstVersion);
                CPPUNIT_ASSERT(chunkDelta.isValidDelta());

                // Make sure we actually got data that's different
                CPPUNIT_ASSERT((chunkDelta.getSize() != firstVersion.getSize())
                                        || (!memcmp(chunkDelta.getData(), firstVersion.getData(), chunkDelta.getSize())));

                // Clone the second delta, to test out the alternate ChunkDelta constructor
                ChunkDelta chunkDelta_fromData(chunkDelta2.getData(), chunkDelta2.getSize());
                CPPUNIT_ASSERT(chunkDelta.isValidDelta());

                // Make sure we actually got data that's different
                CPPUNIT_ASSERT((chunkDelta.getSize() != firstVersion.getSize())
                                        || (!memcmp(chunkDelta.getData(), firstVersion.getData(), chunkDelta.getSize())));

                //

                chunkDelta.pushDelta(chunkDelta_fromData);

                // And try applying it; see if we get our raw data back
                MemChunk buf;
                buf.allocate(firstVersion.getSize());
                chunkDelta.applyDelta(firstVersion, buf);
                basicInit(buf, false, false);

                shared_ptr<ChunkIterator> bufIter = buf.getIterator(emptyQuery, ChunkIterator::NO_EMPTY_CHECK | ChunkIterator::APPEND_CHUNK);
                firstIter->reset();
                do {
                        CPPUNIT_ASSERT_EQUAL(bufIter->getItem().getInt64(), firstIter->getItem().getInt64());
                        ++(*firstIter);
                        ++(*bufIter);
                } while (!firstIter->end());

                CPPUNIT_ASSERT_EQUAL(firstVersion.getSize(), buf.getSize());
                CPPUNIT_ASSERT_EQUAL(0, memcmp(firstVersion.getData(), buf.getData(), buf.getSize()));

                // Allow a dense array, but should only use one byte per value,
                // and so be relatively small.
                // The current format should use exactly 161 bytes of data,
                // mostly headers of various sorts, but this may change.
                CPPUNIT_ASSERT(chunkDelta.getSize() < firstVersion.getSize());
        }

        void testDeltaVersionControlAPI()
        {
                //cout<<"testDeltaVersionControlAPI()" <<endl;

                MemChunk firstVersion;
                basicInit(firstVersion, false);

                Value val(TypeLibrary::getType("uint64"));
                val.setUint64(200);

                boost::shared_ptr<Query> emptyQuery;

                // Go through and zero the array
                shared_ptr<ChunkIterator> firstIter = firstVersion.getIterator(emptyQuery, ChunkIterator::NO_EMPTY_CHECK);
                do {
                        firstIter->writeItem(val);
                        ++(*firstIter);
                } while (!firstIter->end());
                firstIter->flush();

                // The differencing code makes this assumption about Chunk::size(),
                // that for a dense array the raw data size will be exactly the
                // product of the total number of elements and the size of each element
                CPPUNIT_ASSERT_EQUAL((size_t)(sizeof(uint64_t)*xDim*yDim), firstVersion.getSize());


                // Now, create and zero a second array
                MemChunk secondVersion;
                basicInit(secondVersion, false);

                val.setUint64(283);

                shared_ptr<ChunkIterator> secondIter = secondVersion.getIterator(emptyQuery, ChunkIterator::NO_EMPTY_CHECK);
                do {
                        secondIter->writeItem(val);
                        ++(*secondIter);
                } while (!secondIter->end());
                secondIter->flush();
                // And set one value, so that there is some difference:

                val.setUint64((uint64_t)314159265358979);  // Arbitrary number that fits into a uint64 but not any smaller type
                secondIter->setPosition(secondIter->getFirstPosition());
                secondIter->writeItem(val);

                // Make sure we didn't mess up setting Value sizes somewhere
                CPPUNIT_ASSERT_EQUAL(val.getUint64(), secondIter->getItem().getUint64());

                // Double-check, just for good measure
                CPPUNIT_ASSERT_EQUAL((size_t)(sizeof(uint64_t)*xDim*yDim), secondVersion.getSize());

                // Get a DeltaVersionControl instance to play with
                DeltaVersionControl dvc;
                MemChunk rawBuf;
        rawBuf.initialize(firstVersion);
                rawBuf.allocate(firstVersion.getSize());
                memcpy(rawBuf.getData(), firstVersion.getData(), rawBuf.getSize());

                // Stuff a few versions into our versioned chunk
                // Use monotonic but arbitrary version numbers; make sure
                // that things don't break if versions are nonconsecutive.
                // Also, assert that all new versions are successful.
                //CPPUNIT_ASSERT(dvc.newVersion(rawBuf, firstVersion, 0, false));

                // Clear out stale catalog data
                if (SystemCatalog::getInstance()->containsArray(firstVersion.getArrayDesc().getName())) {
                        SystemCatalog::getInstance()->deleteArray(firstVersion.getArrayDesc().getName());
                }

                ArrayID id = SystemCatalog::getInstance()->addArray(firstVersion.getArrayDesc(), psHashPartitioned);
                CPPUNIT_ASSERT(dvc.newVersion(rawBuf, secondVersion, 2, false));
                CPPUNIT_ASSERT(dvc.newVersion(rawBuf, secondVersion, 3, true));
                SystemCatalog::getInstance()->deleteArray(id);

                // getVersion() needs a proper Chunk
                basicInit(rawBuf, false, false);

                MemChunk tmpBuf;
        tmpBuf.initialize(rawBuf);
                dvc.getVersion(tmpBuf, rawBuf, 2);
                CPPUNIT_ASSERT_EQUAL(tmpBuf.getSize(), secondVersion.getSize());
                CPPUNIT_ASSERT_EQUAL(0, memcmp(tmpBuf.getData(), secondVersion.getData(), secondVersion.getSize()));

                dvc.getVersion(tmpBuf, rawBuf, 1);
                CPPUNIT_ASSERT_EQUAL(tmpBuf.getSize(), firstVersion.getSize());
                CPPUNIT_ASSERT_EQUAL(0, memcmp(tmpBuf.getData(), firstVersion.getData(), firstVersion.getSize()));

                dvc.getVersion(tmpBuf, rawBuf, 3);
                CPPUNIT_ASSERT_EQUAL(tmpBuf.getSize(), secondVersion.getSize());
                CPPUNIT_ASSERT_EQUAL(0, memcmp(tmpBuf.getData(), secondVersion.getData(), secondVersion.getSize()));
        }

    void setUp()
    {
                //cout << "Start Testing Delta-Compression library" <<endl;
    }

        void tearDown()
    {
                //cout << "End Testing Delta-Compression library" <<endl;
    }

};

CPPUNIT_TEST_SUITE_REGISTRATION(DeltaTests);

#endif /* DELTA_UNIT_TESTS_H_ */
