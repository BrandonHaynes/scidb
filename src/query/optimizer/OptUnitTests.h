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
 * @file OptUnitTests.h
 *
 * @author poliocough@gmail.com
 */

#ifndef OPT_UNIT_TESTS_H_
#define OPT_UNIT_TESTS_H_

#include <cppunit/TestFixture.h>
#include <cppunit/extensions/HelperMacros.h>
#include <algorithm>
#include <boost/archive/text_oarchive.hpp>
#include <boost/archive/text_iarchive.hpp>
#include <boost/make_shared.hpp>
#include <boost/filesystem.hpp>
#include <boost/shared_ptr.hpp>

#include <query/Operator.h>
#include <query/QueryPlan.h>
#include <query/QueryProcessor.h>
#include <system/SystemCatalog.h>
#include <array/Metadata.h>
#include <query/TypeSystem.h>
#include <system/Config.h>
#include <system/Cluster.h>

#include "smgr/compression/BuiltinCompressors.h"
#include "query/optimizer/HabilisOptimizer.h"

namespace scidb
{

class OptimizerTests: public CppUnit::TestFixture
{

    typedef boost::shared_ptr<PhysicalOperator> PhysOpPtr;
    typedef boost::shared_ptr<PhysicalQueryPlanNode> PhysNodePtr;

CPPUNIT_TEST_SUITE(OptimizerTests);
        CPPUNIT_TEST(testBoundaries);
        CPPUNIT_TEST(testBoundaries2);
        CPPUNIT_TEST(testBasic);
        CPPUNIT_TEST(testSubArrayReshapeSgInsertions);
        CPPUNIT_TEST(testInputSgInsert);
        CPPUNIT_TEST(testConcatSgInsert);
        CPPUNIT_TEST(testHabilisCollapse);
//        CPPUNIT_TEST(testTwoPhase);
//        CPPUNIT_TEST(testMultiply);
        CPPUNIT_TEST(testFlipStoreRewrite);
        CPPUNIT_TEST(testReplication);
    CPPUNIT_TEST_SUITE_END();

private:
    boost::shared_ptr<QueryProcessor> _queryProcessor;
    boost::shared_ptr<Optimizer> _habilisDisabled;
    boost::shared_ptr<Optimizer> _habilis;

    ArrayDesc _dummyArray;
    Coordinates _dummyArrayStart;
    Coordinates _dummyArrayEnd;
    ArrayID _dummyArrayId;

    ArrayDesc _dummyShiftedArray;
    Coordinates _dummyShiftedArrayStart;
    Coordinates _dummyShiftedArrayEnd;
    ArrayID _dummyShiftedArrayId;

    ArrayDesc _smallArray;
    Coordinates _smallArrayStart;
    Coordinates _smallArrayEnd;
    ArrayID _smallArrayId;

    ArrayDesc _singleDim;
    Coordinates _singleDimStart;
    Coordinates _singleDimEnd;
    ArrayID _singleDimId;

    ArrayDesc _partiallyFilledArray;
    Coordinates _partiallyFilledStart;
    Coordinates _partiallyFilledEnd;
    ArrayID _partiallyFilledId;

    ArrayDesc _dummyFlippedArray;
    Coordinates _dummyFlippedStart;
    Coordinates _dummyFlippedEnd;
    ArrayID _dummyFlippedId;

    ArrayDesc _dummyReplicatedArray;
    ArrayID _dummyReplicatedArrayId;

public:

#define ASSERT_OPERATOR(instance, opName) (CPPUNIT_ASSERT( instance ->getPhysicalOperator()->getPhysicalName() == opName ))

    inline static ArrayID s_addArray (const ArrayDesc & desc, PartitioningSchema ps = psHashPartitioned)
    {
        SystemCatalog* systemCat = SystemCatalog::getInstance();
        if (systemCat->containsArray(desc.getName()))
        {
            systemCat->deleteArray(desc.getName());
        }
        ArrayDesc d2 = desc;
        systemCat->addArray(d2, ps);
        return d2.getId();
    }

    inline static ArrayID s_addArray (const ArrayDesc & desc, const Coordinates & start, const Coordinates & end, PartitioningSchema ps = psHashPartitioned)
    {
        ArrayID id = s_addArray(desc, ps);
        SystemCatalog::getInstance()->updateArrayBoundaries(desc, PhysicalBoundaries(start, end));
        return id;
    }

    void setUp()
    {
        _queryProcessor = QueryProcessor::create();

        //DUMMY
        Attributes dummyArrayAttributes;
        dummyArrayAttributes.push_back(AttributeDesc(0, "att0", TID_INT64, 0, (uint16_t) CompressorFactory::NO_COMPRESSION));
        dummyArrayAttributes.push_back(AttributeDesc(1, "att1", TID_INT64, 0, (uint16_t) CompressorFactory::NO_COMPRESSION));

        Dimensions dummyArrayDimensions;
        dummyArrayDimensions.push_back(DimensionDesc("x", 0, 0, 8, 9, 1, 0));
        dummyArrayDimensions.push_back(DimensionDesc("y", 0, 1, 9, 9, 1, 0));

        _dummyArray = ArrayDesc("opttest_dummy_array", dummyArrayAttributes, dummyArrayDimensions);
        _dummyArrayStart.push_back(0);
        _dummyArrayStart.push_back(1);
        _dummyArrayEnd.push_back(8);
        _dummyArrayEnd.push_back(9);
        _dummyArrayId = s_addArray(_dummyArray, _dummyArrayStart, _dummyArrayEnd);

        //DUMMY_SHIFTED
        Dimensions dummyShiftedArrayDimensions;
        dummyShiftedArrayDimensions.push_back(DimensionDesc("x", 5, 5, 12, 14, 1, 0));
        dummyShiftedArrayDimensions.push_back(DimensionDesc("y", 5, 6, 13, 14, 1, 0));

        _dummyShiftedArray = ArrayDesc("opttest_dummy_shifted_array", dummyArrayAttributes, dummyShiftedArrayDimensions);
        _dummyShiftedArrayStart.push_back(5);
        _dummyShiftedArrayStart.push_back(6);
        _dummyShiftedArrayEnd.push_back(12);
        _dummyShiftedArrayEnd.push_back(13);
        _dummyShiftedArrayId = s_addArray(_dummyShiftedArray, _dummyShiftedArrayStart, _dummyShiftedArrayEnd);

        //SMALL
        Dimensions smallArrayDimensions;
        smallArrayDimensions.push_back(DimensionDesc("x", 0, 0, 0, 2, 1, 0));
        smallArrayDimensions.push_back(DimensionDesc("y", 0, 1, 2, 2, 1, 0));

        _smallArray = ArrayDesc("opttest_small_array", dummyArrayAttributes, smallArrayDimensions);
        _smallArrayStart.push_back(0);
        _smallArrayStart.push_back(1);
        _smallArrayEnd.push_back(0);
        _smallArrayEnd.push_back(2);
        _smallArrayId = s_addArray(_smallArray, _smallArrayStart, _smallArrayEnd);

        //SINGLEDIM
        Dimensions singleDimDimensions;
        singleDimDimensions.push_back(DimensionDesc("x", 0, 0, 3, 3, 1, 0));

        _singleDim = ArrayDesc("opttest_single_dim", dummyArrayAttributes, singleDimDimensions);
        _singleDimStart.push_back(0);
        _singleDimEnd.push_back(3);
        _singleDimId = s_addArray(_singleDim, _singleDimStart, _singleDimEnd);

        //PARTIALLYFILLED
        Dimensions partiallyFilledDimensions;
        partiallyFilledDimensions.push_back(DimensionDesc("x", 0, 0, 9, 9, 3, 0));
        partiallyFilledDimensions.push_back(DimensionDesc("y", 0, 0, 9, 9, 3, 0));

        _partiallyFilledArray = ArrayDesc("opttest_partially_filled", dummyArrayAttributes, partiallyFilledDimensions);
        _partiallyFilledStart.push_back(0);
        _partiallyFilledStart.push_back(0);
        _partiallyFilledEnd.push_back(9);
        _partiallyFilledEnd.push_back(9);
        _partiallyFilledId = s_addArray(_partiallyFilledArray, _partiallyFilledStart, _partiallyFilledEnd);

        //DUMMYFLIPPED
        Dimensions dummyFlippedDimensions;
        dummyFlippedDimensions.push_back(DimensionDesc("att0", 0, 5, 1, 0));
        dummyFlippedDimensions.push_back(DimensionDesc("att1", 0, 5, 1, 0));

        Attributes dummyFlippedAttributes;
        dummyFlippedAttributes.push_back(AttributeDesc(0, "x", TID_INT64, 0, (uint16_t) CompressorFactory::NO_COMPRESSION));
        dummyFlippedAttributes.push_back(AttributeDesc(1, "y", TID_INT64, 0, (uint16_t) CompressorFactory::NO_COMPRESSION));
        dummyFlippedAttributes.push_back(AttributeDesc(2, DEFAULT_EMPTY_TAG_ATTRIBUTE_NAME,  TID_INDICATOR, AttributeDesc::IS_EMPTY_INDICATOR, 0));

        _dummyFlippedArray = ArrayDesc("opttest_dummy_flipped", dummyFlippedAttributes, dummyFlippedDimensions);
        _dummyFlippedId = s_addArray(_dummyFlippedArray);

        _dummyReplicatedArray = ArrayDesc("opttest_dummy_replicated_array", dummyArrayAttributes, dummyArrayDimensions);
        _dummyReplicatedArrayId = s_addArray(_dummyReplicatedArray,_dummyArrayStart, _dummyArrayEnd, psReplication);

        ////////////////

        HabilisOptimizer *hopt = new HabilisOptimizer();
        hopt->_featureMask=0;
        _habilisDisabled = boost::shared_ptr<Optimizer>(hopt);

        _habilis = boost::shared_ptr<Optimizer>(new HabilisOptimizer());
    }

    void tearDown()
    {
        SystemCatalog* systemCat = SystemCatalog::getInstance();

        systemCat->deleteArray(_dummyArrayId);
        systemCat->deleteArray(_dummyShiftedArrayId);
        systemCat->deleteArray(_smallArrayId);
        systemCat->deleteArray(_singleDimId);
        systemCat->deleteArray(_partiallyFilledId);
        systemCat->deleteArray(_dummyFlippedId);
        systemCat->deleteArray(_dummyReplicatedArrayId);
    }

    boost::shared_ptr<Query> getQuery()
    {
        boost::shared_ptr<Query> query;
        boost::shared_ptr<const InstanceLiveness> liveness(Cluster::getInstance()->getInstanceLiveness());
        int32_t longErrorCode = SCIDB_E_NO_ERROR;
        query = Query::createFakeQuery(0, 0, liveness, &longErrorCode);
        if (longErrorCode != SCIDB_E_NO_ERROR &&
            longErrorCode != SCIDB_LE_INVALID_FUNCTION_ARGUMENT) {
                // NetworkManger::createWorkQueue() may complain about null queue
                // we can ignore that error because NetworkManager is not used
            throw SYSTEM_EXCEPTION(SCIDB_LE_UNKNOWN_ERROR, longErrorCode);
        }
        return query;
    }

    boost::shared_ptr<PhysicalPlan> habilis_d_generatePPlanFor(const char* queryString)
    {
        boost::shared_ptr<Query> query = getQuery();

        query->queryString = queryString;
        _queryProcessor->parseLogical(query, true);
        _queryProcessor->inferTypes(query);

        _queryProcessor->optimize(_habilisDisabled, query);
        return query->getCurrentPhysicalPlan();
    }

    boost::shared_ptr<PhysicalPlan> habilis_generatePPlanFor(const char* queryString, bool ail = true)
    {
        boost::shared_ptr<Query> query = getQuery();

        query->queryString = queryString;
        _queryProcessor->parseLogical(query, ail);
        _queryProcessor->inferTypes(query);

        _queryProcessor->optimize(_habilis, query);
        return query->getCurrentPhysicalPlan();
    }

    int countDfNodes(boost::shared_ptr<PhysicalPlan> pp)
    {
        int result = 1;
        boost::shared_ptr<PhysicalQueryPlanNode> node = pp->getRoot();

        while (node->getChildren().size() > 0)
        {
            node = node->getChildren()[0];
            result++;
        }
        return result;
    }

    int countTotalNodes(PhysNodePtr node)
    {
        int result = 1;
        for (size_t i = 0; i < node->getChildren().size(); i++)
        {
            result += countTotalNodes(node->getChildren()[i]);
        }
        return result;
    }

    //TODO: make this equivalence logic visible to the outside world? Overload operator==?
    bool equivalent(const AttributeDesc& lhs, const AttributeDesc& rhs)
    {
        if (lhs.getId() != rhs.getId() || lhs.getType() != rhs.getType() || lhs.getFlags() != rhs.getFlags() || lhs.getDefaultCompressionMethod()
                != rhs.getDefaultCompressionMethod())
        {
            return false;
        }

        return true;
    }

    bool equivalent(const DimensionDesc& lhs, const DimensionDesc& rhs)
    {
        if (  lhs.getChunkInterval() != rhs.getChunkInterval() ||
              lhs.getChunkOverlap() != rhs.getChunkOverlap() ||
              lhs.getCurrEnd() != rhs.getCurrEnd() ||
              lhs.getCurrStart() != rhs.getCurrStart() ||
              lhs.getEndMax() != rhs.getEndMax() ||
              lhs.getLength() != rhs.getLength() ||
              lhs.getStartMin() != rhs.getStartMin() ||
              lhs.getStartMin() != rhs.getStartMin())
        {
            return false;
        }

        return true;
    }

    bool equivalent(const ArrayDesc& lhs, const ArrayDesc& rhs)
    {
        if (lhs.getAttributes().size() != rhs.getAttributes().size())
        {
            return false;
        }
        for (size_t i = 0; i < lhs.getAttributes().size(); i++)
        {
            if (!equivalent(lhs.getAttributes()[i], rhs.getAttributes()[i]))
            {
                return false;
            }
        }

        if (lhs.getDimensions().size() != rhs.getDimensions().size())
        {
            return false;
        }
        for (size_t i = 0; i < lhs.getDimensions().size(); i++)
        {
            if (!equivalent(lhs.getDimensions()[i], rhs.getDimensions()[i]))
            {
                return false;
            }
        }

        if (lhs.getEmptyBitmapAttribute() == NULL)
        {
            if (rhs.getEmptyBitmapAttribute() != NULL)
            {
                return false;
            }
        }
        else if (rhs.getEmptyBitmapAttribute() == NULL)
        {
            return false;
        }
        else if (!equivalent(*lhs.getEmptyBitmapAttribute(), *rhs.getEmptyBitmapAttribute()))
        {
            return false;
        }

        if (lhs.getFlags() != rhs.getFlags() || lhs.getSize() != rhs.getSize())
        {
            return false;
        }

        return true;
    }

    void testBoundariesHelper(int64_t dim0Start, int64_t dim0End, int64_t dim0Chunk, int64_t dim1Start, int64_t dim1End, int64_t dim1Chunk,
                              int64_t start0, int64_t start1, int64_t end0, int64_t end1, uint64_t expectedNumCells, uint64_t expectedNumChunks)
    {
        Dimensions dummyArrayDimensions;
        dummyArrayDimensions.push_back(DimensionDesc("x", dim0Start, dim0End, dim0Chunk, 0));
        dummyArrayDimensions.push_back(DimensionDesc("y", dim1Start, dim1End, dim1Chunk, 0));

        Coordinates start;
        start.push_back(start0);
        start.push_back(start1);

        Coordinates end;
        end.push_back(end0);
        end.push_back(end1);

        PhysicalBoundaries bounds(start, end);
        uint64_t numCells = bounds.getNumCells();
        uint64_t numChunks = bounds.getNumChunks(dummyArrayDimensions);

        CPPUNIT_ASSERT(numCells == expectedNumCells);
        CPPUNIT_ASSERT(numChunks == expectedNumChunks);
    }

    void testBoundaries()
    {
        int64_t dim0Start, dim0End, dim0Chunk, dim1Start, dim1End, dim1Chunk, start0, start1, end0, end1;
        uint64_t expectedNumCells, expectedNumChunks;

#define TEST() testBoundariesHelper(dim0Start, dim0End, dim0Chunk, dim1Start, dim1End, dim1Chunk, start0, start1, end0, end1, expectedNumCells, expectedNumChunks)

        dim0Start = 0;
        dim0End = 9;
        dim0Chunk = 1;
        dim1Start = 0;
        dim1End = 9;
        dim1Chunk = 1;
        start0 = 3;
        start1 = 3;
        end0 = 4;
        end1 = 4;
        expectedNumCells = 4;
        expectedNumChunks = 4;
        TEST();

        start0 = 0;
        start1 = 0;
        end0 = 0;
        end1 = 0;
        expectedNumCells = 1;
        expectedNumChunks = 1;
        TEST();

        start0 = 0;
        start1 = 0;
        end0 = 9;
        end1 = 9;
        expectedNumCells = 100;
        expectedNumChunks = 100;
        TEST();

        start0 = 0;
        start1 = 0;
        end0 = -1;
        end1 = -1;
        expectedNumCells = 0;
        expectedNumChunks = 0;
        TEST();

        end0 = 3;
        TEST();

        dim0Start = 0;
        dim0End = 9;
        dim0Chunk = 3;
        dim1Start = 0;
        dim1End = 9;
        dim1Chunk = 3;
        start0 = 3;
        start1 = 3;
        end0 = 4;
        end1 = 4;
        expectedNumCells = 4;
        expectedNumChunks = 1;
        TEST();

        start0 = 2;
        start1 = 2;
        end0 = 7;
        end1 = 7;
        expectedNumCells = 36;
        expectedNumChunks = 9;
        TEST();

        start0 = 1;
        start1 = 2;
        end0 = 3;
        end1 = 9;
        expectedNumCells = 24;
        expectedNumChunks = 8;
        TEST();
    }

    void testBoundaries2()
    {
        Coordinates start1; start1.push_back(-75); start1.push_back(-74);
        Coordinates end1; end1.push_back(25); end1.push_back(26);

        Coordinates start2; start2.push_back(0); start2.push_back(1);
        Coordinates end2; end2.push_back(100); end2.push_back(101);

        PhysicalBoundaries bounds1(start1, end1, 0.25);
        PhysicalBoundaries bounds2(start2, end2, 0.25);

        PhysicalBoundaries intersection = bounds1.intersectWith(bounds2);
        CPPUNIT_ASSERT(intersection.getStartCoords()[0] == 0);
        CPPUNIT_ASSERT(intersection.getStartCoords()[1] == 1);
        CPPUNIT_ASSERT(intersection.getEndCoords()[0] == 25);
        CPPUNIT_ASSERT(intersection.getEndCoords()[1] == 26);
        CPPUNIT_ASSERT(intersection.getDensity() == 1.0);

        PhysicalBoundaries union_b = bounds1.unionWith(bounds2);
        CPPUNIT_ASSERT(union_b.getStartCoords()[0] == -75);
        CPPUNIT_ASSERT(union_b.getStartCoords()[1] == -74);
        CPPUNIT_ASSERT(union_b.getEndCoords()[0] == 100);
        CPPUNIT_ASSERT(union_b.getEndCoords()[1] == 101);
        CPPUNIT_ASSERT(union_b.getDensity() > 0.16 && union_b.getDensity() < 0.17);

        PhysicalBoundaries xproduct = bounds1.crossWith(bounds2);
        CPPUNIT_ASSERT(xproduct.getStartCoords()[0] == -75);
        CPPUNIT_ASSERT(xproduct.getStartCoords()[1] == -74);
        CPPUNIT_ASSERT(xproduct.getStartCoords()[2] == 0);
        CPPUNIT_ASSERT(xproduct.getStartCoords()[3] == 1);

        CPPUNIT_ASSERT(xproduct.getEndCoords()[0] == 25);
        CPPUNIT_ASSERT(xproduct.getEndCoords()[1] == 26);
        CPPUNIT_ASSERT(xproduct.getEndCoords()[2] == 100);
        CPPUNIT_ASSERT(xproduct.getEndCoords()[3] == 101);
        CPPUNIT_ASSERT(xproduct.getDensity() == bounds1.getDensity() * bounds2.getDensity());

        Dimensions dimOrig;
        dimOrig.push_back(DimensionDesc("dim1", -100,299,1,0));
        dimOrig.push_back(DimensionDesc("dim2", -124,275,1,0));

        Dimensions dimEnd;
        dimEnd.push_back(DimensionDesc("dim1", -5000,154999,1,0));

        PhysicalBoundaries reshapedBounds = bounds1.reshape(dimOrig,dimEnd);
        PhysicalBoundaries reshapedBack = reshapedBounds.reshape(dimEnd, dimOrig);

        CPPUNIT_ASSERT(reshapedBounds.getStartCoords()[0] == 5050);
        CPPUNIT_ASSERT(reshapedBounds.getEndCoords()[0] == 45150);

        CPPUNIT_ASSERT(reshapedBounds.getNumCells() * reshapedBounds.getDensity() == bounds1.getNumCells() * bounds1.getDensity());

        CPPUNIT_ASSERT(reshapedBack.getStartCoords()[0] == bounds1.getStartCoords()[0] );
        CPPUNIT_ASSERT(reshapedBack.getEndCoords()[0] == bounds1.getEndCoords()[0] );

        CPPUNIT_ASSERT(reshapedBack.getNumCells() * reshapedBack.getDensity() == bounds1.getNumCells() * bounds1.getDensity());
    }

    void testBasic()
    {
        boost::shared_ptr<PhysicalPlan> pp = habilis_d_generatePPlanFor("scan(opttest_dummy_array)");
        CPPUNIT_ASSERT(pp->isDdl()==false);
        CPPUNIT_ASSERT(countDfNodes(pp) == 1);
        ASSERT_OPERATOR(pp->getRoot(), "physicalScan");
        CPPUNIT_ASSERT(pp->getRoot()->hasParent() == false);
        ArrayDesc opSchema = pp->getRoot()->getPhysicalOperator()->getSchema();
        CPPUNIT_ASSERT(equivalent(opSchema, _dummyArray));

        pp = habilis_generatePPlanFor("scan(opttest_dummy_array)");
        CPPUNIT_ASSERT(pp->isDdl()==false);
        CPPUNIT_ASSERT(countDfNodes(pp) == 1);
        ASSERT_OPERATOR(pp->getRoot(), "physicalScan");
        CPPUNIT_ASSERT(pp->getRoot()->hasParent() == false);
        opSchema = pp->getRoot()->getPhysicalOperator()->getSchema();
        CPPUNIT_ASSERT(equivalent(opSchema, _dummyArray));
    }

    void testThreeInstanceSgInsert(const char* query, const char* opName)
    {
        boost::shared_ptr<PhysicalPlan> pp = habilis_d_generatePPlanFor(query);

        CPPUNIT_ASSERT(countDfNodes(pp) == 2);
        PhysNodePtr expectedSubArrayNode = pp->getRoot();
        ASSERT_OPERATOR(expectedSubArrayNode,opName);
        PhysOpPtr expectedSubArrayOp = expectedSubArrayNode->getPhysicalOperator();
        CPPUNIT_ASSERT(expectedSubArrayNode->getChildren().size() == 1);
        CPPUNIT_ASSERT(expectedSubArrayNode->hasParent() == false);

        PhysNodePtr expectedScanNode = expectedSubArrayNode->getChildren()[0];
        PhysOpPtr expectedScanOp = expectedScanNode->getPhysicalOperator();

        ASSERT_OPERATOR(expectedScanNode, "physicalScan");
        CPPUNIT_ASSERT(expectedScanNode->getParent()==expectedSubArrayNode);
        CPPUNIT_ASSERT(expectedScanNode->getChildren().size() == 0);
        CPPUNIT_ASSERT(equivalent (expectedScanOp->getSchema(), _dummyArray));
        CPPUNIT_ASSERT(!equivalent (expectedScanOp->getSchema(), expectedSubArrayOp->getSchema()));

        pp = habilis_generatePPlanFor(query);

        CPPUNIT_ASSERT(countDfNodes(pp) == 2);
        expectedSubArrayNode = pp->getRoot();
        ASSERT_OPERATOR(expectedSubArrayNode,opName);
        expectedSubArrayOp = expectedSubArrayNode->getPhysicalOperator();
        CPPUNIT_ASSERT(expectedSubArrayNode->getChildren().size() == 1);
        CPPUNIT_ASSERT(expectedSubArrayNode->hasParent() == false);

        expectedScanNode = expectedSubArrayNode->getChildren()[0];
        expectedScanOp = expectedScanNode->getPhysicalOperator();

        ASSERT_OPERATOR(expectedScanNode, "physicalScan");
        CPPUNIT_ASSERT(expectedScanNode->getParent()==expectedSubArrayNode);
        CPPUNIT_ASSERT(expectedScanNode->getChildren().size() == 0);
        CPPUNIT_ASSERT(equivalent (expectedScanOp->getSchema(), _dummyArray));
        CPPUNIT_ASSERT(!equivalent (expectedScanOp->getSchema(), expectedSubArrayOp->getSchema()));
    }

    void testSubArrayReshapeSgInsertions()
    {
        testThreeInstanceSgInsert("subarray(opttest_dummy_array, 5,5,10,10)", "physicalSubArray");
        testThreeInstanceSgInsert("reshape(opttest_dummy_array, opttest_dummy_shifted_array)", "physicalReshape");
    }

    void testInputSgInsert()
    {
        if (!boost::filesystem::exists("/tmp/tmpfile"))
        {
            int fd = open("/tmp/tmpfile", O_RDWR|O_TRUNC|O_EXCL|O_CREAT, 0666);
            close(fd);
        }

        boost::shared_ptr<PhysicalPlan> pp = habilis_d_generatePPlanFor("input(opttest_dummy_array, '/tmp/tmpfile')");

        CPPUNIT_ASSERT(countDfNodes(pp) == 1);
        PhysNodePtr expectedInputNode = pp->getRoot();
        ASSERT_OPERATOR(expectedInputNode, "impl_input");
        PhysOpPtr expectedInputOp = expectedInputNode->getPhysicalOperator();

        CPPUNIT_ASSERT(equivalent (expectedInputOp->getSchema(), _dummyArray) );
        CPPUNIT_ASSERT(expectedInputNode->hasParent() == false);
        CPPUNIT_ASSERT(expectedInputNode->getChildren().size() == 0);

        pp = habilis_generatePPlanFor("input(opttest_dummy_array, '/tmp/tmpfile')");

        CPPUNIT_ASSERT(countDfNodes(pp) == 1);
        expectedInputNode = pp->getRoot();
        ASSERT_OPERATOR(expectedInputNode, "impl_input");

        expectedInputOp = expectedInputNode->getPhysicalOperator();

        CPPUNIT_ASSERT(equivalent (expectedInputOp->getSchema(), _dummyArray) );
        CPPUNIT_ASSERT(expectedInputNode->hasParent() == false);
        CPPUNIT_ASSERT(expectedInputNode->getChildren().size() == 0);
    }

    void testConcatSgInsert()
    {
        //this test case will need to change as optimizer gets smarter about concat
        boost::shared_ptr<PhysicalPlan> pp = habilis_d_generatePPlanFor("concat(opttest_dummy_array, opttest_dummy_array)");

        CPPUNIT_ASSERT(countDfNodes(pp) == 2);
        CPPUNIT_ASSERT(countTotalNodes(pp->getRoot()) == 3);

        PhysNodePtr expectedConcatNode = pp->getRoot();
        ASSERT_OPERATOR(expectedConcatNode, "physicalConcat");

        PhysOpPtr expectedConcatOp = expectedConcatNode->getPhysicalOperator();
        CPPUNIT_ASSERT(expectedConcatNode->hasParent() == false);
        CPPUNIT_ASSERT(expectedConcatNode->getChildren().size() == 2);

        PhysNodePtr expectedScanLeftNode = expectedConcatNode->getChildren()[0];
        PhysOpPtr expectedScanLeftOp = expectedScanLeftNode->getPhysicalOperator();

        CPPUNIT_ASSERT(expectedScanLeftNode->getChildren().size() == 0);
        CPPUNIT_ASSERT(expectedScanLeftNode->getParent() == expectedConcatNode);
        CPPUNIT_ASSERT(equivalent(expectedScanLeftOp->getSchema(), _dummyArray) ||
                equivalent(expectedScanLeftOp->getSchema(), _dummyShiftedArray));
        CPPUNIT_ASSERT(!equivalent(expectedScanLeftOp->getSchema(), expectedConcatOp->getSchema()));

        PhysNodePtr expectedScanRightNode = expectedConcatNode->getChildren()[0];
        PhysOpPtr expectedScanRightOp = expectedScanRightNode->getPhysicalOperator();

        CPPUNIT_ASSERT(expectedScanRightNode->getParent() == expectedConcatNode);
        CPPUNIT_ASSERT(expectedScanRightNode->getChildren().size() == 0);
        CPPUNIT_ASSERT(equivalent(expectedScanRightOp->getSchema(), _dummyArray) ||
                equivalent(expectedScanRightOp->getSchema(), _dummyShiftedArray));
        CPPUNIT_ASSERT(!equivalent(expectedScanRightOp->getSchema(), expectedConcatOp->getSchema()));

        pp = habilis_generatePPlanFor("concat(opttest_dummy_array, opttest_dummy_array)");

        CPPUNIT_ASSERT(countDfNodes(pp) == 2);
        CPPUNIT_ASSERT(countTotalNodes(pp->getRoot()) == 3);

        expectedConcatNode = pp->getRoot();
        ASSERT_OPERATOR(expectedConcatNode, "physicalConcat");
        expectedConcatOp = expectedConcatNode->getPhysicalOperator();

        CPPUNIT_ASSERT(expectedConcatNode->hasParent() == false);
        CPPUNIT_ASSERT(expectedConcatNode->getChildren().size() == 2);

        expectedScanLeftNode = expectedConcatNode->getChildren()[0];
        expectedScanLeftOp = expectedScanLeftNode->getPhysicalOperator();

        CPPUNIT_ASSERT(expectedScanLeftNode->getChildren().size() == 0);
        CPPUNIT_ASSERT(expectedScanLeftNode->getParent() == expectedConcatNode);
        CPPUNIT_ASSERT(equivalent(expectedScanLeftOp->getSchema(), _dummyArray) ||
                equivalent(expectedScanLeftOp->getSchema(), _dummyShiftedArray));
        CPPUNIT_ASSERT(!equivalent(expectedScanLeftOp->getSchema(), expectedConcatOp->getSchema()));

        expectedScanRightNode = expectedConcatNode->getChildren()[0];
        expectedScanRightOp = expectedScanRightNode->getPhysicalOperator();

        CPPUNIT_ASSERT(expectedScanRightNode->getParent() == expectedConcatNode);
        CPPUNIT_ASSERT(expectedScanRightNode->getChildren().size() == 0);
        CPPUNIT_ASSERT(equivalent(expectedScanRightOp->getSchema(), _dummyArray) ||
                equivalent(expectedScanRightOp->getSchema(), _dummyShiftedArray));
        CPPUNIT_ASSERT(!equivalent(expectedScanRightOp->getSchema(), expectedConcatOp->getSchema()));
    }

    void testHabilisCollapse()
    {
        std::ostringstream out;
        boost::shared_ptr<PhysicalPlan> pp;
        ArrayDistribution dist;

        PhysNodePtr root, leftChild, rightChild;

        //remove all sgs
        pp = habilis_generatePPlanFor("subarray(subarray(opttest_dummy_array, 5,5,10,10),2,3,2,4)");
//        pp->toString(out);
//        std::cout<<out.str();
//        out.str("");

        root = pp->getRoot();
        dist = root->getDistribution();
        CPPUNIT_ASSERT(dist.isViolated() == true && dist.hasMapper() == true && dist.getMapper()->getOffsetVector()[0] == 7 && dist.getMapper()->getOffsetVector()[1]==8);
        ASSERT_OPERATOR(root,"physicalSubArray");
        CPPUNIT_ASSERT(root->getChildren().size() == 1);

        root = root->getChildren()[0];
        dist = root->getDistribution();
        CPPUNIT_ASSERT(dist.isViolated() == true && dist.hasMapper() == true && dist.getMapper()->getOffsetVector()[0] == 5 && dist.getMapper()->getOffsetVector()[1]==5);
        ASSERT_OPERATOR(root,"physicalSubArray");
        CPPUNIT_ASSERT(root->getChildren().size() == 1);

        root = root->getChildren()[0];
        dist = root->getDistribution();
        CPPUNIT_ASSERT(dist.isViolated() == false && dist.hasMapper() ==false);
        ASSERT_OPERATOR(root,"physicalScan");
        CPPUNIT_ASSERT(root->getChildren().size() == 0);

        //remove toplevel sg and left sg
        pp = habilis_generatePPlanFor("subarray(join(subarray(opttest_dummy_array,3,3,4,4),subarray(opttest_dummy_array,1,1,2,2)), 1,1,1,1)");
//        pp->toString(out);
//        std::cout<<out.str();
//        out.str("");

        root = pp->getRoot();
        ASSERT_OPERATOR(root,"physicalSubArray");
        dist = root->getDistribution();
        CPPUNIT_ASSERT(dist.isViolated() == true && dist.hasMapper() == true && dist.getMapper()->getOffsetVector()[0] ==4 && dist.getMapper()->getOffsetVector()[1]==4);
        CPPUNIT_ASSERT(root->getChildren().size() == 1);

        root = root->getChildren()[0];
        ASSERT_OPERATOR(root,"physicalJoin");
        dist = root->getDistribution();
        CPPUNIT_ASSERT(dist.isViolated() == true && dist.hasMapper() == true && dist.getMapper()->getOffsetVector()[0] == 3 && dist.getMapper()->getOffsetVector()[1]==3);
        CPPUNIT_ASSERT(root->getChildren().size() == 2);

        leftChild = root->getChildren()[0];
        ASSERT_OPERATOR(leftChild,"physicalSubArray");
        CPPUNIT_ASSERT(leftChild->getDataWidth() == leftChild->getChildren()[0]->getDataWidth() * 4.0 / 81.0);
        dist = leftChild->getDistribution();
        CPPUNIT_ASSERT(dist.isViolated() == true && dist.hasMapper() == true && dist.getMapper()->getOffsetVector()[0]==3 && dist.getMapper()->getOffsetVector()[1]==3);
        CPPUNIT_ASSERT(leftChild->getChildren().size() == 1);

        leftChild = leftChild->getChildren()[0];
        ASSERT_OPERATOR(leftChild,"physicalScan");
        dist = leftChild->getDistribution();
        CPPUNIT_ASSERT(dist.isViolated() == false && dist.hasMapper() == false);
        CPPUNIT_ASSERT(leftChild->getChildren().size() == 0);

        rightChild = root->getChildren()[1];
        ASSERT_OPERATOR(rightChild,"impl_sg");
        dist = rightChild->getDistribution();
        CPPUNIT_ASSERT(dist.isViolated() == true && dist.hasMapper() == true && dist.getMapper()->getOffsetVector()[0]==3 && dist.getMapper()->getOffsetVector()[1]==3);
        CPPUNIT_ASSERT(rightChild->getChildren().size() == 1);

        rightChild = rightChild->getChildren()[0];
        ASSERT_OPERATOR(rightChild,"physicalSubArray");
        dist = rightChild->getDistribution();
        CPPUNIT_ASSERT(dist.isViolated() == true && dist.hasMapper() == true && dist.getMapper()->getOffsetVector()[0]==1 && dist.getMapper()->getOffsetVector()[1]==1);
        CPPUNIT_ASSERT(rightChild->getChildren().size() == 1);

        rightChild = rightChild->getChildren()[0];
        ASSERT_OPERATOR(rightChild,"physicalScan");
        dist = rightChild->getDistribution();
        CPPUNIT_ASSERT(dist.isViolated() == false && dist.hasMapper() == false);
        CPPUNIT_ASSERT(rightChild->getChildren().size() == 0);

        //remove SG after subarray; keep SG after reshape
        pp = habilis_generatePPlanFor("subarray(reshape(opttest_dummy_array, opttest_dummy_shifted_array), 6,6,7,7)");
        root = pp->getRoot();
        ASSERT_OPERATOR(root,"physicalSubArray");
        dist = root->getDistribution();
        CPPUNIT_ASSERT(dist.isViolated() == true && dist.hasMapper() == true && dist.getMapper()->getOffsetVector()[0] == 1 && dist.getMapper()->getOffsetVector()[1]==1);
        CPPUNIT_ASSERT(root->getChildren().size() == 1);

        root = root->getChildren()[0];
        ASSERT_OPERATOR(root, "impl_sg");
        dist = root->getDistribution();
        CPPUNIT_ASSERT(dist.isViolated() == false && dist.hasMapper() == false);
        CPPUNIT_ASSERT(root->getChildren().size() == 1);

        root = root->getChildren()[0];
        ASSERT_OPERATOR(root, "physicalReshape");
        CPPUNIT_ASSERT(root->getDataWidth() == root->getChildren()[0]->getDataWidth());
        dist = root->getDistribution();
        CPPUNIT_ASSERT(dist.isViolated() == true && dist.hasMapper() == false);
        CPPUNIT_ASSERT(root->getChildren().size() == 1);

        root = root->getChildren()[0];
        ASSERT_OPERATOR(root, "physicalScan");
        dist = root->getDistribution();
        CPPUNIT_ASSERT(dist.isViolated() == false && dist.hasMapper() == false);
        CPPUNIT_ASSERT(root->getChildren().size() == 0);

        //remove both sgs since reshape is toplevel
        pp = habilis_generatePPlanFor("reshape(subarray(opttest_dummy_array,3,4,5,6), opttest_small_array)");
        root = pp->getRoot();
        ASSERT_OPERATOR(root, "physicalReshape");
        dist = root->getDistribution();
        CPPUNIT_ASSERT(dist.isViolated() == true && dist.hasMapper() == false);
        CPPUNIT_ASSERT(root->getChildren().size() == 1);

        root = root->getChildren()[0];
        ASSERT_OPERATOR(root,"physicalSubArray");
        dist = root->getDistribution();
        CPPUNIT_ASSERT(dist.isViolated() == true && dist.hasMapper() == true && dist.getMapper()->getOffsetVector()[0]==3 && dist.getMapper()->getOffsetVector()[1]==4);
        CPPUNIT_ASSERT(root->getChildren().size() == 1);

        root = root->getChildren()[0];
        ASSERT_OPERATOR(root, "physicalScan");
        dist = root->getDistribution();
        CPPUNIT_ASSERT(dist.isViolated() == false && dist.hasMapper() == false);
        CPPUNIT_ASSERT(root->getChildren().size() == 0);

        //only remove toplevel sg and sg between two subarrays
        pp
                = habilis_generatePPlanFor(
                                           "subarray(join (subarray(subarray(opttest_dummy_array,0,0,9,9),0,0,9,9), reshape(opttest_dummy_shifted_array,opttest_dummy_array)), 1,1,1,1)");

        root = pp->getRoot();
        ASSERT_OPERATOR(root,"physicalSubArray");
        CPPUNIT_ASSERT(root->getChildren().size() == 1);
        root = root->getChildren()[0];
        ASSERT_OPERATOR(root,"physicalJoin");
        CPPUNIT_ASSERT(root->getChildren().size() == 2);
        leftChild = root->getChildren()[0];
        ASSERT_OPERATOR(leftChild,"physicalSubArray");
        CPPUNIT_ASSERT(leftChild->getChildren().size() == 1);
        leftChild = leftChild->getChildren()[0];
        ASSERT_OPERATOR(leftChild,"physicalSubArray");
        CPPUNIT_ASSERT(leftChild->getChildren().size() == 1);
        leftChild = leftChild->getChildren()[0];
        ASSERT_OPERATOR(leftChild,"physicalScan");
        CPPUNIT_ASSERT(leftChild->getChildren().size() == 0);

        rightChild = root->getChildren()[1];
        ASSERT_OPERATOR(rightChild,"impl_sg");
        CPPUNIT_ASSERT(rightChild->getChildren().size() == 1);
        rightChild = rightChild->getChildren()[0];
        ASSERT_OPERATOR(rightChild,"physicalReshape");
        CPPUNIT_ASSERT(rightChild->getChildren().size() == 1);
        rightChild = rightChild->getChildren()[0];
        ASSERT_OPERATOR(rightChild,"physicalScan");
        CPPUNIT_ASSERT(rightChild->getChildren().size() == 0);

        //cut out left sg. put right sg before the apply
        pp = habilis_generatePPlanFor("join(subarray(opttest_dummy_array,0,0,1,1),apply(subarray(opttest_dummy_array,1,1,2,2),sum,att0+att0))");
//        pp->toString(out);
//        std::cout<<out.str();
//        out.str("");

        root = pp->getRoot();
        ASSERT_OPERATOR(root,"physicalJoin");

        leftChild = root->getChildren()[0];
        ASSERT_OPERATOR(leftChild,"impl_sg");
        leftChild = leftChild->getChildren()[0];
        ASSERT_OPERATOR(leftChild,"physicalSubArray");
        leftChild = leftChild->getChildren()[0];
        ASSERT_OPERATOR(leftChild,"physicalScan");

        rightChild = root->getChildren()[1];
        ASSERT_OPERATOR(rightChild,"physicalApply");
        rightChild = rightChild->getChildren()[0];
        ASSERT_OPERATOR(rightChild,"physicalSubArray");
        rightChild = rightChild->getChildren()[0];
        ASSERT_OPERATOR(rightChild,"physicalScan");

        pp = habilis_generatePPlanFor("concat(subarray(opttest_dummy_array,0,0,1,1),subarray(opttest_dummy_array,1,1,2,2))");
        root = pp->getRoot();

//        pp->toString(out);
//        std::cout<<out.str();
//        out.str("");

        ASSERT_OPERATOR(root,"physicalConcat");
        CPPUNIT_ASSERT(root->getDistribution().isViolated() == true);
        CPPUNIT_ASSERT(root->getDataWidth() == root->getChildren()[0]->getDataWidth() + root->getChildren()[1]->getDataWidth());

        leftChild = root->getChildren()[0];
        ASSERT_OPERATOR(leftChild,"physicalSubArray");
        leftChild = leftChild->getChildren()[0];
        ASSERT_OPERATOR(leftChild,"physicalScan");

        rightChild = root->getChildren()[1];
        ASSERT_OPERATOR(rightChild,"physicalSubArray");
        rightChild = rightChild->getChildren()[0];
        ASSERT_OPERATOR(rightChild,"physicalScan");

        pp
                = habilis_generatePPlanFor(
                                           "join ( subarray ( concat(subarray(opttest_dummy_array,0,0,0,2),subarray(opttest_dummy_array,1,0,2,2)),1,1,2,2), project(apply(join(subarray(opttest_dummy_array,0,0,1,1) as foo, subarray(opttest_dummy_array,0,0,1,1) as bar), sum, foo.att0+bar.att0),sum))");
//        pp->toString(out);
//        std::cout<<out.str();
//        out.str("");

        root = pp->getRoot();
        ASSERT_OPERATOR(root,"physicalJoin");

        leftChild = root->getChildren()[0];
        ASSERT_OPERATOR(leftChild,"impl_sg");
        leftChild = leftChild->getChildren()[0];
        ASSERT_OPERATOR(leftChild,"physicalSubArray");
        leftChild = leftChild->getChildren()[0];
        ASSERT_OPERATOR(leftChild,"physicalConcat");

        rightChild = root->getChildren()[1];
        ASSERT_OPERATOR(rightChild,"physicalProject");
        rightChild = rightChild->getChildren()[0];
        ASSERT_OPERATOR(rightChild,"physicalApply");
        rightChild = rightChild->getChildren()[0];
        ASSERT_OPERATOR(rightChild,"physicalJoin");


        pp
                = habilis_generatePPlanFor(
                                           "join ( apply(subarray(project(opttest_dummy_array,att1),0,0,1,1), att0, att1+att1), subarray(opttest_dummy_array,1,1,2,2))");
//        pp->toString(out);
//        std::cout<<out.str();
//        out.str("");
        //ensure SG gets placed at thinpoint - on the left, before APPLY
        root = pp->getRoot();
        ASSERT_OPERATOR(root, "physicalJoin");
        leftChild = root->getChildren()[0];
        ASSERT_OPERATOR(leftChild, "physicalApply");
        leftChild = leftChild->getChildren()[0];
        ASSERT_OPERATOR(leftChild, "impl_sg");
        leftChild = leftChild->getChildren()[0];
        ASSERT_OPERATOR(leftChild, "physicalSubArray");
        leftChild = leftChild->getChildren()[0];
	ASSERT_OPERATOR(leftChild, "impl_materialize");
	leftChild = leftChild->getChildren()[0];
        ASSERT_OPERATOR(leftChild, "physicalProject");
        CPPUNIT_ASSERT(leftChild->getDataWidth() == leftChild->getChildren()[0]->getDataWidth() / 2.0 );
        leftChild = leftChild->getChildren()[0];
        ASSERT_OPERATOR(leftChild, "physicalScan");
        rightChild = root->getChildren()[1];
        ASSERT_OPERATOR(rightChild, "physicalSubArray");
        rightChild = rightChild->getChildren()[0];
        ASSERT_OPERATOR(rightChild, "physicalScan");

        //just don't crash... will add more checks when optimizer is smarter
        pp
                = habilis_generatePPlanFor(
                                           "store(join ( subarray ( concat(subarray(opttest_dummy_array,0,0,0,2),subarray(opttest_dummy_array,1,0,2,2)),1,1,2,2), project(apply(join(subarray(opttest_dummy_array,0,0,1,1) as foo, subarray(opttest_dummy_array,0,0,1,1) as bar), sum, foo.att0+bar.att0),sum)), foobar)");
        pp = habilis_generatePPlanFor("join(subarray(opttest_dummy_array,0,0,9,9), sg(subarray(opttest_dummy_array,0,0,9,9),1,-1))");
        //        pp->toString(out);
        //        std::cout<<out.str();
        //        out.str("");
        pp
                = habilis_generatePPlanFor(
                                           "concat(subarray(join (subarray(subarray(opttest_dummy_array,0,0,9,9),0,0,9,9), reshape(opttest_dummy_shifted_array,opttest_dummy_array)), 1,1,3,3), join(opttest_small_array, reshape(opttest_small_array,opttest_small_array)))");
        //        pp->toString(out);
        //        std::cout<<out.str();
        //        out.str("");

        pp = habilis_generatePPlanFor(
                                      " concat( subarray(apply(join(subarray(opttest_dummy_array,2,2,4,4) as foo,subarray(opttest_dummy_array,0,0,2,2) as bar),sum,foo.att0+bar.att0),1,1,2,2), "
                                          "         subarray(apply(join(subarray(opttest_dummy_array,0,0,2,2) as foo,subarray(opttest_dummy_array,2,2,4,4) as bar),sum,foo.att0+bar.att0),0,0,1,1)"
                                          " )");

        pp = habilis_generatePPlanFor(" join ( "
            "       subarray ("
            "             concat ( subarray(opttest_dummy_array,0,0,0,0), subarray(opttest_dummy_array,1,1,1,1)),"
            "             0,0,0,0),"
            "       subarray ("
            "             join ( subarray(opttest_dummy_array,1,1,1,1), subarray(opttest_dummy_array,0,0,0,0)),"
            "                  0,0,0,0))");
        //        pp->toString(out);
        //        std::cout<<out.str();
        //        out.str("");
    }

    void testTwoPhase()
    {
        std::ostringstream out;
        boost::shared_ptr<PhysicalPlan> pp;
        PhysNodePtr root, child;

        pp = habilis_generatePPlanFor("sum(opttest_single_dim)");
        //        pp->toString(out);
        //        std::cout<<out.str();
        //        out.str("");
        root = pp->getRoot();
        ASSERT_OPERATOR(root,"physicalSum2");
        root = root->getChildren()[0];
        ASSERT_OPERATOR(root,"physicalSum");
        root = root->getChildren()[0];
        ASSERT_OPERATOR(root,"physicalScan");
        CPPUNIT_ASSERT(root->getChildren().size() == 0);

        pp = habilis_generatePPlanFor("join(opttest_single_dim, sort (opttest_single_dim,att0) )");
        //        pp->toString(out);
        //        std::cout<<out.str();
        //        out.str("");
        root = pp->getRoot();
        ASSERT_OPERATOR(root,"physicalJoin");
        child = root->getChildren()[0];
        ASSERT_OPERATOR(child,"physicalScan");
        CPPUNIT_ASSERT(child->getChildren().size() == 0);
        child = root->getChildren()[1];
        ASSERT_OPERATOR(child,"impl_sg");
        child = child->getChildren()[0];
        ASSERT_OPERATOR(child,"physicalSort2");
        child = child->getChildren()[0];
        ASSERT_OPERATOR(child,"physicalSort");
        child = child->getChildren()[0];
        ASSERT_OPERATOR(child,"physicalScan");
        CPPUNIT_ASSERT(child->getChildren().size() == 0);

        pp = habilis_generatePPlanFor("join( subarray(opttest_single_dim,0,0) , sum(opttest_single_dim))");
        //        pp->toString(out);
        //        std::cout<<out.str();
        //        out.str("");
        root = pp->getRoot();
        ASSERT_OPERATOR(root,"physicalJoin");
        child = root->getChildren()[0];
        ASSERT_OPERATOR(child,"physicalSubArray");
        child = child->getChildren()[0];
        ASSERT_OPERATOR(child,"physicalScan");
        CPPUNIT_ASSERT(child->getChildren().size() == 0);
        child = root->getChildren()[1];
        ASSERT_OPERATOR(child,"impl_sg");
        child = child->getChildren()[0];
        ASSERT_OPERATOR(child,"physicalSum2");
        child = child->getChildren()[0];
        ASSERT_OPERATOR(child,"physicalSum");
        child = child->getChildren()[0];
        ASSERT_OPERATOR(child,"physicalScan");
        CPPUNIT_ASSERT(child->getChildren().size() == 0);

        pp = habilis_generatePPlanFor("join( subarray(opttest_single_dim,0,1) , subarray (sort (opttest_single_dim,att0), 1,2 ))");
        //        pp->toString(out);
        //        std::cout<<out.str();
        //        out.str("");
        root = pp->getRoot();
        ASSERT_OPERATOR(root,"physicalJoin");
        child = root->getChildren()[0];
        ASSERT_OPERATOR(child,"physicalSubArray");
        child = child->getChildren()[0];
        ASSERT_OPERATOR(child,"physicalScan");
        CPPUNIT_ASSERT(child->getChildren().size() == 0);
        child = root->getChildren()[1];
        ASSERT_OPERATOR(child, "impl_sg");
        child = child->getChildren()[0];
        ASSERT_OPERATOR(child, "physicalSubArray");
        child = child->getChildren()[0];
        ASSERT_OPERATOR(child, "impl_sg");
        child = child->getChildren()[0];
        ASSERT_OPERATOR(child, "physicalSort2");
        child = child->getChildren()[0];
        ASSERT_OPERATOR(child, "physicalSort");
        CPPUNIT_ASSERT(child->getDataWidth() == child->getChildren()[0]->getDataWidth());
        child = child->getChildren()[0];
        ASSERT_OPERATOR(child, "physicalScan");
        CPPUNIT_ASSERT(child->getChildren().size()==0);

        pp = habilis_generatePPlanFor("join( subarray(load(opttest_single_dim,'dummy_file_path'), 0,1) , subarray(opttest_single_dim,1,2))");
        //        pp->toString(out);
        //        std::cout<<out.str();
        //        out.str("");
        root = pp->getRoot();
        child = root->getChildren()[0];
        ASSERT_OPERATOR(child, "physicalSubArray");
        child = child->getChildren()[0];

        std::vector<InstanceID> instances;
        size_t nInstances = Cluster::getInstance()->getInstanceMembership()->getInstances().size();

        if (nInstances == 1)
        {
            ASSERT_OPERATOR(child, "physicalStore");
            child = child->getChildren()[0];
        }

        ASSERT_OPERATOR(child, "impl_sg");
        if (nInstances != 1)
        {
            CPPUNIT_ASSERT( child->isSgMovable() == false);
        }

        child = child->getChildren()[0];
        ASSERT_OPERATOR(child, "impl_input");
        child = root->getChildren()[1];
        ASSERT_OPERATOR(child, "impl_sg");
        child = child->getChildren()[0];
        ASSERT_OPERATOR(child, "physicalSubArray");
        child = child->getChildren()[0];
        ASSERT_OPERATOR(child, "physicalScan");
    }

    void testMultiply()
    {
        std::ostringstream out;
        boost::shared_ptr<PhysicalPlan> pp;
        PhysNodePtr root, child;

        //SG results of subarray to match results of multply
        pp
                = habilis_generatePPlanFor(
                                           "join ( apply(multiply(project(opttest_small_array,att0), project(opttest_small_array,att0)), att1, instanceid()), subarray(project(opttest_dummy_array,att0), 2,2,4,4))");
//                pp->toString(out);
//                std::cout<<out.str();
//                out.str("");
        root = pp->getRoot();
        child = root->getChildren()[0];
        ASSERT_OPERATOR(child, "physicalApply");
        child = child->getChildren()[0];
        ASSERT_OPERATOR(child, "impl_sg");

        //Multiply of two subarrays - no sgs needed
        pp
                = habilis_generatePPlanFor(
                                           "multiply ( subarray(project(opttest_dummy_array,att0), 0,0,2,2), subarray(project(opttest_dummy_array,att0), 0,0,2,2))");
        //        pp->toString(out);
        //        std::cout<<out.str();
        //        out.str("");
        root = pp->getRoot();
        child = root->getChildren()[0];
        ASSERT_OPERATOR(child, "physicalSubArray");
        child = root->getChildren()[1];
        ASSERT_OPERATOR(child, "physicalSubArray");

        pp
                = habilis_generatePPlanFor(
                                           "join(transpose(multiply(project(opttest_dummy_array,att0), project(opttest_dummy_array,att0))),transpose(project(opttest_dummy_array,att0)))");
//                pp->toString(out);
//                std::cout<<out.str();
//                out.str("");
        //LHS is a transpose that emits byrow distribution
        //RHS is a transpose that has violated round robin -- put SG on top of it.
        root = pp->getRoot();
        child = root->getChildren()[0];
        ASSERT_OPERATOR(child, "impl_sg");
        child = root->getChildren()[1];
        ASSERT_OPERATOR(child, "impl_sg");
    }

    void testFlipStoreRewrite()
    {
        boost::shared_ptr<PhysicalPlan> pp;
        PhysNodePtr root;

        pp = habilis_generatePPlanFor( "select * into some_weird_array_we_hope_does_not_exist from opttest_dummy_array", false);
        root = pp->getRoot();
        ASSERT_OPERATOR(root, "physicalStore");

        pp = habilis_generatePPlanFor (" select * into opttest_dummy_array from opttest_dummy_array", false);
        root = pp->getRoot();
        ASSERT_OPERATOR(root, "physicalStore");

        pp = habilis_generatePPlanFor (" select * into opttest_dummy_flipped from opttest_dummy_array", false);
        root = pp->getRoot();
        ASSERT_OPERATOR(root, "physicalStore");

        bool thrown = false;
        try
        {
            pp = habilis_generatePPlanFor (" select * into opttest_single_dim from opttest_dummy_array", false);
        }
        catch(...)
        {
            thrown = true;
        }
        //now an exception is thrown as AQL rewriter has been changed
        CPPUNIT_ASSERT(thrown);
    }

    void testReplication()
    {
        boost::shared_ptr<PhysicalPlan> pp;
        PhysNodePtr root;
        std::ostringstream out;


        //Verify that there's a reduceDistro (not sg!) inserted between scan and store.
        pp = habilis_generatePPlanFor( "store(opttest_dummy_replicated_array, some_weird_array_we_hope_does_not_exist)");
//        pp->toString(out);
//        std::cout<<out.str();
//        out.str("");
        root = pp->getRoot();
        ASSERT_OPERATOR(root, "physicalStore");
        root = root->getChildren()[0];
        ASSERT_OPERATOR(root, "physicalReduceDistro");
        root = root->getChildren()[0];
        ASSERT_OPERATOR(root, "physicalScan");

        //Verify that there's a reduceDistro inserted between scan and aggregate so we get the correct count
        pp = habilis_generatePPlanFor( "aggregate(opttest_dummy_replicated_array, count(*))");
//        pp->toString(out);
//        std::cout<<out.str();
//        out.str("");
        root = pp->getRoot();
        ASSERT_OPERATOR(root, "physical_aggregate");
        root = root->getChildren()[0];
        ASSERT_OPERATOR(root, "physicalReduceDistro");
        root = root->getChildren()[0];
        ASSERT_OPERATOR(root, "physicalScan");

        //Verify there's reduce distro inserted on the right
        pp = habilis_generatePPlanFor( "join(opttest_dummy_array, opttest_dummy_replicated_array)");
//        pp->toString(out);
//        std::cout<<out.str();
//        out.str("");
        root = pp->getRoot();
        ASSERT_OPERATOR(root, "physicalJoin");
        PhysNodePtr child = root->getChildren()[0];
        ASSERT_OPERATOR(child, "physicalScan");
        child = root->getChildren()[1]->getChildren()[0];
        ASSERT_OPERATOR(child, "physicalReduceDistro");

        //Verify there's reduce distro inserted on both sides
        pp = habilis_generatePPlanFor( "merge(opttest_dummy_replicated_array, opttest_dummy_replicated_array)");
//        pp->toString(out);
//        std::cout<<out.str();
//        out.str("");
        root = pp->getRoot();
        ASSERT_OPERATOR(root, "physicalMerge");
        child = root->getChildren()[0]->getChildren()[0];
        ASSERT_OPERATOR(child, "physicalReduceDistro");
        child = root->getChildren()[1]->getChildren()[0];
        ASSERT_OPERATOR(child, "physicalReduceDistro");

        //Verify there's reduce distro inserted on the left
        pp = habilis_generatePPlanFor( "join(opttest_dummy_replicated_array, opttest_dummy_array)");
//        pp->toString(out);
//        std::cout<<out.str();
//        out.str("");
        root = pp->getRoot();
        ASSERT_OPERATOR(root, "physicalJoin");
        child = root->getChildren()[0]->getChildren()[0];
        ASSERT_OPERATOR(child, "physicalReduceDistro");
        child = root->getChildren()[1];
        ASSERT_OPERATOR(child, "physicalScan");

        pp = habilis_generatePPlanFor( "merge(opttest_dummy_array, opttest_dummy_replicated_array)");
//        pp->toString(out);
//        std::cout<<out.str();
//        out.str("");
        root = pp->getRoot();
        ASSERT_OPERATOR(root, "physicalMerge");
        child = root->getChildren()[0];
        ASSERT_OPERATOR(child, "physicalScan");
        child = root->getChildren()[1]->getChildren()[0];
        ASSERT_OPERATOR(child, "physicalReduceDistro");
    }

};
CPPUNIT_TEST_SUITE_REGISTRATION(OptimizerTests);

}

#endif /* OPT_UNIT_TESTS_H_ */
