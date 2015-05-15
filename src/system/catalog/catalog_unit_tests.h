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

#ifndef CATALOG_UNIT_TESTS_H_
#define CATALOG_UNIT_TESTS_H_

#include <cppunit/TestFixture.h>
#include <cppunit/extensions/HelperMacros.h>
#include <algorithm>

#include <system/SystemCatalog.h>
#include <array/Metadata.h>

//FIXME: Some tests will be work only when tests running consequentially.
//Need cleaninig of catalog for each test to get independent tests.

class CatalogTests: public CppUnit::TestFixture
{
CPPUNIT_TEST_SUITE(CatalogTests);
CPPUNIT_TEST(catalogInit);
CPPUNIT_TEST(addArray);
CPPUNIT_TEST(getArrayByName);
CPPUNIT_TEST(getArrayByID);
CPPUNIT_TEST(getArrayAttributes);
CPPUNIT_TEST(getArrayDimensions);
CPPUNIT_TEST(addGetUpdateInstance);
CPPUNIT_TEST(getArrays);
CPPUNIT_TEST(deleteArrayByName);
CPPUNIT_TEST(deleteArrayByID);
CPPUNIT_TEST(addLogicalOp);
CPPUNIT_TEST(addPhysicalOp);
CPPUNIT_TEST(getLogicalOp);
CPPUNIT_TEST(getPhysicalOp);
CPPUNIT_TEST(getPhysicalOps);
CPPUNIT_TEST_SUITE_END();

public:
	void setUp()
	{
	}

	void tearDown()
	{
	}

	void catalogInit()
	{
		std::string catalog_uuid = SystemCatalog::getInstance()->initializeCluster();
		CPPUNIT_ASSERT(catalog_uuid != "");
	}

	void addArray()
	{
		 Attributes att;
		att.push_back(AttributeDesc("a", 0, AttributeDesc::IS_NULLABLE, 0));
		att.push_back(AttributeDesc("b", 1, AttributeDesc::IS_EMPTY_INDICATOR, 1));
		 Dimensions dim;
		dim.push_back(DimensionDesc("x", 0, 10, 5, 1));
		dim.push_back(DimensionDesc("y", -10, 20, 5, 1));

		 ArrayDesc array = ArrayDesc("test_array", 0, 0, att, dim);
		 ArrayID id = SystemCatalog::getInstance()->addArray(array);

		CPPUNIT_ASSERT(id > 0);

		 SystemCatalog::getInstance()->deleteArray(id);
	}

	void getArrayByName()
	{
		 Attributes att;
		att.push_back(AttributeDesc("a", 0, AttributeDesc::IS_NULLABLE, 0));
		att.push_back(AttributeDesc("b", 1, AttributeDesc::IS_EMPTY_INDICATOR, 1));
		 Dimensions dim;
		dim.push_back(DimensionDesc("x", 0, 10, 5, 1));
		dim.push_back(DimensionDesc("y", -10, 20, 5, 1));

		 ArrayDesc array_orig = ArrayDesc("test_array_1", 20, 30, att, dim);
		 ArrayID id_orig = SystemCatalog::getInstance()->addArray(array_orig);

		 ArrayDesc array_get;
		 SystemCatalog::getInstance()->getArrayDesc("test_array_1", array_get);

		CPPUNIT_ASSERT(id_orig == array_get.getId());
		CPPUNIT_ASSERT(array_get.getName() == "test_array_1");
		CPPUNIT_ASSERT(array_get.getCellNum() == 20);
		CPPUNIT_ASSERT(array_get.getSize() == 30);

		 SystemCatalog::getInstance()->deleteArray(id_orig);
	}

	void getArrayByID()
	{
		 Attributes att;
		att.push_back(AttributeDesc("a", 0, AttributeDesc::IS_NULLABLE, 0));
		att.push_back(AttributeDesc("b", 1, AttributeDesc::IS_EMPTY_INDICATOR, 1));
		 Dimensions dim;
		dim.push_back(DimensionDesc("x", 0, 10, 5, 1));
		dim.push_back(DimensionDesc("y", -10, 20, 6, 2));

		 ArrayDesc array_orig = ArrayDesc("test_array_2", 10, 100, att, dim);
		 ArrayID id_orig = SystemCatalog::getInstance()->addArray(array_orig);

		 ArrayDesc array_get;
		 SystemCatalog::getInstance()->getArrayDesc(id_orig, array_get);

		CPPUNIT_ASSERT(id_orig == array_get.getId());
		CPPUNIT_ASSERT(array_get.getName() == "test_array_2");
		CPPUNIT_ASSERT(array_get.getCellNum() == 10);
		CPPUNIT_ASSERT(array_get.getSize() == 100);

		 SystemCatalog::getInstance()->deleteArray(id_orig);
	}

	void getArrayAttributes()
	{
		 Attributes att;
		att.push_back(AttributeDesc("a", 0, AttributeDesc::IS_NULLABLE, 3));
		att.push_back(AttributeDesc("b", 1, AttributeDesc::IS_EMPTY_INDICATOR, 4));
		att.push_back(AttributeDesc("c", 2, AttributeDesc::IS_NULLABLE | AttributeDesc::IS_EMPTY_INDICATOR, 5));
		 Dimensions dim;
		dim.push_back(DimensionDesc("x", 0, 10, 5, 1));
		dim.push_back(DimensionDesc("y", -10, 20, 6, 2));

		 ArrayDesc array_orig = ArrayDesc("test_array_3", 10, 100, att, dim);
		 ArrayID id_orig = SystemCatalog::getInstance()->addArray(array_orig);

		 ArrayDesc array_get;
		 SystemCatalog::getInstance()->getArrayDesc(id_orig, array_get);

		CPPUNIT_ASSERT(array_get.getAttributes().size() == 3);

		CPPUNIT_ASSERT(array_get.getAttributes()[0].getId() == 0);
		CPPUNIT_ASSERT(array_get.getAttributes()[0].getName() == "a");
		CPPUNIT_ASSERT(array_get.getAttributes()[0].getType() == 0);
		CPPUNIT_ASSERT(array_get.getAttributes()[0].isNullable());
		CPPUNIT_ASSERT(array_get.getAttributes()[0].getDefaultCompressionMethod() == 3);

		CPPUNIT_ASSERT(array_get.getAttributes()[1].getId() == 1);
		CPPUNIT_ASSERT(array_get.getAttributes()[1].getName() == "b");
		CPPUNIT_ASSERT(array_get.getAttributes()[1].getType() == 1);
		CPPUNIT_ASSERT(array_get.getAttributes()[1].isEmptyIndicator());
		CPPUNIT_ASSERT(array_get.getAttributes()[1].getDefaultCompressionMethod() == 4);

		CPPUNIT_ASSERT(array_get.getAttributes()[2].getId() == 2);
		CPPUNIT_ASSERT(array_get.getAttributes()[2].getName() == "c");
		CPPUNIT_ASSERT(array_get.getAttributes()[2].getType() == 2);
		CPPUNIT_ASSERT(array_get.getAttributes()[2].isEmptyIndicator() &&
				array_get.getAttributes()[2].isNullable());
		CPPUNIT_ASSERT(array_get.getAttributes()[2].getDefaultCompressionMethod() == 5);
		
		 SystemCatalog::getInstance()->deleteArray(id_orig);
	}

	void getArrayDimensions()
	{
		 Attributes att;
		att.push_back(AttributeDesc("a", 0, AttributeDesc::IS_NULLABLE, 0));
		att.push_back(AttributeDesc("b", 1, AttributeDesc::IS_EMPTY_INDICATOR, 1));
		 Dimensions dim;
		dim.push_back(DimensionDesc("x", 0, 10, 5, 1));
		dim.push_back(DimensionDesc("y", -10, 20, 6, 2));

		 ArrayDesc array_orig = ArrayDesc("test_array_4", 10, 100, att, dim);
		 ArrayID id_orig = SystemCatalog::getInstance()->addArray(array_orig);

		 ArrayDesc array_get;
		 SystemCatalog::getInstance()->getArrayDesc(id_orig, array_get);

		CPPUNIT_ASSERT(array_get.getDimensions()[0].getName() == "x");
		CPPUNIT_ASSERT(array_get.getDimensions()[0].getStartMin() == 0);
		CPPUNIT_ASSERT(array_get.getDimensions()[0].getLength() == 10);
		CPPUNIT_ASSERT(array_get.getDimensions()[0].getChunkInterval() == 5);
		CPPUNIT_ASSERT(array_get.getDimensions()[0].getChunkOverlap() == 1);

		CPPUNIT_ASSERT(array_get.getDimensions()[1].getName() == "y");
		CPPUNIT_ASSERT(array_get.getDimensions()[1].getStartMin() == -10);
		CPPUNIT_ASSERT(array_get.getDimensions()[1].getLength() == 20);
		CPPUNIT_ASSERT(array_get.getDimensions()[1].getChunkInterval() == 6);
		CPPUNIT_ASSERT(array_get.getDimensions()[1].getChunkOverlap() == 2);
		
		 SystemCatalog::getInstance()->deleteArray(id_orig);
	}

	void addGetUpdateInstance()
	{
		// add
		 InstanceDesc instance1_orig = InstanceDesc("host1", 8001, true);
		 InstanceDesc instance2_orig = InstanceDesc("host2", 8002, false);
		 InstanceID instance1_orig_id = SystemCatalog::getInstance()->addInstance(instance1_orig);
		 InstanceID instance2_orig_id = SystemCatalog::getInstance()->addInstance(instance2_orig);

		CPPUNIT_ASSERT(instance1_orig_id == 0);
		CPPUNIT_ASSERT(instance2_orig_id == instance1_orig_id + 1);

		// get 1
		 InstanceDesc instance1_get, instance2_get;
		 SystemCatalog::getInstance()->getInstance(instance1_orig_id, instance1_get);
		 SystemCatalog::getInstance()->getInstance(instance2_orig_id, instance2_get);

		CPPUNIT_ASSERT(instance1_orig_id == instance1_get.getInstanceId());
		CPPUNIT_ASSERT(instance1_get.getHost() == "host1");
		CPPUNIT_ASSERT(instance1_get.getPort() == 8001);
		CPPUNIT_ASSERT(instance1_get.isOnline() == true);

		CPPUNIT_ASSERT(instance2_orig_id == instance2_get.getInstanceId());
		CPPUNIT_ASSERT(instance2_get.getHost() == "host2");
		CPPUNIT_ASSERT(instance2_get.getPort() == 8002);
		CPPUNIT_ASSERT(instance2_get.isOnline() == false);

		// get 2
		 Instances instances;
		 SystemCatalog::getInstance()->getInstances(instances);

		CPPUNIT_ASSERT(instance1_orig_id == instances[0].getInstanceId());
		CPPUNIT_ASSERT(instances[0].getHost() == "host1");
		CPPUNIT_ASSERT(instances[0].getPort() == 8001);
		CPPUNIT_ASSERT(instances[0].isOnline() == true);

		CPPUNIT_ASSERT(instance2_orig_id == instances[1].getInstanceId());
		CPPUNIT_ASSERT(instances[1].getHost() == "host2");
		CPPUNIT_ASSERT(instances[1].getPort() == 8002);
		CPPUNIT_ASSERT(instances[1].isOnline() == false);

		// update 1
		 InstanceDesc instance1_update = InstanceDesc(instance1_orig_id, "host3", 8003, false);
		 SystemCatalog::getInstance()->updateInstance(instance1_update);

		 SystemCatalog::getInstance()->getInstance(instance1_orig_id, instance1_get);
		CPPUNIT_ASSERT(instance1_orig_id == instance1_get.getInstanceId());
		CPPUNIT_ASSERT(instance1_get.getHost() == "host3");
		CPPUNIT_ASSERT(instance1_get.getPort() == 8003);
		CPPUNIT_ASSERT(instance1_get.isOnline() == false);

		// update 2
		 SystemCatalog::getInstance()->markInstanceOnline(instance1_orig_id, "host4", 8004);

		 SystemCatalog::getInstance()->getInstance(instance1_orig_id, instance1_get);
		CPPUNIT_ASSERT(instance1_orig_id == instance1_get.getInstanceId());
		CPPUNIT_ASSERT(instance1_get.getHost() == "host4");
		CPPUNIT_ASSERT(instance1_get.getPort() == 8004);
		CPPUNIT_ASSERT(instance1_get.isOnline() == true);

		// update 3
		 SystemCatalog::getInstance()->markInstanceOffline(instance1_orig_id);

		 SystemCatalog::getInstance()->getInstance(instance1_orig_id, instance1_get);
		CPPUNIT_ASSERT(instance1_orig_id == instance1_get.getInstanceId());
		CPPUNIT_ASSERT(instance1_get.getHost() == "host4");
		CPPUNIT_ASSERT(instance1_get.getPort() == 8004);
		CPPUNIT_ASSERT(instance1_get.isOnline() == false);
	}

	void getArrays()
	{
		 Attributes att;
		 Dimensions dim;

		 ArrayDesc array = ArrayDesc("first", 10, 100, att, dim);
		 ArrayID id1 = SystemCatalog::getInstance()->addArray(array);

		array = ArrayDesc("second", 10, 100, att, dim);
		 ArrayID id2 = SystemCatalog::getInstance()->addArray(array);
		
		array = ArrayDesc("third", 10, 100, att, dim);
		 ArrayID id3 = SystemCatalog::getInstance()->addArray(array);
		
		std::vector<std::string> arrays;
		 SystemCatalog::getInstance()->getArrays(arrays);
		
		CPPUNIT_ASSERT(arrays.size() == 3);
		CPPUNIT_ASSERT(std::find(arrays.begin(), arrays.end(), "first") != arrays.end());
		CPPUNIT_ASSERT(std::find(arrays.begin(), arrays.end(), "second") != arrays.end());
		CPPUNIT_ASSERT(std::find(arrays.begin(), arrays.end(), "third") != arrays.end());
		
		 SystemCatalog::getInstance()->deleteArray(id1);
		 SystemCatalog::getInstance()->deleteArray(id2);
		 SystemCatalog::getInstance()->deleteArray(id3);
	}
	
	void deleteArrayByName()
	{
		 Attributes att;
		 Dimensions dim;

		 ArrayDesc array = ArrayDesc("array", 10, 100, att, dim);
		 SystemCatalog::getInstance()->addArray(array);
		
		std::vector<std::string> arrays;
		 SystemCatalog::getInstance()->getArrays(arrays);
		
		CPPUNIT_ASSERT(arrays.size() == 1);
		CPPUNIT_ASSERT(std::find(arrays.begin(), arrays.end(), "array") != arrays.end());
		
		 SystemCatalog::getInstance()->deleteArray("array");
		
		 SystemCatalog::getInstance()->getArrays(arrays);
		CPPUNIT_ASSERT(arrays.size() == 0);
	}
	
	void deleteArrayByID()
	{
		 Attributes att;
		 Dimensions dim;

		 ArrayDesc array = ArrayDesc("array", 10, 100, att, dim);
		 ArrayID id = SystemCatalog::getInstance()->addArray(array);
		
		std::vector<std::string> arrays;
		 SystemCatalog::getInstance()->getArrays(arrays);
		
		CPPUNIT_ASSERT(arrays.size() == 1);
		CPPUNIT_ASSERT(std::find(arrays.begin(), arrays.end(), "array") != arrays.end());
		
		 SystemCatalog::getInstance()->deleteArray(id);
		
		 SystemCatalog::getInstance()->getArrays(arrays);
		CPPUNIT_ASSERT(arrays.size() == 0);
	}
	
	void addLogicalOp()
	{
		 SystemCatalog *cat = SystemCatalog::getInstance();

		 LogicalOpDesc opDesc1("opName1", "foo1", "bar1");
		 LogicalOpDesc opDesc2("opName2", "foo2", "bar2");

		 OpID opId1 = cat->addLogicalOp(opDesc1);
		 OpID opId2 = cat->addLogicalOp(opDesc2);

		CPPUNIT_ASSERT(opId1 == 1);
		CPPUNIT_ASSERT(opId2 == 2);
		
		cat->deleteLogicalOp("opName1");
		cat->deleteLogicalOp("opName2");
	}
	
	void addPhysicalOp()
	{
		 SystemCatalog *cat = SystemCatalog::getInstance();

		 LogicalOpDesc opDesc1("opName1", "foo1", "bar1");
		 LogicalOpDesc opDesc2("opName2", "foo2", "bar2");
		 PhysicalOpDesc opDesc3("opName1", "opName3", "foo1", "bar1");
		 PhysicalOpDesc opDesc4("opName2", "opName4", "foo1", "bar1");
		
		 OpID opId1 = cat->addLogicalOp(opDesc1);
		 OpID opId2 = cat->addLogicalOp(opDesc2);
		 OpID opId3 = cat->addPhysicalOp(opDesc3);
		 OpID opId4 = cat->addPhysicalOp(opDesc4);
		
		CPPUNIT_ASSERT(opId1 == 3);
		CPPUNIT_ASSERT(opId2 == 4);
		CPPUNIT_ASSERT(opId3 == 1);
		CPPUNIT_ASSERT(opId4 == 2);
		
		cat->deleteLogicalOp("opName1");
		cat->deleteLogicalOp("opName2");
	}

	void getLogicalOp()
	{
		 SystemCatalog *cat = SystemCatalog::getInstance();

		 LogicalOpDesc opDesc1_orig("opName1", "foo1", "bar1");
		 LogicalOpDesc opDesc2_orig("opName2", "foo2", "bar2");

		cat->addLogicalOp(opDesc1_orig);
		cat->addLogicalOp(opDesc2_orig);

		 LogicalOpDesc opDesc1 = cat->getLogicalOp("opName1");
		 LogicalOpDesc opDesc2 = cat->getLogicalOp("opName2");
		
		CPPUNIT_ASSERT(opDesc1.getLogicalOpId() == 5);
		CPPUNIT_ASSERT(opDesc1.getName() == "opName1");
		CPPUNIT_ASSERT(opDesc1.getModule() == "foo1");
		CPPUNIT_ASSERT(opDesc1.getEntry() == "bar1");

		CPPUNIT_ASSERT(opDesc2.getLogicalOpId() == 6);
		CPPUNIT_ASSERT(opDesc2.getName() == "opName2");
		CPPUNIT_ASSERT(opDesc2.getModule() == "foo2");
		CPPUNIT_ASSERT(opDesc2.getEntry() == "bar2");

		cat->deleteLogicalOp("opName1");
		cat->deleteLogicalOp("opName2");
	}
	
	void getPhysicalOp()
	{
		 SystemCatalog *cat = SystemCatalog::getInstance();

		 LogicalOpDesc logicalOp_orig("opName1", "foo1", "bar1");
		 PhysicalOpDesc physicalOp1_orig("opName1", "opName2", "foo2", "bar2");
		 PhysicalOpDesc physicalOp2_orig("opName1", "opName3", "foo3", "bar3");
		
		cat->addLogicalOp(logicalOp_orig);
		cat->addPhysicalOp(physicalOp1_orig);
		cat->addPhysicalOp(physicalOp2_orig);
		
		 PhysicalOpDesc physicalOp1 = cat->getPhysicalOp("opName1", "opName2");
		 PhysicalOpDesc physicalOp2 = cat->getPhysicalOp("opName1", "opName3");
		
		CPPUNIT_ASSERT(physicalOp1.getId() == 3);
		CPPUNIT_ASSERT(physicalOp1.getLogicalName() == "opName1");
		CPPUNIT_ASSERT(physicalOp1.getName() == "opName2");
		CPPUNIT_ASSERT(physicalOp1.getModule() == "foo2");
		CPPUNIT_ASSERT(physicalOp1.getEntry() == "bar2");

		CPPUNIT_ASSERT(physicalOp2.getId() == 4);
		CPPUNIT_ASSERT(physicalOp2.getLogicalName() == "opName1");
		CPPUNIT_ASSERT(physicalOp2.getName() == "opName3");
		CPPUNIT_ASSERT(physicalOp2.getModule() == "foo3");
		CPPUNIT_ASSERT(physicalOp2.getEntry() == "bar3");
		
		cat->deleteLogicalOp("opName1");
	}

	void getPhysicalOps()
	{
		 SystemCatalog *cat = SystemCatalog::getInstance();

		 LogicalOpDesc logicalOp_orig("opName1", "foo1", "bar1");
		 PhysicalOpDesc physicalOp1_orig("opName1", "opName2", "foo2", "bar2");
		 PhysicalOpDesc physicalOp2_orig("opName1", "opName3", "foo3", "bar3");
		
		cat->addLogicalOp(logicalOp_orig);
		cat->addPhysicalOp(physicalOp1_orig);
		cat->addPhysicalOp(physicalOp2_orig);

		 PhysicalOps ops = cat->getPhysicalOps("opName1");
		
		CPPUNIT_ASSERT(ops.size() == 2);

		CPPUNIT_ASSERT(ops[0].getId() == 5);
		CPPUNIT_ASSERT(ops[0].getLogicalName() == "opName1");
		CPPUNIT_ASSERT(ops[0].getName() == "opName2");
		CPPUNIT_ASSERT(ops[0].getModule() == "foo2");
		CPPUNIT_ASSERT(ops[0].getEntry() == "bar2");
		
		CPPUNIT_ASSERT(ops[1].getId() == 6);
		CPPUNIT_ASSERT(ops[1].getLogicalName() == "opName1");
		CPPUNIT_ASSERT(ops[1].getName() == "opName3");
		CPPUNIT_ASSERT(ops[1].getModule() == "foo3");
		CPPUNIT_ASSERT(ops[1].getEntry() == "bar3");
		
		cat->deleteLogicalOp("opName1");		
	}	
};

CPPUNIT_TEST_SUITE_REGISTRATION(CatalogTests);

#endif /* CATALOG_UNIT_TESTS_H_ */
