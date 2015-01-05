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
 * @file FunctionLibraryUnitTests.h
 *
 * @author paul_geoffrey_brown@yahoo.com
 */

#ifndef FUNCTIONLIBRARY_UNIT_TESTS_H_
#define FUNCTIONLIBRARY_UNIT_TESTS_H_

#include <stdio.h>

#include <cppunit/TestFixture.h>
#include <cppunit/extensions/HelperMacros.h>
#include <algorithm>
#include <sstream>
#include <string>

#include <boost/archive/text_oarchive.hpp>
#include <boost/archive/text_iarchive.hpp>
#include <boost/algorithm/string.hpp>
#include <boost/format.hpp>

#include "system/Config.h"
#include "util/PluginManager.h"
#include "query/FunctionDescription.h"
#include "system/ErrorCodes.h"
#include "query/TypeSystem.h"

#include "query/FunctionLibrary.h"
#include "query/Expression.h"


class FunctionLibraryTests: public CppUnit::TestFixture
{

CPPUNIT_TEST_SUITE(FunctionLibraryTests);

CPPUNIT_TEST_SUITE_END();

private:

public:
	void loadFuncs()
	{
        // TODO: Implement new tests
	}

    void setUp()
    {
		std::cout << "Start Testing FunctionLibrary and FunctionDescription Class handling\n";
    }

	void tearDown()
    {
		std::cout << "End Testing FunctionLibrary and FunctionDescription Class handling\n";
    }

};

CPPUNIT_TEST_SUITE_REGISTRATION(FunctionLibraryTests);

#endif /* FUNCTIONLIBRARY_UNIT_TESTS_H_ */
