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
 * @file TypeLibraryUnitTests.h
 *
 * @author paul_geoffrey_brown@yahoo.com
 */

#ifndef TYPELIBRARY_UNIT_TESTS_H_
#define TYPELIBRARY_UNIT_TESTS_H_

#include <cppunit/TestFixture.h>
#include <cppunit/extensions/HelperMacros.h>
#include <algorithm>
#include <sstream>
#include <string>

#include <boost/archive/text_oarchive.hpp>
#include <boost/archive/text_iarchive.hpp>
#include <boost/shared_ptr.hpp>
#include <boost/algorithm/string.hpp>
#include <boost/format.hpp>

#include "system/ErrorCodes.h"
#include "query/TypeSystem.h"
#include "query/ParsingContext.h"

class TypeLibraryTests: public CppUnit::TestFixture
{

CPPUNIT_TEST_SUITE(TypeLibraryTests);

CPPUNIT_TEST(checkBuiltInTypes);
CPPUNIT_TEST(checkRegisterType);

CPPUNIT_TEST_SUITE_END();

private:

public:

    void setUp()
    {
		std::cout << "Start Testing TypeLibrary and Type Class handling\n";
    }

	void tearDown()
    {
		std::cout << "End Testing TypeLibrary and Type Class handling\n";
    }

	void checkBuiltInTypes()
	{
        std::vector< TypeId> typeNames =  TypeLibrary::typeIds();
		std::cout << "List of Types\n";

		for(size_t i = 0, l = typeNames.size();i < l; i++ )
		{
             Type t1 =  TypeLibrary::getType(typeNames[i]);
			std::cout << "\t" << t1 << "\n";
             Type t2 =  TypeLibrary::getType(t1.name());
			CPPUNIT_ASSERT ( t1 == t2 );
		}
	}

	void checkRegisterType()
	{
		char names[8][32] = {"_not_exists_foo_","_not_exists_bar_", "_not_exists_mug_", "_not_exists_wump_","_not_exists_foobar_","_not_exists_foomug_","_not_exists_barmug_"};

		for(size_t i = 0; i < 8; i++) {
             Type t1(names[i],8);
             TypeLibrary::registerType( t1 );
             Type t2 =  TypeLibrary::getType(names[i]);
			CPPUNIT_ASSERT (t1 == t2);

             Type t4 =  TypeLibrary::getType(names[0]);
			for ( size_t j = 1; j < i; j++) {
                 Type t5 =  TypeLibrary::getType(names[j]);
				CPPUNIT_ASSERT (!(t4 == t5));
			}
		}
	}
};

CPPUNIT_TEST_SUITE_REGISTRATION(TypeLibraryTests);

#endif /* TYPELIBRARY_UNIT_TESTS_H_ */
