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
 * @file ExceptionUnitTests.h
 *
 * @author paul_geoffrey_brown@yahoo.com
 */

#ifndef EXCEPTION_UNIT_TESTS_H_
#define EXCEPTION_UNIT_TESTS_H_

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
#include "system/Exceptions.h"
#include "query/ParsingContext.h"

class ExceptionTests: public CppUnit::TestFixture
{

CPPUNIT_TEST_SUITE(ExceptionTests);

CPPUNIT_TEST(system_exception);
CPPUNIT_TEST(user_exception);
CPPUNIT_TEST(user_query_exception);
CPPUNIT_TEST(user_check);
CPPUNIT_TEST(system_check);

CPPUNIT_TEST_SUITE_END();

private:
	void throwSystemException(int ErrorNum) {
		throw SYSTEM_EXCEPTION(ErrorNum, boost::str(boost::format("Throwing Error Number %d") % ErrorNum));
	}

	void throwUserException(int ErrorNum) {
		throw USER_EXCEPTION(ErrorNum, boost::str(boost::format("Throwing Error Number %d") % ErrorNum));
	}

	void throwUserQueryException(int ErrorNum) {
        boost::shared_ptr< ParsingContext> foo(new  ParsingContext("Foo Bar", 1, 0));
		throw USER_QUERY_EXCEPTION( ErrorNum, boost::str(boost::format("Throwing Error Number %d") % ErrorNum), foo );
	}

public:

    void setUp()
    {
		std::string s = boost::str(boost::format("SCIDB_ERROR_MESG_COUNT = %d") %SCIDB_ERROR_MESG_COUNT);
		std::cout << "Start Testing Exceptions and Exception handling " << s << "\n";
    }

	void tearDown()
    {
		std::cout << "End Testing Exceptions and Exception handling\n";
    }

	void system_exception()
	{
		//
		//
		//
		char strBuf[32];

		for(int i = 0; i < SCIDB_ERROR_MESG_COUNT + 10; i++) {
			try {
				throwSystemException(i);
            } catch ( SystemException &e ) {

				sprintf(strBuf, "Throwing Error Number %d", i);

				CPPUNIT_ASSERT(i == e.getErrorCode());
				//
				// This is the line on which the Exception is thrown in the
				// function throwSystemException...
				//
				CPPUNIT_ASSERT(64 == e.getLine());

				CPPUNIT_ASSERT(0 == strcmp("src/system/ExceptionUnitTests.h", e.getFile().c_str()));
				CPPUNIT_ASSERT(0 == strcmp("throwSystemException", e.getFunction().c_str()));
				CPPUNIT_ASSERT(0 == strcmp(strBuf, e.getWhatStr().c_str()));

				if (i <= SCIDB_ERROR_MESG_COUNT) {
					CPPUNIT_ASSERT(0 == strcmp(SCIDB_ERROR_MESSAGES[i],SCIDB_ERROR_GET_SHORT_MESSAGE(e.getErrorCode())));
				} else {
					CPPUNIT_ASSERT(0 == strcmp(SCIDB_ERROR_MESSAGES[1],SCIDB_ERROR_GET_SHORT_MESSAGE(e.getErrorCode())));
				}
			}
		}
	}

	void user_exception()
	{
		//
		//
		char strBuf[32];

		for(int i = 0; i < SCIDB_ERROR_MESG_COUNT + 10; i++) {
			try {
				throwUserException(i);
            } catch ( UserException &e ) {

				sprintf(strBuf, "Throwing Error Number %d", i);
				CPPUNIT_ASSERT(i == e.getErrorCode());
				//
				// This is the line on which the UserException is thrown
				// in the throwUserException() function.
				//
				CPPUNIT_ASSERT(68 == e.getLine());
				CPPUNIT_ASSERT(0 == strcmp("src/system/ExceptionUnitTests.h", e.getFile().c_str()));
				CPPUNIT_ASSERT(0 == strcmp("throwUserException", e.getFunction().c_str()));
				CPPUNIT_ASSERT(0 == strcmp(strBuf, e.getWhatStr().c_str()));

				if (i <= SCIDB_ERROR_MESG_COUNT) {
					CPPUNIT_ASSERT(0 == strcmp(SCIDB_ERROR_MESSAGES[i],SCIDB_ERROR_GET_SHORT_MESSAGE(e.getErrorCode())));
				} else {
					CPPUNIT_ASSERT(0 == strcmp(SCIDB_ERROR_MESSAGES[1],SCIDB_ERROR_GET_SHORT_MESSAGE(e.getErrorCode())));
				}
			}
		}
	}

	void user_query_exception()
	{
		//
		//
		//
		char strBuf[32];

		for(int i = 0; i < SCIDB_ERROR_MESG_COUNT + 10; i++) {
			try {
				throwUserQueryException(i);
            } catch ( UserQueryException &e ) {

				sprintf(strBuf, "Throwing Error Number %d", i);
				CPPUNIT_ASSERT(i == e.getErrorCode());
				//
				// This is the line on which the UserQueryException is thrown
				// in the throwUserQueryException() function.
				//
				CPPUNIT_ASSERT(73 == e.getLine());
				CPPUNIT_ASSERT(0 == strcmp("src/system/ExceptionUnitTests.h", e.getFile().c_str()));
				CPPUNIT_ASSERT(0 == strcmp("throwUserQueryException", e.getFunction().c_str()));
				CPPUNIT_ASSERT(0 == strcmp(strBuf, e.getWhatStr().c_str()));

				if (i <= SCIDB_ERROR_MESG_COUNT) {
					CPPUNIT_ASSERT(0 == strcmp(SCIDB_ERROR_MESSAGES[i],SCIDB_ERROR_GET_SHORT_MESSAGE(e.getErrorCode())));
				} else {
					CPPUNIT_ASSERT(0 == strcmp(SCIDB_ERROR_MESSAGES[1],SCIDB_ERROR_GET_SHORT_MESSAGE(e.getErrorCode())));
				}
			}
		}
	}

	void user_check() {

		for(int i = 0; i < SCIDB_ERROR_MESG_COUNT + 10; i++) {

			try {
				USER_CHECK(i, (i == i), "Will not happen");
				USER_CHECK(i, (i == (i+1)), "Will happen");

            } catch ( UserException &e ) {

                CPPUNIT_ASSERT(i == e.getErrorCode());
				//
				// This is the line for the second of the two USER_CHECK()
				// tests above. Note that the first will not cause an
				// Exception to be generated.
				//
                CPPUNIT_ASSERT(192 == e.getLine());
                CPPUNIT_ASSERT(0 == strcmp("src/system/ExceptionUnitTests.h", e.getFile().c_str()));
                CPPUNIT_ASSERT(0 == strcmp("user_check", e.getFunction().c_str()));
                CPPUNIT_ASSERT(0 == strcmp("Will happen: (i == (i+1))", e.getWhatStr().c_str()));

                if (i <= SCIDB_ERROR_MESG_COUNT) {
                    CPPUNIT_ASSERT(0 == strcmp(SCIDB_ERROR_MESSAGES[i],SCIDB_ERROR_GET_SHORT_MESSAGE(e.getErrorCode())));
                } else {
                    CPPUNIT_ASSERT(0 == strcmp(SCIDB_ERROR_MESSAGES[1],SCIDB_ERROR_GET_SHORT_MESSAGE(e.getErrorCode())));
                }
			}
		}
	}

	void system_check() {

		for(int i = 0; i < SCIDB_ERROR_MESG_COUNT + 10; i++) {

			try {
				SYSTEM_CHECK(i, (i == i), "Will not happen");
				SYSTEM_CHECK(i, (i == (i+1)), "Will happen");

            } catch ( SystemException &e ) {

                CPPUNIT_ASSERT(i == e.getErrorCode());
				//
				// This is the line for the second of the two USER_CHECK()
				// tests above. Note that the first will not cause an
				// Exception to be generated.
				//
                CPPUNIT_ASSERT(222 == e.getLine());
                CPPUNIT_ASSERT(0 == strcmp("src/system/ExceptionUnitTests.h", e.getFile().c_str()));
                CPPUNIT_ASSERT(0 == strcmp("system_check", e.getFunction().c_str()));
                CPPUNIT_ASSERT(0 == strcmp("Will happen: (i == (i+1))", e.getWhatStr().c_str()));

                if (i <= SCIDB_ERROR_MESG_COUNT) {
                    CPPUNIT_ASSERT(0 == strcmp(SCIDB_ERROR_MESSAGES[i],SCIDB_ERROR_GET_SHORT_MESSAGE(e.getErrorCode())));
                } else {
                    CPPUNIT_ASSERT(0 == strcmp(SCIDB_ERROR_MESSAGES[1],SCIDB_ERROR_GET_SHORT_MESSAGE(e.getErrorCode())));
                }
			}
		}
	}
};

CPPUNIT_TEST_SUITE_REGISTRATION(ExceptionTests);

#endif /* EXCEPTION_UNIT_TESTS_H_ */
