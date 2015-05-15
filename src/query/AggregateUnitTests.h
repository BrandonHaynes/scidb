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
 * @file AggregateUnitTests.h
 *
 * @author poliocough@gmail.com
 */

#ifndef AGGREGATE_UNIT_TESTS_H_
#define AGGREGATE_UNIT_TESTS_H_

#include <cppunit/TestFixture.h>
#include <cppunit/extensions/HelperMacros.h>

#include <cmath>
#include <limits>

#include <boost/shared_ptr.hpp>

#include "query/Aggregate.h"

using namespace scidb;

class AggregateTests: public CppUnit::TestFixture
{
    CPPUNIT_TEST_SUITE(AggregateTests);
    CPPUNIT_TEST(testIntegerSum);
    CPPUNIT_TEST(testFloatSum);
    CPPUNIT_TEST(testIntegerAvg);
    CPPUNIT_TEST(testDoubleAvg);
    CPPUNIT_TEST_SUITE_END();

    void testIntegerSum()
    {
        AggregateLibrary* al = AggregateLibrary::getInstance();
        Type tInt32 = TypeLibrary::getType(TID_INT32);

        AggregatePtr sum = al->createAggregate("sum", tInt32);
        CPPUNIT_ASSERT(sum.get() != 0);

        CPPUNIT_ASSERT(sum->getAggregateType() == TypeLibrary::getType(TID_INT32));
        CPPUNIT_ASSERT(sum->getStateType() == TypeLibrary::getType(TID_BINARY));
        CPPUNIT_ASSERT(sum->getResultType() == TypeLibrary::getType(TID_INT64));

        Value input(sum->getAggregateType());
        Value state(sum->getStateType());
        Value final(sum->getResultType());

        sum->initializeState(state);
        sum->finalResult(final, state);
        CPPUNIT_ASSERT(isDefaultFor(final,sum->getResultType().typeId()));

        sum->initializeState(state);
        input = TypeLibrary::getDefaultValue(sum->getAggregateType().typeId());
        sum->accumulateIfNeeded(state, input);
        sum->accumulateIfNeeded(state, input);
        CPPUNIT_ASSERT(!isDefaultFor(state,sum->getStateType().typeId()));
        input.setNull();
        sum->accumulateIfNeeded(state, input);
        CPPUNIT_ASSERT(!isDefaultFor(state,sum->getStateType().typeId()));

        Value state2(sum->getStateType());
        sum->initializeState(state2);
        CPPUNIT_ASSERT(!isDefaultFor(state2,sum->getStateType().typeId()));
        sum->mergeIfNeeded(state, state2);

        sum->finalResult(final, state);
        CPPUNIT_ASSERT(isDefaultFor(final,sum->getResultType().typeId()));

        sum->initializeState(state);
        input = TypeLibrary::getDefaultValue(sum->getAggregateType().typeId());
        sum->accumulateIfNeeded(state, input);
        input.setInt32(5);
        sum->accumulateIfNeeded(state, input);
        input.setInt32(3);
        sum->accumulateIfNeeded(state, input);

        state2 = Value(sum->getStateType());
        sum->mergeIfNeeded(state2, state);
        sum->mergeIfNeeded(state, state2);

        sum->finalResult(final, state);

        CPPUNIT_ASSERT(final.getInt64() == 16);
    }

    void testFloatSum()
    {
        AggregateLibrary* al = AggregateLibrary::getInstance();
        Type tFloat = TypeLibrary::getType(TID_FLOAT);

        AggregatePtr sum = al->createAggregate("sum", tFloat);
        CPPUNIT_ASSERT(sum.get() != 0);

        CPPUNIT_ASSERT(sum->getAggregateType() == TypeLibrary::getType(TID_FLOAT));
        CPPUNIT_ASSERT(sum->getStateType() == TypeLibrary::getType(TID_BINARY));
        CPPUNIT_ASSERT(sum->getResultType() == TypeLibrary::getType(TID_DOUBLE));

        Value input(sum->getAggregateType());
        Value state(sum->getStateType());
        Value final(sum->getResultType());

        sum->initializeState(state);
        sum->finalResult(final, state);
        CPPUNIT_ASSERT(isDefaultFor(final,sum->getResultType().typeId()));

        sum->initializeState(state);
        input = TypeLibrary::getDefaultValue(sum->getAggregateType().typeId());
        sum->accumulateIfNeeded(state, input);
        sum->accumulateIfNeeded(state, input);
        sum->finalResult(final, state);
        CPPUNIT_ASSERT(isDefaultFor(final,sum->getResultType().typeId()));

        sum->initializeState(state);
        input = TypeLibrary::getDefaultValue(sum->getAggregateType().typeId());
        sum->accumulateIfNeeded(state, input);
        input.setFloat(5.1);
        sum->accumulateIfNeeded(state, input);
        input.setFloat(3.1);
        sum->accumulateIfNeeded(state, input);

        Value state2(sum->getStateType());
        sum->mergeIfNeeded(state2, state);
        sum->mergeIfNeeded(state, state2);

        sum->finalResult(final, state);

        //We incur a float epsilon every time we convert from float to double (two times multiplied by two)
        CPPUNIT_ASSERT( std::fabs(final.getDouble() - 16.4 ) < 4*std::numeric_limits<float>::epsilon() );
    }

    void testIntegerAvg()
    {
        AggregateLibrary* al = AggregateLibrary::getInstance();
        Type tInt32 = TypeLibrary::getType(TID_INT32);

        AggregatePtr avg = al->createAggregate("avg", tInt32);
        CPPUNIT_ASSERT(avg.get() != 0);

        CPPUNIT_ASSERT(avg->getAggregateType() == TypeLibrary::getType(TID_INT32));
//      CPPUNIT_ASSERT(sum->getStateType() == TypeLibrary::getType(TID_INT64));
        CPPUNIT_ASSERT(avg->getResultType() == TypeLibrary::getType(TID_DOUBLE));

        Value input(avg->getAggregateType());
        Value state(avg->getStateType());
        Value final(avg->getResultType());

        avg->initializeState(state);
        input.setInt32(5);
        avg->accumulateIfNeeded(state, input);

        input.setInt32(3);
        avg->accumulateIfNeeded(state, input);

        input.setInt32(0);
        avg->accumulateIfNeeded(state, input);

        avg->finalResult(final, state);
        CPPUNIT_ASSERT( std::fabs(final.getDouble() - (8.0 / 3.0)) < 4*std::numeric_limits<float>::epsilon() );
    }

    void testDoubleAvg()
    {
        AggregateLibrary* al = AggregateLibrary::getInstance();
        Type tDouble = TypeLibrary::getType(TID_DOUBLE);

        AggregatePtr avg = al->createAggregate("avg", tDouble);
        CPPUNIT_ASSERT(avg.get() != 0);

        CPPUNIT_ASSERT(avg->getAggregateType() == TypeLibrary::getType(TID_DOUBLE));
        CPPUNIT_ASSERT(avg->getResultType() == TypeLibrary::getType(TID_DOUBLE));

        Value input(avg->getAggregateType());
        Value state(avg->getStateType());
        Value final(avg->getResultType());

        avg->initializeState(state);
        input.setDouble(5.0);
        avg->accumulateIfNeeded(state, input);

        input.setDouble(3.0);
        avg->accumulateIfNeeded(state, input);

        input.setDouble(0);
        avg->accumulateIfNeeded(state, input);

        avg->finalResult(final, state);
        CPPUNIT_ASSERT( std::fabs(final.getDouble() - (8.0 / 3.0)) < 4*std::numeric_limits<float>::epsilon() );
    }
};

CPPUNIT_TEST_SUITE_REGISTRATION(AggregateTests);

#endif /* AGGREGATE_UNIT_TESTS_H_ */
