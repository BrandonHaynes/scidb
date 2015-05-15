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
 * @file ExpressionUnitTests.h
 *
 * @author roman.simakov@gmail.com
 */

#ifndef EXPRESSION_UNIT_TESTS_H_
#define EXPRESSION_UNIT_TESTS_H_

#include <cppunit/TestFixture.h>
#include <cppunit/extensions/HelperMacros.h>
#include <algorithm>
#include <boost/archive/text_oarchive.hpp>
#include <boost/archive/text_iarchive.hpp>
#include <boost/shared_ptr.hpp>
#include <boost/assign.hpp>
#include <boost/make_shared.hpp>

#include "query/LogicalExpression.h"
#include "query/Expression.h"
#include "query/TypeSystem.h"
#include "query/Parser.h"

using namespace std;
using namespace scidb;

class ExpressionTests: public CppUnit::TestFixture
{
CPPUNIT_TEST_SUITE(ExpressionTests);
CPPUNIT_TEST(evlVectorIsNull);
CPPUNIT_TEST(evlVectorAPlusB);
CPPUNIT_TEST(evlVectorDenseMinusInt32);
CPPUNIT_TEST(evlVectorRLEMinusInt32);
CPPUNIT_TEST(evlVectorDenseBinaryPlusInt32);
CPPUNIT_TEST(evlVectorDenseBinaryAndBool);
CPPUNIT_TEST(evlVectorRLEBinaryPlusInt32);
CPPUNIT_TEST(evlVectorMixBinaryPlusInt32);
CPPUNIT_TEST(evlInt32PlusInt32);
CPPUNIT_TEST(evlInt64PlusInt64);
CPPUNIT_TEST(evlInt32PlusInt64);
CPPUNIT_TEST(evlInt32PlusInt32PlusInt64);
CPPUNIT_TEST(evlInt32PlusNull);
CPPUNIT_TEST(evlUnaryMinusInt32);
CPPUNIT_TEST(evlSinDouble);
CPPUNIT_TEST(evlPowDouble);
CPPUNIT_TEST(evlExplicitConvDouble);
CPPUNIT_TEST(evlExplicitConvInt32);
CPPUNIT_TEST(evliif0);
CPPUNIT_TEST(evliif1);
CPPUNIT_TEST(evlor_lazy);
CPPUNIT_TEST(evland_lazy);
CPPUNIT_TEST(evland_lazy2);
//CPPUNIT_TEST(evlPerfExp);
//CPPUNIT_TEST(evlPerfCPP);
CPPUNIT_TEST(evlBinaryCompileIntPlusDouble);
CPPUNIT_TEST(evlSerialization);
CPPUNIT_TEST(evlInstanceId);
CPPUNIT_TEST(evlInt8PlusInt16);
CPPUNIT_TEST(evlAPlusB);
CPPUNIT_TEST(evlIsNull);
CPPUNIT_TEST(evlMissingReason);
CPPUNIT_TEST(evlStrPlusNull);
CPPUNIT_TEST_SUITE_END();

public:
    void setUp()
    {
    }

    void tearDown()
    {
    }

    void evlVectorIsNull()
    {
        std::vector<FunctionPointer> convs;
        FunctionDescription func;
        CPPUNIT_ASSERT(FunctionLibrary::getInstance()->findFunction("is_null", boost::assign::list_of(TID_INT32), func, convs, true));
        Value inTile(TypeLibrary::getType(TID_INT32), Value::asTile);
        RLEPayload::Segment inSeg;
        inSeg._pPosition = 0;
        inSeg._same = true;
        inSeg._null = false;
        inSeg._valueIndex = inTile.getTile()->addRawValues();
        inTile.getTile()->addSegment(inSeg);
        inSeg._pPosition = 32;
        inSeg._same = true;
        inSeg._null = true;
        inTile.getTile()->addSegment(inSeg);
        inTile.getTile()->flush(64);
        int32_t* p = (int32_t*)inTile.getTile()->getRawValue(0);
        *p = 10;
        Value resTile(TypeLibrary::getType(TID_BOOL), Value::asTile);
        const Value* v = &inTile;
        func.getFuncPtr()(&v, &resTile, NULL);
        // Checking
        CPPUNIT_ASSERT(resTile.getTile()->nSegments() == 2);
        CPPUNIT_ASSERT(*resTile.getTile()->getRawValue(0) == 2);
    }

    void evlVectorAPlusB()
    {
        boost::shared_ptr<LogicalExpression> le = parseExpression("a+b");
        Expression e;
        e.addVariableInfo("a", TID_INT64);
        e.addVariableInfo("b", TID_INT64);
        boost::shared_ptr<scidb::Query> emptyQuery;
        e.compile(le, emptyQuery, true);
        CPPUNIT_ASSERT(e.getType() ==  TID_INT64);
        ExpressionContext ec(e);

        RLEPayload::Segment inSeg;
        inSeg._pPosition = 0;
        inSeg._same = false;
        inSeg._null = false;
        inSeg._valueIndex = ec[0].getTile()->addRawValues(32);
        ec[1].getTile()->addRawValues(32);

        ec[0].getTile()->addSegment(inSeg);
        ec[0].getTile()->flush(32);

        ec[1].getTile()->addSegment(inSeg);
        ec[1].getTile()->flush(32);

        int64_t* p0 = (int64_t*)ec[0].getTile()->getRawValue(0);
        int64_t* p1 = (int64_t*)ec[1].getTile()->getRawValue(0);
        for (int i = 0; i < 32; i++) {
            *p0++ = i;
            *p1++ = -i;
        }
        const Value& resTile = e.evaluate(ec);

        // Checking
        CPPUNIT_ASSERT(resTile.getTile()->nSegments() == 1);
        p0 = (int64_t*)resTile.getTile()->getRawValue(0);
        for (int i = 0; i < 32; i++) {
            CPPUNIT_ASSERT(*p0++ == 0);
        }
    }

    void evlVectorDenseMinusInt32()
    {
        std::vector<FunctionPointer> convs;
        FunctionDescription func;
        FunctionLibrary::getInstance()->findFunction("-", boost::assign::list_of(TID_INT32), func, convs, true);
        Value inTile(TypeLibrary::getType(TID_INT32), Value::asTile);
        RLEPayload::Segment inSeg;
        inSeg._pPosition = 0;
        inSeg._same = false;
        inSeg._null = false;
        inSeg._valueIndex = inTile.getTile()->addRawValues(32);
        inTile.getTile()->addSegment(inSeg);
        inTile.getTile()->flush(32);
        int32_t* p = (int32_t*)inTile.getTile()->getRawValue(0);
        for (int i = 0; i < 32; i++)
            *p++ = i;
        Value resTile(TypeLibrary::getType(TID_INT32), Value::asTile);
        const Value* v = &inTile;
        func.getFuncPtr()(&v, &resTile, NULL);
        // Checking
        CPPUNIT_ASSERT(resTile.getTile()->nSegments() == 1);
        p = (int32_t*)resTile.getTile()->getRawValue(0);
        for (int i = 0; i < 32; i++) {
            CPPUNIT_ASSERT(*p++ == -i);
        }
    }

    void evlVectorRLEMinusInt32()
    {
        std::vector<FunctionPointer> convs;
        FunctionDescription func;
        FunctionLibrary::getInstance()->findFunction("-", boost::assign::list_of(TID_INT32), func, convs, true);
        Value inTile(TypeLibrary::getType(TID_INT32), Value::asTile);
        RLEPayload::Segment inSeg;
        inSeg._pPosition = 0;
        inSeg._same = true;
        inSeg._null = false;
        inSeg._valueIndex = inTile.getTile()->addRawValues();
        inTile.getTile()->addSegment(inSeg);
        inTile.getTile()->flush(32);
        int32_t* p = (int32_t*)inTile.getTile()->getRawValue(0);
        *p = 10;
        Value resTile(TypeLibrary::getType(TID_INT32), Value::asTile);
        const Value* v = &inTile;
        func.getFuncPtr()(&v, &resTile, NULL);
        // Checking
        CPPUNIT_ASSERT(resTile.getTile()->nSegments() == 1);
        p = (int32_t*)resTile.getTile()->getRawValue(0);
        CPPUNIT_ASSERT(*p == -10);
    }

    void evlVectorDenseBinaryPlusInt32()
    {
        std::vector<FunctionPointer> convs;
        FunctionDescription func;
        FunctionLibrary::getInstance()->findFunction("+", boost::assign::list_of(TID_INT32)(TID_INT32), func, convs, true);
        Value inTile[2] = {Value(TypeLibrary::getType(TID_INT32), Value::asTile), Value(TypeLibrary::getType(TID_INT32), Value::asTile)};
        RLEPayload::Segment inSeg;
        inSeg._pPosition = 0;
        inSeg._same = false;
        inSeg._null = false;
        inSeg._valueIndex = inTile[0].getTile()->addRawValues(32);
        inTile[1].getTile()->addRawValues(32);

        inTile[0].getTile()->addSegment(inSeg);
        inTile[0].getTile()->flush(32);

        inTile[1].getTile()->addSegment(inSeg);
        inTile[1].getTile()->flush(32);

        int32_t* p0 = (int32_t*)inTile[0].getTile()->getRawValue(0);
        int32_t* p1 = (int32_t*)inTile[1].getTile()->getRawValue(0);
        for (int i = 0; i < 32; i++) {
            *p0++ = i;
            *p1++ = -i;
        }

        Value resTile(TypeLibrary::getType(TID_INT32), Value::asTile);
        const Value* v[2] = {&inTile[0], &inTile[1]};
        func.getFuncPtr()(v, &resTile, NULL);

        // Checking
        CPPUNIT_ASSERT(resTile.getTile()->nSegments() == 1);
        p0 = (int32_t*)resTile.getTile()->getRawValue(0);
        for (int i = 0; i < 32; i++) {
            CPPUNIT_ASSERT(*p0++ == 0);
        }
    }

    void evlVectorDenseBinaryAndBool()
    {
        std::vector<FunctionPointer> convs;
        FunctionDescription func;
        FunctionLibrary::getInstance()->findFunction("and", boost::assign::list_of(TID_BOOL)(TID_BOOL), func, convs, true);
        CPPUNIT_ASSERT(func.getFuncPtr());
        Value inTile[2] = {Value(TypeLibrary::getType(TID_BOOL), Value::asTile), Value(TypeLibrary::getType(TID_BOOL), Value::asTile)};
        RLEPayload::Segment inSeg;
        inSeg._pPosition = 0;
        inSeg._same = false;
        inSeg._null = false;
        inSeg._valueIndex = inTile[0].getTile()->addRawValues(32);
        inTile[1].getTile()->addRawValues(32);

        inTile[0].getTile()->addSegment(inSeg);
        inTile[0].getTile()->flush(32);

        inTile[1].getTile()->addSegment(inSeg);
        inTile[1].getTile()->flush(32);

        int32_t* p0 = (int32_t*)inTile[0].getTile()->getRawValue(0);
        int32_t* p1 = (int32_t*)inTile[1].getTile()->getRawValue(0);
        *p0 = 0xF0F0F0F0;
        *p1 = 0x0F0F0F0F;

        Value resTile(TypeLibrary::getType(TID_BOOL), Value::asTile);
        const Value* v[2] = {&inTile[0], &inTile[1]};
        func.getFuncPtr()(v, &resTile, NULL);

        // Checking
        CPPUNIT_ASSERT(resTile.getTile()->nSegments() == 1);
        p0 = (int32_t*)resTile.getTile()->getRawValue(0);
        CPPUNIT_ASSERT(*p0 == 0);
    }

    void evlVectorRLEBinaryPlusInt32()
    {
        std::vector<FunctionPointer> convs;
        FunctionDescription func;
        FunctionLibrary::getInstance()->findFunction("+", boost::assign::list_of(TID_INT32)(TID_INT32), func, convs, true);
        Value inTile[2] = {Value(TypeLibrary::getType(TID_INT32), Value::asTile), Value(TypeLibrary::getType(TID_INT32), Value::asTile)};
        RLEPayload::Segment inSeg;
        inSeg._pPosition = 0;
        inSeg._same = true;
        inSeg._null = false;
        inSeg._valueIndex = inTile[0].getTile()->addRawValues();
        inTile[1].getTile()->addRawValues();

        inTile[0].getTile()->addSegment(inSeg);
        inTile[0].getTile()->flush(30);

        inTile[1].getTile()->addSegment(inSeg);
        inTile[1].getTile()->flush(32);

        int32_t* p0 = (int32_t*)inTile[0].getTile()->getRawValue(0);
        int32_t* p1 = (int32_t*)inTile[1].getTile()->getRawValue(0);
        *p0 = 10;
        *p1 = -10;

        Value resTile(TypeLibrary::getType(TID_INT32), Value::asTile);
        const Value* v[2] = {&inTile[0], &inTile[1]};
        func.getFuncPtr()(v, &resTile, NULL);

        // Checking
        CPPUNIT_ASSERT(resTile.getTile()->nSegments() == 1);
        CPPUNIT_ASSERT(resTile.getTile()->getSegment(0).length() == 30);
        p0 = (int32_t*)resTile.getTile()->getRawValue(0);
        CPPUNIT_ASSERT(*p0 == 0);
    }

    void evlVectorMixBinaryPlusInt32()
    {
        /**
         * We add two segments into every operand.
         * Segments have different lengths and mixed encoding.
         */
        std::vector<FunctionPointer> convs;
        FunctionDescription func;
        FunctionLibrary::getInstance()->findFunction("+", boost::assign::list_of(TID_INT32)(TID_INT32), func, convs, true);
        Value inTile[2] = {Value(TypeLibrary::getType(TID_INT32), Value::asTile), Value(TypeLibrary::getType(TID_INT32), Value::asTile)};
        RLEPayload::Segment inSeg;

        // Adding 10 same values into tile 0
        inSeg._pPosition = 0;
        inSeg._same = true;
        inSeg._null = false;
        inSeg._valueIndex = inTile[0].getTile()->addRawValues();
        inTile[0].getTile()->addSegment(inSeg);
        int32_t* p = (int32_t*)inTile[0].getTile()->getRawValue(inSeg._valueIndex);
        *p = 5;

        // Adding 20 different values into tile 0
        inSeg._pPosition = 10;
        inSeg._same = false;
        inSeg._null = false;
        inSeg._valueIndex = inTile[0].getTile()->addRawValues(20);
        inTile[0].getTile()->addSegment(inSeg);
        p = (int32_t*)inTile[0].getTile()->getRawValue(inSeg._valueIndex);
        for (int i = 0; i < 20; i++)
            *p++ = i;

        inTile[0].getTile()->flush(30);

        // Adding 20 different values into tile 1
        inSeg._pPosition = 0;
        inSeg._same = false;
        inSeg._null = false;
        inSeg._valueIndex = inTile[1].getTile()->addRawValues(20);
        inTile[1].getTile()->addSegment(inSeg);
        p = (int32_t*)inTile[1].getTile()->getRawValue(inSeg._valueIndex);
        for (int i = 0; i < 20; i++)
            *p++ = i;

        // Adding 20 same values into tile 1
        inSeg._pPosition = 20;
        inSeg._same = true;
        inSeg._null = false;
        inSeg._valueIndex = inTile[1].getTile()->addRawValues();
        inTile[1].getTile()->addSegment(inSeg);
        p = (int32_t*)inTile[1].getTile()->getRawValue(inSeg._valueIndex);
        *p = 5;

        inTile[1].getTile()->flush(40);

        Value resTile(TypeLibrary::getType(TID_INT32), Value::asTile);
        const Value* v[2] = {&inTile[0], &inTile[1]};
        func.getFuncPtr()(v, &resTile, NULL);

        // Checking
        CPPUNIT_ASSERT(resTile.getTile()->nSegments() == 3);
        CPPUNIT_ASSERT(resTile.getTile()->getSegment(0).length() == 10);
        CPPUNIT_ASSERT(resTile.getTile()->getSegment(1).length() == 10);
        CPPUNIT_ASSERT(resTile.getTile()->getSegment(2).length() == 10);
        p = (int32_t*)resTile.getTile()->getRawValue(0);
        for (int i = 0; i < 10; i++) {
            CPPUNIT_ASSERT(*p++ == i + 5);
        }
        for (int i = 0; i < 10; i++) {
            CPPUNIT_ASSERT(*p++ == i * 2 + 10);
        }
        for (int i = 0; i < 10; i++) {
            CPPUNIT_ASSERT(*p++ == i + 15);
        }
    }

    void evlPerfExp()
    {
        boost::shared_ptr<LogicalExpression> le = parseExpression("5000000000/1000 + 5 + 5000000000/1000 + 5 + 5000000000/1000 + 5 + 5000000000/1000 + 5 + 5000000000/1000 + 5 + 5 + 5000000000/1000 + 5 + 5000000000/1000 + 5 + 5000000000/1000 + 5 + 5000000000/1000 + 5 + 5000000000/1000 + 5 + 5");
        Expression e;
        boost::shared_ptr<scidb::Query> emptyQuery;
        e.compile(le, emptyQuery, false);
        CPPUNIT_ASSERT(e.getType() ==  TID_INT64);
        const clock_t startClock = clock();
        for (size_t i = 0; i < 10000; i++) {
            CPPUNIT_ASSERT(e.evaluate().getInt64() == 50000060);
            CPPUNIT_ASSERT(e.evaluate().getInt64() == 50000060);
            CPPUNIT_ASSERT(e.evaluate().getInt64() == 50000060);
            CPPUNIT_ASSERT(e.evaluate().getInt64() == 50000060);
            CPPUNIT_ASSERT(e.evaluate().getInt64() == 50000060);
            CPPUNIT_ASSERT(e.evaluate().getInt64() == 50000060);
            CPPUNIT_ASSERT(e.evaluate().getInt64() == 50000060);
            CPPUNIT_ASSERT(e.evaluate().getInt64() == 50000060);
            CPPUNIT_ASSERT(e.evaluate().getInt64() == 50000060);
            CPPUNIT_ASSERT(e.evaluate().getInt64() == 50000060);
        }
        const clock_t stopClock = clock();
        std::cout << "expression time:" <<(stopClock - startClock) * 1000 / CLOCKS_PER_SEC << "ms" << std::endl;
    }

    void evlPerfCPP()
    {
        const clock_t startClock = clock();
        for (size_t i = 0; i < 10000; i++) {
            CPPUNIT_ASSERT(5000000000/1000 + 5 + 5000000000/1000 + 5 + 5000000000/1000 + 5 + 5000000000/1000 + 5 + 5000000000/1000 + 5 + 5 + 5000000000/1000 + 5 + 5000000000/1000 + 5 + 5000000000/1000 + 5 + 5000000000/1000 + 5 + 5000000000/1000 + 5 + 5 == 50000060);
            CPPUNIT_ASSERT(5000000000/1000 + 5 + 5000000000/1000 + 5 + 5000000000/1000 + 5 + 5000000000/1000 + 5 + 5000000000/1000 + 5 + 5 + 5000000000/1000 + 5 + 5000000000/1000 + 5 + 5000000000/1000 + 5 + 5000000000/1000 + 5 + 5000000000/1000 + 5 + 5 == 50000060);
            CPPUNIT_ASSERT(5000000000/1000 + 5 + 5000000000/1000 + 5 + 5000000000/1000 + 5 + 5000000000/1000 + 5 + 5000000000/1000 + 5 + 5 + 5000000000/1000 + 5 + 5000000000/1000 + 5 + 5000000000/1000 + 5 + 5000000000/1000 + 5 + 5000000000/1000 + 5 + 5 == 50000060);
            CPPUNIT_ASSERT(5000000000/1000 + 5 + 5000000000/1000 + 5 + 5000000000/1000 + 5 + 5000000000/1000 + 5 + 5000000000/1000 + 5 + 5 + 5000000000/1000 + 5 + 5000000000/1000 + 5 + 5000000000/1000 + 5 + 5000000000/1000 + 5 + 5000000000/1000 + 5 + 5 == 50000060);
            CPPUNIT_ASSERT(5000000000/1000 + 5 + 5000000000/1000 + 5 + 5000000000/1000 + 5 + 5000000000/1000 + 5 + 5000000000/1000 + 5 + 5 + 5000000000/1000 + 5 + 5000000000/1000 + 5 + 5000000000/1000 + 5 + 5000000000/1000 + 5 + 5000000000/1000 + 5 + 5 == 50000060);
            CPPUNIT_ASSERT(5000000000/1000 + 5 + 5000000000/1000 + 5 + 5000000000/1000 + 5 + 5000000000/1000 + 5 + 5000000000/1000 + 5 + 5 + 5000000000/1000 + 5 + 5000000000/1000 + 5 + 5000000000/1000 + 5 + 5000000000/1000 + 5 + 5000000000/1000 + 5 + 5 == 50000060);
            CPPUNIT_ASSERT(5000000000/1000 + 5 + 5000000000/1000 + 5 + 5000000000/1000 + 5 + 5000000000/1000 + 5 + 5000000000/1000 + 5 + 5 + 5000000000/1000 + 5 + 5000000000/1000 + 5 + 5000000000/1000 + 5 + 5000000000/1000 + 5 + 5000000000/1000 + 5 + 5 == 50000060);
            CPPUNIT_ASSERT(5000000000/1000 + 5 + 5000000000/1000 + 5 + 5000000000/1000 + 5 + 5000000000/1000 + 5 + 5000000000/1000 + 5 + 5 + 5000000000/1000 + 5 + 5000000000/1000 + 5 + 5000000000/1000 + 5 + 5000000000/1000 + 5 + 5000000000/1000 + 5 + 5 == 50000060);
            CPPUNIT_ASSERT(5000000000/1000 + 5 + 5000000000/1000 + 5 + 5000000000/1000 + 5 + 5000000000/1000 + 5 + 5000000000/1000 + 5 + 5 + 5000000000/1000 + 5 + 5000000000/1000 + 5 + 5000000000/1000 + 5 + 5000000000/1000 + 5 + 5000000000/1000 + 5 + 5 == 50000060);
            CPPUNIT_ASSERT(5000000000/1000 + 5 + 5000000000/1000 + 5 + 5000000000/1000 + 5 + 5000000000/1000 + 5 + 5000000000/1000 + 5 + 5 + 5000000000/1000 + 5 + 5000000000/1000 + 5 + 5000000000/1000 + 5 + 5000000000/1000 + 5 + 5000000000/1000 + 5 + 5 == 50000060);
        }
        const clock_t stopClock = clock();
        std::cout << "c++ time:" <<(stopClock - startClock) * 1000 / CLOCKS_PER_SEC << "ms" << std::endl;
    }

    void evlInt32PlusInt32()
    {
        boost::shared_ptr< LogicalExpression> le = parseExpression("1+1");
         Expression e;
        boost::shared_ptr<scidb::Query> emptyQuery;
        e.compile(le, emptyQuery, false);
        CPPUNIT_ASSERT(e.getType() ==  TID_INT64);
        CPPUNIT_ASSERT(e.evaluate().getInt64() == 2);
    }

    void evlInt64PlusInt64()
    {
        boost::shared_ptr< LogicalExpression> le = parseExpression("5000000000+5000000000");
         Expression e;
        boost::shared_ptr<scidb::Query> emptyQuery;
        e.compile(le, emptyQuery, false);
        CPPUNIT_ASSERT(e.getType() ==  TID_INT64);
        CPPUNIT_ASSERT(e.evaluate().getInt64() == 10000000000);
    }

    void evlInt32PlusInt64()
    {
        boost::shared_ptr< LogicalExpression> le = parseExpression("5+5000000000");
         Expression e;
        boost::shared_ptr<scidb::Query> emptyQuery;
        e.compile(le, emptyQuery, false);
        CPPUNIT_ASSERT(e.getType() ==  TID_INT64);
        CPPUNIT_ASSERT(e.evaluate().getInt64() == 5000000005);
    }

    void evlInt32PlusInt32PlusInt64()
    {
        boost::shared_ptr< LogicalExpression> le = parseExpression("5+5+5000000000");
         Expression e;
        boost::shared_ptr<scidb::Query> emptyQuery;
        e.compile(le, emptyQuery, false);
        CPPUNIT_ASSERT(e.getType() ==  TID_INT64); // TODO: It's not so correct. We need to implement minimal number of used converters for arguments when find functions.
        CPPUNIT_ASSERT(e.evaluate().getInt64() == 5000000010);
    }

    void evlInt32PlusNull()
    {
        boost::shared_ptr< LogicalExpression> le = parseExpression("5+NULL");
         Expression e;
        boost::shared_ptr<scidb::Query> emptyQuery;
        e.compile(le, emptyQuery, false);
        CPPUNIT_ASSERT(e.getType() ==  TID_INT64);
        CPPUNIT_ASSERT(e.evaluate().isNull());
    }

    void evlUnaryMinusInt32()
    {
        boost::shared_ptr< LogicalExpression> le = parseExpression("-5");
         Expression e;
        boost::shared_ptr<scidb::Query> emptyQuery;
        e.compile(le, emptyQuery, false);
        CPPUNIT_ASSERT(e.getType() ==  TID_INT64);
        CPPUNIT_ASSERT(e.evaluate().getInt64() == -5);
    }

    void evlPowDouble()
    {
        boost::shared_ptr< LogicalExpression> le = parseExpression("pow(0.5, 2.0)");
         Expression e;
        boost::shared_ptr<scidb::Query> emptyQuery;
        e.compile(le, emptyQuery, false);
        CPPUNIT_ASSERT(e.getType() ==  TID_DOUBLE);
        CPPUNIT_ASSERT(e.evaluate().getDouble() == 0.25);
    }

    void evlSinDouble()
    {
        boost::shared_ptr< LogicalExpression> le = parseExpression("sin(0.0)");
         Expression e;
        boost::shared_ptr<scidb::Query> emptyQuery;
        e.compile(le, emptyQuery, false);
        CPPUNIT_ASSERT(e.getType() ==  TID_DOUBLE);
        CPPUNIT_ASSERT(e.evaluate().getDouble() == 0);
    }

    void evlExplicitConvDouble()
    {
        boost::shared_ptr< LogicalExpression> le = parseExpression("double(0)");
         Expression e;
        boost::shared_ptr<scidb::Query> emptyQuery;
        e.compile(le, emptyQuery, false);
        CPPUNIT_ASSERT(e.getType() ==  TID_DOUBLE);
        CPPUNIT_ASSERT(e.evaluate().getDouble() == 0);
    }

    void evlExplicitConvInt32()
    {
        boost::shared_ptr< LogicalExpression> le = parseExpression("int32(0.0)");
         Expression e;
        boost::shared_ptr<scidb::Query> emptyQuery;
        e.compile(le, emptyQuery, false);
        CPPUNIT_ASSERT(e.getType() ==  TID_INT32);
        CPPUNIT_ASSERT(e.evaluate().getInt32() == 0);
    }

    void evliif0()
    {
        boost::shared_ptr<LogicalExpression> le = parseExpression("iif(1 < 0, 0/0 , 1)");
        Expression e;
        boost::shared_ptr<scidb::Query> emptyQuery;
        e.compile(le, emptyQuery, false);
        CPPUNIT_ASSERT(e.getType() ==  TID_INT64);
        CPPUNIT_ASSERT(e.evaluate().getInt64() == 1);
    }

    void evliif1()
    {
        boost::shared_ptr<LogicalExpression> le = parseExpression("iif(1 > 0, 5000000000, 0/0)");
        Expression e;
        boost::shared_ptr<scidb::Query> emptyQuery;
        e.compile(le, emptyQuery, false);
        CPPUNIT_ASSERT(e.getType() ==  TID_INT64);
        CPPUNIT_ASSERT(e.evaluate().getInt64() == 5000000000);
    }

    void evlor_lazy()
    {
        boost::shared_ptr<LogicalExpression> le = parseExpression("(1 > 0) or 0/0");
        Expression e;
        boost::shared_ptr<scidb::Query> emptyQuery;
        e.compile(le, emptyQuery, false);
        CPPUNIT_ASSERT(e.getType() ==  TID_BOOL);
        CPPUNIT_ASSERT(e.evaluate().getBool() == true);
    }

    void evland_lazy()
    {
        boost::shared_ptr<LogicalExpression> le = parseExpression("(1 < 0) and 0/0");
        Expression e;
        boost::shared_ptr<scidb::Query> emptyQuery;
        e.compile(le, emptyQuery, false);
        CPPUNIT_ASSERT(e.getType() ==  TID_BOOL);
        CPPUNIT_ASSERT(e.evaluate().getBool() == false);
    }

    void evland_lazy2()
    {
        boost::shared_ptr<LogicalExpression> le = parseExpression("3 > 2 and not (2 = 1)");
        Expression e;
        boost::shared_ptr<scidb::Query> emptyQuery;
        e.compile(le, emptyQuery, false);
        CPPUNIT_ASSERT(e.getType() ==  TID_BOOL);
        CPPUNIT_ASSERT(e.evaluate().getBool() == true);
    }

    void evlBinaryCompileIntPlusDouble()
    {
         Expression e;
        e.compile("+", false,  TID_INT32,  TID_DOUBLE);
        CPPUNIT_ASSERT(e.getType() ==  TID_DOUBLE);
         ExpressionContext c(e);
        c[0].setInt32(10);
        c[1].setDouble(20);
        CPPUNIT_ASSERT(e.evaluate(c).getDouble() == 30);
    }

    void evlSerialization()
    {
        Expression e;
        e.compile("+", false, TID_INT32, TID_DOUBLE);
        CPPUNIT_ASSERT(e.getType() == TID_DOUBLE);
        ExpressionContext c(e);
        c[0].setInt32(10);
        c[1].setDouble(20);
        CPPUNIT_ASSERT(e.evaluate(c).getDouble() == 30);

        std::stringstream ss;
        boost::archive::text_oarchive oa(ss);
        oa & e;
//        std::cout << ss.str();

        Expression r;
        boost::archive::text_iarchive ia(ss);
        ia & r;
        ExpressionContext c1(e);
        c1[0].setInt32(10);
        c1[1].setDouble(20);
        CPPUNIT_ASSERT(r.getType() ==  TID_DOUBLE);
        CPPUNIT_ASSERT(r.evaluate(c1).getDouble() == 30);
    }

    void evlInstanceId()
    {
        boost::shared_ptr< LogicalExpression> le = parseExpression("instanceid()");
         Expression e;

        boost::shared_ptr<scidb::Query> emptyQuery;
        e.compile(le, emptyQuery, false);
        CPPUNIT_ASSERT(e.getType() ==  TID_INT64);

        //we can't really evaluate this one because it requires a running network manager.
        //I tried creating a fake network manager, and overriding instanceId, but then we need to be in the same namespace - not worth it.
    }

    void evlInt8PlusInt16()
    {
        boost::shared_ptr< LogicalExpression> le = parseExpression("int8(8)+int16(-8)");
        Expression e;
        boost::shared_ptr<scidb::Query> emptyQuery;
        e.compile(le, emptyQuery, false);
        CPPUNIT_ASSERT(e.getType() ==  TID_INT16);
        CPPUNIT_ASSERT(e.evaluate().getInt16() == 0);
    }

    void evlAPlusB()
    {
        vector<string> names;
        names.push_back("a");
        names.push_back("b");
        names.push_back("c");
        names.push_back("x");
        vector<TypeId> types;
        types.push_back(TID_INT64);
        types.push_back(TID_INT64);
        types.push_back(TID_INT64);
        types.push_back(TID_INT64);
        Expression e;
        e.compile("a*x*x+b*x+c", names, types);
        CPPUNIT_ASSERT(e.getType() ==  TID_INT64);
        ExpressionContext ec(e);
        ec[0].setInt64(5);
        ec[1].setInt64(10);
        ec[2].setInt64(15);
        ec[3].setInt64(10);
        CPPUNIT_ASSERT(e.evaluate(ec).getInt64() == 615);
    }

    void evlIsNull()
    {
        boost::shared_ptr< LogicalExpression> le = parseExpression("is_null(NULL)");
         Expression e;
        boost::shared_ptr<scidb::Query> emptyQuery;
        e.compile(le, emptyQuery, false);
        CPPUNIT_ASSERT(e.getType() ==  TID_BOOL);
        CPPUNIT_ASSERT(e.evaluate().getBool() == true);
    }

    void evlMissingReason()
    {
        boost::shared_ptr<LogicalExpression> le = parseExpression("missing_reason(NULL)");
        Expression e;
        boost::shared_ptr<scidb::Query> emptyQuery;
        e.compile(le, emptyQuery, false);
        CPPUNIT_ASSERT(e.getType() == TID_INT32);
        CPPUNIT_ASSERT(e.evaluate().getInt32() == 0);
    }

    void evlStrPlusNull()
    {
        boost::shared_ptr<LogicalExpression> le = parseExpression("NULL + 'xyz'");
        Expression e;
        boost::shared_ptr<scidb::Query> emptyQuery;
        e.compile(le, emptyQuery, false);
        CPPUNIT_ASSERT(e.getType() == TID_STRING);
        CPPUNIT_ASSERT(e.evaluate().isNull());
    }
};

CPPUNIT_TEST_SUITE_REGISTRATION(ExpressionTests);

#endif /* EXPRESSION_UNIT_TESTS_H_ */
