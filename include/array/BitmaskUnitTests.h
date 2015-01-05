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
 * BitmaskUnitTests.h
 *
 * @author: poliocough@gmail.com
 */

#ifndef BITMASKUNITTESTS_H_
#define BITMASKUNITTESTS_H_

#include <cppunit/TestFixture.h>
#include <cppunit/extensions/HelperMacros.h>
#include <iostream>

#include "array/RLE.h"


using namespace scidb;

class BitmaskTests: public CppUnit::TestFixture
{
CPPUNIT_TEST_SUITE(BitmaskTests);
CPPUNIT_TEST(testSimplePrint);
CPPUNIT_TEST(testIterator);
CPPUNIT_TEST(testCopy);
CPPUNIT_TEST(testBoolPayload);
CPPUNIT_TEST(testAppender);
CPPUNIT_TEST_SUITE_END();


public:
    std::ostringstream stream;
    RLEEmptyBitmap emptyBitmask;
    RLEEmptyBitmap bitmaskOne;

    void setUp()
    {
        stream.str("");

        emptyBitmask.clear();
        bitmaskOne.clear();

        RLEEmptyBitmap::Segment seg;
        seg._lPosition = 3;
        seg._pPosition = 5;
        seg._length=5;
        bitmaskOne.addSegment(seg);
        seg._lPosition = 10;
        seg._pPosition = 11;
        seg._length=1;
        bitmaskOne.addSegment(seg);
        seg._lPosition = 13;
        seg._pPosition = 14;
        seg._length=3;
        bitmaskOne.addSegment(seg);
    }

    void tearDown()
    {}

    void testSimplePrint()
    {
        stream<<emptyBitmask;
        CPPUNIT_ASSERT(stream.str() == "[empty]");
        stream.str("");

        stream<<bitmaskOne;
        CPPUNIT_ASSERT(stream.str() == "[3,5,5];[10,11,1];[13,14,3];");
    }

    void testIteratorHelper(ConstRLEEmptyBitmap const& empty, ConstRLEEmptyBitmap const& bm1)
    {
        RLEEmptyBitmap::iterator e_iter = empty.getIterator();
        CPPUNIT_ASSERT(e_iter.end());
        CPPUNIT_ASSERT(e_iter.setPosition(0)==false);
        CPPUNIT_ASSERT(e_iter.end());
        CPPUNIT_ASSERT(e_iter.setPosition(1)==false);
        CPPUNIT_ASSERT(e_iter.end());

        RLEEmptyBitmap::iterator iter = bm1.getIterator();
        CPPUNIT_ASSERT(iter.end() == false);
        CPPUNIT_ASSERT(iter.getLPos() == 3 && iter.getPPos() == 5);
        ++iter;
        CPPUNIT_ASSERT(iter.getLPos() == 4 && iter.getPPos() == 6);
        ++iter;
        CPPUNIT_ASSERT(iter.getLPos() == 5 && iter.getPPos() == 7);
        ++iter;
        CPPUNIT_ASSERT(iter.getLPos() == 6 && iter.getPPos() == 8);
        ++iter;
        CPPUNIT_ASSERT(iter.getLPos() == 7 && iter.getPPos() == 9);
        ++iter;
        CPPUNIT_ASSERT(iter.getLPos() == 10 && iter.getPPos() == 11);
        ++iter;
        CPPUNIT_ASSERT(iter.getLPos() == 13 && iter.getPPos() == 14);
        ++iter;
        CPPUNIT_ASSERT(iter.getLPos() == 14 && iter.getPPos() == 15);
        ++iter;
        CPPUNIT_ASSERT(iter.getLPos() == 15 && iter.getPPos() == 16);
        ++iter;
        CPPUNIT_ASSERT(iter.end());

        CPPUNIT_ASSERT(iter.setPosition(3));
        CPPUNIT_ASSERT(iter.getLPos() == 3 && iter.getPPos() == 5);
        ++iter;
        CPPUNIT_ASSERT(iter.getLPos() == 4 && iter.getPPos() == 6);

        CPPUNIT_ASSERT(iter.setPosition(10));
        CPPUNIT_ASSERT(iter.getLPos() == 10 && iter.getPPos() == 11);
        ++iter;
        CPPUNIT_ASSERT(iter.getLPos() == 13 && iter.getPPos() == 14);

        CPPUNIT_ASSERT(iter.setPosition(7));
        CPPUNIT_ASSERT(iter.getLPos() == 7 && iter.getPPos() == 9);
        ++iter;
        CPPUNIT_ASSERT(iter.getLPos() == 10 && iter.getPPos() == 11);

        CPPUNIT_ASSERT(iter.setPosition(15));
        CPPUNIT_ASSERT(iter.getLPos() == 15 && iter.getPPos() == 16);
        ++iter;
        CPPUNIT_ASSERT(iter.end());

        CPPUNIT_ASSERT(iter.setPosition(2)==false);
        CPPUNIT_ASSERT(iter.end());
        CPPUNIT_ASSERT(iter.setPosition(8)==false);
        CPPUNIT_ASSERT(iter.end());

        CPPUNIT_ASSERT(iter.setPosition(9)==false);
        CPPUNIT_ASSERT(iter.end());
        CPPUNIT_ASSERT(iter.setPosition(11)==false);
        CPPUNIT_ASSERT(iter.end());

        CPPUNIT_ASSERT(iter.setPosition(16)==false);
        CPPUNIT_ASSERT(iter.end());

        iter.reset();
        CPPUNIT_ASSERT(iter.end()==false);
        CPPUNIT_ASSERT(iter.getLPos() == 3 && iter.getPPos() == 5);
    }


    void testIterator()
    {
        testIteratorHelper(emptyBitmask, bitmaskOne);
    }

    void testCopy()
    {
        //shallow copy of pointer
        ConstRLEEmptyBitmap eCopy(emptyBitmask);
        ConstRLEEmptyBitmap bm1Copy(bitmaskOne);
        testIteratorHelper(eCopy, bm1Copy);

        eCopy = emptyBitmask;
        bm1Copy = bitmaskOne;
        testIteratorHelper(eCopy, bm1Copy);

        //deep copy of data
        RLEEmptyBitmap eCopy2(emptyBitmask);
        RLEEmptyBitmap bm1Copy2(bitmaskOne);
        testIteratorHelper(eCopy2, bm1Copy2);

        eCopy2 = emptyBitmask;
        bm1Copy2 = bitmaskOne;
        testIteratorHelper(eCopy2, bm1Copy2);

        RLEEmptyBitmap::Segment ns;
        ns._lPosition=17;
        ns._pPosition=17;
        ns._length=2;

        eCopy2.addSegment(ns);
        bm1Copy2.addSegment(ns);

        //adding data to copy2 should not mess with the originals
        testIteratorHelper(emptyBitmask, bitmaskOne);

        RLEEmptyBitmap::iterator iter = eCopy2.getIterator();
        CPPUNIT_ASSERT(iter.setPosition(17));
        iter = bm1Copy2.getIterator();
        CPPUNIT_ASSERT(iter.setPosition(17));

        //deep copy of data from pointer
        RLEEmptyBitmap eCopy3(eCopy);
        RLEEmptyBitmap bm1Copy3(bm1Copy);
        testIteratorHelper(eCopy3, bm1Copy3);

        eCopy3=eCopy;
        bm1Copy3=bm1Copy;
        testIteratorHelper(eCopy3, bm1Copy3);
    }

    void testBoolPayload()
    {
        ValueMap vm;
        Value nullVal;
        nullVal.setNull(0);

        Value bTrue;
        bTrue.setBool(true);

        Value bFalse;
        bFalse.setBool(false);

        vm[0]=bTrue;        //0
        vm[1]=bFalse;       //1
        vm[2]=bFalse;
        vm[3]=bFalse;
        //4 is NULL
        vm[5]=bTrue;        //2
        //6 is NULL
        vm[7]=bTrue;        //3
        //8 is NULL
        //9 is NULL
        vm[10]=bFalse;      //4
        vm[11]=bTrue;       //5
        vm[12]=bFalse;      //6
        vm[13]=bTrue;       //7
        vm[14]=bFalse;      //8
        vm[15]=bTrue;       //9

        RLEPayload payload(vm, 16, 1, nullVal, true, false);

        Value v;
        payload.getValueByIndex(v,0);
        CPPUNIT_ASSERT(v.getBool()==true);
        payload.getValueByIndex(v,1);
        CPPUNIT_ASSERT(v.getBool()==false);
        payload.getValueByIndex(v,2);
        CPPUNIT_ASSERT(v.getBool()==true);
        payload.getValueByIndex(v,3);
        CPPUNIT_ASSERT(v.getBool()==true);
        payload.getValueByIndex(v,4);
        CPPUNIT_ASSERT(v.getBool()==false);
        payload.getValueByIndex(v,5);
        CPPUNIT_ASSERT(v.getBool()==true);
        payload.getValueByIndex(v,6);
        CPPUNIT_ASSERT(v.getBool()==false);
        payload.getValueByIndex(v,7);
        CPPUNIT_ASSERT(v.getBool()==true);
        payload.getValueByIndex(v,8);
        CPPUNIT_ASSERT(v.getBool()==false);
        payload.getValueByIndex(v,9);
        CPPUNIT_ASSERT(v.getBool()==true);

        ConstRLEPayload::iterator iter = payload.getIterator();
        iter.getItem(v);
        CPPUNIT_ASSERT(v.getBool()==true);
        ++iter;
        iter.getItem(v);
        CPPUNIT_ASSERT(v.getBool()==false);
        ++iter;
        iter.getItem(v);
        CPPUNIT_ASSERT(v.getBool()==false);
        ++iter;
        iter.getItem(v);
        CPPUNIT_ASSERT(v.getBool()==false);
        ++iter;
        iter.getItem(v);
        CPPUNIT_ASSERT(v.isNull());
        ++iter;
        iter.getItem(v);
        CPPUNIT_ASSERT(v.getBool()==true);
        ++iter;
        iter.getItem(v);
        CPPUNIT_ASSERT(v.isNull());
        ++iter;
        iter.getItem(v);
        CPPUNIT_ASSERT(v.getBool()==true);
        ++iter;
        iter.getItem(v);
        CPPUNIT_ASSERT(v.isNull());
        ++iter;
        iter.getItem(v);
        CPPUNIT_ASSERT(v.isNull());
        ++iter;
        iter.getItem(v);
        CPPUNIT_ASSERT(v.getBool()==false);
        ++iter;
        iter.getItem(v);
        CPPUNIT_ASSERT(v.getBool()==true);
        ++iter;
        iter.getItem(v);
        CPPUNIT_ASSERT(v.getBool()==false);
        ++iter;
        iter.getItem(v);
        CPPUNIT_ASSERT(v.getBool()==true);
        ++iter;
        iter.getItem(v);
        CPPUNIT_ASSERT(v.getBool()==false);
        ++iter;
        iter.getItem(v);
        CPPUNIT_ASSERT(v.getBool()==true);
        ++iter;
        CPPUNIT_ASSERT(iter.end());
    }

    void testAppender()
    {
        RLEPayloadAppender a1(8*sizeof(int32_t));
        a1.finalize();

        RLEPayload const* p1 = a1.getPayload();
        CPPUNIT_ASSERT(p1->elementSize() == sizeof(int32_t));
        CPPUNIT_ASSERT(p1->count()==0);
        ConstRLEPayload::iterator iter = p1->getIterator();
        CPPUNIT_ASSERT(iter.end());

        Value null0;
        null0.setNull(0);
        Value null1;
        null1.setNull(1);

        Value v;
        v.setInt32(0);

        RLEPayloadAppender a2(8*sizeof(int32_t));
        a2.append(v);
        a2.append(v);
        a2.append(null0);
        a2.append(v);
        v.setInt32(1);
        a2.append(v);
        v.setInt32(2);
        a2.append(v);
        v.setInt32(3);
        a2.append(v);
        a2.append(v);
        a2.append(v);
        a2.append(null0);
        a2.append(null0);
        a2.append(null1);
        a2.append(null0);
        a2.append(v);
        a2.append(v);
        a2.append(v);
        a2.finalize();

        RLEPayload const* p2 = a2.getPayload();
        iter = p2->getIterator();

        CPPUNIT_ASSERT(iter.end()==false);
        iter.getItem(v);
        CPPUNIT_ASSERT(v.getInt32() == 0);
        ++iter;
        iter.getItem(v);
        CPPUNIT_ASSERT(v.getInt32() == 0);
        ++iter;
        iter.getItem(v);
        CPPUNIT_ASSERT(v.isNull() && v.getMissingReason() == 0);
        ++iter;
        iter.getItem(v);
        CPPUNIT_ASSERT(v.getInt32() == 0);
        ++iter;
        iter.getItem(v);
        CPPUNIT_ASSERT(v.getInt32() == 1);
        ++iter;
        iter.getItem(v);
        CPPUNIT_ASSERT(v.getInt32() == 2);
        ++iter;
        iter.getItem(v);
        CPPUNIT_ASSERT(v.getInt32() == 3);
        ++iter;
        iter.getItem(v);
        CPPUNIT_ASSERT(v.getInt32() == 3);
        ++iter;
        iter.getItem(v);
        CPPUNIT_ASSERT(v.getInt32() == 3);
        ++iter;
        iter.getItem(v);
        CPPUNIT_ASSERT(v.isNull() && v.getMissingReason() == 0);
        ++iter;
        iter.getItem(v);
        CPPUNIT_ASSERT(v.isNull() && v.getMissingReason() == 0);
        ++iter;
        iter.getItem(v);
        CPPUNIT_ASSERT(v.isNull() && v.getMissingReason() == 1);
        ++iter;
        iter.getItem(v);
        CPPUNIT_ASSERT(v.isNull() && v.getMissingReason() == 0);
        ++iter;
        iter.getItem(v);
        CPPUNIT_ASSERT(v.getInt32() == 3);
        ++iter;
        iter.getItem(v);
        CPPUNIT_ASSERT(v.getInt32() == 3);
        ++iter;
        iter.getItem(v);
        CPPUNIT_ASSERT(v.getInt32() == 3);
        ++iter;
        CPPUNIT_ASSERT(iter.end());
   }
};



CPPUNIT_TEST_SUITE_REGISTRATION(BitmaskTests);

#endif /* BITMASKUNITTESTS_H_ */
