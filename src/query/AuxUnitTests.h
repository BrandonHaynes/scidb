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
 * @file AuxUnitTests.h
 * Auxiliary unit tests for smaller components.
 * @author poliocough@gmail.com
 */

#ifndef AUX_UNIT_TESTS
#define AUX_UNIT_TESTS

#include <cppunit/TestFixture.h>
#include <cppunit/extensions/HelperMacros.h>

#include <boost/shared_ptr.hpp>

#include "query/Operator.h"

using namespace scidb;

class AuxTests: public CppUnit::TestFixture
{
CPPUNIT_TEST_SUITE(AuxTests);
CPPUNIT_TEST(testChunkInstanceMap);
CPPUNIT_TEST_SUITE_END();

private:

public:
    void setUp()
    {
    }

    void tearDown()
    {
    }

    void testCoordinateStreaming()
    {
        {
            std::ostringstream out;
            Coordinates coords(1);

            coords[0]=7;

            out << coords;
            CPPUNIT_ASSERT(out.str() == "{7}");
        }
        {
            std::ostringstream out;
            Coordinates coords(2);

            coords[0]=7;
            coords[1]=8;

            out << coords;
            CPPUNIT_ASSERT(out.str() == "{7, 8}");
        }
    }

    void testFilledMap(ChunkInstanceMap const& cm)
    {
        std::ostringstream out;
        out<<cm;
        CPPUNIT_ASSERT(out.str() == "[0,0]:0 [0,10]:1 [0,40]:0 [0,60]:3 | [10,20]:0 [10,30]:1 | [20,20]:1 | ");
        out.str("");

        Coordinates coords(2);

        coords[0]=0;
        coords[1]=0;
        ChunkLocation next = cm.getNextChunkFor(coords);
        CPPUNIT_ASSERT(next->first[0]==0 && next->first[1] == 10 && next->second == 1);
        next = cm.getNextChunkFor(next->first);
        CPPUNIT_ASSERT(next->first[0]==0 && next->first[1] == 40 && next->second == 0);
        ChunkLocation prev = cm.getPrevChunkFor(next->first);
        CPPUNIT_ASSERT(prev->first[0]==0 && prev->first[1] == 10 && prev->second == 1);
        prev = cm.getPrevChunkFor(prev->first);
        CPPUNIT_ASSERT(prev->first[0]==0 && prev->first[1] == 0 && prev->second == 0);
        prev = cm.getPrevChunkFor(prev->first);
        CPPUNIT_ASSERT(prev.get() ==0);

        next = cm.getNextChunkFor(next->first);
        CPPUNIT_ASSERT(next->first[0]==0 && next->first[1] == 60 && next->second == 3);
        next = cm.getNextChunkFor(next->first);
        CPPUNIT_ASSERT(next.get() == 0);

        coords[0]=20;
        coords[1]=20;
        ChunkLocation chunk = cm.getChunkFor(coords);
        CPPUNIT_ASSERT(chunk->first[0]==20 && chunk->first[1]==20 && chunk->second == 1);
        next = cm.getNextChunkFor(chunk->first);
        CPPUNIT_ASSERT(next.get() == 0);
        next = cm.getPrevChunkFor(chunk->first);
        CPPUNIT_ASSERT(next.get() == 0);

        coords[0]=-1;
        next = cm.getChunkFor(coords);
        CPPUNIT_ASSERT(next.get()==0);
        next = cm.getNextChunkFor(coords);
        CPPUNIT_ASSERT(next.get()==0);
        next = cm.getPrevChunkFor(coords);
        CPPUNIT_ASSERT(next.get()==0);

        coords[0]=0;
        coords[1]=61;
        next = cm.getChunkFor(coords);
        CPPUNIT_ASSERT(next.get()==0);
    }

    void testChunkInstanceMap()
    {
        ChunkInstanceMap cm(2, 1);

        Coordinates coords(2);
        coords[0]=0;
        coords[1]=0;

        ChunkLocation c = cm.getChunkFor(coords);
        CPPUNIT_ASSERT(c.get()==0);
        c = cm.getNextChunkFor(coords);
        CPPUNIT_ASSERT(c.get()==0);
        c = cm.getPrevChunkFor(coords);
        CPPUNIT_ASSERT(c.get()==0);

        cm.addChunkInfo(coords, 0);

        coords[1]=10;
        cm.addChunkInfo(coords, 1);

        coords[1]=40;
        cm.addChunkInfo(coords, 0);

        coords[1]=60;
        cm.addChunkInfo(coords, 3);

        coords[0]=10;
        coords[1]=20;
        cm.addChunkInfo(coords, 0);

        coords[0]=20;
        coords[1]=20;
        cm.addChunkInfo(coords, 1);

        coords[0]=10;
        coords[1]=30;
        cm.addChunkInfo(coords, 1);

        testFilledMap(cm);

        boost::shared_ptr<SharedBuffer> serialized = cm.serialize();
        CPPUNIT_ASSERT(serialized->getSize() == cm.getBufferedSize());

        ChunkInstanceMap cm2(2, 1);
        cm2.merge(serialized);
        testFilledMap(cm2);

        ChunkInstanceMap cm3(2, 1);
        coords[0]=10;
        coords[1]=50;
        cm3.addChunkInfo(coords, 7);
        coords[1]=80;
        cm3.addChunkInfo(coords, 8);

        cm2.merge(cm3.serialize());

        coords[0]=10;
        coords[1]=30;

        c= cm2.getChunkFor(coords);
        CPPUNIT_ASSERT(c->second==1);
        c= cm2.getNextChunkFor(c->first);
        CPPUNIT_ASSERT(c->second==7);
        c= cm2.getNextChunkFor(c->first);
        CPPUNIT_ASSERT(c->second==8);
        c= cm2.getNextChunkFor(c->first);
        CPPUNIT_ASSERT(c.get() == NULL);

        testCoordinateStreaming();
    }
};

CPPUNIT_TEST_SUITE_REGISTRATION(AuxTests);

#endif
