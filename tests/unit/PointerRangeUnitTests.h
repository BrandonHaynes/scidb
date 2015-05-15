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

#ifndef POINTER_RANGE_UNIT_TESTS
#define POINTER_RANGE_UNIT_TESTS

/****************************************************************************/

#include <cppunit/TestFixture.h>
#include <cppunit/extensions/HelperMacros.h>
#include <boost/assign/list_of.hpp>
#include <util/PointerRange.h>

/****************************************************************************/
#define test CPPUNIT_ASSERT
/****************************************************************************/

class PointerRangeTests : public CppUnit::TestFixture
{
 private:
    typedef PointerRange<      char>    chars;
    typedef PointerRange<const char>   cchars;

 private:
    static  string            asString(cchars,char);
    static  string            m( chars r)           {return asString(r,'m');}
    static  string            c(cchars r)           {return asString(r,'c');}
    static  string            s( chars r)           {return asString(r,'m');}
    static  string            s(cchars r)           {return asString(r,'c');}

 public:
            void              conversions();
            void              generics();
            void              comparisons();

 public:
    CPPUNIT_TEST_SUITE(PointerRangeTests);
    CPPUNIT_TEST(conversions);
    CPPUNIT_TEST(generics);
    CPPUNIT_TEST(comparisons);
    CPPUNIT_TEST_SUITE_END();
};

/**
 * Format the sequence 'r' as a string of the form "<c> ( <r> )".
 */
string PointerRangeTests::asString(cchars r,char c)
{
    std::ostringstream o;
    o << c << '(';
    insertRange(o,r);
    o << ')';
    return o.str();
}

/**
 * Strategy: initialize constant and mutable arrays, strings, and vectors of
 * characters, and pass these in all legal combinations to functions:
 *
 *  m() - a function that requires a mutable range
 *  c() - a function that requires a constant range
 *  s() - a function that is loverloaded to accept either
 *
 * each of which formats its argument as a string that we can then compare
 * directly against a string literal for equality.
 */
void PointerRangeTests::conversions()
{
    using boost::assign::list_of;

    char                ma[] = {'m','a'};
    char          const ca[] = {'c','a'};
    string              ms   = "ms";
    string        const cs   = "cs";
    vector<char>        mv   = list_of('m')('v');
    vector<char>  const cv   = list_of('c')('v');
     chars              mr   =  chars(2,(char*)"mr");
    cchars              cr   = cchars(2,"cr");

    test(m(ma)               == "m(ma)");
    test(m(ms)               == "m(ms)");
    test(m(mv)               == "m(mv)");
    test(m(mr)               == "m(mr)");
  //test(m(ca)               == "m(ca)");                // illegal
  //test(m(cs)               == "m(cs)");                // illegal
  //test(m(cv)               == "m(cv)");                // illegal
  //test(m(cr)               == "m(cr)");                // illegal
    test(c(ma)               == "c(ma)");
    test(c(ms)               == "c(ms)");
    test(c(mv)               == "c(mv)");
    test(c(mr)               == "c(mr)");
    test(c(ca)               == "c(ca)");
    test(c(cs)               == "c(cs)");
    test(c(cv)               == "c(cv)");
    test(c(cr)               == "c(cr)");
  //test(s(ma)               == "?(ma)");                // ambiguous
    test(s( chars(ma))       == "m(ma)");                //  ok
    test(s(cchars(ma))       == "c(ma)");                //  ok
    test(s(pointerRange(ma)) == "m(ma)");                //  ok
  //test(s(ms)               == "?(ms)");                // ambiguous
    test(s( chars(ms))       == "m(ms)");                //  ok
    test(s(cchars(ms))       == "c(ms)");                //  ok
  //test(s(mv)               == "?(mv)");                // ambiguous
    test(s( chars(mv))       == "m(mv)");                //  ok
    test(s(cchars(mv))       == "c(mv)");                //  ok
    test(s(mr)               == "m(mr)");
    test(s(ca)               == "c(ca)");
    test(s(cs)               == "c(cs)");
    test(s(cv)               == "c(cv)");
    test(s(cr)               == "c(cr)");
}

/**
 *  Put the generic range manipulation functions through their paces.
 */
void PointerRangeTests::generics()
{
    cchars const r(7,"ABCDEFG");

  //test(m(nullTerminated("ABC"))   == "?(ABC)");        // illegal
    test(c(nullTerminated("ABC"))   == "c(ABC)");
    test(s(nullTerminated("ABC"))   == "c(ABC)");
    test(s(take(r))                 == "c()");
    test(s(take(r,1))               == "c(A)");
    test(s(take(r,7))               == "c(ABCDEFG)");
    test(s(drop(r))                 == "c(ABCDEFG)");
    test(s(drop(r,1))               == "c(BCDEFG)");
    test(s(drop(r,0,1))             == "c(ABCDEF)");
    test(s(drop(r,1,1))             == "c(BCDEF)");
    test(s(subrange(r))             == "c()");
    test(s(subrange(r,4,3))         == "c(EFG)");
    test(s(shift(drop(r,0,1),+1))   == "c(BCDEFG)");
    test(s(shift(drop(r,1,0),-1))   == "c(ABCDEF)");
    test(s(grow(drop(r,1,1),+1,+1)) == "c(ABCDEFG)");
    test(s(grow(r,-1,-1))           == "c(BCDEF)");
}

/**
 *  Put the various comparison operators through their paces.
 */
void PointerRangeTests::comparisons()
{
    cchars a(7,"ABCDEFG");
    cchars b(8,"ABCDEFGK");
    cchars c(9,"ABCDEFGKL");

    test(a==a && b==b && c==c);
    test(a!=b && b!=c && a!=c);
    test(a< b && b< c && a< c);
    test(a<=b && b<=c && a<=c);

    swap(a,c);

    test(a> b && b> c && a> c);
    test(a>=b && b>=c && a>=c);

 /* Notice that the ranges being compared need not have exactly the same
    element type as one another...*/
    int x = 387;
    test(PointerRange<const int>(387)  == PointerRange<int>(x));
    test(PointerRange<const char>('X') == PointerRange<const int>('X'));
}

/****************************************************************************/
#undef test
/****************************************************************************/

CPPUNIT_TEST_SUITE_REGISTRATION(PointerRangeTests);

/****************************************************************************/
#endif
/****************************************************************************/
