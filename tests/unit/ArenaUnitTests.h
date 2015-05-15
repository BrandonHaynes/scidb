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

#ifndef ARENA_UNIT_TESTS
#define ARENA_UNIT_TESTS

/****************************************************************************/

#include <cppunit/TestFixture.h>
#include <cppunit/extensions/HelperMacros.h>
#include <util/PointerRange.h>
#include <util/arena/String.h>
#include <util/arena/Vector.h>
#include <util/arena/Deque.h>
#include <util/arena/List.h>
#include <util/arena/Set.h>
#include <util/arena/Map.h>
#include <util/arena/UnorderedSet.h>
#include <util/arena/UnorderedMap.h>

/****************************************************************************/
namespace scidb { namespace arena {
/****************************************************************************/

/**
 *  Implements a suite of unit tests for the arena custom allocator library.
 */
struct ArenaTests : public CppUnit::TestFixture
{
    struct Custom             {~Custom() throw()   {}};
    struct Throws1            {Throws1() throw(int){throw 1;}};
    struct Throws2:Allocated  {Throws2() throw(int){throw 2;}};
    struct Throws3:Throws2    {};

                    void      setUp();
                    void      tearDown();

                    void      test();
                    void      testOptions();
                    void      testFinalizer();
                    void      testGlobalNew();
                    void      testRootArena();
                    void      testLimitedArena();
                    void      testScopedArena();
                    void      testLeaArena();
                    void      testSharedPtr();
                    void      testLimiting();
                    void      testStringConcat();
                    void      testManualAuto();
                    void      anExample();

                    void      arena     (Arena&);
                    void      limiting  (Arena&);
                    void      allocator (Arena&);
                    void      alignment (Arena&);
                    void      containers(Arena&);
                    void      randomized(Arena&);
                    void      direct    (Arena&,size_t);
    template<class> void      opnew     ();
    template<class> void      opnew     (Arena&);
    template<class> void      scalars   (Arena&);
    template<class> void      vectors   (Arena&,count_t);
    template<class> void      nesting   (Arena&);
    template<class> void      container (Arena&);
    template<class> void      destroy0  (Arena&);

    static          void      custom(void*){}

    CPPUNIT_TEST_SUITE(ArenaTests);
    CPPUNIT_TEST      (test);
    CPPUNIT_TEST      (testOptions);
    CPPUNIT_TEST      (testFinalizer);
    CPPUNIT_TEST      (testGlobalNew);
    CPPUNIT_TEST      (testRootArena);
    CPPUNIT_TEST      (testLimitedArena);
    CPPUNIT_TEST      (testScopedArena);
    CPPUNIT_TEST      (testLeaArena);
    CPPUNIT_TEST      (testSharedPtr);
    CPPUNIT_TEST      (testLimiting);
    CPPUNIT_TEST      (testStringConcat);
    CPPUNIT_TEST      (testManualAuto);
    CPPUNIT_TEST      (anExample);
    CPPUNIT_TEST_SUITE_END();
};

/**
 *  An empty test placeholder that does nothing at all.
 */
void ArenaTests::test()
{}

/**
 *  A quick example of how we use the 'named parameter idiom' to initialize an
 *  instance of class Options.
 */
void ArenaTests::testOptions()
{
    cout << Options("A").pagesize(1*KiB).threading(false) << endl;
    cout << Options("B").resetting(true).threading(true)  << endl;
}

/**
 *  Check that the finalizer() function is working correctly:
 *
 *   types with trivial destructors      : 0
 *   types derived from arena::Allocated : arena::allocated
 *   all other types 't'                 : &finalize<t>
 *
 *  The first two  represent an optimization: it is not that &finalize<double>
 *  does not work, for example, but rather that saving this pointer in a block
 *  wastes space and takes longer to invoke.
 */
void ArenaTests::testFinalizer()
{
    CPPUNIT_ASSERT(finalizer<int>()       == 0);
    CPPUNIT_ASSERT(finalizer<char>()      == 0);
    CPPUNIT_ASSERT(finalizer<double>()    == 0);
    CPPUNIT_ASSERT(finalizer<Throws1>()   == 0);
    CPPUNIT_ASSERT(finalizer<Throws2>()   == arena::allocated);
    CPPUNIT_ASSERT(finalizer<Throws3>()   == arena::allocated);
    CPPUNIT_ASSERT(finalizer<Allocated>() == arena::allocated);
    CPPUNIT_ASSERT(finalizer<Custom>()    == finalizer_t(&finalize<Custom>));
    CPPUNIT_ASSERT(finalizer<string>()    == finalizer_t(&finalize<string>));
}

/**
 *  Check that the arena::Allocator<> template models the basic std::allocator
 *  interface.
 */
void ArenaTests::allocator(Arena& a)
{
    Allocator<void>    v;                                // void
    Allocator<int>     i(&a);                            // Arena&
    Allocator<int>     j(i);                             // const Allocator&
    Allocator<double>  d(i);                             // const Allocator&

    assert(i == j);                                      // operator==()
    assert(!(i != j));                                   // operator!=()

    int x = 3; int const y = 3;

    v = v;                                               // operator=()
    i = j = d;                                           // operator=()

    CPPUNIT_ASSERT(i==j && j==d);                        // operator==()
    CPPUNIT_ASSERT(&x == i.address(x));                  // address()
    CPPUNIT_ASSERT(&y == i.address(y));                  // address() const
}

/**
 *  Check that the usual global new/delete operators are still availlable. The
 *  arena library introduces a plethora of overloaded new and delete operators
 *  and we want to know that none of these hide the normal global versions.
 */
void ArenaTests::testGlobalNew()
{
    opnew<int>      ();
    opnew<char>     ();
    opnew<double>   ();
    opnew<Throws1>  ();
    opnew<Throws2>  ();
    opnew<Throws3>  ();
    opnew<Allocated>();
    opnew<Custom>   ();
    opnew<string>   ();
}

/**
 *  Put class RootArena through its paces.
 */
void ArenaTests::testRootArena()
{
    arena(*newArena(Options()));
}

/**
 *  Put class LimitedArena through its paces.
 */
void ArenaTests::testLimitedArena()
{
    arena(*newArena(Options("limited 1").limit(1*GiB)));
    arena(*newArena(Options("limited 2").limit(1*GiB).debugging(true)));
}

/**
 *  Put class ScopedArena through its paces.
 */
void ArenaTests::testScopedArena()
{
    arena(*newArena(Options("scoped 1").resetting(true)));
    arena(*newArena(Options("scoped 2").resetting(true).pagesize(0)));
    arena(*newArena(Options("scoped 3").resetting(true).pagesize(0) .debugging(true)));
    arena(*newArena(Options("scoped 4").resetting(true).pagesize(96)                .threading(true)));
    arena(*newArena(Options("scoped 5").resetting(true).pagesize(96).debugging(true).threading(true)));
}

/**
 *  Put class LeaArena through its paces.
 */
void ArenaTests::testLeaArena()
{
    arena(*newArena(Options("lea 1").resetting(true).recycling(true).pagesize(0)));
    arena(*newArena(Options("lea 2").resetting(true).recycling(true).pagesize(96)));
    arena(*newArena(Options("lea 3").resetting(true).recycling(true).pagesize(10*KiB)));
    arena(*newArena(Options("lea 4").resetting(true).recycling(true).pagesize(64*MiB)));
}

/**
 *  Verify that all 6 of the usual globally available variants of operator new
 *  together with their associated overloads for operator delete are all still
 *  available at the given type.
 *
 *  On some platforms, operator new[]() saves the array length at the front or
 *  back of the allocation,  so we guard the placement target 't' with padding
 *  either side, just in case.
 */
template<class type>
void ArenaTests::opnew()
{
    char prePad [16];                                    // Guard with padding
    char t      [sizeof(type)];                          // Placement target
    char postPad[16];                                    // Guard with padding

    try{delete   new          type;}    catch(...){}     // Regliar
    try{delete   new(nothrow) type;}    catch(...){}     // 'nothrow'
    try{         new(&t)      type;}    catch(...){}     // Placement
    try{delete[] new          type[1];} catch(...){}     // Array
    try{delete[] new(nothrow) type[1];} catch(...){}     // Array nothrow
    try{         new(&t)      type[1];} catch(...){}     // Array placement

    (void)prePad;(void)postPad;                          // Silence warnings
}

/**
 *  Take the arena 'a' through all of the tests we have.
 */
void ArenaTests::arena(Arena& a)
{
    direct          (a,0);
    direct          (a,1);
    direct          (a,8);

    opnew<int>      (a);
    opnew<char>     (a);
    opnew<double>   (a);
    opnew<Throws1>  (a);
    opnew<Throws2>  (a);
    opnew<Throws3>  (a);
    opnew<Custom>   (a);
    opnew<string>   (a);
    opnew<Allocated>(a);

    allocator       (a);
    alignment       (a);
    containers      (a);
    randomized      (a);

    std::cout << a << std::endl;
}

/**
 *  Test the allocate()/recycle()/destroy() interfaces directly, without going
 *  through operator new. Notice that 'simple' allocations - those that do not
 *  supply a finalizer - are returned to the arena by calling 'recycle', while
 *  'complex' allocations are returned by calling 'destroy'.  Reversing either
 *  should lead to an assertion firing in either 'recycle' or 'destroy'.
 */
void ArenaTests::direct(Arena& a,size_t n)
{
    a.reset(); CPPUNIT_ASSERT(a.allocated() == 0);

 // Trivial allocations:

    a.recycle(a.allocate(n,0,                       0)  );
    a.destroy(a.allocate(n,custom,                  0)  );
    a.destroy(a.allocate(n,allocated,               0)  );
    a.destroy(a.allocate(n,finalizer<string>(),     0)  );
    a.destroy(a.allocate(n,finalizer<Custom>(),     0)  );
    a.destroy(a.allocate(n,finalizer<Allocated>(),  0)  );

 // Scalar allocations:

    a.recycle(a.allocate(n));

    a.recycle(a.allocate(n,0                         )  );
    a.destroy(a.allocate(n,custom                    ),1);
    a.destroy(a.allocate(n,allocated                 ),0);
    a.destroy(a.allocate(n,finalizer<string>()       ),0);
    a.destroy(a.allocate(n,finalizer<Custom>()       ),1);
    a.destroy(a.allocate(n,finalizer<Allocated>()    ),0);

    a.recycle(a.allocate(n,0,                       1)  );
    a.destroy(a.allocate(n,custom,                  1),1);
    a.destroy(a.allocate(n,allocated,               1),0);
    a.destroy(a.allocate(n,finalizer<string>(),     1),0);
    a.destroy(a.allocate(n,finalizer<Custom>(),     1),1);
    a.destroy(a.allocate(n,finalizer<Allocated>(),  1),0);

// Array allocations:

    a.recycle(a.allocate(n,0,                       2)  );
    a.destroy(a.allocate(n,custom,                  2),2);
    a.destroy(a.allocate(n,allocated,               2),0);
    a.destroy(a.allocate(n,finalizer<string>(),     2),0);
    a.destroy(a.allocate(n,finalizer<Custom>(),     2),2);
    a.destroy(a.allocate(n,finalizer<Allocated>(),  2),0);

    a.reset(); CPPUNIT_ASSERT(a.allocated() == 0);
}

/**
 *  Allocate an object of type 't', whose constructor may throw, and destroy
 *  the resulting allocation.
 */
template<class t>
void ArenaTests::scalars(Arena& a)
{
    a.reset(); CPPUNIT_ASSERT(a.allocated() == 0);

    try
    {
        destroy(a,::new(a,finalizer<t>()) t);
    }
    catch (int)
    {}

    a.reset(); CPPUNIT_ASSERT(a.allocated() == 0);
}

/**
 *  Allocate a vector of type 't', whose element constructors may throw, and
 *  destroy the resulting allocation.
 */
template<class t>
void ArenaTests::vectors(Arena& a,count_t n)
{
    a.reset(); CPPUNIT_ASSERT(a.allocated() == 0);

    try
    {
        destroy(a,newVector<t>(a,n));
    }
    catch (int)
    {}

    a.reset(); CPPUNIT_ASSERT(a.allocated() == 0);
}

/**
 *  Check that destroying and recycling a null pointer do nothing, just as for
 *  operator delete.
 */
template<class t>
void ArenaTests::destroy0(Arena& a)
{
    a.recycle(static_cast<t*>(0));
    a.destroy(static_cast<t*>(0));
    destroy(a,static_cast<t*>(0));
    destroy(a,static_cast<const t*>(0));
}

/**
 *  Check that various scalar and vector allocations of 't's work as expected.
 */
template<class t>
void ArenaTests::opnew(Arena& a)
{
    scalars <t>(a);
    vectors <t>(a,0);
    vectors <t>(a,1);
    vectors <t>(a,2);
    vectors <t>(a,4);
    destroy0<t>(a);
}

/**
 *  Check that the managed container classes are working correctly.
 */
void ArenaTests::containers(Arena& a)
{
 // We want managed versions of containers in what follows:

    using namespace managed;

    container<string                     >(a);

    container<set                 <char> >(a);
    container<list                <char> >(a);
    container<deque               <char> >(a);
    container<vector              <char> >(a);
    container<multiset            <char> >(a);
    container<basic_string        <char> >(a);
    container<unordered_set       <char> >(a);
    container<unordered_multiset  <char> >(a);

    container<set               <double> >(a);
    container<list              <double> >(a);
    container<deque             <double> >(a);
    container<vector            <double> >(a);
    container<multiset          <double> >(a);
    container<basic_string      <double> >(a);
    container<unordered_set     <double> >(a);
    container<unordered_multiset<double> >(a);

    nesting<set                 <string> >(a);
    nesting<list                <string> >(a);
    nesting<deque               <string> >(a);
    nesting<vector              <string> >(a);
    nesting<multiset            <string> >(a);
 // nesting<unordered_set       <string> >(a); // No scoped alloc support yet
 // nesting<unordered_multiset  <string> >(a); // No scoped alloc support yet
}

/**
 *  Randomly allocate and recycle a large number of blocks of arbitrary sizes
 *  from the arena 'a'.
 *
 *  Paul suggested the technique used here of multiplying and dividing by two
 *  primes as a cheap means of determinstically synthesizing a random-ish list
 *  of trials.
 */
void ArenaTests::randomized(Arena& a)
{
    std::vector<void*> v;                                // Live allocations

    for (size_t i=0; i!=100000; ++i)                     // For each 'trial'
    {
        size_t n = (i * 7561) % 17;                      // ...pseudo random

        if (n % 2 == 0)                                  // ...is even?
        {
            v.push_back(a.allocate(n));                  // ....allocate
        }

        if (n % 5 == 0 && !v.empty())                    // ...try freeing?
        {
            n %= v.size();                               // ....item to free
            a.recycle(v[n]);                             // ....so recycle it
            v.erase  (v.begin() + n);                    // ....drop from list
        }
    }

    while (!v.empty())                                   // For all remaining
    {
        a.recycle(v.back());                             // ...recycle block
        v.pop_back();                                    // ...and remove it
    }
}

/**
 *  Check that the given container works ok.  Not a very extensive test, but
 *  verifies that the various constructors are working correctly when passed
 *  an arena both implicitly and explicitly.
 */
template<class container_t>
void ArenaTests::container(Arena& a)
{
    typename container_t::value_type e[] = {'A','B'};    // Element sequence
    {
        container_t c1;                                  // ...default
        container_t c2(c1);                              // ...copy
        container_t c3(c2,&a);                           // ...copy (extended)
        container_t c4(e,e+SCIDB_SIZE(e));               // ...fill
        container_t c5(c4.begin(),c4.end());             // ...range
        container_t c6(&a);                              // ...allocator
        container_t c7(&a,e,e+SCIDB_SIZE(e));            // ...allocator fill
        container_t c8(&a,c4.begin(),c4.end());          // ...allocator range

        swap(c4,c5);                                     // Check swap works
        swap(c7,c8);                                     // And for allocators

        CPPUNIT_ASSERT(c1 != c8);                        // Check they differ

        c1 = c2 = c3 = c4 = c5 = c6 = c7 = c8;           // Check assignment

        CPPUNIT_ASSERT(c1 == c8);                        // Check they match

        cout<<'{';insertRange(cout,c1,',');cout<<'}';    // Check iteration
    }

    a.reset();                                           // Reset the arena
}

/**
 *  Check that the given arena is aligning its allocations correctly. Assumes
 *  a little-endian memory organization; so sue me.
 */
void ArenaTests::alignment(Arena& a)
{
    struct {void operator()(const void* p)               // Local function
    {
        CPPUNIT_ASSERT(reinterpret_cast<uintptr_t>(p) % sizeof(alignment_t) == 0);
    }}  aligned;                                         // The local function

    for (size_t i=1; i!=sizeof(alignment_t) + 1; ++i)    // For various sizes
    {
       {void* p = a.malloc(i)  ;          aligned(p);a.free(p,i);}
       {void* p = a.calloc(i)  ;          aligned(p);a.free(p,i);}
       {void* p = a.malloc(i,1);          aligned(p);a.free(p,i);}
       {void* p = a.allocate(i);          aligned(p);a.recycle(p);}
       {void* p = new(a) Allocated;       aligned(p);a.destroy(p);}
       {void* p = a.allocate(i,custom);   aligned(p);a.destroy(p);}
       {void* p = a.allocate(i,custom,2) ;aligned(p);a.destroy(p);}
    }
}

/**
 *  Check that container_t supports the scoped allocator model of C++11.
 */
template<class container_t>
void ArenaTests::nesting(Arena& a)
{
    ArenaPtr        p(newArena("bogus"));                // A local arena
    managed::string s(p,"some string");                  // A local string
    container_t     c(&a,&s,&s+1);                       // Copy to 's' to 'a'

    CPPUNIT_ASSERT(c.begin()->get_allocator().arena() == &a);
}

/**
 *  Check that allocate_shared() is wired up and working correctly.
 */
void ArenaTests::testSharedPtr()
{
    ArenaPtr a(getArena());                              // The current arena

 /* A number of ways of saying essentially the same thing. Note that the first
    two calls bind to boost::allocate_shared() while the last two bind instead
    to arena::allocate_shared(). Guess which variation we prefer...*/

    shared_ptr<int> w(allocate_shared<int,Allocator<int> >(a,78));   // theirs
    shared_ptr<int> x(boost::allocate_shared<int>         (Allocator<int>(a),78));
    shared_ptr<int> y(arena::allocate_shared<int>         (*a,78));  // ours
    shared_ptr<int> z(allocate_shared<int>                (*a,78));  // ours

    cout << *z << ": extensive testing shows that allocate_shared() is AOK.\n";
}

/**
 *  Check that the memory limiting mechanism is work correctly.
 */
void ArenaTests::testLimiting()
{
    ArenaPtr a(newArena(Options("100").limit(100)));     // New limited arena

    try
    {
        a->recycle(a->allocate(88));                     // This succeeds
        a->recycle(a->allocate(101));                    // But this fails...
    }
    catch (arena::Exhausted& e)
    {
        cout << e << endl;                               // ...and jumps here
    }

    a->recycle(a->allocate(10));                         // This succeeds too
}

/**
 *  Check that managed string concatenation is working correctly.
 *
 *  Managed string concatentation broke due to bug #9064 in Boost 1.54:
 *
 *      https://svn.boost.org/trac/boost/ticket/9064
 *
 *  This bug was fixed in Boost 1.55.
 */
void ArenaTests::testStringConcat()
{
    using namespace managed;                             // Arena-aware cntnrs

    ArenaPtr a(getArena());                              // The current arena

    string s(a,"s");                                     // A managed string
    string t(s + s);                                     // Crashed in v1.54
    cout << "test string concatenation: " << t << endl;  // Check it works ok
}

/**
 *  Test the ability of newScalar() and newVector() to optionally register (or
 *  skip registration of) a finalizer that will be automatically applied to an
 *  allocation when it is ventually destroyed.
 */
void ArenaTests::testManualAuto()
{
    using std::string;                                   // For string object

    ArenaPtr A(getArena());                              // The current arena
    Arena&   a(*A);                                      // A reference to it
    size_t   n(a.allocated());                           // Record allocations

    destroy(a,newScalar<int>    (a, 3           )  );    // Default = automatic
    destroy(a,newVector<int>    (a, 3           )  );    // Default = automatic
    destroy(a,newScalar<string> (a,"3",manual   ),1);    // Manual    scalar
    destroy(a,newScalar<string> (a,"3",automatic)  );    // Automatic scalar
    destroy(a,newVector<string> (a, 3 ,manual   ),3);    // Manual    vector
    destroy(a,newVector<string> (a, 3 ,automatic)  );    // Automatic vector

 /* Check that the automatic cleanup of a partiallly constructed vector of
    elements works in both manual and automatic finalization mode...*/

    try{newVector<Throws1>(a,3);}           catch(int){} //
    try{newVector<Throws2>(a,3);}           catch(int){} //
    try{newVector<Throws1>(a,3,manual);}    catch(int){} //
    try{newVector<Throws2>(a,3,manual);}    catch(int){} //
    try{newVector<Throws1>(a,3,automatic);} catch(int){} //
    try{newVector<Throws2>(a,3,automatic);} catch(int){} //

    CPPUNIT_ASSERT(a.allocated() == n);                  // We cleaned up ok?
}

/**
 *  Print a line on entry to each test case to make the output more readable.
 */
void ArenaTests::setUp()
{
    cout << endl;                                        // Print a blank line
}

/**
 *  If the current arena has live allocations remaining, then one of the above
 *  tests must have leaked memory and we format a message and sound the alarm.
 */
void ArenaTests::tearDown()
{
    ArenaPtr a(getArena());                              // The current arena

    if (a->allocations() != 0)                           // Live allocations?
    {
        ostringstream s;                                 // ...message buffer

        s << "leaks detected in arena " << *a;           // ...format message

        CPPUNIT_FAIL(s.str());                           // ...sound the alarm
    }
}

/**
 *  An example of how one might use Arenas within a SciDB operator.
 */
void ArenaTests::anExample()
{
    cout << "An Example ==================================================\n";

 /* We will be making implicit use of the 'managed' versions of the containers
    in what follows...*/

    using namespace managed;                             // Arena-aware cntnrs

 /* Imagine that we are at the top of the main entry point for some operator
    'Foo'.

    ...PhysicalFoo::execute( ... boost::shared_ptr<Query> const& query) {

    In practice, 'parent' would either be passed into the execute() function
    via the query context, for example:

        ArenaPtr parent(query->getArena());

    or else have already been installed in the operator object itself by the
    executor:

        ArenaPtr parent(this->_arena);

    but either way this would give us an arena with a preset limit already in
    place. But for this example, let's just build the arena explicitly...*/

        ArenaPtr parent(newArena(Options("Foo").limit(1*GiB)));

 /* Imagine further that wish we to track  two distinct groups of allocations
    made from within the call to Foo, say groups 'A' and 'B', and furthermore,
    that, for reasons of our own, we wish to prevent group 'B' from exceeding,
    say, 1MiB. So, we attach two more local arenas to our parent, like so...*/

    ArenaPtr A(newArena(Options("A")));
    ArenaPtr B(newArena(Options("B").limit(1*MiB)));

 /* One code path within Foo allocates from 'A' using various standard library
    containers. By and large, the managed containers have identical interfaces
    to their standard library counterparts...*/
    {
        set<int> u(A);                                   // Allocates from A

        u.insert(7);

     /* ...though they also support some C++11 features such as emplacement and
        move semantics...*/

        u.emplace(8);

        vector<string> v(A,3);                           // Allocates from A

        v[0] = "alex";
        v[1] = "tigor";
        v[2] = "donghui";

     /* Let's check that the mapped strings have indeed picked up the correct
        arena A, shall we?  There are a number of ways of saying  essentially
        the same thing here...*/

        assert(v.get_allocator()    == A);
        assert(v[0].get_allocator() == A);
        assert(v[1].get_allocator().arena() == A.get());
        assert(v[2].get_allocator() == v.get_allocator());

     /* There's some magic going on here behind the scenes that makes this all
        'just work'. Where we wrote 'vector' above, we could just as well have
        chosen a list, deque, set, multiset, map, multimap, or string.  Sadly,
        however, the boost unordered containers are not yet in on the game...*/

        unordered_map<int,double> m(A);                  // Fine, no problem

        m[0] = 7.0;
        m[1] = 7.8;

        unordered_map<int,string> n(A);                  // Rather Less fine...

        n[0] = "marilyn";
        n[1] = "james";

        assert(n[0].get_allocator() != A);               // Argh!!!
        assert(n[1].get_allocator() == getArena());      // In the root!

     /* As you can see,  the unordered containers have not yet been taught how
        to propagate their allocators on down into their elements recursively;
        rather, the element strings are relying on the fact that their default
        constructors are grabbing the default, global,root arena. It's not the
        end of the world, but we can do better if we are willing to supply the
        element allocators explicitly...*/

        n.emplace(std::make_pair(2,string(A,"paul")));   // Specify explicitly

        assert(n[2].get_allocator() == A);               // Ah, that's better!

     /* Pretty it ain't; i'm currently looking into other alternatives...*/
    }

 /* The other code path allocates from 'B', and we might perhaps want to wrap
    this with a try block...*/
    {
        vector<double> v(B);                             // Allocates from B

        try
        {
            v.push_back(7);                              // ...as per usual

         /* Simple objects are allocated with a placement new operator...*/

            double*    pDbl = new(*B)                     double(3.1415927);
            Allocated* pAll = new(*B)                     Allocated();
            string*    pStr = new(*B,finalizer<string>()) string(B,"string");

        /* ...or, if you prefer, the overloaded template function newScalar(),
           which automatically registers any non-trivial destructor for you..*/

            string*    pStr2= newScalar<string>(*B,B,"another string");

         /* But deletion works differently: you must either *recycle* objects
            with trivial destructors...*/

            B->recycle(pDbl);                            // No finalization

         /* ...or else *destroy* objects with non-trivial destructors...*/

            B->destroy(pAll);                            // And finalizes it

         /* The good news, however, is that if you get this wrong you will get
            an assertion. In general, however, we prefer to call the destroy()
            helper function,  which figures this  out for you statically using
            some rather nifty template meta-programming...*/

            destroy(*B,pStr);                            // Do the right thing
            destroy(*B,pStr2);                           // Do the right thing

         /* You can also allocate vectors...*/

            pDbl = newVector<double>(*B,2);              // A 2 element vector
            pDbl[0] = 7;
            pDbl[1] = 8;

         /* ..and these, too, are cleaned up with a call destroy()...*/

            destroy(*B,pDbl);                            // Destroy vector

         /* Question: what happens if we try to allocate more than our arena
            is set up to allow? */

            v.resize(1000000);                           // Exceeds B's limit
        }
        catch (arena::Exhausted& e)
        {
         /* Answer: a recoverable exception is thrown...*/

            cout << e << endl;                           // ...so, recover...
        }
    }

 /* There are a number of different Arena implementations, each with different
    performance characteristics.  A particularly interesting implementation is
    class ScopedArena - sometimes known as a Zone, Region, or Stack Alocator -
    that defers requests to recycle  memory in favour of freeing everything it
    has ever allocated all at once. Used carefully, this can often out-perform
    the standard system allocator...*/
    {
        ArenaPtr C(newArena(Options("C").resetting(true)));

        map<int,int> m(C);

        m[1] = 2; m[2] = 3; m[3] = 4;

        C->malloc(78);
        // ...
        C->calloc(387,2);
        // ...
        double* p = newVector<double>(*C,8483);
        // ...
        destroy(*C,p);

     /* If you're following along in the debugger, observe that C's memory is
        flushed in one fell swoop at this point. If not, well.., just take my
        word for it...*/
    }

 /* At any point we can ask our arenas how they are doing...*/

    if (A->available() > 1*GiB)
    {
        //...
    }

    if (B->allocated() < 1*GiB)
    {
        //...
    }

 /* And, of course, we can always inquire after the parent...*/

    if (A->parent()->available() > 1*GiB)
    {
        //...
    }

 /* We can also send a snapshot of the arena's current allocation statistics
    off to the system resource monitor...*/

    parent->checkpoint("PhysicalFoo.cpp checkpoint");

    cout << "=============================================================\n";
}

/****************************************************************************/
}}
/****************************************************************************/

CPPUNIT_TEST_SUITE_REGISTRATION(scidb::arena::ArenaTests);

/****************************************************************************/
#endif
/****************************************************************************/
