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

#ifndef UTIL_ARENA_H_
#define UTIL_ARENA_H_

/****************************************************************************/

#include <new>                                           // For placement new
#include <iosfwd>                                        // For ostream
#include <boost/utility.hpp>                             // For noncopyable
#include <boost/make_shared.hpp>                         // For shared_ptr
#include <boost/type_traits.hpp>                         // For has_trivial...
#include <util/Utility.h>                                // For stackonly
#include <util/Platform.h>                               // For SCIDB_NORETURN

/****************************************************************************/
namespace scidb { namespace arena {
/****************************************************************************/

using boost::shared_ptr;                                 // A tracking pointer
using boost::noncopyable;                                // An RIIA base class

/****************************************************************************/

class Arena;                                             // An abstract arena
template<class>                                          // For each type 't'
class Allocator;                                         //  allocator of 't'
class Allocated;                                         // Auto finalizing
class Options;                                           // An arena args list
class Exhausted;                                         // An arena exception
class Checkpoint;                                        // An arena checkpoint

/****************************************************************************/

typedef char const*           name_t;                    // An arena name
typedef char                  byte_t;                    // A byte of memory
typedef size_t                count_t;                   // A 1d array length
typedef unsigned              features_t;                // A feature bitfield
typedef void                (*finalizer_t)(void*);       // A finalizer callback
typedef shared_ptr<Arena>     ArenaPtr;                  // A tracking pointer

/****************************************************************************/

const size_t                  unlimited = ~0 >> 4;       // Maximum allocation
const size_t                  alignment = sizeof(double);// Payload alignment
const finalizer_t             allocated = finalizer_t(1);// Special finalizer

/****************************************************************************/

ArenaPtr                      getArena();                // The current arena
ArenaPtr                      newArena(Options);         // Adds a new branch

/****************************************************************************/

template<class t> finalizer_t finalizer();               // Gives &finalize<t>
template<class t> void        finalize (void*);          // Calls t::~t()
template<class t> t*          newScalar(Arena&/*...*/);  // Creates a scalar
template<class t> t*          newVector(Arena&,count_t); // Creates a vector
template<class t> void        destroy  (Arena&,const t*,count_t = unlimited);

/****************************************************************************/

std::ostream&                 operator<<(std::ostream&,const Arena&);
std::ostream&                 operator<<(std::ostream&,const Options&);
std::ostream&                 operator<<(std::ostream&,const Allocated&);
std::ostream&                 operator<<(std::ostream&,const Exhausted&);

/****************************************************************************/

enum
{
    finalizing = 1,      ///< Supports automatic invocation of finalizers.
    recycling  = 2,      ///< Supports eager recycling of memory allocations.
    resetting  = 4,      ///< Supports deferred recycling of memory allocations.
    debugging  = 8,      ///< Pads allocations with guards and checks for leaks.
    locking    = 16      ///< Synchronizes access from across multiple threads.
};

/****************************************************************************/
/**
 *  @brief      Represents an abstract memory allocator.
 *
 *  @details    Class Arena forms the root  of a  hierarchy of abstract memory
 *              allocators.  The interface is designed to support the needs of
 *              both operator new() and the standard library containers, while
 *              still allowing for  a variety of interesting memory allocation
 *              strategies to  be implemented by subclasses that can be chosen
 *              from amongst at runtime, and that include support for:
 *
 *              - memory limiting
 *              - allocation monitoring
 *              - region-based allocation
 *              - per-class instance pooling
 *              - debugging support, with leak detection and memory painting
 *
 *              Arenas come in a number of 'flavours' according to the various
 *              features they choose to support:
 *
 *              - @b finalizing:  the caller can register a finalizer with the
 *              allocation - that is, a (pointer to a) function (usually a C++
 *              destructor, though it does not have to be) that the Arena will
 *              automatically apply to the allocation when it's later returned
 *              with a call to destroy(), or when the Arena is later reset:
 *  @code
 *                  p = new(arena,finalizer<X>()) X(...);// Register finalizer
 *                  ...
 *                  destroy(arena,p);                    // Also finalizes 'p'
 *  @endcode
 *              - @b recycling: the Arena implements the recycle() / destroy()
 *              member functions to eagerly recover the storage being returned
 *              and make it available for reuse in subsequent allocations. All
 *              finalizing arenas implement destroy() to invoke the registered
 *              finalizer, of course, but non-recycling arenas stop there, and
 *              instead defer the recycling of memory until the Arena is later
 *              reset.
 *
 *              - @b resetting:  the Arena puts allocations on a list and then
 *              recycles them en masse when reset() is called. This represents
 *              a simple minded form of garbage collection, and, in conjuction
 *              with using the stack to control the lifetime of arenas, yields
 *              a form of region-based memory allocation that can usually out-
 *              perform the default system allocator.
 *
 *              - @b limiting: the Arena is constructed with an upper limit on
 *              the total memory it can allocate before throwing an exception,
 *              and can report at any time on how much memory it has allocated
 *              and how much remains available for allocation.
 *
 *              - @b locking:  the Arena can synchronize access from more than
 *              one thread at the same time, perhaps because it embeds a mutex,
 *              for example.
 *
 *              - @b debugging:  the Arena adds guard areas to each allocation
 *              it performs,  validates their integrity when the allocation is
 *              later returned, and maintains a list of live allocations which
 *              it uses to track and dump memory leaks.
 *
 *              Arenas must support either recycling or resetting - to support
 *              neither would be to leak memory; supporting both is also quite
 *              possible, however.
 *
 *  @see        Class Allocator for more on use with the standard library.
 *
 *  @see        Class Allocated for more on automatic finalization.
 *
 *  @see        Class Exhausted for more on recovering from an out-of-memory
 *              exception.
 *
 *  @see        Class Options and function newArena() for more on how to create
 *              an Arena.
 *
 *  @see        Namespace managed for specializations  of many of the standard
 *              library containers that allocate their internal memory from an
 *              abstract Arena.
 *
 *  @see        http://trac.scidb.net/wiki/Development/OperatorMemoryManagement
 *              for more on the design and use of the SciDB arena library.
 *
 *  @author     jbell@paradigm4.com.
 */
class Arena : noncopyable
{
 public:                   // Attributes
    virtual name_t            name()                     const;
    virtual ArenaPtr          parent()                   const;
    virtual size_t            available()                const;
    virtual size_t            allocated()                const;
    virtual size_t            peakUsage()                const;
    virtual size_t            allocations()              const;
    virtual bool              supports(features_t)       const;
    virtual void              checkpoint(name_t = "")    const;
    virtual void              insert(std::ostream&)      const;

 public:                   // Operations
    virtual void*             allocate(size_t);
    virtual void*             allocate(size_t,finalizer_t);
    virtual void*             allocate(size_t,finalizer_t,count_t);
    virtual void              recycle(void*);
    virtual void              destroy(void*,count_t = unlimited);
    virtual void              reset();

 public:                   // Allocation
            void*             malloc(size_t);
            void*             calloc(size_t);
            void*             malloc(size_t,count_t);
            void*             calloc(size_t,count_t);
            char*             strdup(const char*);
            char*             strdup(const std::string&);
            void              free  (void*,size_t);

 public:                   // Implementation
    virtual void*             doMalloc(size_t)           = 0;
    virtual size_t            doFree  (void*,size_t)     = 0;

 protected:                // Implementation
            void              overflowed()               const SCIDB_NORETURN;
            void              exhausted(size_t)          const SCIDB_NORETURN;
                             ~Arena()                                          {}
};

/**
 *  @brief      Adapts an Arena to support the standard allocator interface.
 *
 *  @details    Class Allocator<t>  models the concept of a standard allocator
 *              by delegating to an abstract Arena,  enabling standard library
 *              containers such as std::vector, std::list, std::string, and so
 *              on, to allocate their internal storage from off the Arena.
 *
 *  @see        http://www.cplusplus.com/reference/memory/allocator for more
 *              on the standard allocator interface.
 *
 *  @see        The managed namespace for specialized versions of the standard
 *              library containers that work well with arenas.
 *
 *  @author     jbell@paradigm4.com.
 */
template<class type>
class Allocator
{
 public:                   // Supporting Types
    template<class t>
    struct  rebind            {typedef Allocator<t> other;};
    typedef size_t            size_type;
    typedef ptrdiff_t         difference_type;
    typedef type*             pointer;
    typedef type const* const_pointer;
    typedef type&             reference;
    typedef type const& const_reference;
    typedef type              value_type;

 public:                   // Construction
                              Allocator()                                      : _arena(getArena().get()){assert(consistent());}
                              Allocator(Arena* p)                              : _arena(p)               {assert(consistent());}
                              Allocator(const ArenaPtr& p)                     : _arena(p.get())         {assert(consistent());}
    template<class t>         Allocator(const Allocator<t>& a)                 : _arena(a.arena())       {assert(consistent());}

 public:                   // Operations
            Arena*            arena()                    const                 {return _arena;}
            count_t           max_size()                 const                 {return unlimited / sizeof(type);}
            type*             address(type& v)           const                 {return &v;}
      const type*             address(const type& v)     const                 {return &v;}
            bool              operator==(Allocator a)    const                 {return _arena == a._arena;}
            bool              operator!=(Allocator a)    const                 {return _arena != a._arena;}

 public:                   // Allocation
            void              construct(type* p,const type& v)                 {new(p) type(v);}
            void              destroy(type* p)                                 {finalize<type>(p);}
            type*             allocate(count_t c,const void* = 0)              {return static_cast<type*>(_arena->malloc(sizeof(type),c));}
            void              deallocate(type* p,count_t c)                    {assert(c <= max_size());  _arena->free(p,sizeof(type)*c) ;}

 private:                  // Implementation
            bool              consistent()               const                 {return _arena != 0;}

 private:                  // Representation
            Arena*            _arena;                    // The abstract arena
};

/**
 *  @brief      Specializes class Allocator<t> to describe arbitrary pointers.
 *
 *  @details    Class Allocator<void> specializes the Allocator template class
 *              to allow it to be applied to the type void, which the standard
 *              library containers, for reasons of their own, may occasionally
 *              need to do.  Without this specialization the application would
 *              fail because it would refer to such non types such as void&.
 *
 *              Its only real use, however, is as a mechansim for rebinding to
 *              an allocator of another, distinct, value type.
 *
 *  @see        http://stackoverflow.com/questions/7143812/why-does-allocator-in-c-provide-specialization-for-type-void.
 *
 *  @author     jbell@paradigm4.com.
 */
template<>
struct Allocator<void>
{
    template<class t>
    struct  rebind            {typedef Allocator<t> other;};
    typedef void              value_type;
    typedef void*             pointer;
    typedef void const* const_pointer;
};

/**
 *  @brief      Provides support for automatic finalization.
 *
 *  @details    Class Allocated provides a common base class for those classes
 *              with non-trivial destructors that are to be allocated from off
 *              an Arena. Its virtual destructor is an example of  a finalizer
 *              function for Allocated objects,  but is 'special' in the sense
 *              that its address is *known* to the arena library and so can be
 *              represented particularly efficiently; by comparison, a 'custom
 *              finalizer'- i.e. an arbitrary function of type void(*)(void) -
 *              requires an additional pointer to be saved within an allocated
 *              block.
 *
 *              The class overloads the new operator to make arena allocations
 *              more convenient:
 *  @code
 *                  struct Foo : arena::Allocated { ... };
 *
 *                  p = new(arena) Foo (...);
 *  @endcode
 *              but note that like every other arena allocation, the resulting
 *              pointer 'p' cannot and must not be deleted, or Bad Things Will
 *              Happen. Instead, we call the destroy() function:
 *  @code
 *               // delete p;                       // wrong!
 *                  destroy(arena,p);               // right
 *  @endcode
 *              Do not let the overloaded 'operator delete(void*,Arena&)' fool
 *              you:  the language simply provides no mechanism whatsoever for
 *              this to be invoked with a delete expression; rather, it exists
 *              to free memory in the event that the object under construction
 *              throws an exception from out of its constructor.
 *
 *              Notice also that there is no comparable overload for 'operator
 *              new []': again, there's just no portable way to implement this
 *              operator (see article below for the gory details). Instead, we
 *              have the function newVector() for this purpose:
 *  @code
 *                  Foo* p = newVector(arena,78);   // allocate off arena
 *                      ...
 *                  destroy(arena,p);               // return to the arena
 *  @endcode
 *              and, as always, we call destroy() to dispose of the result.
 *
 *              The remaining overloads exist merely to enable class Allocated
 *              and its descendants to still work with the global operator new
 *              as before: in other words, Allocated objects do not Have to be
 *              allocated off an Arena, they merely make doing so convenient.
 *
 *  @see        http://www.gotw.ca/publications/mill16.htm more on overloading
 *              the new and delete operators.
 *
 *  @see        http://www.scs.stanford.edu/~dm/home/papers/c++-new.html for a
 *              description of some of the difficulties in overloading the new
 *              and delete operators.
 *
 *  @author     jbell@paradigm4.com.
 */
class Allocated
{
 public:                   // Construction
    virtual                  ~Allocated()                                      {}

 public:                   // Allocation
            void*             operator new   (size_t n)                        {return ::operator new   (n);}
            void*             operator new   (size_t n,void* p)                {return ::operator new   (n,p);}
            void*             operator new   (size_t n,const std::nothrow_t& x){return ::operator new   (n,x);}
            void              operator delete(void* v)                         {return ::operator delete(v);}
            void              operator delete(void* v,void* p)                 {return ::operator delete(v,p);}
            void              operator delete(void* v,const std::nothrow_t& x) {return ::operator delete(v,x);}

 public:                   // Allocation
            void*             operator new   (size_t n,Arena& a)               {return a.allocate(n,allocated);}
            void              operator delete(void*  p,Arena& a)               {a.destroy(p,0);}

 public:                   // Operations
    virtual void              insert(std::ostream&)      const;
};

/**
 *  @brief      A union of the various possible Arena construction arguments.
 *
 *  @details    Class Options provides a sort of union of the many options and
 *              arguments with which an Arena can be constructed.  It uses the
 *              'named parameter idiom' to enable these options to be supplied
 *              by name in any convenient order. For example:
 *  @code
 *                  newArena(Options("A1").locking(true));
 *  @endcode
 *              creates an Arena named "A1" that also supports synchronization
 *              across multiple threads. In addition, collecting these options
 *              up into a structure allows the Arena factory functions such as
 *              newArena() to thread them down through the decorator chain and
 *              inheritance graph.
 *
 *  @see        http://www.parashift.com/c++-faq/named-parameter-idiom.html for
 *              a description of the 'named parameter idiom'.
 *
 *  @author     jbell@paradigm4.com.
 */
class Options
{
 public:                   // Construction
                              Options(name_t n = "");

 public:                   // Attributes
            name_t            name()               const                       {return _name;}       ///< The name of the Arena as it appears in a resource monitor report.
            size_t            limit()              const                       {return _limit;}      ///< An upper limit on the memory this Arena can allocate before throwing an Exhausted exception.
            size_t            pagesize()           const                       {return _psize;}      ///< The size of a memory page, for those arenas that allocate memory one fixed size page at a time.
            ArenaPtr          parent()             const                       {return _parent;}     ///< Specifies the next Arena in the parent chain.
            bool              finalizing()         const                       {return _finalizing;} ///< Request support for automatic invocation of finalizers.
            bool              recycling()          const                       {return _recycling;}  ///< Request support for eager recycling of allocations.
            bool              resetting()          const                       {return _resetting;}  ///< Request support for deferred recycling of allocations.
            bool              debugging()          const                       {return _debugging;}  ///< Request surrounding allocations with guards and checking for leaks.
            bool              locking()            const                       {return _locking;}    ///< Request support for concurrent access from multiple threads.
            features_t        features()           const                       {return _finalizing|_recycling|_resetting|_debugging|_locking;}

 public:                   // Operations
            Options&          name      (name_t n)                             {_name       = n;assert(consistent());return *this;}
            Options&          limit     (size_t l)                             {_limit      = l;assert(consistent());return *this;}
            Options&          pagesize  (size_t s)                             {_psize      = s;assert(consistent());return *this;}
            Options&          parent    (ArenaPtr p)                           {_parent     = p;assert(consistent());return *this;}
            Options&          finalizing(bool f)                               {_finalizing = f;assert(consistent());return *this;}
            Options&          recycling (bool r)                               {_recycling  = r;assert(consistent());return *this;}
            Options&          resetting (bool r)                               {_resetting  = r;assert(consistent());return *this;}
            Options&          debugging (bool d)                               {_debugging  = d;assert(consistent());return *this;}
            Options&          locking   (bool l)                               {_locking    = l;assert(consistent());return *this;}

 private:                  // Implementation
            bool              consistent()         const;

 private:                  // Representation
            name_t            _name;                     // The arena name
            size_t            _limit;                    // The memory limit
            size_t            _psize;                    // The size of a page
            ArenaPtr          _parent;                   // The parent arena
            unsigned          _finalizing : 1;           // Supports finalizing
            unsigned          _recycling  : 1;           // Supports recycling
            unsigned          _resetting  : 1;           // Supports resetting
            unsigned          _debugging  : 1;           // Supports debugging
            unsigned          _locking    : 1;           // Supports locking
};

/**
 *  @brief      Thrown in the event that an Arena's memory limit is exhausted.
 *
 *  @details    Class arena::Exhausted specializes the familiar std::bad_alloc
 *              exception to indicate that a request to allocate memory off an
 *              Arena would exceed the Arena's maximum allocation limit and so
 *              has been denied. The exception is potentially recoverable - it
 *              does not indicate that the entire system is out of memory, but
 *              only that the resources managed by This Arena are insufficient
 *              to satisfy the current request.
 *
 *              For example:
 *  @code
 *                  try  {return new(a) double[vast];}   // Can't hurt to try
 *                  catch(std::bad_alloc&)   {throw;}    // ...catastrophic
 *                  catch(arena::Exhausted&) {}          // ...recoverable
 *                  return new(a) double[a.available()]; // Recovering...
 * @endcode
 *              allows the caller to distinguish between both catastrophic and
 *              recoverable allocation failures.
 *
 *  @author     jbell@paradigm4.com.
 */
class Exhausted : public std::bad_alloc
{};

/**
 *  @brief      Checkpoints an Arena with the system resource monitor.
 *
 *  @details    Class Checkpoint uses the RIIA idiom to ensure that the system
 *              resource monitor is updated with a snapshot of a given Arena's
 *              allocation statistics both on entry to and exit from the scope
 *              in which the checkpoint was initialized.
 *
 *              For example:
 *  @code
 *                  {
 *                      Checkpoint(arena,"foo");         // Take snapshot here
 *                          ...
 *                  }                                    // ...as well as here
 *  @endcode
 *              The checkpoint label can be any string you like - it is simply
 *              passed on through to the monitor as is, although including the
 *              file and line number somehow might well prove helpful.
 *
 *  @author     jbell@paradigm4.com.
 */
class Checkpoint : noncopyable, stackonly
{
 public:                   // Construction
                              Checkpoint(const Arena&,name_t = "");
                             ~Checkpoint();

 private:                   // Representation
            const Arena&      _arena;                    // The arena
            const char* const _label;                    // The label
};

/**
 *  Finalize the object 'p' by explictly invoking its destructor.  Notice that
 *  for any given type 't', finalize<t> is indeed a finalizer for objects with
 *  type 't', and in fact has type 'finalizer_t', hence is a suitable argument
 *  for both the arena::allocate() and std::operator new() interfaces.
 */
template<class type>
inline void finalize(void* p)
{
    assert(p != 0);                                      // Validate argument

    static_cast<type*>(p)->~type();                      // Invoke destructor
}

/**
 *  Synthesize a finalizer function that is suitable for cleaning up values of
 *  the given type.  Two special cases are detected here,  both connected with
 *  minimizing the per allocation overhead associated with storing a finalizer
 *  function in an allocation; those types with trivial destructors don't need
 *  a finalizer at all, and for subclasses of Allocated, we *know* the address
 *  of the finalizer to invoke a priori.
 *
 *  For example:
 *  @code
 *      new(a,finalizer<string>()) string("delete me");  // Allocate a string
 *  @endcode
 *  is the correct way to allocate a value requiring destruction off an Arena.
 */
template<class type>
inline finalizer_t finalizer()
{
    if (boost::has_trivial_destructor<type>())           // Trivial destructor?
    {
        return 0;                                        // ...special encoding
    }

    if (boost::is_base_of<Allocated,type>())             // Is arena allocated?
    {
        return allocated;                                // ...special encoding
    }

    return &finalize<type>;                              // Invokes destructor
}

/**
 *  Allocate an object of the given type from off the Arena 'a'.
 *
 *  The function is available at all arities - well, 8, at any rate - with any
 *  additional arguments being passed on to the constructor for class 'type'.
 *
 *  @param x1,...,xn    Optional arguments to the constructor for class 'type'
 *  (not shown in the synopsis above).
 */
template<class type>
inline type* newScalar(Arena& a)
{
    return new(a,finalizer<type>()) type();              // Allocate off arena
}

/** @cond ********************************************************************
 * In the absence of proper compiler support for variadic templates, we create
 * the additional overloads with the help of the preprocessor...*/
#define SCIDB_NEW_SCALAR(_,i,__)                                               \
                                                                               \
template<class type,BOOST_PP_ENUM_PARAMS(i,class X)>                           \
inline type* newScalar(Arena& a,BOOST_PP_ENUM_BINARY_PARAMS(i,X,const& x))     \
{                                                                              \
    return new(a,finalizer<type>()) type(BOOST_PP_ENUM_PARAMS(i,x));           \
}

BOOST_PP_REPEAT_FROM_TO(1,8,SCIDB_NEW_SCALAR,"")         // Emit the overloads
#undef SCIDB_NEW_SCALAR                                  // And clean up after
/** @endcond ****************************************************************/

/**
 *  Allocate an array of 'c' elements of the given type from the Arena 'a' and
 *  default construct each element, guarding against a possible exception that
 *  the constructor might throw. The array should be returned to the Arena 'a'
 *  - and *only* the Arena 'a'- for destruction by calling 'destroy(a,p)'. For
 *  example:
 *  @code
 *      double* p = newVector<string>(arena,13);         // Allocate a vector
 *          ...
 *      destroy(arena,p);                                // Destroy the array
 *  @endcode
 *  allocates and initializes an array of 13 string objects, then returns this
 *  array to the Arena to be destroyed and the underlying memory recycled.
 */
template<class type>
type* newVector(Arena& a,count_t c)
{
    finalizer_t f = finalizer<type>();                   // Find the finalizer
    void* const v = a.allocate(sizeof(type),f,c);        // Allocate the array
    type* const p = static_cast<type*>(v);               // Recast the pointer

 /* If 'type' has a default constructor,  we call it now for each element from
    within a try block. Note that this condition is evaluated at complile time
    and then folded away.  Here we see the reason for the count_t parameter to
    arena::destroy(): any one of the constructors could fail and we need to be
    sure to only destroy those elements that were successfully constructed...*/

    if (!boost::has_trivial_default_constructor<type>()) // Needs construction?
    {
        count_t i = 0;                                   // ...items created

        try                                              // ...ctor may throw
        {
            for ( ; i!=c; ++i)                           // ....for each item
            {
                ::new(p + i) type();                     // .....construct it
            }
        }
        catch (...)                                      // ...the ctor threw
        {
            destroy(a,p,i);                              // ....undo first i
            throw;                                       // ....then rethrow
        }
    }

    return p;                                            // Your array, sir!
}

/**
 *  Destroy the 'c' element array 'p' that was allocated from the Arena 'a' by
 *  invoking the destructor of each element in order (from last to first), and
 *  then returning the underlying allocation to the Arena for recycling.
 *
 *  The count_t 'c' is only really needed when some, but not all, of the array
 *  elements require finalizing, as might be the case if a constructor were to
 *  throw an exception part-way through initializing the array; in other cases
 *  the default value of 'unlimited' is fine.
 *
 *  This is the preferred way to return an allocation to an Arena, rather than
 *  calling recycle() or destroy() directly. The latter *might* work, but some
 *  arenas optimize allocations of simple objects by omitting the block header
 *  and the virtual function Arena::destroy() needs this header to be there in
 *  order to recover the allocation's finalizer.
 */
template<class type>
inline void destroy(Arena& a,const type* p,count_t c)
{
    if (type* q = const_cast<type*>(p))                  // Is valid object?
    {
        if (boost::has_trivial_destructor<type>())       // ...no finalizer?
        {
            a.recycle(q);                                // ....just recycle
        }
        else                                             // ...has finalizer
        {
            a.destroy(q,c);                              // ....so invoke it
        }
    }
}

/**
 *  Insert a formatted representation of the Arena 'a' onto the ostream 'o'.
 */
inline std::ostream& operator<<(std::ostream& o,const Arena& a)
{
    return o << '{', a.insert(o), o << '}';              // Insert and return
}

/**
 *  Insert a formatted representation of the Arena allocated object 'a' onto
 *  the ostream 'o'.
 */
inline std::ostream& operator<<(std::ostream& o,const Allocated& a)
{
    return o << '{', a.insert(o), o << '}';              // Insert and return
}

/**
 *  Insert a formatted representation of the Exhausted exception 'e' onto the
 *  ostream 'o'.
 */
inline std::ostream& operator<<(std::ostream& o,const Exhausted& e)
{
    return o << e.what();                                // Insert and return
}

/**
 *  Construct an object of the given type within memory that is allocated from
 *  the Arena 'a', and wrap this new object with a shared_ptr.
 *
 *  This function overloads the boost function of the same name so as to allow
 *  it to be called directly with a reference to an Arena,  without the caller
 *  being required to first explicitly wrap the Arena in an Allocator.
 *
 *  The function is available at all arities - well, 8, at any rate - with any
 *  additional arguments being passed on to the constructor for class 'type'.
 *
 *  @param x1,...,xn    Optional arguments to the constructor for class 'type'
 *  (not shown in the synopsis above).
 *
 *  @see http://en.cppreference.com/w/cpp/memory/shared_ptr/allocate_shared
 *  @see http://www.boost.org/doc/libs/1_54_0/libs/smart_ptr/make_shared.html
 */
template<class type>
inline shared_ptr<type> allocate_shared(Arena& a)
{
    return boost::allocate_shared<type>(Allocator<type>(&a));
}

/** @cond ********************************************************************
 * In the absence of proper compiler support for variadic templates, we create
 * the additional overloads with the help of the preprocessor...*/
#define SCIDB_ALLOCATE_SHARED(_,i,__)                                          \
                                                                               \
template<class type,BOOST_PP_ENUM_PARAMS(i,class X)>                           \
inline shared_ptr<type>                                                        \
allocate_shared(Arena& a,BOOST_PP_ENUM_BINARY_PARAMS(i,X,const& x))            \
{                                                                              \
    return boost::allocate_shared<type>(Allocator<type>(&a),                   \
                                        BOOST_PP_ENUM_PARAMS(i,x));            \
}

BOOST_PP_REPEAT_FROM_TO(1,8,SCIDB_ALLOCATE_SHARED,"")    // Emit the overloads
#undef SCIDB_ALLOCATE_SHARED                             // And clean up after
/** @endcond ****************************************************************/


/****************************************************************************/
namespace detail {                                       // For implementation
/****************************************************************************/

class arena_deleter : private ArenaPtr
{
 public:                   // Construction
                              arena_deleter(const ArenaPtr& a)
                               : ArenaPtr(a)             {}

 public:                   // Operations
    template<class type>void  operator()(type* p)  const {destroy(*get(),p);}
};

/****************************************************************************/
}
/****************************************************************************/

/**
 *  Attach a shared_ptr wrapper to the native pointer 'p',  which points at an
 *  object - or graph of objects - that were originally allocated from off the
 *  Arena 'a'.  The resulting shared_ptr drags a strong reference to 'a' along
 *  with it and will return 'p' to this Arena when the reference count finally
 *  falls to zero.
 *
 *  Useful for adding sharing semantics to a graph of objects after the fact.
 *
 *  Notice the third argument to the shared_ptr constructor, which serves only
 *  to ensure that the header block used to store the reference counting stuff
 *  is also allocated within the Arena 'a'.
 */
template<class type>
shared_ptr<type> attach_shared(type* p,const ArenaPtr& a)
{
    return shared_ptr<type>(p,detail::arena_deleter(a),Allocator<int>(a));
}

/****************************************************************************/
}}
/****************************************************************************/

inline void* operator new   (size_t n,scidb::arena::Arena& a,scidb::arena::finalizer_t f=0) {return f ? a.allocate(n,f) : a.allocate(n);}
inline void  operator delete(void*  p,scidb::arena::Arena& a,scidb::arena::finalizer_t f=0) {       f ? a.destroy (p,0) : a.recycle (p);}

/****************************************************************************/
#endif
/** @file *******************************************************************/
