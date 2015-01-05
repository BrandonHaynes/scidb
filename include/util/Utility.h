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

#ifndef UTIL_UTILITY_H_
#define UTIL_UTILITY_H_

/****************************************************************************/

#include <new>                                           // For operator new
#include <assert.h>                                      // For assert()
#include <sstream>
#include <vector>
#include <system/Exceptions.h>

/****************************************************************************/
namespace scidb {
/****************************************************************************/
/**
 *  @brief      Prevents subclasses from being allocated on the heap.
 *
 *  @details    Class stackonly hides its new operators to prevent it, and any
 *              class that might derive from it, from being directly allocated
 *              on the heap. It mimics similar boost utility classes that seek
 *              to constrain the semantics of a class through inheritance, and
 *              though not entirely foolproof, nevertheless serves as a useful
 *              hint that an object is being initialized for its side effect.
 *
 *              It's useful when implementing the RIIA idiom, where it ensures
 *              that the lifetime of an object is tied to the lexical scope in
 *              which it is instantiated:
 *  @code
 *              class Lock : stackonly, boost::noncopyable
 *              {
 *                 Lock(...) ...
 *                ~Lock(...) ...
 *              }  lock(...);
 *  @endcode
 *              since without allocating an object on the heap there is no way
 *              for it to escape the current program block.
 *
 *  @see        http://en.wikibooks.org/wiki/More_C%2B%2B_Idioms/Requiring_or_Prohibiting_Heap-based_Objects
 *              for more on the underlying idiom.
 *
 *  @see        http://en.wikipedia.org/wiki/Resource_Acquisition_Is_Initialization
 *              for more on the RIIA pattern.
 *
 *  @see        http://www.boost.org/doc/libs/1_54_0/libs/utility/utility.htm
 *              for boost::noncopyable.
 *
 *  @author     jbell@paradigm4.com
 */
class stackonly
{
            void*             operator new(size_t);
            void*             operator new[](size_t);
            void              operator delete(void*);
            void              operator delete[](void*);
};

/**
 *  @brief      A trivial custom deleter for use with class shared_ptr.
 *
 *  @details    Sometimes it is desirable to create a shared_ptr to an already
 *              existing object so that the shared_ptr does not try to destroy
 *              the object when there are no remaining references. The factory
 *              function:
 *  @code
 *                  shared_ptr<X>   newX();
 *  @endcode
 *              might sometimes wish to return a statically allocated instance
 *              for example. The solution is to use a null_deleter:
 *  @code
 *                  shared_ptr<X> newX()
 *                  {
 *                      static X x;                      // Must Not delete x
 *
 *                      return shared_ptr<X>(&x,null_deleter());
 *                  }
 *  @endcode
 *              This same technique also works for any object that is known to
 *              outlive the shared_ptr that is aimed at it.
 *
 *  @see        http://www.boost.org/doc/libs/1_55_0/libs/smart_ptr/sp_techniques.html
 *              for use of class shared_ptr with statically allocated objects.
 *
 *  @author     jbell@paradigm4.com
 */
struct null_deleter
{
            void              operator()(const void*) const {}
};

/**
 *  Cast the pointer 'pb' from type 'base*' to type 'derived'. Equivalent to a
 *  static cast in a release build, but has the advantage that the downcast is
 *  checked with an assertion in a debug build.
 */
template<class derived,class base>
inline derived downcast(base* pb)
{
    assert(dynamic_cast<derived>(pb) == pb);             // Is the cast safe?

    return static_cast<derived>(pb);                     // So cast it already
}


/**
 *  Cast the pointer 'pb' from type 'base*' to type 'derived'. Equivalent to a
 *  static cast in a release build, but has the advantage that the downcast is
 *  checked with an assertion in a debug build.
 */
template<class derived_t, class base_t>
inline derived_t safe_dynamic_cast(base_t pb)
{
    derived_t ptr = dynamic_cast<derived_t>(pb);
    if (ptr != pb) {
        assert(false);
        std::stringstream ss;
        ss << " invalid cast from " << typeid(pb).name() << " to " << typeid(ptr).name();
        throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_UNREACHABLE_CODE) << ss.str();
    }
    return ptr;
}

/**
 *  Return true if the truth of 'a' logically implies the truth of 'b'; that
 *  is, 'b' is true whenever 'a' is true.
 */
inline bool implies(bool a,bool b)
{
    return !a || b;                                      // 'a' => 'b
}

/**
 *  Return true if the truth of 'a' logically implies the truth of 'b' and so
 *  also vice versa; in other words, both 'a' and 'b' have precisely the same
 *  truth value.
 */
inline bool iff(bool a,bool b)
{
    return a == b;                                       // 'a' <=> 'b'
}

/**
 * @brief Behold the One True TSV Parser.
 *
 * @description
 * This routine parses lines of "tab separated values" text.  It
 * modifies the input line in-place to unescape TSV escape characters.
 * It returns a vector of pointers to individual fields in the
 * modified line.
 *
 * The only possible error is caused by a backslash tab sequence,
 * which is illegal.
 *
 * @note If you choose a field delimiter other than TAB (ascii 0x09),
 * be certain that your data columns do not contain that character, or
 * you will get unexpected results.  Use of non-TAB delimiters is
 * discouraged for this reason.
 *
 * @param line modifiable null-terminated input line
 * @param fields vector of pointers into the modified line
 * @param delim alternate field delimiter
 * @return true iff line was successfully parsed
 *
 * @see http://dataprotocols.org/linear-tsv/
 */
extern bool tsv_parse(char *line,
                      std::vector<char*>& fields,
                      char delim = '\t');

/**
 * @brief Match an integer, floating point number, or "nan".
 * @note Leading and trailing whitespace in @c val are ignored.
 * @param val NULL-terminated string to search for a number
 * @return true iff an integer, floating point number, or "nan" is found
 * @throws runtime_error on regex compilation failure (should not happen)
 *
 * @note The initial call compiles a regular expression and so is not
 * thread-safe.  To get thread safety, call once from main() before
 * launching threads.  Subsequent calls are fine.
 */
extern bool isnumber(const char* val);

/****************************************************************************/
}
/****************************************************************************/
#endif
/****************************************************************************/
