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
/**
 * @file forced_lexical_cast.h
 *
 * @brief Make lexical_cast to unsigned types fail for negative inputs.
 *
 * @description Sadly, @c boost::lexical_cast<size_t>("-1") returns
 * 18446744073709551615 rather than throwing @c boost::bad_lexical_cast.
 * This is the workaround as suggested by Boost.org ticket 5494.
 *
 * @see https://svn.boost.org/trac/boost/ticket/5494
 */

#include <boost/lexical_cast.hpp>
#include <boost/type_traits/is_unsigned.hpp>

template <bool is_unsigned>
struct unsigned_checker
{
    template<typename String_type>
    static inline void do_check(const String_type & str) { }
};

template <>
struct unsigned_checker<true>
{
    template<typename String_type>
    static inline void do_check(const String_type & str)
    {
        if( str[0] == '-' )
            boost::throw_exception( boost::bad_lexical_cast() );
    }
};

template<typename Target, typename Source>
inline Target forced_lexical_cast(const Source &arg)
{
    unsigned_checker< boost::is_unsigned<Target>::value >::do_check(arg);
    return boost::lexical_cast<Target>( arg );
}
