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
 * @file
 *
 * @author Artyom Smirnov <smirnoffjr@gmail.com>
 *
 * @brief Misc string utilities
 */

#ifndef STRINGUTIL_H_
#define STRINGUTIL_H_

#ifndef PROJECT_ROOT
#error #############################################################################################
#error # You must define PROJECT_ROOT with full path to your source directory before including
#error # Exceptions.h. PROJECT_ROOT used for reducing absolute __FILE__ path to relative in SciDB
#error # exceptions.
#error #############################################################################################
#endif

#include <strings.h>
#include <stdio.h>
#include <sstream>
#include <iomanip>

inline int compareStringsIgnoreCase(const std::string& a, const std::string& b)
{
    return strcasecmp(a.c_str(), b.c_str());
}
    
/**
 * Structure for creating case insensitive maps
 */
struct __lesscasecmp
{
	bool operator()(const std::string& a, const std::string& b) const
	{
		return strcasecmp(a.c_str(), b.c_str()) < 0;
	}
};

/**
 * Not all pathes from __FILE__ macros are absolute. Pathes from *.yy and *.ll seems to be relative
 * so check needed or bad things happens.
 */
#define REL_FILE \
    (__FILE__[0] == '/' ? std::string(__FILE__).substr(strlen(PROJECT_ROOT)).c_str() : __FILE__)

/**
 * Encode the non-printable characters in a string in hex format.
 *
 * For instance, encode the DEL character with '%7F'.  The '%'
 * character itself becomes '%25'.  Beware: this is NOT true URL
 * encoding by any stretch, nor does it have any knowledge of UTF-8.
 * Its intended use is for error logging.
 */
inline std::string debugEncode(const std::string& s)
{
    std::stringstream ss;
    for (std::string::const_iterator pos = s.begin(); pos != s.end(); ++pos) {
        char ch = *pos;
        if (ch == '%' || !::isprint(ch)) {
            ss << '%' << std::hex << std::setw(2) << std::uppercase
               << std::setfill('0') << int(ch);
        } else {
            ss << ch;
        }
    }
    return ss.str();
}

/// @see debugEncode(const std::string&)
inline std::string debugEncode(const char* s)
{
    std::string str;
    if (s) {
        str = s;
    }
    return debugEncode(str);
}

/**
 * Perform a bitwise operation between two blocks of data with the same size, and assign to the first.
 * To improve performance, the implementation tries to operate on 8 bytes of data at a time.
 *
 * @param Op_uint64_t  A class that provides operator() between two uint64_t values.
 * @param Op_char      A class that provides operator() between two char values.
 *
 * @param s1  dest buffer
 * @param s2  src buffer
 * @param n   size of each buffer in #bytes
 *
 * @example
 *    char s1[] = "dog";
 *    char s2[] = "cat";
 *    bitwiseOpAndAssign<WrapperForOr<uint64_t>, WrapperForOr<char> >(s1, s2, sizeof(s1));
 *
 *    After the call, s1 becomes "gow", i.e. the bitwise-or result of "dog" and "cat".
 *
 */
template<typename Op_uint64_t, typename Op_char>
void bitwiseOpAndAssign(char* s1, char const* s2, const size_t n) {
    Op_uint64_t op_uint64_t;
    Op_char op_char;
    size_t i=0;
    while (i+8<=n) {
        *reinterpret_cast<uint64_t*>(s1+i) = op_uint64_t(*reinterpret_cast<uint64_t*>(s1+i), *reinterpret_cast<uint64_t const*>(s2+i));
        i += 8;
    }
    while (i<n) {
        *(s1+i) = op_char(*(s1+i), *(s2+i));
        i++;
    }
}

/**
 * Input OR operator to biwiseOpAndAssign.
 */
template<typename T>
class WrapperForOr {
public:
    T operator()(T s1, T s2) {
        return s1 | s2;
    }
};

/**
 * Input AND operator to biwiseOpAndAssign.
 */
template<typename T>
class WrapperForAnd {
public:
    T operator()(T s1, T s2) {
        return s1 & s2;
    }
};

/**
 * Input XOR operator to biwiseOpAndAssign.
 */
template<typename T>
class WrapperForXor {
public:
    T operator()(T s1, T s2) {
        return s1 ^ s2;
    }
};

#endif /* STRINGUTIL_H_ */
