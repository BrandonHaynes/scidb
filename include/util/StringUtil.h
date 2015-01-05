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
 * Encode the non-printable characters in a string with their octal format.
 * For instance, encode the 'DEL' character with '\177'.
 */
inline std::string encodeString(std::string s) {
    std::string dest;
    char buf[] = {'\\', 0, 0, 0, 0}; // 3 digits are needed; the fourth one is to avoid overwriting.
    for (size_t i=0; i<s.size(); ++i) {
        char c = s[i];
        if (isprint(c)) {
            dest += c;
        }
        else {
            snprintf(&buf[1], 3, "%03o", static_cast<unsigned char>(c)); // print c as three octal digits, padding with 0.
            dest.append(buf, 4);
        }
    }
    return dest;
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
