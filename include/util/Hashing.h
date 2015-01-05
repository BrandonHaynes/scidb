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
 * Hashing.h
 *
 *  Created on: Jun 12, 2012
 *      Author: dzhang
 *
 *  This file is meant to be a place holder for all hashing needs that is not satisfied by MurmurHash3.h and boost::hash.
 */

#ifndef HASHING_H_
#define HASHING_H_

#include <../extern/MurmurHash/MurmurHash3.h>
#include <boost/functional/hash.hpp>
#include <functional>

namespace scidb
{

/**
 * A wrapper class that computes the hash value of a pointer by hashing on the actual object.
 * @example See Lru.h.
 */
template<typename T, typename Hash = boost::hash<T> >
struct hash_with_ptr: public std::unary_function<T*, size_t> {
    size_t operator()(const T* const & ptr) const {
        Hash myHash;
        return myHash(*ptr);
    }
};

/**
 * A wrapper class that compares two pointers by comparing the actual objects.
 * @example See Lru.h.
 */
template<class T, class EqualTo = std::equal_to<T> >
struct equal_to_with_ptr: public std::binary_function<T*, T*, bool> {
    bool operator() (const T* const& x, const T* const& y) const {
        EqualTo myEqualTo;
        return myEqualTo(*x, *y);
    }
};

/**
 * A hash over a vector of values.
 * @note
 *   boost::hash does not produce uniformly-distributed results.
 *
 * @note
 *   The class only supports T if fmix() is defined over T. Right now int64_t, uint64_t, int32_t, and uint32_t are supported.
 *
 * @example
 *   An example would be to hash over Coordinates. For instance:
 *   Coordinates c(2);
 *   c[0] = 100;
 *   c[1] = 200;
 *   size_t hashValue = VectorHash<Coordinate>()(c);
 *
 */
template<typename T>
struct VectorHash: public std::unary_function<std::vector<T>, size_t> {
    size_t operator()(const std::vector<T>& c) const {
        size_t ret = 0;
        for (size_t i=0; i<c.size(); ++i) {
            ret += fmix(c[i]);
        }
        return ret;
    }
};

/**
 * A hash over a single value.
 *
 * @note
 *   The class only supports T if fmix() is defined over T. Right now int64_t, uint64_t, int32_t, and uint32_t are supported.
 *
 * */
template<typename T>
struct IntHash: public std::unary_function<T, size_t> {
    size_t operator()(const T& c) const {
        return fmix(c);
    }
};

}


#endif /* HASHING_H_ */
