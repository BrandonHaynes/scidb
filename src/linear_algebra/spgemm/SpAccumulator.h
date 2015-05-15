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
#ifndef SPARSE_ACCUMULATOR_H_
#define SPARSE_ACCUMULATOR_H_

/*
 * SpAccumulator.h
 *
 *  Created on: November 4, 2013
 */


// std::
#include <vector>
// scidb::
#include "array/Array.h"

namespace scidb
{


/**
 * This class implements the "SParse Accumulator" (SPA) abstract data type (ADT) that makes it much easier
 * to comprehend the standard [Gustavson1978] sparse multiplication algorithm. The SPA was first presented
 * in [Gilbert1991].
 *
 * SPAs maintain a logical vector of values, initialized to 0, supporting the following operations:
 *     accumulator[i] += value  in O(1) time
 *     extraction of only accumulated values can be performed in O(num-accumulated-values) time.
 *
 * This is done by maintaining an array of values, an array of flags indicating whether the corresponding value is in use,
 * and a list of indices of values in use.  For details see [Gilbert1991].
 * @note
 *     [Gilbert1991] Gilbert, Moler, and Schreiber, SIAM Journal on Matrix Analysis and Applications, 13.1 (1992) pp 333-356<br>
 * *   [Gustavson1978] Gustavson, Fred G, ACM Transactions on Mathematical Software, Vol 4, No 3, September 1978, pp 250-269<br>
 */

// SpAcumulator only does the addition operator from the ring.
// So its traits interface is the additive group from the ring.
// NOTE: could take a SemigroupTraits_t but for now it will just take the value and
//       additive operator -- it avoids using the additive identity completely
template<class Val_tt, class OpAdd_tt>
class SpAccumulator {
public:

    /**
     * constructor -- initializes the SPA to contain no values.
     * @param indexBegin -- minumum index that will be used
     * @param indexSize  -- the number of consecutive indices that can be used.
     */
    SpAccumulator(ssize_t indexBegin, size_t indexSize);
    /**
     * reset the SPA to contain no non-zeros.
     */
    void reset();
    /**
     * @param value is added ...
     * @param index ...to the value at this index.
     */
    void addScatter(Val_tt value, ssize_t index);
    /**
     * @return true when no addScatter() of a non-zero has occurred since construction,
     *         reset(), or consume() of every iterator position followed by clearIndices().
     */
    bool empty() const { return _indicesUsed.empty(); }
    /**
     * sort the internally-maintained list of indices in use.
     * may be used prior to iterating over the values contained, if they are desired
     * in sorted order.
     */
    void sort() { std::sort(_indicesUsed.begin(), _indicesUsed.end()); }
    /**
     * clear the internally-maintained list of indices in use. must only be done
     * after iterating over the contents, and calling conusme() at each position
     * of the iterator.
     */
    void clearIndices() { _indicesUsed.clear(); }

    /**
     * what the SPA logically contains at each index on which addScatter() was called
     */
    struct IdxValPair {
        ssize_t index;
        Val_tt value;
        IdxValPair(ssize_t index_, Val_tt value_) : index(index_), value(value_) {;}
    }; // what a dereferenced iterator returns.

    /**
     * A std "input iterator" over the logical contents (array of IdxValPair) of the SPA.
     * Because it fulfills the std:: concept "input iterator" it need not have explict documentation.
     * Instead, refer directly to the std "input iterator" concept (which you probably already know).
     */
    friend class const_iterator;
    class const_iterator {
    public:
        const_iterator(SpAccumulator& accum, std::vector<ssize_t>::const_iterator it) : _accumulator(accum), _indicesUsedIt(it) {;}
        bool operator==(const_iterator rhs) const { return _indicesUsedIt == rhs._indicesUsedIt; }
        bool operator!=(const_iterator rhs) const { return !(*this==rhs); }
        const IdxValPair operator*() const {
            size_t indexZeroBased = *(_indicesUsedIt) ;
            assert(_accumulator._valsUsed[indexZeroBased]);
            return _accumulator.getExternalIdxVal(indexZeroBased);
        }
        const_iterator& operator++() { ++_indicesUsedIt; return *this;}
    protected:
        SpAccumulator&                          _accumulator;
        std::vector<ssize_t>::const_iterator    _indicesUsedIt;
    };

    /**
     * a non-const iterator that provides a non-standard "consume" operator.
     */
    friend class iterator;
    class iterator : public const_iterator
    {
    private:
        typedef const_iterator Base_t;
    public:
        iterator(SpAccumulator& accum, std::vector<ssize_t>::const_iterator it) : const_iterator(accum, it) {;}
        /**
         * the only non-const iterator operation that can modify the contents of the SPA.
         * It resets the internal state at the corresponding index back to an implicit zero,
         * before returning the same IdxValPar that would have been returned by operator*()
         * @return what const_iterator::operator*() would have returned.
         */
        IdxValPair consume() { // like operator*(), but this one resets _valsUsed while its still in the cache
            size_t indexZeroBased = *(Base_t::_indicesUsedIt) ;
            assert(Base_t::_accumulator._valsUsed[indexZeroBased]);
            Base_t::_accumulator._valsUsed[indexZeroBased] = false; // the one difference from operator*() !!
            return Base_t::_accumulator.getExternalIdxVal(indexZeroBased);
        }
    };
    /**
     * get a const_iterator
     */
    const_iterator begin() const { return const_iterator(*this, _indicesUsed.begin()); }
    /**
     * get the end condition
     */
    const_iterator end()   const { return const_iterator(*this, _indicesUsed.end()); }
    /**
     * get an iterator
     */
    iterator begin() { return iterator(*this, _indicesUsed.begin()); }
    /**
     * get the end condition
     */
    iterator end()   { return iterator(*this, _indicesUsed.end()); }

    /**
     * export Val_tt for implementing, e.g. spAccumulatorFlushToChunk
     */
    typedef Val_tt Val_t ;
private:
    IdxValPair getExternalIdxVal(size_t indexZeroBased) {
        Val_tt value = _values[indexZeroBased];
        ssize_t indexExternal = _minExternalIndex + indexZeroBased;
        return IdxValPair(indexExternal, value) ;
    }

    std::vector<Val_tt>     _values;
    std::vector<bool>       _valsUsed; // Note vector<bool> specialization provides 8 bools per byte of storage.
                                       // TODO: upgrade from bool to sequence numbers, to further reduce memory bandwidth.

    typedef typename std::vector<ssize_t> IndicesUsed_t;
    IndicesUsed_t           _indicesUsed;  // indices in _values[] that are in use
    ssize_t                 _minExternalIndex;
};


// construction is O(size)
template<class Val_tt, class OpAdd_tt>
SpAccumulator<Val_tt, OpAdd_tt>::SpAccumulator(ssize_t indexBegin, size_t indexSize)
:
    _values(indexSize),             // pre-allocated, doesn't actually need initialization. will hold values at indices written in random order
    _valsUsed(indexSize,false),     // pre-allocated, initialized false.     _valsUsed[i] will be true <-> values[i] was addScattered()
    _indicesUsed(),                 // maintained by doing .push_back(i) when _valsUsed[i] is first set true.
    _minExternalIndex(indexBegin)
{
    assert(indexSize >= 1);

    if(0) {
        // TODO: need manual validation that the size allocated is the minimum possible for the situation
        std::cerr << "SpAccumulator size is " << indexSize << std::endl;
    }
}


template<class Val_tt, class OpAdd_tt>
void SpAccumulator<Val_tt, OpAdd_tt>::reset()
{
    for(typename IndicesUsed_t::iterator it = _indicesUsed.begin(); it != _indicesUsed.end(); ++it) {
        _valsUsed[ *it ] = false;
    }
    _indicesUsed.clear();   // retention of capacity (no reallocation) is helpful here.
}


template<class Val_tt, class OpAdd_tt>
void SpAccumulator<Val_tt, OpAdd_tt>::addScatter(Val_tt value, ssize_t index)
{
    // convert carefully to size_t indexing (zero-based)
    ssize_t tmp = index - _minExternalIndex ;
    assert(tmp >= 0);   // works until size is so big, it wouldn't be addressable as an index in this process
    size_t indexZeroBased = tmp ;
    assert(indexZeroBased <  _values.size());


    if( !_valsUsed[indexZeroBased]) {
        _valsUsed[indexZeroBased] = true ;
        _indicesUsed.push_back(indexZeroBased); // track indices that got used, for output and reset()
        _values[indexZeroBased] = value;       // first time -- set it
    } else {
        _values[indexZeroBased] = OpAdd_tt::operate(_values[indexZeroBased], value); // nth time -- accumulate it with the semiring's addition operator.
    }
}


} // end namespace scidb

#endif // SPARSE_ACCUMULATOR_H__

