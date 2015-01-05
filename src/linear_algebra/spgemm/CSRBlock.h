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
#ifndef CSRBLOCK_H_
#define CSRBLOCK_H_

/*
 * CSRBlock.h
 *
 *  Created on: November 4, 2013
 */


// boost
#include <boost/shared_ptr.hpp>
#include <boost/unordered_map.hpp>
// local
#include "SpgemmBlock.h"
#include "SpAccumulator.h"

namespace scidb
{


//
// declarations required prior to class CSRBlock definition
//
template<class Value_tt>
class CSRBlock;     // required to declare spGemm

template<class SemiringTraits_tt>   // template functions must be declared prior to a class that befriends them.
void spGemm(ssize_t leftRow, const CSRBlock<typename SemiringTraits_tt::Value_t >& leftBlock,
                             const CSRBlock<typename SemiringTraits_tt::Value_t >& rightBlock,
                             SpAccumulator<typename SemiringTraits_tt::Value_t,
                                           typename SemiringTraits_tt::OpAdd_t>& spRowAccumulator);

//
// declarations required prior to class CSRBlock definition in order to friend the specialized spGemm
//
template<class Value_tt, class IdAdd_tt>
class CSRBlockVector;     // required to declare spGemm

template<class SemiringTraits_tt>   // template functions must be declared prior to a class that befriends them.
void spGemm(ssize_t leftRow, const CSRBlock<typename SemiringTraits_tt::Value_t >& leftBlock,
                             const CSRBlockVector<typename SemiringTraits_tt::Value_t, typename SemiringTraits_tt::IdAdd_t>& rightBlock,
                             SpAccumulator<typename SemiringTraits_tt::Value_t,
                                           typename SemiringTraits_tt::OpAdd_t>& spRowAccumulator);

/**
 * CSRBlock is an abstract data type that efficiently represents a typical
 * Compressed Sparse Row (CSR) representation of a sparse matrix.  For example, see the classic
 * paper [Gilbert1991]
 *
 *
 * @note [Gilbert1991] Gilbert, Moler, and Schreiber, SIAM Journal on Matrix Analysis and Applications, 13.1 (1992) pp 333-356 ;
 * @note You may not add functionality or SciDB knowledge to this class, as it has exacting
 *       performance requirements.
 *       To extend it, you must use it as a base class or member of a distinct class, but you
 *       may not make any of its functions virtual (except for a destructor).
 */

template<class Value_tt>
class CSRBlock : public SpgemmBlock<Value_tt>
{
public:
    /**
     * @param[in] rowBegin the minimum row that can be appended
     * @param[in] colBegin the minimum column that can be appended
     * @param[in] numRow the number of consecutive rows that can be appended (logical block width)
     * @param[in] numCol the number of consecutive columns that can be appended (logical block height)
     */
    CSRBlock(ssize_t rowBegin, ssize_t colBegin, size_t numRow,     size_t numCol, size_t nnz);

    /**
     * append the tuple (colNum, value) to the end of row[rowNum],
     * creating the row if it does not exist yet.
     * Note that append does not explicitly order by colNum, as this is not required by spGemm.
     * @param[in] rowNum -- the row to append to
     * @param[in] colNum -- the column number of the value
     * @param[in] value -- the value to save
     */
    virtual void append(ssize_t rowNum, ssize_t colNum, const Value_tt& value);
    /**
     * @return true == no tuples were appended since construction or reset.
     */
    virtual bool empty() { return _rowMap.empty(); }

    // the following are logically methods of leftBlock, but as is typical for binary math operators
    // are implemented as friends rather than members so that normal binary-operator notation can
    // be used
    template<class SemiringTraits_tt>
    friend void spGemm(ssize_t leftRow, const CSRBlock<typename SemiringTraits_tt::Value_t>& leftBlock,
                                        const CSRBlock<typename SemiringTraits_tt::Value_t>& rightBlock,
                       SpAccumulator<typename SemiringTraits_tt::Value_t,
                       typename SemiringTraits_tt::OpAdd_t>& spRowAccumulator);
    template<class SemiringTraits_tt>
    friend void spGemm(ssize_t leftRow, const CSRBlock<typename SemiringTraits_tt::Value_t>& leftBlock,
                                        const CSRBlockVector<typename SemiringTraits_tt::Value_t, typename SemiringTraits_tt::IdAdd_t>& rightBlock,
                       SpAccumulator<typename SemiringTraits_tt::Value_t,
                       typename SemiringTraits_tt::OpAdd_t>& spRowAccumulator);

private:
    // TYPES
    struct ColVal {
        ssize_t column;
        Value_tt          value;
        inline ColVal(ssize_t col, Value_tt val) : column(col), value(val) {;}
    };
    typedef std::vector<ColVal > Row_t;

    typedef Row_t&               RowRef_t;
    typedef const Row_t&         ConstRowRef_t;
    typedef boost::unordered_map<ssize_t, Row_t> RowMap_t;


    // DATA
    RowMap_t _rowMap;

    // mostly for assert() checks
    size_t      _nnz;
    ssize_t     _rowBegin;
    ssize_t     _colBegin;
    size_t      _numRow;
    size_t      _numCol;
};


template<class Value_tt>
CSRBlock<Value_tt>::CSRBlock(ssize_t rowBegin, ssize_t colBegin, size_t numRow, size_t numCol, size_t nnz)
:
    _nnz(nnz),
    _rowBegin(rowBegin),
    _colBegin(colBegin),
    _numRow(numRow),
    _numCol(numCol)
{
    assert(numRow > 0);
    assert(numCol > 0);
}


template<class Value_tt>
inline void CSRBlock<Value_tt>::append(ssize_t rowNum, ssize_t colNum, const Value_tt& value)
{
    assert (rowNum >= _rowBegin);
    assert (colNum >= _colBegin);


    // NOTE: prior to the introduction of CSRBlockVector,
    //       the next 4 lines were the hotspot of sparse-matrix, dense vector multiplication

    typename RowMap_t::iterator it = _rowMap.find(rowNum); // leftValue's column corresponds to this matrix's row

    if (it ==_rowMap.end()) { // doesn't exist
        RowRef_t rowRef = _rowMap[rowNum]; // initialize at this key 
        rowRef.push_back(ColVal(colNum, value));
    } else {
        RowRef_t rowRef = (*it).second;
        rowRef.push_back(ColVal(colNum, value));
    }
}

/** @file **/
/**
 * @brief Multiply a row of a block matrix by a block matrix. add the result [row] to the accumulator.
 *
 * @param leftRowNum -- the row number of the row
 * @param leftBlock -- the block containing the row
 * @param rightBlock -- the right-hand matrix
 * @param spRowAccumulator -- a special ADT supporting efficient accumulation of row products.
 *
 * @note This is the classic sparse multiplication algorithm as in the following papers
 *       See:<br>
 *       <br>
 *       [Gustavson1978] Gustavson, Fred G, ACM Transactions on Mathematical Software, Vol 4, No 3, September 1978, pp 250-269<br>
 *       [Gilbert1991] Gilbert, Moler, and Schreiber, SIAM Journal on Matrix Analysis and Applications, 13.1 (1992) pp 333-356<br>
 */
template<class SemiringTraits_tt>
void spGemm(ssize_t leftRowNum, const CSRBlock<typename SemiringTraits_tt::Value_t>& leftBlock,
                                const CSRBlock<typename SemiringTraits_tt::Value_t>& rightBlock,
                                SpAccumulator<typename SemiringTraits_tt::Value_t,
                                              typename SemiringTraits_tt::OpAdd_t>& spRowAccumulator)
{
    typedef typename SemiringTraits_tt::Value_t Value_t;
    typedef typename SemiringTraits_tt::OpAdd_t OpAdd_t ;
    typedef typename SemiringTraits_tt::OpMul_t OpMul_t ;
    typedef typename SemiringTraits_tt::IdAdd_t  IdAdd_t ;
    typedef typename SemiringTraits_tt::IdMul_t  IdMul_t ;

    typedef CSRBlock<Value_t> CSR_t;
    typedef typename CSR_t::RowMap_t RowMap_t ;
    typedef typename CSR_t::Row_t    Row_t;
    typedef typename CSR_t::ConstRowRef_t ConstRowRef_t ;

    assert (leftRowNum >= leftBlock._rowBegin);

    // for all leftValues in the leftRow
    typename RowMap_t::const_iterator leftMapIt = leftBlock._rowMap.find(leftRowNum); // leftValue's column corresponds to this matrix's row
    if (leftMapIt != leftBlock._rowMap.end()) {
        const ConstRowRef_t leftRow = (*leftMapIt).second;
        for (typename Row_t::const_iterator leftRowIt = leftRow.begin(); leftRowIt != leftRow.end(); ++leftRowIt) {
            Value_t leftValue = (*leftRowIt).value;
            ssize_t leftColNum = (*leftRowIt).column ;
            assert(leftValue != IdAdd_t::value());      // semiring 0 should not be explicit in block, else asymptotic time is violated
                                                        // they must have been removed during memory loading.

            ssize_t rightRowNum = leftColNum; // leftBlock's column corresponds to rightBlock's row
            assert (rightRowNum >= rightBlock._rowBegin);

            // for all rightValues in the rightRow
            typename RowMap_t::const_iterator rightMapIt = rightBlock._rowMap.find(rightRowNum);
            if (rightMapIt != rightBlock._rowMap.end()) {
                const ConstRowRef_t rightRow = (*rightMapIt).second;
                for (typename Row_t::const_iterator rightRowIt = rightRow.begin(); rightRowIt != rightRow.end(); ++rightRowIt) {
                    Value_t rightValue = (*rightRowIt).value;
                    ssize_t rightColNum = (*rightRowIt).column ;
                    assert(rightValue != IdAdd_t::value());     // semiring 0 should not be explicit in block, see above

                    // add leftValue * rightValue to the appropriate column of the output row (the accumulator)
                    Value_t product = OpMul_t::operate(leftValue, rightValue); // semiring multiplication may not be *
                    //Value_t product = OpTimes(leftValue, rightValue);
                    if (product != IdAdd_t::value()) {    // correct behavior for IEEE types and C++ "*", tropical semirings
                        spRowAccumulator.addScatter(product, rightColNum);
                    }
                }
            }
        }
    }
}

} // end namespace scidb
#endif // CSRBLOCK_H_
