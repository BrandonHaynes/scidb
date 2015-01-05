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
#ifndef CSRBLOCKVECTOR_H_
#define CSRBLOCKVECTOR_H_

/*
 * CSRBlockVector.h
 *
 *  Created on: November 4, 2013
 */

//
#include <iostream>
// boost
#include <boost/shared_ptr.hpp>
#include <boost/unordered_map.hpp>
// local
#include "SpAccumulator.h"
#include "SpgemmBlock.h"

namespace scidb
{




//
// see the top of CSRBlock.h for
// declarations required prior to class CSRBlockVector definition,
// as they must be present in that file too in order to allow a
// friend declaration of the spGemm() implemented in this file
//


/**
 * CSRBlockVector is an abstract data type that efficiently represents a sparse matrix (column) vector.
 * Since there is only one column, we can look the values up directly by row.
 * See the comments preceding CSRBlock for more information about the general
 * role this class plays.
 *
 *
 */
template<class Value_tt, class IdAdd_tt>
class CSRBlockVector : public SpgemmBlock<Value_tt>
{
public:
    /**
     * @param[in] rowBegin the minimum row that can be appended
     * @param[in] colBegin the minimum column that can be appended
     * @param[in] numRow the number of consecutive rows that can be appended (logical block width)
     * @param[in] numCol the number of consecutive columns that can be appended (logical block height)
     */
    CSRBlockVector(ssize_t rowBegin, ssize_t colBegin,
                    size_t numRow,     size_t numCol,
                    size_t nnz);


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
    virtual bool empty() { return false; }

    // the following is logically a method of leftBlock, but as is typical for binary math operators, uses an explicit left argument.
    template<class SemiringTraits_tt>
    friend void spGemm(ssize_t leftRow, const CSRBlock<typename SemiringTraits_tt::Value_t>& leftBlock,
                                        const CSRBlockVector<typename SemiringTraits_tt::Value_t, typename SemiringTraits_tt::IdAdd_t>& rightBlock,
                         SpAccumulator<typename SemiringTraits_tt::Value_t,
                                       typename SemiringTraits_tt::OpAdd_t>& spRowAccumulator);

private:
   /**
     * Get the value of the vector at a specified row.
     * For use by the spgemm() friend only.
     * @param[in] row -- the row to look up
     * @return        -- the value looked up
     */
    Value_tt getValue(ssize_t row) const {
        assert (_rowBegin <= row);       // (note 1)

        size_t localIdx= row-_rowBegin ; // ligitimate by (note 1)
        assert(localIdx < _numRow);
        return _rows[localIdx];
    }

//  RowMap_t _rowMap;               // simplified to just the vector below compared to the parent class
    std::vector<Value_tt>   _rows;  // rows of the (column) vector

    // data mostly for assert() checks
    size_t      _nnz;               //  TODO: possibly may become _rows.size()
    ssize_t     _rowBegin;
    ssize_t     _colBegin;
    size_t      _numRow;
    const size_t      _numCol;
};


template<class Value_tt, class IdAdd_tt>
CSRBlockVector<Value_tt, IdAdd_tt>::CSRBlockVector(ssize_t rowBegin, ssize_t colBegin, size_t numRow, size_t numCol, size_t nnz)
:
    _rows(numRow, IdAdd_tt::value()),  // initialized to the additive identity

    _nnz(nnz),
    _rowBegin(rowBegin),
    _colBegin(colBegin),
    _numRow(numRow),
    _numCol(numCol)
{
    assert(numRow > 0);
    assert(numCol > 0);
    assert(numCol == 1);
}

/** @file **/
/**
 * @brief Append a value to the vector block
 *
 * @param rowNum -- the row number of the row
 * @param colNum -- the column number of the column, preserves the matrix api
 * @param value  -- the value to store at [row,col]
 *
 */
template<class Value_tt, class IdAdd_tt>
inline void CSRBlockVector<Value_tt, IdAdd_tt>::append(ssize_t rowNum, ssize_t colNum, const Value_tt& value)
{
    assert (_rowBegin <= rowNum); // (note1)
    assert (_colBegin == colNum); // this is the specialization criterion

    size_t localIdx= rowNum-_rowBegin ;  // unsigned permitted by (note1)
    assert(localIdx < _numRow);

    _rows[localIdx] = value;
}

/** @file **/
/**
 * @brief Multiply a row of a block matrix by a block vector. add the result [row-element] to the accumulator.
 *
 * @param leftRowNum -- the row number of the row
 * @param leftBlock -- the block containing the row
 * @param rightBlock -- the right-hand vector
 * @param spRowAccumulator -- a special ADT supporting efficient accumulation of row products.
 *
 */
template<class SemiringTraits_tt>
void spGemm(ssize_t leftRowNum, const CSRBlock<typename SemiringTraits_tt::Value_t>& leftBlock,
                                const CSRBlockVector<typename SemiringTraits_tt::Value_t, typename SemiringTraits_tt::IdAdd_t>& rightBlock,
                                SpAccumulator<typename SemiringTraits_tt::Value_t,
                                              typename SemiringTraits_tt::OpAdd_t>& spRowAccumulator)
{
    typedef typename SemiringTraits_tt::Value_t Value_t;
    typedef typename SemiringTraits_tt::OpAdd_t OpAdd_t ;
    typedef typename SemiringTraits_tt::OpMul_t OpMul_t ;
    typedef typename SemiringTraits_tt::IdAdd_t IdAdd_t ;
    typedef typename SemiringTraits_tt::IdMul_t IdMul_t ;

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

            Value_t rightValue = rightBlock.getValue(rightRowNum);
            const ssize_t rightColNum = rightBlock._colBegin ; // the only column

            // NOTE: normally the following assertion is correct when storage is sparse,
            //       but in the Vector case, the storage is actually dense for now
            //       so the zeros are actually present.
            // assert(rightValue != IdAdd_t::value());     // semiring 0 should not be explicit in block, see above

            // add leftValue * rightValue to the appropriate column of the output row (the accumulator)
            Value_t product = OpMul_t::operate(leftValue, rightValue); // semiring multiplication may not be *
            if (product != IdAdd_t::value()) {    // correct behavior for IEEE types and C++ "*", tropical semirings
                spRowAccumulator.addScatter(product, rightColNum);
            }
        }
    }
}

} // end namespace scidb
#endif // CSRBLOCKVECTOR_H_
