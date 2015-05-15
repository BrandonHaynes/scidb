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
#ifndef SPGEMM_BLOCK_IMPL_H_
#define SPGEMM_BLOCK_IMPL_H_

/*
 * SpgemmBlock.h
 *
 *  Created on: November 4, 2013
 */


// boost
#include <boost/shared_ptr.hpp>
#include <boost/unordered_map.hpp>
// scidb
#include <system/ErrorCodes.h>
#include <system/Exceptions.h>
#include <util/Utility.h>

// scidb/p4
#include "../LAErrors.h"

// local
#include "CSRBlock.h"
#include "CSRBlockVector.h"
#include "SpAccumulator.h"
#include "SpgemmBlock.h"


namespace scidb
{

/** @file **/
/**
 * A "factory" function that creates either a full CSRBlock when necessary,
 * or the CSRBlockVector optimization when that will be faster.
 * @param rowBegin     -- first row in this block
 * @param colBegin     -- first column in this block
 * @param numRow       -- number of rows in the block
 * @param numCol       -- number of columns in the block
 * @param nnzEstimate
 * @return
 */
template<class SemiringTraits_tt>
shared_ptr<SpgemmBlock<typename SemiringTraits_tt::Value_t> >
SpgemmBlockFactory(ssize_t rowBegin, ssize_t colBegin, size_t numRow, size_t numCol, size_t nnzEstimate)
{
    typedef typename SemiringTraits_tt::Value_t Value_t;
    typedef typename SemiringTraits_tt::IdAdd_t IdAdd_t;

    // if its a vector, then it also must not be too sparse, else
    // the sparse matrix block form would still be a win.
    // if there's one nnz for at least every 10 rows, it should still be a win
    // we may use 10x more space and lookups, but they won't be through a hashtable
    // which is about one magnitude slower.

    // criteria for using a CSRBlockVector:
    // single column and the NNZ's are in the same magnitude as the logical size
    const unsigned ONE_MAGNITUDE = 10u ;
    bool isRightVectorAndSufficientlyDense = numCol == ssize_t(1) &&
                                             nnzEstimate >= numRow/ONE_MAGNITUDE;

    SpgemmBlock<Value_t>* tmp = NULL;
    if(isRightVectorAndSufficientlyDense) {
        tmp = new CSRBlockVector<Value_t, IdAdd_t>(rowBegin, colBegin, numRow, numCol, nnzEstimate);
    } else {
        tmp = new CSRBlock<Value_t>(rowBegin, colBegin, numRow, numCol, nnzEstimate);
    }
    return shared_ptr<SpgemmBlock<Value_t> >(tmp);
}

/**
 * @brief Multiply a row of a block matrix by a block matrix. add the result [row] to the accumulator.
 *
 * @param leftRowNum -- the row number of the row
 * @param leftBlock -- the block containing the row
 * @param rightBlock -- the right-hand matrix
 * @param spRowAccumulator -- a special ADT supporting efficient accumulation of row products.
 *
 * @note This is merely a wrapper that dispatches on the type of rightBlock.
 *       rightBlock needs specialization when it is dense, is a vector, or is both.
 */
template<class SemiringTraits_tt>
void spGemm(ssize_t leftRowNum, const CSRBlock<typename SemiringTraits_tt::Value_t>& leftBlock,
                                const SpgemmBlock<typename SemiringTraits_tt::Value_t>& rightBlock,
                                SpAccumulator<typename SemiringTraits_tt::Value_t,
                                              typename SemiringTraits_tt::OpAdd_t>& spRowAccumulator)
{
    typedef typename SemiringTraits_tt::Value_t Value_t;
    typedef typename SemiringTraits_tt::IdAdd_t IdAdd_t;
    typedef CSRBlockVector<Value_t, IdAdd_t> RightCSRBlockVector_t;
    typedef CSRBlock<Value_t>                RightCSRBlock_t;

    const RightCSRBlockVector_t* rightCSRBlockVector= dynamic_cast<const RightCSRBlockVector_t*>(&rightBlock);
    if(rightCSRBlockVector) {
        spGemm<SemiringTraits_tt>(leftRowNum, leftBlock, *rightCSRBlockVector, spRowAccumulator);
        return ;
    }

    const RightCSRBlock_t* rightCSRBlock= dynamic_cast<const RightCSRBlock_t*>(&rightBlock);
    if(rightCSRBlock) {
        spGemm<SemiringTraits_tt>(leftRowNum, leftBlock, *rightCSRBlock, spRowAccumulator);
        return;
    }

    // NOTREACHED in principle
    std::stringstream ss;
    ss << " invalid cast from " << typeid(rightBlock).name() << " to " << typeid(rightCSRBlock).name();
    ASSERT_EXCEPTION(false, ss.str());
}


} // end namespace scidb
#endif // SPGEMM_BLOCK_IMPL_H_
