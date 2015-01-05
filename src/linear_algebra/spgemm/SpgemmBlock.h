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
#ifndef SPGEMM_BLOCK_H_
#define SPGEMM_BLOCK_H_

#include "SpAccumulator.h"

/*
 * SpgemmBlock.h
 *
 *  Created on: November 4, 2013
 */


namespace scidb

{

template<class Value_tt>
class SpgemmBlock
{
public:
    /**
     * destructor, to assure derived class destructors are virtual
     */
    virtual ~SpgemmBlock() {;}
    /**
     * @param[in] rowBegin the minimum row that can be appended
     * @param[in] colBegin the minimum column that can be appended
     * @param[in] numRow the number of consecutive rows that can be appended (logical block width)
     * @param[in] numCol the number of consecutive columns that can be appended (logical block height)
     */
    SpgemmBlock() { ; }
    /**
     * append the tuple (colNum, value) to the end of row[rowNum],
     * creating the row if it does not exist yet.
     * Note that append does not explicitly order by colNum, as this is not required by spGemm.
     * @param[in] rowNum -- the row to append to
     * @param[in] colNum -- the column number of the value
     * @param[in] value -- the value to save
     */
    virtual void append(ssize_t rowNum, ssize_t colNum, const Value_tt& value) {;}
    /**
     * @return true == no tuples were appended since construction or reset.
     */
    virtual bool empty() {return false;}


private:
};


} // end namespace scidb
#endif // SPGEMM_BLOCK_H_
