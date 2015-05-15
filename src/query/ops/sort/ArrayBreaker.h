/*
**
* BEGIN_COPYRIGHT
*
* This file is part of SciDB.
* Copyright (C) 2014 SciDB, Inc.
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
 * ArrayBreaker.h
 *
 *  Created on: Nov 25, 2014
 *      Author: Donghui Zhang
 *
 *  Utility function to break an input array into multiple arrays.
 */

#ifndef _ARRAYBREAKER_H_
#define _ARRAYBREAKER_H_

#include <boost/shared_ptr.hpp>
#include <array/Array.h>

namespace scidb
{
/**
 * The type of a function, that decides which output array an input-array cell should go to.
 * @param cellPos            the cell position in the input array.
 * @param previousResult     the result of the previous call to this function.
 * @param query              the query context.
 * @param dims               the dimensions descriptions.
 * @param additionalInfo     additional information needed to make the decision.
 * @return which output array the input cell should be sent.
 */
typedef size_t (*BreakerOnCoordinates)(
        Coordinates const& cellPos,
        size_t previousResult,
        boost::shared_ptr<Query>& query,
        Dimensions const& dims,
        void* additionalInfo);

/**
 * Break an input array into multiple arrays.
 * @param inputArray           the input array.
 * @param outputArrays         a vector of output arrays.
 * @param query                the query context.
 * @param breakerFuncByCellPos a function of type BreakerOnCoordinates, to assign a cellPos to an instanceID.
 * @param isBreakerConsecutive whether the breaker function guarantees that, if two cell positions are assigned to the same instance,
 *                             all cell positions in between will be assigned to the same instance.
 *                             With this guarantee, the algorithm may run faster by avoiding processing one cell at a time if apply.
 * @param additionalInfo       some information to be passed to the breaker function.
 */
void breakOneArrayIntoMultiple(
        boost::shared_ptr<Array> const& inputArray,
        std::vector<boost::shared_ptr<Array> >& outputArrays,
        boost::shared_ptr<Query>& query,
        BreakerOnCoordinates breaker,
        bool isBreakerConsecutive,
        void* additionalInfo);

} // namespace scidb
#endif
