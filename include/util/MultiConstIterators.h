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
 * MultiConstIterators.h
 *
 *  Created on: Jun 10, 2013
 *      Author: dzhang
 */

#ifndef MULTI_CONST_ITERATORS_H
#define MULTI_CONST_ITERATORS_H

#include <vector>
#include <set>
#include <boost/shared_ptr.hpp>
#include <array/Array.h>

namespace scidb
{

/**
 * A Coordinates and a size_t.
 */
struct CoordinatesAndID
{
    Coordinates _coord;
    size_t _ID;

    CoordinatesAndID(Coordinates const& coord, size_t ID)
    : _coord(coord), _ID(ID)
    {}
};

/**
 * less() operator for CoordinatesAndID.
 * Order primarily by Coordinates, then by ID.
 *
 * @note
 * The implementation of MultiChunkIterators assumes the implementation of this operator is unchanged.
 */
inline bool operator< (const CoordinatesAndID& a, const CoordinatesAndID& b)
{
    int64_t c = coordinatesCompare(a._coord, b._coord);
    if (c < 0) {
        return true;
    }
    if (c > 0 ) {
        return false;
    }
    return a._ID < b._ID;
}

/**
 * Wrapper of multiple ConstIterators, that are scanned synchronously in increasing order of chunk position.
 * The purpose is to provide ordered access according to the position.
 *
 * @example
 * Say you have several ArrayIterator objects, one per attribute. You want to examine all the chunks at chunk position 0,
 * then all the chunks at chunk position 1, then all the chunks at chunk position 2, and so on.
 * You would need to keep track of which ArrayIterator is at which position, as some ArrayIterator may have 'holes'
 * at certain positions.
 * This class does it for you.
 *
 * @usage The usage pattern is:
 *
 *     vector<shared_ptr<ConstIterator> > inputIters;
 *     Fill the inputIters;
 *     MultiConstIterators multiIters(inputIters);
 *     while (!multiIters.end()) {
 *         const Coordinates& minPos = multiIters.getPosition();  // this is the min position
 *         vector<size_t> IDs;
 *         multiIters.getIDsAtMinPosition(IDs); // these are the indices of inputIters, that have data at the min position
 *         for (size_t i=0; i<IDs.size(); ++i) {
 *             Use the iterator inputIters[ IDs[i] ];
 *         }
 *         ++ multiIters; // increment all the input iterators that are at the min position
 *     }
 *
 * @limitation
 *     You cannot call reset() or setPosition().
 *     If you need to rewind, define another MultiConstIterator.
 *
 * @note
 *     Because MultiConstIterators is an instance of ConstIterator, it is fine to treat a MultiConstIteratos as the input
 *     to another, higher-level, MultiConstIterators. But in this case, the caller shall not manually call operator++()
 *     on the lower-level MultiConstIterators.
 *
 */
class MultiConstIterators: public ConstIterator
{
    /**
     * the input array iterators
     */
    std::vector<boost::shared_ptr<ConstIterator> >& _inputIters;

    /**
     * The sorted list of (coordinates, ID),
     * for the iterators that have not ended yet.
     */
    std::set<CoordinatesAndID> _coordinatesAndIDs;

public:
    /**
     * Constructor sets the array iterators.
     * @param[inout] inputIters   the input iterators, on which ++() may be called
     */
    MultiConstIterators(std::vector<boost::shared_ptr<ConstIterator> >& inputIters);

    /**
     * Get the IDs of the inputIters that have data at the current position.
     * @param[out] IDs   the vector of IDs in increasing order
     */
    void getIDsAtMinPosition(std::vector<size_t>& IDs);

    /**
     * true if ALL the input iterators have ended.
     */
    virtual bool end();

    /**
     * Increase the input iterators that are at the min position.
     */
    virtual void operator ++();

    /**
     * Get the min position.
     */
    virtual Coordinates const& getPosition();

    /**
     * Not supported.
     */
    virtual bool setPosition(Coordinates const& pos);

    /**
     * Not supported.
     */
    virtual void reset();
};

} // namespace
#endif
