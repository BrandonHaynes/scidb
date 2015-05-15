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
 * SpatialRangesChunkPosIterator.h
 *
 *  Created on: Aug 15, 2014
 *      Author: Donghui Zhang
 */

#ifndef SPATIALRANGESCHUNKPOSITERATOR_H_
#define SPATIALRANGESCHUNKPOSITERATOR_H_

#include <util/SpatialType.h>
#include <util/MultiConstIterators.h>
#include <util/RegionCoordinatesIterator.h>
#include <boost/shared_ptr.hpp>

namespace scidb
{
/**
 * A class that enumerates the positions of the chunks that intersect at least one of the stored ranges.
 *
 * @note Use with caution! This class iterates over the logical space.
 * @see THE REQUEST TO JUSTIFY LOGICAL-SPACE ITERATION in RegionCoordinatesIterator.h.
 */
class SpatialRangesChunkPosIterator: public ConstIterator
{
public:
    /**
     * @param spatialRanges  a SpatialRanges object.
     * @param schema  the array schema.
     */
    SpatialRangesChunkPosIterator(boost::shared_ptr<SpatialRanges> const& spatialRanges, ArrayDesc const& schema);

    /**
     * @return true if done with enumeration.
     */
    virtual bool end();

    /**
     * Advance to the next chunkPos (that intersect with at least one of the ranges).
     */
    virtual void operator ++();

    /**
     * Get coordinates of the current element in the chunk
     */
    virtual Coordinates const& getPosition();

    /**
     * Not supported.
     */
    virtual bool setPosition(Coordinates const& pos);

    /**
     * Reset builds the rawIterators from low and high positions, and generate wrapperIterator..
     */
    virtual void reset();

    /**
     * Make minimal advancement of the iterator, provided that its position >= the specified newPos.
     * @param newPos  the position to reach or exceed.
     * @return whether any advancement is made.
     * @note it is possible that as result of the advancement, end() is reached.
     *
     */
    bool advancePositionToAtLeast(Coordinates const& newPos);

private:
    /**
     * How many spatial ranges are there?
     */
    const size_t _numRanges;

    /**
     * A spatialRanges object.
     */
    boost::shared_ptr<SpatialRanges> _spatialRanges;

    /**
     * Array schema.
     */
    ArrayDesc _schema;

    /**
     * A vector of RegionCoordinatesIterator objects.
     */
    std::vector<boost::shared_ptr<ConstIterator> > _rawIterators;

    /**
     * A MultiConstIterators object wrapping _rawIterators, for synchronized advancement.
     */
    boost::shared_ptr<MultiConstIterators> _wrapperIterator;

    /**
     * Each Coordinates in the vector will be used to initialize the 'low' for one of _rawIterators.
     * This is set in the constructor and never change.
     */
    std::vector<Coordinates> _lowPositionsForRawIterators;

    /**
     * Each Coordinates in the vector will be used to initialize the 'high' for one of _rawIterators.
     * This is set in the constructor and never changes.
     */
    std::vector<Coordinates> _highPositionsForRawIterators;

    /**
     * Chunk intervals.
     */
    std::vector<size_t> _intervals;
};

}

#endif /* SPATIALRANGESCHUNKPOSITERATOR_H_ */
