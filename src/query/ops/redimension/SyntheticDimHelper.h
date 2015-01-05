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
 * SyntheticDimHelper.h
 *
 *  Created on: Aug 9, 2012
 *      Author: dzhang
 *  This file contains helper routines for the synthetic dimension.
 */

#ifndef SYNTHETICDIMHELPER_H_
#define SYNTHETICDIMHELPER_H_

namespace scidb
{

#include <boost/unordered_map.hpp>

typedef boost::unordered_map<Coordinates, size_t> MapCoordToCount;

/**
 * Helper routines for dealing with the synthetic dim.
 */
class SyntheticDimHelper {
public:
    /**
     * Which dimension is the synthetic one.
     */
    size_t _dimSynthetic;

    /**
     * What is the dimStart of the synthetic dim.
     */
    size_t _dimStartSynthetic;

    SyntheticDimHelper(size_t dimSynthetic, size_t dimStartSynthetic):
        _dimSynthetic(dimSynthetic), _dimStartSynthetic(dimStartSynthetic) {
    }

    /**
     * Get a coordinates where the synthetic dim is 'collapsed', i.e. uses _dimStartSynthetic.
     */
    void useStartForSyntheticDim(Coordinates& coord) {
        coord[_dimSynthetic] = _dimStartSynthetic;
    }

    /**
     * Increase the synthetic dim's coordinate by an offset.
     */
    void increaseSyntheticDim(Coordinates& coord, size_t offset) {
        coord[_dimSynthetic] += offset;
    }

    /**
     * Update the count in a MapCoordToCount with a LruMemChunk.
     * @param mapCoordToCount   [inout] an initially empty map that will receive the count per collapsed coordinates
     * @param chunk             [in] the LruMemChunk from which the count should be aquired
     */
    void updateMapCoordToCount(boost::shared_ptr<MapCoordToCount>& mapCoordToCount, ConstChunk const* chunk) {
        // XXX tigor TODO: investigate if it does not have to be a MemChunk 
        assert(dynamic_cast<MemChunk const*>(chunk)!=NULL);

        // Note that default values can't be ignored. Otherwise the coordinate in the synthetic dimension would mess up.
        boost::shared_ptr<ConstChunkIterator> chunkIter = chunk->getConstIterator(ChunkIterator::IGNORE_EMPTY_CELLS|ChunkIterator::APPEND_CHUNK);
        while (!chunkIter->end()) {
            Coordinates collapsed = chunkIter->getPosition();
            useStartForSyntheticDim(collapsed);
            MapCoordToCount::iterator mapIter = mapCoordToCount->find(collapsed);
            if (mapIter==mapCoordToCount->end()) {
                mapCoordToCount->insert(std::pair<Coordinates, size_t>(collapsed, 1));
            } else {
                ++(mapIter->second);
            }
            ++(*chunkIter);
        }
    }

    /**
     * Calculate a new coordinates, by offseting what the map stores.
     * @param coord            [inout] the coordinates in which the synthetic dim should be increased
     * @param mapCoordToCount  [in] the map which stores for each coordinates how many elements exist
     */
    void calcNewCoord(Coordinates& coord, boost::shared_ptr<MapCoordToCount> const& mapCoordToCount) {
        Coordinates collapsed = coord;
        useStartForSyntheticDim(collapsed);
        MapCoordToCount::iterator mapIter = mapCoordToCount->find(collapsed);
        if (mapIter == mapCoordToCount->end()) {
            return;
        }
        increaseSyntheticDim(coord, mapIter->second);
    }
};

}


#endif /* SYNTHETICDIMHELPER_H_ */
