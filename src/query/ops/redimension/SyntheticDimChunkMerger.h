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
 * SyntheticDimChunkMerger.h
 *
 *  This file contains helper routines for merging partial chunks with a synthetic dimension.
 */

#ifndef SYNTHETIC_DIM_CHUNK_MERGER_H_
#define SYNTHETIC_DIM_CHUNK_MERGER_H_

#include <vector>
#include <boost/unordered_map.hpp>
#include <array/StreamArray.h>
#include <array/MemChunk.h>
#include <query/Operator.h>

namespace scidb
{
/**
 * A partial chunk merger which adjusts the synthetic dimension coordinate for incoming partial chunks
 * Each new cell gets the synthetic dimension coordiante equal to the current count of cells
 * with the same non-synthetic dimension coordinates.
 * The partial chunks are merged in the order of instance of origin IDs.
 */
class SyntheticDimChunkMerger : public MultiStreamArray::PartialChunkMerger
{
private:

    /// Helper class to keep track of the current count of cells with the same non-synthetic coordinates
    class SyntheticDimAdjuster
    {
    private:
        typedef boost::unordered_map<Coordinates, size_t> MapCoordToCount;

        /// Maps "collapsed" coordinates to count of collisions
        /// Collapsed coordinates have the synthetic dimension set to the same value, the dimension start
        MapCoordToCount _coord2Count;

        /**
         * Which dimension is the synthetic one.
         */
        size_t _dimSynthetic;

        /**
         * What is the dimStart of the synthetic dim.
         */
        Coordinate _dimStartSynthetic;

        Coordinates _collapsed;

    public:
        void clear()
        {
            _coord2Count.clear();
        }

        SyntheticDimAdjuster(size_t dimSynthetic, Coordinate dimStartSynthetic);
        ~SyntheticDimAdjuster() {}

        /**
         * Get a coordinates where the synthetic dim is 'collapsed', i.e. uses _dimStartSynthetic.
         */
        void useStartForSyntheticDim(Coordinates& coord)
        {
            coord[_dimSynthetic] = _dimStartSynthetic;
        }

        /**
         * Increase the synthetic dim's coordinate by an offset.
         */
        void increaseSyntheticDim(Coordinates& coord, size_t offset)
        {
            coord[_dimSynthetic] += offset;
        }

        /**
         * Update the count in a MapCoordToCount from a chunk
         * @param chunk from which the count should be aquired
         * @param chunkIter an optional cached iterator for the chunk
         */
        void updateMapCoordToCount(MemChunk const* chunk,
                                   ConstChunkIterator* chunkIter=NULL);

        /// Update the current count for a given positon
        void updateCount(const Coordinates& coords);

        /**
         * Calculate a new coordinates, by offseting what the map stores.
         * @param coord [in/out] the coordinates in which the synthetic dim should be increased
         */
        void calcNewCoord(Coordinates& coord);
    };

    SyntheticDimAdjuster _syntheticDimHelper;
    std::vector<boost::shared_ptr<MemChunk> > _partialChunks;
    Coordinates _currChunkPos;
    Coordinates _coord;

public:

    /**
     * Constructor
     * @param redimInfo information about the synthetic dimension
     * @param numInstances number of instance
     */
    SyntheticDimChunkMerger(const RedimInfo* redimInfo, size_t numInstances);

    /// Destructor
    virtual ~SyntheticDimChunkMerger() {}

    /// @see MultiStreamArray::PartialChunkMerger::mergePartialChunk
    bool
    mergePartialChunk(InstanceID instanceId,
                      AttributeID attId,
                      boost::shared_ptr<MemChunk>& chunk,
                      const boost::shared_ptr<Query>& query);

    /// @see MultiStreamArray::PartialChunkMerger::getMergedChunk
    boost::shared_ptr<MemChunk>
    getMergedChunk(AttributeID attId,
                   const boost::shared_ptr<Query>& query);

private:
    void
    mergeChunks(boost::shared_ptr<ChunkIterator>& dstIterator,
                boost::shared_ptr<MemChunk>& src);
    void clear();
};

}
#endif
