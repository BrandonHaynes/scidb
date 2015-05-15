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
 *  SortArray.h
 *
 *  Created on: Sep 23, 2010
 */
#ifndef SORT_ARRAY_H
#define SORT_ARRAY_H

#include <array/Metadata.h>
#include <array/Array.h>
#include <array/MemArray.h>
#include <array/TupleArray.h>
#include <util/Arena.h>

#include <stdio.h>
#include <ctype.h>
#include <inttypes.h>
#include <limits.h>
#include <string>
#include <log4cxx/logger.h>

namespace scidb
{

/**
 * Utility class for sorting arrays
 */
class SortArray
{
public:

    static log4cxx::LoggerPtr logger;

    /**
     * Class will sort Arrays into a 1-d output array based on sort keys
     * provided
     * @param[in] inputSchema schema of array to sort
     * @param[in] arena parent mem arena for SortArray's mem usage
     * @param[in] preservePositions  whether the cell positions of all records need to be preserved.
     * @param[in] chunkSize optional chunk size for ouput
     */
    SortArray(const ArrayDesc& inputSchema, arena::ArenaPtr const& arena, bool preservePositions, size_t chunkSize = 0)
        : _inputSchema(inputSchema), _arena(arena), _preservePositions(preservePositions)
    {
        calcOutputSchema(inputSchema, chunkSize);
    }

    /**
     * Sort the array
     * tcomp should be instance of TupleComparator or instance of a derived
     * class if caller wishes to override standard compare behavior.
     * @param[in] inputArray array to sort, schema must match input schema
     * @param[in] query query context
     * @param[in] tcomp class which provides comparison operator
     * @return sorted one-dimensional array.
     */
    boost::shared_ptr<MemArray> getSortedArray(boost::shared_ptr<Array> inputArray,
                                               boost::shared_ptr<Query> query,
                                               boost::shared_ptr<TupleComparator> tcomp);

    /**
     * @return the array descriptor from the input array.
     */
    const ArrayDesc& getInputArrayDesc()
    {
        return _inputSchema;
    }

    /**
     * @return the array descriptor for the output array
     */
    const ArrayDesc& getOutputArrayDesc()
    {
        return *_outputSchema;
    }

    /**
     * @return whether the SortArray needs to preserve the cell positions of all records.
     * @note The output array schema depends on this!
     *       In particular, to preserve positions, there output schema has two more attributes:
     *       - chunk_pos: an integer that corresponds to the chunk number. @see ArrayCoordinatesMapper::chunkPos2pos, pos2chunkPos.
     *       - cell_pos:  an integer that corresponds to the cell position inside a chunk. @see ArrayCoordinatesMapper::coord2pos, pos2coord.
     */
    bool preservePositions() const
    {
        return _preservePositions;
    }

private:

    /**
     * SortIterators are the array iterators that can be used by a sort job
     * while partitioning the input into sorted runs.  An instance of SortIterator
     * may only be used by a *single* sort job at a time.
     */
    class SortIterators
    {
    public:
        SortIterators(boost::shared_ptr<Array>& input,
                      size_t shift,
                      size_t step);

        void advanceIterators();
        bool end() 
            { return _arrayIters[0]->end(); }
        std::vector< boost::shared_ptr<ConstArrayIterator> > const& getIterators()
            { return _arrayIters; }

    private:
        // iterators under the control of this instance
        std::vector< boost::shared_ptr<ConstArrayIterator> > _arrayIters;

        size_t _shift;    // initial chunk offset
        size_t _step;     // number of chunks to skip on advance
    };


    /**
     * A SortJob is a Job that partitions part of the array into a mem-sized chunk
     * and sorts it.  The result of producing this sorted run is a MemArray constructed
     * from a TupleArray (each one of which fits in memory).  Note that the input array
     * being sorted may or may not have an empty tag, but the sort job produces a
     * result with an empty tag.
     */
    class SortJob : public Job, protected SelfStatistics
    {
    public:
        SortJob(SortArray& sorter,
                boost::shared_ptr<Query> query,
                size_t id,
                SortIterators& iters);

        virtual void run();
        bool complete() { return _complete; };

    private:
        SortArray& _sorter;               // enclosing sort context
        SortIterators& _sortIters;        // iterators into array to partition/sort
        bool _complete;                   // true if at and of input
        size_t _id;                       // job id, determines which chunks to partition
    };


    /**
     * A MergeJob is a Job that merges sorted runs into larger sorted runs.
     * It uses a MergeSortArray to do the merging
     */
    class MergeJob : public Job, protected SelfStatistics
    {
    public:
        MergeJob(SortArray& sorter,
                 boost::shared_ptr<Query> query,
                 size_t id);

        virtual void run();

    private:
        SortArray& _sorter;     // enclosing sort context
        size_t _id;             // job id, used to communicate with main thread
    };


    /**
     * Calculate the schema for the output array
     */
    void calcOutputSchema(const ArrayDesc& inputSchema, size_t chunkSize);

    ArrayDesc const _inputSchema;
    arena::ArenaPtr const _arena;                    // parent memory arena
    boost::shared_ptr<Array> _input;           // array to sort
    boost::shared_ptr<ArrayDesc> _outputSchema;      // shape of output
    boost::shared_ptr<TupleComparator> _tupleComp;   // comparison to use between cells

    size_t _memLimit;           // how big are the sorted runs
    size_t _nStreams;           // how many runs to merge at once
    size_t _pipelineLimit;      // how many runs to allow in the pipeline
    size_t _tupleSize;          // how big is each cell

    // list which accumulates sorted runs
    std::list< boost::shared_ptr<Array> > _results;

    // state to track sort/merge progress and jobs
    Mutex  _sortLock;
    Event  _sortEvent;
    size_t _nRunningJobs;       
    size_t _runsProduced;
    std::vector<bool> _partitionComplete;
    std::vector< boost::shared_ptr<SortIterators> > _sortIterators;
    std::vector< boost::shared_ptr<Job> > _runningJobs;
    std::vector< boost::shared_ptr<Job> > _waitingJobs;
    std::vector< boost::shared_ptr<Job> > _stoppedJobs;
    boost::shared_ptr<Job> _failedJob;

    /**
     * Whether the cell positions of all records need to be preserved.
     */
    bool _preservePositions;
};

} //namespace scidb

#endif /* SORT_ARRAY_H */
