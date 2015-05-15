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
 *  SortArray.cpp
 *
 *  Created on: Aug 14, 2013
 *  Based on implementation of operator sort
 */

#include <vector>
#include <list>

#include <array/SortArray.h>
#include <array/Metadata.h>
#include <array/TupleArray.h>
#include <array/MemArray.h>
#include <system/Config.h>
#include <array/MergeSortArray.h>
#include <util/Timing.h>
#include <boost/scope_exit.hpp>

using namespace std;
using namespace boost;

namespace scidb {

    log4cxx::LoggerPtr SortArray::logger(log4cxx::Logger::getLogger("scidb.array.SortArray"));

    /**
     * Helper class SortIterators
     */

    /**
     * Constructor -- create iterators and position them at the correct
     * chunk
     */
    SortArray::SortIterators::SortIterators(boost::shared_ptr<Array>& input,
                                            size_t shift,
                                            size_t step) :
        _arrayIters(input->getArrayDesc().getAttributes().size()),
        _shift(shift),
        _step(step)
    {
        // Create the iterators
        for (size_t i = 0; i < _arrayIters.size(); i++)
        {
            _arrayIters[i] = input->getConstIterator(i);
        }

        // Position iterators at the correct chunk
        for (size_t j = _shift; j != 0 && !_arrayIters[0]->end(); --j)
        {
            for (size_t i = 0; i < _arrayIters.size(); i++)
            {
                ++(*_arrayIters[i]);
            }
        }
    }

    /**
     * Advance iterators the proper number of chunks
     */
    void SortArray::SortIterators::advanceIterators()
    {
        for (size_t j = _step-1; j != 0 && !_arrayIters[0]->end(); --j)
        {
            for (size_t i = 0; i < _arrayIters.size(); i++)
            {
                ++(*_arrayIters[i]);
            }
        }
    }


    /**
     * Helper class SortJob
     */

    /**
     * The input array may not have an empty tag,
     * but the output array has an empty tag.
     */
    SortArray::SortJob::SortJob(SortArray& sorter,
                                shared_ptr<Query> query,
                                size_t id,
                                SortIterators& iters)
        : Job(query),
          _sorter(sorter),
          _sortIters(iters),
          _complete(false),
          _id(id)
    {
    }

    /**
     * Here we try to partition part of the array into manageable sized chunks
     * and then sort them in-memory.  Each resulting sorted run is converted to
     * a MemArray and pushed onto the result list.  If we run out of input, or
     * we reach the limit on the size of the result list, we stop
     */
    void SortArray::SortJob::run()
    {
        // At the end of run(), we must always mark ourselves on the stopped
        // job list and signal the main thread.
        BOOST_SCOPE_EXIT ( (&_sorter) (&_id) )
        {
            ScopedMutexLock sm(_sorter._sortLock);

            _sorter._stoppedJobs[_id] = _sorter._runningJobs[_id];
            _sorter._runningJobs[_id].reset();
            _sorter._nRunningJobs--;
            _sorter._sortEvent.signal();
        } BOOST_SCOPE_EXIT_END;

        // TupleArray must handle the case that outputDesc.getAttributes().size()
        // is 1 larger than arrayIterators.size(), i.e. the case when the input array
        // does not have an empty tag (but the output array does).
        //
        size_t tupleArraySizeHint = _sorter._memLimit / _sorter._tupleSize;
        shared_ptr<TupleArray> buffer =
            make_shared<TupleArray>(*(_sorter._outputSchema),
                                    _sortIters.getIterators(),
                                    _sorter.getInputArrayDesc(),
                                    0,
                                    tupleArraySizeHint,
                                    10*MiB,
                                    _sorter._arena,
                                    _sorter.preservePositions());

        // Append chunks to buffer until we run out of input or reach limit
        bool limitReached = false;
        while (!_sortIters.end() && !limitReached)
        {
            buffer->append(_sorter.getInputArrayDesc(), _sortIters.getIterators(), 1);
            size_t currentSize = buffer->getNumberOfTuples() * _sorter._tupleSize;
            if (currentSize > _sorter._memLimit)
            {
                shared_ptr<Array> baseBuffer =
                    static_pointer_cast<TupleArray, Array> (buffer);
                buffer->sort(_sorter._tupleComp);
                buffer->truncate();
                {
                    shared_ptr<Array> ma(new MemArray(baseBuffer, getQuery()));
                    ScopedMutexLock sm(_sorter._sortLock);
                    _sorter._results.push_back(ma);
                    _sorter._runsProduced++;
                    LOG4CXX_DEBUG(logger, "[SortArray] Produced sorted run # " << _sorter._runsProduced);
                    if (_sorter._results.size() > _sorter._pipelineLimit)
                    {
                        limitReached = true;
                    }
                }
                if (limitReached) {
                    buffer.reset();
                } else {
                    buffer.reset(new TupleArray(*(_sorter._outputSchema),
                                                _sortIters.getIterators(),
                                                _sorter._inputSchema,
                                                0,
                                                tupleArraySizeHint,
                                                10*MiB,
                                                _sorter._arena,
                                                _sorter.preservePositions()));
                }
            }
            _sortIters.advanceIterators();
        }

        // May have some left-overs --- only in the case where we are at the end
        if (_sortIters.end())
        {
            if (buffer && buffer->getNumberOfTuples())
            {
                shared_ptr<Array> baseBuffer =
                    static_pointer_cast<TupleArray, Array> (buffer);
                buffer->sort(_sorter._tupleComp);
                buffer->truncate();
                {
                    shared_ptr<Array> ma(new MemArray(baseBuffer, getQuery()));
                    ScopedMutexLock sm(_sorter._sortLock);
                    _sorter._results.push_back(ma);
                    _sorter._runsProduced++;
                    LOG4CXX_DEBUG(logger, "[SortArray] Produced sorted run # " << _sorter._runsProduced);
                }
            }
            _complete = true;
        }
    }


    /**
     * Helper class MergeJob
     */

    /**
     * Constructor
     */
    SortArray::MergeJob::MergeJob(SortArray& sorter,
                                  boost::shared_ptr<Query> query,
                                  size_t id) :
        Job(query),
        _sorter(sorter),
        _id(id)
    {
    }

    /**
     * Remove a group of arrays from the list, merge them using a MergeSortArray,
     * then add the result back to the end of the list.
     */
    void SortArray::MergeJob::run()
    {
        vector< shared_ptr<Array> > mergeStreams;
        shared_ptr<Array> merged;
        shared_ptr<Array> materialized;

        // At the end of run(), we must always put the result (if it exists) on the end of the
        // list, mark ourselves on the stopped job list, and signal the main thread
        BOOST_SCOPE_EXIT ( (&materialized) (&_sorter) (&_id) )
        {
            ScopedMutexLock sm(_sorter._sortLock);

            if (materialized.get())
            {
                _sorter._results.push_back(materialized);
            }
            _sorter._stoppedJobs[_id] = _sorter._runningJobs[_id];
            _sorter._runningJobs[_id].reset();
            _sorter._nRunningJobs--;
            _sorter._sortEvent.signal();
        } BOOST_SCOPE_EXIT_END;

        // remove the correct number of streams from the list
        {
            ScopedMutexLock sm(_sorter._sortLock);

            size_t nSortedRuns = _sorter._results.size();
            size_t currentStreams =
                nSortedRuns < _sorter._nStreams ? nSortedRuns : _sorter._nStreams;
            mergeStreams.resize(currentStreams);

            LOG4CXX_DEBUG(logger, "[SortArray] Found " << currentStreams << " runs to merge");

            for (size_t i = 0; i < currentStreams; i++)
            {
                mergeStreams[i] = _sorter._results.front();
                _sorter._results.pop_front();
            }
        }

        // merge the streams -- true means the array contains local data only
        size_t nStreams = mergeStreams.size();
        shared_ptr<vector<size_t> > streamSizes = shared_ptr<vector<size_t> >(new vector<size_t>(nStreams));
        for (size_t i=0; i<nStreams; ++i) {
            (*streamSizes)[i] = -1;
        }
        merged.reset(new MergeSortArray(getQuery(),
                                        *(_sorter._outputSchema),
                                        mergeStreams,
                                        _sorter._tupleComp,
                                        0,  // Do not add an offset to the cell's coordinates.
                                        streamSizes // Using -1 preserves the behavior of the original code here.
                                        ));

        // false means perform a horizontal copy (copy all attributes for chunk 1,
        // all attributes for chunk 2,...)
        materialized.reset(new MemArray(merged, getQuery(), false));
    }


    /**
     * Output schema matches input attributes, but contains only one dimension, "n".
     * An emptytag attribute is added to the schema if it doesn't already exist.
     * ChunkSize is specified in the constructor and used here.
     */
    void SortArray::calcOutputSchema(const ArrayDesc& inputSchema, size_t chunkSize)
    {
        //Let's always call the output dimension "n". Because the input dimension no longer has any meaning!
        //It could've been "time", or "latitude", or "price" but after the sort it has no value.

        //Right now we always return an unbounded array. You can use subarray to bound it if you need to
        //(but you should not need to very often!!). TODO: if we return bounded arrays, some logic inside
        //MergeSortArray gives bad results. We should fix this some day.

        //If the user does not specify a chunk size, we'll use MIN( max_logical_size, 1 million).

        size_t inputSchemaSize = inputSchema.getSize();
        if (chunkSize == 0)
        {   //not set by user

            //1M is a good/recommended chunk size for most one-dimensional arrays -- unless you are using
            //large strings or UDTs
            chunkSize = 1000000;

            //If there's no way that the input has one million elements - reduce the chunk size further.
            //This is ONLY done for aesthetic purposes - don't want to see one million "()" on the screen.
            //In fact, sometimes it can become a liability...
            if(inputSchemaSize<chunkSize)
            {
                //make chunk size at least 1 to avoid errors
                chunkSize = std::max<size_t>(inputSchemaSize,1);
            }
        }

        Dimensions newDims(1);
        newDims[0] = DimensionDesc("n", 0, 0, MAX_COORDINATE, MAX_COORDINATE, chunkSize, 0);

        const bool excludeEmptyBitmap = true;
        Attributes attributes = inputSchema.getAttributes(excludeEmptyBitmap);
        size_t nAttrsIn = attributes.size();
        if (_preservePositions) {
            attributes.push_back(AttributeDesc(
                            nAttrsIn,
                            "chunk_pos",
                            TID_INT64,
                            0, // flags
                            0  // defaultCompressionMethod
                            ));
            attributes.push_back(AttributeDesc(
                            nAttrsIn+1,
                            "cell_pos",
                            TID_INT64,
                            0, // flags
                            0  // defaultCompressionMethod
                            ));
        }
        _outputSchema.reset(new ArrayDesc(inputSchema.getName(),
                                          addEmptyTagAttribute(attributes),
                                          newDims));
    }


    /***
     * Sort works by first transforming the input array into a series of sorted TupleArrays.
     * Then the TupleArrays are linked under a single MergeSortArray which encodes the merge
     * logic within its iterator classes.  To complete the sort, we materialize the merge
     * Array.
     */
    shared_ptr<MemArray> SortArray::getSortedArray(boost::shared_ptr<Array> inputArray,
                                                   boost::shared_ptr<Query> query,
                                                   boost::shared_ptr<TupleComparator> tcomp)
    {
        // Timing for Sort
        LOG4CXX_DEBUG(logger, "[SortArray] Sort for array " << _outputSchema->getName() << " begins");
        ElapsedMilliSeconds timing;

        // Init config parameters
        size_t numJobs = inputArray->getSupportedAccess() == Array::RANDOM ?
	           Config::getInstance()->getOption<int>(CONFIG_RESULT_PREFETCH_QUEUE_SIZE) : 1;
        _memLimit = Config::getInstance()->getOption<int>(CONFIG_MERGE_SORT_BUFFER)*MiB;
        _nStreams = Config::getInstance()->getOption<int>(CONFIG_MERGE_SORT_NSTREAMS);
        _pipelineLimit = Config::getInstance()->getOption<int>(CONFIG_MERGE_SORT_PIPELINE_LIMIT);

        // Validate config parameters
        if (_pipelineLimit <= 1)
        {
            _pipelineLimit = 2;
        }
        if (_nStreams <= 1)
        {
            _nStreams = 2;
        }
        if (_pipelineLimit < _nStreams)
        {
            _pipelineLimit = _nStreams;
        }

        // Init sorting state
        shared_ptr<JobQueue> queue = PhysicalOperator::getGlobalQueueForOperators();
        _input = inputArray;
        _tupleComp = tcomp;
        _tupleSize = TupleArray::getTupleFootprint(_outputSchema->getAttributes());
        _nRunningJobs = 0;
	    _runsProduced = 0;
        _partitionComplete.resize(numJobs);
        _waitingJobs.resize(numJobs);
        _runningJobs.resize(numJobs);
        _stoppedJobs.resize(numJobs);
        _sortIterators.resize(numJobs);

        // Create the iterator groups and sort jobs
        for (size_t i = 0; i < numJobs; i++)
        {
            _sortIterators[i] =
                shared_ptr<SortIterators>(new SortIterators(_input, i, numJobs));
            _waitingJobs[i] =
                shared_ptr<Job>(new SortJob(*this,
                                            query,
                                            i,
                                            *(_sortIterators[i])));
            _partitionComplete[i] = false;
        }

        // Main loop
        while (true)
        {
            Event::ErrorChecker ec;
            ScopedMutexLock sm(_sortLock);

            // Try to spawn jobs
            for (size_t i = 0; i < numJobs; i++)
            {
                if (_waitingJobs[i])
                {
                    _runningJobs[i] = _waitingJobs[i];
                    _waitingJobs[i].reset();
                    _nRunningJobs++;
                    queue->pushJob(_runningJobs[i]);
                }
            }

            // If nothing is running, get out
            if (_nRunningJobs == 0)
            {
                break;
            }

            // Wait for a job to be done
            _sortEvent.wait(_sortLock, ec);

            // Reap the stopped jobs and re-schedule
            for (size_t i = 0; i < numJobs; i++)
            {
                if (_stoppedJobs[i])
                {
                    enum nextJobType { JobNone, JobMerge, JobSort };

                    nextJobType nextJob = JobNone;
                    bool jobSuccess = true;
                    shared_ptr<SortJob> sJob;

                    jobSuccess = _stoppedJobs[i]->wait();

                    if (!_failedJob && jobSuccess)
                    {
                        sJob = dynamic_pointer_cast<SortJob, Job>(_stoppedJobs[i]);
                        if (sJob.get() && sJob->complete())
                        {
                            _partitionComplete[i] = true;
                        }

                        if (_partitionComplete[i])
                        {
                            // partition is complete, schedule the merge if necessary
                            if (_results.size() > _nStreams)
                            {
                                nextJob = JobMerge;
                            }
                        }
                        else
                        {
                            // partition is not complete, schedule the sort if we can,
                            // or the merge if we must
                            if (_results.size() < _pipelineLimit)
                            {
                                nextJob = JobSort;
                            }
                            else
                            {
                                nextJob = JobMerge;
                            }
                        }

                        if (nextJob == JobSort)
                        {
                            _waitingJobs[i].reset(new SortJob(*this,
                                                              query,
                                                              i,
                                                              *(_sortIterators[i])));
                        }
                        else if (nextJob == JobMerge)
                        {
                            _waitingJobs[i].reset(new MergeJob(*this,
                                                               query,
                                                               i));
                        }
                    }
                    else
                    {
                        // An error occurred.  Save this job so we can re-throw the exception
                        // when the rest of the jobs have stopped.
                        if (!jobSuccess)
                        {
                            _failedJob = _stoppedJobs[i];
                        }
                    }
                    _stoppedJobs[i].reset();
                }
            }
        }

        // If there is a failed job, we need to just get out of here
        if (_failedJob)
        {
            _failedJob->rethrow();
        }

        // If there were no failed jobs, we still may need one last merge
        if (_results.size() > 1)
        {
            shared_ptr<Job> lastJob(new MergeJob(*this,
                                                 query,
                                                 0));
            queue->pushJob(lastJob);
            lastJob->wait(true);
        }

        timing.logTiming(logger, "[SortArray] merge sorted chunks complete");

        // Return the result
        if (_results.size() == 0)
        {
            shared_ptr<Array> materialized(new MemArray(*_outputSchema, query));
            _results.push_back(materialized);
        }

        shared_ptr<MemArray> ret = dynamic_pointer_cast<MemArray, Array> (_results.front());
        _results.clear();
        return ret;
    }

}  // namespace scidb
