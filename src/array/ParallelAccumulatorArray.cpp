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
 * @file ParallelAccumulatorArray.cpp
 *
 * @author Konstantin Knizhnik <knizhnik@garret.ru>
 */

#include <stdio.h>
#include <vector>
#include <log4cxx/logger.h>
#include "array/ParallelAccumulatorArray.h"
#include "system/Exceptions.h"
#include "system/Config.h"
#include "system/SciDBConfigOptions.h"
#include "query/Operator.h"

namespace scidb
{
    using namespace boost;
    using namespace std;

    static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.qproc.processor"));

    //
    // ParallelAccumulatorArray
    //
    ConstChunk const* ParallelAccumulatorArray::ChunkPrefetchJob::getResult()
    {
        wait(true, false);
        if (!_resultChunk) {
            throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_NO_CURRENT_CHUNK);
        }
        return _resultChunk;
    }

    void ParallelAccumulatorArray::ChunkPrefetchJob::cleanup()
    {
        _iterator.reset();
    }

    ParallelAccumulatorArray::ChunkPrefetchJob::~ChunkPrefetchJob()
    {
        LOG4CXX_TRACE(logger, "ChunkPrefetchJob::~ChunkPrefetchJob "<<this);
    }

    ParallelAccumulatorArray::ChunkPrefetchJob::ChunkPrefetchJob(const shared_ptr<ParallelAccumulatorArray>& array,
                                                                 AttributeID attr,
                                                                 const shared_ptr<Query>& query)
    : Job(query),
      _queryLink(query),
      _arrayLink(array),
      _iterator(array->pipe->getConstIterator(attr)),
      _attrId(attr),
      _resultChunk(NULL)
    {
        assert(query);
    }

    void ParallelAccumulatorArray::ChunkPrefetchJob::run()
    {
        static int pass = 0; // DEBUG ONLY
        pass++;
        StatisticsScope sScope(_statistics);

        _query = _queryLink.lock();
        if (!_query) {
            return;
        }
        shared_ptr<ParallelAccumulatorArray> acc = _arrayLink.lock();
        if (!acc) {
            return;
        }

        try {
            if (_iterator->setPosition(_pos)) {
                ConstChunk const& inputChunk = _iterator->getChunk();

                if (inputChunk.isMaterialized()) {
                    _resultChunk = &inputChunk;
                } else {
                    Address addr(_attrId, inputChunk.getFirstPosition(false));
                    _accChunk.initialize(acc.get(), &acc->desc, addr, inputChunk.getCompressionMethod());
                    _accChunk.setBitmapChunk((Chunk*)&inputChunk);
                    shared_ptr<ConstChunkIterator> src = inputChunk.getConstIterator(ChunkIterator::INTENDED_TILE_MODE |
                                                                                            ChunkIterator::IGNORE_EMPTY_CELLS);
                    shared_ptr<ChunkIterator> dst =
                        _accChunk.getIterator(_query,
                                              (src->getMode() & ChunkIterator::TILE_MODE) |
                                              ChunkIterator::NO_EMPTY_CHECK |
                                              ChunkIterator::SEQUENTIAL_WRITE);
                    size_t count = 0;
                    while (!src->end()) {
                        const Coordinates& srcPos = src->getPosition();
                        if (dst->setPosition(srcPos)) {
                            Value& v = src->getItem();
                            dst->writeItem(v);
                            count += 1;
                        }
                        ++(*src);
                    }
                    if (!acc->desc.hasOverlap()) {
                        _accChunk.setCount(count);
                    }
                    dst->flush();
                    _resultChunk = &_accChunk;
             }
            } else {
                _error = SYSTEM_EXCEPTION_SPTR(SCIDB_SE_EXECUTION, SCIDB_LE_OPERATION_FAILED) << "setPosition";
            }
        } catch (Exception const& x) {
            _error = x.copy();
        }
    }

    ParallelAccumulatorArray::ParallelAccumulatorArray(const shared_ptr<Array>& array)
    : StreamArray(array->getArrayDesc(), false),
      iterators(array->getArrayDesc().getAttributes().size()),
      pipe(array),
      activeJobs(iterators.size()),
      completedJobs(iterators.size())
    {
        if (iterators.size() <= 0) {
            LOG4CXX_FATAL(logger, "Array descriptor arrId = " << array->getArrayDesc().getId()
                          << " name = " << array->getArrayDesc().getId()
                          << " has no attributes ");
            assert(false);
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_INCONSISTENT_ARRAY_DESC);
        }
    }

    void ParallelAccumulatorArray::start(const shared_ptr<Query>& query)
    {
        PhysicalOperator::getGlobalQueueForOperators();
        size_t nAttrs = iterators.size();
        assert(nAttrs>0);
        for (size_t i = 0; i < nAttrs; i++) {
            iterators[i] = pipe->getConstIterator(i);
        }
        int nPrefetchedChunks = Config::getInstance()->getOption<int>(CONFIG_RESULT_PREFETCH_QUEUE_SIZE);
        do {
            for (size_t i = 0; i < nAttrs; i++) {
                shared_ptr<ChunkPrefetchJob> job = make_shared<ChunkPrefetchJob>(shared_from_this(), i, query);
                doNewJob(job);
            }
        } while ((nPrefetchedChunks -= nAttrs) > 0);
    }

    ParallelAccumulatorArray::~ParallelAccumulatorArray()
    {
        LOG4CXX_TRACE(logger, "ParallelAccumulatorArray::~ParallelAccumulatorArray "<<this << ", active jobs #="<<activeJobs.size());
        for (size_t i = 0; i < activeJobs.size(); i++) {
            list< shared_ptr<ChunkPrefetchJob> >& jobs = activeJobs[i];
            for (list< shared_ptr<ChunkPrefetchJob> >::iterator j = jobs.begin(); j != jobs.end(); ++j) {
                (*j)->skip();
            }
        }
    }

    void ParallelAccumulatorArray::doNewJob(shared_ptr<ChunkPrefetchJob>& job)
    {
        AttributeID attrId = job->getAttributeID();
        if (!iterators[attrId]->end()) {
            job->setPosition(iterators[attrId]->getPosition());
            PhysicalOperator::getGlobalQueueForOperators()->pushJob(job);
            activeJobs[attrId].push_back(job);
            ++(*iterators[attrId]);
        }
    }


    ConstChunk const* ParallelAccumulatorArray::nextChunk(AttributeID attId, MemChunk& chunk)
    {
        //XXX TODO: should this method be synchronized ?
        if (completedJobs[attId]) {
            doNewJob(completedJobs[attId]);
            completedJobs[attId].reset();
        }
        if (activeJobs[attId].empty()) {
            return NULL;
        }
        completedJobs[attId] = activeJobs[attId].front();
        activeJobs[attId].pop_front();
        return completedJobs[attId]->getResult();
    }

}
