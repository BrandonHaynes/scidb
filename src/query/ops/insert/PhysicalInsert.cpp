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
 * PhysicalInsert.cpp
 *
 *  Created on: Sep 14, 2012
 *      Author: poliocough@gmail.com
 */


#include "query/Operator.h"
#include "query/QueryProcessor.h"
#include "query/TypeSystem.h"
#include "array/Metadata.h"
#include "array/Array.h"
#include "array/DBArray.h"
#include "array/TransientCache.h"
#include "system/SystemCatalog.h"
#include "network/NetworkManager.h"
#include "smgr/io/Storage.h"
#include "query/Statistics.h"

#include "array/ParallelAccumulatorArray.h"
#include "array/DelegateArray.h"
#include "system/Config.h"
#include "system/SciDBConfigOptions.h"

using namespace std;
using namespace boost;

namespace scidb
{

/**
 * Insert operator.
 */
class PhysicalInsert: public PhysicalOperator
{
private:
    /**
     * Lock over the target array.
     */
    shared_ptr<SystemCatalog::LockDesc> _lock;

    /**
     * Descriptor of previous version. Not initialized if not applicable.
     */
    ArrayDesc _previousVersionDesc;

public:
    /**
    * Vanilla. Same as most operators.
    */
    PhysicalInsert(const string& logicalName, const string& physicalName, const Parameters& parameters, const ArrayDesc& schema):
        PhysicalOperator(logicalName, physicalName, parameters, schema)
    {}

    /**
    * Find the descriptor for the previous version and populate placeHolder with it.
    * @param[out] placeholder the returned descriptor
    */
    void fillPreviousDesc(ArrayDesc& placeholder) const
    {
       string arrayName = ((shared_ptr<OperatorParamReference>&)_parameters[0])->getObjectName();
       if(_schema.getId() == 0) //new version (our version) was not created yet
       {
           SystemCatalog::getInstance()->getArrayDesc(arrayName, LAST_VERSION, placeholder, true);
       }
       else //new version was already created; locate the previous
       {
           VersionID ver = _schema.getVersionId() - 1;
           if (ver == 0)
           {
               return;
           }
           SystemCatalog::getInstance()->getArrayDesc(arrayName, ver, placeholder, true);
       }
    }

    /**
    * Find the descriptor of the previous version if exists.
    * @return the descriptor of the previous version of the target array, NULL if we are inserting into version 1
    */
    ArrayDesc const* getPreviousDesc()
    {
       if(_previousVersionDesc.getUAId() == 0)
       {
           fillPreviousDesc(_previousVersionDesc);
       }

       if(_previousVersionDesc.getVersionId() == 0)
       {
           return NULL;
       }

       return &_previousVersionDesc;
    }

    /**
     * Record the array 't' in the transient array cache. Implements a callback
     * that is suitable for use as a query finalizer.
     */
    static void recordTransient(const MemArrayPtr& t,const QueryPtr& query)
    {
        if (query->wasCommitted())                       // Was committed ok?
        {
            transient::record(t);                        // ...record in cache
        }
    }

    /**
    * Take necessary locks and perform catalog changes. Initialize the internal _schema field to
    * the proper descriptor of the target array.
    * @param query the query context
    */
    void preSingleExecute(shared_ptr<Query> query)
    {
       shared_ptr<const InstanceMembership> membership(Cluster::getInstance()->getInstanceMembership());
       assert(membership);
       if ((membership->getViewId() != query->getCoordinatorLiveness()->getViewId()) ||
           (membership->getInstances().size() != query->getInstancesCount()))
       {
           throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_NO_QUORUM2);
       }

       _lock = make_shared<SystemCatalog::LockDesc>(_schema.getName(),
                                                   query->getQueryID(),
                                                   Cluster::getInstance()->getLocalInstanceId(),
                                                   SystemCatalog::LockDesc::COORD,
                                                   SystemCatalog::LockDesc::WR);

       shared_ptr<Query::ErrorHandler> ptr(new UpdateErrorHandler(_lock));
       query->pushErrorHandler(ptr);

       ArrayDesc parentDesc;
       SystemCatalog::getInstance()->getArrayDesc(_schema.getName(), parentDesc, false); //Must exist, checked at logical phase

       if (parentDesc.isTransient())
       {
           _schema.setIds(parentDesc.getId(),parentDesc.getUAId(),0);
           _lock->setArrayId       (parentDesc.getUAId());
           _lock->setArrayVersion  (0);
           _lock->setArrayVersionId(parentDesc.getId());
           BOOST_VERIFY(SystemCatalog::getInstance()->updateArrayLock(_lock));
           return;
       }

       VersionID newVersion = SystemCatalog::getInstance()->getLastVersion(parentDesc.getId()) + 1;

       _lock->setArrayId(parentDesc.getUAId());
       _lock->setArrayVersion(newVersion);
       bool rc = SystemCatalog::getInstance()->updateArrayLock(_lock);
       SCIDB_ASSERT(rc);

       //Cookalacka: this pattern is another "marvelous invention" that's been adapted from operators store() and redimension_store().
       //It does many marvelous things - it creates the target array name entry in the catalog
       //It also mutates the _schema field! After this, _schema's name changes to a versioned string and _schema's IDs are set.
       //Note also that this is called BEFORE the plan is sent to remote nodes. So in fact THIS is how remote instances find out what the
       //new array ID and name is!
       _schema = ArrayDesc(ArrayDesc::makeVersionedName(_schema.getName(), newVersion), parentDesc.getAttributes(), parentDesc.getDimensions());
       SystemCatalog::getInstance()->addArray(_schema, psHashPartitioned);
       _lock->setArrayVersionId(_schema.getId());
       rc = SystemCatalog::getInstance()->updateArrayLock(_lock);
       SCIDB_ASSERT(rc);
    }

    /**
     * Add entry about newly created version to the catalog.
     * @param query the query context.
     */
    virtual void postSingleExecute(shared_ptr<Query> query)
    {
        assert(_lock);

        if (!_schema.isTransient())
        {
            SystemCatalog::getInstance()->createNewVersion(_schema.getUAId(), _schema.getId());
        }
    }

    /**
     * Get the estimated upper bound of the output array for the optimizer.
     * @param inputBoundaries the boundaries of the input arrays
     * @param inputSchemas the shapes of the input arrays
     * @return inputBoundaries[0] if we're inserting into version 1, else
     *         an intersection of inputBoudnaries[0] with the boundaries of the previous version.
     */
    virtual PhysicalBoundaries getOutputBoundaries(const std::vector<PhysicalBoundaries> & inputBoundaries,
                                                   const std::vector< ArrayDesc> & inputSchemas) const
    {
        ArrayDesc prevVersionDesc;
        fillPreviousDesc(prevVersionDesc);
        if (prevVersionDesc.getVersionId() == 0)
        {
            return inputBoundaries[0];
        }
        else
        {
            Coordinates currentLo = SystemCatalog::getInstance()->getLowBoundary(prevVersionDesc.getId());
            Coordinates currentHi = SystemCatalog::getInstance()->getHighBoundary(prevVersionDesc.getId());
            PhysicalBoundaries currentBoundaries(currentLo, currentHi);
            return currentBoundaries.unionWith(inputBoundaries[0]);
        }
    }

    /**
     * Get the distribution requirement.
     * @return round-robin
     */
    virtual DistributionRequirement getDistributionRequirement (const std::vector< ArrayDesc> & inputSchemas) const
    {
        vector<ArrayDistribution> requiredDistribution;
        requiredDistribution.push_back(ArrayDistribution(psHashPartitioned));
        return DistributionRequirement(DistributionRequirement::SpecificAnyOrder, requiredDistribution);
    }

    /**
     * Internal helper: write a cell from sourceIter to outputIter at pos and set flag to true.
     * @param sourceIter a chunk iterator to write from
     * @param outputIter a chunk iterator to write to
     * @param pos the position where to write the element
     * @param flag variable that is set to true after writing
     */
    void writeFrom(shared_ptr<ConstChunkIterator>& sourceIter, shared_ptr<ChunkIterator>& outputIter, Coordinates const* pos, bool& flag)
    {
        outputIter->setPosition(*pos);
        outputIter->writeItem(sourceIter->getItem());
        flag = true;
    }

    /**
     * Merge previous version chunk with new chunk and insert result into the target chunk.
     * @param query the query context
     * @param materializedInputChunk a materialized chunk from input
     * @param existingChunk an existing chunk from the previous version
     * @param newChunk the newly created blank chunk to be written
     * @param nDims the number of dimensions
     */
    void insertMergeChunk(shared_ptr<Query>& query,
                          ConstChunk* materializedInputChunk,
                          ConstChunk const& existingChunk,
                          Chunk& newChunk,
                          size_t nDims)
    {
        shared_ptr<ConstChunkIterator> inputCIter = materializedInputChunk->getConstIterator(ChunkIterator::IGNORE_EMPTY_CELLS);
        shared_ptr<ConstChunkIterator> existingCIter = existingChunk.getConstIterator(ChunkIterator::IGNORE_EMPTY_CELLS);
        shared_ptr<ChunkIterator> outputCIter = newChunk.getIterator(query, ChunkIterator::NO_EMPTY_CHECK | ChunkIterator::SEQUENTIAL_WRITE);

        Coordinates const* inputPos = inputCIter->end() ? NULL : &inputCIter->getPosition();
        Coordinates const* existingPos = existingCIter->end() ? NULL : &existingCIter->getPosition();

        while ( inputPos || existingPos )
        {
            bool nextInput = false;
            bool nextExisting = false;
            if (inputPos == NULL)
            {
                writeFrom(existingCIter, outputCIter, existingPos, nextExisting);
            }
            else if (existingPos == NULL)
            {
                writeFrom(inputCIter, outputCIter, inputPos, nextInput);
            }
            else
            {
                int64_t res = coordinatesCompare(*inputPos, *existingPos);
                if ( res < 0 )
                {
                    writeFrom(inputCIter, outputCIter, inputPos, nextInput);
                }
                else if ( res > 0 )
                {
                    writeFrom(existingCIter, outputCIter, existingPos, nextExisting);
                }
                else
                {
                    writeFrom(inputCIter, outputCIter, inputPos, nextInput);
                    nextExisting = true;
                }
            }
            if(inputPos && nextInput)
            {
                ++(*inputCIter);
                inputPos = inputCIter->end() ? NULL : &inputCIter->getPosition();
            }
            if(existingPos && nextExisting)
            {
                ++(*existingCIter);
                existingPos = existingCIter->end() ? NULL : &existingCIter->getPosition();
            }
        }
        outputCIter->flush();
    }

    /**
     * Insert inputArray into a nev version based on _schema, update catalog boundaries.
     * @param inputArray the input to insert.
     * @param query the query context
     * @param currentLowBound the current lower-bound coordinates of the data in the previous version
     * @param currentHiBound the current hi-bound coordinates of the data in the previous version
     */
    shared_ptr<Array> performInsertion(shared_ptr<Array>& inputArray, shared_ptr<Query>& query,
                                       Coordinates const& currentLowBound, Coordinates const& currentHiBound,
                                       size_t const nDims)
    {
        size_t nAttrs = _schema.getAttributes().size();

        shared_ptr<Array> dstArray;

        if (_schema.isTransient())
        {
            dstArray = transient::lookup(_schema,query);

            transient::remove(_schema);

            query->pushFinalizer(bind(&recordTransient,static_pointer_cast<MemArray>(dstArray),_1));
        }
        else
        {
            dstArray = DBArray::newDBArray(_schema.getName(), query);
        }

        SCIDB_ASSERT(dstArray->getArrayDesc().getAttributes(true).size() == inputArray->getArrayDesc().getAttributes(true).size());
        assert(dstArray->getArrayDesc().getId()   == _schema.getId());
        assert(dstArray->getArrayDesc().getUAId() == _schema.getUAId());

        query->getReplicationContext()->enableInboundQueue(_schema.getId(), dstArray);

        PhysicalBoundaries bounds(currentLowBound, currentHiBound);
        if (inputArray->getArrayDesc().getEmptyBitmapAttribute() == NULL && _schema.getEmptyBitmapAttribute())
        {
            inputArray = make_shared<NonEmptyableArray>(inputArray);
        }

        vector<shared_ptr<ConstArrayIterator> > inputIters(nAttrs);    //iterators over the input array
        vector<shared_ptr<ConstArrayIterator> > existingIters(nAttrs); //iterators over the data already in the output array
        vector<shared_ptr<ArrayIterator> > outputIters(nAttrs);        //write-iterators into the output array

        for(AttributeID i = 0; i < nAttrs; i++)
        {
            inputIters[i] = inputArray->getConstIterator(i);
            existingIters[i] = dstArray->getConstIterator(i);
            outputIters[i] = dstArray->getIterator(i);
        }

        while(!inputIters[0]->end())
        {
            Coordinates const& pos = inputIters[0]->getPosition();
            bool haveExistingChunk = existingIters[0]->setPosition(pos);
            for(AttributeID i = 0; i < nAttrs; i++)
            {
                if ( haveExistingChunk && i != 0 )
                {
                    existingIters[i]->setPosition(pos);
                }

                ConstChunk const& inputChunk = inputIters[i]->getChunk();
                ConstChunk* matChunk = inputChunk.materialize();
                if(matChunk->count() == 0)
                {
                    break;
                }

                if(haveExistingChunk)
                {
                    insertMergeChunk(query, matChunk, existingIters[i]->getChunk(),
                                     getNewChunk(pos,outputIters[i]),
                                     nDims);
                }
                else
                {
                    outputIters[i]->copyChunk(*matChunk);
                }

                if (i == nAttrs-1)
                {
                    bounds.updateFromChunk(matChunk, _schema.getEmptyBitmapAttribute() == NULL);
                }
            }

            for(AttributeID i = 0; i < nAttrs; i++)
            {
                ++(*inputIters[i]);
            }
        }

        SystemCatalog::getInstance()->updateArrayBoundaries(_schema, bounds);

        if (!_schema.isTransient())
        {
            query->getReplicationContext()->replicationSync(_schema.getId());
            query->getReplicationContext()->removeInboundQueue(_schema.getId());
            StorageManager::getInstance().flush();
        }

        return dstArray;
    }

    Chunk&
    getNewChunk(const Coordinates& chunkPos,
                const shared_ptr<ArrayIterator> & outputIter)
    {
        Chunk* chunk = NULL;
        try {
            chunk = &outputIter->newChunk(chunkPos);
            assert(chunk);
        } catch (const SystemException& err) {
            if (err.getLongErrorCode() != SCIDB_LE_CHUNK_ALREADY_EXISTS ||
                !_schema.isTransient()) {
                throw;
            }
            bool rc = outputIter->setPosition(chunkPos);
            ASSERT_EXCEPTION(rc, "PhysicalInsert::getNewChunk");
            chunk = &outputIter->updateChunk();
            assert(chunk);
        }
        return *chunk;
    }

    /**
     * Runs the insert op.
     * @param inputArrays one-sized list containing the input
     * @param query the query context
     */
    shared_ptr<Array> execute(vector< shared_ptr<Array> >& inputArrays, shared_ptr<Query> query)
    {
        assert(inputArrays.size() == 1);
        VersionID version = _schema.getVersionId();
        string baseArrayName = ArrayDesc::makeUnversionedName(_schema.getName());

        if (_schema.isTransient())
        {
            inputArrays[0].reset(new MemArray(inputArrays[0],query));
        }

        if (!_lock && !_schema.isTransient())
        {
           _lock = shared_ptr<SystemCatalog::LockDesc>(new SystemCatalog::LockDesc(baseArrayName,
                                                                                   query->getQueryID(),
                                                                                   Cluster::getInstance()->getLocalInstanceId(),
                                                                                   SystemCatalog::LockDesc::WORKER,
                                                                                   SystemCatalog::LockDesc::WR));
           _lock->setArrayVersion(version);
           shared_ptr<Query::ErrorHandler> ptr(new UpdateErrorHandler(_lock));
           query->pushErrorHandler(ptr);
           Query::Finalizer f = bind(&UpdateErrorHandler::releaseLock,  _lock, _1);
           query->pushFinalizer(f);
           SystemCatalog::ErrorChecker errorChecker = bind(&Query::validate, query);
           bool rc = SystemCatalog::getInstance()->lockArray(_lock, errorChecker);
           if (!rc)
           {
              throw USER_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_CANT_INCREMENT_LOCK) << baseArrayName;
           }
        }

        size_t nDims = _schema.getDimensions().size();
        Coordinates currentLo(nDims, MAX_COORDINATE);
        Coordinates currentHi(nDims, MIN_COORDINATE);

        if (const ArrayDesc* previousDesc = getPreviousDesc())
        {
            currentLo = SystemCatalog::getInstance()->getLowBoundary(previousDesc->getId());
            currentHi = SystemCatalog::getInstance()->getHighBoundary(previousDesc->getId());
        }

        shared_ptr<Array> dstArray =  performInsertion(inputArrays[0], query, currentLo, currentHi, nDims);

        getInjectedErrorListener().check();
        return dstArray;
    }
};

DECLARE_PHYSICAL_OPERATOR_FACTORY(PhysicalInsert, "insert", "physicalInsert")

}  // namespace ops
