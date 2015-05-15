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
#include "RedimensionCommon.h"
#include <array/SortArray.h>
#include <system/Config.h>
#include "SyntheticDimChunkMerger.h"

namespace scidb
{

using namespace std;
using namespace boost;

const size_t redimMinChunkSize = 1*KiB;
const size_t redimMaxChunkSize = 1*MiB;

log4cxx::LoggerPtr RedimensionCommon::logger(log4cxx::Logger::getLogger("scidb.array.RedimensionCommon"));

void RedimensionCommon::setupMappings(ArrayDesc const& srcArrayDesc,
                                       vector<AggregatePtr> & aggregates,
                                       vector<size_t>& attrMapping,
                                       vector<size_t>& dimMapping,
                                       Attributes const& destAttrs,
                                       Dimensions const& destDims)
{
    assert(aggregates.size() == attrMapping.size());
    assert(_schema.getAttributes(true).size() == aggregates.size());
    assert(_schema.getAttributes().size() == aggregates.size()+1);

    Attributes const& srcAttrs = srcArrayDesc.getAttributes(true);
    Dimensions const& srcDims = srcArrayDesc.getDimensions();

    for(size_t i =1; i<_parameters.size(); i++)
    {
        if(_parameters[i]->getParamType() == PARAM_AGGREGATE_CALL)
        {
            AttributeID inputAttId;
            string aggOutputName;
            AggregatePtr agg = resolveAggregate((shared_ptr<OperatorParamAggregateCall>&) _parameters[i],
                    srcArrayDesc.getAttributes(),
                    &inputAttId,
                    &aggOutputName);

            bool found = false;
            if (inputAttId == (AttributeID) -1)
            {
                inputAttId = 0;
            }

            for (size_t j = 0; j<_schema.getAttributes(true).size(); j++)
            {
                if (_schema.getAttributes()[j].getName() == aggOutputName)
                {
                    aggregates[j] = agg;
                    attrMapping[j] = inputAttId;
                    found = true;
                    break;
                }
            }
            if (!found) {
                throw USER_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_OP_REDIMENSION_STORE_ERROR6) << aggOutputName;
            }
        } else {
            assert(_parameters[i]->getParamType() == PARAM_PHYSICAL_EXPRESSION);
            assert(i==1);
        }
    }

    for (size_t i = 0; i < destAttrs.size(); i++)
    {
        if (aggregates[i].get())
        {//already populated
            continue;
        }

        for (size_t j = 0; j < srcAttrs.size(); j++) {
            if (srcAttrs[j].getName() == destAttrs[i].getName()) {
                attrMapping[i] = j;
                goto NextAttr;
            }
        }
        for (size_t j = 0; j < srcDims.size(); j++) {
            if (srcDims[j].hasNameAndAlias(destAttrs[i].getName())) {
                attrMapping[i] = turnOn(j, FLIP);
                goto NextAttr;
            }
        }
        assert(false); // A dest attribute either comes from a src dimension or a src attribute. Can't reach here.

        NextAttr:;
    }
    for (size_t i = 0; i < destDims.size(); i++) {
        for (size_t j = 0; j < srcDims.size(); j++) {
            if (srcDims[j].hasNameAndAlias(destDims[i].getBaseName())) {
                dimMapping[i] = j;
                goto NextDim;
            }
        }
        for (size_t j = 0; j < srcAttrs.size(); j++) {
            if (destDims[i].hasNameAndAlias(srcAttrs[j].getName())) {
                dimMapping[i] = turnOn(j, FLIP);
                goto NextDim;
            }
        }
        dimMapping[i] = SYNTHETIC;
        NextDim:;
    }
}


size_t RedimensionCommon::mapChunkPosToId(Coordinates const& chunkPos,
                                          ChunkIdMaps& maps)
{
    /* No need to search the map twice.  Try to insert the initial element.
       If it is already there, insert will tell us and provide an iterator
       to the existing element.
     */
    size_t                   chunkId(0);
    size_t                   entries(maps._chunkPosToIdMap.size());
    ChunkIdNumPair           cpair(entries, 1);
    ChunkToIdMap::value_type entry(chunkPos, cpair);

    pair<ChunkToIdMap::iterator, bool> insert_result =
      maps._chunkPosToIdMap.insert(entry);

    ChunkToIdMap::iterator& it = insert_result.first;
    bool&                   newentry = insert_result.second;

    if (newentry)
    {
        maps._idToChunkPosMap[entries] = chunkPos;
        if (_chunkOverheadLimit)
        {
            if ((entries * _chunkOverhead) > (_chunkOverheadLimit * MiB))
            {
                throw USER_EXCEPTION(SCIDB_SE_OPERATOR,
                                     SCIDB_LE_TOO_MANY_CHUNKS_IN_REDIMENSION)
                    << entries << _chunkOverhead << _chunkOverheadLimit;
            }
        }
    }
    else
    {
        ++(it->second.second);
    }
    chunkId = it->second.first;

    return chunkId;
}


Coordinates& RedimensionCommon::mapIdToChunkPos(size_t id, ChunkIdMaps& maps)
{
    return maps._idToChunkPosMap[id];
}


shared_ptr<MemArray> RedimensionCommon::initializeRedimensionedArray(
    shared_ptr<Query> const& query,
    Attributes const& srcAttrs,
    Attributes const& destAttrs,
    vector<size_t> const& attrMapping,
    vector<AggregatePtr> const& aggregates,
    vector< shared_ptr<ArrayIterator> >& redimArrayIters,
    vector< shared_ptr<ChunkIterator> >& redimChunkIters,
    size_t& redimCount,
    size_t const& redimChunkSize)
{
    // Create a 1-D MemArray called 'redimensioned' to hold the redimensioned records.
    // Each cell in the array corresponds to a cell in the destination array,
    // where its position within the destination array is determined by two
    // additional attributes: the destination chunk identifier, and the
    // position within the destination chunk.

    // The schema is adapted from destArrayDesc, with the following differences:
    //    (a) An aggregate field's type is replaced with the source field type, but still uses the name of the dest attribute.
    //        The motivation is that multiple dest aggregate attribute may come from the same source attribute,
    //        in which case storing under the source attribute name would cause a conflict.
    //    (b) Two additional attributes are appended to the end:
    //        (1) 'tmpDestChunkPosition', that stores the location of the item in the dest chunk
    //        (2) 'tmpDestChunkId', that stores the id of the destination chunk
    //
    // The data is derived from the inputarray as follows.
    //    (a) They are "redimensioned".
    //    (b) Each record is stored as a distinct record in the MemArray. For an aggregate field, no aggregation is performed;
    //        For a synthetic dimension, just use dimStartSynthetic.
    //
    // Local aggregation will be performed at a later step, when generating the MemArray called 'beforeRedistribute'.
    // Global aggregation will be performed at the redistributeAggregate() step.
    //

    Dimensions dimsRedimensioned(1);
    Attributes attrsRedimensioned;
    for (size_t i=0; i<destAttrs.size(); ++i) {
        // For aggregate field, store the source data but under the name of the dest attribute.
        // The motivation is that multiple dest aggregate attribute may come from the same source attribute,
        // in which case storing under the source attribute name would cause conflict.
        //
        // An optimization is possible in this special case, to only store the source attribute once.
        // But some unintuitive bookkeeping would be needed.
        // We decide to skip the optimization at least for now.
        if (aggregates[i]) {
            AttributeDesc const& srcAttrForAggr = srcAttrs[ attrMapping[i] ];
            attrsRedimensioned.push_back(AttributeDesc(i,
                                                       destAttrs[i].getName(),
                                                       srcAttrForAggr.getType(),
                                                       srcAttrForAggr.getFlags(),
                                                       srcAttrForAggr.getDefaultCompressionMethod()));
        } else {
            attrsRedimensioned.push_back(destAttrs[i]);
        }
    }
    attrsRedimensioned.push_back(AttributeDesc(destAttrs.size(), "tmpDestPositionInChunk", TID_INT64, 0, 0));
    attrsRedimensioned.push_back(AttributeDesc(destAttrs.size()+1, "tmpDestChunkId", TID_INT64, 0, 0));
    dimsRedimensioned[0] = DimensionDesc("Row", 0, MAX_COORDINATE, redimChunkSize, 0);

    Attributes attrsRedimensionedWithET(attrsRedimensioned);
    attrsRedimensionedWithET.push_back(AttributeDesc(attrsRedimensioned.size(),
                                                     DEFAULT_EMPTY_TAG_ATTRIBUTE_NAME,
                                                     TID_INDICATOR,
                                                     AttributeDesc::IS_EMPTY_INDICATOR,
                                                     0));
    ArrayDesc schemaRedimensioned("",
                                  attrsRedimensionedWithET,
                                  dimsRedimensioned);
    shared_ptr<MemArray> redimensioned(new MemArray(schemaRedimensioned, query));

    // Initialize the iterators
    redimCount = 0;
    redimArrayIters.resize(attrsRedimensioned.size());
    redimChunkIters.resize(attrsRedimensioned.size());
    for (size_t i = 0; i < attrsRedimensioned.size(); i++)
    {
        redimArrayIters[i] = redimensioned->getIterator(i);
    }

    return redimensioned;
}

void RedimensionCommon::appendItemToRedimArray(vector<Value> const& item,
                                               shared_ptr<Query> const& query,
                                               vector< shared_ptr<ArrayIterator> >& redimArrayIters,
                                               vector< shared_ptr<ChunkIterator> >& redimChunkIters,
                                               size_t& redimCount,
                                               size_t const& redimChunkSize)
{
    // if necessary, refresh the chunk iterators
    if (redimCount % redimChunkSize == 0)
    {
        Coordinates chunkPos(1);
        int chunkMode = ChunkIterator::SEQUENTIAL_WRITE;  // important for performance & mem usage
        chunkPos[0] = redimCount;
        for (size_t i = 0; i < redimArrayIters.size(); i++)
        {
            Chunk& chunk = redimArrayIters[i]->newChunk(chunkPos, 0);
            redimChunkIters[i] = chunk.getIterator(query, chunkMode);
            chunkMode |= ChunkIterator::NO_EMPTY_CHECK;  // creat iterator without this flag only for first attr
        }
    }

    // append the item to the current chunks
    for (size_t i = 0; i < item.size(); i++)
    {
        redimChunkIters[i]->writeItem(item[i]);
    }
    redimCount++;

    // flush the current chunks, or advance the iters
    if (redimCount % redimChunkSize == 0)
    {
        for (size_t i = 0; i < redimChunkIters.size(); i++)
        {
            redimChunkIters[i]->flush();
            redimChunkIters[i].reset();
        }
    }
    else
    {
        for (size_t i = 0; i < redimChunkIters.size(); i++)
        {
            ++(*redimChunkIters[i]);
        }
    }
}

// used during PHASE 3
bool RedimensionCommon::updateSyntheticDimForRedimArray(shared_ptr<Query> const& query,
                                                        ArrayCoordinatesMapper const& coordMapper,
                                                        ChunkIdMaps& chunkIdMaps,
                                                        size_t dimSynthetic,
                                                        shared_ptr<MemArray>& redimensioned)
{
    // If there is a synthetic dimension, and if there are duplicates, modify the values
    // (so that the duplicates get distinct coordinates in the synthetic dimension).
    //

    queue< pair<position_t, position_t> > updates;
    bool needsResort = false;
    size_t currChunkId;
    size_t nextChunkId;
    position_t prevPosition;
    position_t currPosition;
    Coordinates currPosCoord(coordMapper.getDims().size());
    size_t chunkIdAttr = redimensioned->getArrayDesc().getAttributes(true).size() - 1;
    size_t posAttr = chunkIdAttr - 1;
    shared_ptr<ConstArrayIterator> arrayChunkIdIter = redimensioned->getConstIterator(chunkIdAttr);
    shared_ptr<ArrayIterator> arrayPosIter = redimensioned->getIterator(posAttr);
    SCIDB_ASSERT(!arrayChunkIdIter->end());
    SCIDB_ASSERT(!arrayPosIter->end());
    shared_ptr<ConstChunkIterator> chunkChunkIdIter = arrayChunkIdIter->getChunk().getConstIterator();
    shared_ptr<ConstChunkIterator> chunkPosReadIter = arrayPosIter->getChunk().getConstIterator();
    shared_ptr<ChunkIterator> chunkPosWriteIter;
    Coordinates lows(coordMapper.getDims().size()), intervals(coordMapper.getDims().size());

    // initialize the previous position value, current chunk id, and lows and intervals
    prevPosition = chunkPosReadIter->getItem().getInt64();
    currChunkId = chunkChunkIdIter->getItem().getInt64();
    coordMapper.chunkPos2LowsAndIntervals(mapIdToChunkPos(currChunkId, chunkIdMaps),
                                          lows,
                                          intervals);
    coordMapper.pos2coordWithLowsAndIntervals(lows, intervals, prevPosition, currPosCoord);
    ++(*chunkPosReadIter);
    ++(*chunkChunkIdIter);

    // scan array from beginning to end
    Coordinates updatePos(1);                           // moved out of inner loop to avoid malloc
    while (!arrayChunkIdIter->end())
    {
        while (!chunkChunkIdIter->end())
        {
            // Are we processing a new output chunk id?
            nextChunkId = chunkChunkIdIter->getItem().getInt64();
            if (nextChunkId != currChunkId)
            {
                prevPosition = chunkPosReadIter->getItem().getInt64();
                currChunkId = nextChunkId;
                coordMapper.chunkPos2LowsAndIntervals(mapIdToChunkPos(currChunkId, chunkIdMaps),
                                                      lows,
                                                      intervals);
                coordMapper.pos2coordWithLowsAndIntervals(lows, intervals, prevPosition, currPosCoord);
                goto nextitem;
            }

            // Are we processing a run of identical positions?
            currPosition = chunkPosReadIter->getItem().getInt64();
            if (currPosition == prevPosition)
            {
                // found a duplicate --- add an update to the list
                pair<position_t, position_t> pu;

                currPosCoord[dimSynthetic]++;
                pu.first = chunkPosReadIter->getPosition()[0];
                pu.second = coordMapper.coord2posWithLowsAndIntervals(lows,
                                                                      intervals,
                                                                      currPosCoord);
                updates.push(pu);

                // make sure the number of duplicates is less than chunk interval (for the synthetic dim)
                if ((currPosCoord[dimSynthetic] - lows[dimSynthetic]) >=
                    intervals[dimSynthetic])
                {
                    throw USER_EXCEPTION(SCIDB_SE_OPERATOR, SCIDB_LE_OP_REDIMENSION_STORE_ERROR7);
                }
            }
            else
            {
                prevPosition = currPosition;
                coordMapper.pos2coordWithLowsAndIntervals(lows, intervals, currPosition, currPosCoord);
            }

        nextitem:
            ++(*chunkPosReadIter);
            ++(*chunkChunkIdIter);
        }

        // At the end of a chunk, process any updates we have accumulated...
        if (!updates.empty())
        {
            needsResort = true;

            // OVERWRITING existing cells
            chunkPosWriteIter = arrayPosIter->updateChunk().getIterator(query,
                                                                        ChunkIterator::APPEND_CHUNK |
                                                                        ChunkIterator::APPEND_EMPTY_BITMAP |
                                                                        ChunkIterator::NO_EMPTY_CHECK);
            while (!updates.empty())
            {
                Value updateVal;

                updatePos[0] = updates.front().first;
                updateVal.setInt64(updates.front().second);
                chunkPosWriteIter->setPosition(updatePos);
                chunkPosWriteIter->writeItem(updateVal);

                updates.pop();
            }
            chunkPosWriteIter->flush();
            chunkPosWriteIter.reset();
        }

        // Goto next chunk
        ++(*arrayPosIter);
        ++(*arrayChunkIdIter);
        if (!arrayChunkIdIter->end())
        {
            chunkChunkIdIter = arrayChunkIdIter->getChunk().getConstIterator();
            chunkPosReadIter = arrayPosIter->getChunk().getConstIterator();
        }
    }

    return needsResort;
}


void RedimensionCommon::appendItemToBeforeRedistribution(ArrayCoordinatesMapper const& coordMapper,
                                                         Coordinates const& lows,
                                                         Coordinates const& intervals,
                                                         Coordinates & tmp,
                                                         position_t prevPosition,
                                                         vector< shared_ptr<ChunkIterator> >& chunkItersBeforeRedist,
                                                         StateVector& stateVector)
{
    // Do nothing if stateVector has nothing in it
    if (stateVector.isValid())
    {
        coordMapper.pos2coordWithLowsAndIntervals(lows, intervals, prevPosition, tmp);

        vector<Value> const& destItem = stateVector.get();
        for (size_t a = 0, s=chunkItersBeforeRedist.size(); a < s; ++a) {
            bool rc = chunkItersBeforeRedist[a]->setPosition(tmp);
            if (!rc) {
                throw USER_EXCEPTION(SCIDB_SE_OPERATOR, SCIDB_LE_INVALID_REDIMENSION_POSITION) << CoordsToStr(tmp);
            }
            chunkItersBeforeRedist[a]->writeItem(destItem[a]);
        }
    }
}

/**
 * A template function that reads a scidb::Value of any integral type, and returns an int64_t value.
 * This is needed when turning an arbitrary integral type attribute to a dimension.
 *
 * @param v   a scidb::Value object, storing a value of type T.
 * @return    the value of type int64_t.
 *
 * @note T==int8_t || T==int16_t || T==int32_t || T==int64_t || T==uint8_t || T==uint16_t || T==uint32_t || T==uint64_t
 *
 */
template<class T>
int64_t integerTypeToInt64(Value const& v)
{
    return static_cast<int64_t>(v.get<T>());
}

typedef int64_t (*IntegerCoercion)(const Value&);

shared_ptr<Array> RedimensionCommon::redimensionArray(shared_ptr<Array>& srcArray,
                                                      vector<size_t> const& attrMapping,
                                                      vector<size_t> const& dimMapping,
                                                      vector<AggregatePtr> const& aggregates,
                                                      shared_ptr<Query> const& query,
                                                      ElapsedMilliSeconds& timing,
                                                      RedistributeMode redistributeMode)
{
    // def of the meta data
    ArrayDesc const& srcArrayDesc = srcArray->getArrayDesc();
    Attributes const& srcAttrs = srcArrayDesc.getAttributes(true); // true = exclude the empty tag
    Attributes const& destAttrs = _schema.getAttributes(true);
    Dimensions const& destDims = _schema.getDimensions();

    // Does the dest array have a synthetic dimension?
    bool hasSynthetic = false;
    size_t dimSynthetic = 0;
    Coordinate dimStartSynthetic = MIN_COORDINATE;
    Coordinate dimEndSynthetic = MAX_COORDINATE;

    for (size_t i=0; i<dimMapping.size(); ++i) {
        if (dimMapping[i] == SYNTHETIC) {
            hasSynthetic = true;
            dimSynthetic = i;
            dimStartSynthetic = destDims[i].getStartMin();
            dimEndSynthetic = dimStartSynthetic + destDims[i].getChunkInterval() - 1;
            SCIDB_ASSERT(dimEndSynthetic>=dimStartSynthetic);
            break;
        }
    }

    // Does the dest array have any aggregate?
    bool hasAggregate = false;
    for (size_t i=0; i<aggregates.size(); ++i) {
        if (aggregates[i]) {
            hasAggregate = true;
            break;
        }
    }

    // Does the dest array have any overlap?
    bool hasOverlap = false;
    for (size_t i = 0; i < destDims.size(); i++) {
        if (destDims[i].getChunkOverlap() != 0) {
            hasOverlap = true;
            break;
        }
    }

    // Initialize 'redimensioned' array
    shared_ptr<MemArray> redimensioned;
    vector< shared_ptr<ArrayIterator> > redimArrayIters;
    vector< shared_ptr<ChunkIterator> > redimChunkIters;
    size_t redimCount = 0;
    size_t redimChunkSize =
        Config::getInstance()->getOption<size_t>(CONFIG_REDIMENSION_CHUNKSIZE);

    if (redimChunkSize > redimMaxChunkSize)
        redimChunkSize = redimMaxChunkSize;
    if (redimChunkSize < redimMinChunkSize)
        redimChunkSize = redimMinChunkSize;

    redimensioned = initializeRedimensionedArray(query,
                                                 srcAttrs,
                                                 destAttrs,
                                                 attrMapping,
                                                 aggregates,
                                                 redimArrayIters,
                                                 redimChunkIters,
                                                 redimCount,
                                                 redimChunkSize);

    SCIDB_ASSERT(redimArrayIters.size() == destAttrs.size() + 2);
    SCIDB_ASSERT(redimChunkIters.size() == destAttrs.size() + 2);

    timing.logTiming(logger, "[RedimensionArray] PHASE 0 'redimensioned' initialized");
    // PHASE 1 - convert to redimensioned form (but not order)

    // Iterate through the input array, generate the output data, and append to the MemArray.
    // Note: For an aggregate field, its source value (in the input array) is used.
    // Note: The synthetic dimension is not handled here. That is, multiple records, that will be differentiated along the synthetic dimension,
    //       are all appended to the 'redimensioned' array with the same 'position'.
    //
    size_t iterAttr = 0;    // one of the attributes from the input array that needs to be iterated

    vector< shared_ptr<ConstArrayIterator> > srcArrayIterators(srcAttrs.size());
    vector< shared_ptr<ConstChunkIterator> > srcChunkIterators(srcAttrs.size());

    // A vector of functors, to be used to get value from an input array and return an int64_t value.
    vector<IntegerCoercion> functorsGetSourceValue(destDims.size());

    // Initialize the source array iters
    for (size_t i = 0; i < destAttrs.size(); i++) {
        size_t j = attrMapping[i];
        if (!isFlipped(j)) {
            if (!srcArrayIterators[iterAttr]) {
                iterAttr = j;
            }
            srcArrayIterators[j] = srcArray->getConstIterator(j);
        }
    }
    for (size_t i = 0; i < destDims.size(); i++) {
        size_t j = dimMapping[i];
        if (isFlipped(j)) {
            j = turnOff(j, FLIP);
            if (!srcArrayIterators[iterAttr]) {
                iterAttr = j;
            }
            srcArrayIterators[j] = srcArray->getConstIterator(j);

            TypeId tid = srcAttrs[j].getType();
            if (tid == TID_INT8) {
                functorsGetSourceValue[i] = &integerTypeToInt64<int8_t>;
            }
            else if (tid == TID_INT16) {
                functorsGetSourceValue[i] = &integerTypeToInt64<int16_t>;
            }
            else if (tid == TID_INT32) {
                functorsGetSourceValue[i] = &integerTypeToInt64<int32_t>;
            }
            else if (tid == TID_INT64) {
                functorsGetSourceValue[i] = &integerTypeToInt64<int64_t>;
            }
            else if (tid == TID_UINT8) {
                functorsGetSourceValue[i] = &integerTypeToInt64<uint8_t>;
            }
            else if (tid == TID_UINT16) {
                functorsGetSourceValue[i] = &integerTypeToInt64<uint16_t>;
            }
            else if (tid == TID_UINT32) {
                functorsGetSourceValue[i] = &integerTypeToInt64<uint32_t>;
            }
            else if (tid == TID_UINT64) {
                functorsGetSourceValue[i] = &integerTypeToInt64<uint64_t>;
            }
            else {
                ASSERT_EXCEPTION(false, "In RedimensionCommon::redimensionArray(), src attr type must be of integer type.");
            }
        }
        else {
            functorsGetSourceValue[i] = &integerTypeToInt64<int64_t>;
        }
    }
    if (!srcArrayIterators[iterAttr]) {
        // If no src attribute needs to be scanned, open one anyways.
        assert(iterAttr == 0);
        srcArrayIterators[0] = srcArray->getConstIterator(0);
    }

    // Start scanning the input
    ArrayCoordinatesMapper arrayCoordinatesMapper(destDims);
    ChunkIdMaps arrayChunkIdMaps;

    Coordinates destPos(destDims.size());                       // in outermost loop to avoid mallocs
    vector<Value> valuesInRedimArray(destAttrs.size()+2);       // in outermost loop to avoid mallocs
    while (!srcArrayIterators[iterAttr]->end())
    {
        // Initialize src chunk iterators
        for (size_t i = 0; i < srcAttrs.size(); i++) {
            if (srcArrayIterators[i]) {
                srcChunkIterators[i] = srcArrayIterators[i]->getChunk().getConstIterator();
            }
        }

        // Initialize the dest
        Coordinates chunkPos;

        // Loop through the chunks content
        while (!srcChunkIterators[iterAttr]->end()) {
            Coordinates const& srcPos = srcChunkIterators[iterAttr]->getPosition();

            // Get the destPos for this item -- for the SYNTHETIC dim, use the same value (dimStartSynthetic) for all.
            size_t nDims = destDims.size();
            for (size_t i = 0; i < nDims; i++) {
                size_t j = dimMapping[i];
                if (isFlipped(j)) {
                    Value const& value = srcChunkIterators[turnOff(j,FLIP)]->getItem();
                    if (value.isNull()) {
                        // a dimension is NULL. Just skip this item.
                        goto ToNextItem;
                    }
                    destPos[i] = (*functorsGetSourceValue[i])(value);
                } else if (j == SYNTHETIC) {
                    destPos[i] = dimStartSynthetic;
                } else {
                    destPos[i] = srcPos[j];
                }
            }

            // sanity check
            for (size_t i=0; i < nDims; ++i) {
                if (destPos[i]<destDims[i].getStartMin() || destPos[i]>destDims[i].getEndMax()) {
                    throw USER_EXCEPTION(SCIDB_SE_OPERATOR, SCIDB_LE_INVALID_REDIMENSION_POSITION) << CoordsToStr(destPos);
                }
            }

            chunkPos = destPos;
            _schema.getChunkPositionFor(chunkPos);

            // Build data (except the last two fields, i.e. position/chunkid) to be written
            for (size_t i = 0; i < destAttrs.size(); i++) {
                size_t j = attrMapping[i];
                if ( isFlipped(j) ) { // if flipped from a dim
                    valuesInRedimArray[i].setInt64( srcPos[turnOff(j, FLIP)] );
                } else { // from an attribute
                    valuesInRedimArray[i] = srcChunkIterators[j]->getItem();
                }
            }

            // Set the last two fields of the data, and append to the redimensioned array
            if (hasOverlap) {
                // OverlappingChunksIterator iterates over the logical space.
                // Per THE REQUEST TO JUSTIFY LOGICAL-SPACE ITERATION (see RegionCoordinatesIterator.h),
                // here is why it is ok.
                // If chunkOverlap = 0, there is only one chunk in the space so it is ok.
                // With non-zero chunkOverlaps, the space includes only the neighbor chunks that need to store
                // a copy of this record. We have no option but to iterate over all of them.
                //
                OverlappingChunksIterator allChunks(destDims, destPos);
                while (!allChunks.end()) {
                    Coordinates const& overlappingChunkPos = allChunks.getPosition();
                    position_t pos = arrayCoordinatesMapper.coord2pos(overlappingChunkPos, destPos);
                    valuesInRedimArray[destAttrs.size()].setInt64(pos);
                    position_t chunkId = mapChunkPosToId(overlappingChunkPos, arrayChunkIdMaps);
                    valuesInRedimArray[destAttrs.size()+1].setInt64(chunkId);
                    appendItemToRedimArray(valuesInRedimArray,
                                           query,
                                           redimArrayIters,
                                           redimChunkIters,
                                           redimCount,
                                           redimChunkSize);

                    // Must increment after overlappingChunkPos is no longer needed, because the increment will modify overlappingChunkPos.
                    ++allChunks;
                }
            } else {
                position_t pos = arrayCoordinatesMapper.coord2pos(chunkPos, destPos);
                valuesInRedimArray[destAttrs.size()].setInt64(pos);
                position_t chunkId = mapChunkPosToId(chunkPos, arrayChunkIdMaps);
                valuesInRedimArray[destAttrs.size()+1].setInt64(chunkId);
                appendItemToRedimArray(valuesInRedimArray,
                                       query,
                                       redimArrayIters,
                                       redimChunkIters,
                                       redimCount,
                                       redimChunkSize);
            }

            // Advance chunk iterators
        ToNextItem:

            for (size_t i = 0; i < srcAttrs.size(); i++) {
                if (srcChunkIterators[i]) {
                    ++(*srcChunkIterators[i]);
                }
            }
        }

        // Advance array iterators
        for (size_t i = 0; i < srcAttrs.size(); i++) {
            if (srcArrayIterators[i]) {
                ++(*srcArrayIterators[i]);
            }
        }
    } // while

    // If there are leftover values, flush the output iters one last time
    if (redimCount % redimChunkSize != 0)
    {
        for (size_t i = 0; i < redimChunkIters.size(); ++i)
        {
            redimChunkIters[i]->flush();
            redimChunkIters[i].reset();
        }
    }
    for (size_t i = 0; i < redimArrayIters.size(); ++i)
    {
        redimArrayIters[i].reset();
    }

    // LOG4CXX_DEBUG(logger, "[RedimensionArray] redimensioned values: ");
    // redimensioned->printArrayToLogger();

    timing.logTiming(logger, "[RedimensionArray] PHASE 1: conversion to redimensioned form (not order)");
    // PHASE 2 - sort "redimensioned" to global order

    // drop the source array
    redimChunkIters.clear();
    redimArrayIters.clear();
    srcChunkIterators.clear();
    srcArrayIterators.clear();
    srcArray.reset();

    // Sort the redimensioned array based on the chunkid, followed by the position in the chunk
    //
    SortingAttributeInfos sortingAttributeInfos(2);
    SortingAttributeInfo k;
    k.columnNo = destAttrs.size() + 1;
    k.ascent = true;
    sortingAttributeInfos[0] = k;
    k.columnNo = destAttrs.size();
    k.ascent = true;
    sortingAttributeInfos[1] = k;

    const bool preservePositions = false;
    SortArray sorter(redimensioned->getArrayDesc(), _arena, preservePositions);
    shared_ptr<TupleComparator> tcomp(boost::make_shared<TupleComparator>(sortingAttributeInfos, redimensioned->getArrayDesc()));
    if (redimCount)
    {
        shared_ptr<MemArray> sortedRedimensioned = sorter.getSortedArray(redimensioned, query, tcomp);
        redimensioned = sortedRedimensioned;
    }

    timing.logTiming(logger, "[RedimensionArray] PHASE 2A: redimensioned sort pass 1");

    // LOG4CXX_DEBUG(logger, "[RedimensionArray] redimensioned sorted values: ");
    // redimensioned->printArrayToLogger();

    // If hasSynthetic, each record with the same position get assigned a distinct value in the synthetic dimension, effectively
    // assigning a distinct position to every record.  After updating the redimensioned array, it will need to be re-sorted.
    //
    if (hasSynthetic && redimCount)
    {
        bool updated = updateSyntheticDimForRedimArray(query,
                                                       arrayCoordinatesMapper,
                                                       arrayChunkIdMaps,
                                                       dimSynthetic,
                                                       redimensioned);
        timing.logTiming(logger, "[RedimensionArray] PHASE 2B: redimensioned updateSynthetic");
        if(updated)
        {
            // LOG4CXX_DEBUG(logger, "[RedimensionArray] redimensioned after update synthetic before sort: ");
            // redimensioned->printArrayToLogger();

            shared_ptr<MemArray> sortedRedimSynthetic = sorter.getSortedArray(redimensioned, query, tcomp);
            redimensioned = sortedRedimSynthetic;
            timing.logTiming(logger, "[RedimensionArray] PHASE 2C: redimensioned sort pass 2");
        }
        // LOG4CXX_DEBUG(logger, "[RedimensionArray] redimensioned after update synthetic after sort2: ");
        // redimensioned->printArrayToLogger();
    }

    timing.logTiming(logger, "[RedimensionArray] PHASE 2: complete");
    // PHASE 3 - aggregate into 'beforeRedistribution'

    // Create a MemArray call 'beforeRedistribution'.
    //
    // The schema is adapted from destArrayDesc as follows:
    //    (a) For an aggregate field, the type is the 'State' of the aggregate, rather than the destination field type.
    //
    // The data is computed as follows:
    //    (a) For an aggregate field, the aggregate state, among all records with the same position, is stored.
    //    (b) If !hasAggregate and !hasSynthetic, for duplicates, only one record is kept.
    //
    // Also, the MemArray has the empty tag, regardless to what the input array has.
    //
    Attributes attrsBeforeRedistribution;
    attrsBeforeRedistribution.reserve(destAttrs.size());

    if (hasAggregate) {
        for (size_t i=0; i<destAttrs.size(); ++i) {
            if (aggregates[i]) {
                attrsBeforeRedistribution.push_back(AttributeDesc(i, destAttrs[i].getName(), aggregates[i]->getStateType().typeId(),
                        destAttrs[i].getFlags(), destAttrs[i].getDefaultCompressionMethod()));
            } else {
                attrsBeforeRedistribution.push_back(destAttrs[i]);
            }
        }
    } else {
        attrsBeforeRedistribution = destAttrs;
    }

    shared_ptr<MemArray> beforeRedistribution = make_shared<MemArray>(
              ArrayDesc(_schema.getName(), addEmptyTagAttribute(attrsBeforeRedistribution), _schema.getDimensions() ),query);

    // Write data from the 'redimensioned' array to the 'beforeRedistribution' array
    //

    // Initialize iterators
    //
    vector<shared_ptr<ArrayIterator> > arrayItersBeforeRedistribution(attrsBeforeRedistribution.size());
    vector<shared_ptr<ChunkIterator> > chunkItersBeforeRedistribution(attrsBeforeRedistribution.size());
    for (size_t i=0; i<destAttrs.size(); ++i)
    {
        arrayItersBeforeRedistribution[i] = beforeRedistribution->getIterator(i);
    }
    vector< shared_ptr<ConstArrayIterator> > redimArrayConstIters(destAttrs.size() + 2);
    vector< shared_ptr<ConstChunkIterator> > redimChunkConstIters(destAttrs.size() + 2);
    for (size_t i = 0; i < redimArrayConstIters.size(); ++i)
    {
        redimArrayConstIters[i] = redimensioned->getConstIterator(i);
    }

    // Initialize current chunk id to a value that is never in the map
    //
    size_t chunkIdAttr = redimArrayConstIters.size() - 1;
    size_t positionAttr = redimArrayConstIters.size() - 2;
    size_t nDestAttrs = _schema.getDimensions().size();
    size_t chunkId = arrayChunkIdMaps._chunkPosToIdMap.size();

    // Coordinates outside of loops to reduce number of mallocs
    Coordinates lows(nDestAttrs), intervals(nDestAttrs), tmp(nDestAttrs);
    Coordinates outputCoord(nDestAttrs);

    // Init state vector and prev position
    StateVector stateVector(aggregates, 0);
    position_t prevPosition = -1;

    // Scan through the items, aggregate (if apply), and write to the MemArray.
    //
    vector<Value> destItem(destAttrs.size());  // moved outside inner loop to avoid repeated malloc
    while (!redimArrayConstIters[0]->end())
    {
        // Set up chunk iters for the input chunk
        for (size_t i = 0; i < redimChunkConstIters.size(); ++i)
        {
            redimChunkConstIters[i] = redimArrayConstIters[i]->getChunk().getConstIterator(i);
        }

        while (!redimChunkConstIters[0]->end())
        {
            // Have we found a new output chunk?
            //
            size_t nextChunkId = redimChunkConstIters[chunkIdAttr]->getItem().getInt64();
            if (chunkId != nextChunkId)
            {
                // Write the left-over stateVector
                //
                appendItemToBeforeRedistribution(arrayCoordinatesMapper,
                                                 lows, intervals, tmp,
                                                 prevPosition,
                                                 chunkItersBeforeRedistribution,
                                                 stateVector);

                // Flush current output iters
                //
                for (size_t i = 0; i < destAttrs.size(); ++i)
                {
                    if (chunkItersBeforeRedistribution[i].get())
                    {
                        chunkItersBeforeRedistribution[i]->flush();
                        chunkItersBeforeRedistribution[i].reset();
                    }
                }

                // Init the coordinate mapper for the new chunk
                //
                chunkId = nextChunkId;
                arrayCoordinatesMapper.chunkPos2LowsAndIntervals(mapIdToChunkPos(chunkId, arrayChunkIdMaps),
                                                                 lows,
                                                                 intervals);

                // Create new chunks and get the iterators.
                // The first non-empty-tag attribute does NOT use NO_EMPTY_CHECK (so as to help take care of the empty tag); Others do.
                //
                int iterMode = ConstChunkIterator::SEQUENTIAL_WRITE;
                for (size_t i=0; i<destAttrs.size(); ++i)
                {
                    Chunk& chunk = arrayItersBeforeRedistribution[i]->newChunk(mapIdToChunkPos(chunkId, arrayChunkIdMaps));
                    chunkItersBeforeRedistribution[i] = chunk.getIterator(query, iterMode);
                    iterMode |= ConstChunkIterator::NO_EMPTY_CHECK;
                }

                // Update prevPosition, reset state vector
                //
                prevPosition = -1;
                stateVector.init();
            }

            // When seeing the first item with a new position, the attribute values in the item are populated into the destItem as follows.
            //  - For a scalar field, the value is copied.
            //  - For an aggregate field, the value is initialized and accumulated.
            //
            // When seeing subsequent items with the same position, the attribute values in the item are populated as follows.
            //  - For a scalar field, the value is ignored (just select the first item).
            //  - For an aggregate field, the value is accumulated.
            //
            for (size_t i = 0; i < destAttrs.size(); ++i)
            {
                destItem[i] = redimChunkConstIters[i]->getItem();
            }

            position_t currPosition = redimChunkConstIters[positionAttr]->getItem().getInt64();
            if (currPosition == prevPosition)
            {
                if (!hasAggregate) {
                    if (redistributeMode==VALIDATED) {
                        arrayCoordinatesMapper.pos2coordWithLowsAndIntervals(lows, intervals, currPosition, outputCoord);
                        throw USER_EXCEPTION(SCIDB_SE_OPERATOR, SCIDB_LE_DATA_COLLISION)
                                                                    << CoordsToStr(outputCoord);
                    }
                    if (!_hasDataIntegrityIssue && logger->isWarnEnabled()) {
                        arrayCoordinatesMapper.pos2coordWithLowsAndIntervals(lows, intervals, currPosition, outputCoord);
                        LOG4CXX_WARN(logger, "RedimensionCommon::redimensionArray: "
                                     << "Data collision is detected at cell position "
                                     << CoordsToStr(outputCoord)
                                     <<  " for attribute ID = " << positionAttr
                                     << ". Add log4j.logger.scidb.array.RedimensionCommon=TRACE to the log4cxx config file for more");
                        _hasDataIntegrityIssue=true;
                    } else if (_hasDataIntegrityIssue && logger->isTraceEnabled()) {
                        arrayCoordinatesMapper.pos2coordWithLowsAndIntervals(lows, intervals, currPosition, outputCoord);
                        LOG4CXX_TRACE(logger, "RedimensionCommon::redimensionArray: "
                                      << "Data collision is detected at cell position "
                                      << CoordsToStr(outputCoord)
                                      <<  " for attribute ID = " << positionAttr);
                    }
                }
                stateVector.accumulate(destItem);
            }
            else
            {
                // Output the previous state vector.
                appendItemToBeforeRedistribution(arrayCoordinatesMapper,
                                                 lows, intervals, tmp,
                                                 prevPosition,
                                                 chunkItersBeforeRedistribution,
                                                 stateVector);

                // record the new prevPosition
                prevPosition = currPosition;

                // Init and accumulate with the current item.
                stateVector.init();
                stateVector.accumulate(destItem);
            }

            // Advance chunk iterators
            for (size_t i = 0; i < redimChunkConstIters.size(); ++i)
            {
                ++(*redimChunkConstIters[i]);
            }
        } // while chunk iterator

        // Advance array iterators
        for (size_t i = 0; i < redimArrayConstIters.size(); ++i)
        {
            ++(*redimArrayConstIters[i]);
        }
    } // while array iterator

    // Flush the leftover statevector
    appendItemToBeforeRedistribution(arrayCoordinatesMapper,
                                     lows, intervals, tmp,
                                     prevPosition,
                                     chunkItersBeforeRedistribution,
                                     stateVector);

    // Flush the chunks one last time
    for (size_t i=0; i<destAttrs.size(); ++i)
    {
        if (chunkItersBeforeRedistribution[i].get())
        {
            chunkItersBeforeRedistribution[i]->flush();
        }
        chunkItersBeforeRedistribution[i].reset();
    }

    for (size_t i=0; i<destAttrs.size(); ++i) {
        arrayItersBeforeRedistribution[i].reset();
        chunkItersBeforeRedistribution[i].reset();
    }

    timing.logTiming(logger, "[RedimensionArray] PHASE 3: [aggregate] and build 'BeforeRedistribution'");

    // PHASE 4 redistribute

    // drop redimensioned
    redimChunkConstIters.clear();
    redimArrayConstIters.clear();
    redimensioned.reset();

    ArrayDesc outSchema(_schema.getName(), _schema.getAttributes(), _schema.getDimensions());

    if( !hasAggregate && redistributeMode!=AGGREGATED) {
        assert(!hasSynthetic);
        const bool enforceDataIntegrity = (redistributeMode==VALIDATED);
        if (!enforceDataIntegrity) {
            // return without redistributing : optimizer will have to insert SG
            // Tigor does not like the physical plan because it SG's everthing into a MemArray, which store
            // must then read and write to the disk again.
            // We want the optimizer to insert a storing SG at this point (which really is terminal -- store isn't needed then)
            // For redim to be correct is must return true changesDistribution() and
            // returnFullChuns() returns false.
            // but that's as much as it can doo
            timing.logTiming(logger, "[RedimStore] PHASE 4: redistribution: non-aggregate early return");
            return beforeRedistribution;
        }
        assert(redistributeMode!=AUTO);

        shared_ptr<Array> array = beforeRedistribution;
        array = redistributeToRandomAccess(array, query, psHashPartitioned,
                                           ALL_INSTANCE_MASK,
                                           shared_ptr<DistributionMapper>(),
                                           0,
                                           shared_ptr<PartitioningSchemaData>(),
                                           enforceDataIntegrity);
        timing.logTiming(logger, "[RedimStore] PHASE 4: redistribution: redistributeToRandomAccess");
        assert(outSchema == array->getArrayDesc());
        return array;
    }

    shared_ptr<Array> afterRedistribution;

    if ( hasSynthetic) {
        RedimInfo redimInfo(hasSynthetic, dimSynthetic, destDims[dimSynthetic]);
        shared_ptr<Array> input(beforeRedistribution);
        afterRedistribution = redistributeWithSynthetic(input, query, &redimInfo);
    } else {
        assert(hasAggregate);
        const bool enforceDataIntegrity = (redistributeMode == VALIDATED);
        shared_ptr<Array> input(beforeRedistribution);

        afterRedistribution = redistributeWithAggregates(input,
                                                         outSchema,
                                                         query,
                                                         enforceDataIntegrity,
                                                         hasOverlap,
                                                         aggregates);
    }
    // drop beforeRedistribution
    chunkItersBeforeRedistribution.clear();
    arrayItersBeforeRedistribution.clear();
    beforeRedistribution.reset();

    timing.logTiming(logger, "[RedimStore] PHASE 4: redistribution: full redistribution");
    assert(outSchema == afterRedistribution->getArrayDesc());
    return  afterRedistribution;
}

shared_ptr<Array>
RedimensionCommon::redistributeWithSynthetic(shared_ptr<Array>& inputArray,
                                             const shared_ptr<Query>& query,
                                             const RedimInfo* redimInfo)
{
    const ArrayDesc& desc = inputArray->getArrayDesc();
    size_t numAttrs = desc.getAttributes().size();
    PartialChunkMergerList chunkMergers(numAttrs);
    for (AttributeID a=0; a < numAttrs; ++a) {
        shared_ptr<MultiStreamArray::PartialChunkMerger> merger =
        boost::make_shared<SyntheticDimChunkMerger>(redimInfo, query->getInstancesCount());
        chunkMergers[a] = merger;
     }

    // regardless of user settings there should be no data collisions with a synthetic dimension
    const bool enforceDataIntegrity = true;
    return redistributeToRandomAccess(inputArray,
                                      query,
                                      chunkMergers,
                                      psHashPartitioned,
                                      ALL_INSTANCE_MASK,
                                      shared_ptr<DistributionMapper>(),
                                      0,
                                      shared_ptr<PartitioningSchemaData>(),
                                      enforceDataIntegrity );
}

/**
 * A partial chunk merger which uses an aggregate function to form a complete chunk.
 * It expects the partial chunks to contain aggreagte state values suitable for using with the Aggregate methods.
 */
class FinalAggregateChunkMerger : public AggregateChunkMerger
{
    const bool _hasOverlap;
    const ArrayDesc* _desc;
public:
    /// Constructor
    FinalAggregateChunkMerger(AggregatePtr const& agg,
                              const ArrayDesc*  desc,
                              bool isEmptyable,
                              bool hasOverlap)
    : AggregateChunkMerger(agg, isEmptyable), _hasOverlap(hasOverlap), _desc(desc) { assert(desc); }

    /// Destructor
    ~FinalAggregateChunkMerger() {}

    /// @see MultiStreamArray::PartialChunkMerger::getMergedChunk
    virtual boost::shared_ptr<MemChunk> getMergedChunk(AttributeID attId,
                                                       const boost::shared_ptr<Query>& query)
    {
        boost::shared_ptr<MemChunk> chunk = AggregateChunkMerger::getMergedChunk(attId, query);
        boost::shared_ptr<MemChunk> finalChunk = boost::make_shared<MemChunk>();

        LOG4CXX_TRACE(RedimensionCommon::logger, "FinalAggregateChunkMerger::getMergedChunk: "
                      << "attId=" << attId
                      <<" old desc= " << chunk->getArrayDesc()
                      <<" new desc=" << (*_desc)
                      <<" pos=" << chunk->getFirstPosition(false));

        Address addr(chunk->getAttributeDesc().getId(), chunk->getFirstPosition(false));
        finalChunk->initialize(&chunk->getArray(),
                   _desc,
                   addr,
                   chunk->getCompressionMethod());

        // src chunk
        shared_ptr<ConstChunkIterator> src = chunk->getConstIterator(ChunkIterator::IGNORE_EMPTY_CELLS);

        const size_t ebmSize(chunk->getBitmapSize());
        if (ebmSize>0) {
            const size_t off = chunk->getSize() - ebmSize;
            shared_ptr<ConstRLEEmptyBitmap> emptyBitmap =
               boost::make_shared<ConstRLEEmptyBitmap>(static_cast<char*>(chunk->getData()) + off);
            finalChunk->setEmptyBitmap(emptyBitmap);
        } else {
            ASSERT_EXCEPTION(false, "Merged chunk has no emptybitmap");
            // Technically, it is not a problem if finalChunk is not a "closure" (i.e. emptybitmap is not appended to the data)
            // and the result of redistribute(redimension()) is materialized (into a MemArray).
            // However, we would like it to also work without full array materialization.
        }

        // dest chunk
        int destMode = ConstChunkIterator::SEQUENTIAL_WRITE |
                       ConstChunkIterator::NO_EMPTY_CHECK |
                       ConstChunkIterator::APPEND_EMPTY_BITMAP;
        boost::shared_ptr<ChunkIterator> dst = finalChunk->getIterator(query, destMode);

        // copy
        Value result;
        size_t count = 0;
        while (!src->end()) {
            ++ count;
            Coordinates const& destPos = src->getPosition();
            bool rc = dst->setPosition(destPos);
            SCIDB_ASSERT(rc);
            _aggregate->finalResult(result, src->getItem());
            dst->writeItem(result);
            ++(*src);
        }
        src.reset();
        dst->flush();
        dst.reset();

        finalChunk->setEmptyBitmap(shared_ptr<ConstRLEEmptyBitmap>());
        finalChunk->setBitmapChunk(NULL);
        assert(finalChunk->getBitmapSize()>0);

        if (!_hasOverlap) { // the count should not include overlapped items; just leave as 0.
            finalChunk->setCount(count);
        } else {
            finalChunk->setCount(0);
        }
        return finalChunk;
    }
};

class FinalETChunkMerger : public MultiStreamArray::DefaultChunkMerger
{
    const ArrayDesc *_desc;
public:
    /// Constructor
    FinalETChunkMerger(const ArrayDesc* desc,
                       bool enforceDataIntegrity)
    : DefaultChunkMerger(enforceDataIntegrity), _desc(desc) { assert(desc); }

    /// Destructor
    ~FinalETChunkMerger() {}

    /// @see MultiStreamArray::PartialChunkMerger::getMergedChunk
    virtual boost::shared_ptr<MemChunk> getMergedChunk(AttributeID attId,
                                                       const shared_ptr<Query>& query)
    {
        assert(attId == _desc->getAttributes().size()-1);
        boost::shared_ptr<MemChunk> chunk = MultiStreamArray::DefaultChunkMerger::getMergedChunk(attId, query);
        LOG4CXX_TRACE(RedimensionCommon::logger, "FinalETChunkMerger::getMergedChunk: "
                      << "attId=" << attId
                      <<" old desc= " << chunk->getArrayDesc()
                      <<" new desc=" << (*_desc)
                      <<" pos=" << chunk->getFirstPosition(false));

        chunk->setArrayDesc(_desc);
        return chunk;
    }
};

shared_ptr<Array>
RedimensionCommon::redistributeWithAggregates(shared_ptr<Array>& inputArray,
                                              ArrayDesc const& outSchema,
                                              const shared_ptr<Query>& query,
                                              bool enforceDataIntegrity,
                                              bool hasOverlap,
                                              const vector<AggregatePtr>& aggregates)

{
    const ArrayDesc& desc = inputArray->getArrayDesc();
    const size_t numAttrs = desc.getAttributes().size();
    assert(numAttrs == (aggregates.size()+1));
    const bool isEmptyable = (desc.getEmptyBitmapAttribute() != NULL);

    PartialChunkMergerList chunkMergers(numAttrs);

    shared_ptr<Array> withAggregates = make_shared<MemArray>(outSchema,query);

    for (AttributeID a=0; a < (numAttrs-1); ++a) {
        if (aggregates[a]) {
            shared_ptr<MultiStreamArray::PartialChunkMerger> merger =
               make_shared<FinalAggregateChunkMerger>(aggregates[a], &outSchema, isEmptyable, hasOverlap);
            chunkMergers[a] = merger;
        }
    }
    assert(chunkMergers.size() == numAttrs);
    assert(!chunkMergers[numAttrs-1]);
    chunkMergers[numAttrs-1] = make_shared<FinalETChunkMerger>(&outSchema, enforceDataIntegrity);

    redistributeToArray(inputArray, withAggregates, chunkMergers, NULL,
                        query,
                        psHashPartitioned,
                        ALL_INSTANCE_MASK,
                        shared_ptr<DistributionMapper>(),
                        0,
                        shared_ptr<PartitioningSchemaData>(),
                        enforceDataIntegrity );
     return withAggregates;
}

} //namespace
