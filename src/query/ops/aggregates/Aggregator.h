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
 * AggPartitioningOperator.h
 *
 *  Created on: Jul 25, 2011
 *      Author: poliocough@gmail.com
 */

#ifndef _AGGREGATOR_H
#define _AGGREGATOR_H

#include "query/Operator.h"
#include "util/arena/Vector.h"
#include "array/Metadata.h"
#include "array/MemArray.h"
#include "array/TileIteratorAdaptors.h"
#include "query/QueryProcessor.h"
#include "network/NetworkManager.h"
#include "query/Aggregate.h"
#include "array/DelegateArray.h"
#include "system/Sysinfo.h"

#include <boost/unordered_map.hpp>
#include <boost/foreach.hpp>
#include <log4cxx/logger.h>

namespace scidb
{

// Logger for operator. static to prevent visibility of variable outside of file
static log4cxx::LoggerPtr aggLogger(log4cxx::Logger::getLogger("scidb.qproc.aggregator"));

class FinalResultChunkIterator : public DelegateChunkIterator
{
private:
    AggregatePtr _agg;
    Value _outputValue;

public:
    FinalResultChunkIterator (DelegateChunk const* sourceChunk, int iterationMode, AggregatePtr const& agg):
       DelegateChunkIterator(sourceChunk, iterationMode), _agg(agg->clone()), _outputValue(_agg->getResultType())
    {}

    virtual ~FinalResultChunkIterator()
    {}

    virtual Value &getItem()
    {
        Value input = inputIterator->getItem();
        _agg->finalResult(_outputValue, input);
        return _outputValue;
    }
};

class FinalResultMapCreator : public DelegateChunkIterator, protected CoordinatesMapper
{
private:
    RLEEmptyBitmap _bm;
    RLEEmptyBitmap::iterator _iter;
    Value _boolValue;
    Coordinates _coords;

public:
    FinalResultMapCreator(DelegateChunk const* sourceChunk, int iterationMode):
        DelegateChunkIterator(sourceChunk, iterationMode), CoordinatesMapper(*sourceChunk), _bm(NULL, 0)
    {
        ConstChunk const& srcChunk = sourceChunk->getInputChunk();
        PinBuffer scope(srcChunk);
        ConstRLEPayload payload((char*)srcChunk.getData());
        ConstRLEPayload::iterator iter = payload.getIterator();
        while (!iter.end())
        {
            if(iter.isNull() && iter.getMissingReason()==0)
            {}
            else
            {
                RLEEmptyBitmap::Segment seg;
                seg._lPosition=iter.getPPos();
                seg._pPosition=iter.getPPos();
                seg._length = iter.getSegLength();
                _bm.addSegment(seg);
            }
            iter.toNextSegment();
        }
        _iter = _bm.getIterator();
        _boolValue.setBool(true);
        _coords.resize(sourceChunk->getArrayDesc().getDimensions().size());
        reset();
    }

    virtual ~FinalResultMapCreator()
    {}

    Value& getItem()
    {
        if(_iter.end())
        {
            throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_NO_CURRENT_ELEMENT);
        }

        return _boolValue;
    }

    bool isEmpty()
    {
        if(_iter.end())
        {
            throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_NO_CURRENT_ELEMENT);
        }

        return false;
    }

    bool end()
    {
        return _iter.end();
    }

    void operator ++()
    {
        ++_iter;
    }

    Coordinates const& getPosition()
    {
        pos2coord(_iter.getLPos(), _coords);
        return _coords;
    }

    bool setPosition(Coordinates const& pos)
    {
        position_t p = coord2pos(pos);
        return _iter.setPosition(p);
    }

    void reset()
    {
        _iter.reset();
    }
};

class EmptyFinalResultChunkIterator : public FinalResultMapCreator
{
private:
    AggregatePtr _agg;
    Value _outputValue;

public:
    EmptyFinalResultChunkIterator (DelegateChunk const* sourceChunk, int iterationMode, AggregatePtr const& agg):
        FinalResultMapCreator(sourceChunk,iterationMode),
        _agg(agg->clone()), _outputValue(_agg->getResultType())
    {}

    virtual ~EmptyFinalResultChunkIterator()
    {}

    virtual Value &getItem()
    {
        inputIterator->setPosition(getPosition());
        Value input = inputIterator->getItem();
        _agg->finalResult(_outputValue, input);
        return _outputValue;
    }
};



class FinalResultArray : public DelegateArray
{
private:
    std::vector <AggregatePtr> _aggs;
    bool _createEmptyMap;
    AttributeID _emptyMapScapegoat;

public:
    FinalResultArray (ArrayDesc const& desc,
                      boost::shared_ptr<Array> const& stateArray,
                      std::vector<AggregatePtr> const& aggs,
                      bool createEmptyMap = false)
      : DelegateArray(desc, stateArray)
      , _aggs(aggs)
      , _createEmptyMap(createEmptyMap)
      , _emptyMapScapegoat(0)
    {
        if(_createEmptyMap)
        {
            if(!desc.getEmptyBitmapAttribute())
            {
                throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION)
                    << "improper use of FinalResultArray";
            }

            for(AttributeID i =0, n=desc.getAttributes().size(); i<n; i++)
            {
                if (_aggs[i].get())
                {
                    _emptyMapScapegoat=i;
                    break;
                }

                if (i==desc.getAttributes().size()-1)
                {
                    throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION)
                        << "improper use of FinalResultArray";
                }
            }
        }
    }

    virtual ~FinalResultArray()
    {}

    virtual DelegateChunk* createChunk(DelegateArrayIterator const* iterator, AttributeID attrID) const
    {
        return new DelegateChunk(*this, *iterator, attrID, false);
    }

    virtual DelegateArrayIterator* createArrayIterator(AttributeID attrID) const
    {
        if(_createEmptyMap && attrID == desc.getEmptyBitmapAttribute()->getId())
        {
            return new DelegateArrayIterator(*this, attrID, inputArray->getConstIterator(_emptyMapScapegoat));
        }
        return new DelegateArrayIterator(*this, attrID, inputArray->getConstIterator(attrID));
    }

    virtual DelegateChunkIterator* createChunkIterator(DelegateChunk const* chunk, int iterationMode) const
    {
        StatisticsScope sScope(_statistics);
        AggregatePtr agg = _aggs[chunk->getAttributeDesc().getId()];
        if (agg.get())
        {
            if(_createEmptyMap)
            {
                return new EmptyFinalResultChunkIterator(chunk, iterationMode, agg);
            }
            else
            {
                return new FinalResultChunkIterator(chunk, iterationMode, agg);
            }
        }
        else if(_createEmptyMap && chunk->getAttributeDesc().getId() == desc.getEmptyBitmapAttribute()->getId())
        {
            return new FinalResultMapCreator(chunk, iterationMode);
        }
        else
        {
            return new DelegateChunkIterator(chunk, iterationMode);
        }
    }
};


struct AggregationFlags
{
    int iterationMode;
    bool countOnly;
    std::vector<bool> shapeCountOverride;
    std::vector<bool> nullBarrier;
};

/**
 * The aggregator computes a distributed aggregation to the input array, based on several parameters.
 * The pieces of the puzzle are:
 *  - one or more AGGREGATE_CALLs in the given parameters
 *  - input schema
 *  - output schema
 *  - the transformCoordinates() function
 */
class AggregatePartitioningOperator : public PhysicalOperator
{
  protected:
     std::vector <AggIOMapping> _ioMappings;
     std::vector <AggregatePtr> _aggs;

  public:
    typedef mgd::vector<Coordinates> CoordsVector;

    AggregatePartitioningOperator(const std::string& logicalName,
                                  const std::string& physicalName,
                                  const Parameters& parameters,
                                  const ArrayDesc& schema)
         : PhysicalOperator(logicalName, physicalName, parameters, schema)
    {}

    virtual ~AggregatePartitioningOperator()
    {}

    virtual bool changesDistribution(std::vector<ArrayDesc> const&) const
    {
        return true;
    }

    virtual ArrayDistribution getOutputDistribution(
            std::vector<ArrayDistribution> const&,
            std::vector< ArrayDesc> const&) const
    {
        return ArrayDistribution(psHashPartitioned);
    }

    virtual void initializeOperator(ArrayDesc const& inputSchema)
    {
        assert(_aggs.empty());
        _aggs.resize(_schema.getAttributes().size());
        AggIOMapping countMapping;

        bool countStar = false;
        AttributeID attID = 0;
        for (size_t i =0, n=_parameters.size(); i<n; i++)
        {
            if (_parameters[i]->getParamType() == PARAM_AGGREGATE_CALL)
            {
                boost::shared_ptr <OperatorParamAggregateCall>const& ac =
                    (boost::shared_ptr <OperatorParamAggregateCall> const&) _parameters[i];
                AttributeID inAttributeId;
                AggregatePtr agg = resolveAggregate(ac, inputSchema.getAttributes(), &inAttributeId);
                _aggs[attID] = agg;

                if (inAttributeId == INVALID_ATTRIBUTE_ID)
                {
                    //this is for count(*) - set it aside in the countMapping pile
                    countStar = true;
                    countMapping.push_back(attID, agg);
                }
                else
                {
                    //is anyone else scanning inAttributeId?
                    size_t k, kn;
                    for(k=0, kn=_ioMappings.size(); k<kn; k++)
                    {
                        if (inAttributeId == _ioMappings[k].getInputAttributeId())
                        {
                            _ioMappings[k].push_back(attID, agg);
                            break;
                        }
                    }

                    if (k == _ioMappings.size())
                    {
                        _ioMappings.push_back(AggIOMapping(inAttributeId, attID, agg));
                    }
                }
                attID++;
            }
        }

        if (countStar)
        {
            //We have things in the countMapping pile - find an input for it
            int64_t minSize = -1;
            size_t j=0;
            if (!_ioMappings.empty())
            {
                //We're scanning other attributes - let's piggyback on one of them (the smallest)
                for (size_t i=0, n=_ioMappings.size(); i<n; i++)
                {
                    size_t attributeSize = inputSchema.getAttributes()[_ioMappings[i].getInputAttributeId()].getSize();
                    if (attributeSize > 0)
                    {
                        if (minSize == -1 || minSize > (int64_t) attributeSize)
                        {
                            minSize = attributeSize;
                            j = i;
                        }
                    }
                }
                _ioMappings[j].merge(countMapping);
            }
            else
            {
                //We're not scanning other attributes - let'pick the smallest attribute out of the input
                int64_t minSize = -1;
                for (size_t i =0, n=inputSchema.getAttributes().size(); i<n; i++)
                {
                    size_t attributeSize = inputSchema.getAttributes()[i].getSize();
                    if (attributeSize > 0 && inputSchema.getAttributes()[i].getType() != TID_INDICATOR)
                    {
                        if (minSize == -1 || minSize > (int64_t) attributeSize)
                        {
                            minSize = attributeSize;
                            j = i;
                        }
                    }
                }
                countMapping.setInputAttributeId(j);
                _ioMappings.push_back(countMapping);
            }
        }
    }

    virtual void transformCoordinates(Coordinates const & inPos, Coordinates & outPos) = 0;

    ArrayDesc createStateDesc()
    {
        Attributes outAttrs;
        for (size_t i=0, n=_schema.getAttributes().size(); i<n; i++)
        {
            if (_schema.getEmptyBitmapAttribute() == NULL || _schema.getEmptyBitmapAttribute()->getId() != i)
            {
                Value defaultNull;
                defaultNull.setNull(0);
                outAttrs.push_back(AttributeDesc (i,
                                                  _schema.getAttributes()[i].getName(),
                                                  _aggs[i]->getStateType().typeId(),
                                                  AttributeDesc::IS_NULLABLE,
                                                  0, std::set<std::string>(), &defaultNull, ""));
            }
        }

        return ArrayDesc(_schema.getName(), outAttrs, _schema.getDimensions(), _schema.getFlags());
    }


    inline void initializeOutput( boost::shared_ptr<ArrayIterator>& stateArrayIterator,
                                  boost::shared_ptr<ChunkIterator>& stateChunkIterator,
                                  Coordinates const& outPos)
    {
        Chunk& stateChunk = stateArrayIterator->newChunk(outPos);
        boost::shared_ptr<Query> query(stateArrayIterator->getQuery());
        stateChunkIterator = stateChunk.getIterator(query);
    }

    inline void setOutputPosition(  boost::shared_ptr<ArrayIterator>& stateArrayIterator,
                                    boost::shared_ptr<ChunkIterator>& stateChunkIterator,
                                    Coordinates const& outPos)
    {
        if (stateChunkIterator.get() == NULL)
        {
            initializeOutput(stateArrayIterator, stateChunkIterator, outPos);
        }

        if (!stateChunkIterator->setPosition(outPos))
        {
            stateChunkIterator->flush();
            if (!stateArrayIterator->setPosition(outPos))
            {
                initializeOutput(stateArrayIterator, stateChunkIterator, outPos);
            }
            else
            {
                Chunk& stateChunk = stateArrayIterator->updateChunk();
                boost::shared_ptr<Query> query(stateArrayIterator->getQuery());
                stateChunkIterator = stateChunk.getIterator(query, ChunkIterator::APPEND_CHUNK);
            }
            if (!stateChunkIterator->setPosition(outPos))
                throw SYSTEM_EXCEPTION(SCIDB_SE_QPROC, SCIDB_LE_OPERATION_FAILED) << "setPosition";
        }
    }

    AggregationFlags composeFlags(boost::shared_ptr<Array> const& inputArray, AggIOMapping const& mapping)
    {
        AttributeID inAttID = mapping.getInputAttributeId();
        AttributeDesc const& inputAttributeDesc = inputArray->getArrayDesc().getAttributes()[inAttID];

        bool arrayEmptyable = (inputArray->getArrayDesc().getEmptyBitmapAttribute() != NULL);
        bool attributeNullable = inputAttributeDesc.isNullable();

        bool countOnly = true;
        bool readZeroes = false;
        bool readNulls = false;

        size_t const nAggs = mapping.size();

        //first pass: set countOnly, iterateWithoutZeroes, iterateWithoutNulls
        for(size_t i =0; i<nAggs; i++)
        {
            AggregatePtr agg = mapping.getAggregate(i);
            if (agg->isCounting() == false)
            {
                countOnly = false;
                if(agg->ignoreZeroes() == false)
                {   readZeroes = true; }
                if(agg->ignoreNulls() == false && attributeNullable)
                {   readNulls = true;  }
            }
            else
            {
                CountingAggregate* cagg = (CountingAggregate*) agg.get();
                if (cagg->needsAccumulate())
                {   countOnly = false;  }
                if (arrayEmptyable) //we can't infer count from shape
                {
                    readZeroes = true;
                    if (cagg->ignoreNulls()==false && attributeNullable) //nulls must be included in count
                    {   readNulls = true; }
                }
                else if (attributeNullable && cagg->ignoreNulls())
                {   readNulls=true; readZeroes = true; }
            }
        }

        std::vector<bool> shapeCountOverride (nAggs,false);
        std::vector<bool> nullBarrier(nAggs,false);

        for(size_t i =0; i<nAggs; i++)
        {
            AggregatePtr agg = mapping.getAggregate(i);
            if(readNulls && agg->ignoreNulls())
            {   nullBarrier[i] = true;    }
            if (agg->isCounting())
            {
                CountingAggregate* cagg = (CountingAggregate*) agg.get();
                if(!arrayEmptyable &&
                   ((attributeNullable && cagg->ignoreNulls() == false) || !attributeNullable) )
                {
                    shapeCountOverride[i] = true;
                }
            }
        }

        AggregationFlags result;
        result.countOnly = countOnly;
        result.iterationMode = ConstChunkIterator::IGNORE_EMPTY_CELLS | ConstChunkIterator::IGNORE_OVERLAPS;
        if (!readNulls)
        {
            result.iterationMode |= ConstChunkIterator::IGNORE_NULL_VALUES;
        }
        if (!readZeroes && inputAttributeDesc.getDefaultValue().isDefault(inputAttributeDesc.getType()))
        {
            result.iterationMode |= ConstChunkIterator::IGNORE_DEFAULT_VALUES;
        }
        result.nullBarrier=nullBarrier;
        result.shapeCountOverride=shapeCountOverride;
        return result;
    }

    AggregationFlags composeGroupedFlags(boost::shared_ptr<Array> const& inputArray, AggIOMapping const& mapping)
    {
        AttributeID inAttID = mapping.getInputAttributeId();
        AttributeDesc const& inputAttributeDesc = inputArray->getArrayDesc().getAttributes()[inAttID];

        bool attributeNullable = inputAttributeDesc.isNullable();

        bool countOnly = false;
        bool readZeroes = false;
        bool readNulls = false;

        size_t const nAggs = mapping.size();

        //first pass: set countOnly, iterateWithoutZeroes, iterateWithoutNulls
        for(size_t i =0; i<nAggs; i++)
        {
            AggregatePtr agg = mapping.getAggregate(i);
            if(agg->ignoreZeroes() == false)
            {   readZeroes = true; }
            if(agg->ignoreNulls() == false && attributeNullable)
            {   readNulls = true;  }
        }

        std::vector<bool> shapeCountOverride (nAggs,false);
        std::vector<bool> nullBarrier(nAggs,false);

        for(size_t i =0; i<nAggs; i++)
        {
            AggregatePtr agg = mapping.getAggregate(i);
            if(readNulls && agg->ignoreNulls())
            {   nullBarrier[i] = true;    }
        }

        AggregationFlags result;
        result.countOnly = countOnly;
        result.iterationMode = ConstChunkIterator::IGNORE_EMPTY_CELLS | ConstChunkIterator::IGNORE_OVERLAPS;
        if (!readNulls)
        {
            result.iterationMode |= ConstChunkIterator::IGNORE_NULL_VALUES;
        }
        if (!readZeroes && inputAttributeDesc.getDefaultValue().isDefault(inputAttributeDesc.getType()))
        {
            result.iterationMode |= ConstChunkIterator::IGNORE_DEFAULT_VALUES;
        }
        result.nullBarrier=nullBarrier;
        result.shapeCountOverride=shapeCountOverride;
        return result;
    }

    void grandCount(Array* stateArray,
                    boost::shared_ptr<Array> & inputArray,
                    AggIOMapping const& mapping,
                    AggregationFlags const& aggFlags)
    {
        boost::shared_ptr<ConstArrayIterator> inArrayIterator =
            inputArray->getConstIterator(mapping.getInputAttributeId());
        size_t nAggs = mapping.size();

        std::vector<uint64_t> counts(nAggs,0);
        bool dimBasedCount = true;
        for(size_t i=0; i<nAggs; i++)
        {
            if(aggFlags.shapeCountOverride[i] == false)
            {
                dimBasedCount = false;
                break;
            }
        }

        if (dimBasedCount)
        {
            while (!inArrayIterator->end())
            {
                {
                    ConstChunk const& chunk = inArrayIterator->getChunk();
                    uint64_t chunkCount = chunk.getNumberOfElements(false);
                    for (size_t i=0; i<nAggs; i++)
                    {
                        counts[i]+=chunkCount;
                    }
                }
                ++(*inArrayIterator);
            }
        }
        else
        {
            while (!inArrayIterator->end())
            {
                {
                    ConstChunk const& chunk = inArrayIterator->getChunk();
                    uint64_t itemCount = 0;
                    uint64_t noNullCount = 0;
                    
                    uint64_t chunkCount = chunk.getNumberOfElements(false);
                    boost::shared_ptr <ConstChunkIterator> inChunkIterator =
                        chunk.getConstIterator(aggFlags.iterationMode);
                    while(!inChunkIterator->end())
                    {
                        Value& v = inChunkIterator->getItem();
                        if(!v.isNull())
                        {
                            noNullCount++;
                        }
                        itemCount++;
                        ++(*inChunkIterator);
                    }
                    for (size_t i=0; i<nAggs; i++)
                    {
                        if (aggFlags.shapeCountOverride[i])
                        {
                            counts[i]+=chunkCount;
                        }
                        else if (aggFlags.nullBarrier[i])
                        {
                            counts[i]+=noNullCount;
                        }
                        else
                        {
                            counts[i]+=itemCount;
                        }
                    }
                }
                ++(*inArrayIterator);
            }
        }

        Coordinates outPos(_schema.getDimensions().size());
        for(size_t i =0, n=outPos.size(); i<n; i++)
        {
            outPos[i]=_schema.getDimensions()[i].getStartMin();
        }

        for(size_t i =0; i<nAggs; i++)
        {
            boost::shared_ptr<ArrayIterator> stateArrayIterator =
                stateArray->getIterator(mapping.getOutputAttributeId(i));
            boost::shared_ptr<ChunkIterator> stateChunkIterator;
            initializeOutput(stateArrayIterator, stateChunkIterator, outPos);
            stateChunkIterator->setPosition(outPos);
            Value state;
            AggregatePtr agg = mapping.getAggregate(i);
            agg->initializeState(state);
            ((CountingAggregate*)agg.get())->overrideCount(state,counts[i]);
            stateChunkIterator->writeItem(state);
            stateChunkIterator->flush();
        }
    }

    void grandTileAggregate(Array* stateArray,
                            boost::shared_ptr<Array> & inputArray,
                            AggIOMapping const& mapping,
                            AggregationFlags const& aggFlags)
    {
        boost::shared_ptr<ConstArrayIterator> inArrayIterator =
            inputArray->getConstIterator(mapping.getInputAttributeId());
        size_t nAggs = mapping.size();
        std::vector<Value> states(nAggs);

        while (!inArrayIterator->end())
        {
            {
                ConstChunk const& inChunk = inArrayIterator->getChunk();
                boost::shared_ptr <ConstChunkIterator> inChunkIterator = inChunk.getConstIterator(
                    ChunkIterator::TILE_MODE|aggFlags.iterationMode);
                while (!inChunkIterator->end())
                {
                    Value &v = inChunkIterator->getItem();
                    RLEPayload *tile = v.getTile();
                    if (tile->count())
                    {
                        for (size_t i=0; i<nAggs; i++)
                        {
                            AggregatePtr agg = mapping.getAggregate(i);
                            if (states[i].getMissingReason() == 0) {
                                agg->initializeState(states[i]);
                            }
                            agg->accumulatePayload(states[i], tile);
                        }
                    }
                    
                    ++(*inChunkIterator);
                }
            }
            ++(*inArrayIterator);
        }

        Coordinates outPos(_schema.getDimensions().size());
        for(size_t i =0, n=outPos.size(); i<n; i++)
        {
            outPos[i]=_schema.getDimensions()[i].getStartMin();
        }

        for(size_t i =0; i<nAggs; i++)
        {
            boost::shared_ptr<ArrayIterator> stateArrayIterator =
                stateArray->getIterator(mapping.getOutputAttributeId(i));
            boost::shared_ptr<ChunkIterator> stateChunkIterator;
            initializeOutput(stateArrayIterator, stateChunkIterator, outPos);
            stateChunkIterator->setPosition(outPos);
            stateChunkIterator->writeItem(states[i]);
            stateChunkIterator->flush();
        }
    }

    /**
     * In a CoordsVector, search the interval [start, end) for the first
     * entry that doesn't match *start .
     */
    static size_t findEndOfRun(
        const AggregatePartitioningOperator::CoordsVector& cv,
        size_t start)
    {
        const Coordinates& runValue = cv[start];
        size_t end = cv.size();
        assert(start <= end);
        for (size_t result = start; result < end; ++result) {
            if (cv[result] != runValue) {
                return result;
            }
        }
        return end;
    }

    /**
     * For each position in tile, compute corresponding output coordinates.
     */
    void computeOutputCoordinates(
        boost::shared_ptr<BaseTile> const& coordsTile,
        CoordsVector& outCoordsVector)
    {
        const size_t N = coordsTile->size();
        outCoordsVector.clear();
        outCoordsVector.reserve(N);

        // The positions tile returned from ...::getData() uses ArrayEncoding.
        Tile<Coordinates, ArrayEncoding>* cTile =
            safe_dynamic_cast<Tile<Coordinates, ArrayEncoding>* >(coordsTile.get());

        Coordinates inCoords;
        Coordinates outCoords(_schema.getDimensions().size());
        for (size_t i = 0; i < N; ++i)
        {
            cTile->at(i, inCoords);
            transformCoordinates(inCoords, outCoords);
            outCoordsVector.push_back(outCoords);
        }
    }

    void groupedTileFixedSizeAggregate(
        Array* stateArray,
        boost::shared_ptr<Array> & inputArray,
        AggIOMapping const& mapping,
        AggregationFlags const& aggFlags,
        size_t attSize)
    {
        const size_t VALUES_PER_TILE =
            Sysinfo::INTEL_L1_DATA_CACHE_BYTES / attSize;

        // Each aggregate will have its own array and chunk iterator.
        // (Note that the index into the AggIOMapping is *not*
        // necessarily equal to the outAttributeID... that's only true
        // if no count() function is involved!  I.e. you cannot assume
        // that mapping.getOutputAttributeId(x) == x.)
        //
        const size_t N_AGGS = mapping.size();
        mgd::vector <boost::shared_ptr<ArrayIterator> > stateArrayIters(N_AGGS);
        mgd::vector <boost::shared_ptr<ChunkIterator> > stateChunkIters(N_AGGS);
        for (size_t i = 0; i < N_AGGS; ++i)
        {
            stateArrayIters[i] = stateArray->getIterator(mapping.getOutputAttributeId(i));
        }

        // Intermediate states for the aggregator functions are keyed by
        // Coordinates.
        //
        typedef mgd::vector<Value> States;
        typedef boost::unordered_map<Coordinates, States>  StateMap;
        StateMap outStateMap;

        // Tiles to hold the input data, the input positions that
        // correspond to each of these data values, and a tile's worth
        // of positions in the OUTPUT, which correspond to each
        // position in the INPUT.
        // 
        boost::shared_ptr<BaseTile> dataTile;
        boost::shared_ptr<BaseTile> inPositionsTile;
        CoordsVector outCoordinates;

        // Need to enforce IGNORE_NULL_VALUES ourselves because this
        // iteration mode is broken (yet the dead code somehow remains
        // to confuse the hell out of us).
        //
        bool noNulls =
            aggFlags.iterationMode & ChunkIterator::IGNORE_NULL_VALUES;

        // Input phase.  For each input chunk...
        boost::shared_ptr<ConstArrayIterator> inArrayIterator =
            inputArray->getConstIterator(mapping.getInputAttributeId());
        while (!inArrayIterator->end())
        {
            // Obtain tile mode input chunk iterator.
            ConstChunk const& chunk = inArrayIterator->getChunk();
            boost::shared_ptr<ConstChunkIterator> rawInChunkIterator =
                chunk.getConstIterator(aggFlags.iterationMode);
            // Wrap the ordinary chunk iterator with a tile mode iterator.
            boost::shared_ptr<Query> query(Query::getValidQueryPtr(_query));
            boost::shared_ptr<ConstChunkIterator> inChunkIterator =
                boost::make_shared<TileConstChunkIterator<boost::shared_ptr<ConstChunkIterator> > >(
                    rawInChunkIterator, query); 

            // Empty chunk?  Next!
            if (inChunkIterator->end()) {
                ++(*inArrayIterator);
                continue;
            }

            // For each tile in the chunk...
            Coordinates cursor = inChunkIterator->getPosition();
            while (!cursor.empty())
            {
                // Get tile data and positions, and compute output positions.
                cursor = inChunkIterator->getData(cursor, VALUES_PER_TILE, dataTile, inPositionsTile);
                if (!dataTile || dataTile->empty()) {
                    assert(cursor.empty());
                    break;
                }
                const size_t TILE_SIZE = dataTile->size();
                computeOutputCoordinates(inPositionsTile, outCoordinates);

                // For each run of identical output coordinates...
                size_t runIndex = 0;
                size_t endOfRun = 0;
                while (endOfRun < TILE_SIZE)
                {
                    // Next run.
                    runIndex = endOfRun;
                    endOfRun = findEndOfRun(outCoordinates, runIndex);

                    // Find the States vector for this output position.
                    const Coordinates& outCoords = outCoordinates[endOfRun - 1];
                    StateMap::iterator states = outStateMap.find(outCoords);
                    if (states == outStateMap.end())
                    {
                        // Need a new States vector with one entry per aggregate.
                        states = outStateMap.insert(std::make_pair(outCoords, States(N_AGGS))).first;

                        // We also need to initialize each state entry from the state chunk iterator,
                        // since prior calls might have placed intermediate state there.
                        for (size_t ag = 0; ag < N_AGGS; ++ag)
                        {
                            setOutputPosition(stateArrayIters[ag], stateChunkIters[ag], outCoords);
                            Value& state = states->second[ag];
                            state = stateChunkIters[ag]->getItem();
                            if (state.getMissingReason() == 0)
                            {
                                // Looks like we are the first to want to write to this position, so
                                // initialize it per its corresponding aggregator.
                                mapping.getAggregate(ag)->initializeState(state);
                            }
                        }
                    }

                    // Aggregate this run of data into the States vector.
                    for (size_t i = runIndex; i < endOfRun; ++i)
                    {
                        Value v;
                        dataTile->at(i, v);

                        // IGNORE_NULL_VALUES logic: generic prohibition.
                        bool isNull = v.isNull();
                        if (isNull && noNulls)
                                continue;

                        for (size_t ag = 0; ag < N_AGGS; ++ag)
                        {
                            // IGNORE_NULL_VALUES logic: per-aggregate prohibition.
                            if (isNull && aggFlags.nullBarrier[ag])
                                continue;

                            Value& state = states->second[ag];
                            mapping.getAggregate(ag)->accumulate(state, v);
                        }
                    }
                }
            }

            // Output phase.  Write out chunk's accumulated aggregate results.
            for (StateMap::const_iterator pos = outStateMap.begin();
                 pos != outStateMap.end();
                 ++pos)
            {
                const Coordinates& coords = pos->first;
                for (size_t ag = 0; ag < N_AGGS; ++ag)
                {
                    const Value& state = pos->second[ag];
                    boost::shared_ptr<ArrayIterator>& aIter = stateArrayIters[ag];
                    boost::shared_ptr<ChunkIterator>& cIter = stateChunkIters[ag];

                    setOutputPosition(aIter, cIter, coords);
                    cIter->writeItem(state);
                }
            }

            outStateMap.clear();
            ++(*inArrayIterator);
        }

        // Finally, flush the chunk iterators.
        for (size_t i = 0; i < N_AGGS; ++i)
        {
            ChunkIterator* cIter = stateChunkIters[i].get();
            if (cIter)
                cIter->flush();
        }
    }


    void grandAggregate(Array* stateArray,
                        boost::shared_ptr<Array> & inputArray,
                        AggIOMapping const& mapping,
                        AggregationFlags const& aggFlags)
    {
        boost::shared_ptr<ConstArrayIterator> inArrayIterator =
            inputArray->getConstIterator(mapping.getInputAttributeId());
        size_t const nAggs = mapping.size();
        Value null;
        null.setNull(0);
        std::vector<Value> states(nAggs,null);
        int64_t chunkCount = 0;
        bool noNulls = aggFlags.iterationMode & ChunkIterator::IGNORE_NULL_VALUES;

        while (!inArrayIterator->end())
        {
            {
                ConstChunk const& inChunk = inArrayIterator->getChunk();
                chunkCount += inChunk.getNumberOfElements(false);
                boost::shared_ptr <ConstChunkIterator> inChunkIterator =
                    inChunk.getConstIterator(aggFlags.iterationMode);
                while (!inChunkIterator->end())
                {
                    Value &v = inChunkIterator->getItem();
                    if (noNulls && v.isNull())
                    {
                        ++(*inChunkIterator);
                        continue;
                    }
                    
                    for (size_t i =0; i<nAggs; i++)
                    {
                        if ( !(aggFlags.nullBarrier[i] && v.isNull()) )
                        {
                            AggregatePtr agg = mapping.getAggregate(i);
                            if(states[i].getMissingReason()==0)
                            {
                                agg->initializeState(states[i]);
                            }
                            agg->accumulate(states[i], v);
                        }
                    }
                    ++(*inChunkIterator);
                }
            }
            ++(*inArrayIterator);
        }

        Coordinates outPos(_schema.getDimensions().size());
        for(size_t i =0, n=outPos.size(); i<n; i++)
        {
            outPos[i]=_schema.getDimensions()[i].getStartMin();
        }

        for(size_t i =0; i<nAggs; i++)
        {
            boost::shared_ptr<ArrayIterator> stateArrayIterator =
                stateArray->getIterator(mapping.getOutputAttributeId(i));
            boost::shared_ptr<ChunkIterator> stateChunkIterator;
            initializeOutput(stateArrayIterator, stateChunkIterator, outPos);
            stateChunkIterator->setPosition(outPos);
            if(aggFlags.shapeCountOverride[i])
            {
                AggregatePtr agg = mapping.getAggregate(i);
                if (states[i].getMissingReason()==0)
                {
                    agg->initializeState(states[i]);
                }
                ((CountingAggregate*)agg.get())->overrideCount(states[i], chunkCount);
            }
            stateChunkIterator->writeItem(states[i]);
            stateChunkIterator->flush();
        }
    }

    void groupedAggregate(Array* stateArray,
                          boost::shared_ptr<Array> & inputArray,
                          AggIOMapping const& mapping,
                          AggregationFlags const& aggFlags)
    {
        boost::shared_ptr<ConstArrayIterator> inArrayIterator =
            inputArray->getConstIterator(mapping.getInputAttributeId());
        size_t const nAggs = mapping.size();

        bool noNulls = aggFlags.iterationMode & ChunkIterator::IGNORE_NULL_VALUES;

        std::vector <boost::shared_ptr<ArrayIterator> > stateArrayIterators(nAggs);
        for (size_t i =0; i<nAggs; i++)
        {
            stateArrayIterators[i] = stateArray->getIterator(mapping.getOutputAttributeId(i));
        }
        std::vector <boost::shared_ptr<ChunkIterator> > stateChunkIterators(nAggs);
        Coordinates outPos(_schema.getDimensions().size());
        while (!inArrayIterator->end())
        {
            {
                boost::shared_ptr <ConstChunkIterator> inChunkIterator =
                    inArrayIterator->getChunk().getConstIterator( aggFlags.iterationMode);
                while (!inChunkIterator->end())
                {
                    transformCoordinates(inChunkIterator->getPosition(), outPos);
                    Value &v = inChunkIterator->getItem();
                    //XXX: Yes this whole thing is over-engineered and needs to be simplified and
                    //adapted to new tile mode next release we hope...
                    for (size_t i =0; i<nAggs; i++)
                    {
                        size_t const aggNum = mapping.getOutputAttributeId(i);
                        setOutputPosition(stateArrayIterators[i], stateChunkIterators[i], outPos);
                        Value& state = stateChunkIterators[i]->getItem();
                        if (state.getMissingReason()==0)
                        {
                            _aggs[aggNum]->initializeState(state);
                        }
                        if (v.isNull() && (noNulls || aggFlags.nullBarrier[i]))
                        {
                            stateChunkIterators[i]->writeItem(state);
                            continue;
                        }
                        _aggs[aggNum]->accumulate(state, v);
                        stateChunkIterators[i]->writeItem(state);
                    }
                    ++(*inChunkIterator);
                }
            }
            ++(*inArrayIterator);
        }

        for (size_t i = 0; i <nAggs; i++)
        {
            if (stateChunkIterators[i].get())
            {
                stateChunkIterators[i]->flush();
            }
        }
    }

    void logMapping(AggIOMapping const& mapping, AggregationFlags const& flags)
    {
        LOG4CXX_DEBUG(aggLogger, "AggIOMapping input " << mapping.getInputAttributeId()
                      << " countOnly " << flags.countOnly
                      << " iterMode " << flags.iterationMode);

        for (size_t i=0, n=mapping.size(); i<n; i++)
        {
            LOG4CXX_DEBUG(aggLogger, ">>aggregate " << mapping.getAggregate(i)->getName()
                          << " outputatt " << mapping.getOutputAttributeId(i)
                          << " nullbarrier " << flags.nullBarrier[i]
                          << " sco " << flags.shapeCountOverride[i]);
        }
    }

    boost::shared_ptr<Array>
    execute(std::vector< boost::shared_ptr<Array> >& inputArrays, boost::shared_ptr<Query> query)
    {
        ArrayDesc const& inArrayDesc = inputArrays[0]->getArrayDesc();
        initializeOperator(inArrayDesc);

        ArrayDesc stateDesc = createStateDesc();
        boost::shared_ptr<MemArray> stateArray (new MemArray(stateDesc,query));
        boost::shared_ptr<Array> inputArray = ensureRandomAccess(inputArrays[0], query);

        if (_schema.getSize()==1)
        {
            for (size_t i=0; i<_ioMappings.size(); i++)
            {
                AggregationFlags aggFlags = composeFlags(inputArray, _ioMappings[i]);
                logMapping(_ioMappings[i],aggFlags);

                if (_tileMode)
                {
                    grandTileAggregate(stateArray.get(),inputArray, _ioMappings[i], aggFlags);
                }
                else
                {
                    if(aggFlags.countOnly)
                    {
                        grandCount(stateArray.get(), inputArray, _ioMappings[i], aggFlags);
                    }
                    else
                    {
                        grandAggregate(stateArray.get(),inputArray, _ioMappings[i], aggFlags);
                    }
                }
            }
        }
        else
        {
            for (size_t i=0, n=_ioMappings.size(); i<n; i++)
            {
                AggregationFlags aggFlags = composeGroupedFlags( inputArray, _ioMappings[i]);
                logMapping(_ioMappings[i], aggFlags);

                size_t attributeSize = inArrayDesc.getAttributes()[_ioMappings[i].getInputAttributeId()].getSize();
                if (inArrayDesc.getAttributes()[_ioMappings[i].getInputAttributeId()].getType() != TID_BOOL
                    && attributeSize > 0)
                {
                    groupedTileFixedSizeAggregate(stateArray.get(), inputArray,
                                                  _ioMappings[i], aggFlags, attributeSize);
                }
                else
                {
                    groupedAggregate(stateArray.get(), inputArray, _ioMappings[i], aggFlags);
                }
            }
        }

        boost::shared_ptr<Array> mergedArray = redistributeAggregate(stateArray, query, _aggs);
        stateArray.reset();

        boost::shared_ptr<Array> finalResultArray (new FinalResultArray(
                                                _schema, mergedArray, _aggs, _schema.getEmptyBitmapAttribute()));
        if (_tileMode)
        {
            return boost::shared_ptr<Array> (new MaterializedArray(
                                          finalResultArray, query, MaterializedArray::RLEFormat));
        }
        return finalResultArray;
    }
};

}  // namespace scidb

#endif
