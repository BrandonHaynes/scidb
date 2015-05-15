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
 * PhysicalVariableWindow.cpp
 *  Created on: Feb 9, 2012
 *      Author: poliocough@gmail.com
 */

#include <map>
#include <query/Aggregate.h>
#include <query/Operator.h>
#include <util/Network.h>
#include <array/Metadata.h>
#include <array/MemArray.h>
#include <log4cxx/logger.h>

#include "VariableWindow.h"

// Logger for operator. static to prevent visibility of variable outside of file
static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.query.ops.variable_window"));

using namespace std;
using namespace boost;

//Let's Assume that each value held in unflushed chunks incurs a 24-byte overhead from pointers, etc.
#define VALUE_BYTE_OVERHEAD (24)

namespace scidb
{

class PhysicalVariableWindow: public PhysicalOperator
{
private:
    size_t _nPreceding;
    size_t _nFollowing;
    size_t _dimNum;
    size_t _nDims;
    scoped_ptr<ChunkInstanceMap> _localChunkMap;
    scoped_ptr<ChunkInstanceMap> _globalChunkMap;
    unordered_map<Coordinates, size_t> _chunkCounts;
    size_t _nInstances;
    InstanceID _coordinatorId;
    InstanceID _myInstanceId;
    ArrayDesc _srcDesc;
    size_t _localCellCount;

    vector<AggIOMapping> collectIOMappings()
    {
        vector<AggIOMapping> resultMappings;
        AggIOMapping countMapping;

        bool countStar = false;
        AttributeID attID = 0;
        for (size_t i =0; i<_parameters.size(); i++)
        {
            if (_parameters[i]->getParamType() == PARAM_AGGREGATE_CALL)
            {
                shared_ptr <OperatorParamAggregateCall>const& ac =
                    (shared_ptr <OperatorParamAggregateCall> const&) _parameters[i];
                AttributeID inAttributeId;
                AggregatePtr agg = resolveAggregate(ac, _srcDesc.getAttributes(), &inAttributeId);
                if (inAttributeId == INVALID_ATTRIBUTE_ID)
                {
                    //this is for count(*) - set it aside in the countMapping pile
                    countStar = true;
                    countMapping.push_back(attID, agg);
                }
                else
                {
                    //is anyone else scanning inAttributeId?
                    size_t k;
                    for(k=0; k<resultMappings.size(); k++)
                    {
                        if (inAttributeId == resultMappings[k].getInputAttributeId())
                        {
                            resultMappings[k].push_back(attID, agg);
                            break;
                        }
                    }

                    if (k == resultMappings.size())
                    {
                        resultMappings.push_back(AggIOMapping(inAttributeId, attID, agg));
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
            if (resultMappings.size())
            {
                //We're scanning other attributes - let's piggyback on one of them (the smallest)
                for (size_t i=0; i<resultMappings.size(); i++)
                {
                    size_t attributeSize =
                        _srcDesc.getAttributes()[resultMappings[i].getInputAttributeId()].getSize();
                    if (attributeSize > 0)
                    {
                        if (minSize == -1 || minSize > (int64_t) attributeSize)
                        {
                            minSize = attributeSize;
                            j = i;
                        }
                    }
                }
                resultMappings[j].merge(countMapping);
            }
            else
            {
                //We're not scanning other attributes - let'pick the smallest attribute out of the input
                int64_t minSize = -1;
                for (size_t i =0; i< _srcDesc.getAttributes().size(); i++)
                {
                    size_t attributeSize = _srcDesc.getAttributes()[i].getSize();
                    if (attributeSize > 0 && _srcDesc.getAttributes()[i].getType() != TID_INDICATOR)
                    {
                        if (minSize == -1 || minSize > (int64_t) attributeSize)
                        {
                            minSize = attributeSize;
                            j = i;
                        }
                    }
                }
                countMapping.setInputAttributeId(j);
                resultMappings.push_back(countMapping);
            }
        }
        return resultMappings;
    }

    static size_t estimateValueSize(AggIOMapping const& mapping, ArrayDesc const& dstDesc)
    {
        size_t res =0;
        for (size_t i = 0; i < mapping.size(); ++i)
        {
            Type resultType = mapping.getAggregate(i)->getResultType();
            size_t fixedSize = resultType.byteSize();
            size_t varSize = dstDesc.getAttributes()[mapping.getOutputAttributeId(i)].getVarSize();

            //estimate the size of a Value inside of a ValueMap. +1 for an over-estimation.
            size_t size = sizeof(Value) + sizeof(position_t) + sizeof(_Rb_tree_node_base) + 1;
            if (fixedSize>8)
            {
                size+=fixedSize;
            }
            else if (fixedSize == 0 && varSize)
            {
                size+=varSize;
            }
            else if (fixedSize ==0)
            {
                size+=Config::getInstance()->getOption<int>(CONFIG_STRING_SIZE_ESTIMATION);
            }
            res += size;
        }
        return res;
    }

    struct ChunkWriterInfo
    {
        size_t valuesWritten;
        size_t valuesTotal;
        vector<shared_ptr<ChunkIterator> > iters;
        bool opened;
    };

    template <bool USE_PREFLUSH>
    class AttributeWriter
    {
    private:
        int64_t _totalSize;
        int64_t const _maxSize;
        size_t const _nAggs;
        shared_ptr<Query> const _query;
        size_t const _eVSize;
        unordered_map< Coordinates, ChunkWriterInfo > _map;
        vector<shared_ptr<ArrayIterator> >_daiters;
        vector<Value> _stubs;

    public:
        AttributeWriter( unordered_map<Coordinates, size_t> const& chunkCounts,
                         shared_ptr<MemArray> const& dstArray,
                         int64_t maxSize,
                         shared_ptr<Query> const& query,
                         AggIOMapping const& aggMapping):
            _totalSize(0),
            _maxSize(maxSize),
            _nAggs(aggMapping.size()),
            _query(query),
            _eVSize(estimateValueSize(aggMapping, dstArray->getArrayDesc()))
        {
            assert(_nAggs > 0 && _nAggs == aggMapping.size());

            unordered_map<Coordinates, size_t>::const_iterator iter = chunkCounts.begin();
            while(iter!=chunkCounts.end())
            {
                Coordinates const& coords = iter->first;
                ChunkWriterInfo& info = _map[coords];
                info.valuesWritten=0;
                info.valuesTotal=iter->second;
                info.opened = false;
                for(size_t i=0; i<_nAggs; i++)
                {
                    info.iters.push_back(shared_ptr<ChunkIterator> ());
                }
                iter++;
            }

            for(size_t i=0; i<_nAggs; i++)
            {
                _daiters.push_back(dstArray->getIterator(aggMapping.getOutputAttributeId(i)));
                const Type& type = aggMapping.getAggregate(i)->getResultType();
                Value val(type);
                val = TypeLibrary::getDefaultValue(type.typeId());
                _stubs.push_back(val);
            }
        }

        virtual ~AttributeWriter()
        {}

        inline void flushAll()
        {
            unordered_map< Coordinates, ChunkWriterInfo >::iterator iter = _map.begin();
            while(iter!=_map.end())
            {
                ChunkWriterInfo& info = iter->second;
                if(info.iters[0].get())
                {
                    LOG4CXX_TRACE(logger, "Swapping out chunk at "<<CoordsToStr(iter->first));
                    for(size_t i=0; i<_nAggs; i++)
                    {
                        info.iters[i]->flush();
                        info.iters[i].reset();
                    }
                    if(USE_PREFLUSH)
                    {
                        _totalSize -= (info.valuesTotal*_eVSize);
                    }
                    else
                    {
                        _totalSize -= (info.valuesWritten*_eVSize);
                    }
                }
                iter++;
            }
            assert(_totalSize==0);
        }

        inline void openChunk(Coordinates const& chunkPos)
        {
            ChunkWriterInfo& info = _map[chunkPos];
            assert(info.valuesTotal>0 && info.valuesWritten < info.valuesTotal);
            if(info.iters[0].get())
            {
                //already open
                return;
            }
            else
            {
                //need to open or reopen
                if (USE_PREFLUSH)
                {
                    int64_t newSize = _totalSize + info.valuesTotal * _eVSize;
                    if ( newSize > _maxSize )
                    {
                        LOG4CXX_DEBUG(logger, "Mem threshold exceeded, flushing chunks");
                        flushAll();
                    }
                    _totalSize+= (info.valuesTotal * _eVSize);
                    LOG4CXX_TRACE(logger, "Opening chunk "<<CoordsToStr(chunkPos)<<", new size "<<_totalSize)
                }
                else
                {
                    _totalSize+= info.valuesWritten;
                }
                if(info.valuesWritten)
                {
                    //reopen
                    for(size_t i=0; i<_nAggs; i++)
                    {
                        _daiters[i]->setPosition(chunkPos);
                        info.iters[i] = _daiters[i]->updateChunk().getIterator(_query, ConstChunkIterator::NO_EMPTY_CHECK |
                                                                               ConstChunkIterator::APPEND_EMPTY_BITMAP |
                                                                               ConstChunkIterator::APPEND_CHUNK);
                    }
                }
                else
                {
                    for(size_t i=0; i<_nAggs; i++)
                    {
                        assert(_daiters[i]->setPosition(chunkPos) == false);
                        info.iters[i] = _daiters[i]->newChunk(chunkPos).getIterator(_query, ConstChunkIterator::NO_EMPTY_CHECK);
                    }
                }
            }
        }

        inline void writeValue(Coordinates const& chunkPos, Coordinates const& valuePos, vector<Value> const& v)
        {
            ChunkWriterInfo& info = _map[chunkPos];
            assert(v.size() >= 1);
            assert(info.valuesTotal>0 && info.valuesWritten < info.valuesTotal);
            if(info.iters[0].get() == 0)
            {
                openChunk(chunkPos);
            }
            if(!USE_PREFLUSH)
            {   //otherwise, size is already accounted for
                _totalSize+=_eVSize;
            }
            for(size_t i=0; i<_nAggs; i++)
            {
                info.iters[i]->setPosition(valuePos);
                info.iters[i]->writeItem(v[i]);
            }
            info.valuesWritten++;
            if(info.valuesWritten==info.valuesTotal)
            {   //guaranteed we won't need to touch this chunk again
                LOG4CXX_DEBUG(logger, "Finished with chunk at "<<CoordsToStr(chunkPos)<<"; flushing");
                for(size_t i=0; i<_nAggs; i++)
                {
                    info.iters[i]->flush();
                    info.iters[i].reset();
                }
                _totalSize -= (info.valuesTotal * _eVSize);
            }
        }

        inline void notifyValue(Coordinates const& chunkPos, Coordinates const& valuePos)
        {
            if(USE_PREFLUSH)
            {
                ChunkWriterInfo& info = _map[chunkPos];
                assert(info.valuesTotal>0 && info.valuesWritten < info.valuesTotal);
                if(info.iters[0].get() == 0)
                {
                    openChunk(chunkPos);
                }
                for(size_t i=0; i<_nAggs; i++)
                {
                    info.iters[i]->setPosition(valuePos);
                    info.iters[i]->writeItem(_stubs[i]);
                }
            }
        }

        inline void notifyChunk(Coordinates const& chunkPos)
        {
            ChunkWriterInfo& info = _map[chunkPos];
            info.opened=true;
        }

        inline bool chunkWasOpen(Coordinates const& chunkPos)
        {
            ChunkWriterInfo& info = _map[chunkPos];
            return info.opened;
        }
    };


public:
    PhysicalVariableWindow(const string& logicalName, const string& physicalName, const Parameters& parameters, const ArrayDesc& schema):
        PhysicalOperator(logicalName, physicalName, parameters, schema)
    {}

    void copyEmptyTagAttribute(shared_ptr<Array> & srcArray, shared_ptr<MemArray> & dstArray)
    {
        shared_ptr<Query> query = Query::getValidQueryPtr(_query);
        assert (srcArray->getArrayDesc().getEmptyBitmapAttribute()!=NULL);
        shared_ptr<ConstArrayIterator> saiter = srcArray->getConstIterator(srcArray->getArrayDesc().getEmptyBitmapAttribute()->getId());
        shared_ptr<ArrayIterator> daiter = dstArray->getIterator(_schema.getEmptyBitmapAttribute()->getId());
        _localCellCount =0;
        while(!saiter->end())
        {
           Coordinates const& chunkPos = saiter->getPosition();
           size_t chunkCount = 0;
           shared_ptr<ConstChunkIterator> sciter = saiter->getChunk().getConstIterator();
           if( !sciter->end())
           {
               _localChunkMap->addChunkInfo(saiter->getPosition(), _myInstanceId);
               shared_ptr<ChunkIterator> dciter = daiter->newChunk(chunkPos).getIterator(query, ConstChunkIterator::SEQUENTIAL_WRITE | ConstChunkIterator::NO_EMPTY_CHECK);
               while(!sciter->end())
               {
                   dciter->setPosition(sciter->getPosition());
                   dciter->writeItem(sciter->getItem());
                   chunkCount++;
                   ++(*sciter);
               }
               dciter->flush();
               _chunkCounts[chunkPos] = chunkCount;
               _localCellCount += chunkCount;
           }
           ++(*saiter);
        }
        LOG4CXX_TRACE(logger, "Chunk instance map: "<<*(_localChunkMap)<<" total count "<<_localCellCount);
    }

    void mergeChunkMap()
    {
        shared_ptr<Query> query = Query::getValidQueryPtr(_query);
        _globalChunkMap.reset( new ChunkInstanceMap(_nDims, _dimNum));
        shared_ptr<SharedBuffer> mapBuf = _localChunkMap->serialize();
        if ( _coordinatorId != INVALID_INSTANCE) //I am NOT the coordinator
        {
            BufSend(_coordinatorId, mapBuf, query);
            mapBuf = BufReceive(_coordinatorId, query);
            _globalChunkMap->merge(mapBuf);
        }
        else
        {
            _globalChunkMap->merge(mapBuf);
            for(InstanceID i=0; i<_nInstances; i++)
            {
                if(i != _myInstanceId)
                {
                    mapBuf = BufReceive(i,query);
                    _globalChunkMap->merge(mapBuf);
                }
            }
            mapBuf = _globalChunkMap->serialize();
            for(InstanceID i=0; i<_nInstances; i++)
            {
                if(i != _myInstanceId)
                {
                    BufSend(i, mapBuf, query);
                }
            }
        }
        LOG4CXX_DEBUG(logger, "Merged global chunk instance map: "<<*(_globalChunkMap));
    }

    struct ArrayStats
    {
        //Global stats across all nodes used to choose algorithm
        //Total number of pairs of chunks that are on the same axis
        double _totalAxisLinks;
        //Total number of pairs of consecutive chunks that are on the same node
        double _contiguousAxisLinks;
        //Total number of pairs of consecutive chunks that are on different nodes
        double _splitAxisLinks;

        //Local stats on this node used to calculate memory footprint
        double _longestLocalAxis;
        double _localChunkCount;

        ArrayStats():
            _totalAxisLinks(0),
            _contiguousAxisLinks(0),
            _splitAxisLinks(0),
            _longestLocalAxis(0),
            _localChunkCount(0)
        {}
    };

    ArrayStats calculateArrayStats()
    {
        ArrayStats res;
        ChunkInstanceMap::axial_iterator iter = _globalChunkMap->getAxialIterator();
        bool moreChunksInAxis;
        ChunkLocation pcl;
        while(!iter.end())
        {
            ChunkLocation cl = iter.getNextChunk(moreChunksInAxis);
            if(pcl.get()==0)
            {
                //new axis
                if(moreChunksInAxis)
                {
                    pcl=cl;
                }
                continue;
            }
            res._totalAxisLinks++;
            if(pcl->second != cl->second)
            {
                res._splitAxisLinks+=1;
            }
            else
            {
                res._contiguousAxisLinks+=1;
            }
            if(!moreChunksInAxis)
            {
                pcl.reset();
            }
            else
            {
                pcl=cl;
            }
        }

        ChunkInstanceMap::axial_iterator iter2 =_localChunkMap->getAxialIterator();
        pcl.reset();
        double lla = 0;
        while(!iter2.end())
        {
            ChunkLocation cl = iter2.getNextChunk(moreChunksInAxis);
            res._localChunkCount+=1;

            if(pcl.get()==0)
            {   //new axis
                if(lla > res._longestLocalAxis)
                {
                    res._longestLocalAxis = lla;
                }
                lla=1;
                if(moreChunksInAxis)
                {
                    pcl=cl;
                }
                continue;
            }
            lla+=1;
            if(!moreChunksInAxis)
            {
                pcl.reset();
            }
            else
            {
                pcl=cl;
            }
        }
        if(lla > res._longestLocalAxis)
        {
            res._longestLocalAxis = lla;
        }
        LOG4CXX_DEBUG(logger, "Calculated array stats: tal "<<res._totalAxisLinks
                                                   <<" cal "<<res._contiguousAxisLinks
                                                   <<" sal "<<res._splitAxisLinks
                                                   <<" lla "<<res._longestLocalAxis
                                                   <<" lcc "<<res._localChunkCount);
        return res;
    }

    void exchangeMessages(vector<VariableWindowMessage>& inMessages, vector<VariableWindowMessage>& outMessages, size_t nAggs)
    {
        shared_ptr<Query> query = Query::getValidQueryPtr(_query);
        for(InstanceID i =0; i<_nInstances; i++)
        {
            if(i==_myInstanceId)
            {
                continue;
            }
            shared_ptr<SharedBuffer> buf(new MemoryBuffer(NULL, outMessages[i].getBinarySize(_nDims, nAggs)));
            outMessages[i].marshall(_nDims, nAggs, (char*) buf->getData());
            outMessages[i].clear();
            BufSend(i, buf, query);
        }
        for(InstanceID i =0; i<_nInstances; i++)
        {
            if(i==_myInstanceId)
            {
                continue;
            }
            shared_ptr<SharedBuffer> buf = BufReceive(i,query);
            inMessages[i].unMarshall((char*) buf->getData(), _nDims, nAggs);
        }
    }

    /**
     * If all nodes call this with true - return true.
     * Otherwise, return false.
     */
    bool agreeOnBoolean(bool value)
    {
        shared_ptr<Query> query = Query::getValidQueryPtr(_query);
        if ( _coordinatorId != INVALID_INSTANCE) //I am NOT the coordinator
        {
            shared_ptr<SharedBuffer> buf(new MemoryBuffer(NULL, sizeof(bool)));
            *((bool*) buf->getData()) = value;
            BufSend(_coordinatorId, buf, query);
            buf = BufReceive(_coordinatorId, query);
            value = *((bool*) buf->getData());
        }
        else
        {
            for(InstanceID i=0; i<_nInstances; i++)
            {
                if(i != _myInstanceId)
                {
                    shared_ptr<SharedBuffer> buf = BufReceive(i,query);
                    bool otherInstanceFinished = *((bool*) buf->getData());
                    value = value && otherInstanceFinished;
                }
            }
            shared_ptr<SharedBuffer> buf(new MemoryBuffer(NULL, sizeof(bool)));
            *((bool*) buf->getData()) = value;
            for(InstanceID i=0; i<_nInstances; i++)
            {
                if(i != _myInstanceId)
                {
                    BufSend(i, buf, query);
                }
            }
        }
        return value;
    }

    template <bool USE_SWAP>
    void messageCycle(vector<VariableWindowMessage>& outMessages,
                      vector<VariableWindowMessage>& inMessages,
                      unordered_map<Coordinates, shared_ptr<ChunkEdge> >& leftEdges,
                      AttributeWriter<USE_SWAP>& output,
                      vector<AggregatePtr> const& aggs)
    {
        bool isFinished = false;
        while(!isFinished)
        {
            isFinished = true;
            exchangeMessages(inMessages, outMessages, aggs.size());
            for (InstanceID i =0; i<_nInstances; i++)
            {
                VariableWindowMessage& message = inMessages[i];
                if(message.hasData() == false)
                {
                    continue;
                }
                assert(i != _myInstanceId);
                unordered_map<Coordinates, shared_ptr<ChunkEdge> >::iterator iter = message._chunkEdges.begin();
                while(iter!=message._chunkEdges.end())
                {
                    //We've received a chunkEdge from another instance
                    Coordinates const& chunkPos = iter->first;
                    shared_ptr<ChunkEdge> const& chunkEdge = iter->second;

                    //find the next chunk for this edge; verify it lives on this instance
                    ChunkLocation ncl = _globalChunkMap->getNextChunkFor(chunkPos);
                    assert(ncl.get() && ncl->second == _myInstanceId);
                    Coordinates const& nextChunkPos = ncl->first;

                    //after we process this chunk edge, we may need to forward it to another instance!
                    shared_ptr<ChunkEdge> forwardChunkEdge;
                    InstanceID forwardInstanceId = INVALID_INSTANCE;

                    //but if we have a "run" of multiple continuous chunks on this instance, things get a little tricky
                    bool haveRunOfChunks = false;
                    Coordinates lastChunkInRun(0);

                    //look for the next-next chunk. keep going until you hit end of the run or a chunk on a different instance
                    ChunkLocation nncl = _globalChunkMap->getNextChunkFor(nextChunkPos);
                    while(nncl.get() && nncl->second == _myInstanceId)
                    {
                        haveRunOfChunks = true;
                        lastChunkInRun = nncl->first;
                        nncl = _globalChunkMap->getNextChunkFor(nncl->first);
                    }
                    if (nncl.get())
                    {
                        //need to forward to forwardInstanceId
                        forwardChunkEdge.reset(new ChunkEdge());
                        forwardInstanceId = nncl->second;
                    }

                    //find the matching leftEdge
                    shared_ptr<ChunkEdge>& leftEdge = leftEdges[nextChunkPos];
                    if(leftEdge.get() == 0 || (haveRunOfChunks && !output.chunkWasOpen(lastChunkInRun)))
                    {
                        //This means that either:
                        //1. we haven't locally reached the required chunk (and so leftEdge is null) OR
                        //2. we have an entire run of consecutive chunks and we haven't reached the LAST chunk.
                        //   so the edge is not necessarily complete.
                        //In both cases, this right edge will wait until later.
                        iter++;
                        continue;
                    }

                    //process all the window edges inside the chunk edge:
                    ChunkEdge::iterator iter2 = chunkEdge->begin();
                    ChunkEdge::iterator liter2 = leftEdge->begin();
                    while(iter2 != chunkEdge->end())
                    {
                        Coordinates const& axisPos = iter2->first;
                        shared_ptr<WindowEdge>& rightWEdge = iter2->second;
                        LOG4CXX_TRACE(logger, "Received right wedge at chunk "<<CoordsToStr(nextChunkPos)<<
                                                                      " axis "<<CoordsToStr(axisPos)<<
                                                                      " nCoords "<<rightWEdge->getNumCoords()<<
                                                                      " nVals "<<rightWEdge->getNumValues());

                        bool needToForward = false;
                        liter2 = leftEdge->find(axisPos);
                        if( liter2 != leftEdge->end())
                        {
                            shared_ptr<WindowEdge>& leftWEdge = liter2->second;
                            if(leftWEdge->getNumValues() < _nPreceding + _nFollowing)
                            {
                                needToForward = true;
                            }
                            rightWEdge->addLeftEdge(leftWEdge);
                            liter2 = leftEdge->erase(liter2);
                        }
                        else
                        {
                            needToForward = true;
                        }
                        while(rightWEdge->getNumCoords() && (rightWEdge->getNumValues() > (_nPreceding + _nFollowing) || forwardChunkEdge.get()==0 ))
                        {
                            shared_ptr<AggregatedValue> val = rightWEdge->churn(_nPreceding, _nFollowing, aggs);
                            Coordinates valPos = axisPos;
                            valPos[_dimNum] = val->coord;
                            Coordinates chunkPos = valPos;
                            _srcDesc.getChunkPositionFor(chunkPos);
                            if(val->instanceId != _myInstanceId)
                            {
                                isFinished = false;
                                outMessages[val->instanceId].addValues(chunkPos, valPos, val->vals);
                            }
                            else
                            {
                                LOG4CXX_TRACE(logger, "W3 chunk "<<CoordsToStr(chunkPos)<< " position "<<CoordsToStr(valPos));
                                output.writeValue(chunkPos, valPos, val->vals);
                            }
                        }
                        //if there is a forwarding chunk edge then add incomplete windows to it
                        if(forwardChunkEdge.get() && needToForward)
                        {
                            (*forwardChunkEdge)[axisPos] = rightWEdge;
                        }
                        iter2++;
                    }
                    //forward the chunk edge if there is data in it
                    if(forwardChunkEdge.get() && forwardChunkEdge->size())
                    {
                        isFinished = false;
                        Coordinates const& coordsToUse = lastChunkInRun.size() ? lastChunkInRun : nextChunkPos;
                        outMessages[forwardInstanceId]._chunkEdges[coordsToUse] = forwardChunkEdge;
                    }
                    iter= message._chunkEdges.erase(iter);
                }

                unordered_map<Coordinates, shared_ptr< unordered_map <Coordinates, vector<Value> > > >::iterator iter3 = inMessages[i]._computedValues.begin();
                while(iter3!=message._computedValues.end())
                {
                    Coordinates const& chunkPos = iter3->first;
                    shared_ptr< unordered_map <Coordinates, vector<Value> > > const& values = iter3->second;
                    unordered_map <Coordinates, vector<Value> >::iterator iter4 = values->begin();
                    while(iter4 != values->end())
                    {
                        LOG4CXX_TRACE(logger, "W4 chunk "<<CoordsToStr(chunkPos)<< " position "<<CoordsToStr(iter4->first));
                        output.writeValue(chunkPos, iter4->first, iter4->second);
                        iter4++;
                    }
                    iter3 = message._computedValues.erase(iter3);
                }
            }
            isFinished = agreeOnBoolean(isFinished);
        }
    }

    template <bool USE_SWAP>
    void processChunk(ChunkLocation const& cl,
                      shared_ptr<ConstArrayIterator>& saiter,
                      shared_ptr<ChunkEdge>& currentRightEdge,
                      shared_ptr<ChunkEdge>& currentLeftEdge,
                      unordered_map<Coordinates, shared_ptr<ChunkEdge> >& leftEdges,
                      AttributeWriter<USE_SWAP>& output,
                      vector<VariableWindowMessage>& outMessages,
                      vector<AggregatePtr> const& aggs)
    {
        bool havePrevChunk = false;
        bool prevChunkLocal = false;
        bool haveNextChunk = false;
        bool nextChunkLocal = false;
        Coordinates const& chunkPos = cl->first;
        ChunkLocation pcl = _globalChunkMap->getPrevChunkFor(chunkPos);
        if(pcl.get())
        {
            havePrevChunk= true;
            if(pcl->second == _myInstanceId)
            {
                prevChunkLocal = true;
            }
            else
            {
                //previous chunk is remote - so we need a new leftEdge and rightEdge
                currentLeftEdge.reset(new ChunkEdge());
                leftEdges[chunkPos]= currentLeftEdge;
                currentRightEdge.reset(new ChunkEdge());
            }
        }
        else
        {
            //this chunk is the first chunk in axis. so there is no need for a left edge.
            currentLeftEdge.reset();
            currentRightEdge.reset(new ChunkEdge());
        }

        ChunkLocation ncl = _globalChunkMap->getNextChunkFor(chunkPos);
        if(ncl.get())
        {
            haveNextChunk=true;
            if(ncl->second == _myInstanceId)
            {
                nextChunkLocal = true;
            }
        }
        LOG4CXX_DEBUG(logger, "Processing chunk at "<<CoordsToStr(chunkPos)
                               <<" nc "<<haveNextChunk<<" ncl "<<nextChunkLocal
                               <<" pc "<<havePrevChunk<<" pcl "<<prevChunkLocal);

        saiter->setPosition(chunkPos);
        output.notifyChunk(chunkPos);
        shared_ptr<ConstChunkIterator> sciter = saiter->getChunk().getConstIterator();
        while (!sciter->end())
        {
            Coordinates axisPos = sciter->getPosition();
            output.notifyValue(chunkPos, axisPos);
            Coordinate valueCoord = axisPos[_dimNum];
            axisPos[_dimNum] = 0;
            Value const& v = sciter->getItem();

            shared_ptr<WindowEdge>& rightWedge = (*currentRightEdge)[axisPos];
            if(currentLeftEdge.get())
            {
                shared_ptr<WindowEdge>& leftWedge = (*currentLeftEdge)[axisPos];
                if (leftWedge.get()==0)
                {
                    leftWedge.reset(new WindowEdge());
                }
                if(leftWedge->getNumValues() < _nPreceding + _nFollowing)
                {
                    assert(rightWedge.get()==0);
                    leftWedge->addCentral(v, valueCoord, _myInstanceId);
                }

                if(rightWedge.get() == 0 && leftWedge->getNumValues() == _nPreceding + _nFollowing)
                {
                    rightWedge = leftWedge->split(_nPreceding, _nFollowing);
                }
                else if(rightWedge.get())
                {
                    rightWedge->addCentral(v, valueCoord, _myInstanceId);
                }
            }
            else
            {
                if(rightWedge.get()==0)
                {
                    rightWedge.reset(new WindowEdge());
                }
                rightWedge->addCentral(v, valueCoord, _myInstanceId);
            }

            //the loop iterates multiple times only when we are at the starting edge of an array
            while(rightWedge.get() && rightWedge->getNumValues() > _nPreceding + _nFollowing)
            {
                shared_ptr<AggregatedValue> result = rightWedge->churn(_nPreceding, _nFollowing, aggs);
                Coordinate prevAxisCoord = result->coord;
                Coordinates prevValuePos = axisPos;
                prevValuePos[_dimNum] = prevAxisCoord;
                Coordinates prevChunkPos = prevValuePos;
                _srcDesc.getChunkPositionFor(prevChunkPos);
                LOG4CXX_TRACE(logger, "W1 chunk "<<CoordsToStr(prevChunkPos)<< " position "<<CoordsToStr(prevValuePos));
                output.writeValue(prevChunkPos, prevValuePos, result->vals);
            }
            ++(*sciter);
        }
        if(_nFollowing == 0 || !haveNextChunk)
        {
            ChunkEdge::iterator iter = currentRightEdge->begin();
            while(iter!= currentRightEdge->end())
            {
                Coordinates const& edgePos = iter->first;
                Coordinates valuePos = edgePos;
                shared_ptr<WindowEdge>& wEdge = iter->second;
                while(wEdge.get() && wEdge->getNumCoords())
                {
                    shared_ptr<AggregatedValue> result = wEdge->churn(_nPreceding, _nFollowing, aggs);
                    valuePos[_dimNum]=result->coord;
                    Coordinates chunkPos = valuePos;
                    _srcDesc.getChunkPositionFor(chunkPos);
                    LOG4CXX_TRACE(logger, "W2 chunk "<<CoordsToStr(chunkPos)<< " position "<<CoordsToStr(valuePos));
                    output.writeValue(chunkPos, valuePos, result->vals);
                }
                iter++;
            }
        }
        if(haveNextChunk && !nextChunkLocal)
        {
            InstanceID nextInstance = ncl->second;
            //Next chunk is on a different instance. So we need to forward some window edges to that instance.
            //But we can only forward those window edges that have enough values.
            shared_ptr<ChunkEdge> edgeToForward(new ChunkEdge());
            ChunkEdge::iterator iter = currentRightEdge->begin();
            while(iter!= currentRightEdge->end())
            {
                Coordinates const& axisPos = iter->first;
                shared_ptr<WindowEdge>& wEdge = iter->second;
                if(wEdge.get() ==0)
                {
                    iter++;
                    continue;
                }

                assert(wEdge->getNumValues() <= _nPreceding + _nFollowing);
                LOG4CXX_TRACE(logger, "F1: forwarding edge from chunk "<<CoordsToStr(chunkPos)<<
                                                               " axis "<<CoordsToStr(axisPos)<<
                                                            " nCoords "<<wEdge->getNumCoords()<<
                                                              " nVals "<<wEdge->getNumValues()<<
                                                               " to n "<<nextInstance);
                (*edgeToForward)[axisPos] = wEdge;
                iter ++;
            }
            outMessages[nextInstance]._chunkEdges[chunkPos] = edgeToForward;
        }
    }

    template <bool USE_SWAP>
    void flushLeftEdges(unordered_map<Coordinates, shared_ptr<ChunkEdge> > & leftEdges,
                        AttributeWriter<USE_SWAP> & output,
                        vector<AggregatePtr> const& aggs)
    {
        unordered_map<Coordinates, shared_ptr<ChunkEdge> >::iterator iter = leftEdges.begin();
        while(iter!=leftEdges.end())
        {
            shared_ptr<ChunkEdge>& leftEdge = iter->second;
            ChunkEdge::iterator iter2 = leftEdge->begin();
            while(iter2!=leftEdge->end())
            {
                Coordinates valuePos = iter2->first;
                shared_ptr<WindowEdge>& leftWedge = iter2->second;
                while(leftWedge->getNumCoords())
                {
                    shared_ptr<AggregatedValue> result = leftWedge->churn(_nPreceding, _nFollowing, aggs);
                    valuePos[_dimNum]=result->coord;
                    Coordinates chunkPos = valuePos;
                    _srcDesc.getChunkPositionFor(chunkPos);
                    LOG4CXX_TRACE(logger, "W5 chunk "<<CoordsToStr(chunkPos)<< " position "<<CoordsToStr(valuePos));
                    output.writeValue(chunkPos, valuePos, result->vals);
                }
                iter2++;
            }
            iter++;
        }
    }

    Coordinates agreeOnNextAxis(vector<Coordinates> const& axesList, size_t& currentAxis)
    {
        Coordinates result(0);
        shared_ptr<Query> query = Query::getValidQueryPtr(_query);
        if ( _coordinatorId != INVALID_INSTANCE) //I am NOT the coordinator
        {
            shared_ptr<SharedBuffer> buf= BufReceive(_coordinatorId, query);
            if(buf->getSize() == sizeof(bool))
            {
                assert(*((bool*) buf->getData()) == false);
                return result;
            }
            assert(buf->getSize() == _nDims * sizeof(Coordinate));
            result.assign( (Coordinate*) buf->getData(), (Coordinate*) buf->getData() + _nDims);
        }
        else
        {
            if(currentAxis == axesList.size())
            {
                shared_ptr<SharedBuffer> buf(new MemoryBuffer(NULL, sizeof(bool)));
                *((bool*) buf->getData())=false;
                for(InstanceID i=0; i<_nInstances; i++)
                {
                    if(i != _myInstanceId)
                    {
                        BufSend(i, buf, query);
                    }
                }
                return result;
            }
            result = axesList[currentAxis];
            currentAxis++;
            shared_ptr<SharedBuffer> buf(new MemoryBuffer(NULL, _nDims * sizeof(Coordinate)));
            Coordinate* coordPtr = (Coordinate*) buf->getData();
            for(size_t i=0; i<_nDims; i++)
            {
                *coordPtr= result[i];
                coordPtr++;
            }
            for(InstanceID i=0; i<_nInstances; i++)
            {
                if(i != _myInstanceId)
                {
                    BufSend(i, buf, query);
                }
            }
        }
        return result;
    }

    template <bool USE_SWAP, bool AXIAL_SYNC>
    void axialMultiInstanceVariableWindow(shared_ptr <Array> const& srcArray,
                                   shared_ptr <MemArray>& dstArray,
                                   AggIOMapping const& mapping,
                                   size_t sizeLimit)
    {
        shared_ptr<Query> query = Query::getValidQueryPtr(_query);
        vector<VariableWindowMessage> outMessages(_nInstances);
        vector<VariableWindowMessage> inMessages(_nInstances);

        vector<Coordinates> axesList = _globalChunkMap->getAxesList();
        size_t currentAxis = 0;

        unordered_map<Coordinates, shared_ptr<ChunkEdge> > leftEdges;
        shared_ptr<ConstArrayIterator> saiter = srcArray->getConstIterator(mapping.getInputAttributeId());
        AttributeWriter<USE_SWAP> output(_chunkCounts, dstArray, sizeLimit, query, mapping);

        shared_ptr<ChunkEdge> currentRightEdge;
        shared_ptr<ChunkEdge> currentLeftEdge;
        ChunkInstanceMap::axial_iterator axiter = _localChunkMap->getAxialIterator();

        if (AXIAL_SYNC)
        {
            Coordinates nextAxis = agreeOnNextAxis(axesList, currentAxis);
            while(nextAxis.size())
            {
                LOG4CXX_DEBUG(logger, "Processing axis "<<CoordsToStr(nextAxis));
                axiter.setAxis(nextAxis);
                bool exitLoop = false;
                while(!exitLoop)
                {
                    if(!axiter.endOfAxis())
                    {
                        ChunkLocation cl = axiter.getNextChunk();
                        processChunk <USE_SWAP> (cl, saiter, currentRightEdge, currentLeftEdge, leftEdges, output, outMessages, mapping.getAggregates());
                    }
                    else
                    {
                        LOG4CXX_DEBUG(logger, "End of axis");
                        exitLoop = true;
                    }

                    messageCycle <USE_SWAP> (outMessages, inMessages, leftEdges, output, mapping.getAggregates());
                    exitLoop = agreeOnBoolean(exitLoop);
                    if(exitLoop)
                    {
                        flushLeftEdges(leftEdges, output, mapping.getAggregates());
                        leftEdges.clear();
                        for(size_t i =0; i<inMessages.size(); i++)
                        {
                            inMessages[i].clear();
                        }
                        for(size_t i =0; i<outMessages.size(); i++)
                        {
                            outMessages[i].clear();
                        }
                    }
                }
                nextAxis = agreeOnNextAxis(axesList, currentAxis);
                getInjectedErrorListener().check();
            }
        }
        else
        {
            while (!axiter.end())
            {
                ChunkLocation cl = axiter.getNextChunk();
                processChunk <USE_SWAP> (cl, saiter, currentRightEdge, currentLeftEdge, leftEdges, output, outMessages,  mapping.getAggregates());
            }
            getInjectedErrorListener().check();
            messageCycle <USE_SWAP> (outMessages, inMessages, leftEdges, output,  mapping.getAggregates());
            flushLeftEdges(leftEdges, output,  mapping.getAggregates());
        }
        output.flushAll();
    }

    shared_ptr<Array> execute(vector< shared_ptr<Array> >& inputArrays, shared_ptr<Query> query)
    {
#if 0
        runVariableWindowUnitTests();
#endif
        assert(inputArrays.size() == 1);
        shared_ptr<Array> srcArray = ensureRandomAccess(inputArrays[0], query);

        _srcDesc = srcArray->getArrayDesc();
        const string& dimName  = ((boost::shared_ptr<OperatorParamReference>&)_parameters[0])->getObjectName();
        const string& dimAlias = ((boost::shared_ptr<OperatorParamReference>&)_parameters[0])->getArrayName();
        _dimNum = -1;
        bool found = false;
        _nDims = _srcDesc.getDimensions().size();

        for (size_t i=0; i<_nDims; i++)
        {
            if (_srcDesc.getDimensions()[i].hasNameAndAlias(dimName, dimAlias))
            {
                _dimNum = i;
                found = true;
            }
        }
        found = found;  // to remove release-build warning
        assert(found);
        _nPreceding = ((boost::shared_ptr<OperatorParamPhysicalExpression>&)_parameters[1])->getExpression()->evaluate().getInt64();
        _nFollowing = ((boost::shared_ptr<OperatorParamPhysicalExpression>&)_parameters[2])->getExpression()->evaluate().getInt64();
        _localChunkMap.reset(new ChunkInstanceMap(_srcDesc.getDimensions().size(), _dimNum));
        _myInstanceId = query->getInstanceID();
        _coordinatorId = query->getCoordinatorID();
        _nInstances = query->getInstancesCount();
        _query = query;

        shared_ptr<MemArray> dstArray(new MemArray(_schema, query));
        copyEmptyTagAttribute(srcArray, dstArray);
        mergeChunkMap();
        ArrayStats stats = calculateArrayStats();

        bool useAxialSync = false;
        if(stats._splitAxisLinks > stats._contiguousAxisLinks)
        {
            LOG4CXX_DEBUG(logger, "Using axial sync");
            useAxialSync = true;
        }

        double avgValuesInChunk = _localCellCount * 1.0 / stats._localChunkCount;
        size_t estimatedCellsInMemory = 0;

        //If most axes are on separate nodes and everyone is working on their own axis, then
        //footprint is at worst one axis.
        //Also if most axes are perfectly split between nodes, and we're using axial sync,
        //then footprint is ALSO at worst one axis.
        if(stats._contiguousAxisLinks >= stats._totalAxisLinks * .9 ||
           stats._splitAxisLinks >= stats._totalAxisLinks * .9)
        {
            estimatedCellsInMemory = (size_t) (stats._longestLocalAxis * avgValuesInChunk);
        }
        else
        {
            //mixed case - assume footprint is entire array
            estimatedCellsInMemory = _localCellCount;
        }
        size_t maxSize = (Config::getInstance()->getOption<size_t>(CONFIG_MEM_ARRAY_THRESHOLD) * MiB) / 2;

        LOG4CXX_DEBUG(logger, "Estimated cells in memory: "<<estimatedCellsInMemory);

        vector<AggIOMapping> mappings = collectIOMappings();
        for(size_t i=0; i<mappings.size(); i++)
        {
            AggIOMapping const& mapping = mappings[i];
            size_t estValueSize = estimateValueSize(mapping, _schema);
            LOG4CXX_DEBUG(logger, "Estimated cells size: "<<estValueSize);
            size_t estArraySize = estValueSize * estimatedCellsInMemory;

            //assume that half of MEM_ARRAY_THRESHOLD is used by array itself, and half is used by our edges.
            bool useSwap = false;
            if(estArraySize >= maxSize)
            {
                LOG4CXX_DEBUG(logger, "Estimated array size "<<estArraySize<<" exceeded threshold "<<maxSize<<". Using swap.");
                useSwap = true;
            }

            if (useSwap && useAxialSync)
            {   axialMultiInstanceVariableWindow<true, true>(srcArray, dstArray, mapping, maxSize); }
            else if (useSwap && !useAxialSync)
            {   axialMultiInstanceVariableWindow<true, false>(srcArray, dstArray, mapping, maxSize); }
            else if (!useSwap && useAxialSync)
            {   axialMultiInstanceVariableWindow<false, true>(srcArray, dstArray, mapping, maxSize); }
            else
            {   axialMultiInstanceVariableWindow<false, false>(srcArray, dstArray, mapping, maxSize); }
        }

        return dstArray;
    }
};

DECLARE_PHYSICAL_OPERATOR_FACTORY(PhysicalVariableWindow, "variable_window", "PhysicalVariableWindow")

}
