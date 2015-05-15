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
 * PhysicalRank.cpp
 *  Created on: Mar 11, 2011
 *      Author: poliocough@gmail.com
 *  Revision 1: May 2012
 *      Author: Donghui
 *      Revision note:
 *          Adding the ability to deal with big data, i.e. when the data does not fit in memory.
 */

#ifndef RANK_COMMON
#define RANK_COMMON

#include <boost/shared_ptr.hpp>
#include <boost/foreach.hpp>
#include <log4cxx/logger.h>
#include <sys/time.h>
#include <boost/unordered_map.hpp>

#include <query/Operator.h>
#include <system/Exceptions.h>
#include <query/LogicalExpression.h>
#include <array/Metadata.h>
#include <system/Cluster.h>
#include <array/DelegateArray.h>
#include <util/Network.h>
#include <array/RowCollection.h>
#include <query/AttributeComparator.h>

namespace scidb
{

typedef boost::unordered_map <Coordinates, uint64_t> CountsMap;

boost::shared_ptr<SharedBuffer> rMapToBuffer( CountsMap const& input, size_t nCoords);
void updateRmap(CountsMap& input, boost::shared_ptr<SharedBuffer> buf, size_t nCoords);

class RankingStats
{
public:
    CountsMap counts;
};

class BlockTimer
{
  public:
    BlockTimer(int64_t* globalCounter): _globalCounter(globalCounter)
    {
         struct timeval tv;
         gettimeofday(&tv,0);
         _startTime = tv.tv_sec * 1000000 + tv.tv_usec;
    }

    ~BlockTimer()
    {
         struct timeval tv;
         gettimeofday(&tv,0);
         int64_t endTime = tv.tv_sec * 1000000 + tv.tv_usec;
         *_globalCounter += (endTime - _startTime);
    }

  private:
    int64_t _startTime;
    int64_t* _globalCounter;
};


class PreSortMap
{
public:
    PreSortMap(boost::shared_ptr<Array>& input, AttributeID neededAttributeID, Dimensions const& groupedDims):
        _dimGrouping(input->getArrayDesc().getDimensions(), groupedDims)
    {}

    virtual ~PreSortMap()
    {}
    virtual double lookupRanking( Value const& input, Coordinates const& inCoords) = 0;
    virtual double lookupHiRanking( Value const& input, Coordinates const& inCoords) = 0;
    Coordinates getGroupCoords( Coordinates const& pos) const
    {
        return _dimGrouping.reduceToGroup(pos);
    }

protected:
    DimensionGrouping _dimGrouping;
};


class ValuePreSortMap : public PreSortMap
{
    typedef std::map<Value, uint64_t, AttributeComparator> ValueCountMap;
    typedef boost::shared_ptr<ValueCountMap> ValueCountMapPtr;
    typedef boost::unordered_map<Coordinates, ValueCountMapPtr> CoordinatesCountsMap;
    typedef boost::unordered_map<Coordinates, uint64_t> CoordinatesMaxMap;

public:

    ValuePreSortMap(boost::shared_ptr<Array>& input, AttributeID neededAttributeID, Dimensions const& groupedDims):
        PreSortMap(input, neededAttributeID, groupedDims)
    {
        ArrayDesc const& inputSchema = input->getArrayDesc();
        TypeId tid = inputSchema.getAttributes()[neededAttributeID].getType();

        size_t numPresorts = 0;
        size_t actualValues = 0;
        size_t distinctValues = 0;

        const unsigned CHUNK_FLAGS =
            ConstChunkIterator::IGNORE_OVERLAPS |
            ConstChunkIterator::IGNORE_EMPTY_CELLS |
            ConstChunkIterator::IGNORE_NULL_VALUES;

        CoordinatesCountsMap::iterator mIter;
        ValueCountMap::iterator iter;
        {
            boost::shared_ptr<ConstArrayIterator> arrayIterator = input->getConstIterator(neededAttributeID);
            while (!arrayIterator->end())
            {
                {
                    boost::shared_ptr<ConstChunkIterator> chunkIterator =
                        arrayIterator->getChunk().getConstIterator(CHUNK_FLAGS);
                    while (!chunkIterator->end())
                    {
                        Value &v = chunkIterator->getItem();
                        if (v.isNull())
                        {
                            ++(*chunkIterator);
                            continue;
                        }

                        actualValues++;
                        Coordinates pos = _dimGrouping.reduceToGroup(chunkIterator->getPosition());

                        mIter = _preSortMaps.find(pos);
                        if (mIter == _preSortMaps.end())
                        {
                            ValueCountMapPtr ptr(new ValueCountMap(AttributeComparator(tid)));
                            mIter=_preSortMaps.insert(std::make_pair(pos, ptr)).first;
                            numPresorts++;
                        }

                        iter = mIter->second->find(v);
                        if (iter == mIter->second->end())
                        {
                            mIter->second->insert(std::make_pair(v,1));
                            distinctValues++;
                        }
                        else
                        {
                            iter->second++;
                        }
                        ++(*chunkIterator);
                    }
                }
                ++(*arrayIterator);
            }
        }

        LOG4CXX_DEBUG(logger, "Processed "<<actualValues<<" values into "
                      << numPresorts << " presort maps with "<< distinctValues
                      <<" distinct values");

        mIter = _preSortMaps.begin();
        while (mIter != _preSortMaps.end())
        {
           ValueCountMap::iterator iter = mIter->second->begin();
           uint64_t count = 0, tmp = 0;
           while (iter != mIter->second->end())
           {
               tmp=iter->second;
               (*iter).second=count;
               count += tmp;
               iter++;
           }

           Coordinates const& pos = mIter->first;
           _maxMap[pos] = count;
           mIter++;
        }

        LOG4CXX_DEBUG(logger, "Computed counts");
    }

    virtual double lookupRanking( Value const& input, Coordinates const& inCoords)
    {
        Coordinates pos = getGroupCoords(inCoords);
        CoordinatesCountsMap::iterator iter = _preSortMaps.find(pos);
        if(iter == _preSortMaps.end())
        {
            return 0;
        }
        else
        {
           ValueCountMapPtr& innerMap = iter->second;
           ValueCountMap::iterator innerIter = innerMap->lower_bound(input);
           if(innerIter == innerMap->end())
           {
               return (double) _maxMap[pos];
           }
           return (double) innerIter->second;
        }

        return 0;   // dummy code to avoid warning
    }

    virtual double lookupHiRanking( Value const& input, Coordinates const& inCoords)
    {
        Coordinates pos = getGroupCoords(inCoords);
        CoordinatesCountsMap::iterator iter = _preSortMaps.find(pos);
        if(iter == _preSortMaps.end())
        {
            return 0;
        }
        else
        {
           ValueCountMapPtr& innerMap = iter->second;
           ValueCountMap::iterator innerIter = innerMap->upper_bound(input);
           if(innerIter == innerMap->end())
           {
               return (double) _maxMap[pos];
           }
           return (double) innerIter->second;
        }
    }

private:
    CoordinatesCountsMap        _preSortMaps;
    CoordinatesMaxMap           _maxMap;
};

template <class T>
struct IsFP
{
    static const bool value = false;
};

template<>
struct IsFP <float>
{
    static const bool value = true;
};

template<>
struct IsFP <double>
{
    static const bool value = true;
};

//TODO: we could reorganize the templates better and get rid of virtual methods, BUT THEN the RankArray class would have to be
//templatized - and there will be no reduction in the number of virtual function calls -- UNTIL we change RankArray to work
//in tile mode. Soon.
template <typename INPUT>
class PrimitivePreSortMap : public PreSortMap
{
    typedef std::map<INPUT, uint64_t> ValueCountMap;
    typedef boost::shared_ptr<ValueCountMap> ValueCountMapPtr;
    typedef boost::unordered_map<Coordinates, ValueCountMapPtr> CoordinatesCountsMap;
    typedef boost::unordered_map<Coordinates, uint64_t> CoordinatesMaxMap;

public:

    PrimitivePreSortMap(boost::shared_ptr<Array>& input, AttributeID neededAttributeID, Dimensions const& groupedDims):
        PreSortMap(input, neededAttributeID, groupedDims)
    {
        ArrayDesc const& inputSchema = input->getArrayDesc();
        TypeId tid = inputSchema.getAttributes()[neededAttributeID].getType();

        size_t numPresorts = 0;
        size_t actualValues = 0;
        size_t distinctValues = 0;

        const unsigned CHUNK_FLAGS =
            ConstChunkIterator::IGNORE_OVERLAPS |
            ConstChunkIterator::IGNORE_EMPTY_CELLS |
            ConstChunkIterator::IGNORE_NULL_VALUES;

        typename CoordinatesCountsMap::iterator mIter;
        typename ValueCountMap::iterator iter;
        {
            boost::shared_ptr<ConstArrayIterator> arrayIterator = input->getConstIterator(neededAttributeID);
            while (!arrayIterator->end())
            {
                {
                    boost::shared_ptr<ConstChunkIterator> chunkIterator =
                        arrayIterator->getChunk().getConstIterator(CHUNK_FLAGS);
                    while (!chunkIterator->end())
                    {
                        Value v = chunkIterator->getItem();
                        if (v.isNull() || (IsFP<INPUT>::value && isnan( *(INPUT*) v.data())))
                        {
                            ++(*chunkIterator);
                            continue;
                        }

                        actualValues++;
                        Coordinates pos = _dimGrouping.reduceToGroup(chunkIterator->getPosition());

                        mIter = _preSortMaps.find(pos);
                        if (mIter == _preSortMaps.end())
                        {
                            mIter=_preSortMaps.insert(std::make_pair(pos, ValueCountMapPtr(new ValueCountMap))).first;
                            numPresorts++;
                        }

                        INPUT* val = static_cast<INPUT*>(v.data());
                        iter = mIter->second->find(*val);
                        if (iter == mIter->second->end())
                        {
                            mIter->second->insert(std::make_pair(*val,1));
                            distinctValues++;
                        }
                        else
                        {
                            iter->second++;
                        }
                        ++(*chunkIterator);
                    }
                }
                ++(*arrayIterator);
            }
        }

        LOG4CXX_DEBUG(logger, "Processed "<<actualValues<<" values into "
                      << numPresorts << " presort maps with "<< distinctValues
                      <<" distinct values");

        mIter = _preSortMaps.begin();
        while (mIter != _preSortMaps.end())
        {
            typename ValueCountMap::iterator iter = mIter->second->begin();
            uint64_t count = 0, tmp = 0;
            while (iter != mIter->second->end())
            {
                tmp=iter->second;
                (*iter).second=count;
                count += tmp;
                iter++;
            }

            Coordinates const& pos = mIter->first;
            _maxMap[pos] = count;
            mIter++;
        }

        LOG4CXX_DEBUG(logger, "Computed counts");
    }

    virtual double lookupRanking( Value const& input, Coordinates const& inCoords)
    {
        INPUT* val = (INPUT*) input.data();
        if(IsFP<INPUT>::value && isnan(*val))
        {
            return -1;
        }

        Coordinates pos = getGroupCoords(inCoords);
        typename CoordinatesCountsMap::iterator iter = _preSortMaps.find(pos);
        if(iter == _preSortMaps.end())
        {
            return 0;
        }
        else
        {
           ValueCountMapPtr&  innerMap = iter->second;
           typename ValueCountMap::iterator innerIter = innerMap->lower_bound(*val);
           if(innerIter == innerMap->end())
           {
               return (double) _maxMap[pos];
           }
           return (double) innerIter->second;
        }
    }

    virtual double lookupHiRanking( Value const& input, Coordinates const& inCoords)
    {
        INPUT* val = (INPUT*) input.data();
        if(IsFP<INPUT>::value && std::isnan(*val))
        {
            return -1;
        }

        Coordinates pos = getGroupCoords(inCoords);
        typename CoordinatesCountsMap::iterator iter = _preSortMaps.find(pos);
        if(iter == _preSortMaps.end())
        {
            return 0;
        }
        else
        {
           ValueCountMapPtr&  innerMap = iter->second;
           typename ValueCountMap::iterator innerIter = innerMap->upper_bound(*val);
           if(innerIter == innerMap->end())
           {
               return (double) _maxMap[pos];
           }
           return (double) innerIter->second;
        }
    }

private:
    CoordinatesCountsMap        _preSortMaps;
    CoordinatesMaxMap           _maxMap;
};


class RankChunkIterator : public DelegateChunkIterator
{
public:
    RankChunkIterator (DelegateChunk const* sourceChunk,
                       int iterationMode,
                       const boost::shared_ptr<PreSortMap>& preSortMap,
                       const boost::shared_ptr<Array>& mergerArray,
                       const boost::shared_ptr<RankingStats>& rStats):
       DelegateChunkIterator(sourceChunk, (iterationMode & ~IGNORE_DEFAULT_VALUES) | IGNORE_OVERLAPS ),
       _preSortMap(preSortMap),
       _outputValue(TypeLibrary::getType(TID_DOUBLE)),
       _rStats(rStats)
    {
        if (mergerArray.get())
        {
            _mergerArrayIterator = mergerArray->getConstIterator(1);
            if (!_mergerArrayIterator->setPosition(sourceChunk->getFirstPosition(false))) {
                throw SYSTEM_EXCEPTION(SCIDB_SE_UDO, SCIDB_LE_OPERATION_FAILED) << "setPosition";
            }
            _mergerIterator = _mergerArrayIterator->getChunk().getConstIterator(iterationMode & ~IGNORE_DEFAULT_VALUES);
        }
    }

    virtual Value &getItem()
    {
        Value input = inputIterator->getItem();
        if (input.isNull())
        {
            _outputValue.setNull();
        }
        else
        {
            double ranking = _preSortMap->lookupRanking(input, getPosition());
            if(ranking < 0)
            {
                // non-null values that do not compare (i.e. double NAN)
                _outputValue.setDouble(NAN);
            }
            else
            {
                if (_mergerIterator.get())
                {
                    if (!_mergerIterator->setPosition(getPosition()))
                        throw SYSTEM_EXCEPTION(SCIDB_SE_UDO, SCIDB_LE_OPERATION_FAILED) << "setPosition";
                    double mergedRanking = _mergerIterator->getItem().getDouble();
                    ranking += mergedRanking;
                }
                else
                {
                    ranking = ranking + 1.0;
                }

                if (_rStats.get())
                {
                    Coordinates groupCoords = _preSortMap->getGroupCoords(getPosition());
                    _rStats->counts[groupCoords]++;
                }

                _outputValue.setDouble(ranking);
            }
        }
        return _outputValue;
    }

protected:
    boost::shared_ptr<PreSortMap> _preSortMap;
    Value _outputValue;
    boost::shared_ptr<ConstArrayIterator> _mergerArrayIterator;
    boost::shared_ptr<ConstChunkIterator> _mergerIterator;
    boost::shared_ptr<RankingStats> _rStats;
};

class HiRankChunkIterator : public RankChunkIterator
{
public:
    HiRankChunkIterator (DelegateChunk const* sourceChunk,
                          int iterationMode,
                          const boost::shared_ptr<PreSortMap>& preSortMap,
                          const boost::shared_ptr<Array>& mergerArray,
                          const boost::shared_ptr<RankingStats>& rStats):
        RankChunkIterator(sourceChunk, iterationMode, preSortMap, mergerArray, rStats)
    {
        if (mergerArray.get())
        {
            _mergerArrayIterator = mergerArray->getConstIterator(2);
            if (!_mergerArrayIterator->setPosition(sourceChunk->getFirstPosition(false))) {
                throw SYSTEM_EXCEPTION(SCIDB_SE_UDO, SCIDB_LE_OPERATION_FAILED) << "setPosition";
            }
            _mergerIterator = _mergerArrayIterator->getChunk().getConstIterator(iterationMode & ~IGNORE_DEFAULT_VALUES);
        }
    }

    virtual Value &getItem()
    {
        Value input = inputIterator->getItem();
        if (input.isNull())
        {
            _outputValue.setNull();
        }
        else
        {
            double ranking = _preSortMap->lookupHiRanking(input, getPosition());
            if(ranking < 0)
            {
                // non-null values that do not compare (i.e. double NAN)
                _outputValue.setDouble(NAN);
            }
            else
            {
                if (_mergerIterator.get())
                {
                    if (!_mergerIterator->setPosition(getPosition()))
                        throw SYSTEM_EXCEPTION(SCIDB_SE_UDO, SCIDB_LE_OPERATION_FAILED) << "setPosition";
                    double mergedRanking = _mergerIterator->getItem().getDouble();
                    ranking += mergedRanking;
                }
                if (_rStats.get())
                {
                    Coordinates groupCoords = _preSortMap->getGroupCoords(getPosition());
                    _rStats->counts[groupCoords]++;
                }
                _outputValue.setDouble(ranking);
            }
        }
        return _outputValue;
    }
};

class AvgRankChunkIterator : public DelegateChunkIterator
{
public:
    AvgRankChunkIterator (DelegateChunk const* sourceChunk,
                          int iterationMode,
                          boost::shared_ptr<Array>& mergerArray):
       DelegateChunkIterator(sourceChunk, iterationMode | IGNORE_OVERLAPS)
    {
        _mergerArrayIterator = mergerArray->getConstIterator(2);
        if (!_mergerArrayIterator->setPosition(sourceChunk->getFirstPosition(false))) {
            throw SYSTEM_EXCEPTION(SCIDB_SE_UDO, SCIDB_LE_OPERATION_FAILED) << "setPosition";
        }
        _mergerIterator = _mergerArrayIterator->getChunk().getConstIterator(iterationMode & ~IGNORE_DEFAULT_VALUES);
    }

    virtual Value &getItem()
    {
        Value input = inputIterator->getItem();
        if (input.isNull())
        {
            _outputValue.setNull();
        }
        else
        {
            // Note: NaN case is handled here automatically.

            double ranking = input.getDouble();

            if (!_mergerIterator->setPosition(getPosition()))
                throw SYSTEM_EXCEPTION(SCIDB_SE_UDO, SCIDB_LE_OPERATION_FAILED) << "setPosition";
            double mergedRanking = _mergerIterator->getItem().getDouble();
            ranking = (ranking + mergedRanking) / 2;

            _outputValue.setDouble(ranking);
        }
        return _outputValue;
    }

private:
    /**
     * Memory for the returned value.
     */
    Value _outputValue;

    /**
     * The high-rank array iterator.
     *
     * The data from this->inputIterator is averaged with the data
     * from _mergerArrayIterator to produce the average rank.
     */
    boost::shared_ptr<ConstArrayIterator> _mergerArrayIterator;

    /**
     * The high-rank chunk iterator.
     * The data from this->inputIterator is averaged with the data from _mergeIterator to produce the average rank.
     * VERY IMPORTANT: this must be declared after _mergerArrayIterator to enforce proper order of destruction. Otherwise,
     * _mergerArrayIterator is destructed first, which could cause a crash.
     */
    boost::shared_ptr<ConstChunkIterator> _mergerIterator;
};

class RankArray : public DelegateArray
{
public:
    RankArray (ArrayDesc const& desc,
               boost::shared_ptr<Array> const& inputArray,
               boost::shared_ptr<PreSortMap> const& preSortMap,
               AttributeID inputAttributeID,
               bool merger,
               boost::shared_ptr<RankingStats> const& rStats):
        DelegateArray(desc, inputArray),
        _preSortMap(preSortMap),
        _inputAttributeID(inputAttributeID),
        _merger(merger),
        _rStats(rStats)
    {
        _inputHasOlap = false;
        ArrayDesc const& inputDesc = inputArray->getArrayDesc();
        for(size_t i=0; i<inputDesc.getDimensions().size(); i++)
        {
            if(inputDesc.getDimensions()[i].getChunkOverlap()>0)
            {
                _inputHasOlap=true;
                break;
            }
        }
    }

    virtual DelegateChunk* createChunk(DelegateArrayIterator const* iterator, AttributeID attrID) const
    {
        bool isClone = (attrID != 1 && _inputHasOlap == false);
        return new DelegateChunk(*this, *iterator, attrID, isClone);
    }

    virtual DelegateArrayIterator* createArrayIterator(AttributeID attrID) const
    {
        if (attrID == 0 || attrID == 1)
        {
            return new DelegateArrayIterator(*this, attrID, inputArray->getConstIterator(_inputAttributeID));
        }
        else
        {
            //Client must be asking for empty tag
            if (!inputArray->getArrayDesc().getEmptyBitmapAttribute()) {
                throw SYSTEM_EXCEPTION(SCIDB_SE_UDO, SCIDB_LE_DLA_ERROR13);
            }
            AttributeID etID = inputArray->getArrayDesc().getEmptyBitmapAttribute()->getId();
            return new DelegateArrayIterator(*this, attrID, inputArray->getConstIterator(etID));
        }
    }

    virtual DelegateChunkIterator* createChunkIterator(DelegateChunk const* chunk, int iterationMode) const
    {
        StatisticsScope sScope(_statistics);

        if (chunk->getAttributeDesc().getId() == 1)
        {
            boost::shared_ptr<Array> mergerArray;
            if (_merger)
            {
                mergerArray = inputArray;
            }

            return new RankChunkIterator(chunk, iterationMode, _preSortMap, mergerArray, _rStats);
        }
        else
        {
            return DelegateArray::createChunkIterator(chunk, iterationMode | ConstChunkIterator::IGNORE_OVERLAPS);
        }
    }

protected:
    boost::shared_ptr<PreSortMap> _preSortMap;
    AttributeID _inputAttributeID;
    bool _merger;
    boost::shared_ptr<RankingStats> _rStats;
    bool _inputHasOlap;
};


class DualRankArray : public RankArray
{
public:
    DualRankArray (ArrayDesc const& desc,
                   boost::shared_ptr<Array> const& inputArray,
                   boost::shared_ptr<PreSortMap>& preSortMap,
                   AttributeID inputAttributeID,
                   bool merger,
                   boost::shared_ptr<RankingStats>& rStats):
       RankArray(desc, inputArray, preSortMap, inputAttributeID, merger, rStats)
    {}

    virtual DelegateChunk* createChunk(DelegateArrayIterator const* iterator, AttributeID attrID) const
    {
        bool isClone = (attrID != 1 && attrID != 2 && _inputHasOlap == false);
        return new DelegateChunk(*this, *iterator, attrID, isClone);
    }

    virtual DelegateArrayIterator* createArrayIterator(AttributeID attrID) const
    {
        if (attrID == 0 || attrID == 1 || attrID == 2)
        {
            return new DelegateArrayIterator(*this, attrID, inputArray->getConstIterator(_inputAttributeID));
        }
        else
        {
            //Client must be asking for empty tag
            if (!inputArray->getArrayDesc().getEmptyBitmapAttribute()) {
                throw SYSTEM_EXCEPTION(SCIDB_SE_UDO, SCIDB_LE_DLA_ERROR13);
            }
            AttributeID etID = inputArray->getArrayDesc().getEmptyBitmapAttribute()->getId();
            return new DelegateArrayIterator(*this, attrID, inputArray->getConstIterator(etID));
        }
    }

    virtual DelegateChunkIterator* createChunkIterator(DelegateChunk const* chunk, int iterationMode) const
    {
        StatisticsScope sScope(_statistics);

        if (chunk->getAttributeDesc().getId() == 1)
        {
            boost::shared_ptr<Array> mergerArray;
            if (_merger)
            {
                mergerArray = inputArray;
            }
            return new RankChunkIterator(chunk, iterationMode, _preSortMap, mergerArray, _rStats);
        }
        else if (chunk->getAttributeDesc().getId() == 2)
        {
            boost::shared_ptr<Array> mergerArray;
            if (_merger)
            {
                mergerArray = inputArray;
            }
            return new HiRankChunkIterator(chunk, iterationMode, _preSortMap, mergerArray, _rStats);
        }
        else
        {
            return DelegateArray::createChunkIterator(chunk, iterationMode | ConstChunkIterator::IGNORE_OVERLAPS);
        }
    }
};

class AvgRankArray : public DelegateArray
{
public:
    AvgRankArray (ArrayDesc const& desc,
                  boost::shared_ptr<Array> const& inputArray):
      DelegateArray (desc, inputArray)
    {}

    virtual DelegateChunk* createChunk(DelegateArrayIterator const* iterator, AttributeID attrID) const
    {
        bool isClone = attrID != 1;
        return new DelegateChunk(*this, *iterator, attrID, isClone);
    }

    virtual DelegateArrayIterator* createArrayIterator(AttributeID attrID) const
    {
        if (attrID == 0 || attrID == 1)
        {
            return new DelegateArrayIterator(*this, attrID, inputArray->getConstIterator(attrID));
        }
        else
        {
            //Client must be asking for empty tag
            if (!inputArray->getArrayDesc().getEmptyBitmapAttribute()) {
                throw SYSTEM_EXCEPTION(SCIDB_SE_UDO, SCIDB_LE_DLA_ERROR13);
            }
            AttributeID etID = inputArray->getArrayDesc().getEmptyBitmapAttribute()->getId();
            return new DelegateArrayIterator(*this, attrID, inputArray->getConstIterator(etID));
        }
    }

    virtual DelegateChunkIterator* createChunkIterator(DelegateChunk const* chunk, int iterationMode) const
    {
        StatisticsScope sScope(_statistics);

        if (chunk->getAttributeDesc().getId() == 1)
        {
            boost::shared_ptr<Array> mergerArray = inputArray;
            return new AvgRankChunkIterator(chunk, iterationMode, mergerArray);
        }
        else
        {
            return DelegateArray::createChunkIterator(chunk, iterationMode | ConstChunkIterator::IGNORE_OVERLAPS);
        }
    }
};

ArrayDesc getRankingSchema(ArrayDesc const& inputSchema, AttributeID rankedAttributeID, bool dualRank = false);

//inputArray must be distributed round-robin
boost::shared_ptr<Array> buildRankArray(boost::shared_ptr<Array>& inputArray,
                                 AttributeID rankedAttributeID,
                                 Dimensions const& grouping,
                                 boost::shared_ptr<Query>& query,
                                 boost::shared_ptr<RankingStats> rstats = boost::shared_ptr<RankingStats>());
//inputArray must be distributed round-robin
boost::shared_ptr<Array> buildDualRankArray(boost::shared_ptr<Array>& inputArray,
                                     AttributeID rankedAttributeID,
                                     Dimensions const& grouping,
                                     boost::shared_ptr<Query>& query,
                                     boost::shared_ptr<RankingStats> rstats = boost::shared_ptr<RankingStats>());

/**
 * AllRankedOneChunkIterator.
 *   An iterator for AllRankedOneArray to deal with big data.
 *   Every rank is set to be 1.
 */
class AllRankedOneChunkIterator : public DelegateChunkIterator
{
public:
    AllRankedOneChunkIterator (DelegateChunk const* sourceChunk):
       DelegateChunkIterator(sourceChunk, ChunkIterator::IGNORE_OVERLAPS|ChunkIterator::IGNORE_EMPTY_CELLS ),
       _outputValue(TypeLibrary::getType(TID_DOUBLE))
    {
        AttributeDesc const& aDesc = inputIterator->getChunk().getAttributeDesc();
        _type = getDoubleFloatOther(aDesc.getType());
    }

    virtual Value &getItem()
    {
        Value input = inputIterator->getItem();
        if (isNan(input, _type))
        {
            _outputValue.setDouble(NAN);
        }
        else if (input.isNull())
        {
            _outputValue.setNull();
        }
        else
        {
            _outputValue.setDouble(1.0);
        }
        return _outputValue;
    }

protected:
    Value _outputValue;
    DoubleFloatOther _type;
};

/**
 * AllRankedOneArray.
 *
 * @description The Array that deals with big data, which adds an
 * attribute with name = RANKEDATTIRBUTE_ranked, type = double, and
 * value = 1.
 *
 * @note Should the time come when we reimplement the ranking code
 * (e.g. to reuse code from sort operations), this class and its
 * friends is a candidate for removal.  "Rank each of these cells as
 * 1" is a stupid request; if an app wants to do this it can slice and
 * dice the array in other, far more efficient ways.  So don't worry
 * about preserving this functionality if doing so would make
 * refactoring more difficult.
 */
class AllRankedOneArray : public DelegateArray
{
public:
    AllRankedOneArray (ArrayDesc const& outputSchema,
               boost::shared_ptr<Array> const& inputArray,
               AttributeID inputAttributeID):
        DelegateArray(outputSchema, inputArray),
        _inputAttributeID(inputAttributeID)
    {
        _inputHasOlap = false;
        ArrayDesc const& inputDesc = inputArray->getArrayDesc();
        for(size_t i=0; i<inputDesc.getDimensions().size(); i++)
        {
            if(inputDesc.getDimensions()[i].getChunkOverlap()>0)
            {
                _inputHasOlap=true;
                break;
            }
        }
    }

    virtual DelegateChunk* createChunk(DelegateArrayIterator const* iterator, AttributeID attrID) const
    {
        bool isClone = (attrID != 1 && _inputHasOlap == false);
        return new DelegateChunk(*this, *iterator, attrID, isClone);
    }

    virtual DelegateArrayIterator* createArrayIterator(AttributeID attrID) const
    {
        if (attrID == 0 || attrID == 1)
        {
            return new DelegateArrayIterator(*this, attrID, inputArray->getConstIterator(_inputAttributeID));
        }
        else
        {
            //Client must be asking for empty tag
            if (!inputArray->getArrayDesc().getEmptyBitmapAttribute()) {
                throw SYSTEM_EXCEPTION(SCIDB_SE_UDO, SCIDB_LE_DLA_ERROR13);
            }
            AttributeID etID = inputArray->getArrayDesc().getEmptyBitmapAttribute()->getId();
            return new DelegateArrayIterator(*this, attrID, inputArray->getConstIterator(etID));
        }
    }

    virtual DelegateChunkIterator* createChunkIterator(DelegateChunk const* chunk, int iterationMode) const
    {
        StatisticsScope sScope(_statistics);

        if (chunk->getAttributeDesc().getId() == 1)
        {
            return new AllRankedOneChunkIterator(chunk);
        }
        else
        {
            return DelegateArray::createChunkIterator(chunk,
                                                      ChunkIterator::IGNORE_EMPTY_CELLS |
                                                      ChunkIterator::IGNORE_OVERLAPS);
        }
    }

protected:
    AttributeID _inputAttributeID;
    bool _inputHasOlap;
};

/**
 * SimpleProjectArray.
 *   The Array that projects certain attributes from an existing array.
 */
class SimpleProjectArray : public DelegateArray
{
protected:
    // A vector of attributeIDs to project on, not including the empty tag.
    std::vector<AttributeID> _projection;

    // Whether the input array has overlap in any of the dimensions.
    bool _inputHasOlap;

public:
    /**
     * Constructor.
     * @param   outputSchema    Must contain empty tag. Must be a subset of the inputArray's schema.
     * @param   inputArray      Must contain empty tag.
     * @param   projection      A vector of attributeIDs to project on, not including the empty tag.
     */
    SimpleProjectArray (ArrayDesc const& outputSchema,
               boost::shared_ptr<Array> const& inputArray,
               std::vector<AttributeID> const& projection):
        DelegateArray(outputSchema, inputArray),
        _projection(projection)
    {
        ArrayDesc const& inputDesc = inputArray->getArrayDesc();

        // Input array must have an empty tag
        assert( inputDesc.getEmptyBitmapAttribute() );
        assert( inputDesc.getEmptyBitmapAttribute()->getId() + 1 == inputDesc.getAttributes().size());

        // Suppose inputArray has 2 attributes in addition to the empty tag.
        // Suppose outputSchema also has the three attributes.
        // projection will have two elements: projection[0]=0; projection[1]=1.
        assert(projection.size()>0);
        assert(outputSchema.getAttributes().size() == projection.size()+1);
        assert(outputSchema.getAttributes().size() <= inputDesc.getAttributes().size());
        assert(projection[projection.size()-1] + 1 < inputDesc.getAttributes().size());

        _inputHasOlap = false;
        for(size_t i=0; i<inputDesc.getDimensions().size(); i++)
        {
            if(inputDesc.getDimensions()[i].getChunkOverlap()>0)
            {
                _inputHasOlap=true;
                break;
            }
        }
    }

    virtual DelegateChunk* createChunk(DelegateArrayIterator const* iterator, AttributeID attrID) const
    {
        bool isClone = (_inputHasOlap == false);
        return new DelegateChunk(*this, *iterator, attrID, isClone);
    }

    virtual DelegateArrayIterator* createArrayIterator(AttributeID attrID) const
    {
        size_t attrIDInput = 0;
        if (attrID+1 < desc.getAttributes().size()) { // not EmptyTag
            attrIDInput = _projection[attrID];
        } else {                                         // EmptyTag
            attrIDInput = inputArray->getArrayDesc().getAttributes().size()-1;
        }
        return new DelegateArrayIterator(*this, attrID, inputArray->getConstIterator(attrIDInput));
    }

    virtual DelegateChunkIterator* createChunkIterator(DelegateChunk const* chunk, int iterationMode) const
    {
        return DelegateArray::createChunkIterator(chunk,
                                                  ChunkIterator::IGNORE_EMPTY_CELLS | ChunkIterator::IGNORE_OVERLAPS);
    }
};

typedef RowCollection<size_t> RCChunk;      // every chunk is a row
typedef RowIterator<size_t> RIChunk;
typedef boost::unordered_map<Coordinates, size_t> MapChunkPosToID;
typedef boost::unordered_map<Coordinates, size_t>::iterator MapChunkPosToIDIter;

/**
 * ChunkIterator for GroupbyRankArray, to assign ranks from a RowCollection (one per chunk).
 *
 * In addition to inputIterator inherited from DelegateChunkIterator, this class maintains
 * another iterator: a RowIterator which is used to scan through a row in the RowCollection.
 * Note that the RowCollection uses a different coordinate system.
 * For instance, regardless to how many dimensions in the input array, the RowCollection always has two dimensions.
 * So special care is needed, in setPosition(), to translate the position to a position in RowCollection.
 *
 * This class does not provide getPosition(). Instead, it inherits the behavior from DelegateChunkIterator,
 * to return inputIterator->getPosition().
 */
class GroupbyRankChunkIterator : public DelegateChunkIterator
{
public:
    GroupbyRankChunkIterator (DelegateChunk const* sourceChunk,
            boost::shared_ptr<RIChunk>& rcIterator,
            size_t chunkID):
       DelegateChunkIterator(sourceChunk, ChunkIterator::IGNORE_EMPTY_CELLS|ChunkIterator::IGNORE_OVERLAPS),
       _rcIterator(rcIterator),
       _chunkID(chunkID),
       _outputValue(TypeLibrary::getType(TID_DOUBLE)),
       _validPosToLocInRow(false),
       _locInRow2D(2)
    {
        _locInRow2D[0] = _rcIterator->getRowId();
    }

    virtual ~GroupbyRankChunkIterator()
    {
        _rcIterator.reset();
    }

    virtual void operator++();

    virtual Value &getItem()
    {
        assert(! _rcIterator->end());
        std::vector<Value> itemInRCChunk(2);
        _rcIterator->getItem(itemInRCChunk);
        _outputValue = itemInRCChunk[0];
        return _outputValue;
    }

    virtual bool setPosition(const Coordinates& pos);

    virtual void reset() {
        inputIterator->reset();
        _rcIterator->reset();
    }

    virtual bool end() {
        bool ret = _rcIterator->end();
        assert(ret == inputIterator->end());
        return ret;
    }

protected:
    boost::shared_ptr<RIChunk> _rcIterator;
    size_t _chunkID;
    Value _outputValue;

    // _posToLocInRow is used to turn a Coordinates to RowIterator::_locInRow,
    // which is needed to call RowIterator::setPosition()
    boost::unordered_map<Coordinates, size_t> _posToLocInRow;

    // _validPosToLocInRow indicates whether _posToLocInRow has been computed.
    // It is computed in the first call of setPosition().
    bool _validPosToLocInRow;

    // _locInRow2D is used to support setPosition.
    // It is a 2D coordinates, where:
    //   - the row is fixed as rowId in the RowCollection
    //   - the column is the desired RowIterator::_posInRow
    Coordinates _locInRow2D;
};

/**
 * An array that returns the ranked value (from the input array) and the ranks of each field (from RCChunk).
 *
 * @note    The array can ONLY be scanned sequentially. setPosition() will fail.
 */
class GroupbyRankArray : public DelegateArray
{
protected:
    boost::shared_ptr<RCChunk> _pRCChunk;
    AttributeID _inputAttributeID;
    bool _inputHasOlap;
    boost::shared_ptr<MapChunkPosToID> _mapChunkPosToID;

    /**
     *  mutex is used to protect concurrent access of shared data members:
     *    - _mapChunkPosToID: it is an unordered map which is not thread safe.
     *    - inputArray: probably not needed because inputArray->getConstIterator() should be thread safe.
     *    - pRCChunk: probably not needed because there are synchronization done in the RowCollection class.
     *  At some point in the past I (Donghui Zhang) saw synchronization bugs which were extremely hard to reproduce.
     *  To be safe let me add mutex to protect all the above shared data members, even though I believe
     *  the unordered map is the only one that needs protection.
     */
    Mutex _mutex;

public:
    GroupbyRankArray (ArrayDesc const& desc,
               boost::shared_ptr<Array> const& inputArray,
               boost::shared_ptr<RCChunk>const& pRCChunk,
               AttributeID const& inputAttributeID,
               boost::shared_ptr<MapChunkPosToID> const& mapChunkPosToID
               ):
        DelegateArray(desc, inputArray),
        _pRCChunk(pRCChunk),
        _inputAttributeID(inputAttributeID),
        _mapChunkPosToID(mapChunkPosToID)
    {
        _inputHasOlap = false;
        ArrayDesc const& inputDesc = inputArray->getArrayDesc();
        for(size_t i=0; i<inputDesc.getDimensions().size(); i++)
        {
            if(inputDesc.getDimensions()[i].getChunkOverlap()>0)
            {
                _inputHasOlap=true;
                break;
            }
        }
    }

    virtual DelegateChunk* createChunk(DelegateArrayIterator const* iterator, AttributeID attrID) const
    {
        bool isClone = (attrID != 1 && _inputHasOlap == false);
        return new DelegateChunk(*this, *iterator, attrID, isClone);
    }

    virtual DelegateArrayIterator* createArrayIterator(AttributeID attrID) const
    {
        // We want to use '_mutex' here.
        // But since this function is const, if we just put _mutex here, the compiler will treat it as
        // having type = 'const Mutex', and will refuse to compile.
        // So we use *((Mutex*)&_mutex) to trick the compiler in believe it is of type 'Mutex' (i.e. no const).
        ScopedMutexLock lock(*((Mutex*)&_mutex));
        if (attrID == 0 || attrID == 1 )   // value
        {
            return new DelegateArrayIterator(*this, attrID, inputArray->getConstIterator(_inputAttributeID));
        }
        else
        {
            //Client must be asking for empty tag
            if (!inputArray->getArrayDesc().getEmptyBitmapAttribute()) {
                throw SYSTEM_EXCEPTION(SCIDB_SE_UDO, SCIDB_LE_DLA_ERROR13);
            }
            AttributeID etID = inputArray->getArrayDesc().getEmptyBitmapAttribute()->getId();
            return new DelegateArrayIterator(*this, attrID, inputArray->getConstIterator(etID));
        }
    }

    virtual DelegateChunkIterator* createChunkIterator(DelegateChunk const* chunk, int iterationMode) const
    {
        // See the comment in createArrayIterator.
        ScopedMutexLock lock(*((Mutex*)&_mutex));
        if (chunk->getAttributeDesc().getId() == 1)
        {
            MapChunkPosToIDIter iter = _mapChunkPosToID->find(chunk->getFirstPosition(false));
            assert(iter!=_mapChunkPosToID->end());
            size_t chunkID = iter->second;
            size_t rowId = _pRCChunk->rowIdFromExistingGroup(chunkID);
            boost::shared_ptr<RIChunk> rcIterator(_pRCChunk->openRow(rowId));
            return new GroupbyRankChunkIterator(chunk, rcIterator, chunkID);
        }
        return DelegateArray::createChunkIterator(chunk,
                                                  ChunkIterator::IGNORE_EMPTY_CELLS|ChunkIterator::IGNORE_OVERLAPS);
    }
};

}
#endif
