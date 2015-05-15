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
 */

#include <query/Operator.h>
#include <array/Metadata.h>
#include <boost/foreach.hpp>
#include <array/DelegateArray.h>
#include "RankCommon.h"
#include <sys/time.h>
#include <array/RowCollection.h>
#include <util/Hashing.h>

using namespace std;

#include "log4cxx/logger.h"
#include "log4cxx/basicconfigurator.h"
#include "log4cxx/helpers/exception.h"
#include "util/Timing.h"

namespace scidb
{

/**
 * Add a mapping between chunkPos to chunkID, if not exist.
 */
void addChunkPosToID(boost::shared_ptr<MapChunkPosToID>& mapChunkPosToID, Coordinates pos, size_t chunkID) {
    MapChunkPosToIDIter iter = mapChunkPosToID->find(pos);
    if (iter==mapChunkPosToID->end()) {
        mapChunkPosToID->insert(std::pair<Coordinates, size_t>(pos, chunkID));
    } else {
        assert(iter->second == chunkID);
    }
}

class PhysicalRank: public PhysicalOperator
{
    typedef RowCollection<Coordinates> RowCollectionGroup; // every group is a row
    typedef RowCollection<size_t> RowCollectionChunk;      // every chunk is a row
    typedef RowIterator<Coordinates> RIGroup;
    typedef RowIterator<size_t> RIChunk;

public:
    PhysicalRank(const std::string& logicalName, const std::string& physicalName, const Parameters& parameters, const ArrayDesc& schema):
        PhysicalOperator(logicalName, physicalName, parameters, schema)
    {
    }

    virtual PhysicalBoundaries getOutputBoundaries(const std::vector<PhysicalBoundaries> & inputBoundaries,
                                                   const std::vector<ArrayDesc> & inputSchemas) const
    {
        return inputBoundaries[0];
    }

    //We require that input is distributed round-robin so that our parallel trick works
    virtual DistributionRequirement getDistributionRequirement(const std::vector<ArrayDesc> & inputSchemas) const
    {
        vector<ArrayDistribution> requiredDistribution;
        requiredDistribution.push_back(ArrayDistribution(psHashPartitioned));
        return DistributionRequirement(DistributionRequirement::SpecificAnyOrder, requiredDistribution);
    }

    virtual bool changesDistribution(std::vector<ArrayDesc> const&) const
    {
        return true;
    }

    virtual ArrayDistribution getOutputDistribution(const std::vector<ArrayDistribution> & inputDistributions,
                                                    const std::vector< ArrayDesc> & inputSchemas) const
    {
        return ArrayDistribution(psUndefined);
    }

    shared_ptr<Array> execute(std::vector< boost::shared_ptr<Array> >& inputArrays, boost::shared_ptr<Query> query)
    {
        shared_ptr<Array> inputArray = inputArrays[0];
        if (inputArray->getSupportedAccess() == Array::SINGLE_PASS)
        {   //if input supports MULTI_PASS, don't bother converting it
            inputArray = ensureRandomAccess(inputArray, query);
        }

        // timing
        LOG4CXX_DEBUG(logger, "[Rank] Begins.");
        ElapsedMilliSeconds timing;

        const ArrayDesc& inputSchema = inputArray->getArrayDesc();
        string attName = _parameters.size() > 0 ? ((boost::shared_ptr<OperatorParamReference>&)_parameters[0])->getObjectName() :
                                                inputSchema.getAttributes()[0].getName();

        // The attrID of the ranked attribute in the original array.
        AttributeID rankedAttributeID = 0;
        for (size_t i =0 ; i< inputSchema.getAttributes().size(); i++)
        {
            if (inputSchema.getAttributes()[i].getName() == attName)
            {
                rankedAttributeID = inputSchema.getAttributes()[i].getId();
                break;
            }
        }

        // Get the group-by dimensions.
        Dimensions const& dims = inputSchema.getDimensions();
        Dimensions groupBy;
        if (_parameters.size() > 1)
        {
            size_t i, j;
            for (i = 0; i < _parameters.size()-1; i++) {
               const string& dimName = ((boost::shared_ptr<OperatorParamReference>&)_parameters[i + 1])->getObjectName();
               const string& dimAlias = ((boost::shared_ptr<OperatorParamReference>&)_parameters[i + 1])->getArrayName();
               for (j = 0; j < dims.size(); j++) {
                   if (dims[j].hasNameAndAlias(dimName, dimAlias)) {
                       groupBy.push_back(dims[j]);
                       break;
                   }
               }
               assert(j < dims.size());
            }
        }

        // if there is only one group, special care is needed.
        // For now, just use Alex's old implementation.
        if (groupBy.size() == 0) {
            LOG4CXX_DEBUG(logger, "[Rank] Building RankArray, because this is not a group-by rank.");
            shared_ptr<Array> rankArray = buildRankArray(inputArray, rankedAttributeID, groupBy, query);

            timing.logTiming(logger, "[Rank] buildRankArray", false); // false = no need to restart
            LOG4CXX_DEBUG(logger, "[Rank] finished!")

            return rankArray;
        }

        // if every cell is a separate group, return "all-ranked-one"
        if (groupBy.size() == dims.size()) {
            LOG4CXX_DEBUG(logger, "[Rank] Building AllRankedOneArray, because all the dimensions are involved.");
            shared_ptr<Array> allRankedOne = make_shared<AllRankedOneArray>(
                getRankingSchema(inputSchema, rankedAttributeID),
                inputArray, rankedAttributeID);

            timing.logTiming(logger, "[Rank] Building AllRankedOneArray", false);
            LOG4CXX_DEBUG(logger, "[Rank] finished!")

            return allRankedOne;
        }

        LOG4CXX_DEBUG(logger, "[Rank] Begin redistribution (first phase of group-by rank).");

        // For every dimension, determine whether it is a groupby dimension
        shared_ptr<PartitioningSchemaDataGroupby> psdGroupby = make_shared<PartitioningSchemaDataGroupby>();
        psdGroupby->_arrIsGroupbyDim.reserve(dims.size());
        for (size_t i=0; i<dims.size(); ++i)
        {
            bool isGroupbyDim = false;
            for (size_t j=0; j<groupBy.size(); ++j) {
                if (dims[i].getBaseName() == groupBy[j].getBaseName()) {
                    isGroupbyDim = true;
                    break;
                }
            }

            psdGroupby->_arrIsGroupbyDim.push_back(isGroupbyDim);
        }

        // Extract just the ranking attribute
        AttributeDesc rankedAttribute = inputSchema.getAttributes()[rankedAttributeID];
        Attributes projectAttrs;
        projectAttrs.push_back(AttributeDesc(0,
                                             rankedAttribute.getName(),
                                             rankedAttribute.getType(),
                                             rankedAttribute.getFlags(),
                                             rankedAttribute.getDefaultCompressionMethod()));

        AttributeDesc const* emptyTag = inputSchema.getEmptyBitmapAttribute();
        if (emptyTag)
        {
            projectAttrs.push_back(AttributeDesc(1,
                                                 emptyTag->getName(),
                                                 emptyTag->getType(),
                                                 emptyTag->getFlags(),
                                                 emptyTag->getDefaultCompressionMethod()));
        }

        Dimensions projectDims(dims.size());
        for (size_t i = 0, n = dims.size(); i < n; i++)
        {
            DimensionDesc const& srcDim = dims[i];
            projectDims[i] = DimensionDesc(srcDim.getBaseName(),
                                       srcDim.getNamesAndAliases(),
                                       srcDim.getStartMin(),
                                       srcDim.getCurrStart(),
                                       srcDim.getCurrEnd(),
                                       srcDim.getEndMax(),
                                       srcDim.getChunkInterval(),
                                       0);
        }

        ArrayDesc projectSchema(inputSchema.getName(), projectAttrs, projectDims);

        vector<AttributeID> projection(1);
        projection[0] = rankedAttributeID;

        shared_ptr<Array> projected = make_shared<SimpleProjectArray>(projectSchema, inputArray, projection);

        // Redistribute, s.t. all records in the same group go to the same instance.
        shared_ptr<Array> redistributed = redistributeToRandomAccess(projected, query, psGroupby,
                                                                     ALL_INSTANCE_MASK,
                                                                     shared_ptr<DistributionMapper>(),
                                                                     0,
                                                                     psdGroupby);
        // timing
        timing.logTiming(logger, "[Rank] redistribute()");
        LOG4CXX_DEBUG(logger, "[Rank] Begin reading input array and appending to rcGroup, reporting a timing every 10 chunks.");

        // Build a RowCollection, where each row is a group.
        // The attribute names are: [XXX, XXX_rank, XXX_chunkID, XXX_itemID]
        Attributes rcGroupAttrs;
        ArrayDesc outputSchema = getRankingSchema(inputSchema, rankedAttributeID);
        const Attributes& outputAttrs = outputSchema.getAttributes();
        rcGroupAttrs.push_back(AttributeDesc(0, outputAttrs[0].getName(), outputAttrs[0].getType(),
                outputAttrs[0].getFlags(), outputAttrs[0].getDefaultCompressionMethod()));
        rcGroupAttrs.push_back(AttributeDesc(1, outputAttrs[1].getName(), outputAttrs[1].getType(),
                outputAttrs[1].getFlags(), outputAttrs[1].getDefaultCompressionMethod()));
        rcGroupAttrs.push_back(AttributeDesc(2, outputAttrs[0].getName() + "_chunkID", TID_UINT64,
                AttributeDesc::IS_NULLABLE, 0));
        rcGroupAttrs.push_back(AttributeDesc(3, outputAttrs[0].getName() + "_itemID", TID_UINT64,
                AttributeDesc::IS_NULLABLE, 0));
        RowCollectionGroup rcGroup(query, "", rcGroupAttrs);

        boost::shared_ptr<ConstArrayIterator> srcArrayIterValue = redistributed->getConstIterator(0);
        size_t chunkID = 0;
        size_t itemID = 0;
        size_t totalItems = 0;
        vector<Value> itemInRowCollectionGroup(4);
        Coordinates group(groupBy.size());

        boost::shared_ptr<MapChunkPosToID> mapChunkPosToID = boost::shared_ptr<MapChunkPosToID>(new MapChunkPosToID());

        size_t reportATimingAfterHowManyChunks = 10; // report a timing after how many chunks?
        while (!srcArrayIterValue->end()) {
            const ConstChunk& chunk = srcArrayIterValue->getChunk();
            addChunkPosToID(mapChunkPosToID, chunk.getFirstPosition(false), chunkID);
            boost::shared_ptr<ConstChunkIterator> srcChunkIter = chunk.getConstIterator();
            while (!srcChunkIter->end()) {
                size_t g = 0;   // g is the number of group-by dimensions whose coordinates have been copied out to 'group'.
                const Coordinates& full = srcChunkIter->getPosition();
                for (size_t i=0; i<dims.size(); ++i) {
                    if (psdGroupby->_arrIsGroupbyDim[i]) {
                        group[g++] = full[i];
                        if (g==groupBy.size()) {
                            break;
                        }
                    }
                }
                assert(g==groupBy.size());

                itemInRowCollectionGroup[0] = srcChunkIter->getItem();
                itemInRowCollectionGroup[1].setDouble(1);
                itemInRowCollectionGroup[2].setUint64(chunkID);
                itemInRowCollectionGroup[3].setUint64(itemID);
                size_t resultRowId = UNKNOWN_ROW_ID;
                rcGroup.appendItem(resultRowId, group, itemInRowCollectionGroup);

                ++itemID;
                ++(*srcChunkIter);
            }

            ++chunkID;
            totalItems += itemID;
            itemID = 0;
            ++(*srcArrayIterValue);

            // timing
            if (logger->isDebugEnabled() && chunkID % reportATimingAfterHowManyChunks == 0) {
                char buf[100];
                sprintf(buf, "[Rank] overall, reading %lu chunks and %lu items", chunkID, totalItems);
                timing.logTiming(logger, buf, false); // don't restart the timing
                if (chunkID==100) {
                    reportATimingAfterHowManyChunks  = 100;
                    LOG4CXX_DEBUG(logger, "[Rank] Now reporting a number after 100 chunks.");
                } else if (chunkID==1000) {
                    reportATimingAfterHowManyChunks = 1000;
                    LOG4CXX_DEBUG(logger, "[Rank] Now reporting a number after 1000 chunks.");
                }
            }
        }
        rcGroup.switchMode(RowCollectionModeRead);

        // timing
        if (logger->isDebugEnabled()) {
            char buf[100];
            sprintf(buf, "[Rank] overall, reading %lu chunks and %lu items", chunkID, totalItems);
            timing.logTiming(logger, buf);
            LOG4CXX_DEBUG(logger, "[Rank] Begin sorting rcGroup to rcGroupSorted.");
        }

        // Sort every row in the RowCollection, by value, into a new array (so as to use sequential write).
        RowCollectionGroup rcGroupSorted(query, "", rcGroupAttrs);
        rcGroupSorted.copyGroupsFrom(rcGroup);
        rcGroup.sortAllRows(0, outputAttrs[0].getType(), &rcGroupSorted);
        rcGroupSorted.switchMode(RowCollectionModeRead);

        timing.logTiming(logger, "[Rank] Sort");

        // To output intermediate timing results, no more than 20 log lines.
        size_t numRowsOfFivePercent = rcGroupSorted.numRows()/20;
        if (numRowsOfFivePercent<1) {
            numRowsOfFivePercent = 1;
        }

        LOG4CXX_DEBUG(logger, "[Rank] Begin scanning all rows of rcGroupSorted.");

        // Define a RowCollection, where each row is a chunk.
        // The attribute names are: [XXX_rank, XXX_itemID]
        Attributes rcChunkAttrs;
        rcChunkAttrs.push_back(AttributeDesc(0, rcGroupAttrs[1].getName(), rcGroupAttrs[1].getType(),
                rcGroupAttrs[1].getFlags(), rcGroupAttrs[1].getDefaultCompressionMethod()));
        rcChunkAttrs.push_back(AttributeDesc(1, rcGroupAttrs[3].getName(), rcGroupAttrs[3].getType(),
                rcGroupAttrs[3].getFlags(), rcGroupAttrs[3].getDefaultCompressionMethod()));
        RowCollectionChunk rcChunk(query, "", rcChunkAttrs);

        // Scan rcGroupSorted; determine rank on-the-fly; and append to rcChunk
        AttributeComparator compareValue(outputAttrs[0].getType());
        vector<Value> itemInRowCollectionChunk(2);
        TypeId strType = rcGroupAttrs[0].getType();
        DoubleFloatOther type = getDoubleFloatOther(strType);

        for (size_t rowId=0; rowId<rcGroupSorted.numRows(); ++rowId) {
            boost::scoped_ptr<RIGroup> riGroupSorted(rcGroupSorted.openRow(rowId));
            double prevRank = 1;
            size_t processed = 0;
            Value prevValue;
            bool nullEncountered = false;

            while (!riGroupSorted->end()) {
                riGroupSorted->getItem(itemInRowCollectionGroup);

                if (!nullEncountered) {
                    if (!isNullOrNan(itemInRowCollectionGroup[0], type)) {
                        if (processed > 0) {
                            if (compareValue(prevValue, itemInRowCollectionGroup[0])) {
                                prevRank = processed+1;
                                prevValue = itemInRowCollectionGroup[0];
                            }
                        } else {
                            prevValue = itemInRowCollectionGroup[0];
                        }
                    } else {
                        nullEncountered = true;
                    }
                }

                size_t chunkID = itemInRowCollectionGroup[2].getUint64();
                if (!nullEncountered) {
                    itemInRowCollectionChunk[0].setDouble(prevRank);
                } else if (itemInRowCollectionGroup[0].isNull()) {
                    itemInRowCollectionChunk[0].setNull();
                } else {
                    itemInRowCollectionChunk[0].setDouble(NAN);
                }
                itemInRowCollectionChunk[1] = itemInRowCollectionGroup[3];    // itemID
                size_t resultRowId = UNKNOWN_ROW_ID;
                rcChunk.appendItem(resultRowId, chunkID, itemInRowCollectionChunk);

                ++(*riGroupSorted);
                ++processed;
            }
            riGroupSorted.reset();

            // timing
            if ( logger->isDebugEnabled() && (rowId+1)%numRowsOfFivePercent==0) {
                char buf[100];
                sprintf(buf, "[Rank] %lu%%", ((rowId+1)*100/rcGroupSorted.numRows()));
                timing.logTiming(logger, buf, false);
            }
        }
        rcChunk.switchMode(RowCollectionModeRead);

        // timing
        timing.logTiming(logger, "[Rank] Scanning all rows of rcGroupSorted");
        LOG4CXX_DEBUG(logger, "[Rank] Begin second sort.");

        // Sort every row in rcChunk, by itemID, to rcChunkSorted.
        // We need a heap allocation because the pointer needs to be valid after exec() finishes.
        shared_ptr<RowCollectionChunk> pRowCollectionChunkSorted = shared_ptr<RowCollectionChunk>(
                new RowCollectionChunk(query, "", rcChunkAttrs));
        pRowCollectionChunkSorted->copyGroupsFrom(rcChunk);
        rcChunk.sortAllRows(1, TID_UINT64, &*pRowCollectionChunkSorted);
        pRowCollectionChunkSorted->switchMode(RowCollectionModeRead);

        // timing
        timing.logTiming(logger, "[Rank] Second sort", false);

        boost::shared_ptr<GroupbyRankArray> dest =
                make_shared<GroupbyRankArray>(outputSchema, redistributed, pRowCollectionChunkSorted, 0, mapChunkPosToID);

        LOG4CXX_DEBUG(logger, "[Rank] finished!")

        // return
        return dest;
    }
};

DECLARE_PHYSICAL_OPERATOR_FACTORY(PhysicalRank, "rank", "physicalRank")

}
