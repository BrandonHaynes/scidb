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
 * @file PullSGContext.h
 *
 * @brief Pull-based SG context serving as a chunk data producer
 */

#ifndef PULL_SG_CONTEXT_H_
#define PULL_SG_CONTEXT_H_

#include <iostream>
#include <vector>
#include <deque>
#include <string>

#include <boost/shared_ptr.hpp>

#include <array/Array.h>
#include <array/MemChunk.h>
#include <query/Query.h>
#include <query/PullSGArray.h>
#include <query/Operator.h>
#include <network/BaseConnection.h>
#include <network/proto/scidb_msg.pb.h>

namespace scidb
{

/**
 * This class is an implementation of the data producer for
 * the pull-based redistribute() (i.e. the Scatter side)
 * @see scidb::PullSGArray for the data consumer side (i.e. the Gather side)
 */
class PullSGContext : virtual public Query::OperatorContext
{
private:

    boost::shared_ptr<Array> _inputSGArray;
    bool _isEmptyable;
    boost::shared_ptr<PullSGArray> _resultArray;
    PartitioningSchema _ps;
    boost::shared_ptr<DistributionMapper> _distMapper;
    size_t _shift;
    InstanceID _instanceIdMask;
    boost::shared_ptr<PartitioningSchemaData> _psData;

    typedef std::deque< boost::shared_ptr<MessageDesc> > MessageQueue;
    struct InstanceState
    {
        MessageQueue _chunks;
        uint64_t _requestedNum;
        uint64_t _lastFetchId;
        InstanceState() : _requestedNum(0), _lastFetchId(0) {}
    };

    // This is the prefetch chunk cache. Each remote request tries to drain iterator until the cache is full
    // and then scatters the chunks eligible for delivery.
    std::vector< std::vector<InstanceState> >  _instanceStates;
    std::vector<boost::shared_ptr<ConstArrayIterator> >  _attributeIterators;
    std::vector<size_t> _instanceStatesSizes;
    std::vector<size_t> _eofs;
    size_t _instanceStatesMaxSize;

public:

    /**
     * Constructor
     * @param source array of data to scatter
     * @param result array of data to gather
     * @param instNum number of participating instances
     * @param attrNum number of attributes in the result array
     * @param ps a new partitioning schema
     * @param distMapper
     * @param shift
     * @param instanceIdMask
     * @param psData a pointer to the data that is specific to the particular partitioning schema
     * @param cacheSize the maximum number of input array chunks to be cached
     */
    PullSGContext(const boost::shared_ptr<Array>& source,
                  const boost::shared_ptr<PullSGArray>& result,
                  const size_t instNum,
                  PartitioningSchema ps,
                  const boost::shared_ptr<DistributionMapper>& distMapper,
                  const size_t shift,
                  const InstanceID instanceIdMask,
                  const boost::shared_ptr<PartitioningSchemaData>& psData,
                  size_t cacheSize=64);

    virtual ~PullSGContext() {}

    boost::shared_ptr<PullSGArray> getResultArray()
    {
        return _resultArray;
    }

    /// A list of network messages with their destinations
    typedef std::list< std::pair<InstanceID, boost::shared_ptr<MessageDesc> > > ChunksWithDestinations;

    /**
     * Get the next set of chunks to send to their destinations (i.e. scatter)
     * @param query current query context
     * @param pullingInstance the instance making the request
     * @param attrId attribute ID
     * @param positionOnlyOK if true the pulling instance is willing to accept just the next position data
     * @param prefetchSize the number of *data* chunks the pulling instance is willing to accept
     * @param fetchId the pulling instance message ID for this request
     * @param chunksToSend [out] a list of chunks and their destination instances to scatter
     */
    void getNextChunks(const boost::shared_ptr<Query>& query,
                       const InstanceID pullingInstance,
                       const AttributeID attrId,
                       const bool positionOnlyOK,
                       const uint64_t prefetchSize,
                       const uint64_t fetchId,
                       ChunksWithDestinations& chunksToSend);
private:
    /// @return true if a chunk has any values/cells
    bool hasValues(ConstChunk const& chunk);

    // Helpers to manipulate scidb_msg::Chunk messages

    void
    setNextPosition(boost::shared_ptr<MessageDesc>& chunkMsg,
                    const InstanceID pullingInstance,
                    boost::shared_ptr<ConstArrayIterator>& inputArrIter,
                    const boost::shared_ptr<Query>& query);
    void
    setNextPosition(boost::shared_ptr<MessageDesc>& chunkMsg,
                    const InstanceID nextDestSGInstance,
                    const Coordinates& nextChunkPosition);

    void
    setNextPosition(boost::shared_ptr<MessageDesc>& chunkMsg,
                    boost::shared_ptr<MessageDesc>& nextChunkMsg);

    boost::shared_ptr<MessageDesc>
    getPositionMesg(const QueryID queryId,
                    const AttributeID attributeId,
                    const InstanceID destSGInstance,
                    const Coordinates& chunkPosition);

    boost::shared_ptr<MessageDesc>
    getPositionMesg(const shared_ptr<MessageDesc>& fullChunkMsg);

    boost::shared_ptr<MessageDesc>
    getEOFChunkMesg(const QueryID queryId,
                    const AttributeID attributeId);

    boost::shared_ptr<MessageDesc>
    getChunkMesg(const QueryID queryId,
                 const AttributeID attributeId,
                 const InstanceID destSGInstance,
                 const ConstChunk& chunk,
                 const Coordinates& chunkPosition);
    /**
     * Extract a chunk and/or position message from the chunk cache
     */
    boost::shared_ptr<MessageDesc>
    reapChunkMsg(const QueryID queryId,
                 const AttributeID attributeId,
                 InstanceState& destState,
                 const InstanceID destInstance,
                 const bool positionOnly);
    /**
     * Find the chunks and/or position messages eligible for sending to their destination instances
     */
    bool
    findCachedChunksToSend(boost::shared_ptr<ConstArrayIterator>& inputArrIter,
                           const boost::shared_ptr<Query>& query,
                           const InstanceID pullingInstance,
                           const AttributeID attrId,
                           const bool positionOnly,
                           ChunksWithDestinations& chunksToSend);
    /**
     * Pull on the input array iterator (i.e. produce chunks) and put them into the chunk cache
     */
    bool
    drainInputArray(boost::shared_ptr<ConstArrayIterator>& inputArrIter,
                    const boost::shared_ptr<Query>& query,
                    const AttributeID attrId);
    /**
     * Insert EOF messages for every instance into the cache
     */
    void
    insertEOFChunks(const QueryID queryId,
                    const AttributeID attributeId);
};

} // namespace

#endif /* OPERATOR_H_ */
