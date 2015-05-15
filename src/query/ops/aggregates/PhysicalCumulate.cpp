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
 * PhysicalCumulate.cpp
 *  Created on: 9/24/2013
 *      Author: paulgeoffreybrown@gmail.com
 *              Donghui Zhang
 */

#include <boost/shared_ptr.hpp>
#include <boost/foreach.hpp>
#include <array/StreamArray.h>
#include <util/CoordinatesToKey.h>
#include <query/Operator.h>
#include <util/SchemaUtils.h>

using namespace std;
using namespace boost;

namespace scidb
{
static log4cxx::LoggerPtr physicalCumulateLogger(log4cxx::Logger::getLogger("query.ops.cumulate"));

/**
 *  @see PhysicalOperator
 *  @note TO-DO:
 *    - Right now, if one attribute in the input array is involved in multiple aggregates, it is scanned multiple times.
 *      We should group the output aggregates together so as to avoid duplicate scanning of the input array.
 *    - Right now, the algorithm generates the full output array in execute().
 *      We should set up a DelegateArray containing one edge vector per local chunk in the input array, that can be used
 *      to generate an output chunk upon pulling.
 *    - Right now, the algorithm duplicates the local edge vectors.
 *      We should explore the possibility of redistributing the input array, so that chunks in the same 'vector'
 *      (along the aggregate dimension) are distributed to the same instance. The benefit is that no duplication of the
 *      edge vector is needed.
 *      But be aware that this approach will be very inefficient, if all chunks are in the same vector.
 */
class PhysicalCumulate: public PhysicalOperator
{
public:

    PhysicalCumulate ( const string& logicalName,
                       const string& physicalName,
                       const Parameters& parameters,
                       const ArrayDesc& schema
                     ) :
        PhysicalOperator(logicalName, physicalName, parameters, schema)
    {}

    ~PhysicalCumulate()
    {}

    /**
     * A class that stores intermediate aggregate states.
     */
    class HashOfAggregateStates
    {
        /**
         * for every position, store an aggregate state
         */
        unordered_map<Coordinates, Value> _hash;

        /**
         * the aggregate function
         */
        AggregatePtr _aggregate;

        /**
         * temporary Value used to return an aggregate result
         */
        Value _tempValue;

    public:
        HashOfAggregateStates(AggregatePtr const& aggregate)
        : _aggregate(aggregate), _tempValue(_aggregate->getResultType())
        {}

        /**
         * Accumulate a value, or merge a state, into a cell.
         * @param[in] pos       a cell position; note that it is the caller's responsibility to change the coordinate in the aggregate dimension
         * @param[in] v         a value or state to accumulate into the structure
         * @param[in] isState   whether v is a state
         *
         */
        void accumulateOrMerge(Coordinates const& pos, Value const& v, bool isState)
        {
            unordered_map<Coordinates, Value>::iterator it = _hash.find(pos);
            Value state(_aggregate->getStateType());
            if (it == _hash.end()) {
                _aggregate->initializeState(state);
            }
            else {
                state = it->second;
            }

            if (isState) {
                _aggregate->mergeIfNeeded(state, v);
            }
            else {
                _aggregate->accumulateIfNeeded(state, v);
            }
            _hash[pos] = state;
        }

        /**
         * Accumulate a value into an existing cell, and return the final aggregate result at the cell.
         * @param[in] pos       a cell position
         * @param[in] v         a value to accumulate into the cell
         *
         * @return the final aggregate result at the cell
         *
         */
        Value const& accumulateOrMergeAndReturnFinalResult(Coordinates const& pos, Value const& v, bool isState)
        {
            accumulateOrMerge(pos, v, isState);
            _aggregate->finalResult(_tempValue, _hash[pos]);
            return _tempValue;
        }

        /**
         * Getter for _hash.
         */
        unordered_map<Coordinates, Value>& getHash()
        {
            return _hash;
        }
    };

    /**
     * Map of vectors of chunkPos.
     * It is used to store the positions of all chunks.
     * The chunks in one 'vector', i.e. having the same chunkPos except in the aggrDim, are grouped together in a vector.
     * The sorting key of the map is chunkPos, with the coordinate in the aggrDim being replaced with some default coordinate.
     */
    class MapOfVectorsOfChunkPos
    {
        CoordinatesToKey _coordsToKey;

    public:
        typedef Coordinates Key;
        typedef Coordinates ChunkPos;
        typedef vector<ChunkPos> VectorOfChunkPos;
        typedef unordered_map<Key, shared_ptr<VectorOfChunkPos> > MyMap;

        // the map
        MyMap _map;

        /**
         * @param[in]  aggrDim      the dimension to aggregate on
         * @param[in]  defaultCoord the default coordinate in aggrDim for a key
         */
        MapOfVectorsOfChunkPos(size_t aggrDim, Coordinate defaultCoord = 0)
        {
            _coordsToKey.addKeyConstraint(aggrDim, defaultCoord);
        }

        /**
         * Append a new chunkPos to the end of the vector, identified by the key computed from chunkPos.
         */
        void append(Coordinates const& chunkPos)
        {
            Coordinates const& key = _coordsToKey.toKey(chunkPos);

            MyMap::iterator it = _map.find(key);
            if (it == _map.end()) {
                _map.insert(std::pair<Key, shared_ptr<VectorOfChunkPos> >(key, make_shared<VectorOfChunkPos>()));
                it = _map.find(key);
                assert(it!=_map.end());
            }
            it->second->push_back(chunkPos);
        }

        /**
         * Return a vector that contains a chunkPos.
         * @param[in]  chunkPos           the chunkPos to search for
         * @param[out] pVectorOfChunkPos  the vector that includes that chunkPos
         *
         * @return whether the vector exists
         */
        bool getVector(Coordinates const& chunkPos, shared_ptr<VectorOfChunkPos>& pVectorOfChunkPos)
        {
            MyMap::iterator it = _map.find( _coordsToKey.toKey(chunkPos) );
            if (it == _map.end()) {
                return false;
            }

            pVectorOfChunkPos = it->second;
            return true;
        }
    };

    /**
     * The variables passed from execute() to sub-routines, in addition to those in CommonVariablesInExecute.
     */
    struct MyVariablesInExecute
    {
        size_t _numAggrs;                    /// number of aggregate functions == _aggregates.size() == _inputAttrIDs.size() == number of output attributues
        size_t _aggrDim;                     /// the dimension to aggregate on
        vector<AggregatePtr> _aggregates;    /// the aggregates, one per output attribute
        vector<AttributeID> _inputAttrIDs;   /// the attributes in the input array, to compute aggregates on
        shared_ptr<Array> _localEdges;       /// the local edges, i.e. the aggregation state built using data in each local chunk
        shared_ptr<Array> _allEdges;         /// local edges from all instances put together
        shared_ptr<MapOfVectorsOfChunkPos> _mapOfVectorsInRemoteEdges;  /// MapOfVectorsOfChunkPos in the remote edges, i.e. in _allEdges but not in _localEdges
        shared_ptr<MapOfVectorsOfChunkPos> _mapOfVectorsInInputArray;   /// MapOfVectorsOfChunkPos in the input array
        shared_ptr<CoordinatesToKey> _cellPosToKey;                     /// a tool to turn a cell position to a key, by replacing the coordinate in the aggrDim with 0
    };

    /**
     * Build the local edges.
     * @param[in] commonVars  variables in CommonVariablesInExecute
     * @param[in] myVars      variables in MyVariablesInExecute
     * @return what should be assigned to myVars._localEdges
     *
     * @pre The variables in MyVariablesInExecute, before the one to be generated in this routine, should already be assigned.
     * @note chunks at the end of the aggrDim do not need to have its local edge built, because such local edges won't be used.
     */
    shared_ptr<MemArray> buildLocalEdges(CommonVariablesInExecute const& commonVars, MyVariablesInExecute const& myVars)
    {
        // Create an array.
        //
        Attributes attrsEdge(myVars._numAggrs);
        for (size_t i=0; i<myVars._numAggrs; ++i) {
            attrsEdge[i] = AttributeDesc(
                    i,
                    commonVars._output._attrsWithoutET[i].getName(),
                    myVars._aggregates[i]->getStateType().typeId(),
                    commonVars._output._attrsWithoutET[i].getFlags(),
                    commonVars._output._attrsWithoutET[i].getDefaultCompressionMethod()
                    );
        }

        shared_ptr<MemArray> localEdges = make_shared<MemArray>(
            ArrayDesc(commonVars._output._schema.getName(),addEmptyTagAttribute(attrsEdge),commonVars._output._dims),
            commonVars._query
            );

        // Fill in data.
        //
        for (AttributeID outputAttr = 0; outputAttr < myVars._numAggrs; ++outputAttr) {
            shared_ptr<ConstArrayIterator> inputArrayIter = commonVars._input._array->getConstIterator(myVars._inputAttrIDs[outputAttr]);
            shared_ptr<ArrayIterator> localEdgesArrayIter = localEdges->getIterator(outputAttr);

            while (!inputArrayIter->end()) {
                Coordinates const& inputChunkPos = inputArrayIter->getPosition();

                // skip, if this chunk is at the end of the aggrDim
                //
                if (inputChunkPos[myVars._aggrDim] + commonVars._input._dims[myVars._aggrDim].getChunkInterval() > commonVars._input._dims[myVars._aggrDim].getEndMax()) {
                    ++(*inputArrayIter);
                    continue;
                }

                ConstChunk const& inputChunk = inputArrayIter->getChunk();
                shared_ptr<ConstChunkIterator> inputChunkIter = inputChunk.getConstIterator();

                // an object to convert a cell Coordinates to a key, i.e. by replacing the coordinate in aggrDim with that in chunkPos
                //
                CoordinatesToKey coordsToKey;
                coordsToKey.addKeyConstraint(myVars._aggrDim, inputChunkPos[myVars._aggrDim]);

                // Fill an EdgeVector with aggregate states of all cells in the chunk
                //
                HashOfAggregateStates edgeVector(myVars._aggregates[outputAttr]);

                while (!inputChunkIter->end()) {
                    Value const& v = inputChunkIter->getItem();
                    Coordinates const& cellPos = inputChunkIter->getPosition();
                    edgeVector.accumulateOrMerge(coordsToKey.toKey(cellPos), v, false);  // false = not state

                    ++(*inputChunkIter);
                }

                // Generate a chunk in localEdges, at inputArray's chunkPos.
                //
                Chunk& chunk = localEdgesArrayIter->newChunk(inputChunkPos);

                int iterMode = ChunkIterator::SEQUENTIAL_WRITE;
                if (outputAttr != 0) {
                    iterMode |= ChunkIterator::NO_EMPTY_CHECK;
                }
                shared_ptr<ChunkIterator> localEdgesChunkIter = chunk.getIterator(commonVars._query, iterMode);

                std::map<Coordinates, Value> tempMap;
                for (unordered_map<Coordinates, Value>::iterator it = edgeVector.getHash().begin(); it != edgeVector.getHash().end(); ++it ) {
                    tempMap[it->first] = it->second;
                }

                for (std::map<Coordinates, Value>::iterator it = tempMap.begin(); it != tempMap.end(); ++it ) {
                    Coordinates const& pos = it->first;
                    Value const& v = it->second;
                    bool mustSucceed = localEdgesChunkIter->setPosition(pos);
                    SCIDB_ASSERT(mustSucceed);
                    localEdgesChunkIter->writeItem(v);
                }
                localEdgesChunkIter->flush();
                localEdgesChunkIter.reset();

                ++(*inputArrayIter);
            } // while (!inputArrayIter->end())
        } // for (AttributeID outputAttr = 0; outputAttr < myVars._numAggrs; ++outputAttr)

        return localEdges;
    }

    /**
     * Build a MapOfVectorsOfChunkPos for all chunkPos in the remote edges.
     *
     * @param[in] commonVars  variables in CommonVariablesInExecute
     * @param[in] myVars      variables in MyVariablesInExecute
     * @return what should be assigned to myVars._mapOfVectorsInRemoteEdges
     *
     * @pre The variables in MyVariablesInExecute, before the one to be generated in this routine, should already be assigned.
     */
    shared_ptr<MapOfVectorsOfChunkPos> buildMapOfVectorsInRemoteEdges(CommonVariablesInExecute const& commonVars, MyVariablesInExecute const& myVars)
    {
        shared_ptr<MapOfVectorsOfChunkPos> mapOfVectorsInRemoteEdges = make_shared<MapOfVectorsOfChunkPos>(myVars._aggrDim);
        shared_ptr<ConstArrayIterator> localEdgesArrayIter = myVars._localEdges->getConstIterator(0);

        shared_ptr<CoordinateSet> chunkPosAllEdges = myVars._allEdges->findChunkPositions();

        for (CoordinateSet::const_iterator itSet=chunkPosAllEdges->begin(); itSet!=chunkPosAllEdges->end(); ++itSet) {
            // only push if this is a remote chunk
            if (!localEdgesArrayIter->setPosition(*itSet)) {
                mapOfVectorsInRemoteEdges->append(*itSet);
            }
        }

        return mapOfVectorsInRemoteEdges;
    }

    /**
     * Build a MapOfVectorsOfChunkPos for all chunkPos in the input array.
     *
     * @param[in] commonVars  variables in CommonVariablesInExecute
     * @param[in] myVars      variables in MyVariablesInExecute
     * @return what should be assigned to myVars._mapOfVectorsInInputArray
     *
     * @pre The variables in MyVariablesInExecute, before the one to be generated in this routine, should already be assigned.
     */
    shared_ptr<MapOfVectorsOfChunkPos> buildMapOfVectorsInInputArray(CommonVariablesInExecute const& commonVars, MyVariablesInExecute const& myVars)
    {
        shared_ptr<MapOfVectorsOfChunkPos> mapOfVectorsInInputArray = make_shared<MapOfVectorsOfChunkPos>(myVars._aggrDim);
        shared_ptr<CoordinateSet> chunkPosInputArray = commonVars._input._array->findChunkPositions();

        for (CoordinateSet::const_iterator itSet=chunkPosInputArray->begin(); itSet!=chunkPosInputArray->end(); ++itSet) {
            mapOfVectorsInInputArray->append(*itSet);
        }

        return mapOfVectorsInInputArray;
    }

    /**
     * doCumulate: the real work to generate cumulate() result.
     *
     * @param[in] commonVars  variables in CommonVariablesInExecute
     * @param[in] myVars      variables in MyVariablesInExecute
     */
    void doCumulate(CommonVariablesInExecute const& commonVars, MyVariablesInExecute const& myVars)
    {
        // one attribute at a time in the outputArray
        //
        for (AttributeID outputAttr = 0; outputAttr < myVars._numAggrs; outputAttr++) {
            // array iterators
            //
            AttributeID inputAttr = myVars._inputAttrIDs[outputAttr];
            shared_ptr<ConstArrayIterator> inputArrayIter = commonVars._input._array->getConstIterator(inputAttr);
            shared_ptr<ConstArrayIterator> remoteEdgesArrayIter = myVars._allEdges->getConstIterator(outputAttr);
            shared_ptr<ArrayIterator> outputArrayIter = commonVars._output._array->getIterator(outputAttr);

            // for every vector of input chunks
            //
            // Reminder:
            // typedef unordered_map<Key=Coordinates, shared_ptr<VectorOfChunkPos> > MapOfVectorsOfChunkPos::MyMap;
            //
            for (MapOfVectorsOfChunkPos::MyMap::const_iterator itMapOfVectorsInInputArray = myVars._mapOfVectorsInInputArray->_map.begin();
                    itMapOfVectorsInInputArray != myVars._mapOfVectorsInInputArray->_map.end();
                    ++itMapOfVectorsInInputArray)
            {
                // Initialize an empty beginEdge
                //
                HashOfAggregateStates beginEdge(myVars._aggregates[outputAttr]);

                // Get an iterator into the matching vector in remote edges.
                //
                MapOfVectorsOfChunkPos::MyMap::const_iterator itMapOfVectorsInRemoteEdges =
                        myVars._mapOfVectorsInRemoteEdges->_map.find(itMapOfVectorsInInputArray->first);
                bool hasRemoteEdges = (itMapOfVectorsInRemoteEdges != myVars._mapOfVectorsInRemoteEdges->_map.end());
                vector<Coordinates>::const_iterator itVectorInRemoteEdges;
                if (hasRemoteEdges) {
                    itVectorInRemoteEdges = itMapOfVectorsInRemoteEdges->second->begin();
                }

                // for every chunkPos in the vector (of the input array)
                //
                vector<Coordinates> const& vectorInInputArray = *(itMapOfVectorsInInputArray->second);

                for (vector<Coordinates>::const_iterator itVectorInInputArray = vectorInInputArray.begin();
                        itVectorInInputArray != vectorInInputArray.end();
                        ++itVectorInInputArray)
                {
                    Coordinates const& chunkPosInput = *itVectorInInputArray;

                    // Aggregate into beginEdge the edges from the matching vector in remoteEdges,
                    // whose aggrDim's coordinate < that of the chunk in the input array.
                    //
                    while (hasRemoteEdges && itVectorInRemoteEdges != itMapOfVectorsInRemoteEdges->second->end()) {
                        Coordinates const& coordsInInputArray = *itVectorInInputArray;
                        Coordinates const& coordsInRemoteEdges = *itVectorInRemoteEdges;

                        // Have we gone too far in remoteEdges?
                        if ( coordsInRemoteEdges[myVars._aggrDim] >= coordsInInputArray[myVars._aggrDim]) {
                            // sanity check; remote edges and local chunks should not overlap
                            assert(coordsInRemoteEdges[myVars._aggrDim] > coordsInInputArray[myVars._aggrDim]);

                            break;
                        }

                        // Merge the remote edge into beginEdge
                        //
                        bool mustSucceed = remoteEdgesArrayIter->setPosition(coordsInRemoteEdges);
                        SCIDB_ASSERT(mustSucceed);
                        ConstChunk const& chunk = remoteEdgesArrayIter->getChunk();
                        shared_ptr<ConstChunkIterator> remoteEdgesChunkIter = chunk.getConstIterator();

                        while (!remoteEdgesChunkIter->end()) {
                            Coordinates const& cellPos = remoteEdgesChunkIter->getPosition();
                            Coordinates const& keyFromCellPos = myVars._cellPosToKey->toKey(cellPos);
                            beginEdge.accumulateOrMerge(keyFromCellPos, remoteEdgesChunkIter->getItem(), true); // state

                            ++(*remoteEdgesChunkIter);
                        }

                        ++itVectorInRemoteEdges;
                    } // while (hasRemoteEdges && itVectorInRemoteEdges != itMapOfVectorsInRemoteEdges->second->end())

                    // Scan the input chunk and generate the output chunk
                    //
                    bool mustSucceed = inputArrayIter->setPosition(chunkPosInput);
                    SCIDB_ASSERT(mustSucceed);
                    ConstChunk const& chunkInput = inputArrayIter->getChunk();
                    shared_ptr<ConstChunkIterator> inputChunkIter = chunkInput.getConstIterator();
                    Chunk& outputChunk = outputArrayIter->newChunk(chunkPosInput);
                    int iterMode = ChunkIterator::SEQUENTIAL_WRITE;
                    if (outputAttr!=0) {
                        iterMode |= ChunkIterator::NO_EMPTY_CHECK;
                    }
                    shared_ptr<ChunkIterator> outputChunkIter = outputChunk.getIterator(commonVars._query, iterMode);

                    while (!inputChunkIter->end()) {
                        Coordinates const& cellPos =inputChunkIter->getPosition();
                        Coordinates const& keyFromCellPos = myVars._cellPosToKey->toKey(cellPos);
                        Value const& aggregateResult = beginEdge.accumulateOrMergeAndReturnFinalResult(
                                keyFromCellPos, inputChunkIter->getItem(), false); // not state
                        outputChunkIter->setPosition(cellPos);
                        outputChunkIter->writeItem(aggregateResult);

                        ++(*inputChunkIter);
                    }

                    // flush the output chunk
                    outputChunkIter->flush();
                } // for (vector<Coordinates>::const_iterator itVectorInInputArray = vectorInInputArray.begin();
            } // for (MapOfVectorsOfChunkPos::MyMap::const_iterator itMapOfVectorsInInputArray = myVars._mapOfVectorsInInputArray->_map.begin();
        } // for (AttributeID outputAttr = 0; outputAttr < commonVars._numAggrs; outputAttr++)
    }

    /**
     * @see PhysicalOperator::execute()
     */
    boost::shared_ptr<Array> execute (
            vector< boost::shared_ptr<Array> >& inputArrays,
            boost::shared_ptr<Query> query)
    {
        SCIDB_ASSERT( _parameters.size() > 0 ); // at least one aggregate call
        SCIDB_ASSERT(inputArrays.size() == 1);

        // CommonVariablesInExecute
        // TO-DO: for now, we make sure the input array is materialized
        //
        shared_ptr<Array> inputArray = inputArrays[0];
        if (!inputArray->isMaterialized()) {
            inputArray = shared_ptr<MemArray>(new MemArray(inputArray, query));
        }
        shared_ptr<Array> outputArray = make_shared<MemArray>(_schema, query);
        CommonVariablesInExecute commonVars(inputArray, outputArray, query);

        // get the aggregate dimension
        //
        MyVariablesInExecute myVars;
        myVars._numAggrs = commonVars._output._attrsWithoutET.size();
        myVars._aggrDim = 0;

        shared_ptr<OperatorParam>& lastParam = _parameters[_parameters.size()-1];
        if ( lastParam->getParamType() == PARAM_DIMENSION_REF ) {
            assert(_parameters.size() == myVars._numAggrs+1);

            string aggrDimName =  dynamic_pointer_cast<OperatorParamDimensionReference>(lastParam)->getObjectName();
            bool found = false;
            for (size_t j = 0; j < commonVars._input._dims.size(); j++) {
                if (commonVars._input._dims[j].hasNameAndAlias(aggrDimName)) {
                    found = true;
                    myVars._aggrDim = j;
                    break;
                }
            }
            SCIDB_ASSERT(found);
        } else {
            assert(_parameters.size() == myVars._numAggrs);
        }

        // get the vector of aggregate functions and the vector of input attrIDs
        //
        myVars._aggregates.resize(myVars._numAggrs);
        myVars._inputAttrIDs.resize(myVars._numAggrs);

        for (size_t i = 0; i<myVars._numAggrs; i++) {
            assert( _parameters[i]->getParamType() == PARAM_AGGREGATE_CALL );

            myVars._aggregates[i] = resolveAggregate(
                    (shared_ptr <OperatorParamAggregateCall> const&) _parameters[i],
                    commonVars._input._attrsWithoutET,
                    &myVars._inputAttrIDs[i],
                    0);

            // If an aggregate has a star, such as count(*), inputAttrIDs[i] will be -1.
            // We should replace with 0, so that we know which attribute in the input array to scan.
            //
            if (myVars._inputAttrIDs[i] == (AttributeID)-1) {
                myVars._inputAttrIDs[i] = 0;
            }
        }

        // Build localEdges, a MemArray that stores one aggregate state per 'vector' of values in each local chunk of inputArray.
        //
        myVars._localEdges = buildLocalEdges(commonVars, myVars);

        // Generate allEdges, by putting together every instance's localEdges.
        //
        myVars._allEdges = redistributeToRandomAccess(myVars._localEdges, query, psReplication,
                                                      ALL_INSTANCE_MASK,
                                                      shared_ptr<DistributionMapper>(),
                                                      0,
                                                      shared_ptr<PartitioningSchemaData>());

        // Generate a map of vector<chunkPos> for chunks in _allEdges, but not in _localEdges.
        // The key of the map is chunkPos, with the coordinate in aggrDim replaced with 0.
        //
        myVars._mapOfVectorsInRemoteEdges = buildMapOfVectorsInRemoteEdges(commonVars, myVars);

        // Generate a map of vector<chunkPos> for chunks in inputArray.
        // The key of the map is chunkPos, with the coordinate in aggrDim replaced with 0.
        //
        myVars._mapOfVectorsInInputArray = buildMapOfVectorsInInputArray(commonVars, myVars);

        // A utility object that turns each cellPos to a 'key', i.e. by turning the coordinate in aggrDim to 0.
        //
        myVars._cellPosToKey = make_shared<CoordinatesToKey>();
        myVars._cellPosToKey->addKeyConstraint(myVars._aggrDim, 0);

        // Generate the cumulate() result.
        //
        doCumulate(commonVars, myVars);

        // return the result
        //
        return outputArray;
    }
};

DECLARE_PHYSICAL_OPERATOR_FACTORY( PhysicalCumulate, "cumulate", "physicalCumulate" )

} // namespace
