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
 * @file StreamArray.cpp
 *
 * @brief Array receiving chunks from abstract stream
 *
 * @author Konstantin Knizhnik <knizhnik@garret.ru>
 */

#include <stdio.h>
#include <log4cxx/logger.h>
#include <array/Metadata.h>
#include <array/StreamArray.h>
#ifndef SCIDB_CLIENT
#include <query/RemoteArray.h>
#include <query/PullSGArray.h>
#endif
#include <system/Config.h>
#include <system/Exceptions.h>
#include <system/SciDBConfigOptions.h>

namespace scidb
{
    using namespace boost;
    using namespace std;
    static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.qproc.streamarray"));

    //
    // StreamArray
    //
    StreamArray::StreamArray(const StreamArray& other)
    : desc(other.desc), emptyCheck(other.emptyCheck), _iterators(other._iterators.size()), currentBitmapChunk(NULL)
    {
    }

    StreamArray::StreamArray(ArrayDesc const& arr, bool emptyable)
    : desc(arr), emptyCheck(emptyable), _iterators(arr.getAttributes().size()), currentBitmapChunk(NULL)
    {
    }

    string const& StreamArray::getName() const
    {
        return desc.getName();
    }

    ArrayID StreamArray::getHandle() const
    {
        return desc.getId();
    }

    ArrayDesc const& StreamArray::getArrayDesc() const
    {
        return desc;
    }

    boost::shared_ptr<ArrayIterator> StreamArray::getIterator(AttributeID attId)
    {
        throw USER_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "StreamArray::getIterator()";
    }

    boost::shared_ptr<ConstArrayIterator> StreamArray::getConstIterator(AttributeID attId) const
    {
        if (!_iterators[attId]) {
            ((StreamArray*)this)->_iterators[attId] = boost::shared_ptr<ConstArrayIterator>(new StreamArrayIterator(*(StreamArray*)this, attId));
        }
        return _iterators[attId];
    }

    //
    // Stream array iterator
    //
    StreamArrayIterator::StreamArrayIterator(StreamArray& arr, AttributeID attr)
    : array(arr), attId(attr), currentChunk(NULL)
    {
        moveNext();
    }

    void StreamArrayIterator::moveNext()
    {
        if (array.currentBitmapChunk == NULL || attId == 0 || array.currentBitmapChunk->getAttributeDesc().getId() != attId) {
            currentChunk = array.nextChunk(attId, dataChunk);
            if (currentChunk != NULL && array.emptyCheck) {
                AttributeDesc const* bitmapAttr = array.desc.getEmptyBitmapAttribute();
                if (bitmapAttr != NULL && bitmapAttr->getId() != attId) {
                    if (array.currentBitmapChunk == NULL ||
                        array.currentBitmapChunk->getFirstPosition(false) != currentChunk->getFirstPosition(false)) {
#ifndef SCIDB_CLIENT
                        assert(!dynamic_cast<scidb::RemoteMergedArray*>(&array));
                        assert(!dynamic_cast<scidb::PullSGArray*>(&array));
#endif
                        LOG4CXX_TRACE(logger, "StreamArrayIterator::moveNext: getting bitmap chunk"
                                      << " attId= " << attId
                                      <<", currChunkPos=" << currentChunk->getFirstPosition(false));

                        array.currentBitmapChunk = array.nextChunk(bitmapAttr->getId(), bitmapChunk);
                        if (!array.currentBitmapChunk)
                            throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_NO_CURRENT_BITMAP_CHUNK);
                    }
                    dataChunk.setBitmapChunk((Chunk*)array.currentBitmapChunk);
                }
            }
        } else {
            currentChunk = array.currentBitmapChunk;
        }
    }

    ConstChunk const& StreamArrayIterator::getChunk()
    {
        if (!currentChunk) {
            throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_NO_CURRENT_CHUNK);
        }
        return *currentChunk;
    }

    bool StreamArrayIterator::end()
    {
        return currentChunk == NULL;
    }

    void StreamArrayIterator::operator ++()
    {
        if (currentChunk != NULL) {
            moveNext();
        }
    }

    Coordinates const& StreamArrayIterator::getPosition()
    {
        if (!currentChunk) {
            throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_NO_CURRENT_CHUNK);
        }
        return currentChunk->getFirstPosition(false);
    }


#ifndef SCIDB_CLIENT
    //
    // AccumulatorArray
    //
    AccumulatorArray::AccumulatorArray(boost::shared_ptr<Array> array,
                                       boost::shared_ptr<Query>const& query)
    : StreamArray(array->getArrayDesc(), false),
      pipe(array),
      iterators(array->getArrayDesc().getAttributes().size())
    {
        assert(query);
        _query=query;
    }
    ConstChunk const* AccumulatorArray::nextChunk(AttributeID attId, MemChunk& chunk)
    {
        if (!iterators[attId]) {
            iterators[attId] = pipe->getConstIterator(attId);
        } else {
            ++(*iterators[attId]);
        }
        if (iterators[attId]->end()) {
            return NULL;
        }
        ConstChunk const& inputChunk = iterators[attId]->getChunk();
        if (inputChunk.isMaterialized()) {
            return &inputChunk;
        }

        Address addr(attId, inputChunk.getFirstPosition(false));
        chunk.initialize(this, &desc, addr, inputChunk.getCompressionMethod());
        chunk.setBitmapChunk((Chunk*)&inputChunk);
        boost::shared_ptr<ConstChunkIterator> src = inputChunk.getConstIterator(ChunkIterator::INTENDED_TILE_MODE|
                                                                                ChunkIterator::IGNORE_EMPTY_CELLS);
        boost::shared_ptr<Query> query(Query::getValidQueryPtr(_query));
        boost::shared_ptr<ChunkIterator> dst = chunk.getIterator(query,
                                                                 (src->getMode() & ChunkIterator::TILE_MODE)|
                                                                 ChunkIterator::NO_EMPTY_CHECK|
                                                                 (inputChunk.isSparse()?ChunkIterator::SPARSE_CHUNK:0)|
                                                                 ChunkIterator::SEQUENTIAL_WRITE);
        bool vectorMode = src->supportsVectorMode() && dst->supportsVectorMode();
        src->setVectorMode(vectorMode);
        dst->setVectorMode(vectorMode);
        size_t count = 0;
        while (!src->end()) {
            if (dst->setPosition(src->getPosition())) {
                dst->writeItem(src->getItem());
                count += 1;
            }
            ++(*src);
        }
        if (!vectorMode && !desc.hasOverlap()) {
            chunk.setCount(count);
        }
        dst->flush();
        return &chunk;
    }

/**
 * Multistream array constructor
 * @param n number of chunk streams
 * @param localStream local stream ID
 * @param arr array descriptor
 * @param query context
 */
MultiStreamArray::MultiStreamArray(size_t n,
                                   size_t localStream,
                                   ArrayDesc const& arr,
                                   boost::shared_ptr<Query>const& query)
: StreamArray(arr, false),
  _nStreams(n),
  _localStream(localStream),
  _resultChunks(arr.getAttributes().size()),
  _resultChunkIterators(arr.getAttributes().size()),
  _readyPositions(arr.getAttributes().size()),
  _notReadyPositions(arr.getAttributes().size()),
  _currPartialStreams(arr.getAttributes().size()),
  _currMinPos(arr.getAttributes().size())
{
    assert(query);
    _query=query;
    list<size_t> notReadyPos;
    for (size_t i=0; i < _nStreams; ++i) {
        notReadyPos.push_back(i);
    }
    for (AttributeID attId=0; attId < _notReadyPositions.size(); ++attId) {
        list<size_t>& current = _notReadyPositions[attId];
        current.insert(current.end(), notReadyPos.begin(), notReadyPos.end());
    }
}

ConstChunk const*
MultiStreamArray::nextChunk(AttributeID attId, MemChunk& chunk)
{
    static const char *funcName = "MultiStreamArray::nextChunk: ";

    ASSERT_EXCEPTION( (attId < getArrayDesc().getAttributes().size()) , funcName);
    assert(attId < _resultChunks.size());
    assert(attId < _resultChunkIterators.size());
    assert(attId < _currMinPos.size());
    assert(attId < _notReadyPositions.size());
    assert(attId < _readyPositions.size());
    assert(attId < _currPartialStreams.size());

    list<size_t>& notReadyPos = _notReadyPositions[attId];
    PositionMap& readyPos = _readyPositions[attId];

    if (logger->isTraceEnabled()) {
        for (list<size_t>::iterator it=notReadyPos.begin(); it != notReadyPos.end(); ++it) {
            LOG4CXX_TRACE(logger, funcName << "NOT ready streams attId= " << attId
                         <<", stream="<< (*it));
        }
    }

    while (true) {

        getAllStreamPositions(readyPos, notReadyPos, attId);

        list<size_t>& currPartialStreams = _currPartialStreams[attId];

        if (readyPos.empty() && currPartialStreams.empty()) {
            // no more remote chunks
            LOG4CXX_TRACE(logger, funcName << "EOF - no more chunks attId= " << attId);
            return NULL;
        }

        if (logger->isTraceEnabled()) {
            for (PositionMap::iterator it=readyPos.begin(); it!=readyPos.begin(); ++it) {
                LOG4CXX_TRACE(logger, funcName << "ready streams attId= " << attId
                              <<", src stream="<< (*it).second.getSrc()
                              <<", dst stream="<< (*it).second.getDest()
                              <<", pos="<< (*it).first);
            }
        }

        bool notMyStream = false;
        if (currPartialStreams.empty()) {
            // starting a new chunk, find all partiall chunk streams
            _resultChunks[attId].reset();
            _resultChunkIterators[attId].reset();
            assert(_currMinPos[attId].empty());
            PositionMap::value_type& minElem = (*readyPos.begin());
            _currMinPos[attId] = minElem.first;
            PositionMap::const_iterator endIter = readyPos.upper_bound(minElem.first);

            notMyStream = (minElem.second.getDest() != _localStream);

            for (PositionMap::iterator it=readyPos.begin(); it!=endIter; ) {
                assert((*it).first == _currMinPos[attId]);
                currPartialStreams.push_back((*it).second.getSrc());
                readyPos.erase(it++);
            }
        }

        if (logger->isTraceEnabled()) {
            for (list<size_t>::iterator it=currPartialStreams.begin();
                 it != currPartialStreams.end(); ++it) {
                LOG4CXX_TRACE(logger, funcName << "partial chunk attId= " << attId
                              <<", stream="<< *it
                              <<", isMyStream="<<!notMyStream);
            }
        }

        if (notMyStream) {
            // some stream positions are for chunks that are not destined for us (this instance)
            _currMinPos[attId].clear();
            // let's check if we have more messages in the stream
            getNextStreamPositions(readyPos, notReadyPos, currPartialStreams, attId);
            // XXX TODO: this amounts to busy-polling of the stream generating extra network traffic etc.
            // we should probably just back-off for a few mils ...
        } else {
            break;
        }
    }

    list<size_t>& currPartialStreams = _currPartialStreams[attId];
    mergePartialStreams(readyPos, notReadyPos, currPartialStreams, attId);

    assert(currPartialStreams.empty());
    assert(_resultChunks[attId]);
    assert(_resultChunks[attId]->getFirstPosition(false) == _currMinPos[attId]);

    LOG4CXX_TRACE(logger, funcName <<"done with chunk attId= " << attId
                  <<", resultChunk=" << _resultChunks[attId].get()
                  <<", resultChunk size=" << _resultChunks[attId]->getSize());

    LOG4CXX_TRACE(logger, funcName <<"done with chunk attId=" <<attId
                  << ", minPos="<< _currMinPos[attId]);

    if (_resultChunkIterators[attId]) {
        _resultChunkIterators[attId]->flush();
        _resultChunkIterators[attId].reset();
    }
    _currMinPos[attId].clear();

    return _resultChunks[attId].get();
}

void
MultiStreamArray::getAllStreamPositions(PositionMap& readyPos,
                                        list<size_t>& notReadyPos,
                                        const AttributeID attId)
{
    static const char *funcName = "MultiStreamArray::getAllStreamPositions: ";
    Coordinates pos;
    shared_ptr<RetryException> err;

    for (list<size_t>::iterator iter = notReadyPos.begin();
         iter != notReadyPos.end();) {

        const size_t stream = *iter;
        pos.clear();
        try {
            size_t destStream(_localStream);
            if (nextChunkPos(stream, attId, pos, destStream)) {
                assert(!pos.empty());
                readyPos.insert(make_pair(pos,SourceAndDest(stream, destStream)));
                LOG4CXX_TRACE(logger, funcName << "ready stream found attId= " << attId
                              <<", src stream="<< stream
                              <<", dest stream="<< destStream
                              <<", pos="<< pos);
            }
            iter = notReadyPos.erase(iter);
            continue;
        } catch (RetryException& e) {
            LOG4CXX_TRACE(logger, funcName << "next position is NOT ready attId= " << attId
                          <<", stream="<< stream);

            if (!err) { err = boost::dynamic_pointer_cast<RetryException>(e.copy()); }
        }
        ++iter;
    }
    if (err) {
        // some positions are still missing
        throw *err;
    }
}

void
MultiStreamArray::mergePartialStreams(PositionMap& readyPos,
                                      list<size_t>& notReadyPos,
                                      list<size_t>& currPartialStreams,
                                      const AttributeID attId)
{
    static const char *funcName = "MultiStreamArray::mergePartialStreams: ";
    Coordinates pos;
    shared_ptr<RetryException> err;
    shared_ptr<MemChunk> mergeChunk = boost::make_shared<MemChunk>();

    // get all partial chunks
    for (list<size_t>::iterator it=currPartialStreams.begin();
         it != currPartialStreams.end(); ) {

        const size_t stream = *it;
        assert(stream<_nStreams);

        ConstChunk const* next = NULL;
        try {

            next = nextChunkBody(stream, attId, *mergeChunk);
            if (!next) {
                throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_CANT_FETCH_CHUNK_BODY);
            }
            assert(next == mergeChunk.get());

            // record next position, request next chunk
            pos.clear();
            try {
                size_t destStream(_localStream);
                if (nextChunkPos(stream, attId, pos, destStream)) {
                    assert(!pos.empty());
                    readyPos.insert(make_pair(pos,SourceAndDest(stream,destStream)));
                    LOG4CXX_TRACE(logger, funcName << "next position is ready attId= " << attId
                                  <<", src stream="<< stream
                                  <<", dst stream="<< destStream);
                } else {
                    LOG4CXX_TRACE(logger, funcName << "next position is not available attId= " << attId
                                  <<", stream="<< stream);
                }
            } catch (RetryException& e) {
                LOG4CXX_TRACE(logger, funcName << "next position is NOT ready attId= " << attId
                              <<", stream="<< stream);
                notReadyPos.push_back(stream);
            }
        } catch (RetryException& e) {
            LOG4CXX_TRACE(logger, funcName << "next chunk is NOT ready attId= " << attId
                          <<", stream="<< stream);

            assert(next==NULL);
            if (!err) {
                err = boost::dynamic_pointer_cast<RetryException>(e.copy());
            }
            ++it;
            continue;
        }

        it = currPartialStreams.erase(it);

        assert(next);

        LOG4CXX_TRACE(logger, funcName << "got next chunk attId=" << attId
                     <<", stream="<< stream
                     <<", next=" << next
                     <<", size=" << next->getSize());

        if (!_resultChunks[attId])  {
            _resultChunks[attId] = mergeChunk;
            _resultChunks[attId]->setCount(0); // unknown
            mergeChunk = boost::make_shared<MemChunk>();
            continue;
        }
        ConstChunk const* result = _resultChunks[attId].get();
        assert(_currMinPos[attId] == next->getFirstPosition(false));
        assert(_currMinPos[attId] == result->getFirstPosition(false));

        mergeChunks(result, next, _resultChunkIterators[attId]);
    }
    if (err) { throw *err; }
}

void
MultiStreamArray::getNextStreamPositions(PositionMap& readyPos,
                                         list<size_t>& notReadyPos,
                                         list<size_t>& currPartialStreams,
                                         const AttributeID attId)
{
    static const char *funcName = "MultiStreamArray::getNextStreamPositions: ";
    Coordinates pos;
    shared_ptr<RetryException> err;
    // re-request next stream positions
    for (list<size_t>::iterator it=currPartialStreams.begin();
         it != currPartialStreams.end(); ++it) {

        const size_t stream = *it;
        assert(stream<_nStreams);

        // record next position, request next chunk
        pos.clear();
        try {
            size_t destStream(_localStream);
            if (nextChunkPos(stream, attId, pos, destStream)) {
                assert(!pos.empty());
                readyPos.insert(make_pair(pos,SourceAndDest(stream,destStream)));
                LOG4CXX_TRACE(logger, funcName << "next position is ready attId= " << attId
                              <<", src stream="<< stream
                              <<", dst stream="<< destStream);
            } else {
                LOG4CXX_TRACE(logger, funcName << "next position is not available attId= " << attId
                              <<", stream="<< stream);
            }
        } catch (RetryException& e) {
            LOG4CXX_TRACE(logger, funcName << "next position is NOT ready attId= " << attId
                          <<", stream="<< stream);
            notReadyPos.push_back(stream);
            if (!err) {
                err = boost::dynamic_pointer_cast<RetryException>(e.copy());
            }
            continue;
        }
    }
    currPartialStreams.clear();
    if (err) { throw *err; }
}

void MultiStreamArray::mergeChunks( ConstChunk const* next,
                                    ConstChunk const* merge,
                                    shared_ptr<ChunkIterator>& dstIterator)
{
    assert(next);
    assert(merge);

    AttributeDesc const& attr = merge->getAttributeDesc();
    Value const& defaultValue = attr.getDefaultValue();

    if (next->isRLE() || merge->isRLE() ||
        next->isSparse() || merge->isSparse() ||
        attr.isNullable() ||
        !defaultValue.isDefault(attr.getType()) ||
        TypeLibrary::getType(attr.getType()).variableSize()) {

        int sparseMode = next->isSparse() ? ChunkIterator::SPARSE_CHUNK : 0;
        boost::shared_ptr<Query> query(Query::getValidQueryPtr(_query));
        if(!dstIterator) {
            dstIterator = ((Chunk*)next)->getIterator(query, sparseMode|
                                                      ChunkIterator::APPEND_CHUNK|
                                                      ChunkIterator::NO_EMPTY_CHECK);
        }
        assert(dstIterator);
        boost::shared_ptr<ConstChunkIterator> srcIterator = merge->getConstIterator(ChunkIterator::IGNORE_DEFAULT_VALUES|
                                                                                    ChunkIterator::IGNORE_NULL_VALUES|
                                                                                    ChunkIterator::IGNORE_EMPTY_CELLS);

        bool hasEmptyBitmap = (next->getArrayDesc().getEmptyBitmapAttribute() != NULL);

        while (!srcIterator->end()) {
                Value const& value = srcIterator->getItem();
                if (hasEmptyBitmap || value != defaultValue) {
                    if (!dstIterator->setPosition(srcIterator->getPosition())) {
                        throw USER_EXCEPTION(SCIDB_SE_MERGE, SCIDB_LE_OPERATION_FAILED) << "setPosition";
                    }
                    dstIterator->writeItem(value);
                }
                ++(*srcIterator);
            }
    } else { //XXX TODO: is this dead code ??? it may only be used for the 'dense' chunk format, which should be removed
        if (next->getSize() != merge->getSize()) {
            throw USER_EXCEPTION(SCIDB_SE_MERGE, SCIDB_LE_CANT_MERGE_CHUNKS_WITH_VARYING_SIZE);
        }
        PinBuffer scope(*merge);
        char* dst = (char*)next->getData();
        char* src = (char*)merge->getData();
        for (size_t j = 0, n = next->getSize(); j < n; j++) {
            dst[j] |= src[j];
        }
    }
}
#endif //SCIDB_CLIENT
}
