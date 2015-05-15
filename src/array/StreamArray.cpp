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
    : _array(arr), _attId(attr), _currentChunk(NULL)
    {
        moveNext();
    }

    void StreamArrayIterator::moveNext()
    {
        if (_array.currentBitmapChunk == NULL ||
            _attId == 0 ||
            _array.currentBitmapChunk->getAttributeDesc().getId() != _attId) {

            _currentChunk = _array.nextChunk(_attId, _dataChunk);
            if (_currentChunk != NULL && _array.emptyCheck) {
                AttributeDesc const* bitmapAttr = _array.desc.getEmptyBitmapAttribute();
                if (bitmapAttr != NULL && bitmapAttr->getId() != _attId) {
                    if (_array.currentBitmapChunk == NULL ||
                        (_array.currentBitmapChunk->getFirstPosition(false) !=
                         _currentChunk->getFirstPosition(false))) {
#ifndef SCIDB_CLIENT
                        assert(!dynamic_cast<scidb::RemoteMergedArray*>(&_array));
                        assert(!dynamic_cast<scidb::PullSGArray*>(&_array));
                        assert(!dynamic_cast<scidb::SinglePassArray*>(&_array));
#endif
                        LOG4CXX_TRACE(logger,
                                      "StreamArrayIterator::moveNext: getting bitmap chunk"
                                      << " attId= " << _attId
                                      << ", currChunkPos="
                                      << _currentChunk->getFirstPosition(false));

                        _array.currentBitmapChunk =
                            _array.nextChunk(bitmapAttr->getId(), _bitmapChunk);
                        if (!_array.currentBitmapChunk)
                            throw USER_EXCEPTION(SCIDB_SE_EXECUTION,
                                                 SCIDB_LE_NO_CURRENT_BITMAP_CHUNK);
                    }
                    assert(_currentChunk == &_dataChunk);
                    if (_array.currentBitmapChunk->getFirstPosition(false) !=
                        _currentChunk->getFirstPosition(false)) {
                        throw SYSTEM_EXCEPTION(SCIDB_SE_EXECUTION,
                                               SCIDB_LE_NO_ASSOCIATED_BITMAP_CHUNK);
                    }
                    _dataChunk.setBitmapChunk((Chunk*)_array.currentBitmapChunk);
                }
            }
        } else {
            _currentChunk = _array.currentBitmapChunk;
        }
    }

    ConstChunk const& StreamArrayIterator::getChunk()
    {
        if (!_currentChunk) {
            throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_NO_CURRENT_CHUNK);
        }
        return *_currentChunk;
    }

    bool StreamArrayIterator::end()
    {
        return _currentChunk == NULL;
    }

    void StreamArrayIterator::operator ++()
    {
        if (_currentChunk != NULL) {
            moveNext();
        }
    }

    Coordinates const& StreamArrayIterator::getPosition()
    {
        if (!_currentChunk) {
            throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_NO_CURRENT_CHUNK);
        }
        return _currentChunk->getFirstPosition(false);
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
        boost::shared_ptr<ChunkIterator> dst =
            chunk.getIterator(query,
                              (src->getMode() & ChunkIterator::TILE_MODE)|
                              ChunkIterator::NO_EMPTY_CHECK|
                              ChunkIterator::SEQUENTIAL_WRITE);
        size_t count = 0;
        while (!src->end()) {
            if (dst->setPosition(src->getPosition())) {
                dst->writeItem(src->getItem());
                count += 1;
            }
            ++(*src);
        }
        if (!desc.hasOverlap()) {
            chunk.setCount(count);
        }
        dst->flush();
        return &chunk;
    }

MultiStreamArray::DefaultChunkMerger::DefaultChunkMerger (bool isEnforceDataIntegrity)
: _isEnforceDataIntegrity(isEnforceDataIntegrity),
  _hasDataIntegrityIssue(false),
  _numElems(0),
  _chunkSizeLimit(0)
{
    const size_t sizeLimit = Config::getInstance()->getOption<size_t>(CONFIG_CHUNK_SIZE_LIMIT);
    _chunkSizeLimit = sizeLimit * MiB;
}

bool
MultiStreamArray::DefaultChunkMerger::mergePartialChunk(size_t stream,
                                                        AttributeID attId,
                                                        shared_ptr<MemChunk>& partialChunk,
                                                        const shared_ptr<Query>& query)
{
    assert(partialChunk);

    AttributeDesc const& attr = partialChunk->getAttributeDesc();
    SCIDB_ASSERT((attId == attr.getId()));

    const bool isEbm = isEmptyBitMap(partialChunk);

    if (!isEbm) {
        const ConstRLEPayload partialPayload(reinterpret_cast<char*>(partialChunk->getData()));
        _numElems += partialPayload.count();
    }

    if (!_mergedChunk)  {
        _mergedChunk.swap(partialChunk);
        assert(!partialChunk);
        return false;
    }
    _mergedChunk->setCount(0); // unknown
    MemChunk* mergedChunk = _mergedChunk.get();
    assert(mergedChunk);
    assert(mergedChunk->getAttributeDesc().getId() == attId);
    assert(mergedChunk->getAttributeDesc().getDefaultValue() == attr.getDefaultValue());
    assert(mergedChunk->getFirstPosition(false) == partialChunk->getFirstPosition(false));

    mergedChunk->merge(*partialChunk, query);

    return true;
}

bool MultiStreamArray::DefaultChunkMerger::isEmptyBitMap(const shared_ptr<MemChunk>& chunk)
{
    AttributeDesc const* ebmAttr = chunk->getArrayDesc().getEmptyBitmapAttribute();
    return (ebmAttr != NULL && chunk->getAttributeDesc().getId() == ebmAttr->getId());
}

shared_ptr<MemChunk>
MultiStreamArray::DefaultChunkMerger::getMergedChunk(AttributeID attId,
                                                     const shared_ptr<Query>& query)
{
    static const char* funcName = "DefaultChunkMerger::getMergedChunk: ";
    assert(_mergedChunk);
    checkChunkMagic(*_mergedChunk);

    const bool isEbm = isEmptyBitMap(_mergedChunk);
    size_t mergedFootprint = 0;
    size_t mergedNumElems = 0;

    if (isEbm) {
        assert(_mergedChunk->getAttributeDesc().getId() ==
               (_mergedChunk->getArrayDesc().getAttributes().size()-1));
        const ConstRLEEmptyBitmap mergedEbm(reinterpret_cast<char*>(_mergedChunk->getData()));
        mergedFootprint = mergedEbm.packedSize();
    } else {
        const ConstRLEPayload mergedPayload(reinterpret_cast<char*>(_mergedChunk->getData()));
        mergedFootprint = mergedPayload.packedSize();
        mergedNumElems  = mergedPayload.count();
    }
    assert(mergedFootprint>0);

    if (_numElems != mergedNumElems && !isEbm) {
        assert(_numElems > mergedNumElems);
        assert(!isEbm);
        if (_isEnforceDataIntegrity) {
            stringstream ss;
            ss << "chunk " << CoordsToStr(_mergedChunk->getFirstPosition(false));
            throw USER_EXCEPTION(SCIDB_SE_REDISTRIBUTE, SCIDB_LE_DATA_COLLISION)
            << ss.str();
        }
        if (!_hasDataIntegrityIssue) {
            LOG4CXX_WARN(logger, funcName
                         << "Data collision is detected in chunk at "
                         << CoordsToStr(_mergedChunk->getFirstPosition(false))
                         << " for attribute ID = " << _mergedChunk->getAttributeDesc().getId()
                         << ". Add log4j.logger.scidb.qproc.streamarray=TRACE to the log4cxx config file for more");
            _hasDataIntegrityIssue = true;
        } else {
            LOG4CXX_TRACE(logger, funcName
                          << "Data collision is detected in chunk at "
                          << CoordsToStr(_mergedChunk->getFirstPosition(false))
                          << " for attribute ID = " << _mergedChunk->getAttributeDesc().getId());
        }
    }

    if (_chunkSizeLimit && mergedFootprint > _chunkSizeLimit) {
        throw USER_EXCEPTION(SCIDB_SE_OPERATOR, SCIDB_LE_CHUNK_TOO_LARGE)
        << mergedFootprint << _chunkSizeLimit;
    }

    shared_ptr<MemChunk> result;
    _mergedChunk.swap(result);
    _numElems = 0;
    assert(result);
    assert(!_mergedChunk);
    return result;
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
                                   bool enforceDataIntegrity,
                                   boost::shared_ptr<Query>const& query)
: StreamArray(arr, false),
  _nStreams(n),
  _localStream(localStream),
  _enforceDataIntegrity(enforceDataIntegrity),
  _resultChunks(arr.getAttributes().size()),
  _chunkMergers(arr.getAttributes().size()),
  _readyPositions(arr.getAttributes().size()),
  _notReadyPositions(arr.getAttributes().size()),
  _currPartialStreams(arr.getAttributes().size()),
  _hasDataIntegrityIssue(false),
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
        _chunkMergers[attId] = boost::make_shared<DefaultChunkMerger>(_enforceDataIntegrity);
    }
}

ConstChunk const*
MultiStreamArray::nextChunk(AttributeID attId, MemChunk& chunk)
{
    static const char *funcName = "MultiStreamArray::nextChunk: ";

    ASSERT_EXCEPTION( (attId < getArrayDesc().getAttributes().size()) , funcName);
    assert(attId < _resultChunks.size());
    assert(attId < _chunkMergers.size());
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

        logReadyPositions(readyPos, attId);

        bool notMyStream = false;
        if (currPartialStreams.empty()) {
            // starting a new chunk, find all partial chunk streams

            assert(_currMinPos[attId].empty());

            PositionMap::value_type const& minElem = readyPos.top();
            _currMinPos[attId] = minElem.getCoords();
            notMyStream = (minElem.getDest() != _localStream);

            if (_resultChunks[attId] && !notMyStream) {
                scidb::CoordinatesLess comp;
                if (!comp(_resultChunks[attId]->getFirstPosition(false), _currMinPos[attId])) {
                    if (isEnforceDataIntegrity()) {
                        throw USER_EXCEPTION(SCIDB_SE_REDISTRIBUTE, SCIDB_LE_CHUNK_POSITION_OUT_OF_ORDER)
                        << CoordsToStr(_currMinPos[attId]);
                    }
                    if (!_hasDataIntegrityIssue) {
                        LOG4CXX_WARN(logger, funcName << "Data chunk at position " << CoordsToStr(_currMinPos[attId])
                                     << " for attribute ID = " << attId << " is received out of (row-major) order"
                                     << ". Add log4j.logger.scidb.qproc.streamarray=TRACE to the log4cxx config file for more");
                        _hasDataIntegrityIssue = true;
                    } else {
                        LOG4CXX_TRACE(logger, funcName << "Data chunk at position " << CoordsToStr(_currMinPos[attId])
                                      << " for attribute ID = " << attId << " is received out of (row-major) order");
                    }
                }
                LOG4CXX_TRACE(logger, funcName << "clearing old chunk attId= " << attId);
                _resultChunks[attId].reset();
            }
            while (!readyPos.empty()) {
                if (readyPos.top().getCoords() != _currMinPos[attId]) { break; }
                assert(notMyStream == (readyPos.top().getDest() != _localStream));
                currPartialStreams.push_back(readyPos.top().getSrc());
                readyPos.pop();
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
            // some stream positions are for chunks that are not destined for this instance
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

    _currMinPos[attId].clear();
    const MemChunk* result = _resultChunks[attId].get();
    return result;
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
                readyPos.push(SourceAndDest(pos, stream, destStream));
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
    boost::shared_ptr<Query> query(Query::getValidQueryPtr(_query));
    assert(_chunkMergers[attId]);

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
                    readyPos.push(SourceAndDest(pos, stream, destStream));
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

        assert(_currMinPos[attId] == next->getFirstPosition(false));
        if (!_chunkMergers[attId]->mergePartialChunk(stream, attId,mergeChunk,query)) {
            assert(!mergeChunk);
            mergeChunk = boost::make_shared<MemChunk>();
        }
        assert(mergeChunk);
    }
    if (err) { throw *err; }

    _resultChunks[attId] = _chunkMergers[attId]->getMergedChunk(attId, query);
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
                readyPos.push(SourceAndDest(pos, stream, destStream));
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


void
MultiStreamArray::logReadyPositions(PositionMap& readyPos,
                                    const AttributeID attId)
{
    static const char *funcName = "MultiStreamArray::logReadyPositions: ";
    if (!logger->isTraceEnabled()) {
        return;
    }
    // a bit ugly but only when tracing
    PositionMap tmp;
    while (!readyPos.empty()) {
        PositionMap::value_type const& top = readyPos.top();
        tmp.push(top);
        LOG4CXX_TRACE(logger, funcName << "ready streams attId= " << attId
                      <<", src stream="<< top.getSrc()
                      <<", dst stream="<< top.getDest()
                      <<", pos="<< top.getCoords());
        readyPos.pop();
    }
    while (!tmp.empty()) {
        PositionMap::value_type const& top = tmp.top();
        readyPos.push(top);
        tmp.pop();
    }
}

SinglePassArray::SinglePassArray(ArrayDesc const& arr)
: StreamArray(arr, false),
  _enforceHorizontalIteration(false),
  _consumed(arr.getAttributes().size()),
  _rowIndexPerAttribute(arr.getAttributes().size(), 0)
{}

shared_ptr<ConstArrayIterator>
SinglePassArray::getConstIterator(AttributeID attId) const
{
    if (_iterators[attId]) { return _iterators[attId]; }

    // Initialize all attribute iterators at once
    // to avoid unnecessary exceptions
    // (in case when getConstIterator(aid) are not called for all attributes before iteration)
    Exception::Pointer err;
    for (AttributeID a=0, n=_iterators.size(); a<n; ++a) {
        try {
            if (!_iterators[a]) {
                StreamArray* self = const_cast<StreamArray*>(static_cast<const StreamArray*>(this));
                boost::shared_ptr<ConstArrayIterator> cai(new StreamArrayIterator(*self, a));
                const_cast< boost::shared_ptr<ConstArrayIterator>& >(_iterators[a]) = cai;
            }
        } catch (const StreamArray::RetryException& e) {
            if (a == attId) {
                err = e.copy();
            }
        }
    }
    if (err) { err->raise(); }
    assert(_iterators[attId]);
    return _iterators[attId];
}

ConstChunk const*
SinglePassArray::nextChunk(AttributeID attId, MemChunk& chunk)
{
    static const char* funcName="SinglePassArray:nextChunk: ";
    // Ensure that attributes are consumed horizontally
    while(true) {

        ConstChunk const* result(NULL);
        assert(attId < _rowIndexPerAttribute.size());

        const size_t nAttrs = _rowIndexPerAttribute.size();
        const size_t currRowIndex = getCurrentRowIndex();

        size_t& chunkIndex = _rowIndexPerAttribute[attId];

        if (chunkIndex != currRowIndex) {
            // the requested chunk must be in the current row
            ASSERT_EXCEPTION((currRowIndex > chunkIndex), funcName);
            ASSERT_EXCEPTION((chunkIndex == (currRowIndex-1)
                              || !_enforceHorizontalIteration) , funcName);

            result = &getChunk(attId, chunkIndex+1);
            ++chunkIndex;
            ++_consumed;
            assert(_consumed<=nAttrs || !_enforceHorizontalIteration);
            return result;
        }

        if (_enforceHorizontalIteration && _consumed < nAttrs) {
            // the previous row has not been fully consumed
            throw RetryException(REL_FILE, __FUNCTION__, __LINE__);
        }

        assert(_consumed==nAttrs || !_enforceHorizontalIteration);

        if (!moveNext(chunkIndex+1)) {
            // no more chunks
            return result;
        }

        // advance to the next row and get the chunk
        _consumed = 0;
        result = &getChunk(attId, chunkIndex+1);
        assert(result);
        ++chunkIndex;
        ++_consumed;

        if (hasValues(result)) { return result; }

        // run through the rest of the attributes discarding empty chunks
        for (size_t a=0; a < nAttrs; ++a) {
            if (a==attId) { continue; }
            ConstChunk const* result = nextChunk(a, chunk);
            ASSERT_EXCEPTION((!hasValues(result)), funcName);
            assert(getCurrentRowIndex() == _rowIndexPerAttribute[a]);
        }
        assert(_consumed == nAttrs);
        assert(getCurrentRowIndex() == _rowIndexPerAttribute[attId]);
    }
    ASSERT_EXCEPTION(false, funcName);
    return NULL;
}

bool
SinglePassArray::hasValues(const ConstChunk* chunk)
{
    bool isEmptyable = (getArrayDesc().getEmptyBitmapAttribute() != NULL);
    bool chunkHasVals = (!isEmptyable) || (!chunk->isEmpty());
    return (chunkHasVals && (chunk->getSize() > 0));
}

#endif //SCIDB_CLIENT
}
