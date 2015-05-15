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
 * RowCollection.cpp
 *
 *  Created on: Sep 5, 2012
 *      Author: dzhang
 */

namespace scidb
{

template<class Group, class Hash>
void RowCollection<Group,Hash>::sortAllRows(uint32_t attrId, TypeId typeId, RowCollection<Group, Hash>* sortedArray) {
    assert(_mode == RowCollectionModeRead);

    CompareValueVectorsByOneValue compareValueVectors(attrId, typeId);

    for (size_t rowId=0; rowId<_counts.size(); ++rowId) {
        // Copy out all the items in the row.
        // Non-null items are pushed to 'items'.
        // Null items are pushed to 'nullItems'.
        vector<vector<Value> > items;
        vector<vector<Value> > nullItems;
        getWholeRow(rowId, items, true, attrId, &nullItems);    // true = separateNull

        // Sort.
        iqsort(&items[0], items.size(), compareValueVectors);

        // Push the nullItems at the end.
        for (size_t i=0; i<nullItems.size(); ++i) {
            items.push_back(nullItems[i]);
        }

        // Append to new array
        for (size_t i=0; i<items.size(); ++i) {
            sortedArray->appendItem(rowId, Group(), items[i]);
        }
    }
}

template<class Group, class Hash>
void RowCollection<Group,Hash>::getChunkIterators(vector<boost::shared_ptr<ChunkIterator> >& chunkIterators, size_t rowId) {
    Coordinates chunkPos(2);
    chunkPos[0] = rowId;
    chunkPos[1] = (_counts[rowId] / _chunkSize) * _chunkSize;

    if (isLastChunkFull(rowId)) {
        int chunkMode = ChunkIterator::SEQUENTIAL_WRITE;
        for (size_t i=0; i<_attributes.size(); ++i) {
            ScopedMutexLock lock(_mutexArrayIterators);
            Chunk& chunk = _arrayIterators[i]->newChunk(chunkPos, 0);
            chunkIterators[i] = chunk.getIterator(_query, chunkMode);
            chunkMode |= ChunkIterator::NO_EMPTY_CHECK;
        }
    }
    else {
        Coordinates itemPos(2);
        itemPos[0] = rowId;
        itemPos[1] = _counts[rowId];
        int chunkMode = ChunkIterator::APPEND_EMPTY_BITMAP | ChunkIterator::APPEND_CHUNK;
        for (size_t i=0; i<_attributes.size(); ++i) {
            ScopedMutexLock lock(_mutexArrayIterators);
            _arrayIterators[i]->setPosition(chunkPos);
            Chunk& chunk = _arrayIterators[i]->updateChunk();
            chunkIterators[i] = chunk.getIterator(_query, chunkMode);
            chunkMode |= ChunkIterator::NO_EMPTY_CHECK; // no empty check except for attribute 0
            chunkIterators[i]->setPosition(itemPos);
        }
    }
}

template<class Group, class Hash>
void RowCollection<Group,Hash>::getConstChunkIterators(
        vector<boost::shared_ptr<ConstChunkIterator> >& chunkIterators,
        Coordinates const& chunkPos) {
    assert( _attributes.size() == chunkIterators.size() );

    for (size_t i=0; i<_attributes.size(); ++i) {
        ScopedMutexLock lock(_mutexArrayIterators);
        _arrayIterators[i]->setPosition(chunkPos);
        const ConstChunk& chunk = _arrayIterators[i]->getChunk(); // getChunk() does not pin it
        chunkIterators[i] = chunk.getConstIterator();
    }
}

template<class Group, class Hash>
void RowCollection<Group,Hash>::flushOneRowInBuffer(size_t rowId, Items const& items) {
    assert(rowId<_counts.size());

    vector<boost::shared_ptr<ChunkIterator> > chunkIterators(_attributes.size());

    try {
        if (! isLastChunkFull(rowId)) { // only get chunk iterators if there exists a non-full last chunk.
            getChunkIterators(chunkIterators, rowId);
        }

        for (size_t v=0; v<items.size(); ++v) {
            vector<Value> const& item = items[v];

            if (isLastChunkFull(rowId)) { // when the last chunk was full, get the iterators here (right before append)
                getChunkIterators(chunkIterators, rowId);
            }

            for (size_t i=0; i<_attributes.size(); ++i) {
                chunkIterators[i]->writeItem(item[i]);
            }
            ++ _counts[rowId];

            if (isLastChunkFull(rowId)) { // after append, flush and clear the chunk iterators if the last chunk becomes full
                for (size_t i=0; i<_attributes.size(); ++i) {
                    chunkIterators[i]->flush();
                    chunkIterators[i].reset();
                }
            } else {
                for (size_t i=0; i<_attributes.size(); ++i) {
                    ++ (*chunkIterators[i]);
                }
            }
        }

        if (items.size()>0 && !isLastChunkFull(rowId)) {
            assert(chunkIterators[0]);
            for (size_t i=0; i<_attributes.size(); ++i) {
                chunkIterators[i]->flush();
                chunkIterators[i].reset();
            }
        } else {
            assert(! chunkIterators[0]);
        }
    } catch (std::exception& e) {
        LOG4CXX_DEBUG(logger, "[RowCollection] std::exception in RowIterator::appendItems(): " << e.what());
        throw;
    } catch (...) {
        LOG4CXX_DEBUG(logger, "[RowCollection] (...) exception in RowIterator::appendItem()" );
        throw;
    }
}

template<class Group, class Hash>
RowCollection<Group,Hash>::RowCollection(boost::shared_ptr<Query> const& query, const string& name, const Attributes& attributes, size_t chunkSize)
: _query(query), _attributes(attributes), _chunkSize(chunkSize), _sizeBuffered(0), _mode(RowCollectionModeAppend)
{
    assert(!attributes.empty());
    assert(chunkSize >= 2);

    // Use (CONFIG_MEM_ARRAY_THRESHOLD / 10) as the #bytes the unflushed items may have.
    _maxSizeBuffered = Config::getInstance()->getOption<size_t>(CONFIG_MEM_ARRAY_THRESHOLD) * MiB / 10;

    // Push the empty tag
    Attributes attributesWithET(attributes);
    attributesWithET.push_back(AttributeDesc(attributes.size(), DEFAULT_EMPTY_TAG_ATTRIBUTE_NAME,
           TID_BOOL, AttributeDesc::IS_EMPTY_INDICATOR, 0));

    // get the schema
    Dimensions dims(2);
    dims[0] = DimensionDesc("Row", 0, MAX_COORDINATE, 1, 0);
    dims[1] = DimensionDesc("Column", 0, MAX_COORDINATE, _chunkSize, 0);
    ArrayDesc schema(name, attributesWithET, dims);

    // create a MemArray
    _theArray = make_shared<MemArray>(schema,query);

    // get the array iterators
    _arrayIterators.reserve(attributes.size());
    for (size_t t=0; t<attributes.size(); ++t) {
        _arrayIterators.push_back(_theArray->getIterator(t));
    }
}

template<class Group, class Hash>
void RowCollection<Group,Hash>::appendItem(size_t& rowId, const Group& group, const vector<Value>& item) {
    assert(_mode == RowCollectionModeAppend);

    // prepare to append, if rowId is not known
    // Get the rowId for the group.
    // If the group did not exist in the map, create it, and add an entry to _counts.
    //
    if (rowId == UNKNOWN_ROW_ID) {
        GroupToRowIdIterator it = _groupToRowId.find(group);
        if (it == _groupToRowId.end()) {
            rowId = _counts.size();
            assert(rowId == _groupToRowId.size());
            std::pair<typename GroupToRowId::iterator, bool> resultPair = _groupToRowId.insert(std::pair<Group, size_t>(group, rowId));
            SCIDB_ASSERT(resultPair.second); // insertion should succeed
            _counts.push_back(0);
        }
        else {
            rowId = it->second;
        }
    }

    // Append to the buffer.
    MapRowIdToItems::iterator it = _appendBuffer.find(rowId);
    if (it == _appendBuffer.end()) {
        std::pair<typename MapRowIdToItems::iterator, bool> resultPair = _appendBuffer.insert(std::pair<size_t, Items>(rowId, Items()));
        assert(resultPair.second); // insertion should succeed
        it = resultPair.first;
    }
    it->second.push_back(item);

    // If the size of the buffered data is too large, flush it.
    BOOST_FOREACH(Value const& v, item) {
        _sizeBuffered += v.size();
    }

    if (_sizeBuffered > _maxSizeBuffered) {
        flushBuffer();
    } else if ((_sizeBuffered % _chunkSize) == 0) {
        _query->validate();
    }
}

template<class Group, class Hash>
void RowCollection<Group,Hash>::getWholeRow(size_t rowId, Items& items, bool separateNull, uint32_t attrId, Items* pNullItems) {
    assert(_mode==RowCollectionModeRead);
    assert(separateNull || (pNullItems==NULL));
    assert(items.empty());
    if (pNullItems!=NULL) {
        assert(pNullItems->empty());
    }

    boost::scoped_ptr<MyRowIterator> rowIterator(openRow(rowId));
    items.reserve(_counts[rowId]);
    TypeId strType = _attributes[attrId].getType();
    DoubleFloatOther type = getDoubleFloatOther(strType);
    while (!rowIterator->end()) {
        vector<Value> item(_attributes.size());
        rowIterator->getItem(item);
        if (separateNull && isNullOrNan(item[attrId], type)) {
            if (pNullItems!=NULL) {
                pNullItems->push_back(item);
            }
        } else {
            items.push_back(item);
        }
        ++(*rowIterator);
    }
}

}


