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
 * RowCollection.h
 *
 *  Created on: May 24, 2012
 *      Author: dzhang
 *
 *  RowCollection is a 2D mem array that simulates a collection of rows.
 *  At any time, the RowCollection is in one of two modes: read, or append.
 *
 *  In the append mode, appendItem() can be called.
 *  The item is not immediately inserted. Rather, it is inserted to a buffer.
 *  The items in the buffer are actually appended to the RowCollection when
 *  (a) The total size of the values exceeds a memory threshold;
 *  (b) The user calls switchMode() to switch to read mode; or
 *  (c) The RowCollection is closed.
 *
 *  In the read mode, openRow() can be called to return a RowIterator.
 *  The member functions of the RowIterator, getItem(), end(), operator++(), and close(), may be called.
 *
 * @example
 *  A typical usage of the RowCollection is as follows.
 *
 *  // Define a RowCollection. A newly defined RowCollection is in append mode.
 *  RowCollection rc(...);
 *
 *  // [slow] Append items by group
 *  size_t resultRowId = UNKNOWN_ROW_ID;
 *  rc.appendItem(resultRowId, group, item);
 *
 *  // [fast] Append item with the returned resultRowId
 *  rc.appendItem(resultRowId, Coordinates(), item);
 *
 *  // Switch to read mode.
 *  rc.switchMode(RowCollectionModeRead)
 *
 *  // Scan the items in a group.
 *  if (!rc.existsGroup(group)) return;
 *  size_t rowId = rc.rowIdFromExistingGroup(group)
 *  scoped_ptr<RowIterator> ptr(rc.openRow(rowId))
 *  while (!ptr->end()) {
 *      ptr->getItem()
 *      ++ (*ptr);
 *  }
 *
 *  // Close the iterator.
 *  ptr.reset()
 *
 *  Each row is a separate group. So the chunk interval in row is 1.
 *  Each column is one element in some src array, which needs to be
 *  regrouped. The chunk interval in column is by default 10 KiB.  A
 *  new type of iterator is provided to manipulate access to the array
 *  elements.
 *
 *  The class is a template class, which takes as input a 'Group' class.
 *  The 'Group' class will be used as a key to boost::unordered_map.
 *
 *  Note to Developers:
 *  Externally, RowIterator::getItem(), and RowCollection::appendItem() deal with vector<Value> of size = #attributes.
 *  Internally, a hidden attribute, the EmptyTag, is used, like in any other SciDB array.
 *  RowCollection::_attributes.size() and RowIterator::_numAttribute() do NOT include the empty tag.
 *  Writing to empty tag is implicit - we make all but the first attribute NO_EMPTY_CHECK.
 *
 */
#ifndef ROWCOLLECTION_H_
#define ROWCOLLECTION_H_

#include <boost/scoped_ptr.hpp>
#include <boost/unordered_map.hpp>
#include <algorithm>
#include <exception>
#include <array/Metadata.h>
#include <system/Constants.h>
#include <util/iqsort.h>
#include <log4cxx/logger.h>
#include <log4cxx/basicconfigurator.h>
#include <log4cxx/helpers/exception.h>
#include <util/ValueVector.h>
#include <util/Mutex.h>

namespace scidb
{
static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.array.RowCollection"));

template<class Group, class Hash> class RowCollection;
template<class Group, class Hash> class RowIterator;

typedef bool RowCollectionMode;
const bool RowCollectionModeRead = true;
const bool RowCollectionModeAppend = false;
const size_t defaultChunkSize = 10*KiB;
const size_t UNKNOWN_ROW_ID = static_cast<size_t>(-1);

/**
 * ConstIterator through all elements in a row. The iterator combines array and chunk iterators.
 *
 * @pre In parent, the group's rowId exists in the GroupToRowId map.
 *
 * @note
 * At any time, end() is true iff _chunkIterators[i] does NOT exist.
 *
 * Array vs. chunk iterators:
 *   - Conceptually, each row iterator has a vector of arrayIterators (one for each attribute), and a vector of chunkIterators.
 *   - All the row iterators share the same vector of arrayIterators.
 *   - Each row iterator manages its own vector of chunkIterators.
 *
 */
template<class Group, class Hash = boost::hash<Group> >
class RowIterator : public ConstIterator
{
    typedef vector<boost::shared_ptr<ConstChunkIterator> > ChunkIterators;
    typedef RowCollection<Group, Hash> MyRowCollection;

private:
    size_t _rowId;
    size_t _numAttributes;
    size_t _chunkSize;
    size_t _totalInRow;
    size_t _locInRow;
    ChunkIterators _chunkIterators;

    // A volatile variable that is used to turn columnId to a two-dim coordinates.
    // This is not thread safe!
    Coordinates _tmpTwoDim;

    // A reference to the RowCollection is stored, so as to call getConstChunkIterators.
    MyRowCollection& _rc;

    // Given a columne, return a 2D coordinate, combining _rowId with the column.
    const Coordinates& toTwoDim(size_t columnId)
    {
        _tmpTwoDim[1] = static_cast<Coordinate>(columnId);
        return _tmpTwoDim;
    }

    /**
     * @return a position corresponding to the start of a chunk
     * on which the input position points.
     */
    size_t cellColToChunkCol(size_t cellColumn) const {
        return cellColumn - (cellColumn % _chunkSize);
    }

    /**
     * @param atColumn the chunk column computed by cellColToChunkCol
     * @return true if the chunk iterators exist and the chunk
     * column is on a chunk for which we have an iterator.
     */
    bool haveChunkIterators(size_t atColumn) const {
        return (_chunkIterators[0] &&
                (atColumn ==
                 cellColToChunkCol(_chunkIterators[0]->getPosition()[1])));
    }

    /**
     * Adjust chunkIterators to match _locInRow.
     *
     * @pre _locInRow was already set
     * @pre end()==false
     */
    void adjustChunkIterators()
    {
        ASSERT_EXCEPTION(!end(), "RowIterator::adjustChunkIterators() called but end() is true.");

        // Acquire the chunkIterators, if needed.
        size_t atColumn = cellColToChunkCol(_locInRow);
        if (!haveChunkIterators(atColumn)) {
            Coordinates const& chunkPos = toTwoDim(atColumn);
            _rc.getConstChunkIterators(_chunkIterators, chunkPos);
        }

        // Set local position for the chunk iterators.
        if (_locInRow % _chunkSize != 0) {
            Coordinates const& cellPos = toTwoDim(_locInRow);
            for (size_t i=0; i<_numAttributes; ++i) {
                _chunkIterators[i]->setPosition(cellPos);
            }
        }
    }

    /**
     * Reset the chunk iterators to NULL.
     */
    void resetChunkIterators()
    {
        if (_chunkIterators[0]) {
            for (size_t i=0; i<_numAttributes; ++i) {
                _chunkIterators[i].reset();
            }
        }
    }

public:
    /**
     * Constructor should not be called directly. The users should get an RowIterator* through RowCollection::openRow().
     */
    RowIterator(size_t rowId, size_t numAttributes, size_t chunkSize, size_t totalInRow, MyRowCollection& rc)
    : _rowId(rowId), _numAttributes(numAttributes), _chunkSize(chunkSize), _totalInRow(totalInRow), _locInRow(0),
      _chunkIterators(numAttributes), _tmpTwoDim(2), _rc(rc)
    {
        _tmpTwoDim[0] =_rowId;
        _tmpTwoDim[1] = 0;
        if (!end()) {
            adjustChunkIterators();
        }
    }

    virtual ~RowIterator()
    {
        resetChunkIterators();
        _locInRow = _totalInRow;
    }

    virtual void getItem(vector<Value>& item)
    {
        assert(! end());
        assert( _chunkIterators[0]);

        for (size_t i=0; i<_numAttributes; ++i) {
            item[i] = _chunkIterators[i]->getItem();
        }
    }

    virtual bool end()
    {
        assert(_locInRow <= _totalInRow);

        return _locInRow == _totalInRow;
    }

    /**
     * Advance to the next item in the same row.
     */
    virtual void operator ++()
    {
        assert(!end());
        assert(_chunkIterators[0]);

        ++ _locInRow;

        if (end()) { // Have I reached the end of the row?
            resetChunkIterators();
        } else if (_locInRow % _chunkSize==0) { // Have I crossed chunk boundary?
            adjustChunkIterators();
        } else { // Otherwise, just increment.
            for (size_t i=0; i<_numAttributes; ++i) {
                ++(*_chunkIterators[i]);
            }
        }
    }

    /**
     * Get the current position.
     */
    virtual Coordinates const& getPosition()
    {
        return toTwoDim(_locInRow);
    }

    /**
     * Set the current position.
     */
    virtual bool setPosition(Coordinates const& pos)
    {
        assert(pos.size()==2);
        assert(pos[0] == static_cast<Coordinate>(_rowId));

        if (pos[1]<0 || pos[1]>=static_cast<Coordinate>(_totalInRow)) {
            return false;
        }

        _locInRow = pos[1];
        adjustChunkIterators();
        return true;
    }

    /**
     * Reset to beginning. Could be made to work if needed.
     */
    virtual void reset()
    {
        _locInRow = 0;
        if (!end()) {
            adjustChunkIterators();
        }
    }

    /**
     * Which row in the RowCollection this iterator is at?
     */
    size_t getRowId() const
    {
        return _rowId;
    }
};

/**
 * The collection of rows.
 * For now, we only allow single-threaded appends.
 */
template<class Group, class Hash = boost::hash<Group> >
class RowCollection
{
public:
    typedef RowIterator<Group, Hash> MyRowIterator;
    typedef typename boost::unordered_map<Group, size_t, Hash> GroupToRowId;
    typedef typename boost::unordered_map<Group, size_t, Hash>::const_iterator GroupToRowIdIterator;
    typedef typename std::vector<std::vector<Value> > Items;
    typedef typename boost::unordered_map<size_t, Items > MapRowIdToItems; // rowId --> Items; this map is used in the buffer of appended items.

private:
    // query
    boost::shared_ptr<Query> _query;

    // the attributes to store
    Attributes _attributes;

    // the size of each row segment
    size_t _chunkSize;

    // the backend MemArray
    boost::shared_ptr<MemArray> _theArray;

    // the map that converts a group to a rowId
    GroupToRowId _groupToRowId;

    // #items in each row
    vector<size_t> _counts;

    // non-const array iterator that is shared among the row-iterators
    vector<boost::shared_ptr<ArrayIterator> > _arrayIterators;

    // mutex that protects the shared _arrayIterators
    Mutex _mutexArrayIterators;

    // a buffer of the appended items
    MapRowIdToItems _appendBuffer;

    // size of the buffered data
    size_t _sizeBuffered;

    // max size of the buffered data
    size_t _maxSizeBuffered;

    // the current mode of the RowCollection
    RowCollectionMode _mode;

    /**
     * Flush the buffer.
     */
    void flushBuffer()
    {
        assert(_mode == RowCollectionModeAppend);

        // Reset the counter.
        _sizeBuffered = 0;

        // Iterate through the buffer. For each <rowId, items>: append, then erase from the map.
        while (true) {
            MapRowIdToItems::iterator it = _appendBuffer.begin();
            if (it==_appendBuffer.end()) {
                return;
            }
            flushOneRowInBuffer(it->first, it->second);
            _appendBuffer.erase(it->first);
        }
    }

    /**
     * Whether the last chunk in a given row (if exists) is completely filled.
     */
    inline bool isLastChunkFull(size_t rowId)
    {
        return _counts[rowId] % _chunkSize == 0;
    }

    /**
     * Given a rowId, get the chunk iterators to append an item to the end.
     * @note If the last chunk was full, newChunk() will be called; otherwise updateChunk() will be called.
     * @note The newChunk() path is more efficient, because SEQUENTIAL_WRITE can be used.
     */
    void getChunkIterators(vector<boost::shared_ptr<ChunkIterator> >& chunkIterators, size_t rowId);

    /**
     * Flush one row.
     */
    void flushOneRowInBuffer(size_t rowId, Items const& items);

public:

    /**
     * Given a chunk position, get the chunk iterators to read data from.
     * @note This is called from RowIterator.
     */
    void getConstChunkIterators(vector<boost::shared_ptr<ConstChunkIterator> >& chunkIterators, Coordinates const& chunkPos);

    /**
     * Constructor.
     * @param   query
     * @param   name        aray name
     * @param   attributes  Should not include empty tag.
     * @param   chunkSize   Number of columns a chunk has.
     */
    RowCollection(boost::shared_ptr<Query> const& query, const string& name, const Attributes& attributes, size_t chunkSize = defaultChunkSize);

    /**
     * This allows the caller to iterate through the existing groups.
     * @return the map of group-->rowId
     */
    GroupToRowId const& getGroupToRowId()
    {
        return _groupToRowId;
    }

    /**
     * Create a RowIterator for read.
     * It is the responsibility of the caller to free the pointer.
     * It is recommended that the caller wraps the iterator in a scoped_ptr.
     * @param rowId  which row
     * @return a pointer to RowIterator
     */
    MyRowIterator* openRow(size_t rowId)
    {
        assert(_mode==RowCollectionModeRead);
        return new MyRowIterator(rowId, _attributes.size(), _chunkSize, _counts[rowId], *this);
    }

    /**
     * Toggle read/append modes.
     */
    void switchMode(RowCollectionMode destMode)
    {
        if (destMode==_mode) {
            return;
        }

        if (destMode == RowCollectionModeRead) {
            flushBuffer();
        }

        _mode = destMode;
    }

    /**
     * Initialize the groups information from another RowCollection.
     * @param  rc  the source RowCollection
     * @note This should be performed on a newly created RowCollection.
     */
    void copyGroupsFrom(const RowCollection& rc)
    {
        assert(_groupToRowId.size()==0);

        // Resize the array of counts.
        size_t size = rc._counts.size();
        _counts.resize(size);

        // Insert into _groupToRowId, and store the iterator in the corresponding GroupInfo.
        for (typename GroupToRowId::const_iterator iter = rc._groupToRowId.begin(); iter!=rc._groupToRowId.end(); ++iter) {
            std::pair<typename GroupToRowId::iterator, bool> resultPair = _groupToRowId.insert(std::pair<Group, size_t>(iter->first, iter->second));
            SCIDB_ASSERT(resultPair.second); // inserted
        }

        assert(_groupToRowId.size()==size);
    }

    /**
     * Get rowId from an existing group.
     * @param   group   group
     * @return  the rowId
     */
    size_t rowIdFromExistingGroup(const Group& group)
    {
        GroupToRowIdIterator it = _groupToRowId.find(group);
        assert(it!=_groupToRowId.end());
        return it->second;
    }

    /**
     * Whether a group exists.
     */
    bool existsGroup(const Group& group)
    {
        GroupToRowIdIterator it = _groupToRowId.find(group);
        return it!=_groupToRowId.end();
    }

    /**
     * Append an item.
     * @param   rowId   [in/out] UNKNOWN_ROW_ID means the rowId should be determined from the group; subsequent calls will use the returned rowId and ignore 'group'.
     * @param   group   the group; only used if rowId == UNKNOWN_ROW_ID.
     * @param   item    the item to append
     */
    void appendItem(size_t& rowId, const Group& group, const vector<Value>& item);

    /**
     * Read a whole row out.
     * @param   rowId           Which row to copy out.
     * @param   items           Placeholder for the items.
     * @param   separateNull    Whether to treat NULL cells separately.
     * @param   pNullItems      A pointer, if not NULL, to the placeholder for items whose value at attrId is null.
     * @param   attrId          Which attribute to treat separately.
     *
     * @note If separateNull == true  && pNullItems == NULL: ignore the NULL cells.
     * @note If separateNull == true  && pNullItems != NULL: put the NULL cells in pNullItems.
     * @note If separateNull == false: include the NULL cells in items.
     */
    void getWholeRow(size_t rowId, Items& items, bool separateNull, uint32_t attrId = 0, Items* pNullItems = NULL);

    /**
     * Sort all the rows based on a given attribute.
     * @param   attrId  which attribute to sort on
     * @param   typeId  what type is the sort attribute
     * @param   sortedArray an empty destination array with the same attributes and groups as the current array;
     * @note    Writing to a new array is faster because sequential-write can be used.
     * @note    Elements with NULL values are placed at the end of the sorted list.
     */
    void sortAllRows(uint32_t attrId, TypeId typeId, RowCollection<Group, Hash>* sortedArray);

    /**
     * How many rows are in the RowCollection?
     * A rowId is in [0..n-1].
     */
    size_t numRows()
    {
        return _counts.size();
    }
};

}

#include "RowCollection.cpp"

#endif /* ROWCOLLECTION_H_ */
