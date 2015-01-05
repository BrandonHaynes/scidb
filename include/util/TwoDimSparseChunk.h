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
 * TwoDimSparseChunk.h
 *
 *  Created on: September 19, 2013.
 */

#include <boost/shared_ptr.hpp>
#include <boost/make_shared.hpp>
#include <boost/unordered_map.hpp>
#include <boost/functional/hash.hpp>
#include <map>

namespace scidb
{
/**
 * TwoDimSparseChunk is a data structure that efficiently represents a two-dim sparse chunk.
 * It supports two access modes: appending to the end of a row, or random access.
 * In the random access case, a dense array is used if the number of logical cells is no more than 10 million.
 *
 * @author Donghui Zhang
 */
template<class T>
class TwoDimSparseChunk
{
    typedef std::pair<int64_t, int64_t> Coords;
public:
    /**
     * threshold value determining if a dense array is used, in random-access mode
     */
    static const int64_t MaxNumLogicalCellsToUseDenseArray = 10*1000*1000;

    enum AccessMode
    {
        RowAppending,
        RandomAccess
    };

    struct CoordsAndVal
    {
        Coords _coords;
        T _v;

        CoordsAndVal(Coords const& coords, T const& v)
        :_coords(coords), _v(v)
        {}

        CoordsAndVal(int64_t row, int64_t col, T const& v)
        :_coords(row, col), _v(v)
        {}

        bool operator()(CoordsAndVal const& a, CoordsAndVal const& b)
        {
            if (a._coords.first < b._coords.first) {
                return true;
            }
            else if (a._coords.first > b._coords.first) {
                return false;
            }
            return a._coords.second < b._coords.second;
        }
    };

    // types used in RowAppending mode
    typedef std::vector<std::pair<int64_t, T> > Row;
    typedef boost::shared_ptr<Row> RowPtr;
    typedef boost::unordered_map<int64_t, RowPtr> Rows;

    // types used in RandomAccess mode -- not using a dense array
    typedef boost::unordered_map<Coords, T> SparseData;

private:
    // mode
    AccessMode _mode;

    // in the RandomAccess mode, whether a dense array is used
    bool _usingDenseArray;

    // metrics
    int64_t _rowStart;
    int64_t _colStart;
    int64_t _rowSize;
    int64_t _colSize;

    // data used in RowAppending mode
    Rows _rows;

    // data used in RandomAccessMode -- using a dense array
    std::vector<size_t> _denseIndex;   // size = _rowSize * _colSize; each value is the index into _denseData
    std::vector<CoordsAndVal> _denseData; // size = #actual elements.

    // data used in RandomAccessMode -- not using a dense array
    SparseData _sparseData;

    /**
     * convert (i, j) to an index in _denseIndex.
     * @param[in] i row
     * @param[in] j col
     * @param[out] indexInDenseIndex  the index of (i,j) in the dense index
     * @return whether the index points to a valid entry in _denseData
     *
     * @pre RandomAccess mode
     *
     */
    inline bool getIndexInDenseIndex(int64_t i, int64_t j, size_t& indexInDenseIndex)
    {
        assert(_mode==RandomAccess);
        assert(i>=_rowStart && i<_rowStart+_rowSize);
        assert(j>=_colStart && j<_colStart+_colSize);

        indexInDenseIndex = (i-_rowStart) * _colSize + j - _colStart;
        size_t indexInDenseData = _denseIndex[indexInDenseIndex];

        return indexInDenseData<_denseData.size() &&
                _denseData[indexInDenseData]._coords.first == i &&
                _denseData[indexInDenseData]._coords.second == j;
    }

public:
    /**
     * @param[in] mode  access mode
     * @param[in] rowStart the starting row coord
     * @param[in] colStart the starting col coord
     * @param[in] rowSize the size of row
     * @param[in] colSize the size of col
     */
    TwoDimSparseChunk(AccessMode mode, int64_t rowStart, int64_t colStart, int64_t rowSize, int64_t colSize)
    : _mode(mode), _usingDenseArray(true), _rowStart(rowStart), _colStart(colStart), _rowSize(rowSize), _colSize(colSize)
    {
        assert(colSize>0 && rowSize>0);

        if (_mode==RandomAccess) {
            // don't use a dense array if the size is more than 2^31-1, or if cannot allocate memory
            int64_t denseSize = rowSize * colSize;
            if ( rowSize > MaxNumLogicalCellsToUseDenseArray ||
                    colSize > MaxNumLogicalCellsToUseDenseArray ||
                    denseSize > MaxNumLogicalCellsToUseDenseArray) {
                _usingDenseArray = false;
            }
            else {
                // try to allocate the space
                try {
                    _denseIndex.resize(denseSize);
                }
                catch (std::bad_alloc& ba) {
                    _usingDenseArray = false;
                }
            }

            if (_usingDenseArray) {
                assert(_denseIndex.size()==static_cast<size_t>(denseSize));
            }
        }
    }

    /**
     * append <j, v> to the end of row i.
     * If row i does not exist, add row i first.
     * @param[in] i row
     * @param[in] j col
     * @param[in] v value
     *
     * @pre RowAppending mode
     */
    void append(int64_t i, int64_t j, const T& v)
    {
        assert(_mode==RowAppending);

        // Get the row; creating one if not exist.
        RowPtr ptr;

        if (!getRow(i, ptr)) {
            _rows[i] = boost::make_shared<Row>();
            getRow(i, ptr);
            assert(ptr);
        }

        // append
        ptr->push_back(std::pair<int64_t, T>(j, v));
    }

    /**
     * Get a whole row.
     * @param[in]   i         row
     * @param[out]  ptr       an iterator pointing at the requested row
     * @return whether the row exists
     *
     * @pre RowAppending mode
     */
    bool getRow(int64_t i, RowPtr& ptr)
    {
        assert(_mode==RowAppending);

        typename Rows::iterator it = _rows.find(i);
        if (it==_rows.end()) {
            return false;
        }
        ptr = it->second;
        return true;
    }

    /**
     * Add a value to an existing location, or set if the location was empty.
     * @param[in] i row
     * @param[in] j col
     * @param[in] v value
     * @param[in] tryToRemoveZero whether to try to remove a cell if after setOrAdd the value becomes zero.
     *
     * @pre RandomAccess mode
     */
    void setOrAdd(int64_t i, int64_t j, T const& v, bool tryRemoveZero = true)
    {
        assert(_mode==RandomAccess);

        if (_usingDenseArray) {
            size_t indexInDenseIndex = 0;
            if (getIndexInDenseIndex(i, j, indexInDenseIndex)) {
                _denseData[ _denseIndex[indexInDenseIndex] ]._v += v;
            }
            else {
                _denseData.push_back(CoordsAndVal(i, j, v));
                _denseIndex[indexInDenseIndex] = _denseData.size()-1;
            }
            return;
        }

        // using the hash
        Coords coords(i, j);

        typename SparseData::iterator it = _sparseData.find(coords);
        if (it==_sparseData.end()) {
            _sparseData[coords] = v;
        }
        else {
            T vNew = v + it->second;
            if (tryRemoveZero && vNew==0) {
               _sparseData.erase(it);
            }
            else {
                _sparseData[coords] = vNew;
            }
        }
    }

    /**
     * Copy out all data.
     * @param[out] output the data
     */
    void getUnsortedData(std::vector<CoordsAndVal>& output)
    {
        if (_mode==RowAppending) {
            for (typename Rows::const_iterator itRow = _rows.begin(); itRow!=_rows.end(); ++itRow) {
                int64_t i = itRow->first;
                Row const& row = *(itRow->second);
                output.reserve(output.size() + row.size());
                for (typename Row::const_iterator itCell = row.begin(); itCell!=row.end(); ++itCell) {
                    int64_t j = itCell->first;
                    T const& v = itCell->second;
                    output.push_back(CoordsAndVal(i, j, v));
                }
            }
        }
        else if (_usingDenseArray) {
            output.insert(output.end(), _denseData.begin(), _denseData.end());
        }
        else {
            for (typename SparseData::const_iterator it = _sparseData.begin(); it!=_sparseData.end(); ++it) {
                output.push_back(CoordsAndVal(it->first, it->second));
            }
        }
    }

    /**
     * Copy out all data, sorted in RMO.
     * @param[out] output the sorted data
     */
    void getSortedData(std::vector<CoordsAndVal>& output)
    {
        getUnsortedData(output);
        CoordsAndVal sorter(0,0,0);
        sort(output.begin(), output.end(), sorter);
    }

    /**
     * Is the set empty?
     * @return whether empty
     */
    bool empty()
    {
        if (_mode==RowAppending) {
            return _rows.empty();
        }
        else if (_usingDenseArray) {
            return _denseData.empty();
        }
        return _sparseData.empty();
    }
};
}
