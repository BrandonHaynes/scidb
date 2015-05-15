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
 * RegionCoordinatesIterator.h
 *
 *  Created on: Jun 15, 2012
 *      Author: dzhang
 *  Iterators over all logical chunkPos in a multi-dim region.
 *
 *********************************************************************************************************************
 *  THE REQUEST TO JUSTIFY LOGICAL-SPACE ITERATION
 *  - The request:
 *    (a) All tools that iterate over the logical chunk/cell space,
 *        such as RegionCoordinatesIterator, should be accompanied with
 *        a warning, and a pointer to this note.
 *        E.g. see the comment for class RegionCoordinatesIterator.
 *    (b) All usages of such tools should be accompanied with a comment
 *        describing why it is ok to iterate over the logical space.
 *  - Requester: Donghui Zhang, 12/8/2014.
 *  - Rationale:
 *    Iterating over the logical space is dangerous. Say you have an extremely sparse array with only a few chunks, but
 *    with billions of logical chunks in the space. If you use a tool to iterate over the logical space, replying on some
 *    kind of probing to decide whether each chunkPos is valid, your algorithm may end up taking too long to run.
 *********************************************************************************************************************
 */

#ifndef REGIONCOORDINATESITERATOR_H_
#define REGIONCOORDINATESITERATOR_H_

#include <vector>
#include <array/Array.h>

namespace scidb
{

/**
 * RegionCoordinatesIteratorParam: parameters to the constructor of RegionCoordinatesIterator.
 */
struct RegionCoordinatesIteratorParam
{
    Coordinates _low;
    Coordinates _high;
    std::vector<size_t> _intervals;

    RegionCoordinatesIteratorParam(size_t size): _low(size), _high(size), _intervals(size)
    {}
};

/**
 * RegionCoordinatesIterator iterates over all the chunk-start coordinates, in a region described by a pair of Coordinates.
 *
 *  @note Use with caution! This class iterates over the logical space.
 *  @see THE REQUEST TO JUSTIFY LOGICAL-SPACE ITERATION in RegionCoordinatesIterator.h.
 */
class RegionCoordinatesIterator: public ConstIterator
{
    Coordinates _low;
    Coordinates _high;
    Coordinates _current;
    std::vector<size_t> _intervals;

public:
    /**
     * Constructor.
     */
    RegionCoordinatesIterator(const Coordinates& low, const Coordinates& high, const std::vector<size_t>& intervals) {
        init(low, high, intervals);
    }

    /**
     * Constructor.
     */
    RegionCoordinatesIterator(const RegionCoordinatesIteratorParam& param) {
        init(param._low, param._high, param._intervals);
    }

    /**
     * By default, every interval is 1.
     */
    RegionCoordinatesIterator(const Coordinates& low, const Coordinates& high) {
        std::vector<size_t> intervals(low.size());
        for (size_t i=0, n=intervals.size(); i<n; ++i) {
            intervals[i] = 1;
        }
        init(low, high, intervals);
    }

    /**
     * Destructor.
     */
    virtual ~RegionCoordinatesIterator() {
    }

    /**
     * Initialize the low and high coordinates.
     */
    void init(const Coordinates& low, const Coordinates& high, const std::vector<size_t>& intervals) {
        assert(low.size()==high.size());
        assert(intervals.size()==high.size());
        assert(low.size()>0);
        for (size_t i=0; i<low.size(); ++i) {
            assert(low[i] <= high[i]);
        }
        _low = low;
        _high = high;
        _intervals = intervals;
        _current = _low;
    }

    /**
     * Check if end is reached
     * @return true if iterator reaches the end of the region
     */
    virtual bool end() {
        return _current > _high;
    }

    /**
     * Position cursor to the next chunk.
     */
    virtual void operator ++() {
        for (size_t i=_current.size()-1; i>=1; --i) {
            _current[i] += _intervals[i];
            if (_current[i] <= _high[i]) {
                return;
            }
            _current[i] = _low[i];
        }
        _current[0] += _intervals[0];
    }

    /**
     * Advance to the smallest position >= a given newPos.
     * @param newPos  the position to reach or exceed.
     * @return whether any advancement is made.
     */
    bool advanceToAtLeast(Coordinates const& newPos) {
        if (_current >= newPos) {
            return false;
        }
        _current = newPos;

        if (end() || inBox()) {
            return true;
        }

        // I will scan all dimension from nDims-1 downto 1.
        // For each dimension, I will make sure current[i] is between low[i] and high[i].
        // If I have to increase current[i], not a problem: the new position is "advancing".
        // But if I have to decrease current[i], there is a "carryover" in that some later (i.e. smaller) dimension has to increase.
        // I'll use this variable to indicate whether there is a carryover.
        bool needToInc = false;

        for (size_t i=_current.size()-1; i>=1; --i) {
            if (_current[i] < _low[i]) {
                // No action needed to satisfy needToInc from before, because we have to increase dim i anyways.

                // Make sure the i'th dim is inside low[i], high[i]:
                _current[i] = _low[i];

                // No request from me to increase later dimensions.
                needToInc = false;
            }
            else if (_current[i] > _high[i]) {
                // We have to reduce _current[i], and increase some later dimension, whatsoever.
                // So we should directly reduce to _low[i].
                _current[i] = _low[i];
                needToInc = true;
            }
            else {
                // Good, this dimension is in range.

                // Try to deal with earlier needToInc.
                if (needToInc) {
                    if (_current[i]+(Coordinate)_intervals[i] <= _high[i]) {
                        _current[i] = _current[i] + _intervals[i];
                        needToInc = false;
                    }
                }

                // If I cannot handle previous needToInc, reduce to low[i] and let later dimensions deal with it.
                if (needToInc) {
                    _current[i] = _low[i];
                }

                // There is no problem with previous needToInc now.
                else {
                    // Shortcut! If inBox(), done with advancing!
                    if (inBox()) {
                        return true;
                    }

                    // Not inBox() means some future dimensions will need to increase.
                    // So I'll try to set myself to _low[i].
                    if (_current[i] > _low[i]) {
                        _current[i] = _low[i];
                        needToInc = true;
                    }
                }
            }
        } // end for

        // Now is the time to process dimension 0.
        if (needToInc) {
            _current[0] += _intervals[0];
        }
        if (_current[0] < _low[0]) {
            _current[0] = _low[0];
        }

        return true;
    }

    /**
     * Get coordinates of the current element.
     */
    virtual Coordinates const& getPosition() {
        return _current;
    }

    /**
     * Set iterator's current positions
     * @return true if specified position is valid (in the region),
     * false otherwise
     * @note the pos MUST be a chunk start.
     */
    virtual bool setPosition(Coordinates const& pos) {
        assert(pos.size()==_current.size());
        for (size_t i=0, n=pos.size(); i<n; ++i) {
            // Out of bound?
            if (pos[i]<_low[i] || pos[i]>_high[i]) {
                return false;
            }

            // Not the first cell in a chunk?
            if ((pos[i]-_low[i]) % _intervals[i] != 0) {
               assert(false);
               throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_UNREACHABLE_CODE) << "RegionCoordinatesIterator::setPosition";
            }
        }
        _current = pos;
        return true;
    }

    /**
     * Reset iterator to the first coordinates.
     */
    virtual void reset() {
        _current = _low;
    }

private:
    /**
     * Whether _current is inside the box specified by _low and _high.
     */
    bool inBox()
    {
        for (size_t i=0, n = _current.size(); i<n; ++i) {
            if (_current[i] < _low[i] || _current[i] > _high[i]) {
                return false;
            }
        }
        return true;
    }
};

}

#endif /* REGIONCOORDINATESITERATOR_H_ */
