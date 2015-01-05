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
 * MultiConstIterators.cpp
 *
 *  Created on: Jun 10, 2013
 *      Author: dzhang
 */

#include <util/MultiConstIterators.h>
using namespace std;
using namespace boost;

namespace scidb
{
MultiConstIterators::MultiConstIterators(std::vector<boost::shared_ptr<ConstIterator> >& inputIters)
: _inputIters(inputIters)
{
    size_t i = 0;
    for (vector<shared_ptr<ConstIterator> >::const_iterator it = inputIters.begin(); it != inputIters.end(); ++it) {
        shared_ptr<ConstIterator> theIter = *it;
        if (theIter && ! theIter->end()) {
            _coordinatesAndIDs.insert(CoordinatesAndID( theIter->getPosition(), i ));
        }
        ++i;
    }
}

void MultiConstIterators::getIDsAtMinPosition(std::vector<size_t>& IDs)
{
    IDs.clear();
    IDs.reserve(_inputIters.size());

    if (_coordinatesAndIDs.empty()) {
        return;
    }

    Coordinates minPos = getPosition();

    // Get the iterator to the first element with a larger coordinates.
    // To perform the search, we'll use set::upper_bound().
    // The search key is composed using the same coordinates with a very large ID.
    // This of course assumes CoordinatesAndID's comparison function is to compare coordinates followed by comparing ID.
    set<CoordinatesAndID>::const_iterator upper = _coordinatesAndIDs.upper_bound(CoordinatesAndID(minPos, _inputIters.size()));
    for (set<CoordinatesAndID>::const_iterator it = _coordinatesAndIDs.begin(); it != upper; ++it) {
        assert(coordinatesCompare(minPos, it->_coord) == 0);
        IDs.push_back(it->_ID);
    }
}

bool MultiConstIterators::end()
{
    return _coordinatesAndIDs.empty();
}

void MultiConstIterators::operator ++() {
    Coordinates minPos = getPosition();
    std::vector<size_t> IDs;
    getIDsAtMinPosition(IDs);

    // Remove the IDs that are at the min position.
    for (vector<size_t>::const_iterator it = IDs.begin(); it != IDs.end(); ++it) {
        assert(_coordinatesAndIDs.begin() != _coordinatesAndIDs.end());
        assert(coordinatesCompare(_coordinatesAndIDs.begin()->_coord, minPos) == 0);
        _coordinatesAndIDs.erase(_coordinatesAndIDs.begin());
    }

    // Add the IDs back with their new positions, after incrementing the input iterators.
    for (vector<size_t>::const_iterator it = IDs.begin(); it != IDs.end(); ++it) {
        boost::shared_ptr<ConstIterator>& inputIter = _inputIters[ *it ];
        assert(! inputIter->end());
        ++(*inputIter);
        if (! inputIter->end()) {
            _coordinatesAndIDs.insert(CoordinatesAndID(inputIter->getPosition(), *it));
        }
    }
}

Coordinates const& MultiConstIterators::getPosition()
{
    std::set<CoordinatesAndID>::const_iterator it = _coordinatesAndIDs.begin();
    if (it == _coordinatesAndIDs.end()) {
        assert(false);
        throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_UNREACHABLE_CODE) << "MultiConstIterators::getPositkion";
    }
    return it->_coord;
}

bool MultiConstIterators::setPosition(Coordinates const& pos)
{
    assert(false);
    throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_UNREACHABLE_CODE) << "MultiConstIterators::setPosition";
    return false;
}

void MultiConstIterators::reset()
{
    assert(false);
    throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_UNREACHABLE_CODE) << "MultiConstIterators::reset";
}

}
