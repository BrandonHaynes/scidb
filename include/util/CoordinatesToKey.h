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
 * CoordinatesToKey.h
 *
 *  Created on: September 26, 2013.
 */

#include <vector>
#include <query/Operator.h>

namespace scidb
{
/**
 * CoordinatesToKey is a utility class that turns a Coordinates to a key by replacing certain coordinates with their default values.
 *
 * @author Donghui Zhang
 */
class CoordinatesToKey
{
    typedef std::pair<size_t, Coordinate> KeyConstraint;

    std::vector<KeyConstraint> _keyConstraints;  // the dimensions for which coordinates should be equal to default to be a valid key

    size_t _maxDim;  // the maximal dim that is in the key constraint; must be less than the size of an actual Coordinates; for debug purposes only

    Coordinates tempCoords;  // a temporary Coordinates that helps turn an input Coordinates to a key

public:
    CoordinatesToKey() : _maxDim(0)
    {}

    /**
     * To initial state, in case a CoordinatesToKey object is reused.
     */
    void init()
    {
        _maxDim = 0;
        _keyConstraints.clear();
    }

    /**
     * Add a pair of dimension and default coordinate in a key.
     * @param[in] dim     a dimension at which all keys have the same coord
     * @param[in] coord   the coord at the dimension to qualify for a key
     */
    void addKeyConstraint(size_t dim, Coordinate coord = 0)
    {
        _keyConstraints.push_back(KeyConstraint(dim, coord));
        if (_maxDim < dim) {
            _maxDim = dim;
        }
    }

    /**
     * Whether a Coordinates is a key.
     */
    bool isKey(Coordinates const& coords)
    {
        assert(coords.size() > _maxDim);

        for (std::vector<KeyConstraint>::const_iterator it=_keyConstraints.begin(); it!=_keyConstraints.end(); ++it) {
            if (coords[it->first] != it->second) {
                return false;
            }
        }

        return true;
    }

    /**
     * Turn a Coordinates to a key.
     * @param[in]  coords   the input Coordinates
     * @return reference to a matching key: either the input coords (if it is already a key) or some constructed key
     */
    Coordinates const& toKey(Coordinates const& coords)
    {
        assert(coords.size() > _maxDim);

        if (isKey(coords)) {
            return coords;
        }

        tempCoords = coords;
        for (std::vector<KeyConstraint>::const_iterator it=_keyConstraints.begin(); it!=_keyConstraints.end(); ++it) {
            tempCoords[it->first] = it->second;
        }
        return tempCoords;
    }
};

}
