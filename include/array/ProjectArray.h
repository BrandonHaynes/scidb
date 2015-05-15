/*
**
* BEGIN_COPYRIGHT
*
* This file is part of SciDB.
* Copyright (C) 2014 SciDB, Inc.
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
 * ProjectArray.h
 *
 *  Created on: Nov 25, 2014
 *      Author: Donghui Zhang
 */

#ifndef PROJECTARRAY_H_
#define PROJECTARRAY_H_

#include <vector>
#include <boost/shared_ptr.hpp>

#include <array/DelegateArray.h>

namespace scidb
{
/**
 * A DelegateArray that is used to deliver a subset of the attributes from an input array, and/or to switch attribute IDs.
 * Internally, it keeps a vector, that projects a dest attrID to a src attrID.
 *
 * @note projection use case:
 *   - You have a shared_ptr<Array> called src, with three attributes <Name, Address, EmptyBitmap>.
 *   - You want to get a shared_ptr<Array> called dest, with two attributes <Name, EmptyBitmap>.
 *   - Solution: return a ProjectArray with projection = [0, 2].
 *   - Explanation: the dest attribute 0 comes from src attribute 0, and
 *     dest attribute 1 (i.e. index in projection) comes from src attribute 2 (i.e. the value at projection[1]).
 * @note switching-order use case:
 *   - You have the same src array with three attributes <Name, Address, EmptyBitmap>.
 *   - You want to get a dest array with attributes <Address, Name, EmptyBitmap>.
 *   - Solution: return a ProjectArray with projection = [1, 0, 2].
 */
class ProjectArray : public DelegateArray
{
private:
    /**
     * _projection[attrIdInDestArray] = attrIdInSourceArray.
     */
    std::vector<AttributeID> _projection;

public:
    virtual DelegateArrayIterator* createArrayIterator(AttributeID id) const
    {
        assert(id < _projection.size());
        assert(_projection[id] < inputArray->getArrayDesc().getAttributes().size());
        return new DelegateArrayIterator(*this, id, inputArray->getConstIterator(_projection[id]));
    }

    ProjectArray(ArrayDesc const& desc, boost::shared_ptr<Array> const& array, std::vector<AttributeID> const& projection)
    : DelegateArray(desc, array, true),
      _projection(projection)
    {
    }
};

} // namespace
#endif /* PROJECTARRAY_H_ */
