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
 * NormalizeArray.cpp
 *
 *  Created on: Jun 30, 2010
 *      Author: Knizhnik
 */

#include "NormalizeArray.h"

namespace scidb {

using namespace boost;

NormalizeChunkIterator::NormalizeChunkIterator(DelegateChunk const* chunk, int iterationMode, double len):
        DelegateChunkIterator(chunk, iterationMode), _value(TypeLibrary::getType(TID_DOUBLE))
{
    _len = len;
}

 Value& NormalizeChunkIterator::getItem()
{
    // TODO: insert converter here
    Value& v = DelegateChunkIterator::getItem();
    if (v.isNull()) { 
        return v;
    } else { 
        _value.setDouble(v.getDouble() / _len);
        return _value;
    }
}

DelegateChunkIterator* NormalizeArray::createChunkIterator(DelegateChunk const* chunk, int iterationMode) const
{
    return chunk->getAttributeDesc().getId() == 0 
        ? (DelegateChunkIterator*)new NormalizeChunkIterator(chunk, iterationMode, _len)
        : DelegateArray::createChunkIterator(chunk, iterationMode);
}

NormalizeArray::NormalizeArray(ArrayDesc const& schema, boost::shared_ptr<Array> inputArray, double len)
: DelegateArray(schema, inputArray, false)
{
    _len = len;
}

}
