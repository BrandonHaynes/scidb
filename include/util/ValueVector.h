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
 * ValueVector.h
 *
 *  Created on: Jun 18, 2012
 *      Author: dzhang
 *  This file defines utility methods for a vector of Values.
 */

#ifndef VALUEVECTOR_H_
#define VALUEVECTOR_H_

#include <vector>
#include <query/AttributeComparator.h>

namespace scidb
{

/**
 * A wrapper class for comparing two vector<Value> using a single attribute.
 * @return -1 (if i1<i2); 0 (if i1==i2); 1 (if i1>i2)
 */
class CompareValueVectorsByOneValue
{
  private:
    uint32_t _attrId;
    AttributeComparator _comp;

  public:
    inline int operator()(const std::vector<Value>& i1, const std::vector<Value>& i2) {
        assert(i1.size()==i2.size());
        assert(_attrId < i1.size());

        if (_comp(i1[_attrId], i2[_attrId])) {
            return -1;
        }
        if (_comp(i2[_attrId], i1[_attrId])) {
            return 1;
        }
        return 0;
    }

    CompareValueVectorsByOneValue(uint32_t attrId, TypeId typeId): _attrId(attrId), _comp(typeId) {
    }
};

}


#endif /* VALUEVECTOR_H_ */
