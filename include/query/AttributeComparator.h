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
 * AttributeComparator.h
 *
 *  Created on: Aug 25, 2014
 *      Author: Donghui Zhang
 *      Note:   Preserved from the removed file DimensionIndex.h.
 */

#ifndef ATTRIBUTECOMPARATOR_H_
#define ATTRIBUTECOMPARATOR_H_

#include <query/FunctionDescription.h>
#include <query/FunctionLibrary.h>

namespace scidb
{
/**
 * A comparator class that compares attribute values using the "<" function.
 *
 */
class AttributeComparator
{
  public:
    AttributeComparator()
     : _less(0)
    {}

    AttributeComparator(TypeId tid)
     : _less(getComparison(tid))
    {}

  public:
    bool operator()(const Value& v1, const Value& v2) const
    {
        const Value* operands[2] = {&v1,&v2};
        Value result;
        _less(operands, &result, NULL);
        return result.getBool();
    }

  private:
    static FunctionPointer getComparison(TypeId tid)
    {
        std::vector<TypeId>          inputTypes(2,tid);
        FunctionDescription     functionDesc;
        std::vector<FunctionPointer> converters;

        if (!FunctionLibrary::getInstance()->findFunction("<", inputTypes, functionDesc, converters, false) || !converters.empty())
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_EXECUTION,SCIDB_LE_OPERATION_NOT_FOUND) << "<" << tid;
        }

        return functionDesc.getFuncPtr();
    }

  private:
    FunctionPointer _less;
};

}

#endif /* ATTRIBUTECOMPARATOR_H_ */
