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
 * AnalyzeAggregate.h
 *
 *  Created on: May 10, 2012
 *      Author: egor.pugin@gmail.com
 */

#include "query/Aggregate.h"

namespace scidb
{

class AnalyzeAggregate : public Aggregate
{
private:
    static const size_t k = 17; //16 = 64K, 17 = 128K, ...
    const size_t k_comp;
    const size_t m;
public:
    AnalyzeAggregate();
    virtual ~AnalyzeAggregate() {}

    virtual bool ignoreNulls() const
    {
        return true;
    }

    virtual Type getStateType() const
    {
        return TypeLibrary::getType(TID_BINARY);
    }

    virtual AggregatePtr clone() const
    {
        return AggregatePtr(new AnalyzeAggregate());
    }

    virtual AggregatePtr clone(Type const& aggregateType) const
    {
        return clone();
    }

    virtual void initializeState(Value& state);
    virtual void accumulate(Value& state, Value const& input);
    virtual void merge(Value& dstState, Value const& srcState);
    virtual void finalResult(Value& result, Value const& state);
};

} //namespace scidb
