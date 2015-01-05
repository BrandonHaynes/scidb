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
 * \file PhysicalCancel.cpp
 *
 * \author roman.simakov@gmail.com
 */

#include <boost/foreach.hpp>
#include <queue>

#include "query/Operator.h"
#include "query/executor/SciDBExecutor.h"

using namespace std;
using namespace boost;

namespace scidb {

class PhysicalCancel: public PhysicalOperator
{
public:
    PhysicalCancel(const string& logicalName, const string& physicalName, const Parameters& parameters, const ArrayDesc& schema):
	    PhysicalOperator(logicalName, physicalName, parameters, schema)
	{
	}

    shared_ptr<Array> execute(vector<shared_ptr<Array> >& inputArrays, shared_ptr<Query> query)
    {
        const scidb::SciDB& scidb = getSciDBExecutor();
        const QueryID queryID = dynamic_pointer_cast<OperatorParamPhysicalExpression>(_parameters[0])->getExpression()->evaluate().getInt64();
        scidb.cancelQuery(queryID);

        return boost::shared_ptr<Array>();
    }
};

DECLARE_PHYSICAL_OPERATOR_FACTORY(PhysicalCancel, "cancel", "cancel_impl")

}  // namespace ops
