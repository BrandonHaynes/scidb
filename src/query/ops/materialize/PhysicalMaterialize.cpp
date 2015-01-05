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
 * @file PhysicalMaterialize.cpp
 *
 * @author roman.simakov@gmail.com
 *
 * @brief This file implements physical set_temp operator
 * to save pointers to array and get in on the next iteration.
 */

#include "query/Operator.h"
#include "query/QueryProcessor.h"
#include "array/DelegateArray.h"

using namespace std;
using namespace boost;

namespace scidb
{

class PhysicalMaterialize: public PhysicalOperator
{
public:
    PhysicalMaterialize(const string& logicalName, const string& physicalName, const Parameters& parameters, const ArrayDesc& schema):
        PhysicalOperator(logicalName, physicalName, parameters, schema)
    {
    }

    virtual PhysicalBoundaries getOutputBoundaries(const std::vector<PhysicalBoundaries> & inputBoundaries,
                                                   const std::vector< ArrayDesc> & inputSchemas) const
    {
        return inputBoundaries[0];
    }

    boost::shared_ptr<Array> execute(vector< boost::shared_ptr<Array> >& inputArrays, boost::shared_ptr<Query> query)
    {
        assert(inputArrays.size() == 1);
        MaterializedArray::MaterializeFormat format = (MaterializedArray::MaterializeFormat)((boost::shared_ptr<OperatorParamPhysicalExpression>&)_parameters[0])->getExpression()->evaluate().getUint32();
        return boost::shared_ptr<Array>(new MaterializedArray(inputArrays[0], query, format));
    }
};

DECLARE_PHYSICAL_OPERATOR_FACTORY(PhysicalMaterialize, "materialize", "impl_materialize")

} //namespace
