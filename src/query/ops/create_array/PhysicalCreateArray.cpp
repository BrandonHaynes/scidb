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
 * @file
 *
 * @brief Physical DDL operator which create new array
 *
 * @author Artyom Smirnov <smirnoffjr@gmail.com>
 */

#include "query/Operator.h"
#include "array/TransientCache.h"
#include "system/SystemCatalog.h"

namespace scidb
{

struct PhysicalCreateArray: PhysicalOperator
{
    PhysicalCreateArray(const string& logicalName, const string& physicalName, const Parameters& parameters, const ArrayDesc& schema)
      : PhysicalOperator(logicalName, physicalName, parameters, schema)
    {}

    PhysicalBoundaries getOutputBoundaries(const vector<PhysicalBoundaries> & inputBoundaries,
                                           const vector< ArrayDesc>         & inputSchemas) const
    {
        return PhysicalBoundaries::createEmpty(
            ((shared_ptr<OperatorParamSchema>&)_parameters[1])->getSchema().getDimensions().size());
    }

    void preSingleExecute(shared_ptr<Query> query)
    {
        assert(_parameters.size() >= 2);
        const string name(((shared_ptr<OperatorParamArrayReference>&)_parameters[0])->getObjectName());
        ArrayDesc  schema(((shared_ptr<OperatorParamSchema>&)        _parameters[1])->getSchema());

        schema.setName(name);

        if (_parameters.size() >= 3)                    // 'temp' flag given?
        {
            schema.setTransient(true);                  // ...so mark schema
        }

        SystemCatalog::getInstance()->addArray(schema, psHashPartitioned);
    }

    shared_ptr< Array> execute(vector< shared_ptr< Array> >& inputArrays,shared_ptr<Query> query)
    {
        assert(inputArrays.empty());

        if (_parameters.size() >= 3) // 'transient' flag supplied?
        {
            ArrayDesc       schema;
            string    const name(((shared_ptr<OperatorParamArrayReference>&)_parameters[0])->getObjectName());

            SystemCatalog::getInstance()->getArrayDesc(name,schema,false);

            transient::record(MemArrayPtr(new MemArray(schema,query)));
        }

     // DDL commands do not return values:

        return shared_ptr<Array>();
    }
};

DECLARE_PHYSICAL_OPERATOR_FACTORY(PhysicalCreateArray, "create_array", "impl_create_array")

} //namespace
