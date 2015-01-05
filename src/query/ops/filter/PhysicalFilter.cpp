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
 * PhysicalFilter.cpp
 *
 *  Created on: Apr 11, 2010
 *      Author: Knizhnik
 */

#include "query/Operator.h"
#include "array/Metadata.h"
#include "array/Array.h"
#include "query/ops/filter/FilterArray.h"


namespace scidb {

using namespace boost;
using namespace std;


class PhysicalFilter: public  PhysicalOperator
{
public:
   PhysicalFilter(const std::string& logicalName, const std::string& physicalName, const Parameters& parameters, const ArrayDesc& schema)
   : PhysicalOperator(logicalName, physicalName, parameters, schema)
    {
    }

    virtual PhysicalBoundaries getOutputBoundaries(const std::vector<PhysicalBoundaries> & inputBoundaries,
                                                   const std::vector< ArrayDesc> & inputSchemas) const
    {
        //TODO:OPTAPI optimization opportunities here
        return inputBoundaries[0];
    }

        /***
         * Filter is a pipelined operator, hence it executes by returning an iterator-based array to the consumer
         * that overrides the chunkiterator method.
         */
        boost::shared_ptr<Array> execute(std::vector< boost::shared_ptr<Array> >& inputArrays,
                              boost::shared_ptr<Query> query)
    {
        assert(inputArrays.size() == 1);
        assert(_parameters.size() == 1);
        assert(_parameters[0]->getParamType() == PARAM_PHYSICAL_EXPRESSION);
        
        return boost::shared_ptr<Array>(new FilterArray(_schema, inputArrays[0],
                                                        ((boost::shared_ptr<OperatorParamPhysicalExpression>&)_parameters[0])->getExpression(), query, _tileMode));
    }

};

DECLARE_PHYSICAL_OPERATOR_FACTORY(PhysicalFilter, "filter", "physicalFilter")

}  // namespace scidb
