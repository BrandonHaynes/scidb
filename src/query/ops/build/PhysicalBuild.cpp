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
 * PhysicalBuild.cpp
 *
 *  Created on: Apr 20, 2010
 *      Author: Knizhnik
 */

#include "query/Operator.h"
#include "array/Metadata.h"
#include "BuildArray.h"
#include "query/ops/input/InputArray.h"

using namespace std;
using namespace boost;

namespace scidb {


class PhysicalBuild: public PhysicalOperator
{
public:
	PhysicalBuild(const string& logicalName, const string& physicalName, const Parameters& parameters, const ArrayDesc& schema):
	    PhysicalOperator(logicalName, physicalName, parameters, schema),
	    _asArrayLiteral(false)
	{
        if (_parameters.size() == 3)
        {
            _asArrayLiteral = ((boost::shared_ptr<OperatorParamPhysicalExpression>&)_parameters[2])->getExpression()->evaluate().getBool();
        }
	}

    virtual ArrayDistribution getOutputDistribution(const std::vector<ArrayDistribution> & inputDistributions,
                                                    const std::vector< ArrayDesc> & inputSchemas) const
    {
        if (_asArrayLiteral)
            return ArrayDistribution(psLocalInstance);
        return ArrayDistribution(psHashPartitioned);
    }

	/***
	 * Build is a pipelined operator, hence it executes by returning an iterator-based array to the consumer
	 * that overrides the chunkiterator method.
	 */
    boost::shared_ptr<Array> execute(vector< boost::shared_ptr<Array> >& inputArrays, boost::shared_ptr<Query> query)
    {
        assert(inputArrays.size() == 0);

        boost::shared_ptr<Expression> expr = ((boost::shared_ptr<OperatorParamPhysicalExpression>&)_parameters[1])->getExpression();
        if (_asArrayLiteral)
        {
            //We will produce this array only on coordinator
            if (query->isCoordinator())
            {
                //InputArray is very access-restrictive, but we're building it from a string - so it's small!
                //So why don't we just materialize the whole literal array:
                static const bool dontEnforceDataIntegrity = false;
                static const bool notInEmptyMode = false;
                InputArray* ary = new InputArray(_schema, "", query, notInEmptyMode, dontEnforceDataIntegrity);
                shared_ptr<Array> input(ary);
                ary->openString(expr->evaluate().getString());
                shared_ptr<Array> materializedInput(new MemArray(input,query,false));
                return materializedInput;
            }
            else
            {
                return boost::shared_ptr<Array>(new MemArray(_schema,query));
            }
        }
        else
        {
            return boost::shared_ptr<Array>(new BuildArray(query, _schema, expr));
        }
    }

private:
    bool _asArrayLiteral;
};

DECLARE_PHYSICAL_OPERATOR_FACTORY(PhysicalBuild, "build", "physicalBuild")

}  // namespace scidb
