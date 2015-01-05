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
 * PhysicalApply.cpp
 *
 *  Created on: Apr 11, 2010
 *      Author: Knizhnik
 */

#include "query/Operator.h"
#include "array/Metadata.h"
#include "array/Array.h"
#include "query/ops/apply/ApplyArray.h"


namespace scidb {

using namespace boost;
using namespace std;

class PhysicalApply: public  PhysicalOperator
{
  public:
    PhysicalApply(const string& logicalName, const string& physicalName, const Parameters& parameters, const ArrayDesc& schema):
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
        assert(_parameters.size()%2 == 0);

        vector<shared_ptr<Expression> > expressions(0);

        size_t currentParam = 0;
        for(size_t i =0; i< _schema.getAttributes().size(); i++)
        {
            assert(_parameters[currentParam]->getParamType() == PARAM_ATTRIBUTE_REF);
            assert(_parameters[currentParam+1]->getParamType() == PARAM_PHYSICAL_EXPRESSION);

            string const& schemaAttName = _schema.getAttributes()[i].getName();
            string const& paramAttName = ((boost::shared_ptr<OperatorParamReference>&)_parameters[currentParam])->getObjectName();

            if(schemaAttName!=paramAttName)
            {
                expressions.push_back( shared_ptr<Expression> ());
            }
            else
            {
                expressions.push_back(((boost::shared_ptr<OperatorParamPhysicalExpression>&)_parameters[currentParam+1])->getExpression());
                currentParam+=2;
            }

            if(currentParam == _parameters.size())
            {
                for (size_t j = i+1; j< _schema.getAttributes().size(); j++)
                {
                    expressions.push_back( shared_ptr<Expression> () );
                }
                break;
            }
        }

        assert(currentParam == _parameters.size());
        assert(expressions.size() == _schema.getAttributes().size());

        boost::shared_ptr<Array> input = inputArrays[0];
        return boost::shared_ptr<Array>(new ApplyArray(_schema, input, expressions, query, _tileMode));
    }
};

DECLARE_PHYSICAL_OPERATOR_FACTORY(PhysicalApply, "apply", "physicalApply")

}  // namespace scidb
