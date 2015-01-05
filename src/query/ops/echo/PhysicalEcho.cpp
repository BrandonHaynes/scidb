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
 * @author Konstantin Knizhnik
 *
 * @brief Print message in log
 */

#include "query/Operator.h"
#include "query/OperatorLibrary.h"
#include "array/TupleArray.h"
#include <log4cxx/logger.h>

using namespace std;
using namespace boost;

namespace scidb
{

static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.query.ops.echo"));

class PhysicalEcho: public PhysicalOperator
{
public:
    PhysicalEcho(const string& logicalName, const string& physicalName, const Parameters& parameters, const ArrayDesc& schema):
        PhysicalOperator(logicalName, physicalName, parameters, schema)
    {
    }

    shared_ptr<Array> execute(vector<shared_ptr<Array> >& inputArrays,
            shared_ptr<Query> query)
    {
        assert(inputArrays.size() == 0);
        assert(_parameters.size() == 1);

        //We will produce this array only on coordinator
        if (query->getCoordinatorID() == COORDINATOR_INSTANCE)
        {
            std::string const text = ((boost::shared_ptr<OperatorParamPhysicalExpression>&)_parameters[0])->getExpression()->evaluate().getString();
            vector<boost::shared_ptr<Tuple> > tuples(1);
            Tuple& tuple = *new Tuple(1);
            tuples[0] = shared_ptr<Tuple>(&tuple);
            tuple[0].setString(text.c_str());
            LOG4CXX_TRACE(logger, text);
            return shared_ptr<Array>(new TupleArray(_schema, tuples));
        }

        return boost::shared_ptr<Array>(new MemArray(_schema,query));
    }
};

DECLARE_PHYSICAL_OPERATOR_FACTORY(PhysicalEcho, "echo", "impl_echo")

} //namespace
