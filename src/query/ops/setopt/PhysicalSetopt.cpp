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
 * @file PhysicalSetopt.cpp
 *
 * @author knizhnik@garret.ru
 *
 * Physical implementation of SETOPT operator for setopting data from text files
 */

#include "query/Operator.h"
#include "query/OperatorLibrary.h"
#include "array/TupleArray.h"
#include "system/Config.h"

#include <log4cxx/logger.h>

// Logger for operator. static to prevent visibility of variable outside of file
static log4cxx::LoggerPtr logger(
    log4cxx::Logger::getLogger("scidb.query.ops.setopt"));

using namespace std;
using namespace boost;

namespace scidb
{

class PhysicalSetopt: public PhysicalOperator
{
  public:
    PhysicalSetopt(const string& logicalName,
                   const string& physicalName,
                   const Parameters& parameters,
                   const ArrayDesc& schema)
        : PhysicalOperator(logicalName, physicalName, parameters, schema)
    { }

    shared_ptr<Array> execute(vector< shared_ptr<Array> >& inputArrays, shared_ptr<Query> query)
    {
        string oldValue;

        boost::shared_ptr<TupleArray> tuples(boost::make_shared<TupleArray>(_schema, _arena));

        shared_ptr<OperatorParamPhysicalExpression> p0 =
            (shared_ptr<OperatorParamPhysicalExpression>&)_parameters[0];
        string name = p0->getExpression()->evaluate().getString();

        if (_parameters.size() == 2) {
            shared_ptr<OperatorParamPhysicalExpression> p1 =
                (shared_ptr<OperatorParamPhysicalExpression>&)_parameters[1];
            string newValue = p1->getExpression()->evaluate().getString();

            try {
                oldValue =
                    Config::getInstance()->setOptionValue(name, newValue);
            }
            catch (std::exception& e) {
                LOG4CXX_WARN(logger, "Cannot set option '" << name << "' to '"
                             << newValue << "': " << e.what());
                throw USER_EXCEPTION(SCIDB_SE_EXECUTION,
                                     SCIDB_LE_ERROR_NEAR_CONFIG_OPTION)
                    << e.what() << name;
            }

            Value tuple[2];
            tuple[0].setString(oldValue.c_str());
            tuple[1].setString(newValue.c_str());
            tuples->appendTuple(tuple);
        } else {
            oldValue = Config::getInstance()->getOptionValue(name);
            Value tuple[1];
            tuple[0].setString(oldValue.c_str());
            tuples->appendTuple(tuple);
        }
        return tuples;
    }
};

DECLARE_PHYSICAL_OPERATOR_FACTORY(PhysicalSetopt, "setopt", "physicalSetopt")

} //namespace
