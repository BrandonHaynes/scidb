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
 * @file PhysicalInput.cpp
 *
 * @author roman.simakov@gmail.com
 *
 * Physical implementation of INPUT operator for inputing data from text file
 * which is located on coordinator
 */

#include <string.h>
#include <log4cxx/logger.h>

#include "query/Operator.h"
#include "query/QueryProcessor.h"
#include "network/NetworkManager.h"
#include "InputArray.h"
#include "system/Cluster.h"

using namespace std;
using namespace boost;

namespace scidb
{

// Logger for network subsystem. static to prevent visibility of variable outside of file
static log4cxx::LoggerPtr oplogger(log4cxx::Logger::getLogger("scidb.ops.impl_input"));

class PhysicalInput : public PhysicalOperator
{
public:
    PhysicalInput(std::string const& logicalName,
                  std::string const& physicalName,
                  Parameters const& parameters,
                  ArrayDesc const& schema):
        PhysicalOperator(logicalName, physicalName, parameters, schema)
    {
    }

    virtual bool changesDistribution(std::vector<ArrayDesc> const&) const
    {
        return true;
    }

    int64_t getSourceInstanceID() const
    {
        if (_parameters.size() >= 3)
        {
            assert(_parameters[2]->getParamType() == PARAM_PHYSICAL_EXPRESSION);
            boost::shared_ptr<OperatorParamPhysicalExpression> paramExpr = (boost::shared_ptr<OperatorParamPhysicalExpression>&)_parameters[2];
            assert(paramExpr->isConstant());
            return paramExpr->getExpression()->evaluate().getInt64();
        }
        return COORDINATOR_INSTANCE_MASK;
    }

    virtual ArrayDistribution getOutputDistribution(
            std::vector<ArrayDistribution> const&,
            std::vector<ArrayDesc> const&) const
    {
        InstanceID sourceInstanceID = getSourceInstanceID();
        if (sourceInstanceID == ALL_INSTANCES_MASK) {
            //The file is loaded from multiple instances - the distribution could be possibly violated - assume the worst
            return ArrayDistribution(psUndefined);
        }
        else {
            return ArrayDistribution(psLocalInstance);
        }
    }

    boost::shared_ptr<Array> execute(vector< boost::shared_ptr<Array> >& inputArrays,
                              boost::shared_ptr<Query> query)
    {
        assert(inputArrays.size() == 0);
        assert(_parameters.size() >= 2);

        assert(_parameters[1]->getParamType() == PARAM_PHYSICAL_EXPRESSION);
        boost::shared_ptr<OperatorParamPhysicalExpression> paramExpr = (boost::shared_ptr<OperatorParamPhysicalExpression>&)_parameters[1];
        assert(paramExpr->isConstant());
        const string fileName = paramExpr->getExpression()->evaluate().getString();

        InstanceID sourceInstanceID = getSourceInstanceID();
        
        if (sourceInstanceID != COORDINATOR_INSTANCE_MASK && sourceInstanceID != ALL_INSTANCES_MASK && (size_t)sourceInstanceID >= query->getInstancesCount())
            throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_INVALID_INSTANCE_ID) << sourceInstanceID;

        if (sourceInstanceID == COORDINATOR_INSTANCE_MASK) { 
            sourceInstanceID = query->getCoordinatorInstanceID();
        }

        int64_t maxErrors = 0;
        string shadowArray;
        InstanceID myInstanceID = query->getInstanceID();

        boost::shared_ptr<Array> result;
        string format;
        if (_parameters.size() >= 4)
        {
            assert(_parameters[3]->getParamType() == PARAM_PHYSICAL_EXPRESSION);
            paramExpr = (boost::shared_ptr<OperatorParamPhysicalExpression>&)_parameters[3];
            assert(paramExpr->isConstant());
            format = paramExpr->getExpression()->evaluate().getString();
            if (_parameters.size() >= 5)
            {
                assert(_parameters[4]->getParamType() == PARAM_PHYSICAL_EXPRESSION);
                paramExpr = (boost::shared_ptr<OperatorParamPhysicalExpression>&)_parameters[4];
                assert(paramExpr->isConstant());
                maxErrors = paramExpr->getExpression()->evaluate().getInt64();
                if (_parameters.size() >= 6)
                {
                    assert(_parameters[5]->getParamType() == PARAM_ARRAY_REF);
                    shadowArray = ((boost::shared_ptr<OperatorParamArrayReference>&)_parameters[5])->getObjectName();
                } 
            }
        } 
        try
        {
            bool isBinary = compareStringsIgnoreCase(format, "opaque") == 0 || format[0] == '(';
            result = boost::shared_ptr<Array>(new InputArray(_schema, fileName, format, query, 
                                                             sourceInstanceID != ALL_INSTANCES_MASK && sourceInstanceID != myInstanceID ? AS_EMPTY : isBinary ? AS_BINARY_FILE : AS_TEXT_FILE, maxErrors, shadowArray, sourceInstanceID == ALL_INSTANCES_MASK));
        }
        catch(const Exception& e)
        {
            if (e.getLongErrorCode() != SCIDB_LE_CANT_OPEN_FILE || sourceInstanceID == myInstanceID)
            {
                throw;
            }

            LOG4CXX_WARN(oplogger, "Failed to open file " << fileName << " for input");
            result = boost::shared_ptr<Array>(new MemArray(_schema,query));
        }

        return result;
    }
};

DECLARE_PHYSICAL_OPERATOR_FACTORY(PhysicalInput, "input", "impl_input")

} //namespace
