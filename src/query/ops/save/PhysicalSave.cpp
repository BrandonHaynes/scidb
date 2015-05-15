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
 * @file PhysicalSave.cpp
 *
 * @author roman.simakov@gmail.com
 *
 * Physical implementation of SAVE operator for saveing data from text file
 * which is located on coordinator
 */

#include <string.h>

#include "query/Operator.h"
#include "array/Array.h"
#include "smgr/io/ArrayWriter.h"
#include "array/DBArray.h"
#include "query/QueryProcessor.h"
#include "system/Config.h"
#include "system/SciDBConfigOptions.h"

using namespace std;
using namespace boost;
using namespace scidb;

// Useful local shorthand.
#define Parm(_n) \
    ((shared_ptr<OperatorParamPhysicalExpression>&)_parameters[(_n)])
#define ParmExpr(_n)    (Parm(_n)->getExpression())

namespace scidb
{

class PhysicalSave: public PhysicalOperator
{
public:
    PhysicalSave(const std::string& logicalName,
                 const std::string& physicalName,
                 const Parameters& parameters,
                 const ArrayDesc& schema)
        : PhysicalOperator(logicalName, physicalName, parameters, schema)
    {
    }

    virtual PhysicalBoundaries getOutputBoundaries(
        const std::vector<PhysicalBoundaries> & inputBoundaries,
        const std::vector< ArrayDesc> & inputSchemas) const
    {
        return inputBoundaries[0];
    }

    int64_t getSourceInstanceID() const
    {
        if (_parameters.size() >= 2)
        {
            assert(_parameters[1]->getParamType() == PARAM_PHYSICAL_EXPRESSION);
            boost::shared_ptr<OperatorParamPhysicalExpression> parm1 = Parm(1);
            assert(parm1->isConstant());
            return parm1->getExpression()->evaluate().getInt64();
        }
        // return ALL_INSTANCES_MASK; -- old behaviour
        return COORDINATOR_INSTANCE_MASK; // new behaviour compatible with LOAD/INPUT
    }

    virtual DistributionRequirement getDistributionRequirement (const std::vector< ArrayDesc> & inputSchemas) const
    {
        InstanceID sourceInstanceID = getSourceInstanceID();
        if (sourceInstanceID == ALL_INSTANCE_MASK)
        {
            return DistributionRequirement(DistributionRequirement::Any);
        }
        else
        {
            vector<ArrayDistribution> requiredDistribution(1);
            requiredDistribution[0] = ArrayDistribution(
                psLocalInstance,
                boost::shared_ptr<DistributionMapper>(),
                sourceInstanceID);
            return DistributionRequirement(DistributionRequirement::SpecificAnyOrder, requiredDistribution);
        }
    }


    boost::shared_ptr<Array> execute(vector< boost::shared_ptr<Array> >& inputArrays,
                                     boost::shared_ptr<Query> query)
    {
        assert(inputArrays.size() == 1);
        assert(_parameters.size() >= 1);

        assert(_parameters[0]->getParamType() == PARAM_PHYSICAL_EXPRESSION);
        const string fileName = ParmExpr(0)->evaluate().getString();
        string format = "store";
        if (_parameters.size() >= 3) {
            format = ParmExpr(2)->evaluate().getString();
        }
        InstanceID sourceInstanceID = getSourceInstanceID();
        if (sourceInstanceID == COORDINATOR_INSTANCE_MASK) {
            sourceInstanceID = (query->isCoordinator() ? query->getInstanceID() : query->getCoordinatorID());
        }
        InstanceID myInstanceID = query->getInstanceID();
        bool parallel = (sourceInstanceID == ALL_INSTANCE_MASK);
        if (parallel || sourceInstanceID == myInstanceID) {
            ArrayWriter::setPrecision(Config::getInstance()->getOption<int>(CONFIG_PRECISION));
            ArrayWriter::save(*inputArrays[0], fileName, query, format,
                              (parallel ? ArrayWriter::F_PARALLEL : 0));
        }

        return inputArrays[0];
    }
};

DECLARE_PHYSICAL_OPERATOR_FACTORY(PhysicalSave, "save", "impl_save")

} //namespace
