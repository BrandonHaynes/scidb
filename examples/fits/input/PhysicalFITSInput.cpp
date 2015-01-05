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
 * @author miguel@spacebase.org
 *
 * @see LogicalFITSInput.cpp
 */
#include "network/NetworkManager.h"
#include "query/Operator.h"
#include "query/QueryProcessor.h"
#include "array/DelegateArray.h"
#include "system/Cluster.h"

#include "FITSInputArray.h"


namespace scidb
{
using namespace std;


class PhysicalFITSInput: public PhysicalOperator
{
public:
    PhysicalFITSInput(const std::string& logicalName, const std::string& physicalName, const Parameters& parameters, const ArrayDesc& schema):
        PhysicalOperator(logicalName, physicalName, parameters, schema)
    {
    }

    virtual bool changesDistribution(std::vector<ArrayDesc> const&) const
    {
        return true;
    }

    uint32_t getHDU() const
    {
        if (_parameters.size() >= 3) {  // Arguments include HDU number
            boost::shared_ptr<OperatorParamPhysicalExpression> paramExpr = (boost::shared_ptr<OperatorParamPhysicalExpression>&)_parameters[2];
            return paramExpr->getExpression()->evaluate().getUint32();
        }
        return 0;                       // Otherwise, assume primary HDU
    }

    InstanceID getFileInstanceID(boost::shared_ptr<Query>& query) const
    {
        if (_parameters.size() == 4) {      // Arguments include instance ID
            boost::shared_ptr<OperatorParamPhysicalExpression> paramExpr = (boost::shared_ptr<OperatorParamPhysicalExpression>&)_parameters[3];
            return paramExpr->getExpression()->evaluate().getUint64();
        }
        return query->getInstanceID();          // Otherwise, use current instance ID
    }

    virtual ArrayDistribution getOutputDistribution(
            std::vector<ArrayDistribution> const&,
            std::vector<ArrayDesc> const&) const
    {
        return ArrayDistribution(psLocalInstance);
    }

    boost::shared_ptr<Array> execute(vector< boost::shared_ptr<Array> >& inputArrays,
                                     boost::shared_ptr<Query> query)
    {
        const string filePath = ((boost::shared_ptr<OperatorParamPhysicalExpression>&)_parameters[1])->getExpression()->evaluate().getString();
        uint32_t hdu = getHDU();
        InstanceID fileInstanceID = getFileInstanceID(query);
        InstanceID myInstanceID = query->getInstanceID();

        boost::shared_ptr<Array> result;
        if (fileInstanceID == myInstanceID) {   // This is the instance containing the file
            result = boost::shared_ptr<Array>(new FITSInputArray(_schema, filePath, hdu, query));
            if (_schema.getEmptyBitmapAttribute() != NULL) {
                result = boost::shared_ptr<Array>(new NonEmptyableArray(result));
            }
        } else {                        // Otherwise, return empty array
            result = boost::shared_ptr<Array>(new MemArray(_schema,query));
        }

        return result;
    }
};

REGISTER_PHYSICAL_OPERATOR_FACTORY(PhysicalFITSInput, "fits_input", "impl_fits_input");

}
