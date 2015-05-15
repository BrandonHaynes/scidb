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
 * PhysicalAverageRank.cpp
 *  Created on: May 11, 2011
 *      Author: poliocough@gmail.com
 */

#include <query/Operator.h>
#include <array/Metadata.h>
#include <boost/foreach.hpp>
#include <array/DelegateArray.h>
#include <array/MergeSortArray.h>
#include "RankCommon.h"
#include <sys/time.h>

using namespace std;

namespace scidb
{

class PhysicalAverageRank: public PhysicalOperator
{
  public:
    PhysicalAverageRank(const std::string& logicalName, const std::string& physicalName, const Parameters& parameters, const ArrayDesc& schema):
        PhysicalOperator(logicalName, physicalName, parameters, schema)
    {
    }

    virtual PhysicalBoundaries getOutputBoundaries(const std::vector<PhysicalBoundaries> & inputBoundaries,
                                                   const std::vector< ArrayDesc> & inputSchemas) const
    {
        return inputBoundaries[0];
    }

    //We require that input is distributed round-robin so that our parallel trick works
    virtual DistributionRequirement getDistributionRequirement(const std::vector<ArrayDesc> & inputSchemas) const
    {
        vector<ArrayDistribution> requiredDistribution;
        requiredDistribution.push_back(ArrayDistribution(psHashPartitioned));
        return DistributionRequirement(DistributionRequirement::SpecificAnyOrder, requiredDistribution);
    }

    virtual bool changesDistribution(std::vector<ArrayDesc> const&) const
    {
        return true;
    }

    virtual ArrayDistribution getOutputDistribution(const std::vector<ArrayDistribution> & inputDistributions,
                                                    const std::vector< ArrayDesc> & inputSchemas) const
    {
        boost::shared_ptr<Query> query(Query::getValidQueryPtr(_query));
        const size_t nInstances = query->getInstancesCount();
        const size_t nDims = _schema.getDimensions().size();

        DimensionVector offset(nDims);

        offset[nDims-1] += (nInstances-1)*_schema.getDimensions()[nDims-1].getChunkInterval();
        return ArrayDistribution(psHashPartitioned, DistributionMapper::createOffsetMapper(offset));
    }

    shared_ptr<Array> execute(std::vector< boost::shared_ptr<Array> >& inputArrays, boost::shared_ptr<Query> query)
    {
        shared_ptr<Array> inputArray = inputArrays[0];
        if (inputArray->getSupportedAccess() == Array::SINGLE_PASS)
        {   //if input supports MULTI_PASS, don't bother converting it
            inputArray = ensureRandomAccess(inputArray, query);
        }

        const ArrayDesc& input = inputArray->getArrayDesc();
        string attName = _parameters.size() > 0 ? ((boost::shared_ptr<OperatorParamReference>&)_parameters[0])->getObjectName() :
                                                input.getAttributes()[0].getName();

        AttributeID rankedAttributeID = 0;
        for (size_t i =0 ; i< input.getAttributes().size(); i++)
        {
            if (input.getAttributes()[i].getName() == attName)
            {
                rankedAttributeID = input.getAttributes()[i].getId();
                break;
            }
        }

        Dimensions const& dims = inputArray->getArrayDesc().getDimensions();
        Dimensions groupBy;
        if (_parameters.size() > 1)
        {
            size_t i, j;
            for (i = 0; i < _parameters.size()-1; i++) {
               const string& dimName = ((boost::shared_ptr<OperatorParamReference>&)_parameters[i + 1])->getObjectName();
               const string& dimAlias = ((boost::shared_ptr<OperatorParamReference>&)_parameters[i + 1])->getArrayName();
               for (j = 0; j < dims.size(); j++) {
                   if (dims[j].hasNameAndAlias(dimName, dimAlias)) {
                       groupBy.push_back(dims[j]);
                       break;
                   }
               }
               assert(j < dims.size());
            }
        }

        return buildDualRankArray(inputArray, rankedAttributeID, groupBy, query);
    }
};

DECLARE_PHYSICAL_OPERATOR_FACTORY(PhysicalAverageRank, "avg_rank", "physicalAverageRank")

}
