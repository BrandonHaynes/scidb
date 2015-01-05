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
 * PhysicalProject.cpp
 *
 *  Created on: Apr 20, 2010
 *      Author: Knizhnik
 */

#include "query/Operator.h"
#include "array/Metadata.h"
#include "array/DelegateArray.h"


namespace scidb {

using namespace boost;
using namespace std;

class ProjectArray : public DelegateArray
{
private:
    vector<AttributeID> projection;

public:
    virtual DelegateArrayIterator* createArrayIterator(AttributeID id) const
    {
        return new DelegateArrayIterator(*this, id, inputArray->getConstIterator(projection[id]));
    }

    ProjectArray(ArrayDesc const& desc, boost::shared_ptr<Array> const& array, const LogicalOperator::Parameters& parameters)
    : DelegateArray(desc, array, true),
      projection(desc.getAttributes().size())
    {
        size_t n = parameters.size();
        for (size_t i = 0; i < n; i++)
        {
        	projection[i] = ((shared_ptr<OperatorParamReference>&)parameters[i])->getObjectNo();
        }
        if (projection.size() > n) { 
            projection[n] = array->getArrayDesc().getEmptyBitmapAttribute()->getId();
        }
    } 
};

class PhysicalProject: public  PhysicalOperator
{
public:
	PhysicalProject(const string& logicalName, const string& physicalName, const Parameters& parameters, const ArrayDesc& schema)
        :  PhysicalOperator(logicalName, physicalName, parameters, schema)
	{
	}

    virtual PhysicalBoundaries getOutputBoundaries(const std::vector<PhysicalBoundaries> & inputBoundaries,
                                                   const std::vector< ArrayDesc> & inputSchemas) const
    {
        return inputBoundaries[0];
    }

	/***
	 * Project is a pipelined operator, hence it executes by returning an iterator-based array to the consumer
	 * that overrides the chunkiterator method.
	 */
	boost::shared_ptr<Array> execute(vector< boost::shared_ptr<Array> >& inputArrays, boost::shared_ptr<Query> query)
    {
		assert(inputArrays.size() == 1);
		return boost::shared_ptr<Array>(new ProjectArray(_schema, inputArrays[0], _parameters));
	 }
};
    
DECLARE_PHYSICAL_OPERATOR_FACTORY(PhysicalProject, "project", "physicalProject")

}  // namespace scidb
