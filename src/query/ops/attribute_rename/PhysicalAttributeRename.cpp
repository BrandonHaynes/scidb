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
 * @file PhysicalAttributeRename.cpp
 *
 * @brief Operator for renaming attributes. Takes input and pairs of attributes (old name + new name)
 * argument. Attributes of input will be replaced with new names in output schema. 
 *
 * @author Artyom Smirnov <smirnoffjr@gmail.com>
 */

#include <boost/make_shared.hpp>

#include "query/Operator.h"

#include "array/DelegateArray.h"

namespace scidb {

using namespace std;
using namespace boost;

class PhysicalAttributeRename: public PhysicalOperator
{
public:
	PhysicalAttributeRename(const string& logicalName, const string& physicalName, const Parameters& parameters, const ArrayDesc& schema):
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
		return boost::make_shared<DelegateArray>(_schema, inputArrays[0]);
	}
};

DECLARE_PHYSICAL_OPERATOR_FACTORY(PhysicalAttributeRename, "attribute_rename", "physical_attribute_rename")

}  // namespace scidb
