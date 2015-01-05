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
 * @file LogicalAttributeRename.cpp
 *
 * @brief Operator for renaming attributes. Takes input and pairs of attributes (old name + new name)
 * argument. Attributes of input will be replaced with new names in output schema. 
 *
 * @author Artyom Smirnov <smirnoffjr@gmail.com>
 */

#include "query/Operator.h"

namespace scidb {

using namespace std;

/**
 * @brief The operator: attribute_rename().
 *
 * @par Synopsis:
 *   attribute_rename( srcArray {, srcAttr, newAttr}+ )
 *
 * @par Summary:
 *   Produces a result array the same as srcArray, but with at least one attribute renamed.
 *
 * @par Input:
 *   - srcArray: a source array with srcAttrs and srcDims.
 *   - 1 or more pairs of a source attribute and the new attribute to rename to.
 *
 * @par Output array:
 *        <
 *   <br>   srcAttrs, with the specified attributes renamed.
 *   <br> >
 *   <br> [
 *   <br>   srcDims
 *   <br> ]
 *
 * @par Examples:
 *   - Given array A <quantity: uint64, sales:double> [year, item] =
 *     <br> year, item, quantity, sales
 *     <br> 2011,  2,      7,     31.64
 *     <br> 2011,  3,      6,     19.98
 *     <br> 2012,  1,      5,     41.65
 *     <br> 2012,  2,      9,     40.68
 *     <br> 2012,  3,      8,     26.64
 *   - attribute_rename(A, sales, totalsales) <quantity: uint64, totalsales:double> [year, item] =
 *     <br> year, item, quantity, totalsales
 *     <br> 2011,  2,      7,     31.64
 *     <br> 2011,  3,      6,     19.98
 *     <br> 2012,  1,      5,     41.65
 *     <br> 2012,  2,      9,     40.68
 *     <br> 2012,  3,      8,     26.64
 *
 * @par Errors:
 *   n/a
 *
 * @par Notes:
 *   n/a
 *
 */
class LogicalAttributeRename: public  LogicalOperator
{
public:
	LogicalAttributeRename(const string& logicalName, const std::string& alias):
	    LogicalOperator(logicalName, alias)
	{
		ADD_PARAM_INPUT()
		ADD_PARAM_VARIES()
	}

	std::vector<boost::shared_ptr<OperatorParamPlaceholder> > nextVaryParamPlaceholder(const std::vector< ArrayDesc> &schemas)
	{
		std::vector<boost::shared_ptr<OperatorParamPlaceholder> > res;

		if (_parameters.size() % 2 == 0 && _parameters.size() >= 2)
		{
			res.push_back(END_OF_VARIES_PARAMS());
		}

		if (_parameters.size() % 2 == 0)
			res.push_back(PARAM_IN_ATTRIBUTE_NAME("void"));
		else			
			res.push_back(PARAM_OUT_ATTRIBUTE_NAME("void"));
		
		return res;
	}
	
    ArrayDesc inferSchema(std::vector< ArrayDesc> schemas, boost::shared_ptr< Query> query)
	{
        assert(schemas.size() == 1);

        const ArrayDesc& desc = schemas[0];
        const Attributes &oldAttributes = desc.getAttributes();
        Attributes newAttributes = desc.getAttributes();

		for (size_t paramNo = 0, paramCount = _parameters.size(); paramNo < paramCount; paramNo+=2)
		{
			int32_t attNo = ((boost::shared_ptr<OperatorParamReference>&)_parameters[paramNo])->getObjectNo();
			AttributeDesc attr = oldAttributes[attNo];
			newAttributes[attNo] = AttributeDesc(attNo,
                                                 ((boost::shared_ptr<OperatorParamReference>&)_parameters[paramNo + 1])->getObjectName(),
                                                 attr.getType(), attr.getFlags(), attr.getDefaultCompressionMethod(), 
                                                 attr.getAliases(), &attr.getDefaultValue(), attr.getDefaultValueExpr());
		}
		
        return ArrayDesc(desc.getId(), desc.getUAId(), desc.getVersionId(), desc.getName(), newAttributes, desc.getDimensions());
	}
};

DECLARE_LOGICAL_OPERATOR_FACTORY(LogicalAttributeRename, "attribute_rename")


}  // namespace scidb
