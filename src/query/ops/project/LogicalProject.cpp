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
 * @file LogicalProject.cpp
 *
 * @author knizhnik@garret.ru
 */

//#include <regex.h>
#include "query/Operator.h"
#include "system/Exceptions.h"
#include "query/TypeSystem.h"

namespace scidb
{

using namespace std;

/**
 * @brief The operator: project().
 *
 * @par Synopsis:
 *   project( srcArray {, selectedAttr}+ )
 *
 * @par Summary:
 *   Produces a result array that includes some attributes of the source array.
 *
 * @par Input:
 *   - srcArray: the source array with srcAttrs and srcDims.
 *   - a list of at least one selectedAttrs from the source array.
 *
 * @par Output array:
 *        <
 *   <br>   selectedAttrs: the selected attributes
 *   <br> >
 *   <br> [
 *   <br>   srcDims
 *   <br> ]
 *
 * @par Examples:
 *   n/a
 *
 * @par Errors:
 *   n/a
 *
 * @par Notes:
 *   n/a
 *
 */
class LogicalProject: public  LogicalOperator
{
public:
	LogicalProject(const std::string& logicalName, const std::string& alias):
	    LogicalOperator(logicalName, alias)
    {
        _properties.tile = true;
		ADD_PARAM_INPUT()
		ADD_PARAM_VARIES() //0 ...
    }

	std::vector<boost::shared_ptr<OperatorParamPlaceholder> > nextVaryParamPlaceholder(const std::vector< ArrayDesc> &schemas)
	{
		std::vector<boost::shared_ptr<OperatorParamPlaceholder> > res;
		res.push_back(PARAM_IN_ATTRIBUTE_NAME("void"));
		if (_parameters.size() > 0)
			res.push_back(END_OF_VARIES_PARAMS());
		return res;
	}

    ArrayDesc inferSchema(std::vector< ArrayDesc> schemas, boost::shared_ptr< Query> query)
    {
        assert(schemas.size() == 1);
        for (Parameters::const_iterator it = _parameters.begin(); it != _parameters.end(); ++it)
        	assert(((boost::shared_ptr<OperatorParamReference>&)*it)->getParamType() == PARAM_ATTRIBUTE_REF);


        Attributes newAttributes;
        const Attributes &oldAttributes = schemas[0].getAttributes();
        bool includesIndicator = false;
        size_t n = _parameters.size();
        for (size_t i = 0; i < n; i++)
        {
        	const AttributeDesc &attr =
        		oldAttributes[((boost::shared_ptr<OperatorParamReference>&)_parameters[i])->getObjectNo()];
        	newAttributes.push_back(AttributeDesc(i, attr.getName(), attr.getType(),
                                                  attr.getFlags(), attr.getDefaultCompressionMethod(),
                                                  attr.getAliases(), &attr.getDefaultValue(),
                                                  attr.getDefaultValueExpr()));
            includesIndicator |= attr.isEmptyIndicator();
        }
        if (!includesIndicator) { 
            AttributeDesc const* indicator = schemas[0].getEmptyBitmapAttribute();
            if (indicator != NULL) { 
                newAttributes.push_back(AttributeDesc(n, indicator->getName(), indicator->getType(),
                                                      indicator->getFlags(), indicator->getDefaultCompressionMethod(),
                                                      indicator->getAliases()));
            }
        }
        return ArrayDesc(schemas[0].getName(), newAttributes, schemas[0].getDimensions());
	}
};

DECLARE_LOGICAL_OPERATOR_FACTORY(LogicalProject, "project")

} //namespace
