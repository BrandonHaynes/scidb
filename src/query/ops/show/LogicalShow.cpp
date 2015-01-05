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
 * @author Artyom Smirnov <smirnoffjr@gmail.com>
 *
 * @brief Shows object. E.g. schema of array.
 */

#include <algorithm>

#include "query/Operator.h"
#include "query/OperatorLibrary.h"

using namespace std;
using namespace boost;

namespace scidb
{

/**
 * @brief The operator: show().
 *
 * @par Synopsis:
 *   show( schemaArray | schema | queryString [, 'aql' | 'afl'] )
 *
 * @par Summary:
 *   Shows the schema of an array.
 *
 * @par Input:
 *   - schemaArray | schema | queryString: an array where the schema is used, the schema itself or arbitrary query string
 *   - 'aql' | 'afl': Language specifier for query string
 * @par Output array:
 *        <
 *   <br>   schema: string
 *   <br> >
 *   <br> [
 *   <br>   i: start=end=0, chunk interval=1
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
class LogicalShow: public LogicalOperator
{
public:
	LogicalShow(const string& logicalName, const string& alias):
        LogicalOperator(logicalName, alias)
    {
		ADD_PARAM_VARIES()
    	_usage = "show(<array name | anonymous schema | query string [, 'aql' | 'afl']>)";
    }

    std::vector<boost::shared_ptr<OperatorParamPlaceholder> > nextVaryParamPlaceholder(const std::vector< ArrayDesc> &schemas)
    {
        std::vector<boost::shared_ptr<OperatorParamPlaceholder> > res;
        if (_parameters.size() == 0)
        {
			res.push_back(PARAM_SCHEMA());
			res.push_back(PARAM_CONSTANT(TID_STRING));
        }
        else if (_parameters.size() == 1)
        {
			if (_parameters[0]->getParamType() == PARAM_LOGICAL_EXPRESSION)
			{
				res.push_back(PARAM_CONSTANT(TID_STRING));
			}
			res.push_back(END_OF_VARIES_PARAMS());
        }
        else if (_parameters.size() == 2)
        {
			res.push_back(END_OF_VARIES_PARAMS());
        }
        return res;
    }

    ArrayDesc inferSchema(vector<ArrayDesc> inputSchemas, shared_ptr<Query> query)
    {
        assert(inputSchemas.size() == 0);

        if (_parameters.size() == 2)
        {
			string lang = evaluate(
					((boost::shared_ptr<OperatorParamLogicalExpression>&) _parameters[1])->getExpression(),
					query, TID_STRING).getString();
			std::transform(lang.begin(), lang.end(), lang.begin(), ::tolower);
			if (lang != "aql" && lang != "afl")
			{
				throw USER_QUERY_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_WRONG_LANGUAGE_STRING,
						_parameters[1]->getParsingContext());
			}
		}

		Attributes atts(1);
		atts[0] = AttributeDesc((AttributeID)0, "schema",  TID_STRING, 0, 0 );

		Dimensions dims(1);
		dims[0] = DimensionDesc("i", 0, 0, 0, 0, 1, 0);

		return ArrayDesc("", atts, dims);
    }

};


DECLARE_LOGICAL_OPERATOR_FACTORY(LogicalShow, "show")


} //namespace
