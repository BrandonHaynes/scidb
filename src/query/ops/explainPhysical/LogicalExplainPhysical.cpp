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
 * @file LogicalExplainPhysical.cpp
 *
 * @author poliocough@gmail.com
 *
 * explain_physical operator / Logical implementation.
 */

#include "log4cxx/logger.h"
#include <boost/make_shared.hpp>


#include "query/Operator.h"
#include "query/OperatorLibrary.h"
#include "query/ParsingContext.h"
#include "system/Exceptions.h"
#include "system/SystemCatalog.h"
#include "array/Metadata.h"

namespace scidb
{

using namespace std;
using namespace boost;

/**
 * @brief The operator: explain_physical().
 *
 * @par Synopsis:
 *   explain_physical( query , language = 'aql' )
 *
 * @par Summary:
 *   Produces a single-element array containing the physical query plan.
 *
 * @par Input:
 *   - query: a query string.
 *   - language: the language string; either 'aql' or 'afl'; default is 'aql'
 *
 * @par Output array:
 *        <
 *   <br>   physical_plan: string
 *   <br> >
 *   <br> [
 *   <br>   No: start=end=0, chunk interval=1.
 *   <br> ]
 *
 * @par Examples:
 *   n/a
 *
 * @par Errors:
 *   n/a
 *
 * @par Notes:
 *   - For internal usage.
 *
 */
class LogicalExplainPhysical: public LogicalOperator
{
public:
    LogicalExplainPhysical(const std::string& logicalName, const std::string& alias):
    LogicalOperator(logicalName, alias)
    {
        ADD_PARAM_CONSTANT("string");
        ADD_PARAM_VARIES();
        _usage = "explain_physical(<querystring> [,language]) language := 'afl'|'aql'";
    }

    std::vector<boost::shared_ptr<OperatorParamPlaceholder> > nextVaryParamPlaceholder(const std::vector< ArrayDesc> &schemas)
    {
        std::vector<boost::shared_ptr<OperatorParamPlaceholder> > res;
        if (_parameters.size() == 1)
        {
            res.push_back(PARAM_CONSTANT("string"));
        }
        res.push_back(END_OF_VARIES_PARAMS());
        return res;
    }

    ArrayDesc inferSchema(std::vector< ArrayDesc> inputSchemas, boost::shared_ptr< Query> query)
    {
        assert(inputSchemas.size() == 0);

        vector<AttributeDesc> attributes(1);
        attributes[0] = AttributeDesc((AttributeID)0, "physical_plan",  TID_STRING, 0, 0);
        vector<DimensionDesc> dimensions(1);

        if ( _parameters.size() != 1 && _parameters.size() != 2 )
        {
                boost::shared_ptr< ParsingContext> pc;
                if (_parameters.size()==0) //need a parsing context for exception!
                {       pc = boost::make_shared<ParsingContext>(); }
                else
                {       pc = _parameters[0]->getParsingContext(); }

                throw USER_QUERY_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_EXPLAIN_ERROR1,
                          pc);
        }

        string queryString =  evaluate(
                        ((boost::shared_ptr<OperatorParamLogicalExpression>&)_parameters[0])->getExpression(),
                        query,
                        TID_STRING).getString();
        // TODO: queryString is not used!

        if (_parameters.size() == 2)
        {
                string languageSpec =  evaluate(
                                ((boost::shared_ptr<OperatorParamLogicalExpression>&)_parameters[1])->getExpression(),
                                query,
                                TID_STRING).getString();

                        if (languageSpec != "aql" && languageSpec != "afl")
                        {
                                throw USER_QUERY_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_EXPLAIN_ERROR2,
                                                _parameters[1]->getParsingContext());
                        }
        }

        dimensions[0] = DimensionDesc("No", 0, 0, 0, 0, 1, 0);

        return ArrayDesc("physical_plan", attributes, dimensions);
    }

};

DECLARE_LOGICAL_OPERATOR_FACTORY(LogicalExplainPhysical, "explain_physical")

} //namespace
