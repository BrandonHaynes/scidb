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
 * @brief This operator shows parameters of other operator
 */

#include "query/Operator.h"
#include "query/OperatorLibrary.h"

using namespace std;

namespace scidb
{

/**
 * @brief The operator: help().
 *
 * @par Synopsis:
 *   help( operator )
 *
 * @par Summary:
 *   Produces a single-element array containing the help information for an operator.
 *
 * @par Input:
 *   - operator: the name of an operator.
 *
 * @par Output array:
 *        <
 *   <br>   help: string
 *   <br> >
 *   <br> [
 *   <br>   i: start=end=0, chunk interval=1.
 *   <br> ]
 *
 * @par Examples:
 *   n/a
 *
 * @par Errors:
 *   n/a
 *
 * @par Notes:
 *
 */
class LogicalHelp: public LogicalOperator
{
public:
        LogicalHelp(const std::string& logicalName, const std::string& alias):
        LogicalOperator(logicalName, alias)
    {
        ADD_PARAM_VARIES()
        _usage = "scan([<operator name>])";
    }

    std::vector<boost::shared_ptr<OperatorParamPlaceholder> > nextVaryParamPlaceholder(const std::vector< ArrayDesc> &schemas)
    {
        std::vector<boost::shared_ptr<OperatorParamPlaceholder> > res;
        if (_parameters.size() == 0)
            res.push_back(PARAM_CONSTANT("string"));
        res.push_back(END_OF_VARIES_PARAMS());
        return res;
    }

    ArrayDesc inferSchema(std::vector< ArrayDesc> inputSchemas, boost::shared_ptr< Query> query)
    {
        assert(inputSchemas.size() == 0);
        assert(_parameters.size() == 0 || _parameters.size() == 1);

        if (_parameters.size() == 1)
        {
            const string &opName =
                evaluate(
                    ((boost::shared_ptr<OperatorParamLogicalExpression>&) _parameters[0])->getExpression(),
                    query, TID_STRING).getString();

            try
            {
                OperatorLibrary::getInstance()->createLogicalOperator(opName);
            }
            catch (Exception &e)
            {
                throw CONV_TO_USER_QUERY_EXCEPTION(e, _parameters[0]->getParsingContext());
            }
        }

        Attributes atts(1);
        atts[0] = AttributeDesc((AttributeID)0, "help",  TID_STRING, 0, 0 );

        Dimensions dims(1);
        dims[0] = DimensionDesc("i", 0, 0, 0, 0, 1, 0);

        return ArrayDesc("Help",atts,dims);
    }

};


DECLARE_LOGICAL_OPERATOR_FACTORY(LogicalHelp, "help")


} //namespace
