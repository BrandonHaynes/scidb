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
 * @author Konstantin Knizhnik
 *
 * @brief Print message in log
 */

#include "query/Operator.h"
#include "query/OperatorLibrary.h"

using namespace std;
using namespace boost;

namespace scidb
{

/**
 * @brief The operator: echo().
 *
 * @par Synopsis:
 *   echo( str )
 *
 * @par Summary:
 *   Produces a single-element array containing the input string.
 *
 * @par Input:
 *   - str: an input string.
 *
 * @par Output array:
 *        <
 *   <br>   text: string
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
 *   - For internal usage.
 *
 */
class LogicalEcho: public LogicalOperator
{
public:
	LogicalEcho(const string& logicalName, const string& alias):
        LogicalOperator(logicalName, alias)
    {
    	ADD_PARAM_CONSTANT("string")
    	_usage = "echo('any text')";
    }

    ArrayDesc inferSchema(vector<ArrayDesc> inputSchemas, shared_ptr<Query> query)
    {
        assert(inputSchemas.size() == 0);
        assert(_parameters.size() == 1);

		Attributes atts(1);
		atts[0] = AttributeDesc((AttributeID)0, "text",  TID_STRING, 0, 0 );

		Dimensions dims(1);
		dims[0] = DimensionDesc("i", 0, 0, 0, 0, 1, 0);

		return ArrayDesc("", atts, dims);
    }

};


DECLARE_LOGICAL_OPERATOR_FACTORY(LogicalEcho, "echo")


} //namespace
