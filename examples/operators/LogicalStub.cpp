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
 * @file LogicalStub.cpp
 * @author roman.simakov@gmail.com
 *
 * @brief Stub for writing plugins with logical operators
 */

#include "query/Operator.h"

namespace scidb
{

class LogicalStub : public LogicalOperator
{
public:
    LogicalStub(const std::string& logicalName, const std::string& alias):
	    LogicalOperator(logicalName, alias)
	{
        /**
         * See built-in operators implementation for example
         */
    }

    ArrayDesc inferSchema(std::vector<ArrayDesc> schemas, boost::shared_ptr<Query> query)
    {
        /**
         * See built-in operators implementation for example
         */
        return ArrayDesc();
	}

};

REGISTER_LOGICAL_OPERATOR_FACTORY(LogicalStub, "stub");

} //namespace
