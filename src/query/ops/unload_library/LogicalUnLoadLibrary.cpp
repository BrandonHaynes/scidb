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
 * @file LogicalUnLoadLibrary.cpp
 *
 * @brief Logical DDL operator which unloads user defined library
 *
 * @author roman.simakov@gmail.com
 */

#include "query/Operator.h"
#include "system/Exceptions.h"


namespace scidb
{

/**
 * @brief The operator: unload_library().
 *
 * @par Synopsis:
 *   unload_library( library )
 *
 * @par Summary:
 *   Unloads a SciDB plugin.
 *
 * @par Input:
 *   - library: the name of the library to unload.
 *
 * @par Output array:
 *   - NULL
 *
 * @par Examples:
 *   n/a
 *
 * @par Errors:
 *   n/a
 *
 * @par Notes:
 *   - This operator is the reverse of load_library().
 *
 */
class LogicalUnLoadLibrary: public LogicalOperator
{
public:
    LogicalUnLoadLibrary(const std::string& logicalName, const std::string& alias):
        LogicalOperator(logicalName, alias)
    {
        ADD_PARAM_CONSTANT("string")
    }

    ArrayDesc inferSchema(std::vector< ArrayDesc> inputSchemas, boost::shared_ptr< Query> query)
    {
        assert(inputSchemas.size() == 0);
        //FIXME: Need parameters to infer the schema correctly
        ArrayDesc schema;
        return schema;
    }
};

DECLARE_LOGICAL_OPERATOR_FACTORY(LogicalUnLoadLibrary, "unload_library")

} //namespace
