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
 * @file LogicalInput.cpp
 *
 * @author roman.simakov@gmail.com
 *
 * Input operator for inputing data from external files into array
 */
#include "query/Operator.h"


namespace scidb
{

/**
 * @brief The operator: input().
 *
 * @par Synopsis:
 *   input( schemaArray | schema, filename, instance=-2, format="", maxErrors=0, shadowArray="", isStrict=false )
 *
 * @par Summary:
 *   Produces a result array and loads data from a given file, and optionally stores to shadowArray.
 *
 * @par Input:
 *   - schemaArray | schema: the array schema.
 *   - filename: where to load data from.
 *   - instance: which instance; default is -2. ??
 *   - format: ??
 *   - maxErrors: ??
 *   - shadowArray: if provided, the result array will be written to it.
 *   - isStrict if true, enables the data integrity checks such as for data collisions and out-of-order input chunks, defualt=false.
 *
 * @par Output array:
 *   n/a
 *
 * @par Examples:
 *   n/a
 *
 * @par Errors:
 *   n/a
 *
 * @par Notes:
 *   - [comment from author] Must be called as INPUT('existing_array_name', '/path/to/file/on/instance'). ?? schema not allowed??
 *   - This really needs to be modified by the author.
 */
class LogicalInput: public LogicalOperator
{
public:
    LogicalInput(const std::string& logicalName, const std::string& alias);
    std::vector<boost::shared_ptr<OperatorParamPlaceholder> > nextVaryParamPlaceholder(const std::vector< ArrayDesc> &schemas);
    ArrayDesc inferSchema(std::vector< ArrayDesc> inputSchemas, boost::shared_ptr< Query> query);
    void inferArrayAccess(boost::shared_ptr<Query>& query);
    static const char* OP_INPUT_NAME;
};

} //namespace
