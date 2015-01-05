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
 * @file LogicalTestCache.cpp
 * @brief Logical shim for the test_cache operator
 * @author poliocough@gmail.com
 */

#include <query/Operator.h>

using namespace std;
using namespace boost;

namespace scidb
{

/**
 *  @brief Operator test_cache()
 *
 *  @par Synopsis:
 *      test_cache()
 *
 *  @par Summary:
 *      Performs a few tests over the scidb::SharedMemCache class. If all tests pass, returns an empty array.
 *      Otherwise throws an exception. This is a non-user-end testing op only.
 *
 *  @par Input:
 *      none
 *
 *  @par Output array:
 *      empty single-celled array
 *
 *  @par Examples:
 *      test_cache()
 *
 *  @par Errors:
 *      SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) thrown in case of test failure
 */
class LogicalTestCache : public LogicalOperator
{
public:
    LogicalTestCache(const std::string& logicalName, const std::string& alias):
        LogicalOperator(logicalName, alias)
    {}

    ArrayDesc inferSchema(std::vector< ArrayDesc> schemas, boost::shared_ptr< Query> query)
    {
        Attributes outputAttrs;
        outputAttrs.push_back(AttributeDesc(0, "dummy", TID_DOUBLE, AttributeDesc::IS_NULLABLE, 0));
        Dimensions outputDims;
        outputDims.push_back(DimensionDesc("i",0,0,1,0));
        return ArrayDesc("test_cache", outputAttrs, outputDims);
    }
};

REGISTER_LOGICAL_OPERATOR_FACTORY(LogicalTestCache, "test_cache");

} //namespace scidb
