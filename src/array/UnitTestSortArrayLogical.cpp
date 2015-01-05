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
 * @file UnitTestSortArrayLogical.cpp
 *
 * @brief The logical operator interface for testing deep-chunk merge.
 */

#include <query/Query.h>
#include <array/Array.h>
#include <query/Operator.h>

namespace scidb
{
using namespace std;

/**
 * @brief The operator: test_sort_array().
 *
 * @par Synopsis:
 *   test_sort_array()
 *
 * @par Summary:
 *   This operator performs unit tests for sort array. It returns an empty string. Upon failures exceptions are thrown.
 *
 * @par Input:
 *   n/a
 *
 * @par Output array:
 *        <
 *   <br>   dummy_attribute: string
 *   <br> >
 *   <br> [
 *   <br>   dummy_dimension: start=end=chunk_interval=0.
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
class UnitTestSortArrayLogical: public LogicalOperator
{
public:
    UnitTestSortArrayLogical(const string& logicalName, const std::string& alias):
    LogicalOperator(logicalName, alias)
    {
    }

    ArrayDesc inferSchema(std::vector<ArrayDesc> schemas, boost::shared_ptr< Query> query)
    {
        vector<AttributeDesc> attributes(1);
        attributes[0] = AttributeDesc((AttributeID)0, "dummy_attribute",  TID_STRING, 0, 0);
        vector<DimensionDesc> dimensions(1);
        dimensions[0] = DimensionDesc(string("dummy_dimension"), Coordinate(0), Coordinate(0), uint32_t(0), uint32_t(0));
        return ArrayDesc("dummy_array", attributes, dimensions);
    }

};

REGISTER_LOGICAL_OPERATOR_FACTORY(UnitTestSortArrayLogical, "test_sort_array");
}  // namespace scidb
