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
 * LogicalSplitArrayTest.cpp
 *
 */

#include "query/Operator.h"

namespace scidb
{


///
/// @brief The operator: testsplitarray().
///
/// @par Synopsis:
///   testsplitarray( schemaArray )
///
/// @par Summary:
///   Produces an output array with easily-generated values using SplitArray.
///   This makes it easy to test whether SplitArray works across
///   all combinations of size & chunksize (it did not at one time).
///
/// @par Input:
///   - schemaArray: a 2D array of doubles which is used only to specify the schema of the output
///
/// @par Output array:
///   - a schemaArray-sized array of doubles, where each value is the row-major ordering of the cells, beginning with 0
///
/// @par Examples:
///    testsplitarray(<double val> [col=0:<ncol>:0:0, row=0:<nrow>:0:0])
///
/// @par Errors:
///   SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION)
///
/// @par Notes:
///   This is a test operator, designed for testing an operator-under-test.  It is not a goal
///   to subject the test operator to the same level of testing as production operators.  This
///   would be a mis-application of energy.
///   However, it is appropriate to improve this operator in order to more completely test
///   the operator-under-test.
///
class LogicalSplitArrayTest : public  LogicalOperator
{
public:
    LogicalSplitArrayTest(const std::string& logicalName, const std::string& alias):
        LogicalOperator(logicalName, alias)
    {
        ADD_PARAM_INPUT();
    }

    ArrayDesc inferSchema(std::vector< ArrayDesc> schemas, boost::shared_ptr< Query> query)
    {
        if(schemas.size() > 1) {
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "LogicalSplitArrayTest: no input schema.";
        }
        ArrayDesc& schema = schemas[0]; // hereafter reduced to schemas[0] only

        if(schema.getAttributes(true).size() != 1)
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION)
                      << "LogicalSplitArrayTest: no attribute in input schema (must have a first attribute of type double).";

        if(schema.getAttributes()[0].getType() != TID_DOUBLE)
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION)
                      << "LogicalSplitArrayTest: first attribute must be of type double";

        if(schema.getDimensions().size() != 2)
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION)
                      << "LogicalSplitArrayTest: input schema must have 2 dimensions";

        DimensionDesc d0 = schema.getDimensions()[0]; // hereafter reduced to two dims only
        DimensionDesc d1 = schema.getDimensions()[1];
        if (d0.getLength() == INFINITE_LENGTH ||
            d1.getLength() == INFINITE_LENGTH) {
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION)
                      << "LogicalSplitArrayTest: input schema must be bounded in both dimensions";
        }

        Attributes outAtts;
        AttributeDesc resultAttr(AttributeID(0), "v",  TID_DOUBLE, 0, 0);
        outAtts.push_back(resultAttr);

        Dimensions outDims;
        outDims.push_back(DimensionDesc(d0.getBaseName(), d0.getNamesAndAliases(),
                                        d0.getStartMin(), d0.getCurrStart(), d0.getCurrEnd(), d0.getEndMax(),
                                        d0.getChunkInterval(), 0));
        outDims.push_back(DimensionDesc(d1.getBaseName(), d1.getNamesAndAliases(),
                                        d1.getStartMin(), d1.getCurrStart(), d1.getCurrEnd(), d1.getEndMax(),
                                        d1.getChunkInterval(), 0));

        return ArrayDesc("Splitarraytest", outAtts, outDims);
    }
};

DECLARE_LOGICAL_OPERATOR_FACTORY(LogicalSplitArrayTest, "splitarraytest")

} //namespace scidb
