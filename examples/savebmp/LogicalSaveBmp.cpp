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
 * LogicalSaveBmp.cpp
 *
 *  Created on: 8/15/12
 *      Author: poliocough@gmail.com
 */

#include <query/Operator.h>
#include <system/SystemCatalog.h>
#include <system/Exceptions.h>
#include <array/Metadata.h>
#include <boost/filesystem.hpp>

using namespace std;

namespace scidb
{

/**
 * Logical savebmp operator.
 */
class LogicalSaveBmp: public LogicalOperator
{
  public:
    /**
     * Sets parameters to one input array and one string constant.
     */
    LogicalSaveBmp(const string& logicalName, const std::string& alias):
        LogicalOperator(logicalName, alias)
    {
        ADD_PARAM_INPUT()
        ADD_PARAM_CONSTANT("string");
    }

    /**
     * Checks to make sure that the input schema and infers a sample single-cell result schema.
     */
    ArrayDesc inferSchema(std::vector< ArrayDesc> schemas, boost::shared_ptr< Query> query)
    {
        assert(schemas.size() == 1);

        ArrayDesc const& input = schemas[0];
        if(input.getDimensions().size() != 2)
        {
            throw USER_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_ILLEGAL_OPERATION) << "Input to savebmp must be two-dimensional";
        }

        if(input.getDimensions()[0].getLength() == INFINITE_LENGTH ||
           input.getDimensions()[1].getLength() == INFINITE_LENGTH)
        {
            throw USER_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_ILLEGAL_OPERATION) << "Input to savebmp must not be unbounded";
        }

        if(input.getAttributes().size() < 3)
        {
            throw USER_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_ILLEGAL_OPERATION) << "Input to savebmp must have at least 3 attributes";
        }

        for(size_t i =0; i<3; i++)
        {
            AttributeDesc const& attr = input.getAttributes()[i];
            if (attr.getType() != TID_UINT8)
            {
                throw USER_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_ILLEGAL_OPERATION) << "The first 3 attributes of the input to savebmp must be of type uint8";
            }
        }

        Attributes outputAttrs;
        outputAttrs.push_back(AttributeDesc(0, "status", TID_STRING, 0, 0));
        outputAttrs.push_back(AttributeDesc(1, "file_size", TID_DOUBLE, 0, 0));

        Dimensions outputDims;
        outputDims.push_back(DimensionDesc("i", 0, 0, 0, 0, 1, 0));

        return ArrayDesc("savbmp_output", outputAttrs, outputDims);
    }
};

REGISTER_LOGICAL_OPERATOR_FACTORY(LogicalSaveBmp, "savebmp");


} //namespace
