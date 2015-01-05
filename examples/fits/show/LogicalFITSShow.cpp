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
 * @author Miguel Branco <miguel@spacebase.org>
 *
 * @brief Shows schema of FITS file in "table" form with three "columns":
 *        <Does HDU Contain Image?>, <Image Data Type>, <Image Dimensions>
 */

#include "query/Operator.h"
#include "query/OperatorLibrary.h"

#include "../common/FITSParser.h"


namespace scidb
{
using namespace std;


class LogicalFITSShow: public LogicalOperator
{
public:
    LogicalFITSShow(const string& logicalName, const string& alias):
        LogicalOperator(logicalName, alias)
    {
        ADD_PARAM_CONSTANT("string");
    }

    ArrayDesc inferSchema(vector<ArrayDesc> inputSchemas, boost::shared_ptr<Query> query)
    {
        const string &filePath = evaluate(((boost::shared_ptr<OperatorParamLogicalExpression>&)_parameters[0])->getExpression(), query, TID_STRING).getString();

        FITSParser parser(filePath);

        Attributes attrs(3);
        attrs[0] = AttributeDesc((AttributeID) 0, "image",  TID_BOOL, 0, 0);
        attrs[1] = AttributeDesc((AttributeID) 1, "type",  TID_STRING, AttributeDesc::IS_NULLABLE, 0);
        attrs[2] = AttributeDesc((AttributeID) 2, "dimensions",  TID_STRING, AttributeDesc::IS_NULLABLE, 0);

        int size = parser.getNumberOfHDUs();
        int end  = size>0 ? size-1 : 0;

        Dimensions dims(1,DimensionDesc("N", 0, 0, end, end, size, 0));

        return ArrayDesc("", attrs, dims);
    }

};

REGISTER_LOGICAL_OPERATOR_FACTORY(LogicalFITSShow, "fits_show");

}
