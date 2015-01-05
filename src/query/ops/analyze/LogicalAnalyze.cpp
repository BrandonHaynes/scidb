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
 * LogicalAnalyze.cpp
 *
 *  Created on: Feb 1, 2012
 *      Author: egor.pugin@gmail.com
 */
#include <query/Operator.h>
#include <system/Exceptions.h>
#include "PhysicalAnalyze.h"

using namespace std;
using namespace boost;

namespace scidb
{
/**
 * @brief The operator: analyze().
 *
 * @par Synopsis:
 *   analyze( srcArray {, attr}* )
 *
 * @par Summary:
 *   Returns an array describing the following characteristics of the specified attributes (or all the attributes, if no attribute is specified):
 *   - attribute_name
 *   - min
 *   - max
 *   - distinct_count: approximate count of distinct values.
 *   - non_null_count: the number of cells with non-null values.
 *
 * @par Input:
 *   - srcArray: a source array with srcAttrs and srcDims.
 *   - 0 or more attributes.
 *
 * @par Output array:
 *        <
 *   <br>   attribute_name: string
 *   <br>   min: string
 *   <br>   max: string
 *   <br>   distinct_count: uint64
 *   <br>   non_null_count: uint64
 *   <br> >
 *   <br> [
 *   <br>   attribute_number: type=int64, start=0, end=#displayed attributes less 1, chunk interval=1000
 *   <br> ]
 *
 * @par Examples:
 *   - Given array A <quantity: uint64, sales:double> [year, item] =
 *     <br> year, item, quantity, sales
 *     <br> 2011,  2,      7,     31.64
 *     <br> 2011,  3,      6,     19.98
 *     <br> 2012,  1,      5,     41.65
 *     <br> 2012,  2,      9,     40.68
 *     <br> 2012,  3,      8,     26.64
 *   - analyze(A) <attribute_name:string, min:string, max:string, distinct_count:uint64, non_null_count:uint64> [attribute_number]  =
 *     <br> attribute_number, attribute_name, min, max, distinct_count, non_null_count
 *     <br>      0,              "quantity"   "5"  "9"       5,            5
 *     <br>      1,               "sales"  "19.98" "41.65"   5,            5
 *
 * @par Errors:
 *   n/a
 *
 * @par Notes:
 *   - If multiple attributes are specified, the ordering of the attributes in the result array is determined by the ordering of the attributes in srcAttrs.
 *   - The value of attribute_number may be different from the number of an attribute in srcAttrs.
 *
 */
class LogicalAnalyze : public LogicalOperator
{
public:
    LogicalAnalyze(const std::string& logicalName, const std::string& alias)
    : LogicalOperator(logicalName, alias)
    {
        ADD_PARAM_INPUT();
        ADD_PARAM_VARIES();
    }

    vector<boost::shared_ptr<OperatorParamPlaceholder> > nextVaryParamPlaceholder(const vector<ArrayDesc> &schemas)
    {
        std::vector<boost::shared_ptr<OperatorParamPlaceholder> > res;
        res.push_back(END_OF_VARIES_PARAMS());
        res.push_back(PARAM_IN_ATTRIBUTE_NAME("void"));

        return res;
    }

    ArrayDesc inferSchema(std::vector<ArrayDesc> schemas, boost::shared_ptr<Query> query)
    {
        Attributes atts;
        atts.push_back(AttributeDesc(0, "attribute_name", TID_STRING, 0, 0));
        atts.push_back(AttributeDesc(1, "min", TID_STRING, AttributeDesc::IS_NULLABLE, 0));
        atts.push_back(AttributeDesc(2, "max", TID_STRING, AttributeDesc::IS_NULLABLE, 0));
        atts.push_back(AttributeDesc(3, "distinct_count", TID_UINT64, 0, 0));
        atts.push_back(AttributeDesc(4, "non_null_count", TID_UINT64, 0, 0));

        const AttributeDesc *emptyIndicator = schemas[0].getEmptyBitmapAttribute();
        Attributes inputAtts = schemas[0].getAttributes();
        assert(!emptyIndicator || inputAtts.size()-1 == emptyIndicator->getId());

        size_t attsCount(0);
        if (_parameters.empty()) {
            attsCount = emptyIndicator ? inputAtts.size()-1 : inputAtts.size();
        } else {
            attsCount = _parameters.size();
        }
        assert(attsCount <= inputAtts.size());

        Dimensions dims;
        dims.push_back(DimensionDesc("attribute_number", 0, attsCount, ANALYZE_CHUNK_SIZE, 0));

        ArrayDesc arrDesc(schemas[0].getName() + "_analyze", addEmptyTagAttribute(atts), dims);
        return arrDesc;
    }
};

DECLARE_LOGICAL_OPERATOR_FACTORY(LogicalAnalyze, "analyze")

} //namespace scidb
