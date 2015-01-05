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
 * @file LogicalAttributes.cpp
 *
 * @author knizhnik@garret.ru
 *
 * Get list of persistent array attributes
 */

#include "query/Operator.h"
#include "system/Exceptions.h"
#include "array/Metadata.h"
#include "system/SystemCatalog.h"

namespace scidb
{

using namespace std;
using namespace boost;

/**
 * @brief The operator: attributes().
 *
 * @par Synopsis:
 *   attributes( srcArray )
 *
 * @par Summary:
 *   Produces a 1D result array where each cell describes one attribute of the source array.
 *
 * @par Input:
 *   - srcArray: a source array with srcAttrs and srcDims.
 *
 * @par Output array:
 *        <
 *   <br>   name: string
 *   <br>   type_id: string
 *   <br>   nullable: bool
 *   <br> >
 *   <br> [
 *   <br>   No: start=0, end=#attributes less 1, chunk interval=#attributes.
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
 *   - attributes(A) <name:string, type_id:string, nullable:bool> [No] =
 *     <br> No,   name,    type_id, nullable
 *     <br> 0, "quantity", "uint64", false
 *     <br> 1,   "sales",  "double", false
 *
 * @par Errors:
 *   n/a
 *
 * @par Notes:
 *   n/a
 *
 */
class LogicalAttributes: public LogicalOperator
{
public:
    LogicalAttributes(const string& logicalName, const std::string& alias):
        LogicalOperator(logicalName, alias)
    {
    	ADD_PARAM_IN_ARRAY_NAME()
    }

    ArrayDesc inferSchema(std::vector< ArrayDesc> inputSchemas, boost::shared_ptr< Query> query)
    {
        assert(inputSchemas.size() == 0);
        assert(_parameters.size() == 1);

        const string &arrayName = ((boost::shared_ptr<OperatorParamReference>&)_parameters[0])->getObjectName();

        ArrayDesc arrayDesc;
        SystemCatalog::getInstance()->getArrayDesc(arrayName, arrayDesc);

        Attributes attributes(3);
        attributes[0] = AttributeDesc((AttributeID)0, "name", TID_STRING, 0, 0);
        attributes[1] = AttributeDesc((AttributeID)1, "type_id", TID_STRING, 0, 0);
        attributes[2] = AttributeDesc((AttributeID)2, "nullable", TID_BOOL, 0, 0);
        vector<DimensionDesc> dimensions(1);
        size_t nAttrs = arrayDesc.getAttributes(true).size();
        size_t end    = nAttrs>0 ? nAttrs-1 : 0;
        dimensions[0] = DimensionDesc("No", 0, 0, end, end, nAttrs, 0);
        return ArrayDesc("Attributes", attributes, dimensions);
    }

};


DECLARE_LOGICAL_OPERATOR_FACTORY(LogicalAttributes, "attributes")


} //namespace
