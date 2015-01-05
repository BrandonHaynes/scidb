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
 * @file LogicalVersions.cpp
 *
 * @author knizhnik@garret.ru
 *
 * Get list of updatable array versions
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
 * @brief The operator: versions().
 *
 * @par Synopsis:
 *   versions( srcArray )
 *
 * @par Summary:
 *   Lists all versions of an array in the database.
 *
 * @par Input:
 *   - srcArray: a source array.
 *
 * @par Output array:
 *        <
 *   <br>   version_id
 *   <br>   timestamp: a string describing the creation time of the version
 *   <br> >
 *   <br> [
 *   <br>   VersionNo: start=1, end=#versions, chunk interval=#versions
 *   <br> ]
 *
 * @par Examples:
 *   n/a
 *
 * @par Errors:
 *   n/a
 *
 * @par Notes:
 *   n/a
 *
 */
class LogicalVersions: public LogicalOperator
{
public:
    LogicalVersions(const string& logicalName, const std::string& alias):
        LogicalOperator(logicalName, alias)
    {
    	ADD_PARAM_IN_ARRAY_NAME()
    }

    ArrayDesc inferSchema(std::vector<ArrayDesc> inputSchemas, boost::shared_ptr<Query> query)
    {
        assert(inputSchemas.size() == 0);
        assert(_parameters.size() == 1);

        const string &arrayName = ((boost::shared_ptr<OperatorParamReference>&)_parameters[0])->getObjectName();

        ArrayDesc arrayDesc;
        SystemCatalog::getInstance()->getArrayDesc(arrayName, arrayDesc);

        size_t nVersions = SystemCatalog::getInstance()->getArrayVersions(arrayDesc.getId()).size();
        Attributes attributes(2);
        attributes[0] = AttributeDesc((AttributeID)0, "version_id", TID_INT64, 0, 0);
        attributes[1] = AttributeDesc((AttributeID)1, "timestamp", TID_DATETIME, 0, 0);
        vector<DimensionDesc> dimensions(1);

        if (nVersions == 0)
        {
            nVersions = 1; // clamp at 1 for one based indexing
        }

        dimensions[0] = DimensionDesc("VersionNo", 1, 1, nVersions, nVersions, nVersions, 0);
        return ArrayDesc("Versions", attributes, dimensions);
    }

};


DECLARE_LOGICAL_OPERATOR_FACTORY(LogicalVersions, "versions")


} //namespace
