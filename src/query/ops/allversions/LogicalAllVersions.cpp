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
 * @file LogicalAllVersions.cpp
 *
 * @author knizhnik@garret.ru
 *
 * Get list of updatable array versions
 */

#include <utility>

#include "query/Operator.h"
#include "system/Exceptions.h"
#include "array/Metadata.h"
#include "system/SystemCatalog.h"

namespace scidb
{

using namespace std;
using namespace boost;

/**
 * @brief The operator: allversions().
 *
 * @par Synopsis:
 *   allversions( srcArray )
 *
 * @par Summary:
 *   Creates a single array containing all versions of an existing array.
 *
 * @par Input:
 *   - srcArray: a source array with srcAttrs and srcDims.
 *
 * @par Output array:
 *        <
 *   <br>   srcAttrs
 *   <br> >
 *   <br> [
 *   <br>   VersionNo: type=int64, start=1, end=last version no, chunk interval=1
 *   <br>   srcDims
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
 *   - allversions(A) <quantity: uint64, sales:double> [VersionNo, year, item]  =
 *     <br> VersionNo, year, item, quantity, sales
 *     <br>     1,     2011,  2,      7,     31.64
 *     <br>     1,     2011,  3,      6,     19.98
 *     <br>     1,     2012,  1,      5,     41.65
 *     <br>     1,     2012,  2,      9,     40.68
 *     <br>     1,     2012,  3,      8,     26.64
 *
 * @par Errors:
 *   n/a
 *
 * @par Notes:
 *   n/a
 *
 */
class LogicalAllVersions: public LogicalOperator
{
public:
    LogicalAllVersions(const string& logicalName, const std::string& alias):
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

        size_t nAllVersions = std::max(SystemCatalog::getInstance()->getArrayVersions(arrayDesc.getId()).size(),1UL);
        Dimensions const& srcDims = arrayDesc.getDimensions();
        size_t nDims = srcDims.size();
        Dimensions dstDims(nDims+1);

        dstDims[0] = DimensionDesc("VersionNo", 1, 1, nAllVersions, nAllVersions, 1, 0);
        for (size_t i = 0; i < nDims; i++) { 
            DimensionDesc const& dim = srcDims[i];
            dstDims[i+1] = DimensionDesc(dim.getBaseName(),
                                         dim.getNamesAndAliases(),
                                         dim.getStartMin(),
                                         dim.getCurrStart(), 
                                         dim.getCurrEnd(), 
                                         dim.getEndMax(),
                                         dim.getChunkInterval(), 
                                         dim.getChunkOverlap());
        }
        return ArrayDesc(arrayDesc.getName(), arrayDesc.getAttributes(), dstDims);

    }

};


DECLARE_LOGICAL_OPERATOR_FACTORY(LogicalAllVersions, "allversions")


} //namespace
