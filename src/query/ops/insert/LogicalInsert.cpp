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
 * LogicalInsert.cpp
 *
 *  Created on: Aug 30, 2012
 *      Author: poliocough@gmail.com
 */

#include <boost/foreach.hpp>
#include <map>

#include "query/Operator.h"
#include "system/SystemCatalog.h"
#include "system/Exceptions.h"
#include <smgr/io/Storage.h>

using namespace std;
using namespace boost;

namespace scidb
{

/**
 * @brief The operator: insert().
 *
 * @par Synopsis:
 *   insert( sourceArray, targetArrayName )
 *
 * @par Summary:
 *   Inserts all data from left array into the persistent targetArray. targetArray must exist with matching dimensions and attributes.
 *   targetArray must also be mutable. The operator shall create a new version of targetArray that contains all data of the array that would have
 *   been received by merge(sourceArray, targetArrayName). In other words, new data is inserted between old data and overwrites any overlapping old values.
 *   The resulting array is then returned.
 *
 * @par Input:
 *   - sourceArray the array or query that provides inserted data
 *   - targetArrayName: the name of the persistent array inserted into
 *
 * @par Output array:
 *   - the result of insertion
 *   - same schema as targetArray
 *
 * @par Examples:
 *   n/a
 *
 * @par Errors:
 *   n/a
 *
 * @par Notes:
 *   Some might wonder - if this returns the same result as merge(sourceArray, targetArrayName), then why not use store(merge())? The answer is that
 *   1. this runs a lot faster - it does not perform a full scan of targetArray
 *   2. this also generates less chunk headers
 *
 */
class LogicalInsert: public  LogicalOperator
{
public:

    /**
     * Default conforming to the operator factory mechanism
     * @param[in] logicalName "insert"
     * @param[in] alias not used by this operator
     */
    LogicalInsert(const string& logicalName, const std::string& alias):
        LogicalOperator(logicalName, alias)
    {
        _properties.tile = true;
        ADD_PARAM_INPUT()
        ADD_PARAM_OUT_ARRAY_NAME()
    }

    /**
     * Request a lock for all arrays that will be accessed by this operator.
     * Calls requestLock with the write lock over the target array (array inserted into)
     * @param query the query context
     */
    void inferArrayAccess(boost::shared_ptr<Query>& query)
    {
        LogicalOperator::inferArrayAccess(query);
        assert(_parameters.size() > 0);
        assert(_parameters[0]->getParamType() == PARAM_ARRAY_REF);
        const string& arrayName = ((boost::shared_ptr<OperatorParamReference>&)_parameters[0])->getObjectName();
        assert(ArrayDesc::isNameUnversioned(arrayName));
        boost::shared_ptr<SystemCatalog::LockDesc>  lock(new SystemCatalog::LockDesc(arrayName,
                                                                                     query->getQueryID(),
                                                                                     Cluster::getInstance()->getLocalInstanceId(),
                                                                                     SystemCatalog::LockDesc::COORD,
                                                                                     SystemCatalog::LockDesc::WR));
        boost::shared_ptr<SystemCatalog::LockDesc> resLock = query->requestLock(lock);
        assert(resLock);
        assert(resLock->getLockMode() >= SystemCatalog::LockDesc::WR);
    }

    /**
     * Perform operator-specific checks of input and return the shape of the output. Currently,
     * the output array must exist.
     * @param schemas the shapes of the input arrays
     * @param query the query context
     */
    ArrayDesc inferSchema(std::vector< ArrayDesc> schemas, shared_ptr< Query> query)
    {
        assert(schemas.size() == 1);
        assert(_parameters.size() == 1);

        string arrayName = ((shared_ptr<OperatorParamReference>&)_parameters[0])->getObjectName();
        ArrayDesc const& srcDesc = schemas[0];

        //Ensure attributes names uniqueness.
        ArrayDesc dstDesc;
        if (!SystemCatalog::getInstance()->getArrayDesc(arrayName, dstDesc, false))
        {
            throw USER_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_ARRAY_DOESNT_EXIST) << arrayName;
        }

        Dimensions const& srcDims = srcDesc.getDimensions();
        Dimensions const& dstDims = dstDesc.getDimensions();

        if (srcDims.size() != dstDims.size())
        {
            //TODO: this will get lifted when we allow redimension+insert in the same op
            //and when we DO implement redimension+insert - we will need to match attributes/dimensions by name, not position.
            throw USER_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_ILLEGAL_OPERATION)
                    << "Temporary restriction: target of INSERT must have same dimensions as the source";
        }

        for (size_t i = 0, n = srcDims.size(); i < n; i++)
        {
            //TODO: we can also allow arrays that are smaller whose length is not evenly divided by chunk interval
            //but then we have to detect "edge chunks" and rewrite them cleverly
            if( srcDims[i].getStartMin() != dstDims[i].getStartMin() ||
                srcDims[i].getChunkInterval() != dstDims[i].getChunkInterval() ||
                srcDims[i].getChunkOverlap() != dstDims[i].getChunkOverlap() ||
                srcDims[i].getEndMax() > dstDims[i].getEndMax() ||
                ( srcDims[i].getEndMax() < dstDims[i].getEndMax() &&
                  srcDims[i].getLength() % srcDims[i].getChunkInterval() != 0))
            {
                throw USER_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_DIMENSIONS_DONT_MATCH)
                        << srcDims[i].getBaseName() << dstDims[i].getBaseName();
            }
        }

        Attributes const& srcAttrs = srcDesc.getAttributes(true);
        Attributes const& dstAttrs = dstDesc.getAttributes(true);

        if (srcAttrs.size() != dstAttrs.size())
        {
            throw USER_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_ILLEGAL_OPERATION)
                    << "Temporary restriction: target of INSERT must have same attributes as the source";
        }
        for (size_t i = 0, n = srcAttrs.size(); i < n; i++)
        {
            if(srcAttrs[i].getType() != dstAttrs[i].getType())
            {
                throw USER_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_WRONG_ATTRIBUTE_TYPE)
                    << srcAttrs[i].getName() << srcAttrs[i].getType() << dstAttrs[i].getType();
            }

            //can't store nulls into a non-nullable attribute
            if(!dstAttrs[i].isNullable() && srcAttrs[i].isNullable())
            {
                throw USER_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_WRONG_ATTRIBUTE_FLAGS)
                   << srcAttrs[i].getName();
            }
        }

        //Note: let us NOT add arrayID numbers to the schema - because we do not have our ArrayID yet.
        //We will get our ArrayID when we execute and create the array. Until then - don't bother.
        //Old store code adds the arrayID to the schema - but that's the arrayID of the previous version,
        //not the new version created by the op. A dangerous fallacy - stupid and unnecessary.
        return ArrayDesc(arrayName, dstDesc.getAttributes(), dstDesc.getDimensions(), dstDesc.getFlags());
    }
};

DECLARE_LOGICAL_OPERATOR_FACTORY(LogicalInsert, "insert")

}  // namespace scidb
