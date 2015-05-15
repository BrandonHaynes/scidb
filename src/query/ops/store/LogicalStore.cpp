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
 * LogicalStore.cpp
 *
 *  Created on: Apr 17, 2010
 *      Author: Knizhnik
 */

#include <boost/foreach.hpp>
#include <map>

#include "query/Operator.h"
#include "system/SystemCatalog.h"
#include "system/Exceptions.h"
#include <smgr/io/Storage.h>

using namespace std;
using namespace boost;

namespace scidb {

/**
 * @brief The operator: store().
 *
 * @par Synopsis:
 *   store( srcArray, outputArray )
 *
 * @par Summary:
 *   Stores an array to the database. Each execution of store() causes a new version of the array to be created.
 *
 * @par Input:
 *   - srcArray: the source array with srcAttrs and srcDim.
 *   - outputArray: an existing array in the database, with the same schema as srcArray.
 *
 * @par Output array:
 *        <
 *   <br>   srcAttrs
 *   <br> >
 *   <br> [
 *   <br>   srcDims
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
class LogicalStore: public  LogicalOperator
{
public:
	LogicalStore(const string& logicalName, const std::string& alias):
	        LogicalOperator(logicalName, alias)
	{
        _properties.tile = true;
		ADD_PARAM_INPUT()
		ADD_PARAM_OUT_ARRAY_NAME()
	}

    void inferArrayAccess(boost::shared_ptr<Query>& query)
    {
        LogicalOperator::inferArrayAccess(query);
        assert(_parameters.size() > 0);
        assert(_parameters[0]->getParamType() == PARAM_ARRAY_REF);
        const string& arrayName = ((boost::shared_ptr<OperatorParamReference>&)_parameters[0])->getObjectName();

        assert(arrayName.find('@') == std::string::npos);
        boost::shared_ptr<SystemCatalog::LockDesc>  lock(new SystemCatalog::LockDesc(arrayName,
                                                                                     query->getQueryID(),
                                                                                     Cluster::getInstance()->getLocalInstanceId(),
                                                                                     SystemCatalog::LockDesc::COORD,
                                                                                     SystemCatalog::LockDesc::WR));
        boost::shared_ptr<SystemCatalog::LockDesc> resLock = query->requestLock(lock);
        assert(resLock);
        assert(resLock->getLockMode() >= SystemCatalog::LockDesc::WR);
    }

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
            Attributes outAttrs;
            map<string, uint64_t> attrsMap;
            BOOST_FOREACH(const AttributeDesc &attr, srcDesc.getAttributes())
            {
                AttributeDesc newAttr;
                if (!attrsMap.count(attr.getName()))
                {
                    attrsMap[attr.getName()] = 1;
                    newAttr = attr;
                }
                else
                {
                    while (true) {
                        stringstream ss;
                        ss << attr.getName() << "_" << ++attrsMap[attr.getName()];
                        if (attrsMap.count(ss.str()) == 0) {
                            newAttr = AttributeDesc(attr.getId(), ss.str(), attr.getType(), attr.getFlags(),
                                attr.getDefaultCompressionMethod(), attr.getAliases(), &attr.getDefaultValue(),
                                attr.getDefaultValueExpr());
                            attrsMap[ss.str()] = 1;
                            break;
                        }
                    }
                }

                outAttrs.push_back(newAttr);
            }

            Dimensions outDims;
            map<string, uint64_t> dimsMap;
            BOOST_FOREACH(const DimensionDesc &dim, srcDesc.getDimensions())
            {
                DimensionDesc newDim;
                if (!dimsMap.count(dim.getBaseName()))
                {
                    dimsMap[dim.getBaseName()] = 1;
                    newDim = DimensionDesc(dim.getBaseName(),
                                           dim.getStartMin(),
                                           dim.getCurrStart(),
                                           dim.getCurrEnd(),
                                           dim.getEndMax(),
                                           dim.getChunkInterval(),
                                           dim.getChunkOverlap());
                }
                else
                {
                    while (true) {
                        stringstream ss;
                        ss << dim.getBaseName() << "_" << ++dimsMap[dim.getBaseName()];
                        if (dimsMap.count(ss.str()) == 0) {
                            newDim = DimensionDesc(ss.str(),
                                                   dim.getStartMin(),
                                                   dim.getCurrStart(),
                                                   dim.getCurrEnd(),
                                                   dim.getEndMax(),
                                                   dim.getChunkInterval(),
                                                   dim.getChunkOverlap());
                            dimsMap[ss.str()] = 1;
                            break;
                        }
                    }
                }

                outDims.push_back(newDim);
            }

         /* Notice that when storing to a non-existant array, we do not propagate the 
            transience of the source array to to the target ...*/

            return ArrayDesc(arrayName, outAttrs, outDims, srcDesc.getFlags() & (~ArrayDesc::TRANSIENT));
        }
        else
        {
            Dimensions const& srcDims = srcDesc.getDimensions();
            Dimensions const& dstDims = dstDesc.getDimensions();

            //FIXME: Need more clear message and more granular condition
            if (srcDims.size() != dstDims.size())
                throw USER_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_ARRAYS_NOT_CONFORMANT) << "store";
            for (size_t i = 0, n = srcDims.size(); i < n; i++)
            {
                if (!(srcDims[i].getStartMin() == dstDims[i].getStartMin()
                           && (srcDims[i].getEndMax() == dstDims[i].getEndMax()
                               || (srcDims[i].getEndMax() < dstDims[i].getEndMax()
                                   && ((srcDims[i].getLength() % srcDims[i].getChunkInterval()) == 0
                                       || srcDesc.getEmptyBitmapAttribute() != NULL)))
                           && srcDims[i].getChunkInterval() == dstDims[i].getChunkInterval()
                           && srcDims[i].getChunkOverlap() == dstDims[i].getChunkOverlap()))
                {
                    // XXX To do: implement requiresRepart() method, remove interval/overlap checks
                    // above, use SCIDB_LE_START_INDEX_MISMATCH here.
                    throw USER_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_ARRAYS_NOT_CONFORMANT);
                }
            }

            Attributes const& srcAttrs = srcDesc.getAttributes(true);
            Attributes const& dstAttrs = dstDesc.getAttributes(true);

            if (srcAttrs.size() != dstAttrs.size())
                throw USER_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_ARRAYS_NOT_CONFORMANT);

            for (size_t i = 0, n = srcAttrs.size(); i < n; i++) {
                if(srcAttrs[i].getType() != dstAttrs[i].getType() || (!dstAttrs[i].isNullable() && srcAttrs[i].isNullable()))
                    throw USER_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_ARRAYS_NOT_CONFORMANT);
            }
            Dimensions newDims(dstDims.size());
            for (size_t i = 0; i < dstDims.size(); i++) {
                DimensionDesc const& dim = dstDims[i];
                newDims[i] = DimensionDesc(dim.getBaseName(),
                                           dim.getNamesAndAliases(),
                                           dim.getStartMin(), dim.getCurrStart(),
                                           dim.getCurrEnd(), dim.getEndMax(), dim.getChunkInterval(), dim.getChunkOverlap());
            }
            return ArrayDesc(dstDesc.getId(), dstDesc.getUAId(), dstDesc.getVersionId(), arrayName, dstDesc.getAttributes(), newDims, dstDesc.getFlags());
        }
	}
};

DECLARE_LOGICAL_OPERATOR_FACTORY(LogicalStore, "store")

}  // namespace scidb
