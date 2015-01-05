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
 * LogicalShrink.cpp
 * shrink(input : array, r,d) : r/d are the ratio
 *
 *  Created on: May 20, 2010
 *      Author: drkwolf@gmail.com
 *  Modified: ded, difallah@gmail.com
 *
 */
#include "log4cxx/logger.h"
#include "query/Operator.h"
#include "system/Exceptions.h"

/**
 * shrink the slab with 3/10 ration, this is the first attribute only !
 * @slab :
 * Q3_regrid(Slab)
 */

namespace scidb {

static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("query.ops.LogicalOperator"));

class LogicalFindStars: public LogicalOperator
{
public:
  LogicalFindStars(const std::string& logicalName, const std::string& alias):
                   LogicalOperator(logicalName, alias)
  {
    ADD_PARAM_INPUT()
    ADD_PARAM_IN_ATTRIBUTE_NAME("void") // pix attribute
    ADD_PARAM_CONSTANT("uint32") // threshold
  }

  ArrayDesc inferSchema(std::vector< ArrayDesc> schemas, boost::shared_ptr< Query> query)
  {

    //cout << "begingin" << endl; flush(cout);

    assert(schemas.size() == 1);

    ArrayDesc const& desc = schemas[0];
    Dimensions const& dims = desc.getDimensions();

    Attributes aggAttrs(6); // pixel and Object Id (star id)
    Dimensions aggDims(3); // 3 dimension array
    
    // add the dimensions
    aggDims[0] = DimensionDesc(dims[0].getBaseName(), dims[0].getStart() , dims[0].getEndMax() , dims[0].getChunkInterval() , dims[0].getChunkOverlap());
    aggDims[1] = DimensionDesc(dims[1].getBaseName(), dims[1].getStart() , dims[1].getEndMax() , dims[1].getChunkInterval() , dims[1].getChunkOverlap());
    aggDims[2] = DimensionDesc(dims[2].getBaseName(), dims[2].getStart() , dims[2].getEndMax() , dims[2].getChunkInterval() , dims[2].getChunkOverlap());

    //cout << "Logical" << endl;
    // add the fixed attributes
    aggAttrs[0] = AttributeDesc((AttributeID)0, "oid", TID_INT64, AttributeDesc::IS_NULLABLE, 0);
    aggAttrs[1] = AttributeDesc((AttributeID)1, "center", TID_BOOL, AttributeDesc::IS_NULLABLE, 0);
    aggAttrs[2] = AttributeDesc((AttributeID)2, "polygon", TID_INT32, AttributeDesc::IS_NULLABLE, 0);
    aggAttrs[3] = AttributeDesc((AttributeID)3, "sumPixel", TID_INT64, AttributeDesc::IS_NULLABLE, 0);
    aggAttrs[4] = AttributeDesc((AttributeID)4, "avgDist", TID_DOUBLE, AttributeDesc::IS_NULLABLE, 0);
    aggAttrs[5] = AttributeDesc((AttributeID)5, "point", TID_BOOL, AttributeDesc::IS_NULLABLE, 0);
    return ArrayDesc("findstars", aggAttrs, aggDims);
  }
};

REGISTER_LOGICAL_OPERATOR_FACTORY(LogicalFindStars, "findstars");
} // namespcae scidb
