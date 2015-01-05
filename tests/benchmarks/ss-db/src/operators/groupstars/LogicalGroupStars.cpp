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
 *
 *  Created on: Feb 20, 2012
 *  Author: ded, difallah@gmail.com
 *
 */
#include "log4cxx/logger.h"
#include "query/Operator.h"
#include "system/Exceptions.h"

namespace scidb {

static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("query.ops.LogicalOperator"));

class LogicalGroupStars: public LogicalOperator
{
public:
  LogicalGroupStars(const std::string& logicalName, const std::string& alias):
                   LogicalOperator(logicalName, alias)
  {
    ADD_PARAM_INPUT()
    ADD_PARAM_INPUT()
    ADD_PARAM_CONSTANT("double")
    ADD_PARAM_CONSTANT("uint32") // Backtacking
  }

  ArrayDesc inferSchema(std::vector< ArrayDesc> schemas, boost::shared_ptr< Query> query)
  {

    Attributes aggAttrs(3); // obsid, x,y 
    Dimensions aggDims(2); // 2 dimension array
    
    // add the dimensions
    aggDims[0] = DimensionDesc("group", 0 , INFINITE_LENGTH , 1000 , 0);
    aggDims[1] = DimensionDesc("observation", 0 , INFINITE_LENGTH , 20 , 0);

    // add the fixed attributes
    aggAttrs[0] = AttributeDesc((AttributeID)0, "oid", TID_INT64, AttributeDesc::IS_NULLABLE, 0);
    aggAttrs[1] = AttributeDesc((AttributeID)1, "x", TID_INT64, AttributeDesc::IS_NULLABLE, 0);
    aggAttrs[2] = AttributeDesc((AttributeID)2, "y", TID_INT64, AttributeDesc::IS_NULLABLE, 0);
    return ArrayDesc("groupstars", aggAttrs, aggDims);
  }
};

REGISTER_LOGICAL_OPERATOR_FACTORY(LogicalGroupStars, "groupstars");
} // namespcae scidb
