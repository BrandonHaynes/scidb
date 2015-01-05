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
 * @file LogicalMStat.cpp
 *
 * @author roman.simakov@gmail.com
 *
 * mstat operator for gathering mallinfo from every instance
 */

#include "query/Operator.h"
#include "system/Exceptions.h"
#include "array/Metadata.h"


namespace scidb
{

using namespace std;
using namespace boost;

/**
 * @brief The operator: mstat().
 *
 * @par Synopsis:
 *   mstat()
 *
 * @par Summary:
 *   Gathers mallinfo from all the instances.
 *
 * @par Input:
 *   n/a
 *
 * @par Output array:
 *        <
 *   <br>   arena: int32
 *   <br>   ordblks: int32
 *   <br>   smblks: int32
 *   <br>   hblks: int32
 *   <br>   hblkhd: int32
 *   <br>   usmblks: int32
 *   <br>   fsmblks: int32
 *   <br>   uordblks: int32
 *   <br>   fordblks: int32
 *   <br>   keepcost: int32
 *   <br> >
 *   <br> [
 *   <br>   InstanceId: start=0, end=#instances-1, chunk interval=#instances.
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
class LogicalMStat: public LogicalOperator
{
public:
    LogicalMStat(const std::string& logicalName, const std::string& alias):
        LogicalOperator(logicalName, alias)
    {
    }

    ArrayDesc inferSchema(std::vector<ArrayDesc> inputSchemas, boost::shared_ptr<Query> query)
    {
        assert(inputSchemas.size() == 0);

        vector<AttributeDesc> attributes(10);
        attributes[0] = AttributeDesc((AttributeID)0, "arena",  TID_INT32, 0, 0);       /* non-mmapped space allocated from system */
        attributes[1] = AttributeDesc((AttributeID)1, "ordblks",  TID_INT32, 0, 0);     /* number of free chunks */
        attributes[2] = AttributeDesc((AttributeID)2, "smblks",  TID_INT32, 0, 0);      /* number of fastbin blocks */
        attributes[3] = AttributeDesc((AttributeID)3, "hblks",  TID_INT32, 0, 0);       /* number of mmapped regions */
        attributes[4] = AttributeDesc((AttributeID)4, "hblkhd",  TID_INT32, 0, 0);      /* space in mmapped regions */
        attributes[5] = AttributeDesc((AttributeID)5, "usmblks",  TID_INT32, 0, 0);     /* maximum total allocated space */
        attributes[6] = AttributeDesc((AttributeID)6, "fsmblks",  TID_INT32, 0, 0);     /* space available in freed fastbin blocks */
        attributes[7] = AttributeDesc((AttributeID)7, "uordblks",  TID_INT32, 0, 0);    /* total allocated space */
        attributes[8] = AttributeDesc((AttributeID)8, "fordblks",  TID_INT32, 0, 0);    /* total free space */
        attributes[9] = AttributeDesc((AttributeID)9, "keepcost",  TID_INT32, 0, 0);    /* top-most, releasable (via malloc_trim) space */

        vector<DimensionDesc> dimensions(1);
        size_t numNodes = Cluster::getInstance()->getInstanceMembership()->getInstances().size();
        size_t end      = numNodes>0 ? numNodes-1 : 0;
        dimensions[0] = DimensionDesc("InstanceId", 0, 0, end, end , numNodes, 0);

        return ArrayDesc("mstat", attributes, dimensions);
    }
};


DECLARE_LOGICAL_OPERATOR_FACTORY(LogicalMStat, "mstat")


} //namespace
