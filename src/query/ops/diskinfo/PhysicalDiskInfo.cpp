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
 * @file PhysicalDiskInfo.cpp
 *
 * @author knizhnik@garret.ru
 *
 * Physical implementation of DISKINFO operator for DiskInfoing data from text files
 */

#include <string.h>

#include "query/Operator.h"
#include "query/OperatorLibrary.h"
#include "array/TupleArray.h"
#include "smgr/io/Storage.h"

using namespace std;
using namespace boost;

namespace scidb
{
    
class PhysicalDiskInfo: public PhysicalOperator
{
  public:
    PhysicalDiskInfo(const string& logicalName, const string& physicalName, const Parameters& parameters, const ArrayDesc& schema):
        PhysicalOperator(logicalName, physicalName, parameters, schema)
    {
    }

    boost::shared_ptr<Array> execute(vector< boost::shared_ptr<Array> >& inputArrays, boost::shared_ptr<Query> query)
    {
        boost::shared_ptr<TupleArray> tuples(boost::make_shared<TupleArray>(_schema, _arena));
        Value tuple[5];
        Storage::DiskInfo info;
        StorageManager::getInstance().getDiskInfo(info);
        tuple[0].setUint64(info.used);
        tuple[1].setUint64(info.available);
        tuple[2].setUint64(info.clusterSize);
        tuple[3].setUint64(info.nFreeClusters);
        tuple[4].setUint64(info.nSegments);
        tuples->appendTuple(tuple);
        return tuples;
    }
};

DECLARE_PHYSICAL_OPERATOR_FACTORY(PhysicalDiskInfo, "diskinfo", "physicalDiskInfo")

} //namespace
