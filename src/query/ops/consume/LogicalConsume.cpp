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
 * LogicalConsume.cpp
 *
 *  Created on: Aug 6, 2013
 *      Author: Tigor, Fridella
 */

#include <query/Operator.h>
#include <system/SystemCatalog.h>
#include <system/Exceptions.h>
#include <array/Metadata.h>

using namespace std;

namespace scidb
{

/**
 * @brief The operator: consume().
 *
 * @par Synopsis:
 *   consume( array [, numAttrsToScanAtOnce] )
 *
 * @par Summary:
 *   Accesses each cell of an input array, if possible, by extracting tiles and iterating over tiles.
 *   numAttrsToScanAtOnce determines the number of attributes to scan as a group.
 *   Setting this value to "1" will result in a "vertical" scan---all chunks of
 *   the current attribute will be scanned before moving on to the next attribute.
 *   Setting this value to the number of attributes will result in a "horizontal"
 *   scan---chunk i of every attribute will be scanned before moving on to chunk
 *   i+1
 *
 * @par Input:
 *   - array: the array to consume
 *   - numAttrsToScanAtOnce: optional "stride" of the scan, default is 1
 *
 * @par Output array (an empty array):
 *        <
 *   <br> >
 *   <br> [
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
class LogicalConsume: public LogicalOperator
{
private:
    int _numVaryParam;

public:
    LogicalConsume(const string& logicalName, const std::string& alias):
        LogicalOperator(logicalName, alias),
        _numVaryParam(0)
    {
        _properties.tile = true;
        ADD_PARAM_INPUT()
        ADD_PARAM_VARIES()
    }

    std::vector<boost::shared_ptr<OperatorParamPlaceholder> > nextVaryParamPlaceholder(const std::vector< ArrayDesc> &schemas)
    {
        std::vector<boost::shared_ptr<OperatorParamPlaceholder> > res;
        res.push_back(END_OF_VARIES_PARAMS());
        if (_numVaryParam == 0)
        {
            res.push_back(PARAM_CONSTANT("uint64"));
        }
        _numVaryParam++;
        return res;
    }

    ArrayDesc inferSchema(std::vector< ArrayDesc> schemas, boost::shared_ptr< Query> query)
    {
        return schemas[0];
    }
};

DECLARE_LOGICAL_OPERATOR_FACTORY(LogicalConsume, "consume")

} //namespace
