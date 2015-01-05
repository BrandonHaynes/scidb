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

/**
 * @file PhysicalUnLoadLibrary.cpp
 *
 * @brief Physical DDL operator which unload user defined library
 *
 * @author roman.simakov@gmail.com
 */

#include <string.h>

#include "query/Operator.h"
#include "system/SystemCatalog.h"
#include "util/PluginManager.h"
#include "query/FunctionDescription.h"
#include "query/OperatorLibrary.h"
#include "query/FunctionLibrary.h"

using namespace std;
using namespace boost;

namespace scidb
{

class PhysicalUnLoadLibrary: public PhysicalOperator
{
public:
    PhysicalUnLoadLibrary(const std::string& logicalName, const std::string& physicalName, const Parameters& parameters, const ArrayDesc& schema):
        PhysicalOperator(logicalName, physicalName, parameters, schema)
    {
    }

    boost::shared_ptr<Array> execute(std::vector< boost::shared_ptr<Array> >& inputArrays, boost::shared_ptr<Query> query)
    {
        assert(inputArrays.size() == 0);

        const string libraryName = ((boost::shared_ptr<OperatorParamPhysicalExpression>&)_parameters[0])->getExpression()->evaluate().getString();

        PluginManager::getInstance()->unLoadLibrary(libraryName);

        // It's DDL command and should not return a value
        return boost::shared_ptr< Array>();
    }
};

DECLARE_PHYSICAL_OPERATOR_FACTORY(PhysicalUnLoadLibrary, "unload_library", "impl_unload_library")

} //namespace
