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
 * @file point.cpp
 *
 * @author roman.simakov@gmail.com
 *
 * @brief Example of shared library for loading into scidb with working
 * with point types.
 */

#include <vector>
#include <boost/assign.hpp>

#include "query/Operator.h"
#include "query/FunctionLibrary.h"
#include "query/FunctionDescription.h"
#include "query/TypeSystem.h"
#include "system/ErrorsLibrary.h"

using namespace std;
using namespace scidb;
using namespace boost::assign;

#include "functions.h"

/**
 * EXPORT FUNCTIONS
 * Functions from this section will be used by LOAD LIBRARY operator.
 */
vector<BaseLogicalOperatorFactory*> _logicalOperatorFactories;
EXPORTED_FUNCTION const vector<BaseLogicalOperatorFactory*>& GetLogicalOperatorFactories()
{
    return _logicalOperatorFactories;
}

vector<BasePhysicalOperatorFactory*> _physicalOperatorFactories;
EXPORTED_FUNCTION const vector<BasePhysicalOperatorFactory*>& GetPhysicalOperatorFactories()
{
    return _physicalOperatorFactories;
}

vector<Type> _types;
EXPORTED_FUNCTION const vector<Type>& GetTypes()
{
    return _types;
}

vector<FunctionDescription> _functionDescs;
EXPORTED_FUNCTION const vector<FunctionDescription>& GetFunctions()
{
    return _functionDescs;
}

/**
 * Class for registering/unregistering user defined objects
 */
static class PointLibrary
{
public:
    // Registering objects
    PointLibrary()
    {
        _types.push_back(Type("point", 2 * sizeof(double) * 8));

        _functionDescs.push_back(FunctionDescription("point", ArgTypes(), TypeId("point"), &construct_point));
        _functionDescs.push_back(FunctionDescription("str2point", list_of(TID_STRING), TypeId("point"), &str2Point));
        _functionDescs.push_back(FunctionDescription("point2str", list_of("point"), TID_STRING, &point2Str));
        _functionDescs.push_back(FunctionDescription("+",  list_of("point")("point"), TypeId("point"), &sumPoints));

        _errors[POINT_E_CANT_CONVERT_TO_POINT] = "Cannot convert '%1%' to point";
        scidb::ErrorsLibrary::getInstance()->registerErrors("libpoint", &_errors);
    }

    ~PointLibrary()
    {
        scidb::ErrorsLibrary::getInstance()->unregisterErrors("libpoint");
    }

private:
    scidb::ErrorsLibrary::ErrorsMessages _errors;
} _instance;

REGISTER_CONVERTER(point, string, EXPLICIT_CONVERSION_COST, point2Str);
REGISTER_CONVERTER(string, point, EXPLICIT_CONVERSION_COST, str2Point);
