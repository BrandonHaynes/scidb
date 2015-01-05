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
 * @file misc.cpp
 *
 * @author paul@scidb.org
 *
 * @brief Shared library that loads into SciDB a collection of
 *        misc functions and utilities.
 */
#include <sys/time.h>
#include <sys/resource.h>
#include <vector>
#include <boost/assign.hpp>

#include "query/Operator.h"
#include "query/FunctionLibrary.h"
#include "query/FunctionDescription.h"
#include "query/TypeSystem.h"

using namespace std;
using namespace scidb;
using namespace boost::assign;

#include "misc_functions.h"

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
static class MiscLibrary
{
public:
    // Registering objects
    MiscLibrary()
    {
        _functionDescs.push_back(FunctionDescription("sleep", list_of(TID_INT64)(TID_INT32), TID_INT64, &sleepyInt));
        _functionDescs.push_back(FunctionDescription("trapOnNotEqual", list_of(TID_INT64)(TID_INT64), TID_INT64, &trapOnNotEqual));
        _functionDescs.push_back(FunctionDescription("exitOnNotEqual", list_of(TID_INT64)(TID_INT64), TID_INT64, &exitOnNotEqual));
        _functionDescs.push_back(FunctionDescription("netPauseOnNotEqual", list_of(TID_INT64)(TID_INT64)(TID_INT32), TID_INT64, &netPauseOnNotEqual));
        _functionDescs.push_back(FunctionDescription("injectRemoteError", list_of(TID_INT64)(TID_INT64), TID_INT64, &injectRemoteError));
        _functionDescs.push_back(FunctionDescription("killInstance", list_of(TID_INT64)(TID_INT32)(TID_BOOL), TID_INT64, &killInstance));
        _functionDescs.push_back(FunctionDescription("postWarning", list_of(TID_INT64), TID_INT64, &postWarning));
        _functionDescs.push_back(FunctionDescription("injectError", list_of(TID_INT64)(TID_INT64), TID_INT64, &injectError));
        _functionDescs.push_back(FunctionDescription("setMemCap", list_of(TID_INT64)(TID_INT64), TID_INT64, &setMemCap));

        _errors[MISC_FUNCTIONS_ERROR1] = "Generating trap to force transaction abort";
        _errors[MISC_FUNCTIONS_WARNING] = "Posting warning from instance '%1%'";
        scidb::ErrorsLibrary::getInstance()->registerErrors("misc_functions", &_errors);
    }

    ~MiscLibrary()
    {
        scidb::ErrorsLibrary::getInstance()->unregisterErrors("misc_functions");
    }

private:
    scidb::ErrorsLibrary::ErrorsMessages _errors;
} _instance;
