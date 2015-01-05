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
 * @file
 *
 * @author: Artyom Smirnov <smirnoffjr@gmail.com>
 *
 * @brief Class for controlling built-in and user defined error codes and messages.
 */

#include "util/Mutex.h"

#include "system/ErrorsLibrary.h"
#include "system/ErrorCodes.h"
#include "system/Exceptions.h"

namespace scidb
{

ErrorsLibrary::ErrorsLibrary()
{
    #define X(_name, _code, _msg) _builtinShortErrorsMsg[_name] = _msg ;
    #include "system/ShortErrors.inc"
    #undef X

    #define X(_name, _code, _msg) _builtinLongErrorsMsg[_name] = _msg ;
    #include "system/LongErrors.inc"
    #undef X

    registerErrors("scidb", &_builtinLongErrorsMsg);
}

void ErrorsLibrary::registerErrors(const std::string &errorsNamespace, ErrorsMessages* msgMap)
{
    ScopedMutexLock lock(_lock);
    int badErrorNumber = 0;

    if (_errorNamespaces.find(errorsNamespace) != _errorNamespaces.end())
        throw SYSTEM_EXCEPTION(SCIDB_SE_ERRORS_MGR, SCIDB_LE_ERRNS_ALREADY_REGISTERED) << errorsNamespace;

    if ("scidb" != errorsNamespace)
    {
        // Check that non-SciDB error numbers are all > SCIDB_MAX_SYSTEM_ERROR.
        // (If there is more than one "foreign" error space, they still
        // might collide however.  Bummer.)
        for (ErrorsMessages::const_iterator it = msgMap->begin(); it != msgMap->end(); ++it)
        {
            if (it->first <= SCIDB_MAX_SYSTEM_ERROR)
            {
                throw USER_EXCEPTION(SCIDB_SE_ERRORS_MGR, SCIDB_LE_INVALID_USER_ERROR_CODE)
                    << errorsNamespace << it->first << SCIDB_MAX_SYSTEM_ERROR;
            }
        }
    }
    else {
        // Check that SciDB system error numbers are all <= SCIDB_MAX_SYSTEM_ERROR.
        for (ErrorsMessages::const_iterator it = msgMap->begin(); it != msgMap->end(); ++it)
        {
            if (it->first > SCIDB_MAX_SYSTEM_ERROR)
            {
                badErrorNumber = it->first;
                break;
            }
        }
    }

    _errorNamespaces[errorsNamespace] = msgMap;

    // Have to wait for SciDB errors to get registered (previous line)
    // before we throw one!  Assuming SCIDB_LE_INVALID_SYSTEM_ERROR_CODE
    // is itself valid.... ;-D
    if (badErrorNumber) {
        throw SYSTEM_EXCEPTION(SCIDB_SE_ERRORS_MGR,
                               SCIDB_LE_INVALID_SYSTEM_ERROR_CODE)
            << badErrorNumber << SCIDB_MAX_SYSTEM_ERROR;
    }
}

void ErrorsLibrary::unregisterErrors(const std::string &errorsNamespace)
{
    ScopedMutexLock lock(_lock);

    if ("scidb" == errorsNamespace)
        throw SYSTEM_EXCEPTION(SCIDB_SE_ERRORS_MGR, SCIDB_LE_ERRNS_CAN_NOT_BE_UNREGISTERED) << errorsNamespace;

    _errorNamespaces.erase(errorsNamespace);
}

const std::string ErrorsLibrary::getShortErrorMessage(int32_t shortError)
{
    ScopedMutexLock lock(_lock);

    if (_builtinShortErrorsMsg.end() == _builtinShortErrorsMsg.find(shortError))
    {
        return boost::str(boost::format("!!!Cannot obtain short error message for short error code"
            " '%1%' because it was not registered!!!") % shortError);
    }

    return _builtinShortErrorsMsg[shortError];
}

const std::string ErrorsLibrary::getLongErrorMessage(const std::string &errorsNamespace, int32_t longError)
{
    ScopedMutexLock lock(_lock);

    ErrorsNamespaces::const_iterator nsIt = _errorNamespaces.find(errorsNamespace);
    if (_errorNamespaces.end() == nsIt)
    {
        return boost::str(boost::format("!!!Cannot obtain long error message for long error code '%1%' because "
            "errors namespace '%2%' was not registered!!!") % longError % errorsNamespace);
    }

    ErrorsMessages::const_iterator errIt = nsIt->second->find(longError);
    if (nsIt->second->end() == errIt)
    {
        return boost::str(boost::format("!!!Cannot obtain error message for error code '%1%'"
            " from errors namespace '%2%' because error code '%1%' was not registered!!!")
            % longError % errorsNamespace);
    }

    return errIt->second;
}

}
