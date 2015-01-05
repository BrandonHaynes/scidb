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
 * @author Artyom Smirnov <smirnoffjr@gmail.com>
 *
 * @brief Class for posting warning on client side
 */

#include <sstream>
#include <iostream>

#include "system/Warnings.h"
#include "system/ErrorCodes.h"
#include "system/ErrorsLibrary.h"

using namespace std;

namespace scidb
{

Warning::Warning():
    _file(""),
    _function(""),
    _line(0),
    _strings_namespace("scidb"),
    _code(0),
    _stringified_code(""),
    _what_str("")
{
}

Warning::Warning(const char* file, const char* function, int32_t line,
        const char* strings_namespace, int32_t code,
        const char *stringified_code):
    _file(file),
    _function(function),
    _line(line),
    _strings_namespace(strings_namespace),
    _code(code),
    _stringified_code(stringified_code)
{
    _formatter = boost::format(ErrorsLibrary::getInstance()->getLongErrorMessage(
                                   _strings_namespace,
                                   _code));
    format();
}

Warning::Warning(const char* file, const char* function, int32_t line,
        const char* strings_namespace, int32_t code,
        const char* msg, const char *stringified_code):
    _file(file),
    _function(function),
    _line(line),
    _strings_namespace(strings_namespace),
    _code(code),
    _stringified_code(stringified_code),
    _what_str(msg)
{
}

const string& Warning::getStringsNamespace() const
{
    return _strings_namespace;
}

const string& Warning::getFile() const
{
    return _file;
}

const string& Warning::getFunction() const
{
    return _function;
}

int32_t Warning::getLine() const
{
    return _line;
}

const string& Warning::msg() const
{
    return _what_str;
}

int32_t Warning::getCode() const
{
    return _code;
}

const string& Warning::getStringifiedCode() const
{
    return _stringified_code;
}

const string Warning::getWarningId() const
{
    return _strings_namespace + "::" + _stringified_code;
}

string Warning::getMessage() const
{
    try
    {
        return boost::str(_formatter);
    }
    catch(...)
    {
        return "!!!Can not format warning message. Check arguments count!!!";
    }
}

void Warning::format()
{
    _what_str = getMessage();
}

}//namespace scidb
