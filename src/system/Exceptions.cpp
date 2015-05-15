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
 * @file Exceptions.h
 *
 * @author: Artyom Smirnov <smirnoffjr@gmail.com>
 *
 * @brief Exceptions which thrown inside SciDB
 */

#include <iostream>
#include <sstream>

#include "system/ErrorCodes.h"
#include "system/Exceptions.h"
#include "system/ErrorsLibrary.h"
#include "query/Query.h"
#include "query/ParsingContext.h"

using namespace std;
using namespace boost;

namespace scidb
{

/*
 * Exception
 */
Exception::Exception(const char* file, const char* function, int32_t line,
        const char* errors_namespace, int32_t short_error_code, int32_t long_error_code,
        const char* stringified_short_error_code, const char* stringified_long_error_code,
        uint64_t query_id):
    _file(file),
    _function(function),
    _line(line),
    _errors_namespace(errors_namespace),
    _short_error_code(short_error_code),
    _long_error_code(long_error_code),
    _stringified_short_error_code(stringified_short_error_code),
    _stringified_long_error_code(stringified_long_error_code),
    _query_id(query_id)
{
}

format& Exception::getMessageFormatter() const
{
    if (_formatter.size() == 0) {
        _formatter = boost::format(ErrorsLibrary::getInstance()->getLongErrorMessage(_errors_namespace,
                                                                                     _long_error_code));
    }
    assert(_formatter.size() != 0);
    return _formatter;
}

const string& Exception::getErrorsNamespace() const
{
    return _errors_namespace;
}

const char* Exception::what() const throw()
{
	return _what_str.c_str();
}

const std::string& Exception::getFile() const
{
	return _file;
}

const std::string& Exception::getFunction() const
{
	return _function;
}

int32_t Exception::getLine() const
{
	return _line;
}

const std::string& Exception::getWhatStr() const
{
	return _what_str;
}

int32_t Exception::getShortErrorCode() const
{
	return _short_error_code;
}

int32_t Exception::getLongErrorCode() const
{
    return _long_error_code;
}

const std::string& Exception::getStringifiedShortErrorCode() const
{
    return _stringified_short_error_code;
}

const std::string& Exception::getStringifiedLongErrorCode() const
{
    return _stringified_long_error_code;
}

const string Exception::getErrorId() const
{
    stringstream ss;
    ss << _errors_namespace << "::" << _short_error_code << "::" << _long_error_code;
    return ss.str();
}

const string Exception::getStringifiedErrorId() const
{
    return _errors_namespace + "::" + _stringified_short_error_code + "::" + _stringified_long_error_code;
}

std::string Exception::getErrorMessage() const
{
    try
    {
        return boost::str(getMessageFormatter());
    }
    catch(...)
    {
        string s("!!!Cannot format error message. Check argument count for ");
        s += getErrorId();
        s += "!!!";
        return s;
    }
}

uint64_t Exception::getQueryId() const
{
    return _query_id;
}

void Exception::setQueryId(uint64_t queryId)
{
    _query_id = queryId;
}

/*
 * UserException
 */
UserException::UserException(const char* file, const char* function, int32_t line,
    const char* errors_namespace, int32_t short_error_code, int32_t long_error_code,
    const char* stringified_short_error_code, const char* stringified_long_error_code,
    uint64_t query_id):
        Exception(file, function, line, errors_namespace, short_error_code, long_error_code,
            stringified_short_error_code, stringified_long_error_code, query_id)
{
    format();
}

UserException::UserException(const char* file, const char* function, int32_t line,
    const char* errors_namespace, int32_t short_error_code, int32_t long_error_code, const char* what_str,
    const char* stringified_short_error_code, const char* stringified_long_error_code,
    uint64_t query_id):
        Exception(file, function, line, errors_namespace, short_error_code, long_error_code,
            stringified_short_error_code, stringified_long_error_code, query_id)
{
    _what_str = what_str;
}

void UserException::format()
{
    stringstream ss;
    ss << "UserException in file: " << _file << " function: " << _function << " line: " << _line << endl
       << "Error id: " << _errors_namespace << "::" << _stringified_short_error_code << "::" << _stringified_long_error_code << endl
       << "Error description: " << ErrorsLibrary::getInstance()->getShortErrorMessage(_short_error_code) << ". " << getErrorMessage() << ".";
    if (_query_id) ss << endl << "Failed query id: " << _query_id;

    _what_str = ss.str();
}

Exception::Pointer UserException::copy() const
{
    shared_ptr<UserException> e = shared_ptr<UserException>(
        new UserException(_file.c_str(), _function.c_str(), _line, _errors_namespace.c_str(),
            _short_error_code, _long_error_code, _stringified_short_error_code.c_str(),
            _stringified_long_error_code.c_str(), _query_id));

    e->_what_str = _what_str;
    e->_formatter = _formatter;

    return e;
}

void UserException::raise() const
{
    throw *this;
}

/*
 * UserQueryException
 */
UserQueryException::UserQueryException(const char* file, const char* function, int32_t line,
    const char* errors_namespace, int32_t short_error_code, int32_t long_error_code,
    const char* stringified_short_error_code, const char* stringified_long_error_code,
    const boost::shared_ptr<ParsingContext>& parsingContext, uint64_t query_id):
        UserException(file, function, line, errors_namespace, short_error_code, long_error_code,
            stringified_short_error_code, stringified_long_error_code, query_id),
        _parsingContext(parsingContext)
{
    format();
}

UserQueryException::UserQueryException(const char* file, const char* function, int32_t line,
    const char* errors_namespace, int32_t short_error_code, int32_t long_error_code, const char* what_str,
    const char* stringified_short_error_code, const char* stringified_long_error_code,
    const boost::shared_ptr<ParsingContext>& parsingContext, uint64_t query_id):
        UserException(file, function, line, errors_namespace, short_error_code, long_error_code,
            stringified_short_error_code, stringified_long_error_code, query_id),
        _parsingContext(parsingContext)
{
    _what_str = what_str;
}

void carrots(string &str, size_t line, size_t start, size_t end)
{
    const string tmp = str;
    str = "";
    const size_t len = ( end > start ) ? end - start : 1;
    const string carrots = string(start - 1, ' ') + string(len, '^');

    bool done = false;
    size_t lineCounter = 1;
    for (string::const_iterator it = tmp.begin(); it != tmp.end(); ++it)
    {
        str += *it;
        if (*it == '\n')
            ++lineCounter;
        if (lineCounter == line + 1)
        {
            const string tail = string(it, tmp.end());
            str += carrots + tail;//(tail != "" ? "\n" + tail : "");
            done = true;
            break;
        }
    }
    if (!done)
        str += '\n' + carrots;
}

void UserQueryException::format()
{
    stringstream ss;
    string query = _parsingContext->getQueryString();
    carrots(query, _parsingContext->getLineStart(), _parsingContext->getColStart(), _parsingContext->getColEnd());
    ss << "UserQueryException in file: " << _file << " function: " << _function << " line: " << _line << endl
       << "Error id: " << _errors_namespace << "::" << _stringified_short_error_code << "::" << _stringified_long_error_code << endl
       << "Error description: " << ErrorsLibrary::getInstance()->getShortErrorMessage(_short_error_code) << ". " << getErrorMessage() << "." << endl
       << query;
    if (_query_id) ss << endl <<  "Failed query id: " << _query_id;

    _what_str = ss.str();
}

boost::shared_ptr<ParsingContext> UserQueryException::getParsingContext() const
{
    return _parsingContext;
}

Exception::Pointer UserQueryException::copy() const
{
    shared_ptr<UserQueryException> e = shared_ptr<UserQueryException>(
        new UserQueryException(_file.c_str(), _function.c_str(),
            _line, _errors_namespace.c_str(), _short_error_code, _long_error_code,
            _stringified_short_error_code.c_str(), _stringified_long_error_code.c_str(),
            _parsingContext,_query_id));

    e->_what_str = _what_str;
    e->_formatter = _formatter;

    return e;
}

void UserQueryException::raise() const
{
    throw *this;
}

/*
 * SystemException
 */
SystemException::SystemException(const char* file, const char* function, int32_t line,
    const char* errors_namespace, int32_t short_error_code, int32_t long_error_code,
    const char* stringified_short_error_code, const char* stringified_long_error_code,
    uint64_t query_id):
        Exception(file, function, line, errors_namespace, short_error_code, long_error_code,
            stringified_short_error_code, stringified_long_error_code, query_id)
{
    format();
}

SystemException::SystemException(const char* file, const char* function, int32_t line,
    const char* errors_namespace, int32_t short_error_code, int32_t long_error_code, const char* what_str,
    const char* stringified_short_error_code, const char* stringified_long_error_code,
    uint64_t query_id):
        Exception(file, function, line, errors_namespace, short_error_code, long_error_code,
            stringified_short_error_code, stringified_long_error_code, query_id)
{
    _what_str = what_str;
}


void SystemException::format()
{
    stringstream ss;
    ss << "SystemException in file: " << _file << " function: " << _function << " line: " << _line << endl
       << "Error id: " << _errors_namespace << "::" << _stringified_short_error_code << "::" << _stringified_long_error_code << endl
       << "Error description: " << ErrorsLibrary::getInstance()->getShortErrorMessage(_short_error_code) << ". " << getErrorMessage() << ".";
    if (_query_id) ss << endl << "Failed query id: " << _query_id ;

    _what_str = ss.str();
}

Exception::Pointer SystemException::copy() const
{
    shared_ptr<SystemException> e = shared_ptr<SystemException>(new SystemException(_file.c_str(),
        _function.c_str(), _line, _errors_namespace.c_str(), _short_error_code, _long_error_code,
        _stringified_short_error_code.c_str(), _stringified_long_error_code.c_str(), _query_id));

    e->_what_str = _what_str;
    e->_formatter = _formatter;

    return e;
}

void SystemException::raise() const
{
    throw *this;
}

}
