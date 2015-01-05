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

#ifndef WARNINGS_H_
#define WARNINGS_H_

#include <string>
#include <stdint.h>
#include <boost/format.hpp>

#include "util/StringUtil.h"

namespace scidb
{

#define SCIDB_WARNING(code) \
    scidb::Warning(REL_FILE, __FUNCTION__, __LINE__, "scidb", code, #code)

#define SCIDB_PLUGIN_WARNING(strings_namespace, code) \
    scidb::Warning(REL_FILE, __FUNCTION__, __LINE__, strings_namespace, code, #code)


class Warning
{
public:
    Warning();

    Warning(const char* file, const char* function, int32_t line,
            const char* strings_namespace, int32_t code,
            const char* stringified_code);

    Warning(const char* file, const char* function, int32_t line,
            const char* strings_namespace, int32_t code,
            const char* msg, const char* stringified_code);

    const std::string& getStringsNamespace() const;

    const std::string& getFile() const;

    const std::string& getFunction() const;

    int32_t getLine() const;

    const std::string& msg() const;

    int32_t getCode() const;

    const std::string& getStringifiedCode() const;

    const std::string getWarningId() const;

    template <class T>
    Warning& operator <<(const T &param)
    {
        try
        {
            _formatter % param;
        }
        catch (...)
        {
            // Silently ignore errors during adding parameters
        }

        format();

        return *this;
    }

protected:
    std::string getMessage() const;

    void format();

    std::string _file;
    std::string _function;
    int32_t _line;
    std::string _strings_namespace;
    int32_t _code;
    std::string _stringified_code;

    std::string _what_str;
    boost::format _formatter;
};

}
#endif /* WARNINGS_H_ */
