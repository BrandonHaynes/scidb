/*
 **
 * BEGIN_COPYRIGHT
 *
 * This file is part of SciDB.
 * Copyright (C) 2014-2014 SciDB, Inc.
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
 * @file isnumber.cpp
 * @author Mike Leibensperger <mjl@paradigm4.com>
 *
 * @brief Recognize integers, floating point numbers, and nan in strings.
 */

#include <regex.h>
#include <string.h>
#include <sstream>
#include <stdexcept>

using namespace std;

namespace scidb {

/*
 * Match an integer, floating point number, or "nan".
 * NOTE: leading and trailing whitespace is ignored.
 */
bool isnumber(const char* val)
{
    static bool mustCompile = true;
    static regex_t numberRegex;

    if (mustCompile) {
        mustCompile = false;
        int rc = ::regcomp(
            &numberRegex,
            "^[ \t]*([-+]?[0-9]+(\\.[0-9]+([eE][-+]?[0-9/]+)?)?|nan)[ \t]*$",
            REG_NOSUB|REG_NEWLINE|REG_ICASE|REG_EXTENDED);
        if (rc) {
            stringstream ss;
            ss << "isnumber: regex compilation failed: " << ::strerror(rc);
            throw runtime_error(ss.str());
        }
    }

    return 0 == ::regexec(&numberRegex, val, 0, 0, 0);
}

} // namespace
