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
 * @file ErrorCodes.h
 * @author Artyom Smirnov <smirnoffjr@gmail.com>
 * @author Mike Leibensperger <mjl@paradigm4.com>
 * @brief Built-in error codes
 */

#ifndef ERRORCODES_H_
#define ERRORCODES_H_

#include <stdint.h>

// ErrorsLibrary::registerErrors() checks at runtime that this
// boundary is respected.
#define SCIDB_MAX_SYSTEM_ERROR 0xFFFF
#define SCIDB_USER_ERROR_CODE_START (SCIDB_MAX_SYSTEM_ERROR+1)

namespace scidb
{
    // Many P4 tests rely on particular numeric error codes, so we
    // continue to use the numeric codes from the .inc files rather
    // than let the enum mechanism assign numbers.
    enum {
        #define X(_name, _code, _msg)           _name = _code ,
        #include "ShortErrors.inc"
        #include "LongErrors.inc"
        #undef X
        SCIDB_ERROR_COUNT       // Useless since it bears no
                                // relationship to the _codes, but it
                                // silences a compiler warning.
    };

} //namespace scidb

#endif /* ERRORCODES_H_ */
