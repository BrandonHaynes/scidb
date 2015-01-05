#ifndef REQUIRE_H_
#define REQUIRE_H_

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
 * @file require.h
 *
* @brief An assertion tool for unit tests
 *
 * @author James McQueston <jmcqueston@paradigm4.com>
 */

#define REQUIRE_START(name) { int requireErrs_(0); int requirePasses_(0);
#define REQUIRE(expr) require(expr, #expr, & requireErrs_, & requirePasses_, __LINE__, __FILE__)
#define REQUIRE_END(name) require_end( #name, requireErrs_, requirePasses_); }

namespace scidb {
    void require(bool expr, const char* sexpr, int* errs, int* passes, int line, const char * file);
    int  require_end(const char* name, int errs, int passes);
} // namespace scidb

#endif // REQUIRE_H
