
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

#include <cstdlib>
#include <cassert>
#include <iostream>
#include "require.h"

namespace scidb
{
    void require(bool expr, const char* sexpr, int* errs, int* passes, int line, const char * file)
    {
        if (expr) {
            (*passes)++ ;
        } else {
            std::cerr << "REQUIRE: " << sexpr << " false at " << __FILE__ << ":" << __LINE__ << std::endl ;
            (*errs)++ ;
        }
    }

    int require_end(const char* name, int errs, int passes)
    {
        if (errs) {
            std::cout << name << " failed " << errs << " / " << errs + passes << " tests." << std::endl ;
            assert(errs == 0 ); // stop in a debug build
        } else {
            std::cout << name << " passed " << passes << " tests." << std::endl ;
        } 

        return errs ;
    }
}
