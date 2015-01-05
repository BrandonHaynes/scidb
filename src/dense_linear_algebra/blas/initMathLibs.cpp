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


// C
#include <stdlib.h>         // putenv

// C++

// scidb
#include <system/ErrorCodes.h>
#include <system/Exceptions.h>

// local
#include <dense_linear_algebra/blas/initMathLibs.h>

namespace scidb {


/**
 * This function is called before threading starts, in order that any environ variables
 * needed for proper configuration of math libraries can be added, and any other
 * issues that might be needed to make math libraries "happy"
 */
void earlyInitMathLibEnv() {

    int status = setenv("MKL_THREADING_LAYER", "SEQUENTIAL", 0);  // only supported mode for MKL at this time.
    if(status) {
        throw USER_EXCEPTION(SCIDB_SE_CONFIG, SCIDB_LE_CANNOT_MODIFY_ENVIRONMENT);
    }

}

// hack: libblas.so and liblapack.a are linked to scidb for use by plugins.
//       This is sufficient under CentOS 6.3
//       Under ubuntu 12.mumble (NOCHECKIN), the library dependencies themselves are
//       optimized away (they appear in the CMake link.txt, but do not appear in the elf
//       header of scidb)
//       so we take some references to well-known entry points in these libraries to avoid
//       that.

// these are not the correct C prototypes, nor are they necessary,
// because we do not actually call the routines.
extern "C" void dgemm_(); // an arbitrary, but common, double-precision BLAS symbol
extern "C" void dgels_(); // an arbitrary, but common double-precision LAPACK symbol
namespace {
    // initialized during static intialization
    void (* volatile tmp1)() = dgemm_ ;	// refer to BLAS library to preserve its linkage
    void (* volatile tmp2)() = dgels_ ;	// refer to LAPACK library to preserve its linkage
}


} // end namespace scidb
