########################################
# BEGIN_COPYRIGHT
#
# This file is part of SciDB.
# Copyright (C) 2008-2014 SciDB, Inc.
#
# SciDB is free software: you can redistribute it and/or modify
# it under the terms of the AFFERO GNU General Public License as published by
# the Free Software Foundation.
#
# SciDB is distributed "AS-IS" AND WITHOUT ANY WARRANTY OF ANY KIND,
# INCLUDING ANY IMPLIED WARRANTY OF MERCHANTABILITY,
# NON-INFRINGEMENT, OR FITNESS FOR A PARTICULAR PURPOSE. See
# the AFFERO GNU General Public License for the complete license terms.
#
# You should have received a copy of the AFFERO GNU General Public License
# along with SciDB.  If not, see <http://www.gnu.org/licenses/agpl-3.0.html>
#
# END_COPYRIGHT
########################################

# shared library for user defined objects
set (misc_src
    misc.cpp
    ../../src/array/LogicalTestCache.cpp
    ../../src/array/PhysicalTestCache.cpp
    ../../src/array/UnitTestDeepChunkMergeLogical.cpp
    ../../src/array/UnitTestDeepChunkMergePhysical.cpp
    ../../src/array/UnitTestSortArrayLogical.cpp
    ../../src/array/UnitTestSortArrayPhysical.cpp
    ../../src/util/UnitTestFileIOLogical.cpp
    ../../src/util/UnitTestFileIOPhysical.cpp
    ../../src/util/UnitTestDataStoreLogical.cpp
    ../../src/util/UnitTestDataStorePhysical.cpp
    ../../src/array/UnitTestChunkLimitLogical.cpp
    ../../src/array/UnitTestChunkLimitPhysical.cpp
    ../../src/array/UnitTestMemArrayLogical.cpp
    ../../src/array/UnitTestMemArrayPhysical.cpp
)

file(GLOB misc_include "*.h")

add_library(misc SHARED ${misc_src} ${misc_include})
extractDebugInfo("${GENERAL_OUTPUT_DIRECTORY}/plugins" "libmisc${CMAKE_SHARED_LIBRARY_SUFFIX}" misc)
set_target_properties(misc PROPERTIES LIBRARY_OUTPUT_DIRECTORY ${GENERAL_OUTPUT_DIRECTORY}/plugins)
