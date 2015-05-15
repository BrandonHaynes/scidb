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

message(STATUS "****************** BEGIN src/CMakeLists.txt ******************")
add_subdirectory("capi")
add_subdirectory("network")
add_subdirectory("system")
add_subdirectory("smgr")
add_subdirectory("util")
add_subdirectory("mpi")
add_subdirectory("dense_linear_algebra")
add_subdirectory("linear_algebra")
add_subdirectory("jdbc")

if (NOT WITHOUT_SERVER)
    add_subdirectory("array")
    add_subdirectory("query")

    # $include_src is not necessary. It is a convenience for the qtcreator and xcode IDEs
    file(GLOB include_src "../include/*.h" "../include/array/*.h" "../include/system/*.h"
            "../include/query/*.h" "../include/util/*.h")

    set(CMAKE_VERBOSE_MAKEFILE ON)
    add_executable(scidb network/entry.cpp dense_linear_algebra/blas/initMathLibs.cpp ${include_src})

    # removes the symbols from scidb and puts them in a separate file
    extractDebugInfo("${GENERAL_OUTPUT_DIRECTORY}" "scidb" scidb)
    set_target_properties(scidb PROPERTIES RUNTIME_OUTPUT_DIRECTORY ${GENERAL_OUTPUT_DIRECTORY})
    target_link_libraries(scidb network_lib util_lib system_lib qproc_lib array_lib
        ${LIBRT_LIBRARIES} ${CMAKE_THREAD_LIBS_INIT} ${CMAKE_DL_LIBS}
        ${BLAS_LIBRARIES} ${LAPACK_LIBRARIES})
endif()

message(STATUS "****************** END src/CMakeLists.txt ******************")
