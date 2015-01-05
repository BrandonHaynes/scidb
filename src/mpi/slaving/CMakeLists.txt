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
cmake_minimum_required(VERSION 2.8.5)

#
# The program in this directory is not just for mpi.
# Ideally, we would give the mpi directory its own test-slave program, but
# at the moment, SciDB can only manage one slave program.
# So for a while we will build it here, but include slave modules in it from elsewhere.
# Until that is cleaned up, we will extend the include searching to include the root
# directory of all such "elsewhere" slave modules.
# TODO: once dla-scalapack is organized and stable in SciDB, reverse this dependency.
#       and have the dla-scalapack slave include the mpi testing.  This is more legitimate
#       because that code already depends on the mpi directory tree, and the reverse is
#       not desireable (general thing: mpi depending on specific thing: dla-scalapack).
include_directories("${CMAKE_SOURCE_DIR}/src/dense_linear_algebra")


message(STATUS "****************** BEGIN mpi/slaving/CMakeLists.txt ******************")

if (SCALAPACK_FOUND AND LAPACK_FOUND AND BLAS_FOUND AND
    MPI_CXX_FOUND AND MPI_Fortran_FOUND)

    set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} ${MPI_INCLUDE}")
    set(CMAKE_CXX_COMPILER ${MPI_CXX_COMPILER})         # hack - we don't understand FindMPI yet
    set(CMAKE_Fortran_COMPILER ${MPI_Fortran_COMPILER}) # hack - we don't understand FindMPI yet
    set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -I ${SCALAPACK_INC_DIR}")

    message(STATUS "Debug: CMAKE_CXX_COMPLIER is ${CMAKE_CXX_COMPILER}")
    message(STATUS "Debug: CMAKE_Fortran_COMPLIER is ${CMAKE_Fortran_COMPILER}")
    message(STATUS "Debug: CMAKE_C_FLAGS is ${CMAKE_C_FLAGS}")
    message(STATUS "Debug: SCALAPACK_BUILD_DIR  is ${SCALAPACK_BUILD_DIR}")
    message(STATUS "Debug: SCALAPACK_INC_DIR  is ${SCALAPACK_INC_DIR}")

    #
    # build the mpi-slave program
    #
    set (mpi_slave_common_src
    #    NOTE: the following items are to be moved back into their
    #    own slave in the dla area
         ../../dense_linear_algebra/scalapackUtil/test/slaving/mpiCopySlave.cpp
         ../../dense_linear_algebra/scalapackUtil/test/slaving/mpiRankSlave.cpp
         ../../dense_linear_algebra/dlaScaLA/slaving/pdgemmSlave.cpp
         ../../dense_linear_algebra/dlaScaLA/slaving/pdgesvdSlave.cpp
         ../../dense_linear_algebra/dlaScaLA/slaving/pdgesvdMasterSlave.cpp
         ../../dense_linear_algebra/dlaScaLA/slaving/slaveTools.cpp
         ../../dense_linear_algebra/scalapackUtil/reformat.cpp
    )

    add_library(mpi_slave_common STATIC ${mpi_slave_common_src})
    target_link_libraries(mpi_slave_common  ${SCALAPACK_LIBRARIES}
                                            ${MPI_LIBRARIES}
                                            ${LAPACK_LIBRARIES}
                                            ${BLAS_LIBRARIES}
                                            mpi_common)

    #
    # first one is a stripped-down version that is a pure mpi/scalapack program
    # used as a unit test that avoids all scidb-isms, useful for 1st-level
    # divide-and-conquer if slave problems occur when driven by scidb
    #
    add_executable(mpi_slave_direct mpi_slave_direct.cpp ../../dense_linear_algebra/blas/initMathLibs.cpp)
    add_executable(mpi_slave_scidb  mpi_slave_scidb.cpp  ../../dense_linear_algebra/blas/initMathLibs.cpp)

    # Temporary until separate dla-scalapack slave
    target_link_libraries(mpi_slave_direct mpi_slave_common
                                           scidbclient
                                           scidb_msg_lib)

    target_link_libraries(mpi_slave_scidb   mpi_slave_common
                                            scidbclient
                                            scidb_msg_lib
                                            ${LOG4CXX_LIBRARIES}
                                            ${PROTOBUF_LIBRARY}
                                            ${LIBRT_LIBRARIES}
                                            ${Boost_LIBRARIES})

    extractDebugInfo("${GENERAL_OUTPUT_DIRECTORY}/plugins" "mpi_slave_direct" mpi_slave_direct)
    extractDebugInfo("${GENERAL_OUTPUT_DIRECTORY}/plugins" "mpi_slave_scidb" mpi_slave_scidb)
    set_target_properties(mpi_slave_direct PROPERTIES RUNTIME_OUTPUT_DIRECTORY ${GENERAL_OUTPUT_DIRECTORY}/plugins)
    set_target_properties(mpi_slave_scidb PROPERTIES RUNTIME_OUTPUT_DIRECTORY ${GENERAL_OUTPUT_DIRECTORY}/plugins)

endif (SCALAPACK_FOUND AND LAPACK_FOUND AND BLAS_FOUND AND
       MPI_CXX_FOUND AND MPI_Fortran_FOUND)

message(STATUS "****************** END mpi/slaving/CMakeLists.txt ******************")
