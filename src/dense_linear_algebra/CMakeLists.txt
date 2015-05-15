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

message(STATUS "*****BEGIN dense_linear_algebra/CMakeLists.txt ***********************")

#
# The library in this directory is just for dense linear algebra
# based on scalapack.  The files form dlaScaLA should be moved up into
# here.  To protect us from constant re-writing of includes, we will
# add to the default include paths
#
include_directories("${CMAKE_SOURCE_DIR}/src/mpi")
include_directories("${CMAKE_SOURCE_DIR}/src/dense_linear_algebra")  # include our own root

# the above is necessary to keep compatibility with the use of code in here within the
# "mpi" directory owning the "only" slave, but needing it to be build-able from here too,
# because that is where it will be shortly.

# TODO: JHM .... rename scalapackUtil to scalapack
# TODO: JHM .... move all dlaScaLA files to this "dense_linear_algebra" directory

if(SCALAPACK_TIMING)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DSCALAPACK_TIMING")
endif()

if(SCALAPACK_DEBUG)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DSCALAPACK_DEBUG")
endif()

set(MPI_Fortran_COMPILER mpif77)
find_package(MPI)

if (USE_LOCAL_MPI)
  set(MPI_INCLUDE "-I. -I /home/james/packages/ompi/openmpi-1.4.5/build/ompi/include -I /home/james/packages/ompi/openmpi-1.4.5/build/opal/include")
else (USE_LOCAL_MPI)
  MESSAGE(STATUS "MPI_Fortran_COMPILER=${MPI_Fortran_COMPILER}")
endif(USE_LOCAL_MPI)

# shared library for user defined objects
#set (dense_linear_algebra_src
#    #OpsRegistrator.cpp
#)

#
# DLA is built, by default, using the platform-provided set of:
# ScaLAPACK, BLACS, Open MPI, LAPACK, AND BLAS
# (On most, but not all platforms, ScaLAPACK is a recent version that
#  contains BLACS internally, AND is not a separate package)
#
# We will call this list of 4-5 packages "the ScaLAPACK stack"
#

set(USE_PLATFORM_PACKAGES "true")

message(STATUS "Debug: BLAS_LIBRARIES is ${BLAS_LIBRARIES}")
message(STATUS "Debug: LAPACK_LIBRARIES is ${LAPACK_LIBRARIES}")
message(STATUS "Debug: SCALAPACK_LIBRARIES is ${SCALAPACK_LIBRARIES}")
message(STATUS "Debug: MPI_LIBRARIES is ${MPI_LIBRARIES}")
message(STATUS "Debug: CMAKE_C_FLAGS is ${CMAKE_C_FLAGS}")
message(STATUS "Debug: CMAKE_CXX_FLAGS is ${CMAKE_CXX_FLAGS}")
message(STATUS "Debug: CMAKE_Fortran_FLAGS is ${CMAKE_Fortran_FLAGS}")

if (MPI_CXX_FOUND AND MPI_Fortran_FOUND) # SCALAPACK_FOUND AND LAPACK_FOUND AND BLAS_FOUND AND
    message(STATUS "Debug: MPI_Fortran_COMPILER is ${MPI_Fortran_COMPILER}")

    #
    # build the common mpi framework code
    #
    # TODO: this will return
#    set (dense_linear_algebra_common_src mpi/MPIUtils.cpp
#                                         sharedMem/SharedMemoryIpc.cpp
#    )
#   add_library(dense_linear_algebra_common STATIC ${dense_linear_algebra_common_src})
#   set_target_properties(dense_linear_algebra_common PROPERTIES LIBRARY_OUTPUT_DIRECTORY ${GENERAL_OUTPUT_DIRECTORY}/plugins)

    # NOTE: library compiles with regular compilers, not MPI compilers
    # only the slave needs MPI compilation AND ScaLAPACK+MPI libraries
    

    #
    # dense_linear_algebra library
    #

    set (dense_linear_algebra_src ${dense_linear_algebra_src}
        plugin.cpp
        # scalapack emulation:
        dlaScaLA/scalapackEmulation/numroc.f       # called from Physical operators
        dlaScaLA/scalapackEmulation/descinit.f     # called from Physical operators
                dlaScaLA/scalapackEmulation/lsame.f     # compare two single letters!
            dlaScaLA/scalapackEmulation/pdelset.f       # pdelsetOp - phys op
            dlaScaLA/scalapackEmulation/pdelget.f       # pdelgetOp - phys op
                dlaScaLA/scalapackEmulation/infog2l.f   # local indices; referenced by elset/elget?
            dlaScaLA/scalapackEmulation/pxerbla.f  # error handler

        # BLACS emulation:
            dlaScaLA/scalapackEmulation/blacs_info_fake.c # blacs_grid_info() from Physical operators
        scalapackUtil/reformat.cpp
        scalapackUtil/test/MPICopyLogical.cpp
        scalapackUtil/test/MPICopyPhysical.cpp
        scalapackUtil/test/slaving/mpiCopyMaster.cpp
        scalapackUtil/test/MPIRankLogical.cpp
        scalapackUtil/test/MPIRankPhysical.cpp
        scalapackUtil/test/slaving/mpiRankMaster.cpp
        scalapackUtil/ScaLAPACKLogical.cpp
        scalapackUtil/ScaLAPACKPhysical.cpp
        dlaScaLA/GEMMLogical.cpp
        dlaScaLA/GEMMPhysical.cpp
        dlaScaLA/GEMMOptions.cpp
        dlaScaLA/SVDLogical.cpp
        dlaScaLA/SVDPhysical.cpp
        dlaScaLA/slaving/pdgemmMaster.cpp
        dlaScaLA/slaving/pdgesvdMaster.cpp
        dlaScaLA/slaving/pdgesvdMasterSlave.cpp
    )

    #
    # build the mpi-slave program  (not currently linked here, but a version will return here, "soon")
    #
    set (mpi_slave_src
        scalapackUtil/test/slaving/mpiCopySlave.cpp
        scalapackUtil/test/slaving/mpiRankSlave.cpp
        dlaScaLA/slaving/pdgemmSlave.cpp
        dlaScaLA/slaving/pdgesvdSlave.cpp
        dlaScaLA/slaving/pdgesvdMasterSlave.cpp
        dlaScaLA/slaving/slaveTools.cpp
        scalapackUtil/reformat.cpp
    )

    # Temporarily, src/mpi/slaving produces mpi_slave_scidb, which is the "everything" slave
#    add_executable(mpi_slave ${mpi_slave_src})
#    if(${DISTRO_NAME_VER} MATCHES "RedHat-[0-9][.][0-9]")
#     target_link_libraries(mpi_slave ${LIBGFORTRAN_LIBRARIES})
#   endif(${DISTRO_NAME_VER} MATCHES "RedHat-[0-9][.][0-9]")
#   target_link_libraries(mpi_slave ${MPI_LIBRARIES})
#   target_link_libraries(mpi_slave ${BLAS_LIBRARIES} ${LAPACK_LIBRARIES} ${SCALAPACK_LIBRARIES})
#   target_link_libraries(mpi_slave ${LOG4CXX_LIBRARIES} ${PROTOBUF_LIBRARY} ${LIBRT_LIBRARIES} ${Boost_LIBRARIES})
#   TODO: restore this directory to  have its own mpi-slave

#    extractDebugInfo("${GENERAL_OUTPUT_DIRECTORY}/plugins" "mpi_slave" mpi_slave)
#    set_target_properties(mpi_slave PROPERTIES RUNTIME_OUTPUT_DIRECTORY ${GENERAL_OUTPUT_DIRECTORY}/plugins)

#
# AND the tools needed to test gemm AND gesvd
#
configure_file(doSvd.sh "${GENERAL_OUTPUT_DIRECTORY}/doSvd.sh" COPYONLY)
configure_file(doSvdDirect.sh "${GENERAL_OUTPUT_DIRECTORY}/doSvdDirect.sh" COPYONLY)
configure_file(doSvdWithMetrics.sh "${GENERAL_OUTPUT_DIRECTORY}/doSvdWithMetrics.sh" COPYONLY)
configure_file(doSvdMetric.sh "${GENERAL_OUTPUT_DIRECTORY}/doSvdMetric.sh" COPYONLY)
configure_file(diag.sh "${GENERAL_OUTPUT_DIRECTORY}/diag.sh" COPYONLY)
configure_file(test/driver.py "${GENERAL_OUTPUT_DIRECTORY}/dla_driver.py" COPYONLY)

else (MPI_CXX_FOUND AND MPI_Fortran_FOUND) # SCALAPACK_FOUND AND LAPACK_FOUND AND BLAS_FOUND AND

     set (dense_linear_algebra_src ${dense_linear_algebra_src}
          pluginNoSP.cpp
     )

    message(WARNING "@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@")
    message(WARNING "Can't build ScaLAPACK extensions")
    message(WARNING "without ScaLAPACK, Open MPI, LAPACK, or a BLAS")
    message(WARNING "or without MPI_CXX")
    message(WARNING "or without MPI_Fortran interfaces.")
    message(WARNING "This is only a warning AND is not a problem unless")
    message(WARNING "you want the libdense_linear_algebra.so plugin to be built.")
    message(WARNING "")
    message(WARNING "If you intended to built it, the following will help you debug what is missing.")
    message(WARNING "-------------------------------------------------")
    message(WARNING "Debug: MPI_Fortran_COMPLIER is ${MPI_Fortran_COMPILER}")
    message(WARNING "Debug: CMAKE_Fortran_COMPLIER is ${CMAKE_Fortran_COMPILER}")
    message(WARNING "Debug: CMAKE_CXX_COMPILER is ${CMAKE_CXX_COMPILER}")
    message(WARNING "Debug: SCALAPACK_FOUND is ${SCALAPACK_FOUND}")
    message(WARNING "Debug: LAPACK_FOUND is    ${LAPACK_FOUND}")
    message(WARNING "Debug: BLAS_FOUND is      ${BLAS_FOUND}")
    message(WARNING "Debug: MPI_CXX_FOUND is   ${MPI_CXX_FOUND}")
    message(WARNING "Debug: MPI_Fortran_FOUND is ${MPI_Fortran_FOUND}")
    message(WARNING "----------------------------------------------------")

endif (MPI_CXX_FOUND AND MPI_Fortran_FOUND) # SCALAPACK_FOUND AND LAPACK_FOUND AND BLAS_FOUND AND


#
# complete the dense_linear_algebra plugin settings
#
add_library(dense_linear_algebra SHARED ${dense_linear_algebra_src})

if (MPI_CXX_FOUND AND MPI_Fortran_FOUND) # SCALAPACK_FOUND AND LAPACK_FOUND AND BLAS_FOUND AND

    # target_link_libraries(dense_linear_algebra dense_linear_algebra_common)
    target_link_libraries(dense_linear_algebra mpi)

endif (MPI_CXX_FOUND AND MPI_Fortran_FOUND) # SCALAPACK_FOUND AND LAPACK_FOUND AND BLAS_FOUND AND

extractDebugInfo("${GENERAL_OUTPUT_DIRECTORY}/plugins" "libdense_linear_algebra${CMAKE_SHARED_LIBRARY_SUFFIX}" dense_linear_algebra)
set_target_properties(dense_linear_algebra PROPERTIES LIBRARY_OUTPUT_DIRECTORY ${GENERAL_OUTPUT_DIRECTORY}/plugins)

message(STATUS "*****END linear_algebra/CMakeLists.txt ***********************")
