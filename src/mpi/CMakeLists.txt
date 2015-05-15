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

message(STATUS "****************** BEGIN src/mpi/CMakeLists.txt ******************")

if(SCALAPACK_TIMING)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DSCALAPACK_TIMING")
endif()

if(SCALAPACK_DEBUG)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DSCALAPACK_DEBUG")
endif()

######################
# MPI
######################
set(USE_LOCAL_MPI 1) # If set, will find MPICH2, while the current find module will have selected OpenMPI
if (USE_LOCAL_MPI)
   set(LOCAL_MPI_PATH "UNDEFINED_FOR_THIS_PLATFORM")
   set(MPICH2_SUFFIX "")

   if(${DISTRO_NAME_VER} MATCHES "Ubuntu")
       set(LOCAL_MPI_PATH "/opt/scidb/${SCIDB_VERSION_MAJOR}.${SCIDB_VERSION_MINOR}/3rdparty/mpich2")
       set(MPI_INCLUDE "-I ${LOCAL_MPI_PATH}/include/mpich2")
       set(MPICH2_SUFFIX ".mpich2")
   endif()

   if(${DISTRO_NAME_VER} MATCHES "CentOS-6")
       set(LOCAL_MPI_PATH "/opt/scidb/${SCIDB_VERSION_MAJOR}.${SCIDB_VERSION_MINOR}/3rdparty/mpich2")
       set(MPI_INCLUDE "-I ${LOCAL_MPI_PATH}/include/mpich2")
   endif()

   message(STATUS "WARNING-- using locally installed MPICH2")
   message(STATUS "Local MPICH2 path: ${LOCAL_MPI_PATH}")

   if (NOT ${LOCAL_MPI_PATH} MATCHES "UNDEFINED_FOR_THIS_PLATFORM")
       set(MPI_Fortran_COMPILER "${LOCAL_MPI_PATH}/bin/mpif77${MPICH2_SUFFIX}")
       set(MPI_CXX_COMPILER "${LOCAL_MPI_PATH}/bin/mpic++${MPICH2_SUFFIX}")
       set(MPI_CC_COMPILER "${LOCAL_MPI_PATH}/bin/mpicc${MPICH2_SUFFIX}")
       set(MPI_LIBRARIES "${LOCAL_MPI_PATH}/lib/libmpichf90.a") # fill in the blanks
       set(MPI_Fortran_FOUND 1) # emulate find_package()
       set(MPI_CXX_FOUND 1)     # emulate find_package()
       set(MPI_CC_FOUND 1)      # emulate find_package()
       set(MPICH_FOUND 1) # we have MPICH, need custom ScaLAPACK
   endif()
else (USE_LOCAL_MPI)
   set(MPI_Fortran_COMPILER mpif77) # must be before find_package to be path-resolved
   find_package(MPI)
   MESSAGE(STATUS "MPI_Fortran_COMPILER=${MPI_Fortran_COMPILER}")
endif(USE_LOCAL_MPI)

set(USE_FIND "true") # assume not using MKL


if(USE_FIND)
    set(LIBGF "/usr/lib/gcc/x86_64-linux-gnu/4.6/libgfortran.a")

    ###########################
    # ScaLAPACK
    ###########################
    set(USE_LOCAL_SCALAPACK 1) # set 1 to build on a scalapack built by build_scalapack_mpich2.sh, requires USE_LOCAL_MPI settings temporarily
    if(USE_LOCAL_SCALAPACK)
        message(STATUS "WARNING-- using locally installed ScaLAPACK for MPICH2")
        message(STATUS "Debug: To refresh/rebuild the local copy of scalapack, remove ${CMAKE_CURRENT_BINARY_DIR}/local_scalapack/container.")
        add_subdirectory("local_scalapack")
        # TODO: can these next two be "returned" from local_scalapack/CMakeLists.txt?
        set(SCALAPACK_BUILD_DIR "${CMAKE_CURRENT_BINARY_DIR}/local_scalapack/container/src/scalapackForMPICH2-build")
        set(SCALAPACK_LIB_DIR   "${SCALAPACK_BUILD_DIR}/install/lib")
        set(SCALAPACK_LIBRARIES "${SCALAPACK_LIB_DIR}/libscalapack.a") # -ltmg ${LIBGF}
        set(SCALAPACK_INC_DIR   "${SCALAPACK_LIB_DIR}/include")   #UNSURE
        set(SCALAPACK_FOUND 1)   # emulate find_package()
    else()
        message(STATUS "Using platform ScaLAPACK stack")
        find_package(ScaLAPACK)
    endif()

    if (SCALAPACK_FOUND)
        message(STATUS "Debug: SCALAPACK_LIBRARIES is ${SCALAPACK_LIBRARIES}")
        message(STATUS "Debug: SCALAPACK_INC_DIR is ${SCALAPACK_INC_DIR}")
    else (SCALAPACK_FOUND)
        message(WARNING "ScaLAPACK not found. ScaLAPACK-based functionality will NOT be built.")
        message(WARNING "Ubuntu Hint: $ sudo apt-get install libscalapack-mpi1")
    endif (SCALAPACK_FOUND)
endif(USE_FIND)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DOMPI_IGNORE_CXX_SEEK")

if(NOT MPI_CXX_FOUND OR NOT MPI_Fortran_FOUND)
    message(WARNING "MPI not present. ScaLAPACK-based functionality will NOT be built")
    message(WARNING "Debug: MPI_C_FOUND is ${MPI_C_FOUND}, MPI_CXX_FOUND is ${MPI_CXX_FOUND}, MPI_Fortran_FOUND is ${MPI_Fortran_FOUND}")
    message(WARNING "Ubuntu Hint: $ sudo apt-get install libopenmpi-dev")
else(NOT MPI_CXX_FOUND OR NOT MPI_Fortran_FOUND)
    if(${DISTRO_NAME_VER} MATCHES "CentOS")
        set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -lgfortran")
    endif(${DISTRO_NAME_VER} MATCHES "CentOS")
endif(NOT MPI_CXX_FOUND OR NOT MPI_Fortran_FOUND)

message(STATUS "Debug: FINAL PACKAGE SETTINGS src/mpi ================================>")
message(STATUS "Debug: SCALAPACK_LIBRARIES is ${SCALAPACK_LIBRARIES}")
message(STATUS "Debug: LAPACK_LIBRARIES is ${LAPACK_LIBRARIES}")
message(STATUS "Debug: BLAS_LIBRARIES is ${BLAS_LIBRARIES}")
message(STATUS "Debug: MPI_LIBRARIES is ${MPI_LIBRARIES}")
message(STATUS "Debug: MPI_INCLUDE is ${MPI_INCLUDE}")
message(STATUS "Debug: MPI_Fortran_COMPILER is ${MPI_Fortran_COMPILER}")
message(STATUS "Debug: CMAKE_C_FLAGS is ${CMAKE_C_FLAGS}")
message(STATUS "Debug: CMAKE_CXX_FLAGS is ${CMAKE_CXX_FLAGS}")
message(STATUS "Debug: CMAKE_Fortran_FLAGS is ${CMAKE_Fortran_FLAGS}")
message(STATUS "Debug: CMAKE_Fortran_COMPLIER is ${CMAKE_Fortran_COMPILER}")
message(STATUS "Debug: CMAKE_CXX_COMPILER is ${CMAKE_CXX_COMPILER}")
message(STATUS "Debug: FINAL PACKAGE SETTINGS <================================")

if (SCALAPACK_FOUND AND LAPACK_FOUND AND BLAS_FOUND AND
    MPI_CXX_FOUND AND MPI_Fortran_FOUND)

    message(STATUS "Debug: Building MPI Slave")

    #
    # build the common mpi framework code
    #
    set (mpi_common_src
        MPIUtils.cpp
    )
    add_library(mpi_common STATIC ${mpi_common_src})
    target_link_libraries(mpi_common util_lib)

    #
    # add the mpi test operator and scalapack-based DLA operators
    # to the mpi libraray
    #
    set (mpi_src
        MPIManager.cpp
        MPILauncher.cpp
        MPISlaveProxy.cpp
        MPIInitLogical.cpp
        MPIInitPhysical.cpp
        MPIPhysical.cpp
        test/MPITestLogical.cpp
        test/MPITestPhysical.cpp
    )

    #
    # build the mpi-slave program
    #
    add_subdirectory("slaving")
    if(USE_LOCAL_SCALAPACK)
       add_dependencies(mpi_slave_direct scalapackForMPICH2)
       add_dependencies(mpi_slave_scidb scalapackForMPICH2)
    endif()

else (SCALAPACK_FOUND AND LAPACK_FOUND AND BLAS_FOUND AND
      MPI_CXX_FOUND AND MPI_Fortran_FOUND)

    message(WARNING "Can't build MPI/ScaLAPACK functionality")
    message(WARNING "because one of the following is missing:")
    message(WARNING "BLAS_FOUND        is ${BLAS_FOUND}")
    message(WARNING "LAPACK_FOUND      is ${LAPACK_FOUND}")
    message(WARNING "SCALAPACK_FOUND   is ${SCALAPACK_FOUND}")
    message(WARNING "MPI_CXX_FOUND     is ${MPI_CXX_FOUND}")
    message(WARNING "MPI_Fortran_FOUND is ${MPI_Fortran_FOUND}")
    message(WARNING "")

     set (mpi_src
          pluginNoMpi.cpp
     )

    message(WARNING "Can't build ScaLAPACK extensions")
    message(WARNING "without ScaLAPACK, MPI, LAPACK, and BLAS")
    message(WARNING "This is only a warning, SciDB will build but without")
    message(WARNING "the libdense_linear_algebra.so plugin.")
    message(WARNING "If that is not desired, please make sure the following are correctly resolved:")
    message(WARNING "----------------------------------------------------")
    message(WARNING "Debug: MPI_Fortran_COMPLIER is ${MPI_Fortran_COMPILER}")
    message(WARNING "Debug: CMAKE_Fortran_COMPLIER is ${CMAKE_Fortran_COMPILER}")
    message(WARNING "Debug: CMAKE_CXX_COMPILER is ${CMAKE_CXX_COMPILER}")
    message(WARNING "Debug: SCALAPACK_FOUND is ${SCALAPACK_FOUND}")
    message(WARNING "Debug: LAPACK_FOUND is    ${LAPACK_FOUND}")
    message(WARNING "Debug: BLAS_FOUND is      ${BLAS_FOUND}")
    message(WARNING "Debug: MPI_CXX_FOUND is   ${MPI_CXX_FOUND}")
    message(WARNING "Debug: MPI_Fortran_FOUND is ${MPI_Fortran_FOUND}")
    message(WARNING "----------------------------------------------------")

endif (SCALAPACK_FOUND AND LAPACK_FOUND AND BLAS_FOUND AND
       MPI_CXX_FOUND AND MPI_Fortran_FOUND)

#
# complete the linar_algebra library settings
#
add_library(mpi SHARED ${mpi_src})

if (SCALAPACK_FOUND AND LAPACK_FOUND AND BLAS_FOUND AND
      MPI_CXX_FOUND AND MPI_Fortran_FOUND)
    target_link_libraries(mpi mpi_common)
endif (SCALAPACK_FOUND AND LAPACK_FOUND AND BLAS_FOUND AND
      MPI_CXX_FOUND AND MPI_Fortran_FOUND)
extractDebugInfo("${GENERAL_OUTPUT_DIRECTORY}/plugins" "libmpi${CMAKE_SHARED_LIBRARY_SUFFIX}" mpi)
set_target_properties(mpi PROPERTIES LIBRARY_OUTPUT_DIRECTORY ${GENERAL_OUTPUT_DIRECTORY}/plugins)

message(STATUS "****************** END src/mpi/CMakeLists.txt ******************")
