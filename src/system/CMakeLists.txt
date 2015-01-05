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
if (NOT WITHOUT_SERVER)
    add_subdirectory("catalog")
endif()

# Prepare Constants.cpp
set(CONFIGURE_SCIDB_SRC_DIR "${CMAKE_SOURCE_DIR}/src/")
set(CONFIGURE_SCIDB_VERSION "${SCIDB_VERSION}")
set(CONFIGURE_SCIDB_BUILD_TYPE "${CMAKE_BUILD_TYPE}")
set(CONFIGURE_SCIDB_VERSION_MAJOR "${SCIDB_VERSION_MAJOR}")
set(CONFIGURE_SCIDB_VERSION_MINOR "${SCIDB_VERSION_MINOR}")
set(CONFIGURE_SCIDB_VERSION_PATCH "${SCIDB_VERSION_PATCH}")
set(CONFIGURE_SCIDB_VERSION_BUILD "${SCIDB_REVISION}")

if(${DISTRO_NAME_VER} MATCHES "CentOS-6")
  set(CONFIGURE_DEFAULT_MPI_DIR "/opt/scidb/${SCIDB_VERSION_MAJOR}.${SCIDB_VERSION_MINOR}/3rdparty/mpich2")
endif()
if(${DISTRO_NAME_VER} MATCHES "Ubuntu")
  set(CONFIGURE_DEFAULT_MPI_DIR "/opt/scidb/${SCIDB_VERSION_MAJOR}.${SCIDB_VERSION_MINOR}/3rdparty/mpich2")
endif()
set(CONFIGURE_DEFAULT_MPI_TYPE "mpich2-1.2")

set(CONSTANTS_CPP_IN "${CMAKE_CURRENT_SOURCE_DIR}/Constants.cpp.in")
set(CONSTANTS_CPP "${CMAKE_CURRENT_BINARY_DIR}/Constants.cpp")
message(STATUS "Configuring ${CONSTANTS_CPP_IN} as ${CONSTANTS_CPP}")
configure_file(${CONSTANTS_CPP_IN} ${CONSTANTS_CPP} @ONLY)

set(SYSTEM_H_IN "${CMAKE_CURRENT_SOURCE_DIR}/System.h.in")
set(SYSTEM_H "${CMAKE_CURRENT_BINARY_DIR}/System.h")
message(STATUS "Configuring ${SYSTEM_H_IN} as ${SYSTEM_H}")
configure_file(${SYSTEM_H_IN} ${SYSTEM_H})

set(system_src
    System.cpp
    Config.cpp
    Exceptions.cpp
    Cluster.cpp
    Sysinfo.cpp
    ${CONSTANTS_CPP}
    SciDBConfigOptions.cpp
    Resources.cpp
    Warnings.cpp
    ErrorsLibrary.cpp
    BlockCyclic.cpp
)

file(GLOB system_include "*.h")

add_library(system_lib STATIC ${system_src} ${system_include})
target_link_libraries(system_lib json_lib ${Boost_LIBRARIES} ${LOG4CXX_LIBRARIES})
add_dependencies(system_lib scidb_msg_lib)
