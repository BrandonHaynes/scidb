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

# Wrapper around broken on RedHat FindMPI.cmake
set(CMAKE_SYSTEM_PREFIX_PATH_BACKUP ${CMAKE_SYSTEM_PREFIX_PATH})

if(${DISTRO_NAME_VER} MATCHES "RedHat-5")
  # RedHat 5.x
  set(CMAKE_SYSTEM_PREFIX_PATH ${CMAKE_SYSTEM_PREFIX_PATH} "/usr/lib64/openmpi/1.4-gcc")
endif()

if(${DISTRO_NAME_VER} MATCHES "RedHat-6")
  # RedHat 6.x
  set(CMAKE_SYSTEM_PREFIX_PATH ${CMAKE_SYSTEM_PREFIX_PATH} "/usr/lib64/openmpi")
endif()

if(${DISTRO_NAME_VER} MATCHES "CentOS-6")
  # CentOS 6.x
  set(CMAKE_SYSTEM_PREFIX_PATH ${CMAKE_SYSTEM_PREFIX_PATH} "/opt/scidb/${SCIDB_VERSION_MAJOR}.${SCIDB_VERSION_MINOR}/3rdparty/mpich2")
endif()

if(${DISTRO_NAME_VER} MATCHES "Ubuntu")
  # Ubuntu 12.04
  set(CMAKE_SYSTEM_PREFIX_PATH ${CMAKE_SYSTEM_PREFIX_PATH} "/opt/scidb/${SCIDB_VERSION_MAJOR}.${SCIDB_VERSION_MINOR}/3rdparty/mpich2")
endif()

if(${DISTRO_NAME_VER} MATCHES "Ubuntu-14")
  # Ubuntu 14.04
  set(CMAKE_SYSTEM_PREFIX_PATH ${CMAKE_SYSTEM_PREFIX_PATH} "/opt/scidb/${SCIDB_VERSION_MAJOR}.${SCIDB_VERSION_MINOR}/3rdparty/mpich2")
endif()

if(${DISTRO_NAME_VER} MATCHES "Fedora")
  # Fedora 11/16/17
  set(CMAKE_SYSTEM_PREFIX_PATH ${CMAKE_SYSTEM_PREFIX_PATH} "/usr/lib64/openmpi")
endif()

if(NOT DISABLE_SCALAPACK)
  include(${CMAKE_ROOT}/Modules/FindMPI.cmake)
endif(NOT DISABLE_SCALAPACK)

set(CMAKE_SYSTEM_PREFIX_PATH ${CMAKE_SYSTEM_PREFIX_PATH_BACKUP})
