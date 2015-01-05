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
include(ExternalProject)

ExternalProject_Add(shim
  PREFIX "github"
  GIT_REPOSITORY https://github.com/Paradigm4/shim.git
  UPDATE_COMMAND ""
  PATCH_COMMAND ${CMAKE_CURRENT_SOURCE_DIR}/patch_command ${BOOST_ROOT} ${CMAKE_SOURCE_DIR} ${CMAKE_BINARY_DIR} ${SCIDB_SHORT_VERSION}
  CONFIGURE_COMMAND ""
  BUILD_COMMAND ${CMAKE_CURRENT_SOURCE_DIR}/build_command ${SCIDB_SHORT_VERSION}
  INSTALL_COMMAND ${CMAKE_CURRENT_SOURCE_DIR}/install_command ${CMAKE_BINARY_DIR}
)

add_dependencies(shim scidbclient)

add_custom_target(shimsvc ALL)
# For run.py tests
#install(PROGRAMS shimsvc DESTINATION ${CMAKE_INSTALL_PREFIX}/bin)
# For /opt/scidb/...
#install(PROGRAMS shimsvc DESTINATION ${GENERAL_OUTPUT_DIRECTORY})
configure_file(shimsvc "${GENERAL_OUTPUT_DIRECTORY}/shimsvc" COPYONLY)
