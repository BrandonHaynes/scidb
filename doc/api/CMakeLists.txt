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

if(DOXYGEN_FOUND)

    set(DOXY_IN "${CMAKE_CURRENT_SOURCE_DIR}/Doxyfile.in")
    set(DOXY_CONFIG "${CMAKE_CURRENT_BINARY_DIR}/Doxyfile")

    set(CONFIGURE_DOXYGEN_GENERATE_HTML "YES")
    set(CONFIGURE_DOXYGEN_GENERATE_LATEX "NO")
    set(CONFIGURE_DOXYGEN_QUIET "NO")
    set(CONFIGURE_DOXYGEN_EXCLUDE_PATTERNS ".svn CMakeFiles *_unit_tests.h location.hh Scanner.cpp Scanner.fixed.cpp Parser.cpp Parser.hpp position.hh stack.hh scidb_msg.pb.h scidb_msg.pb.cc CatalogMetadata.h")

    if (SCIDB_DOC_TYPE STREQUAL "API")
        set(CONFIGURE_DOXYGEN_INPUT "${CMAKE_SOURCE_DIR}/include ${CMAKE_SOURCE_DIR}/doc/pages")
        set(CONFIGURE_DOXYGEN_STRIP_FROM_PATH "${CMAKE_SOURCE_DIR}/include")
    else (SCIDB_DOC_TYPE STREQUAL "API")
        set(CONFIGURE_DOXYGEN_INPUT "${CMAKE_SOURCE_DIR}/src ${CMAKE_SOURCE_DIR}/examples ${CMAKE_SOURCE_DIR}/include ${CMAKE_SOURCE_DIR}/doc/api/pages")
        set(CONFIGURE_DOXYGEN_STRIP_FROM_PATH "${CMAKE_SOURCE_DIR}")
    endif (SCIDB_DOC_TYPE STREQUAL "API")

    message(STATUS "Configuring ${DOXY_IN} as ${DOXY_CONFIG}")
    configure_file(${DOXY_IN} ${DOXY_CONFIG} @ONLY)

    if (SCIDB_DOC_TYPE STREQUAL "FULL" OR SCIDB_DOC_TYPE STREQUAL "API")
        add_custom_target(doc ALL ${DOXYGEN_EXECUTABLE} ${DOXY_CONFIG} DEPENDS ${DOXY_CONFIG})
        message(STATUS "${SCIDB_DOC_TYPE} documentation will be generated automatically. Use -DSCIDB_DOC_TYPE={NONE,FULL,API} to set type of generated documentation.")
    else (SCIDB_DOC_TYPE STREQUAL "FULL" OR SCIDB_DOC_TYPE STREQUAL "API")
        add_custom_target(doc ${DOXYGEN_EXECUTABLE} ${DOXY_CONFIG} DEPENDS ${DOXY_CONFIG})
        message(STATUS "Documentation will not be generated automatically. Use -DSCIDB_DOC_TYPE={NONE,FULL,API} to set type of generated documentation. Use make doc to generate FULL documentation manually.")
    endif (SCIDB_DOC_TYPE STREQUAL "FULL" OR SCIDB_DOC_TYPE STREQUAL "API")

    set_directory_properties(PROPERTIES ADDITIONAL_MAKE_CLEAN_FILES "html;latex")

else(DOXYGEN_FOUND)
    message(STATUS "Can not find doxygen. Documentation will not be generated.")
endif(DOXYGEN_FOUND)
