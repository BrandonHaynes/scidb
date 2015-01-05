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


if(BUILD_USER_DOC)
    configure_file(${CMAKE_CURRENT_SOURCE_DIR}/xml/custom.xsl.in ${CMAKE_CURRENT_BINARY_DIR}/xml/custom.xsl)
    configure_file(${CMAKE_CURRENT_SOURCE_DIR}/xml/scidb_ug.ent.in ${CMAKE_CURRENT_BINARY_DIR}/xml/scidb_ug.ent)
    file(GLOB USER_DOC_SRC
        "xml/*.xml"
	"p4examples/*")
    add_custom_command(OUTPUT ${CMAKE_CURRENT_BINARY_DIR}/pdf/scidb-userguide-${SCIDB_SHORT_VERSION}.pdf
        COMMAND ${XSLTPROC_EXECUTABLE} --xinclude --path ${CMAKE_CURRENT_BINARY_DIR}/xml -o ${CMAKE_CURRENT_BINARY_DIR}/scidb-userguide-${SCIDB_SHORT_VERSION}.fo ${CMAKE_CURRENT_BINARY_DIR}/xml/custom.xsl ${CMAKE_CURRENT_SOURCE_DIR}/xml/scidb_ug.xml
        COMMAND mkdir -p ${CMAKE_CURRENT_BINARY_DIR}/pdf
        COMMAND ${FOP_EXECUTABLE} -c fopconfig.xml -fo ${CMAKE_CURRENT_BINARY_DIR}/scidb-userguide-${SCIDB_SHORT_VERSION}.fo -pdf ${CMAKE_CURRENT_BINARY_DIR}/pdf/scidb-userguide-${SCIDB_SHORT_VERSION}.pdf
        DEPENDS ${USER_DOC_SRC}
	WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/xml/
    )
    add_custom_target(user_doc ALL DEPENDS ${CMAKE_CURRENT_BINARY_DIR}/pdf/scidb-userguide-${SCIDB_SHORT_VERSION}.pdf)
endif()
