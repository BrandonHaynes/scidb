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

INCLUDE(FindPackageHandleStandardArgs)
INCLUDE(CompareVersionsStrings)

FIND_PROGRAM(BISON_EXECUTABLE bison ${BISON_BIN_DIR})
FIND_PACKAGE_HANDLE_STANDARD_ARGS(BISON DEFAULT_MSG BISON_EXECUTABLE)
MARK_AS_ADVANCED(BISON_EXECUTABLE)

EXECUTE_PROCESS(COMMAND ${BISON_EXECUTABLE} --version
	OUTPUT_VARIABLE BISON_VERSION
	RESULT_VARIABLE BISON_EXEC_RES
	OUTPUT_STRIP_TRAILING_WHITESPACE)

	STRING(COMPARE NOTEQUAL ${BISON_EXEC_RES} 0 BISON_EXEC_RES)
	IF(${BISON_EXEC_RES})
		MESSAGE(FATAL_ERROR "Can not invoke BISON ${BISON_EXECUTABLE}. Check binary!")
	ENDIF(${BISON_EXEC_RES})

STRING(REGEX REPLACE "^bison \\(GNU Bison\\) ([^\n]+)\n.*" "\\1"
	BISON_VERSION "${BISON_VERSION}")

IF(BISON_MINIMAL_VERSION)
	COMPARE_VERSIONS_STRINGS(${BISON_VERSION} ${BISON_MINIMAL_VERSION} RES)

	IF(${RES} LESS 0)
		MESSAGE(FATAL_ERROR "Need at least BISON ${BISON_MINIMAL_VERSION} but found ${BISON_VERSION}")
	ENDIF(${RES} LESS 0)
ENDIF(BISON_MINIMAL_VERSION)

MACRO(BISON Name BisonInput BisonOutput)
    SET(BISON_usage "BISON(<TargetName> <Input> <Output> [FLAGS <string>]")
    SET(BISON_outputs "${BisonOutput}")

    GET_FILENAME_COMPONENT(BISON_output_path "${ARGV2}" PATH)
    GET_FILENAME_COMPONENT(BISON_output_name_we "${ARGV2}" NAME_WE)
    GET_FILENAME_COMPONENT(BISON_output_ext "${ARGV2}" EXT)

    LIST(APPEND BISON_other_outputs "${BISON_output_path}/position.hh")
    LIST(APPEND BISON_other_outputs "${BISON_output_path}/stack.hh")
    LIST(APPEND BISON_other_outputs "${BISON_output_path}/location.hh")
    LIST(APPEND BISON_other_outputs "${BISON_output_path}/${BISON_output_name_we}.output")

    IF(${ARGC} GREATER 3)
        IF(${ARGC} EQUAL 5)
            IF("${ARGV3}" STREQUAL "FLAGS")
                SET(BISON_EXECUTABLE_opts "${ARGV4}")
                SEPARATE_ARGUMENTS(BISON_EXECUTABLE_opts)
            ELSE("${ARGV3}" STREQUAL "FLAGS")
                MESSAGE(SEND_ERROR ${BISON_usage})
            ENDIF("${ARGV3}" STREQUAL "FLAGS")
        ELSE(${ARGC} EQUAL 5)
            MESSAGE(SEND_ERROR ${BISON_usage})
        ENDIF(${ARGC} EQUAL 5)
    ENDIF(${ARGC} GREATER 3)

    LIST(APPEND BISON_cmdopt "-d")
    STRING(REPLACE "c" "h" BISON_output_header_ext ${BISON_output_ext})
    SET(BISON_${Name}_OUTPUT_HEADER "${BISON_output_path}/${BISON_output_name_we}${BISON_output_header_ext}")
    LIST(APPEND BISON_outputs "${BISON_${Name}_OUTPUT_HEADER}")

    ADD_CUSTOM_COMMAND(
        OUTPUT ${BISON_outputs}
               ${BISON_other_outputs}
        COMMAND ${BISON_EXECUTABLE} ${BISON_cmdopt} ${BISON_EXECUTABLE_opts} -o ${ARGV2} ${ARGV1}
        DEPENDS ${ARGV1}
        COMMENT "Building BISON target ${Name} (${BisonOutput})"
        WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
    )
ENDMACRO(BISON)
