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

FIND_PROGRAM(FLEX_EXECUTABLE flex ${FLEX_BIN_DIR})
FIND_LIBRARY(FLEX_LIBRARY NAMES fl PATHS /usr/lib ${FLEX_LIB_DIR})
FIND_PATH(FLEX_INCLUDE_DIRS FlexLexer.h ${FLEX_INCLUDE_DIR})
FIND_PACKAGE_HANDLE_STANDARD_ARGS(FLEX DEFAULT_MSG FLEX_EXECUTABLE FLEX_LIBRARY FLEX_INCLUDE_DIRS)
SET(FLEX_LIBRARIES ${FLEX_LIBRARY})
MARK_AS_ADVANCED(FLEX_EXECUTABLE FLEX_LIBRARY FLEX_INCLUDE_DIRS)

EXECUTE_PROCESS(COMMAND ${FLEX_EXECUTABLE} --version
	OUTPUT_VARIABLE FLEX_VERSION
	RESULT_VARIABLE FLEX_EXEC_RES
	OUTPUT_STRIP_TRAILING_WHITESPACE)

STRING(COMPARE NOTEQUAL ${FLEX_EXEC_RES} 0 FLEX_EXEC_RES)
IF(${FLEX_EXEC_RES})
	MESSAGE(FATAL_ERROR "Can not invoke FLEX ${FLEX_EXECUTABLE}. Check binary!")
ENDIF(${FLEX_EXEC_RES})

STRING(REGEX REPLACE "^flex (.*)$" "\\1"
	FLEX_VERSION "${FLEX_VERSION}")

IF(FLEX_MINIMAL_VERSION)
	COMPARE_VERSIONS_STRINGS(${FLEX_VERSION} ${FLEX_MINIMAL_VERSION} RES)

	IF(${RES} LESS 0)
		MESSAGE(FATAL_ERROR "Need at least FLEX ${FLEX_MINIMAL_VERSION} but found ${FLEX_VERSION}")
	ENDIF(${RES} LESS 0)
ENDIF(FLEX_MINIMAL_VERSION)

MACRO(FLEX TargetName Input Output)
    SET(FLEX_usage "FLEX(<TargetName> <Input> <Output> [FLAGS <string>]")
    IF(${ARGC} GREATER 3)
        IF(${ARGC} EQUAL 5)
            IF("${ARGV3}" STREQUAL "FLAGS")
                SET(FLEX_EXECUTABLE_opts  "${ARGV4}")
                SEPARATE_ARGUMENTS(FLEX_EXECUTABLE_opts)
            ELSE("${ARGV3}" STREQUAL "FLAGS")
                MESSAGE(SEND_ERROR ${FLEX_usage})
            ENDIF("${ARGV3}" STREQUAL "FLAGS")
        ELSE(${ARGC} EQUAL 5)
            MESSAGE(SEND_ERROR ${FLEX_usage})
        ENDIF(${ARGC} EQUAL 5)
    ENDIF(${ARGC} GREATER 3)

    ADD_CUSTOM_COMMAND(
        OUTPUT ${Output}
        COMMAND ${FLEX_EXECUTABLE} ${FLEX_EXECUTABLE_opts} -o${Output} ${Input}
        DEPENDS ${Input}
        COMMENT "Building FLEX target ${TargetName} (${Output})"
        WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
    )

    SET(FLEX_${TargetName}_OUTPUTS ${Output})
ENDMACRO(FLEX)

MACRO(ADD_FLEX_BISON_DEPENDENCY FlexTarget BisonTarget)
    IF(NOT FLEX_${FlexTarget}_OUTPUTS)
        MESSAGE(SEND_ERROR "Flex target `${FlexTarget}' does not exists.")
    ENDIF(NOT FLEX_${FlexTarget}_OUTPUTS)

    IF(NOT BISON_${BisonTarget}_OUTPUT_HEADER)
        MESSAGE(SEND_ERROR "Bison target `${BisonTarget}' does not exists.")
    ENDIF(NOT BISON_${BisonTarget}_OUTPUT_HEADER)

    SET_SOURCE_FILES_PROPERTIES(${FLEX_${FlexTarget}_OUTPUTS}
        PROPERTIES OBJECT_DEPENDS ${BISON_${BisonTarget}_OUTPUT_HEADER})
ENDMACRO(ADD_FLEX_BISON_DEPENDENCY)
