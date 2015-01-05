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

MACRO(COMPARE_VERSIONS_STRINGS ver_a_in ver_b_in result_out)
	STRING(REPLACE "." " " a ${ver_a_in})
	STRING(REPLACE "." " " b ${ver_b_in})
	SEPARATE_ARGUMENTS(a)
	SEPARATE_ARGUMENTS(b)

	LIST(LENGTH a a_length)
	LIST(LENGTH b b_length)

	IF(a_length LESS b_length)
		SET(shorter a)
		MATH(EXPR range "${b_length} - 1")
		MATH(EXPR pad_range "${b_length} - ${a_length} - 1")
	ELSE(a_length LESS b_length)
		SET(shorter b)
		MATH(EXPR range "${a_length} - 1")
		MATH(EXPR pad_range "${a_length} - ${b_length} - 1")
	ENDIF(a_length LESS b_length)

	IF(NOT pad_range LESS 0)
		FOREACH(pad RANGE ${pad_range})
			LIST(APPEND ${shorter} 0)
		ENDFOREACH(pad RANGE ${pad_range})
	ENDIF(NOT pad_range LESS 0)

	SET(result 0)
	FOREACH(index RANGE ${range})
		IF(result EQUAL 0)
			LIST(GET a ${index} a_version)
			LIST(GET b ${index} b_version)
			IF(a_version LESS b_version)
				SET(result -1)
			ENDIF(a_version LESS b_version)
			IF(a_version GREATER b_version)
				SET(result 1)
			ENDIF(a_version GREATER b_version)
		ENDIF(result EQUAL 0)
	ENDFOREACH(index)

	SET(${result_out} ${result})
ENDMACRO(COMPARE_VERSIONS_STRINGS)
