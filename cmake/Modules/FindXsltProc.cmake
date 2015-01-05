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

#
# Finding xsltproc executable.
#
# Once done this will define:
#	XSLTPROC_EXECUTABLE	- Full path to xsltproc binary 

include(FindPackageHandleStandardArgs)

find_program(XSLTPROC_EXECUTABLE xsltproc ${XSLTPROC_BIN_DIR})

find_package_handle_standard_args(XSLTPROC DEFAULT_MSG XSLTPROC_EXECUTABLE)

mark_as_advanced(XSLTPROC_EXECUTABLE)
