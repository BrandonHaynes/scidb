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
# Finding cat executable.
#
# Once done this will define:
#	CAT_EXECUTABLE	- Full path to cat binary 

include(FindPackageHandleStandardArgs)

find_program(CAT_EXECUTABLE cat ${CAT_BIN_DIR})

find_package_handle_standard_args(CAT DEFAULT_MSG CAT_EXECUTABLE)

mark_as_advanced(CAT_EXECUTABLE)
