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
# Finding fop executable.
#
# Once done this will define:
#	FOP_EXECUTABLE	- Full path to fop binary 

include(FindPackageHandleStandardArgs)

find_program(FOP_EXECUTABLE fop ${FOP_BIN_DIR})

find_package_handle_standard_args(FOP DEFAULT_MSG FOP_EXECUTABLE)

mark_as_advanced(FOP_EXECUTABLE)
