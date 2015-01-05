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
#set(generator_src
#  benchGen.cpp
#)

#add_executable(gen ${generator_src})
#set_target_properties(gen PROPERTIES RUNTIME_OUTPUT_DIRECTORY ${SSDB_OUTPUT_DIRECTORY})
add_executable(ssdbgen benchGen.cpp)
extractDebugInfo("${GENERAL_OUTPUT_DIRECTORY}" "ssdbgen" ssdbgen)
set_target_properties(ssdbgen PROPERTIES RUNTIME_OUTPUT_DIRECTORY ${GENERAL_OUTPUT_DIRECTORY})

