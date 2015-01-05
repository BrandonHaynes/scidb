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

# shared library for user defined objects
set (group_stars_src
    LogicalGroupStars.cpp
    PhysicalGroupStars.cpp
    cook.cpp
    cookgroup.cpp
    plugin.cpp
    grouper.cpp
    mysqlwrapper.cpp
)

add_library(groupstars SHARED ${group_stars_src})
set_target_properties(groupstars PROPERTIES LIBRARY_OUTPUT_DIRECTORY ${GENERAL_OUTPUT_DIRECTORY}/plugins)
extractDebugInfo("${GENERAL_OUTPUT_DIRECTORY}/plugins" "libgroupstars${CMAKE_SHARED_LIBRARY_SUFFIX}" groupstars)
if (APPLE)
  set_target_properties(groupstars PROPERTIES LINK_FLAGS "-undefined dynamic_lookup")
endif (APPLE)

