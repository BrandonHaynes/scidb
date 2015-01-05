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

##############################################################
# This module detects variables used in packages:
# ${DISTRO_NAME}
# ${DISTRO_VER}
# ${DISTRO_NAME_VER}
##############################################################

if(${CMAKE_SYSTEM_NAME} MATCHES "Linux")
    if(EXISTS /etc/issue)
        file(READ "/etc/issue" ISSUE_STRING)
        string(REGEX MATCH "^(Red Hat|[a-zA-Z]*|Fedora|CentOS) ([\\.0-9]*).*" ISSUE_STRING ${ISSUE_STRING})

        set(DISTRO_NAME ${CMAKE_MATCH_1})
        set(DISTRO_VER ${CMAKE_MATCH_2})
        if(${DISTRO_NAME} STREQUAL "Red Hat")
            set(DISTRO_NAME "RedHat")
            string(REGEX MATCH "([0-9][.][0-9])" ISSUE_STRING ${ISSUE_STRING})
            set(DISTRO_VER ${CMAKE_MATCH_1})
        endif()
        if(${DISTRO_NAME} STREQUAL "CentOS")
            string(REGEX MATCH "([0-9][.][0-9])" ISSUE_STRING ${ISSUE_STRING})
            set(DISTRO_VER ${CMAKE_MATCH_1})
        endif()
        if(${DISTRO_NAME} STREQUAL "Fedora")
            string(REGEX MATCH "([12][0-9])" ISSUE_STRING ${ISSUE_STRING})
            set(DISTRO_VER ${CMAKE_MATCH_1})
        endif()
	if(NOT ${DISTRO_NAME} STREQUAL "")
	    set(DISTRO_NAME_VER "${DISTRO_NAME}")
	endif()
	if(NOT ${DISTRO_VER} STREQUAL "")
	    set(DISTRO_NAME_VER "${DISTRO_NAME_VER}-${DISTRO_VER}")
	endif()

        set(CPACK_GENERATOR TGZ)
        if(ISSUE_STRING MATCHES "Ubuntu" OR ISSUE_STRING MATCHES "Debian")
            set(CPACK_GENERATOR DEB)
        endif()
        if(ISSUE_STRING MATCHES "SUSE" OR DISTRO_NAME STREQUAL "Fedora" OR DISTRO_NAME STREQUAL "RedHat" OR DISTRO_NAME STREQUAL "CentOS")
            set(CPACK_GENERATOR RPM)
        endif()
    endif()
endif()

if(NOT ${DISTRO_NAME_VER} STREQUAL "")
    set(DISTRO_NAME_VER "${DISTRO_NAME}-${DISTRO_VER}")
else()
    set(DISTRO_NAME_VER "${CMAKE_SYSTEM_NAME}")
endif()
