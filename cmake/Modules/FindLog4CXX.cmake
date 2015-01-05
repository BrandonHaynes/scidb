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
# Try to find log4cxx
#
# Once done this will define
#  LOG4CXX_FOUND       - TRUE if log4cxx found
#  LOG4CXX_INCLUDE_DIRS - Where to find log4cxx include sub-directory
#  LOG4CXX_LIBRARIES   - List of libraries when using log4cxx
#
#   Use -DLOG4CXX_USE_STATIC_LIBS=ON when cmake to enable static linkage of log4cxx
#

# check cache
IF(LOG4CXX_INCLUDE_DIRS)
  SET(LOG4CXX_FIND_QUIETLY TRUE)
ENDIF(LOG4CXX_INCLUDE_DIRS)

SET( _LOG4CXX_ORIG_CMAKE_FIND_LIBRARY_SUFFIXES ${CMAKE_FIND_LIBRARY_SUFFIXES})
IF( LOG4CXX_USE_STATIC_LIBS )
  SET(CMAKE_FIND_LIBRARY_SUFFIXES .a ${CMAKE_FIND_LIBRARY_SUFFIXES})
ENDIF( LOG4CXX_USE_STATIC_LIBS )


# find includes and libraries
FIND_PATH(LOG4CXX_INCLUDE_DIRS log4cxx/log4cxx.h)
FIND_LIBRARY(LOG4CXX_LIBRARY log4cxx)

IF( LOG4CXX_USE_STATIC_LIBS )
    message("find log4cxx: static")
    FIND_LIBRARY(APR_LIBRARY apr-1 /usr/local/apr/lib )
    SET(LOG4CXX_LIBRARY ${LOG4CXX_LIBRARY} ${APR_LIBRARY})
    FIND_LIBRARY(APRUTIL_LIBRARY aprutil-1 /usr/local/apr/lib )
    SET(LOG4CXX_LIBRARY ${LOG4CXX_LIBRARY} ${APRUTIL_LIBRARY})
    FIND_LIBRARY(EXPAT_LIBRARY expat /usr/local/apr/lib )
    SET(LOG4CXX_LIBRARY ${LOG4CXX_LIBRARY} ${EXPAT_LIBRARY})
    message("find log4cxx: static" ${LOG4CXX_LIBRARY})
ENDIF( LOG4CXX_USE_STATIC_LIBS )

SET( CMAKE_FIND_LIBRARY_SUFFIXES ${_LOG4CXX_ORIG_CMAKE_FIND_LIBRARY_SUFFIXES})

# standard handling
INCLUDE(FindPackageHandleStandardArgs)
FIND_PACKAGE_HANDLE_STANDARD_ARGS(Log4CXX DEFAULT_MSG LOG4CXX_LIBRARY LOG4CXX_INCLUDE_DIRS)

IF(LOG4CXX_FOUND)
  SET(LOG4CXX_LIBRARIES ${LOG4CXX_LIBRARY})
ELSE(LOG4CXX_FOUND)
  SET(LOG4CXX_LIBRARIES)
ENDIF(LOG4CXX_FOUND)

# mark cache
MARK_AS_ADVANCED(LOG4CXX_LIBRARIES LOG4CXX_INCLUDE_DIRS)
message(STATUS "LOG4CXX - ${LOG4CXX_LIBRARIES}")
