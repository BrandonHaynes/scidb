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

if(CPPUNIT_FOUND)
    add_executable(unit_tests unit_tests.cpp)
    target_link_libraries(unit_tests ${CPPUNIT_LIBRARIES})
    target_link_libraries(unit_tests pqxx) 
    target_link_libraries(unit_tests catalog_lib)
    target_link_libraries(unit_tests util_lib)
    target_link_libraries(unit_tests qproc_lib)
    target_link_libraries(unit_tests util_lib)
    target_link_libraries(unit_tests array_lib)
    target_link_libraries(unit_tests system_lib)
    target_link_libraries(unit_tests ${CMAKE_THREAD_LIBS_INIT} ${LIBRT_LIBRARIES} ${CMAKE_DL_LIBS})
else(CPPUNIT_FOUND)
	message(STATUS "Can not find cppunit library or headers. Unit tests will not build!")
endif(CPPUNIT_FOUND)
