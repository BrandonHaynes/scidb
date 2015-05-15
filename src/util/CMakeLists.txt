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

set(SAMPLE_CONFIG_IN "${CMAKE_CURRENT_SOURCE_DIR}/scidb-sample.conf.in")
set(SAMPLE_CONFIG "${GENERAL_OUTPUT_DIRECTORY}/packaging_only/scidb-sample.conf")
message(STATUS "Configuring ${SAMPLE_CONFIG_IN} as ${SAMPLE_CONFIG}")
configure_file(${SAMPLE_CONFIG_IN} ${SAMPLE_CONFIG} @ONLY)

add_subdirectory("dmalloc")

set (util_src
    arena/Arena.cpp
    arena/ArenaCheckpoint.cpp
    arena/ArenaDetails.cpp
    arena/ArenaHeader.cpp
    arena/ArenaMonitor.cpp
    arena/ArenaOptions.cpp
    arena/RootArena.cpp
    arena/LimitedArena.cpp
    arena/ScopedArena.cpp
    arena/LeaArena.cpp
    arena/DebugArena.cpp
    arena/ThreadedArena.cpp
    isnumber.cpp
    CsvParser.cpp
    TsvParser.cpp
    Job.cpp
    JobQueue.cpp
    Semaphore.cpp
    Thread.cpp
    ThreadPool.cpp
    PluginManager.cpp
    FileIO.cpp
    PluginObjects.cpp
    require.cpp
    InjectedError.cpp
    BufferedFileInput.cpp
    shm/SharedMemoryIpc.cpp
    MultiConstIterators.cpp
    WorkQueue.cpp
    DataStore.cpp
    SpatialType.cpp
    CoordinatesMapper.cpp
    Counter.cpp
)

file(GLOB util_include "*.h")

configure_file(log4cxx.properties "${GENERAL_OUTPUT_DIRECTORY}/log4cxx.properties" COPYONLY)

find_package(Libcsv REQUIRED)
include_directories(${libcsv_INCLUDE_DIR})

add_library(util_lib STATIC ${util_src} ${util_include})
target_link_libraries(util_lib json_lib ${Boost_LIBRARIES} ${LOG4CXX_LIBRARIES} ${LIBRT_LIBRARIES} ${libcsv_LIBRARY})
