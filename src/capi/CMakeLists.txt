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

# shared library for executing simple text queries
set(scidbclient_src
    SciDBRemote.cpp
    ../network/BaseConnection.cpp
    ../system/Exceptions.cpp
    ../array/Metadata.cpp
    ${CMAKE_BINARY_DIR}/src/system/Constants.cpp
    ../smgr/io/ArrayWriter.cpp
    ../smgr/io/TemplateParser.cpp
    ../array/Array.cpp
    ../array/MemChunk.cpp
    ../array/StreamArray.cpp
    ../query/TypeSystem.cpp
    ../query/FunctionLibrary.cpp
    ../util/PluginManager.cpp
    ../util/FileIO.cpp
    ../query/Statistics.cpp
    ../query/FunctionDescription.cpp
    ../system/Sysinfo.cpp
    ../util/Semaphore.cpp
    ../util/PluginObjects.cpp
    ../array/RLE.cpp
    ../system/ErrorsLibrary.cpp
    ../network/MessageUtils.cpp
    ../system/Warnings.cpp
    ../query/TileFunctions.cpp
    ../array/DeepChunkMerger.cpp
    ../array/Tile.cpp
    ../array/Coordinate.cpp
    ../util/CoordinatesMapper.cpp
)

file(GLOB scidbclient_include "include/*.h")

add_library(scidbclient SHARED ${scidbclient_src} ${scidbclient_include})
extractDebugInfo("${GENERAL_OUTPUT_DIRECTORY}" "libscidbclient${CMAKE_SHARED_LIBRARY_SUFFIX}" scidbclient)
set_target_properties(scidbclient PROPERTIES LIBRARY_OUTPUT_DIRECTORY ${GENERAL_OUTPUT_DIRECTORY})
set_target_properties(scidbclient PROPERTIES COMPILE_FLAGS "-DSCIDB_CLIENT")

    set_target_properties(scidbclient PROPERTIES LINK_FLAGS "-Wl,--no-undefined")
target_link_libraries(scidbclient scidb_msg_lib json_lib compression_lib util_lib
    ${LIBRT_LIBRARIES} ${Boost_LIBRARIES} ${LOG4CXX_LIBRARIES} ${CMAKE_THREAD_LIBS_INIT} ${CMAKE_DL_LIBS})

# RS: I hope double free will not happen again because of I removed linking protobuf to iquery
#add_dependencies(scidbclient ${CURRENT_SOURCE_DIR}/export.map)
#if(${DISTRO_NAME_VER} MATCHES "RedHat-[0-9][.][0-9]" OR ${DISTRO_NAME_VER} MATCHES "SUSE")
#    set_target_properties(scidbclient PROPERTIES LINK_FLAGS "-Wl,--version-script=export.map")
#endif()

if(NOT WITHOUT_PYTHON_BINDING)
	message (STATUS "SWIG 2 and Python found. Python module will built")

    if(CMAKE_SIZEOF_VOID_P EQUAL 8)
        set(SWSIZE SWIGWORDSIZE64)
    else(CMAKE_SIZEOF_VOID_P EQUAL 8)
        set(SWSIZE SWIGWORDSIZE32)
    endif(CMAKE_SIZEOF_VOID_P EQUAL 8)

    add_custom_command(OUTPUT scidbpython.cxx ${GENERAL_OUTPUT_DIRECTORY}/libscidbpython.py

        COMMAND ${SWIG_EXECUTABLE} -c++ -python -D${SWSIZE} -o ${CMAKE_CURRENT_BINARY_DIR}/scidbpython.cxx -outdir ${GENERAL_OUTPUT_DIRECTORY} -I${Boost_INCLUDE_DIRS} -I${CMAKE_SOURCE_DIR}/include -I${CMAKE_SOURCE_DIR}/src scidbpython.i
                DEPENDS scidbpython.i scidbclient
		COMMENT "Building SWIG source in ${SWSIZE} mode: See swigpython.out for errors and warning"
		WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
	)
        
    set(scidbpython_src ${CMAKE_CURRENT_BINARY_DIR}/scidbpython.cxx)
	include_directories(${PYTHON_INCLUDE_PATH})

	add_library(scidbpython SHARED ${scidbpython_src} ${scidbclient_include} )
        extractDebugInfo("${GENERAL_OUTPUT_DIRECTORY}" "_libscidbpython${CMAKE_SHARED_LIBRARY_SUFFIX}" scidbpython)

	if(NOT (${DISTRO_NAME_VER} MATCHES "RedHat-[0-9][.][0-9]" OR ${DISTRO_NAME_VER} MATCHES "SUSE"))
		target_link_libraries(scidbpython ${Boost_LIBRARIES} ${LOG4CXX_LIBRARIES})
	endif(NOT (${DISTRO_NAME_VER} MATCHES "RedHat-[0-9][.][0-9]" OR ${DISTRO_NAME_VER} MATCHES "SUSE"))

	target_link_libraries(scidbpython scidbclient)
        
	set_target_properties(scidbpython PROPERTIES
		LIBRARY_OUTPUT_DIRECTORY ${GENERAL_OUTPUT_DIRECTORY}
		PREFIX "_lib" OUTPUT_NAME "scidbpython"
	)


    configure_file( scidbapi.py.in  "${GENERAL_OUTPUT_DIRECTORY}/scidbapi.py")
    configure_file( pythonexamples/README  "${PYTHON_SAMPLE_DIRECTORY}/README" COPYONLY)
    configure_file( pythonexamples/sample.py.in  "${PYTHON_SAMPLE_DIRECTORY}/sample.py")
    configure_file( pythonexamples/sample2.py.in "${PYTHON_SAMPLE_DIRECTORY}/sample2.py")
    configure_file( pythonexamples/sample2.csv  "${PYTHON_SAMPLE_DIRECTORY}/sample2.csv" COPYONLY)
    configure_file( pythonexamples/simplearray.data  "${PYTHON_SAMPLE_DIRECTORY}/simplearray.data" COPYONLY)
    configure_file( pythonexamples/log4cxx.properties  "${PYTHON_SAMPLE_DIRECTORY}/log4cxx.properties" COPYONLY)
endif()


