########################################
# BEGIN_COPYRIGHT
#
# PARADIGM4 INC.
# This file is part of the Paradigm4 Enterprise SciDB distribution kit
# and may only be used with a valid Paradigm4 contract and in accord
# with the terms and conditions specified by that contract.
#
# Copyright (C) 2010 - 2014 Paradigm4 Inc.
# All Rights Reserved.
#
# END_COPYRIGHT
########################################

# TODO: enable once all build machines are upgraded
#cmake_minimum_required(VERSION 2.8.5)

message(STATUS "**BEGIN util/dmalloc/CMakeLists.txt --------------------------------")

# shared library for user defined objects
set (dmalloc_src
    dmalloc.cpp
)

message(STATUS "Debug: CMAKE_C_FLAGS is ${CMAKE_C_FLAGS}")
message(STATUS "Debug: CMAKE_CXX_FLAGS is ${CMAKE_CXX_FLAGS}")
message(STATUS "Debug: GENERAL_OUTPUT_DIRECTORY is ${GENERAL_OUTPUT_DIRECTORY}")

add_library(dmalloc SHARED ${dmalloc_src})

extractDebugInfo("${GENERAL_OUTPUT_DIRECTORY}" "libdmalloc${CMAKE_SHARED_LIBRARY_SUFFIX}" dmalloc)
set_target_properties(dmalloc PROPERTIES LIBRARY_OUTPUT_DIRECTORY ${GENERAL_OUTPUT_DIRECTORY})


message(STATUS "**END util/dmalloc/CMakeLists.txt --------------------------------")
