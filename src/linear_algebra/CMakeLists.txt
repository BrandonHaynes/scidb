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

message(STATUS "**BEGIN linear_algebra/CMakeLists.txt --------------------------------")

# shared library for user defined objects
set (linear_algebra_src
    plugin.cpp
    spgemm/LogicalSpgemm.cpp
    spgemm/PhysicalSpgemm.cpp
    stats/HypergeometricFunctions.cpp
)

message(STATUS "Debug: CMAKE_C_FLAGS is ${CMAKE_C_FLAGS}")
message(STATUS "Debug: CMAKE_CXX_FLAGS is ${CMAKE_CXX_FLAGS}")

#
# complete the linar_algebra library settings
#
add_library(linear_algebra SHARED ${linear_algebra_src})

extractDebugInfo("${GENERAL_OUTPUT_DIRECTORY}/plugins" "liblinear_algebra${CMAKE_SHARED_LIBRARY_SUFFIX}" linear_algebra)
set_target_properties(linear_algebra PROPERTIES LIBRARY_OUTPUT_DIRECTORY ${GENERAL_OUTPUT_DIRECTORY}/plugins)


message(STATUS "**END linear_algebra/CMakeLists.txt --------------------------------")
