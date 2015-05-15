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
message(STATUS "****************** BEGIN CMakeLists.txt ******************")

cmake_minimum_required(VERSION 2.6)

project(SciDB C CXX Fortran)


file(READ "${CMAKE_SOURCE_DIR}/version" _SCIDB_VERSION)
STRING(REGEX MATCH "^([0-9]*)\\.([0-9]*)\\.([0-9]*).*$" __SCIDB_VERSION "${_SCIDB_VERSION}")
if(NOT __SCIDB_VERSION)
    message(FATAL_ERROR "Can not parse 'version' file")
endif()
set(SCIDB_VERSION_MAJOR "${CMAKE_MATCH_1}")
set(SCIDB_VERSION_MINOR "${CMAKE_MATCH_2}")
set(SCIDB_VERSION_PATCH "${CMAKE_MATCH_3}")

set(SCIDB_VERSION "${SCIDB_VERSION_MAJOR}.${SCIDB_VERSION_MINOR}.${SCIDB_VERSION_PATCH}")
set(SCIDB_SHORT_VERSION "${SCIDB_VERSION_MAJOR}.${SCIDB_VERSION_MINOR}")

set(PACKAGE_NAME "scidb-${SCIDB_SHORT_VERSION}")

if(CMAKE_INSTALL_PREFIX_INITIALIZED_TO_DEFAULT)
  set(CMAKE_INSTALL_PREFIX "/opt/scidb/${SCIDB_SHORT_VERSION}" CACHE STRING "Install path" FORCE)
  message(STATUS "CMAKE_INSTALL_PREFIX Initialized to default setting to: " ${CMAKE_INSTALL_PREFIX})
else(CMAKE_INSTALL_PREFIX_INITIALIZED_TO_DEFAULT)
  message(STATUS "CMAKE_INSTALL_PREFIX not Initialized to default keeping: " ${CMAKE_INSTALL_PREFIX})
endif(CMAKE_INSTALL_PREFIX_INITIALIZED_TO_DEFAULT)

if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE RelWithDebInfo) # Default value
endif(NOT CMAKE_BUILD_TYPE)

find_package(Subversion)
if (${CMAKE_VERSION} VERSION_GREATER 2.8 OR ${CMAKE_VERSION} VERSION_EQUAL 2.8)
    find_package(Git)
endif()
if(Subversion_SVN_EXECUTABLE)
    EXECUTE_PROCESS(COMMAND ${Subversion_SVN_EXECUTABLE} info . OUTPUT_VARIABLE Subversion_INFO_OUT
        ERROR_VARIABLE Subversion_svn_info_error RESULT_VARIABLE Subversion_svn_info_result
        WORKING_DIRECTORY ${CMAKE_SOURCE_DIR} OUTPUT_STRIP_TRAILING_WHITESPACE)
    if (Subversion_svn_info_result EQUAL 0)
        # Get version from SVN working copy by default
        Subversion_WC_INFO(${CMAKE_SOURCE_DIR} SciDB)
        set(SCIDB_REVISION "${SciDB_WC_REVISION}")
        message(STATUS "Reading revision from svn: ${SCIDB_REVISION}")
        file(WRITE "${CMAKE_BINARY_DIR}/revision" ${SCIDB_REVISION})
    endif()
endif()
if(NOT SCIDB_REVISION AND GIT_EXECUTABLE AND EXISTS "${CMAKE_SOURCE_DIR}/.git/svn/.metadata")
    # Get version from git-svn working copy
    execute_process(COMMAND ${GIT_EXECUTABLE} svn find-rev master
        OUTPUT_VARIABLE SCIDB_REVISION
        RESULT_VARIABLE GIT_EXEC_RES
        OUTPUT_STRIP_TRAILING_WHITESPACE
        WORKING_DIRECTORY ${CMAKE_SOURCE_DIR})

    string(COMPARE NOTEQUAL ${GIT_EXEC_RES} 0 GIT_EXEC_RES)
    if(${GIT_EXEC_RES})
        MESSAGE(FATAL_ERROR "Can not invoke GIT ${GIT_EXECUTABLE}. Check binary!")
    endif()

    message(STATUS "Reading revision from git-svn: ${SCIDB_REVISION}")

    file(WRITE "${CMAKE_BINARY_DIR}/revision" ${SCIDB_REVISION})
endif()
if(NOT SCIDB_REVISION AND EXISTS "${CMAKE_SOURCE_DIR}/revision")
    # Get version from plain source tarball/directory
    file(READ "${CMAKE_SOURCE_DIR}/revision" SCIDB_REVISION)
    string(STRIP ${SCIDB_REVISION} SCIDB_REVISION)
    message(STATUS "Reading revision from file: ${SCIDB_REVISION}")
endif()
if(NOT SCIDB_REVISION)
    message(FATAL_ERROR "Can not fetch working copy version and can't find revision file.")
endif()

set(SCIDB_VERSION "${SCIDB_VERSION}.${SCIDB_REVISION}")

set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${CMAKE_SOURCE_DIR}/cmake/Modules/")

message(STATUS "Build type (use -DCMAKE_BUILD_TYPE=[RelWithDebInfo]/Debug/Release/Profile/CC/Valgrind): ${CMAKE_BUILD_TYPE}")

if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE RelWithDebInfo) # Default value for end user who want to install scidb from sources
endif(NOT CMAKE_BUILD_TYPE)
#
# configure DISTRO_NAME_VERSION
#
include(LinuxDistroVersion)
message(STATUS "DISTRO_NAME_VER is " ${DISTRO_NAME_VER})

if(${DISTRO_NAME_VER} MATCHES "Ubuntu-14.04")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++98 -pedantic -W -Wextra -Wall -Wno-unused-local-typedefs -Wno-strict-aliasing -Wno-long-long -Wno-unused-parameter -Wno-variadic-macros -fPIC -D__STDC_FORMAT_MACROS")
else()
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++98 -pedantic -W -Wextra -Wall -Wno-strict-aliasing -Wno-long-long -Wno-unused-parameter -Wno-variadic-macros -fPIC -D__STDC_FORMAT_MACROS")
endif()

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-system-headers -isystem /opt/local/include/ -isystem /usr/local/include/")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -D__STDC_LIMIT_MACROS")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DPROJECT_ROOT=\\\"${CMAKE_SOURCE_DIR}/\\\"")

set(CMAKE_CXX_FLAGS_RELWITHDEBINFO "-DNDEBUG -O3 -fno-omit-frame-pointer -g -ggdb3")
set(CMAKE_CXX_FLAGS_PROFILE "-O2 -pg -DNDEBUG")
set(CMAKE_CXX_FLAGS_CC "-fprofile-arcs -ftest-coverage -g -O0 -DCLEAN_EXIT")
set(CMAKE_CXX_FLAGS_VALGRIND "-g -O0 -DCLEAN_EXIT")

set(CMAKE_C_FLAGS_RELWITHDEBINFO "-DNDEBUG -O3 -fno-omit-frame-pointer -g -ggdb3")

if(${DISTRO_NAME_VER} MATCHES "Ubuntu-14.04")
  set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -pedantic -W -Wextra -Wall -Wno-unused-local-typedefs -Wno-long-long -Wno-unused-parameter -fPIC -D__STDC_FORMAT_MACROS")
else()
  set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -pedantic -W -Wextra -Wall -Wno-long-long -Wno-unused-parameter -fPIC -D__STDC_FORMAT_MACROS")
endif()

set(CMAKE_C_FLAGS_PROFILE "-O2 -pg -DNDEBUG")
set(CMAKE_C_FLAGS_CC "-fprofile-arcs -ftest-coverage -g -O0 -DCLEAN_EXIT")
set(CMAKE_C_FLAGS_VALGRIND "-g -O0 -DCLEAN_EXIT")

set(CMAKE_Fortran_FLAGS "${CMAKE_Fortran_FLAGS} -fno-f2c -Wline-truncation")
set(CMAKE_Fortran_FLAGS "${CMAKE_Fortran_FLAGS} -I/opt/scidb/${SCIDB_VERSION_MAJOR}.${SCIDB_VERSION_MINOR}/3rdparty/mpich2/include/mpi")
set(CMAKE_Fortran_FLAGS "${CMAKE_Fortran_FLAGS} -I/opt/scidb/${SCIDB_VERSION_MAJOR}.${SCIDB_VERSION_MINOR}/3rdparty/boost/include/boost")
set(CMAKE_Fortran_FLAGS "${CMAKE_Fortran_FLAGS} ${ALL_FLAGS}")
set(CMAKE_Fortran_FLAGS_RELWITHDEBINFO "-DNDEBUG -O3 -fno-omit-frame-pointer -g -ggdb3")
set(CMAKE_Fortran_FLAGS_DEBUG "${CMAKE_Fortran_FLAGS_DEBUG} -DDEBUG")  # Just for detection debug mode in code
set(CMAKE_Fortran_FLAGS_PROFILE "${CMAKE_Fortran_PROFILE} -O2 -pg -DNDEBUG")
set(CMAKE_Fortran_FLAGS_CC "${CMAKE_Fortran_FLAGS_CC} -fprofile-arcs -ftest-coverage")

set(CMAKE_EXE_LINKER_FLAGS_PROFILE "-pg")
set(CMAKE_LDFLAGS_CC "-fprofile-arcs -ftest-coverage -g -DCLEAN_EXIT")

set(GENERAL_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/bin")
set(GENERAL_TEST_DIRECTORY "${CMAKE_BINARY_DIR}/tests/basic")
set(GENERAL_HARNESSTEST_DIRECTORY "${CMAKE_BINARY_DIR}/tests/harness")

set(PYTHON_SAMPLE_DIRECTORY "${CMAKE_BINARY_DIR}/bin/pythonexamples")

################################################################
# Setting DT_RUNPATH in the elf header:
#
# A. review of library resolution steps
#
# From "man ld.so":
##The shared libraries needed by the program are searched for in the following order:
##
## 1. (ELF only) Using the directories specified in the DT_RPATH dynamic section attribute of the binary if
## present and DT_RUNPATH attribute does not exist. Use of DT_RPATH is deprecated.
##
## 2. Using the environment variable LD_LIBRARY_PATH. Except if the executable is a set-user-ID/set-group-ID
## binary, in which case it is ignored.
##
## 3. (ELF only) Using the directories specified in the DT_RUNPATH dynamic section attribute of the binary if
## present.
##
## 4. From the cache file /etc/ld.so.cache which contains a compiled list of candidate libraries previously
## found in the augmented library path. If, however, the binary was linked with the -z nodeflib linker
## option, libraries in the default library paths are skipped.
##
## 5. In the default path /lib, and then /usr/lib. If the binary was linked with the -z nodeflib linker
## option, this step is skipped.
#
# NOTE:
#  a. We set DT_RUNPATH so (1) does not apply.
#  b. If the user sets LD_LIBRARY_PATH, then (2) applies, and it is the user's repsonsibility.
#  c. Resolution is normally via step (3).
#  d. Only if that fails, (5) and (6) do apply [we do not use -z nodeflib at this time.]
#
# B. $ORIGIN
#
# From "man ld.so":
##RPATH TOKEN EXPANSION
## The runtime linker provides a number of tokens that can be used in an rpath specification (DT_RPATH or DT_RUNPATH).
##
## $ORIGIN
## ld.so understands the string $ORIGIN (or equivalently ${ORIGIN}) in an rpath specification to mean the directory
## containing the application executable. Thus, an application located in somedir/app could be compiled with
## gcc -Wl,-rpath,'$ORIGIN/../lib' so that it finds an associated shared library in somedir/lib no matter where
## somedir is located in the directory hierarchy.
#
# NOTE:
#   We use $ORIGIN to find the locations
#      ../scidb/<version>/lib and
#      ../scidb/<version>/lib/scidb/plugins
#   from
#        either location above or
#      ../scidb/<version>/bin.
#
# by setting it to "$ORIGIN/../lib:$ORIGIN/../.."
#
#   And we use $ORIGIN/../plugins to find plugins from plugins
#
# NOTE:
#  this also allows resolution to ../scidb/<version>.  This will be fixed at a later point.
#
# C. CMAKE macros and the linker
#
# It should be noted that there is no command to the linker to set RUNPATH directly.
# Instead we set RPATH and pass --enable-new-dtags to the linker.
# The linker then sets DT_RUNPATH rather than DT_RPATH.
#
#  Setting CMAKE_SHARED_LINKER_FLAGS to "-Wl,--enable-new-dtags"
#    means CMAKE macros that set RPATH for shared objects results in RUNPATH being set.
#  Setting CMAKE_EXE_LINKER_FLAGS to "-Wl,--enable-new-dtags"
#    means CMAKE macros that set RPATH for programs results in RUNPATH being set.
#  Setting CMAKE_SKIP_BUILD_RPATH to FALSE
#    assures us that CMAKE sets RPATH at build time.
#  Setting CMAKE_BUILD_WITH_INSTALL_RPATH to TRUE tells CMAKE the object is built with the install time RPATH
#    (which for our purpose is the same as build time) so it will not re-instrument at install time.
#  Setting CMAKE_INSTALL_RPATH_USE_LINK_PATH to TRUE tells CMAKE to add any linkages that were used at build time
#    as absolute paths. This applies to boost objects in /opt/scidb/<version>/3rdparty/boost/lib.
#
set(CMAKE_SKIP_BUILD_RPATH FALSE)
set(CMAKE_BUILD_WITH_INSTALL_RPATH TRUE)
set(CMAKE_INSTALL_RPATH_USE_LINK_PATH TRUE)
set(CMAKE_INSTALL_RPATH "$ORIGIN/../plugins:$ORIGIN/../lib:$ORIGIN/../../")
set(CMAKE_SHARED_LINKER_FLAGS "-Wl,--enable-new-dtags")
set(CMAKE_EXE_LINKER_FLAGS "-Wl,--enable-new-dtags")

include(FindPkgConfig)
#
# CENTOS check
#
if(${DISTRO_NAME_VER} MATCHES "RedHat-6")
 message(FATAL_ERROR "We do not support RedHat build. Use CentOS instead")
endif()

#
# LIBRT config
#
find_package(LibRT REQUIRED)

#
# BOOST CONFIG
#
set(BOOST_ROOT /opt/scidb/${SCIDB_VERSION_MAJOR}.${SCIDB_VERSION_MINOR}/3rdparty/boost)
set(Boost_NO_SYSTEM_PATHS TRUE)
set(Boost_NO_BOOST_CMAKE TRUE)

find_package(Boost REQUIRED system program_options serialization regex filesystem thread date_time)

if(NOT Boost_FOUND)
 message(FATAL_ERROR "Could not find any version of boost (1.54 required)")
endif(NOT Boost_FOUND)

if(NOT (${Boost_MAJOR_VERSION} EQUAL 1 AND
        ${Boost_MINOR_VERSION} EQUAL 54))
 message(FATAL_ERROR "Could not find the required 1.54 version of boost")
endif(NOT (${Boost_MAJOR_VERSION} EQUAL 1 AND
           ${Boost_MINOR_VERSION} EQUAL 54))

#
# PROTOBUF CONFIG
#
# outputs PROTOBUF_LIBRARY, not _LIBRARIES as documented
find_package(Protobuf REQUIRED)
find_package(Log4CXX REQUIRED)
find_package(Doxygen)
find_package(LibPQXX REQUIRED)
find_package(LibPQ REQUIRED)

#
# configure CPPUNIT
# configure SWIG & PYTHON
# TODO: configuring of SWIG and PYTHON should be separated as
#       items should not be grouped by special platform handling
pkg_check_modules(CPPUNIT cppunit)

if (NOT WITHOUT_PYTHON_BINDING)
    find_package(SWIG2 "2.0")
    find_package(PythonLibs)
    if (NOT SWIG2_FOUND OR NOT PYTHONLIBS_FOUND)
        message(FATAL_ERROR "SWIG 2 and Python headers required for building Python API bindings. You can set proper SWIG executable with argument -DSWIG_EXECUTABLE=/path/to/swig2.0")
    endif()
endif()

set(FLEX_MINIMAL_VERSION "2.5.35")
find_package(FLEX REQUIRED)
set(BISON_MINIMAL_VERSION "2.4")
find_package(BISON REQUIRED)
find_package(SED REQUIRED)
find_package(ZLIB REQUIRED)
find_package(BZip2 REQUIRED)
set(LIBREADLINE_STATIC OFF)
find_package(LibReadline REQUIRED)
find_package(Threads REQUIRED)
find_package(PythonInterp REQUIRED)

#
# configure BLAS & LAPACK
#
# set BLA_VENDOR (see Find{BLAS,LAPACK}.cmake) to experiment
find_package(BLAS REQUIRED)
find_package(LAPACK REQUIRED)

#
# doc packages, not required
#
find_package(XsltProc)
find_package(Fop)
find_package(DocBook)
if(XSLTPROC_EXECUTABLE AND FOP_EXECUTABLE AND DOCBOOK_XSL_FILE)
    set(BUILD_USER_DOC 1)
else()
    set(BUILD_USER_DOC 0)
endif()

#
# Function Check
#
include(CheckFunctionExists)
check_function_exists(malloc_stats HAVE_MALLOC_STATS)

#
# INCLUDE DIRECTORIES
#
include_directories("${CMAKE_SOURCE_DIR}/src")
include_directories("${CMAKE_SOURCE_DIR}/include")
include_directories("${CMAKE_SOURCE_DIR}/utils")
include_directories("${CMAKE_CURRENT_BINARY_DIR}/src")
include_directories("${CMAKE_CURRENT_BINARY_DIR}/utils")
include_directories(${Boost_INCLUDE_DIRS})
include_directories(${PROTOBUF_INCLUDE_DIR})
include_directories(${LOG4CXX_INCLUDE_DIRS})
include_directories(${LIBPQXX_INCLUDE_DIRS})
include_directories(${LIBPQ_INCLUDE_DIRS})
include_directories(${ZLIB_INCLUDE_DIRS})
include_directories(${BZIP2_INCLUDE_DIR})
include_directories(${READLINE_INCLUDE_DIRS})
include_directories("${CMAKE_SOURCE_DIR}/extern")

link_directories(${LIBPQXX_LIBRARY_DIRS})
link_directories(${LIBPQ_LIBRARY_DIRS})

# static link requires adding a few more libraries
set (LINK_APR_LIBS "")

#
# DEBUG symbols
#
set(DEBUG_SYMBOLS_DIRECTORY ".debug")
set(DEBUG_SYMBOLS_EXTENSION "${DEBUG_SYMBOLS_DIRECTORY}")

# TODO: should probably factor to a xxxxx.cmake file in scidb
macro(extractDebugInfo input_dir input_file targ)
    if("${CMAKE_BUILD_TYPE}" STREQUAL "RelWithDebInfo")
        set(DEBUG_SYMBOLS_OUTPUT_DIRECTORY "${input_dir}/${DEBUG_SYMBOLS_DIRECTORY}")

        file(MAKE_DIRECTORY "${DEBUG_SYMBOLS_OUTPUT_DIRECTORY}")
        add_custom_command(TARGET ${targ}
            POST_BUILD
            COMMAND objcopy --only-keep-debug "${input_dir}/${input_file}" "${DEBUG_SYMBOLS_OUTPUT_DIRECTORY}/${input_file}${DEBUG_SYMBOLS_EXTENSION}"
            COMMAND strip --strip-debug --strip-unneeded "${input_dir}/${input_file}"
            COMMAND objcopy --add-gnu-debuglink=${DEBUG_SYMBOLS_DIRECTORY}/${input_file}${DEBUG_SYMBOLS_EXTENSION} ${input_dir}/${input_file}
            COMMAND chmod -x "${DEBUG_SYMBOLS_OUTPUT_DIRECTORY}/${input_file}${DEBUG_SYMBOLS_EXTENSION}"
            WORKING_DIRECTORY "${input_dir}"
        )
    endif()
endmacro(extractDebugInfo)

#
# SUBDIRECTORIES
#
add_subdirectory("extern")
add_subdirectory("src")
add_subdirectory("utils")
add_subdirectory("doc")
if (NOT WITHOUT_SERVER)
    add_subdirectory("tests")
    add_subdirectory("examples")
endif()

#
# TESTING: CTest, Dart, and Cdash
#
ENABLE_TESTING()
INCLUDE(CTest)

if (CDASH_CONFIG)
  ADD_TEST(NightlyTest "/bin/bash" ${CMAKE_BINARY_DIR}/cdash2/scidb/build_install_test.sh ${CDASH_CONFIG} "prepare_run")
  ADD_TEST(NightlyTestResult "/bin/bash" ${CMAKE_BINARY_DIR}/cdash2/scidb/harness.sh ${CDASH_CONFIG} "collect_cleanup")
else()
  ADD_TEST(NightlyTest "/bin/bash" ${CMAKE_BINARY_DIR}/cdash/SciDB_Build-Packages_Install_Test.sh)
endif()

#
# TEST TIMEOUT
#
if (NOT HARNESS_TEST_TIMEOUT)
  set(HARNESS_TEST_TIMEOUT 420000)
endif(NOT HARNESS_TEST_TIMEOUT)

set_tests_properties (NightlyTest PROPERTIES TIMEOUT ${HARNESS_TEST_TIMEOUT})


#
# Custom uninstall target
#
configure_file(
    "${CMAKE_CURRENT_SOURCE_DIR}/cmake/uninstall.cmake.in"
    "${CMAKE_CURRENT_BINARY_DIR}/uninstall.cmake"
    IMMEDIATE @ONLY)

add_custom_target(uninstall
    COMMAND ${CMAKE_COMMAND} -P ${CMAKE_CURRENT_BINARY_DIR}/uninstall.cmake)

# BEGIN dummy target for adding misc files into the IDE projects like QtCreator
file(GLOB MISC_FILES_SRC
    "doc/pages/*.dox"
    "doc/*.htm*"
    "src/capi/scidbpython.i"
    "doc/api/*"
    "doc/*.txt"
)
add_library(misc_files EXCLUDE_FROM_ALL examples/point/point.cpp ${MISC_FILES_SRC})
# End of dummy target


#
# all outputs must go to CMAKE_BINARY_DIR to support out-of-tree build and test
#
# TODO: these should be done at make time, not cmake time, so a developer need
#       not do a full rebuild when only changing a test or doc file.
#       (HINT add CMakeLists.txt files, with targets with ADD_CUSTOM_COMMAND()s intead of execute_process()
#

#
# COPY CDASH
#
message(STATUS "copying ${CMAKE_SOURCE_DIR}/cdash to ${CMAKE_BINARY_DIR}/cdash")
execute_process(COMMAND ${CMAKE_COMMAND} -E copy_directory ${CMAKE_SOURCE_DIR}/cdash
                                                           ${CMAKE_BINARY_DIR}/cdash)
#
# COPY DOC
#
    # NOTE: doc must be copied before tests, so long as test/examples is a link into
    # doc. This does not seem to cause a problem when the source and destination
    # are in the same volume.
    # However, when they are in different volumes, copy_directory seems to check
    # and detect that the link "tests/examples" to "../doc/tests/examples is not
    # valid (the first time), and the copy of tests stops with an error, and
    # without copying the rest of tests.
    # To confuse matters, doc is then copied, and the second time this is tried,
    # it succeeds.

    # This is a workaround.  The 'right' fix is that source code repositories
    # should not have links in them.
    #
execute_process(COMMAND ${CMAKE_COMMAND} -E copy_directory ${CMAKE_SOURCE_DIR}/doc
                                                           ${CMAKE_BINARY_DIR}/doc)

#
# COPY TESTS
#
message(STATUS "copying ${CMAKE_SOURCE_DIR}/tests to ${CMAKE_BINARY_DIR}/tests")
execute_process(COMMAND ${CMAKE_COMMAND} -E copy_directory ${CMAKE_SOURCE_DIR}/tests
                                                           ${CMAKE_BINARY_DIR}/tests)

# Remove the actual test cases and expected results from the build area:
# they will be accessed directly from the SVN tree.
message(STATUS "copying ${CMAKE_SOURCE_DIR}/tests/harness/testcases/t to ${CMAKE_BINARY_DIR}/tests/harness/testcases/t")
execute_process(COMMAND ${CMAKE_COMMAND} -E remove_directory ${CMAKE_BINARY_DIR}/tests/harness/testcases/t)
execute_process(COMMAND ${CMAKE_COMMAND} -E remove_directory ${CMAKE_BINARY_DIR}/tests/harness/testcases/data)

#
# COPY VERSION
#
execute_process(COMMAND ${CMAKE_COMMAND} -E copy ${CMAKE_SOURCE_DIR}/version
                                                 ${CMAKE_BINARY_DIR}/version)


#
# INSTALL
#
include(install.cmake)

message(STATUS "****************** END CMakeLists.txt ******************")
