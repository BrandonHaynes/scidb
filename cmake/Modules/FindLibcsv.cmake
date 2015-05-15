# - Try to find libcsv
#
# Once done this will define
#
#  libcsv_FOUND - system has libcsv
#  libcsv_INCLUDE_DIR - the libcsv include directory
#  libcsv_LIBRARY - Link these to use libcsv
#
find_path(libcsv_INCLUDE_DIR
  NAMES csv.h
  HINTS "/opt/scidb/${SCIDB_VERSION_MAJOR}.${SCIDB_VERSION_MINOR}/3rdparty/libcsv/include"
  )

find_library(libcsv_LIBRARY
  NAMES csv
  HINTS "/opt/scidb/${SCIDB_VERSION_MAJOR}.${SCIDB_VERSION_MINOR}/3rdparty/libcsv/lib"
  )

set(libcsv_FOUND TRUE)
if ("${libcsv_INCLUDE_DIR}" STREQUAL "libcsv_INCLUDE_DIR-NOTFOUND")
  set(libcsv_FOUND FALSE)
endif()
if ("${libcsv_LIBRARY}" STREQUAL "libcsv_LIBRARY-NOTFOUND")
  set(libcsv_FOUND FALSE)
endif()

if(NOT libcsv_FOUND)
  if(libcsv_FIND_REQUIRED STREQUAL "libcsv_FIND_REQUIRED")
    message(FATAL_ERROR "CMake was unable to find libcsv")
  endif()
endif()
