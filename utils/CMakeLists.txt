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

# Add subfolder of every utility
add_subdirectory("iquery")
add_subdirectory("shim")
add_subdirectory("scidblib")

set(CONFIGURE_PREPARE_DB_METADATA_LOCAL "data/meta.sql")

set(CONFIGURE_PREPARE_DB_METADATA "${CMAKE_INSTALL_PREFIX}/share/scidb/meta.sql")

set(CONFIGURE_SCIDBPY_CONFIG "${CMAKE_INSTALL_PREFIX}/etc/config.ini")

configure_file(scidb-prepare-db.sh.in "${GENERAL_OUTPUT_DIRECTORY}/scidb-prepare-db.sh")
configure_file(init-db.sh "${GENERAL_OUTPUT_DIRECTORY}/init-db.sh" COPYONLY)
configure_file(log1.properties "${GENERAL_OUTPUT_DIRECTORY}/log1.properties" COPYONLY)
if(CMAKE_BUILD_TYPE STREQUAL "Valgrind")
  set(CONFIGURE_SCIDB_PY_VALGRIND "use_valgrind = True")
else(CMAKE_BUILD_TYPE STREQUAL "Valgrind")
  set(CONFIGURE_SCIDB_PY_VALGRIND "use_valgrind = False")
endif(CMAKE_BUILD_TYPE STREQUAL "Valgrind")
configure_file(scidb.py.in "${GENERAL_OUTPUT_DIRECTORY}/scidb.py")
configure_file(sample_config.ini.in "${GENERAL_OUTPUT_DIRECTORY}/packaging_only/sample_config.ini")
configure_file(prelude.txt.in "${GENERAL_OUTPUT_DIRECTORY}/packaging_only/prelude.txt")
configure_file(scidb_cores "${GENERAL_OUTPUT_DIRECTORY}/packaging_only/scidb_cores")
configure_file(loadcsv.py.in "${GENERAL_OUTPUT_DIRECTORY}/loadcsv.py")
configure_file(loadpipe.py "${GENERAL_OUTPUT_DIRECTORY}/loadpipe.py")
configure_file(scidb_backup.py "${GENERAL_OUTPUT_DIRECTORY}/scidb_backup.py")
configure_file(calculate_chunk_length.py "${GENERAL_OUTPUT_DIRECTORY}/calculate_chunk_length.py")
configure_file(remove_arrays.py "${GENERAL_OUTPUT_DIRECTORY}/remove_arrays.py")

add_executable(benchGen benchGen.cc) 
extractDebugInfo("${GENERAL_OUTPUT_DIRECTORY}" "benchGen" benchGen)
set_target_properties(benchGen PROPERTIES RUNTIME_OUTPUT_DIRECTORY ${GENERAL_OUTPUT_DIRECTORY})

add_executable(csv2scidb csv2scidb.cpp)
extractDebugInfo("${GENERAL_OUTPUT_DIRECTORY}" "csv2scidb" csv2scidb)
target_link_libraries(csv2scidb system_lib)
set_target_properties(csv2scidb PROPERTIES RUNTIME_OUTPUT_DIRECTORY ${GENERAL_OUTPUT_DIRECTORY})

add_executable(tsv2scidb tsv2scidb.cpp)
extractDebugInfo("${GENERAL_OUTPUT_DIRECTORY}" "tsv2scidb" tsv2scidb)
target_link_libraries(tsv2scidb system_lib util_lib)
set_target_properties(tsv2scidb PROPERTIES RUNTIME_OUTPUT_DIRECTORY ${GENERAL_OUTPUT_DIRECTORY})

configure_file(scidbLoadCsv.sh "${GENERAL_OUTPUT_DIRECTORY}/scidbLoadCsv.sh" COPYONLY)

add_executable(osplitcsv splitcsv.c)
extractDebugInfo("${GENERAL_OUTPUT_DIRECTORY}" "osplitcsv" osplitcsv)
set_target_properties(osplitcsv PROPERTIES RUNTIME_OUTPUT_DIRECTORY ${GENERAL_OUTPUT_DIRECTORY})

find_package(Libcsv REQUIRED)
include_directories(${libcsv_INCLUDE_DIR})

add_executable(splitcsv splitcsv.cpp)
extractDebugInfo("${GENERAL_OUTPUT_DIRECTORY}" "splitcsv" splitcsv)
target_link_libraries(splitcsv ${libcsv_LIBRARY} util_lib)
set_target_properties(splitcsv PROPERTIES RUNTIME_OUTPUT_DIRECTORY ${GENERAL_OUTPUT_DIRECTORY})
