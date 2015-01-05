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

set(metadata_files
    meta.sql
    1.sql
    2.sql
)

set(genmeta_output
    ${CMAKE_CURRENT_BINARY_DIR}/CatalogMetadata.h
)

add_custom_command(
  OUTPUT ${genmeta_output}  
  COMMAND python genmeta.py ${metadata_files} -o ${genmeta_output}
  DEPENDS genmeta.py ${metadata_files}
  WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
)

add_custom_target(genmeta DEPENDS ${genmeta_output})
