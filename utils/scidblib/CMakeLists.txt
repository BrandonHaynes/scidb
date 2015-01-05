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

configure_file(PSF_license.txt "${GENERAL_OUTPUT_DIRECTORY}/scidblib/PSF_license.txt" COPYONLY)
configure_file(__init__.py "${GENERAL_OUTPUT_DIRECTORY}/scidblib/__init__.py" COPYONLY)
configure_file(scidb_afl.py "${GENERAL_OUTPUT_DIRECTORY}/scidblib/scidb_afl.py" COPYONLY)
configure_file(scidb_math.py "${GENERAL_OUTPUT_DIRECTORY}/scidblib/scidb_math.py" COPYONLY)
configure_file(scidb_progress.py "${GENERAL_OUTPUT_DIRECTORY}/scidblib/scidb_progress.py" COPYONLY)
configure_file(scidb_schema.py "${GENERAL_OUTPUT_DIRECTORY}/scidblib/scidb_schema.py" COPYONLY)
configure_file(statistics.py "${GENERAL_OUTPUT_DIRECTORY}/scidblib/statistics.py" COPYONLY)
configure_file(util.py "${GENERAL_OUTPUT_DIRECTORY}/scidblib/util.py" COPYONLY)
configure_file(counter.py "${GENERAL_OUTPUT_DIRECTORY}/scidblib/counter.py" COPYONLY)
configure_file(scidb_psf.py "${GENERAL_OUTPUT_DIRECTORY}/scidblib/scidb_psf.py" COPYONLY)
