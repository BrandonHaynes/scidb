#!/bin/bash
#
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
#

#$1 - host
#$2 - port
#$3 - db_name
#$4 - db_user
#$5 - db_passwd

PGPASSWORD="$5" psql --no-psqlrc -U "${4}" -h "${1}" -p "${2}" --dbname "${3}" --command "select AR.name from \"array\" as AR where not exists (select AD.array_id from array_dimension as AD where AR.name=AD.mapping_array_name) and AR.name like '%:%'"
