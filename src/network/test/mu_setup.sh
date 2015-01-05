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

iquery -aq "remove(XXX_MU_XXX_SCHEMA)" 1>/dev/null 2>/dev/null
iquery -aq "remove(XXX_MU_XXX_ORIG)" 1>/dev/null 2>/dev/null
iquery -aq "remove(XXX_MU_XXX_COPY)" 1>/dev/null 2>/dev/null
iquery -aq "remove(XXX_MU_XXX_M1024x1024)" 1>/dev/null 2>/dev/null

iquery -naq "CREATE ARRAY XXX_MU_XXX_SCHEMA < price : double > [ equity=1:300,10,0, time=1:300,10,0 ]"
iquery -naq "store(build(XXX_MU_XXX_SCHEMA, equity*time*7.0), XXX_MU_XXX_ORIG)"
iquery -naq "store(XXX_MU_XXX_ORIG,XXX_MU_XXX_COPY)"
iquery -naq "load_library('dense_linear_algebra')"
iquery -naq "create array XXX_MU_XXX_M1024x1024 <x:double>[i=0:1024-1,32,0, j=0:1024-1,32,0]"
iquery -naq "store(build(XXX_MU_XXX_M1024x1024, iif(i=j,1,0)), XXX_MU_XXX_M1024x1024)"

