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

iquery -aq "list()"

#iquery -aq "gemm(XXX_MU_XXX_M1024x1024, build(XXX_MU_XXX_M1024x1024, 0), build(XXX_MU_XXX_M1024x1024, 1))" 1>/dev/null
iquery -aq "filter(XXX_MU_XXX_COPY, equity=1)" 1>/dev/null
iquery -aq "between(XXX_MU_XXX_COPY, null,null,null,null)" 1>/dev/null
iquery -aq "between(XXX_MU_XXX_COPY, 1,0,null,null)" 1>/dev/null
iquery -aq "between(XXX_MU_XXX_COPY, 0,0,null,null)" 1>/dev/null
iquery -aq "join(XXX_MU_XXX_COPY,XXX_MU_XXX_COPY)" 1>/dev/null
#iquery -aq "gesvd(XXX_MU_XXX_M1024x1024, 'values')" 1>/dev/null
