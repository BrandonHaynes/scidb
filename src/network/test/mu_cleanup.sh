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

iquery -aq "remove(XXX_MU_XXX_SCHEMA)" 1>/dev/null 2>/dev/null
iquery -aq "remove(XXX_MU_XXX_ORIG)" 1>/dev/null 2>/dev/null
iquery -aq "remove(XXX_MU_XXX_COPY)" 1>/dev/null 2>/dev/null
iquery -aq "remove(XXX_MU_XXX_M1024x1024)" 1>/dev/null 2>/dev/null
exit 0

