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

case `./os_detect.sh` in
    "CentOS 6")
	./chroot_build.py -i -d centos-6-x86_64 -t .
	;;
    "RedHat 6")
	echo "We do not support build SciDB under RedHat 6. Please use CentOS 6 instead"
	exit 1
	;;
    "Ubuntu 12.04")
	./chroot_build.py -i -d ubuntu-precise-amd64 -t .
	;;
    "Ubuntu 14.04")
	./chroot_build.py -i -d ubuntu-trusty-amd64 -t .
	;;
    *)
	echo "Not supported OS"
	exit 1
	;;
esac
