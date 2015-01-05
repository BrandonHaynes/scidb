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
OS=`./os_detect.sh`

if [ "${OS}" = "Ubuntu 12.04" ]; then
    # Stop VM NAT
    echo "Attempting to shut down virbr0 ..."
    virsh net-destroy default 1>/dev/null 2>/dev/null
    virsh net-autostart default --disable 1>/dev/null 2>/dev/null
    # virsh net-undefine default #this will remove the default config
    service libvirtd restart 1>/dev/null 2>/dev/null
    service libvirt-bin restart 1>/dev/null 2>/dev/null
    # ifconfig virbr0 down
    if (ifconfig virbr0 1>/dev/null 2>/dev/null); then
	echo "ERROR: network interface virbr0 appears to be running."
	exit 1
    fi
fi
