#!/usr/bin/env python
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

#Become postgres user and
#Edit /etc/postgresql/8.4/main/pg_hba.conf
#Append the following configuration lines to give access to 10.X.Y.Z/N network:
#host all all 10.X.Y.Z/N md5
# For example: 10.0.20.0/24
#
#Edit /etc/postgresql/8.4/main/postgresql.conf
#Set IP address(es) to listen on; you can use comma-separated list of addresses;
#defaults to 'localhost', and '*' is all ip address:
#listen_addresses='*'

import sys
import os
import re

OS=sys.argv[1]
username=sys.argv[2]
password=sys.argv[3]
network=sys.argv[4]

default=[
'host\s+all\s+all\s+127\.0\.0\.1/32\s+ident',
'host\s+all\s+all\s+\:\:1/128\s+ident'
]
if OS == "CentOS 6" or OS == "RedHat 6":
    pg_hba_conf="/var/lib/pgsql/data/pg_hba.conf"
    postgresql_conf="/var/lib/pgsql/data/postgresql.conf"
elif OS == "Ubuntu 12.04":
    pg_hba_conf="/etc/postgresql/8.4/main/pg_hba.conf"
    postgresql_conf="/etc/postgresql/8.4/main/postgresql.conf"
elif OS == "Ubuntu 14.04":
    pg_hba_conf="/etc/postgresql/9.3/main/pg_hba.conf"
    postgresql_conf="/etc/postgresql/9.3/main/postgresql.conf"
else:
    sys.stderr.write("Does not support %s\n" % OS)
    sys.exit(1)

fd=open(pg_hba_conf, 'r')
actual=fd.read()
fd.close()

if len(actual) <= 0:
    sys.stderr.write("%s: empty\n" % pg_hba_conf)
    sys.exit(1)
else:
    for pattern in default:
        m=re.compile(pattern).search(actual)
        if (m is not None):
            newline=m.group().replace('ident', 'md5')
            newline=newline.replace('peer', 'md5')
            actual=actual.replace(m.group(),newline)

actual=actual + ('\nhost    all    all    %s    md5\n' % network)
open(pg_hba_conf, 'w').write(actual)

actual=open(postgresql_conf, 'r').readlines()
extra="listen_addresses='*'\n"
if actual[-1] != extra:
    actual.append(extra)
    open(postgresql_conf,'w').write(''.join(actual))
