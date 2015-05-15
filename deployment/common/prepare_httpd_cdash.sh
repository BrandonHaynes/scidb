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

username=${1}

case `./os_detect.sh` in
    "CentOS 6")
	service iptables stop
	chkconfig iptables off
	yum install -y httpd
	chkconfig httpd on
	service httpd start
	CONFIG=/etc/httpd/conf/httpd.conf
	cat ${CONFIG} | sed -e "s/\/var\/www\/html/\/var\/www/g" > ${CONFIG}.new
	cat ${CONFIG}.new > ${CONFIG}
	rm -f ${CONFIG}.new
	usermod -G apache -a ${username}
	mkdir -p /var/www/cdash_logs
	chmod g+wx -R /var/www/cdash_logs
	chown apache:apache -R /var/www/cdash_logs
	CONFIG=/etc/sysconfig/selinux
	cat ${CONFIG} | sed -e "s/enforcing/disabled/g" > ${CONFIG}.new
	cat ${CONFIG}.new > ${CONFIG}
	rm -f ${CONFIG}.new
	setenforce 0 || true
	;;
    "RedHat 6")
	echo "We do not support build SciDB under RedHat 6. Please use CentOS 6 instead"
	exit 1
	;;
    "Ubuntu 12.04")
	apt-get update
	apt-get install -y apache2
	usermod -G www-data -a ${username}
	mkdir -p /var/www/cdash_logs
	chmod g+wxr -R /var/www/cdash_logs
	chown www-data:www-data -R /var/www/cdash_logs
	;;
    "Ubuntu 14.04")
	apt-get update
	apt-get install -y apache2
	usermod -G www-data -a ${username}
	mkdir -p /var/www/cdash_logs
	chmod g+wxr -R /var/www/cdash_logs
	chown www-data:www-data -R /var/www/cdash_logs
	;;
    *)
	echo "Not supported OS"
	exit 1
	;;
esac
