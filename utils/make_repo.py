#!/usr/bin/python
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

import argparse
import subprocess
import paramiko
import re
import os
from getpass import getpass
import shutil

parser = argparse.ArgumentParser()
parser.add_argument('type', type=str, choices=['apt', 'yum'], help='repository type')
parser.add_argument('root', type=str, help='repository root')
parser.add_argument('repo', type=str, help='repository directory (distro/release)')
parser.add_argument('key', type=str, help='GPG key id')
parser.add_argument('-H', '--host', dest='host', type=str, help='remote host name')
parser.add_argument('-u', '--user', dest='user', type=str, help='remote user name')
parser.add_argument('-r', '--remove', dest='remove', action='store_const', const=True, help='remove old repository directorty (use carefully!)')
parser.add_argument('-p', '--password', dest='askpw', action='store_const', const=True, help='ask ssh password')
parser.add_argument('-f', '--files', dest='files', nargs='+', help='list of files to copy or upload to repository')

parser.set_defaults(
    remove=False,
    askpw=False,
    files=None
)

args = vars(parser.parse_args())

TYPE = args['type']
ROOT = args['root']
REPO = args['repo']
HOST = args['host']
USER = args['user']
REMOVE = args['remove']
KEY = args['key']
ASKPW = args['askpw']
FILES = args['files']

def main():
    ssh = None
    if HOST:
        pw = None
        if ASKPW:
            pw = getpass('SSH password:')
        print 'Connecting to %s' % HOST
        ssh = remote(HOST, USER, pw)

    if REMOVE:
        print 'Cleaning repo dir %s/%s' % (ROOT, REPO)
        execute(['test', '-d', ROOT+'/'+REPO, '&&', 'rm', '-rf', ROOT+'/'+REPO], None, ssh)        
    print 'Preparing repo dir %s in %s' % (REPO, ROOT)
    execute(['test', '!', '-d', ROOT, '&&', 'mkdir', '-p', ROOT], None, ssh)
    execute(['test', '!', '-d', REPO, '&&', 'mkdir', '-p', REPO], ROOT, ssh)

    if FILES:
        print 'Uploading files'
        upload(FILES, ROOT+'/'+REPO, ssh)

    if TYPE == 'apt':
        print 'Creating APT repo in ' + REPO
        execute(['rm', '-f', 'Contents', 'InRelease', 'Packages', 'Release', 'Release.gpg', 'Sources'], ROOT+'/'+REPO, ssh)
        execute(['dpkg-scanpackages', REPO, '>', REPO+'/Packages'], ROOT, ssh)
        execute(['dpkg-scansources', REPO, '>', REPO+'/Sources'], ROOT, ssh)
        execute(['apt-ftparchive', 'contents', REPO, '>', REPO+'/Contents'], ROOT, ssh)
        execute(['echo', 'Codename: %s' % REPO, '>', REPO+'/Release'], ROOT, ssh)
        execute(['apt-ftparchive', 'release', REPO, '>>', REPO+'/Release'], ROOT, ssh)
        execute(['gpg', '--batch', '--no-tty', '-u', KEY, '-abs', '-o', REPO+'/Release.gpg', REPO+'/Release'], ROOT, ssh)
        execute(['gpg', '--batch', '--no-tty', '-u', KEY, '--clearsign', '-o', REPO+'/InRelease', REPO+'/Release'], ROOT, ssh)
    elif TYPE == 'yum':
        print 'Creating YUM repo in ' + REPO
        execute(['rm', '-rf', REPO+'/repodata'], ROOT, ssh)
        execute(['createrepo', REPO], ROOT, ssh)
        execute(['gpg', '-u', KEY, '--detach-sign', '--armor', REPO+'/repodata/repomd.xml'], ROOT, ssh)

def upload(files, directory, remote=None):
    if remote:
        sftp = paramiko.SFTPClient.from_transport(remote.get_transport())
        for file in files:
            print 'Uploading %s...' % file
            sftp.put(file, directory + '/' + os.path.basename(file))
    else:
        for file in files:
            print 'Copying %s...' % file
            shutil.copy(file, directory + '/' + os.path.basename(file))

def execute(args, cwd=None, remote=None):
    res = None
    cmd = ' '.join([argprepare(arg) for arg in args])
    if cwd:
        cmd = "cd %s && %s" % (argprepare(cwd), cmd)
    if remote:
        chan = remote.get_transport().open_session()
        stdout = chan.makefile('rb', -1) 
        stderr = chan.makefile_stderr('rb', -1) 
        chan.exec_command(cmd)
        res = chan.recv_exit_status()
        if res:
            print stderr.read()
    else:
        res = os.system(cmd)

def remote(host, user = None, pw = None):
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(host, username = user, password = pw)
    return ssh

def argprepare(arg):
    if arg in ('<', '>', '>>', '|', '||', '&', '&&'):
        return arg
    arg.replace('\'', '\\\'')
    return '\'%s\'' % arg

if __name__ == '__main__':
    main()

