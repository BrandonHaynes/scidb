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

import sys
import argparse
import subprocess
import os

CMD_BUILD = 0
CMD_INIT = 1
CMD_UPDATE = 2
CMD_LOGIN = 3

BUILD_RESULT='/tmp/scidb_build'
TEMP_RESULT='/tmp'
USE_SUDO = True

class Col():
    grey =   '\033[90;1m'
    red =    '\033[91;1m'
    green =  '\033[92;1m'
    yellow = '\033[93;1m'
    blue =   '\033[94;1m'
    white =  '\033[97;1m'
    reset =  '\033[0m'

    @staticmethod
    def disable():
        Col.grey = ''
        Col.red = ''
        Col.green = ''
        Col.yellow = ''
        Col.blue = ''
        Col.white = ''

def info(str):
    sys.stderr.write(Col.green + str + Col.reset + "\n")
    sys.stderr.flush()

def warn(str):
    sys.stderr.write(Col.yellow + str + Col.reset)
    sys.stderr.flush()

def err(str):
    sys.stderr.write(Col.red + str + Col.reset)
    sys.stderr.flush()
    exit(1)

def which(program):
    import os
    def is_exe(fpath):
        return os.path.isfile(fpath) and os.access(fpath, os.X_OK)

    fpath, fname = os.path.split(program)
    if fpath:
        if is_exe(program):
            return program
    else:
        for path in os.environ["PATH"].split(os.pathsep):
            exe_file = os.path.join(path, program)
            if is_exe(exe_file):
                return exe_file

    return None

def RunAndWait(arguments):
    return subprocess.Popen(arguments).wait()

def RunSudoAndWait(arguments):
    sudoargs = ['sudo'] + arguments if USE_SUDO else arguments
    return RunAndWait(sudoargs)

class TailFileToStdout(object):
    def __init__(self, filepath):
        # read data from filepath and print to stdout
        self.tail = subprocess.Popen(["tail", "-F", filepath], stderr=subprocess.PIPE)
        # read stderr from tail, skip first line, print to stderr
        # (supress "can't read filepath' message
        self.stderr = subprocess.Popen(["tail", "-n+2"], stdout=sys.stderr, stdin=self.tail.stderr)

    def stop(self):
        # kill 'tail -F'
        self.tail.kill()
        # close 'tail -F' process
        self.tail.wait()
        # close 'tail -F' stderr filter process
        self.stderr.wait()

class UbuntuChroot():
    distroname = 'ubuntu'
    pbuilder_tgz_dir='/var/cache/pbuilder'
    ubuntu_mirror='deb http://archive.ubuntu.com/ubuntu/ %s restricted main multiverse universe'
    scidb_3rdparty_mirror='deb https://downloads.paradigm4.com/ ubuntu12.04/3rdparty/'

    def __init__(self, release, arch, temp_dir):
        info('Will use pbuilder for chrooting. Checking environment...')
        #if not which('pbuilder'):
        #    err('Can not find pbuilder! Check your PATH and/or run script as root!')
        self.release = release
        self.arch = arch
        self.tgz = self.pbuilder_tgz_dir+'/'+release+'-'+arch+'.tgz'
        # trusty is Ubuntu 14.04 release. precise is Ubuntu 12.04 release.
        if self.release == "trusty":
            self.scidb_3rdparty_mirror='deb https://downloads.paradigm4.com/ ubuntu14.04/3rdparty/'
        self.mirror = '|'.join([(self.ubuntu_mirror % release), self.scidb_3rdparty_mirror])
        self.temp_dir=temp_dir
        self.logfile = os.path.join(self.temp_dir,'pbuilder.log')

    def init(self):
        pbargs = ['pbuilder', '--create',
            '--basetgz', self.tgz,
            '--architecture', self.arch,
            '--othermirror', self.mirror,
            '--hookdir', '/var/cache/pbuilder/hook.d',
            '--allow-untrusted',
            '--distribution', self.release,
            '--override-config',
            '--logfile', self.logfile]
        info("Initializing %s from mirror %s" % (self.tgz, self.mirror))
        if RunSudoAndWait(pbargs):
            err("pbuilder returned error. See log %s for details." % self.logfile)
        info("Done. Result stored in %s" % self.tgz)

    def update(self):
        pbargs = ['pbuilder', '--update',
            '--basetgz', self.tgz,
            '--distribution', self.release,
            '--othermirror', self.mirror,
            '--allow-untrusted',
            '--architecture', self.arch,
            '--override-config',
            '--logfile', self.logfile]
        info("Updating %s from mirror %s" % (self.tgz, self.mirror))
        if RunSudoAndWait(pbargs):
            err("pbuilder returned error. See log %s for details." % self.logfile)
        info("Done. %s was updated" % self.tgz)

    def build(self, sources, jobs, buildresult):
        logfile = os.path.join(buildresult, "build.log")
        pbargs = ['pbuilder', '--build',
            '--basetgz', self.tgz,
            '--distribution', self.release,
            '--othermirror', self.mirror,
            '--allow-untrusted',
            '--architecture', self.arch,
            '--buildresult', buildresult,
            '--debbuildopts', '-j%i'%jobs,
            '--override-config',
            '--logfile', logfile,
            sources]
        info("Building %s in %s" % (sources, self.tgz))
        if RunSudoAndWait(pbargs):
            err("pbuilder returned error. See log %s for details." % logfile)
        info("Done. Result stored in %s" % buildresult)

    def login(self):
        pbargs = ['pbuilder', '--login',
            '--basetgz', self.tgz,
            '--distribution', self.release,
            '--othermirror', self.mirror,
            '--allow-untrusted',
            '--architecture', self.arch,
            '--override-config']
        info("Logging into %s" % self.tgz)
        RunSudoAndWait(pbargs)

class CentOSChroot():
    distroname = 'centos'

    def __init__(self, release, arch, temp_dir):
        info("Will use mock for chrooting. Checking environment...")
        #if not which('mock'):
        #    err('Can not find mock! Check your PATH and/or run script as root!')
        self.release = release
        self.arch = arch
        self.chroot = '%s-%s-%s' % (self.distroname, release, arch)
        self.temp_dir=temp_dir

    def init(self):
        mockargs = ['mock', '--init',
            '--root', self.chroot,
            '--arch', self.arch,
            '--resultdir', self.temp_dir]
        info("Initializing %s" % self.chroot)
        if RunSudoAndWait(mockargs):
            err("mock returned error. See log %s for details." % os.path.join(self.temp_dir,'root.log'))
        info("Done")

    def update(self):
        mockargs = ['mock', '--update',
            '--root', self.chroot,
            '--arch', self.arch,
            '--resultdir', self.temp_dir]
        info("Updating %s" % self.chroot)
        if RunSudoAndWait(mockargs):
            err("mock returned error. See log %s for details." % os.path.join(self.temp_dir,'root.log'))
        info("Done")

    def build(self, sources, jobs, buildresult):
        build_log = os.path.join(buildresult, "build.log") 
        root_log = os.path.join(buildresult, "root.log")
        tail = TailFileToStdout(build_log)
        mockargs = ['mock', '--rebuild',
            '--root', self.chroot,
            '--arch', self.arch,
            '--resultdir', buildresult,
                    '--no-cleanup-after',
            sources]
        info("Building %s in %s" % (sources, self.chroot))
        try:
            if RunSudoAndWait(mockargs):
                message = "mock returned error. See logs %s, %s for details."
                err(message % (root_log, build_log))
        finally:
            tail.stop()
        info("Done. Result stored in %s" % buildresult)

    def login(self):
        mockargs = ['mock', '--shell',
            '--root', self.chroot,
            '--arch', self.arch]
        info("Logging into %s" % self.chroot)
        RunSudoAndWait(mockargs)

def main():
    parser = argparse.ArgumentParser()
    groupCmd = parser.add_mutually_exclusive_group(required=True)
    parser.add_argument('-d', '--distro', dest='distro', type=str, required=True, help='Target distro name in format distroname-release-arch')
    groupCmd.add_argument('-b', '--build', dest='command', action='store_const', const=CMD_BUILD, help='Build SciDB on selected platforms (by default)')
    groupCmd.add_argument('-i', '--init', dest='command', action='store_const', const=CMD_INIT, help='Initialize pbuilder tgzs')
    groupCmd.add_argument('-u', '--update', dest='command', action='store_const', const=CMD_UPDATE, help='Update pbuilder tgzs')
    groupCmd.add_argument('-l', '--login', dest='command', action='store_const', const=CMD_LOGIN, help='Login to pbuilder chroot')
    parser.add_argument('-s', '--src', dest='src', type=str, nargs='+', help='.dsc or .src.rpm file(s) for building')
    parser.add_argument('-j', '--jobs', dest='build_jobs', type=int, help='Number of build jobs')
    parser.add_argument('--no-color', dest='color', action='store_const', const=False, help='Disable color output')
    parser.add_argument('-r', '--result-dir', dest='result_dir', type=str, help='Directory for result packages (default is %s)' % BUILD_RESULT)
    parser.add_argument('-t', '--temp-dir', dest='temp_dir', type=str, help='Directory for temporary storage (default is %s)' % TEMP_RESULT)
    parser.add_argument('--no-sudo', dest='use_sudo', action='store_const', const=False, help='Do not use sudo internally (you should run as root by yourself)')

    parser.set_defaults(
      command=CMD_BUILD,
      build_jobs=1,
      color=True,
      result_dir=BUILD_RESULT,
      temp_dir=TEMP_RESULT,
      use_sudo = True)
    args = vars(parser.parse_args())

    CMD = args['command']
    DISTRO = args['distro']
    SRC = args['src']
    JOBS = args['build_jobs']
    COLOR = args['color']
    RESULT_DIR = args['result_dir']
    TEMP_DIR = args['temp_dir']
    USE_SUDO = args['use_sudo']

    if not COLOR:
        Col.disable()

    try:
        (distroname, release, arch) = DISTRO.split('-')
    except:
        err("Wrong distro string '%s'! It should be in format 'distroname-release-arch'" % DISTRO)

    if distroname == 'ubuntu':
        chroot = UbuntuChroot(release, arch, TEMP_DIR)
    elif distroname == 'centos':
        chroot = CentOSChroot(release, arch, TEMP_DIR)
    else:
        err("Wrong distro name '%s', only 'ubuntu' and 'centos' allowed" % distroname)

    info("Will " + ['build package(s) for', 'init chroot for', 'update chroot for', 'login into '][CMD] + " " + str(DISTRO))

    if CMD == CMD_LOGIN:
        chroot.login()
        exit(0)
    elif CMD == CMD_INIT:
        chroot.init()
    elif CMD == CMD_UPDATE:
        chroot.update()
    elif CMD == CMD_BUILD:
        if not SRC:
            err('Source file  not specified, use --src argument!')
        for source in SRC:
            chroot.build(source, JOBS, RESULT_DIR)

if __name__ == '__main__':
    main()
