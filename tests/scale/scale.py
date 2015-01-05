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
import os
import sys
import subprocess
import shlex
import time
import re
# Tested on 2-instance cluster

basepath = os.path.realpath(os.path.dirname(sys.argv[0])) 
dgpath = basepath + "/../data_gen/gen_matrix"
#spath = basepath + "/../../bin/storage.data1"
spath = "/mnt/master/storage.data1"
outfilename = '/data/scale.stdout'
errfilename = '/data/scale.stderr'
print "dgpath is " + dgpath

resfilename="scale.4n.log"
if len(sys.argv) < 2:
    print "Usage: python scale.py logfile"
    sys.exit(2)
resfilename = sys.argv[1]
print resfilename

matname = ""
resfile = open(resfilename, "a")

def generate(nxchunks=10, nychunks=10, nx=100, ny=100, val=1.0):
    nrows = (nxchunks * nx)
    ncols = (nychunks * ny)
    filename = "/data" + "/" + "Array.scale." + "%d" % nrows + "_" + "%d" % ncols
    cmdstr = "%(cmd)s -r %(a)d %(b)d %(c)d %(d)d %(e)f NG" % { 'cmd': dgpath, 'a':nxchunks, 'b':nychunks, 'c':nx, 'd':ny, 'e':val }
    print "Generate load file " + filename
    print "cmdstr " + cmdstr
    # subprocess.Popen(["rm", "-f", filename]).wait()
    # If a file exists by this name, assume it's a previously generated data file. Re-use it. 
    if (not(os.path.exists(filename) & os.path.isfile(filename))): 
        start = time.time()
        subprocess.Popen(shlex.split(cmdstr), stdout=open(filename, "w")).wait() 
        end = time.time() 
        print >>resfile, "generate " + "%2.2f" % (end - start) 
    return filename


def gen_load_matrix(arrname, nxchunks=2, nychunks=2, nx=1000, ny=1000, val=1.0):
    nrows = (nxchunks * nx)
    ncols = (nychunks * ny)
    filename = "/data" + "/" + "Matrix.scale." + "%d" % nrows + "_" + "%d" % ncols
    cmdstr = "%(cmd)s -r %(a)d %(b)d %(c)d %(d)d %(e)f G" % { 'cmd': dgpath, 'a':nxchunks, 'b':nychunks, 'c':nx, 'd':ny, 'e':val }
    print "Generate load file " + filename
    print "cmdstr " + cmdstr
    # subprocess.Popen(["rm", "-f", filename]).wait()
    # If a file exists by this name, assume it's a previously generated data file. Re-use it. 
    if (not(os.path.exists(filename) & os.path.isfile(filename))): 
        start = time.time()
        subprocess.Popen(shlex.split(cmdstr), stdout=open(filename, "w")).wait() 
        end = time.time() 
        print >>resfile, "generate " + "%2.2f" % (end - start) 

    start = time.time()
    load(arrname, filename)
    end = time.time()     
    print >>resfile, "load " + "%2.2f" % (end - start) 
    
def create(arrname, nrows, ncols, chunkx, chunky):
    attno = 1
    ccmd = "create array " + arrname + " <a1: int32, a2: double> " 
    ccmd = ccmd + "[" + "I=0:" + "%d" % (nrows-1) + "," + "%d" % chunkx + ",0," 
    ccmd = ccmd + " J=0:" + "%d" % (ncols-1) + "," + "%d" % chunky + ",0" + "]"
    
    print ccmd
    start = time.time()
    subprocess.Popen(["iquery", "--afl", "-q", ccmd], stdout=None).wait()
    end = time.time()
    print >>resfile, "create " + "%2.2f" % (end - start) 
    return arrname

def create_3d(arrname, x1, y1, z1, chunkx1, chunky1, chunkz1):
    attno = 1
    ccmd = "create array " + arrname + " <a1: int32, a2: double> " 
    ccmd = ccmd + "[I=0:%d,%d,%d, " % ((x1-1), chunkx1, 0)
    ccmd = ccmd + "J=0:%d,%d,%d,  " % ((y1-1), chunky1, 0)
    ccmd = ccmd + "K=0:%d,%d,%d]  " % ((z1-1), chunkz1, 0)
    
    print ccmd
    start = time.time()
    subprocess.Popen(["iquery", "--afl", "-q", ccmd], stdout=None).wait()
    end = time.time()
    print >>resfile, "create " + "%2.2f" % (end - start) 
    return arrname

def create_matrix(arrname, nrows, ncols, chunkx, chunky):
    ccmd = "create array " + arrname + " <a: double> " 
    ccmd = ccmd + "[" + "I=0:" + "%d" % (nrows-1) + "," + "%d" % chunkx + ",0," 
    ccmd = ccmd + " J=0:" + "%d" % (ncols-1) + "," + "%d" % chunky + ",0" + "]"

    print "arrname is " + arrname
    
    print ccmd
    start = time.time()
    subprocess.Popen(["iquery", "--afl", "-q", ccmd], stdout=outfile).wait()
    end = time.time()
    print >>resfile, "create_matrix " + "%2.2f" % (end - start) 
    return arrname

def load(arrname, filename):
#    filename="/data/Array.data.1000_1000"
    outfile = open(outfilename, "w")
    errfile = open(errfilename, "w")

    if (os.path.exists(filename) & os.path.isfile(filename)): 
        lcmd = "load(" + arrname + ", " + "'" + filename  + "'" + ")" 
        print lcmd
        start = time.time()
        subprocess.Popen(["iquery", "--afl", "-n", "-q", lcmd], stdout=outfile).wait()
        end = time.time()
        print >>resfile, "load " + "%2.2f" % (end - start) 
    outfile.close()
    errfile.close()
 
def run_cmd(cmd):
    print cmd

    outfile = open(outfilename, "w")
    errfile = open(errfilename, "w")
    start = time.time()
    ret = subprocess.Popen(["iquery", "--afl", "-n", "-q", cmd], stderr=errfile, stdout=outfile).wait()
    end = time.time()
    if (ret > 0):
        efr=open(errfilename, "r")
        line = efr.readline()
        while (line):
            if re.match("Error code", line):
                print line
                break
            line = efr.readline()
    outfile.close()
    errfile.close()
    return (end - start)

def count_scan(arrname):
    cmd = "count(scan(" + arrname + "))"
    el = run_cmd(cmd)
    timing = "scan/count " + "%2.2f\n" % el
    resfile.write(timing)

def build_matrix(matname, bldmatname):
    cmd = "store(build(" + matname + ", I * 40000 + (J-1)), " + bldmatname + ")"
    el = run_cmd(cmd)
    timing = "store/build " + "%2.2f\n" % el
    resfile.write(timing)

def scatter_gather(arrname, newarrname):
    cmd = "sg(scan(" + arrname + "), " + newarrname  + ", 1)"
    el = 0
    timing = "scatter/gather " + "%2.2f\n" % el
    resfile.write(timing)

def apply(arrname):
    cmd = "avg(project(apply(" + arrname + ", " + "b, 2*a), b))" 
    el = run_cmd(cmd)
    timing = "ave/proj/apply " + "%2.2f\n" % el 
    resfile.write(timing)

def count_join_transpose(arrname): 
    cmd = "count(join(transpose(" + arrname + "), " + arrname + "))"
    el = run_cmd(cmd)
    timing = "count/join/transpose " + "%2.2f\n" % el
    resfile.write(timing)
    return 

def multiply1(arrname): 
    cmd = "multiply(" + arrname + ", transpose(" + arrname + "))" 
    el = run_cmd(cmd)
    timing = "multiply/transpose " + "%2.2f\n" % el 
    resfile.write(timing)
    return 

def multiply2(arrname): 
    cmd = "multiply(" + arrname + ", " + arrname + ")" 
    el = run_cmd(cmd)
    timing = "multiply " + "%2.2f\n" % el 
    resfile.write(timing)

def multiply3(arrname): 
    condition = "I = J"
    cmd = "multiply(filter(" + arrname + ", " + condition + "), transpose(filter(" + arrname + "," + condition + ")))" 
    el = run_cmd(cmd)
    timing = "multiply " + "%2.2f\n" % el 
    resfile.write(timing)

def join(a1, a2):
    cmd = "count(subarray(join(" + a1 + " as A1, " + a2 + " as A2), 0, 0, 1000, 1000), J)"
    el = run_cmd(cmd)    
    timing = "count-gb/subarray/join " + "%2.2f\n" % el 
    resfile.write(timing)

def aggregate(a1):
    cmd = "aggregate(" + a1 + "," + "e, 0, e*0.75 + a*0.25" + ")"
    el = run_cmd(cmd)
    timing = "aggregate " + "%2.2f\n" % el 
    resfile.write(timing)

def regrid(a1, gridx=10, gridy=10):
    cmd = "avg(regrid(" + a1 + ", %d" % gridx + ", %d" % gridy + ", s, 0, s + a1*a2" + "))"
    el = run_cmd(cmd)
    timing = "regrid/avg " + "%2.2f\n" % el 
    resfile.write(timing)

def repart(a1, a2):
    cmd = "count(filter(repart(" + a1 + "," + a2 + "), a1 > 100*a2))"
    el = run_cmd(cmd)
    timing = "repart " + "%2.2f\n" % el 
    resfile.write(timing)

def reshape(a1, a2):
    cmd = "count(filter(reshape(" + a1 + "," + a2 + "), a1 > 100*a2))"
    el = run_cmd(cmd)
    timing = "reshape " + "%2.2f\n" % el 
    resfile.write(timing)

def filter(a1):
    cmd = "count(filter(" + a1 + ", a1 > 100*a2))"
    el = run_cmd(cmd)
    timing = "filter/count " + "%2.2f\n" % el
    resfile.write(timing)

def subarray(a1, x1=0, y1=0, x2=200, y2=200):
    cmd = "count(filter(subarray(%s, %d, %d, %d, %d), a1 > 100*a2))" % (a1, x1, y1, x2, y2)
    el = run_cmd(cmd)
    timing = "subarray/filter/count " + "%2.2f\n" % el
    resfile.write(timing)

def sort_aggr(a1):
    cmd1 = "sort(sum(%s, a2, J), 1)" % a1
    el = run_cmd(cmd1)
    timing = "sort/aggr-J " + "%2.2f\n" % el
    resfile.write(timing)

    cmd2 = "sort(sum(%s, a2, I), -1)" % a1
    el = run_cmd(cmd2)
    timing = "rev-sort/aggr-I " + "%2.2f\n" % el
    resfile.write(timing)

def xgrid(arrname, sf=2):
    cmd = "avg(xgrid(%s, %d, %d), a2)" % (arrname, sf, sf)
    el = run_cmd(cmd)
    timing = "avg/xgrid %2.2f\n" %el
    resfile.write(timing)

def cleanup(a1):
    rcmd = "remove(" + a1 + ")"
    run_cmd(rcmd)

def get_storage_size():
    sz = os.path.getsize(spath)
#    timing = "storage file = " + "%3.3f" % (sz/(1024*1024)) + "MB"

def array_scale_tests(nrows, ncols, nx1, ny1):
    arrname = "a_" +  "%d" % nrows + "_" + "%d" % ncols
    repart_arrname = arrname + "_rep"
    reshape_arrname = arrname + "_res"
    sgname = arrname + "_sg"

#    cleanup(arrname)
#    cleanup(repart_arrname)
#    cleanup(reshape_arrname)
#    cleanup(sgname)
    
    title = "\narray %d x %d\n" % (nrows, ncols)
    resfile.write(title)
#  create(arrname, nrows, ncols, nx1, ny1)
#  create(repart_arrname, nrows, ncols, nx1*2, ny1*2)
#  create_3d(reshape_arrname, nrows/10, ncols/10, 100, nx1/10, ny1/10, 100)
#  filename = generate(nxchunks=nxchunks1, nychunks=nychunks1, nx=nx1, ny=ny1, val=1.0)
#  load(arrname, filename)

    count_scan(arrname)
    filter(arrname)
    subarray(arrname)
    join(arrname, arrname)
    sort_aggr(arrname)
    regrid(arrname)
    repart(arrname, repart_arrname)
    reshape(arrname, reshape_arrname)
    xgrid(arrname, sf=2)
    resfile.flush()

def matrix_scale_tests(nrows, ncols, nx1, ny1):
    timing = "\nMatrix size " + "%d" % s + "x" + "%d" % s 

    matname = "mat_" +  "%d" % nrows + "_" + "%d" % ncols
    bldmatname = matname + "_bld"
    sgmatname = matname + "_sg"
    trmatname = matname + "_tp"
    
    cleanup(matname)
    cleanup(bldmatname)
    cleanup(trmatname)
    
    # Create, build, count/scan
    create_matrix(matname, nrows, ncols, nx1, ny1)
    build_matrix(matname, bldmatname)
#    gen_load_matrix(matname, nrows/nx1, ncols/ny1, nx1, ny1, 1.0)
#    sys.exit(1)

    count_scan(bldmatname)
    get_storage_size()
 
#    multiply1(bldmatname)
#    multiply2(bldmatname)
#    multiply3(bldmatname)
    # Scatter gather into a new array
#    cleanup(sgmatname)
#    scatter_gather(bldmatname, sgmatname)
#    count_scan(sgmatname)
    get_storage_size()
    
    # transpose join
    count_join_transpose(bldmatname)
    get_storage_size()

    # apply an operator
    apply(bldmatname)
    
    count_scan(bldmatname)
#    aggregate(bldmatname)
    return
    
if __name__ == "__main__":
    get_storage_size()

    
    sizes2 = [5000, 10000, 20000, 30000, 40000, 50000, 60000]
    for s in sizes2: 
        nrows=s
        ncols=s
        nx1=1000 
        ny1=1000
        nxchunks1=nrows/nx1
        nychunks1=ncols/ny1
#        matrix_scale_tests(nrows, ncols, nx1, ny1)

    sizes1 = [5000, 10000, 15000, 20000, 25000, 30000, 35000, 40000]
#    sizes1 = [5000]
    for s in sizes1: 
        nrows=s
        ncols=s
        nx1=2500
        ny1=2500
        nxchunks1=nrows/nx1
        nychunks1=ncols/ny1
        array_scale_tests(nrows, ncols, nx1, ny1)

    sys.exit()

    
        
    
