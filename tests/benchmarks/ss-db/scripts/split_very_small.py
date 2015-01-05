#!/usr/bin/python
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
import csv
import time
import os
import subprocess
import sys
import traceback
sys.path.append(os.getcwd()) # NOCHECKIN 
sys.path.append('/opt/scidb/12.3/lib')
import scidbapi as scidb


# Start the single instance server. 

def handleException(inst, exitWhenDone, op=None):
    traceback.print_exc()
    if op:
        print >> sys.stderr, "Exception while ", op
    print >> sys.stderr, "     Exception Type: %s" % type(inst)     # the exception instance
    print >> sys.stderr, "     Exception Value: %r" % inst 
    print >> sys.stderr, ""
    if(exitWhenDone):
        exit(2)

def main():
    size=1600
    db = scidb.connect("localhost", 1239)
    pos=[(498808,499958,0),(499134,498721,1),(665014,401542,2),(499784,499372,3),(498548,499698,4),(498873,498461,5),(499198,498786,6),(706545,443074,7),(499849,499437,8),(498613,499762,9),(498938,498526,10),(540090,276619,11),(499589,499177,12),(499914,499502,13),(498678,499827,14),(499003,498591,15),(581621,318150,16),(499654,499242,17),(499979,499567,18),(498743,499892,19)]
    for x,y,z in pos:
	query="store(reshape(slice(very_small_obs,Z,%d),<oid:int64 NULL,center:bool NULL,polygon:int32 NULL,sumPixel:int64 NULL,avgDist:double NULL,point:bool NULL>[J=%d:%d,%d,0,I=%d:%d,%d,0]),very_small_obs_%d)" %(z,x,x+size-1,size,y,y+size-1,size,z)    
	print query
    	result=db.executeQuery(query,"afl")
        db.completeQuery(result.queryID)
    db.disconnect()     #Disconnect from the SciDB server. 

    print "Done!"
    sys.exit(0) #success
if __name__ == "__main__":
    main()
