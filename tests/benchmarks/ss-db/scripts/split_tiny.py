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
import os
import sys
import traceback
sys.path.append(os.getcwd()) # NOCHECKIN 
sys.path.append('/opt/scidb/12.7' + '/lib')
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
    size=10
    db = scidb.connect("localhost", 1239)
    pos=[(499967,500048,0),(499990,499961,1),(666073,402182,2),(500036,500007,3),(499949,500030,4),(499972,499942,5),(499994,499965,6),(707670,443779,7),(500040,500011,8),(499953,500034,9),(499967,500048,10),(499990,499961,11),(666073,402182,12),(500036,500007,13),(499949,500030,14),(499972,499942,15),(499994,499965,16),(707670,443779,17),(500040,500011,18),(499953,500034,19)]
    for x,y,z in pos:
	query="store(reshape(slice(tiny_obs,Z,%d),<oid:int64 NULL,center:bool NULL,polygon:int32 NULL,sumPixel:int64 NULL,avgDist:double NULL,point:bool NULL>[J=%d:%d,%d,0,I=%d:%d,%d,0]),tiny_obs_%d)" %(z,x,x+size-1,size,y,y+size-1,size,z)    
	print query
    	result=db.executeQuery(query,"afl");
        db.completeQuery(result.queryID)
    db.disconnect()     #Disconnect from the SciDB server. 

    print "Done!"
    sys.exit(0) #success
if __name__ == "__main__":
    main()
