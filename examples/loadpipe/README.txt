This directory contains a sample proof-of-concept application for the
loadpipe.py script.  Refer to the SciDB User Guide.

Sample Data
===========

This example uses NYSE "trade and quote" (TAQ) data as input.  A full
days' worth of historical sample TAQ data can be obtained from:

        http://www.nyxdata.com/Data-Products/NYSE-Trades-EOD

The file format for this data is described by http://www.nyxdata.com/doc/76994 .
Be sure to read the "Terms of Use" section of the nyxdata.com website for
restrictions on the use and copying of the sample data.

Files
=====

nyse_tool.py
    Contains some miscellaneous code for analyzing a TAQ file, plus
    the sym_to_id function used to hash ticker symbols to integers so
    they can be used as a SciDB array dimension.

nyse_feeder.py
    Reads TAQ data from stdin and uses the embedded timestamps to play
    it out at a scaled rate, preserving relative intervals between
    trades.  For example, to play a TAQ file into a TCP client socket
    at one tenth trading speed:

        $ zcat huge_taq_file.gz | \
        >    nyse_feeder.py --scale 10 - | \
        >    nc <host> <port>

    NOTE: To do generic rate-limiting of pipelined data, have a look
    at the -L option of the pv(1) command, available on all supported
    platforms: http://www.ivarch.com/programs/pv.shtml

make_nyse_arrays.sh
    This shell script runs iquery to create the SciDB arrays used
    for loading and redimensioning the incoming trade data.

nyse_loader.sh
    This shell script shows how the loadpipe.py script is invoked
    for this demo.  Note that command line options after the '--'
    are passed directly to loadcsv.py, so you have the full range of
    loadcsv capabilities available to you.

rsyslog-conf.diff
    Patch file showing one of many possible ways to configure logging
    for loadpipe.py and other syslog(3)-based scripts.

track_favorites.py
    This script queries the nyse_day SciDB array for selected stocks.
    Running it in a loop during the demo, you can watch stocks' high,
    low, volume, and trade counts change as the array is incrementally
    loaded:

        $ while : ; do
        >   echo ======= ;
        >   iquery -q "select count(*) from nyse_day;" ;
        >   ./track_favorites.py ;
        >   sleep 15 ;
        > done

    NOTE: Running this loop when nyse_feeder.py is feeding data at
    close to real time (--scale=N and N < about 10), the several
    iquery invocations will begin to conflict with loadpipe.py's
    loadcsv calls, and loadpipe.py will fall behind.  Just sayin'.

Demo Cookbook
=============

 1. Start SciDB as usual.  If you want to clean up from a previous
    demo run:

        $ iquery -aq "remove(nyse_flat) ; remove(nyse_day)"

 2. Cd to .../examples/loadpipe and

        $ eval $(./make_nyse_arrays.sh)

 3. Start the loadpipe.py daemon pipeline:

        $ ./nyse_loader.sh

 4. On a client machine, start streaming a large TAQ file using
    nyse_feeder.py:

        $ zcat trades20130404_9to10am.gz | \
        > ./nyse_feeder.py --scale 10 - | \
        > nc <ScidbHost> 8080

 5. Back on the SciDB host, begin running track_favorites.py to watch
    the action as stock trades accumulate in the nyse_day array.

                                -end-
