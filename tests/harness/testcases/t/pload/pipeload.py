#!/usr/bin/python

"""
Streaming load smoke test.

The idea here is to generate two deterministic data sets, one for
odd=True and another for odd=False.  These data sets will be streamed
into the target array in parallel by two threads, and when all is done
we should be able to verify that both data sets are present.
"""

import argparse
import csv
import os
import random
import sys
import time
import threading
import subprocess
from cStringIO import StringIO
from math import pi as PI

_IQUERY_CMD = ['iquery', '-c', os.environ['IQUERY_HOST'], '-p', os.environ['IQUERY_PORT']]

_args = None

_LOAD_ATTRS = "<rowNum:int32,i:int32,odd:int32,piMult:double,twoI:int32>"
_LOAD_DIMS = "[k=0:*,10,0]"
_LOAD_SCHEMA = ''.join((_LOAD_ATTRS, _LOAD_DIMS))
_LOAD_PATTERN = "NNNNN"

_TARGET_ATTRS = "<i:int32,odd:int32,piMult:double>"
_TARGET_DIMS = "[rowNum=0:*,10,0,twoI=0:*,10,0]"
_TARGET_SCHEMA = ''.join((_TARGET_ATTRS, _TARGET_DIMS))
_TARGET = 'sload_test'

def burst_to_batch(n):
    """Compute loadpipe.py batch lines based on burst size."""
    return n / 2

def verify_result():
    """Verify that all streamed data got loaded into the target array.

    @return True IFF all data points got into the target array.

    We know all is well if there is a rowNum entry for every sample.
    We can do some other checking too, based on putrow() below.
    """
    print "Verifying result..."
    fname = '/tmp/%s.csv' % _TARGET
    try:
        os.unlink(fname)
    except OSError:
        pass
    subprocess.call(_IQUERY_CMD + ['-naq',
                     "save(%s, '%s', -2, 'csv+')" % (_TARGET, fname)])
    try:
        with open(fname) as F:
            rdr = csv.reader(F)
            rdr.next()          # skip header
            row_num = 0         # scope to 'with' stmt, not 'for' stmt
            for row_num, row in enumerate(rdr):
                if row_num != int(row[0]):
                    raise ValueError("Should be row %d but isnt: %s" % (
                            row_num, ', '.join(row)))
                # A few data checks based on putrow() (well, one anyway).
                i = int(row[2])
                if (i*PI) - float(row[4]) > 0.001:
                    raise ValueError("Row %d pie eye %f != %s" % (
                            i, i*PI, row[4]))
            missing = _args.samples - (row_num + 1)
            if missing:
                raise ValueError("Missing %d samples" % missing)
    except ValueError as e:
        print e
        return False
    else:
        return True
    finally:
        if _args.keep_tmp:
            print "Target array csv+ output kept in", fname
        else:
            os.unlink(fname)

class DataGenerator(object):

    """File-like wrapper around test data generator."""

    def __init__(self, odd, count, burst, max_sleep_millis):
        """Object emits deterministic CSV data in bursts.

        @param odd make first field odd, else make it even
        @param count number of CSV records to generate
        @param burst records to write between sleeps
        @param max_sleep_millis longest time to sleep in milliseconds
        """
        self.odd = 1 if odd else 0
        if count < burst:
            count = burst
        self.max_sleep_millis = max_sleep_millis
        self.burst_size = burst
        self.bursts = count / burst
        self.last_burst = count % burst
        self.buffer = StringIO()
        self.row_number = 0
        self.burst_number = 0
        return None

    def readline(self, size=None):
        """Return next line of data, pausing if we are between bursts."""
        assert size is None, "Ouch, must implement DataGenerator.readline(size)"
        line = self.buffer.readline()
        if not line:
            # End of burst, get a new burst.
            self._get_burst()
            line = self.buffer.readline()
        return line

    def next(self):
        """Iteration support."""
        line = self.readline()
        if not line:
            raise StopIteration()
        return line

    def __iter__(self):
        """Iteration support."""
        return self

    def _get_burst(self):
        """Generate a burst of data, and then maybe sleep."""
        self.buffer = StringIO() # no StringIO.clear() method, oh well
        writer = csv.writer(self.buffer)
        def putrow(i, odd):
            # Note: MUST agree with _LOAD_ATTRS !!!
            row = [2*i + odd, i, odd, i*PI, 2*i]
            writer.writerow(row)
            return i + 1
        if self.burst_number < self.bursts:
            self.burst_number += 1
            for i in xrange(self.burst_size):
                self.row_number = putrow(self.row_number, self.odd)
        elif self.last_burst:
            for i in xrange(self.last_burst):
                self.row_number = putrow(self.row_number, self.odd)
            self.last_burst = 0
        else:
            # No more data, hand back empty burst right away.
            return
        self.buffer.seek(0)
        if self.max_sleep_millis:
            # Inter-burst sleepy time!
            millis = random.randint(0, self.max_sleep_millis)
            seconds = float(millis) / 1000.0
            time.sleep(seconds)

    def close(self):
        del self.buffer

class Pipeline(object):

    """Wrapper around Popen pipeline."""

    def __init__(self, cmd_list, bufsize=1):
        """Create a pipeline.

        @param cmd_list command list, or list of command lists
        @param bufsize buffer size for pipe I/O, defaults to 1 (line buffered)

        @example
        If cmd_list is

            [ ['echo', 'H1 M0m!'], ['tr', '01', 'oi'] ]

        then the output will be "Hi Mom!".  Love ya, Mom.
        """
        if isinstance(cmd_list[0], basestring):
            cmd_list = [ cmd_list ]
        self.procs = []
        prev_out = subprocess.PIPE
        for cmd in cmd_list:
            try:
                self.procs.append(subprocess.Popen(
                        cmd,
                        stdin=prev_out,
                        stdout=subprocess.PIPE,
                        close_fds=True, # needed, not 100% sure why
                        bufsize=bufsize))
            except Exception as e:
                print >>sys.stderr, "Cannot Popen(", ' '.join(cmd), "):", e
                raise
            else:
                prev_out = self.procs[-1].stdout

    def communicate(self, stdin=None):
        """Communicate with the pipeline, allowing file objects as input.

        @param stdin a string or a file-like object to feed into the pipeline
        @return (out,err) pipeline's stdout and stderr text

        BUG: The assertion in feeder_main() fires unless the pipeline
        contains at least two Popen objects.  Hence the extra cat(1)
        command in Worker.run.  Pondering possible fixes.
        """
        if stdin is None:
            return self.procs[-1].communicate()
        if isinstance(stdin, basestring):
            return self.procs[-1].communicate(stdin)
        # The tricky part: spawning an async thread to feed the
        # pipeline while we communicate with it.
        def feeder_main(source, sink):
            assert not sink.closed, "Pipe closed in feeder_main thread"
            for line in source:
                sink.write(line)
            sink.flush()
            sink.close()
            return None
        assert not self.procs[0].stdin.closed
        feeder = threading.Thread(target=feeder_main,
                                  args=(stdin, self.procs[0].stdin))
        feeder.daemon = True
        feeder.start()
        return self.procs[-1].communicate()

class Worker(threading.Thread):

    """Run a loadpipe.py pipeline in a thread."""

    def __init__(self, odd):
        threading.Thread.__init__(self)
        self.odd = odd

    def run(self):
        who = 'Odd' if self.odd else 'Even'
        print who, 'thread starting.'

        dgen = DataGenerator(self.odd,
                             _args.samples / 2,
                             _args.burst_size,
                             _args.max_sleep)

        load_cmd = ['loadpipe.py']
        load_cmd.extend(['--batch-lines',
                               str(burst_to_batch(_args.burst_size))])
        load_cmd.extend([ '--',
                                '-t', _LOAD_PATTERN,
                                '-s', _LOAD_SCHEMA,
                                '-A', _TARGET ])
        commands = [
            ['cat', '-'],       # See Pipeline.communicate
            load_cmd
        ]
        p = Pipeline(commands)
        print p.communicate(stdin=dgen)[0]

        print who, 'thread exiting.'
        return None

def main(argv=None):
    if argv is None:
        argv = sys.argv

    global _args
    parser = argparse.ArgumentParser(
        description='Smoke test the loadpipe.py script.')
    parser.add_argument('-s', '--samples', default=500, type=int, help='''
        Number of samples (data points) to generate.''')
    parser.add_argument('-b', '--burst-size', default=20, type=int, help='''
        Generate samples in bursts, BURST_SIZE at a time.''')
    parser.add_argument('-S', '--max-sleep', default=200, type=int, help='''
        Maximum time in milliseconds to sleep between bursts.
        Random inter-burst sleep simulates uneven sample arrival rate.''')
    parser.add_argument('-k', '--keep-tmp', default=False, action='store_true',
                        help="Do not delete temporary csv+ output file.")
    _args = parser.parse_args(argv[1:])

    # Validate arguments... crudely.
    assert _args.samples > 0
    assert _args.burst_size > 0
    assert _args.max_sleep >= 0
    # Make life easy, even sample sizes please.
    if _args.samples % 2:
        _args.samples += 1
    # Must have at least one burst per worker!
    if _args.burst_size * 2 > _args.samples:
        _args.burst_size = _args.samples / 2

    # Print some stuff for posterity's sake.
    print '\n'.join(((' Samples: %d' % _args.samples),
                     ('   Burst: %d lines' % _args.burst_size),
                     ('MaxSleep: %dms' % _args.max_sleep),
                     ('   Batch: %d lines' % burst_to_batch(_args.burst_size))))

    # Create (or recreate) target array
    with open('/dev/null', 'wb') as NULL:
        subprocess.call(_IQUERY_CMD + ['-aq', "remove(%s)" % _TARGET], stderr=NULL)
    rc = subprocess.call(_IQUERY_CMD + ['-aq',
                          'create array %s %s' % (_TARGET, _TARGET_SCHEMA)])

    # Stream data into the target array from two threads!
    threads = [Worker(True), Worker(False)]
    for th in threads:
        th.start()
    for th in threads:
        th.join()

    # Make sure all the data got into the array.
    ok = verify_result() 
    if ok:
        print "%s test: PASS" % os.path.basename(argv[0])
    else:
        print "%s test: FAIL" % os.path.basename(argv[0])

    # Clean up and exit.
    with open('/dev/null', 'wb') as NULL:
        subprocess.call(_IQUERY_CMD + ['-aq', "remove(%s)" % _TARGET], stderr=NULL)
    return 0 if ok else 1

if __name__ == '__main__':
    sys.exit(main())
