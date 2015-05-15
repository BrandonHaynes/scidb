#!/usr/bin/python
import os
import select
import sys
import stat
import fcntl
import re
import time
import random
import threading
import argparse
import itertools
import functools
import subprocess
import string
from scidb_backup import CommandRunner as CR

SELECT_STREAM_TIMEOUT = 600

class worker_job(object):
    """ Wrapper class representing a job performed by one worker.
        The class is handed a shell command to run and the number of
        times to repeat it.  Each time the command is repeated by the
        worker, a new process is started.
    """
    def __init__(
        self,
        shell_cmd,
        num_id,
        repeats,
        random_seed
        ):
        """ Constructor for the worker_job class.
            @param shell_cmd string representing the shell command to
                   be run by this worker.
            @param num_id numeric id for this worker
            @param repeats number of times to repeat the shell command
            @param random_seed random number generated for the duration
                   of the script run
            @return new worker_job object
        """
        self._shell_cmd = shell_cmd
        self._num_id = num_id
        self._repeats = repeats
        self._random_seed = random_seed
        self._proc = None
        self._return_codes = {}
        self._last_text_chunk = str(self._random_seed)

    def fileno(self):
        """ Method that exposes the file descriptor of the running
            process currently associated with this worker job.  This
            method is invoked when the worker_job object is passed to
            select function.
        """
        if (self._proc is not None):
            return self._proc.stdout.fileno()
        return None

    def start(self):
        """ Method that runs the process for this job.
        """
        self._proc = subprocess.Popen(
            self._shell_cmd,
            bufsize=0, # Unbuffered: needed for non-blocking reads.
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT, # Redirect stderr into stdout.
            shell=True
            )
        # Set flags on the stdout stream to make reads from it non-blocking.
        flags = fcntl.fcntl(self._proc.stdout, fcntl.F_GETFL)
        fcntl.fcntl(self._proc.stdout, fcntl.F_SETFL, flags | os.O_NONBLOCK)

        self._repeats -= 1

    def finished(self):
        """ Method that checks if the job is done.  If the current process
            is finished, but the job has to repeat it again, the method
            starts the process again.
        """
        # Important: attempt to retrieve the return code from the current
        # process.
        ret_code = self._proc.poll()

        # Store the return code.
        self._return_codes[str(self._proc.pid)] = ret_code

        # If the process does not have to be repeated, the current process
        # has finished, and there is no more text in the stdout pipe, then
        # send back "all done" signal.
        if ((self._repeats == 0) and \
            (ret_code is not None) and \
            (self._last_text_chunk == '')):
            return True

        # If there are repeats left, the current process has finished, and
        # there is no more text in the stdout pipe, then restart the process.
        if ((self._repeats > 0) and \
            (ret_code is not None) and \
            (self._last_text_chunk == '')):
            self.start()

        # Send back "not done yet" signal.
        return False

    def read(self):
        """ Retrieve a chunk of text from the output stream.  The method
            should be invoked only when a call to select marks this
            worker_job object as being in signalled state.
        """
        self._last_text_chunk = self._proc.stdout.read(1024)
        text = self._last_text_chunk
        return text

    @property
    def returncode(self):
        """ Method that returns the maximum numeric return code from
            all repeated processes run by this job.  If any of the
            processes have failed, returning max will ensure that this
            job is counted as a failure.
        """
        return max([int(self._return_codes[pid]) for pid in self._return_codes.keys()])

def get_random_string(size=5):
    """ Return a random string of letters and digits.  String
        always begins with a letter so as to be suitable for
        most language identifiers.  If size is 0, empty string
        is returned.

        @param size length of the rendom string

        @return random string of letters and digits
    """
    if (size <= 0): # Return empty string for bad sizes.
        return ''

    first_char = random.choice(string.letters)
    if (size == 1):
        return first_char # First character is always a letter.
    chars = [first_char]

    # Select the rest of the characters at random: note size-1
    # because the first character is already generated.
    chars.extend(random.sample(string.letters + string.digits,size-1))

    return ''.join(chars)

def get_instance_info():
    """ Obtain server/port information for all running scidb instances.
        Data is returned as a list of lists:
        [
         ['server0','port0'],
         ['server0','port1'],
         ...
         ['serverN-1','portM-1']
        ]
    """
    scidb_host = '127.0.0.1'
    scidb_port = '1239'
    if (os.environ.has_key('IQUERY_HOST')):
        scidb_host = os.environ['IQUERY_HOST']
    if (os.environ.has_key('IQUERY_PORT')):
        scidb_port = os.environ['IQUERY_PORT']

    cmd_runner = CR() # Shell/process runner object.
    cmd = [
        'iquery',
        '-c',
        scidb_host,
        '-p',
        scidb_port,
        '-ocsv',
        '-aq',
        'list(\'instances\')'
        ]
    ret,outs = cmd_runner.waitForProcess(
        cmd_runner.runSubProcess(
            cmd
            )
        )
    lines = outs[0].split('\n')
    lines = lines[1:] # Skip the firt line (header).

    instance_info = []
    for line in lines:
        if (line.strip() == ''):
            continue
        info = line.strip().split(',')
        instance_info.append([info[0].replace('\'',''),info[1]])

    return instance_info

def write_worker_shells(
    nworkers,
    group_selector,
    prefix
    ):
    """ Output shell files that will be used by individual workers.
        @param nworkers number of workers
        @param group_selector callable function that returns the next
               group of shell lines for a worker
        @param prefix string prefix to prepend to each shell script
               file name

        @return list of paths to the newly-created shell files (one
                for each worker)
    """
    shells = []
    for i in range(nworkers):
        shell_lines = ['#!/bin/bash','set -e']
        shell_lines.extend(group_selector())
        # Save the queries in a temporary shell file.
        temp_file_name = prefix
        temp_file_name = temp_file_name + str(i+1) + '.sh'
        temp_file_path = os.path.join('/tmp',temp_file_name)
        with open(temp_file_path,'w') as fd:
            fd.write('\n'.join(shell_lines))
        shells.append(temp_file_path)
    return shells

def get_worker_shell_commands(shells,instance_info):
    """ Prepare and return string commands for every worker.  Commands
        are for the shell execution: they also include variables for
        scidb host (RANDOM_HOST) and port (RANDOM_PORT).  Shells can
        use these variables to communicate with a randomly-selected
        scidb coordinator instance.

        @param shells list of paths to shell scripts (one for each worker)
        @param instance_info host/port info for every running instance of
               scidb.
        @return list of string shell commands (one for each worker)
    """
    shell_cmds = []
    for shell in shells:
        # Pick an instance at random.
        random_instance = random.choice(instance_info)
        # Prepare the env. variables for a shell command.
        env_vars = [
            'RANDOM_HOST="' + random_instance[0] + '"',
            'RANDOM_PORT="' + random_instance[1] + '"',
            ]
        # Put together the variable exports and the actual shell to run
        # into a single string.
        cmd = list(env_vars)
        cmd.extend(['/bin/bash',shell])
        shell_cmds.append(' '.join(cmd))
    return shell_cmds

def start_worker_jobs(shell_cmds,repeats,random_seed):
    """ Start all worker jobs at once.  Stderr stream is redirected into
        stdout for easier output processing.
        @param shell_cmds list of shell commands (one for each worker) in
               string form.
        @return list of started processes (Popen objects)
    """
    worker_jobs = []
    num_id = 1
    for shell_cmd in shell_cmds:
        w_job = worker_job(shell_cmd,num_id,repeats,random_seed)
        w_job.start()
        worker_jobs.append(w_job)
        num_id += 1
    return worker_jobs

def pump_job_output(jobs,quiet=False):
    """ Output processor for all running workers.  Runs until all output
        streams are exhausted and processes have terminated.
        @param procs list of running processes (Popen objects)
        @param quiet flag that indicated if the output from the processes
               should be printed to the stdout.
    """
    # Set up the pipe lists and process list.  Also, set flags on each
    # process stdout stream so that reads on those streams are non-blocking.

    outputs = [list(['']) for i in range(len(jobs))]

    # "Pump" the output streams: get the text out of them and print it
    # out (unless quiet=True)
    active_jobs = list(jobs)
    while (len(active_jobs) >  0):
        # Get the list of streams that are ready.
        readables,_,_ = select.select(active_jobs,[],[],SELECT_STREAM_TIMEOUT)

        # Grab chunks of text for each ready stream and place them into
        # their respective indexed locations in outputs list.
        for readable in readables:
            outputs[jobs.index(readable)].append(readable.read())

        active_jobs = [job for job in active_jobs if not job.finished()]

        # Print text lines (if any and if not quiet).
        print_full_lines(outputs,quiet)

def print_full_lines(outputs,quiet=False):
    """ Print only full text lines from all workers.  If some
        worker's output is cut off mid-line, the function will
        put the partial line back into the output queue (for
        printing next time).  Each worker's chunk is delineated
        with a small descriptive piece of text identifying the
        worker which produced the text.
        @param outputs list where each element is a list of text
               chunks for one worker's output
        @param quiet flag indicating whether to print worker's
               output to stderr
    """
    # Circumfix for the worker's output.
    prefix = '--------Worker *x*----------\n'
    suffix = '----------------------------\n'

    # Print each worker's output.
    for i in range(len(outputs)):
        if (len(outputs[i]) <= 0):
            continue
        pref = prefix.replace('*x*',str(i+1))
        text_so_far = ''.join(outputs[i])
        if (text_so_far == ''):
            continue
        lines = text_so_far.split('\n')
        # Clip off full lines of text and put the last incomplete line back
        # into the output queue.
        if (text_so_far[-1] != '\n'):
            lines = text_so_far.split('\n')
            outputs[i] = [lines[-1]]
            text_so_far = '\n'.join(lines[:-1])
        else:
            outputs[i] = ['']

        if (not quiet):
            sys.stdout.write(pref)
            sys.stdout.write(text_so_far)
            sys.stdout.write(suffix)
            sys.stdout.flush()

def parse_query_groups(query_file):
    """ Gather the query groups from the input file.  File has
        a rather simplistic format (for ease of processing):
        #Group...
        ...
        ...
        #End
        This helps to keep its parsing simple.  All lines between
        #Group and #End are considered valid.

        @param query_file file containing the queries for
               workers
        @return a list where each element is a list of lines for
                workers' shell scripts.
    """
    contents = ''
    with open(query_file) as fd:
        contents = fd.read()

    # Locate all of the individual groups.
    group_text = re.findall('\#Group[^\#]+#End',contents,re.M|re.S|re.I)
    groups = []

    # Go though each group, split it into lines, and skip comments.
    for group in [section.split('\n') for section in group_text]:
        new_group = []
        for query in group:
            if (query == ''):
                continue
            if (query[0] == '#'):
                continue
            new_group.append(query)
        groups.append(new_group)
    return groups # Done: return the groups of lines.

def main():
    """ Main entry point.
    """
    # Set up and parse command line options.
    prog_desc = 'Multi-user scidb query processor.'
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '-q',
        '--quiet',
        action='store_true',
        default=False,
        help='Run silently without printing any messages.'
        )
    parser.add_argument(
        '-n',
        '--nworkers',
        default=5,
        type=int,
        help='Number of threads/workers to start.'
        )
    parser.add_argument(
        '-r',
        '--repeats',
        default=1,
        type=int,
        help='Number of times each thread/worker will repeat its group of queries.'
        )
    parser.add_argument(
        '-s',
        '--seed',
        default=-1,
        type=int,
        help='Integer seed for the random generator.'
        )
    parser.add_argument(
        '-p',
        '--prefix',
        default='test_',
        help='Prefix for each worker\'s shell script file name.'
        )
    parser.add_argument(
        '-m',
        '--mode',
        default='sequential',
        choices=['random','sequential'],
        help='The way workers select their query group: either sequentially (file order) or at random.'
        )
    parser.add_argument(
        'group_file',
        metavar='GROUP_FILE',
        help='File with the query groups for the threads/workers.'
        )

    args = parser.parse_args()

    # Collect query groups.
    query_groups = parse_query_groups(args.group_file)

    # Get the info on all running scidb instances:
    # [['server0','port0'],...,['serverN-1','portM-1']].
    instance_info = get_instance_info()

    # Prepare and print the seed for the random generator.
    random_seed = time.time()
    random_seed -= int(random_seed)
    random_seed *= 1000000
    random_seed = int(random_seed)
    if (args.seed > 0): # If user specified the seed: take it instead.
        random_seed = args.seed

    # Output the seed value so that the exact same behavior could be
    # reproduced.
    print 'Random seed =',random_seed

    random.seed(random_seed)

    # Prepare the selection function for the query groups: if random mode
    # is not specified, then pick query groups sequentially.  If there are
    # more workers than groups, the selector will cycle again through the
    # groups from the top.
    query_group_cycle = itertools.cycle(query_groups)
    select_group = lambda : query_group_cycle.next()

    if (args.mode == 'random'): # Random mode is specified: pick groups randomly.
        select_group = lambda : random.choice(query_groups)

    # Write the shell scripts for all workers.
    worker_shells = write_worker_shells(args.nworkers,select_group,args.prefix)

    worker_shell_commands = get_worker_shell_commands(worker_shells,instance_info)
    worker_jobs = start_worker_jobs(worker_shell_commands,args.repeats,random_seed)
    pump_job_output(worker_jobs,args.quiet)

    # Delete workers' shell scripts.
    map(lambda sh: os.remove(sh),worker_shells)

    # Check for failures and exit with the appropriate code.
    failures = [job.returncode != 0 for job in worker_jobs]

    if (any(failures)):
        sys.stderr.write('Failures detected!\n')
        sys.exit(1)

    print 'Done.'
if __name__ == '__main__':
    main()
