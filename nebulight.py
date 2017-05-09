#! /usr/bin/env python
# PYTHON_ARGCOMPLETE_OK

"""
NebuLight: A super light weight batch processor for arbitrary command line commands.
(c) 2017 Philip Haeusser, haeusser@cs.tum.edu

This library facilitates batch processing of a list of command line commands.

Example usage:
./nebulight.py add "echo 'OK' >> results.log"
./nebulight.py status
./nebulight.py start
"""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import argparse
import datetime
import os
import random
import shlex
import socket
import sqlite3 as sql
import subprocess
import sys
import time

#import argcomplete

# Constants.
QUEUED = 'queued'
PROCESSING = 'processing'
DONE = 'done'
FAILED = 'failed'
HOLD = 'hold'
ALL = [QUEUED, PROCESSING, DONE, FAILED, HOLD]
IDLE_CHECK_INTERVAL_MIN = 0.1


def _time_str():
    return datetime.datetime.now().strftime("%d.%m %H:%M")


def _add_single_job(cursor, cmd, logfile, status):
    cursor.execute("insert into jobs(cmd, logfile, status, tries, host, time) values (?, ?, ?, ?, ?, ?)",
                   (cmd, logfile, status, 0, '', _time_str()))


def _get_or_create_db(db_name):
    conn = sql.connect(db_name, timeout=20)
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS jobs (job_id INTEGER PRIMARY KEY, cmd, logfile, status, tries, host, time);''')
    return conn, c


def _commit_and_close(conn, cursor):
    conn.commit()
    cursor.close()
    conn.close()


def _print_not_implemented():
    print("Sorry, not implemented yet :-( Contributions are welcome!")


def _check_for_queued_jobs(db_name):
    conn, c = _get_or_create_db(db_name)
    c.execute("SELECT * FROM jobs WHERE status=?", (QUEUED,))
    rows = c.fetchall()
    _commit_and_close(conn, c)
    return len(rows)


def _host():
    return socket.gethostname()


def _update_str(sets, where='job_id'):
    if not type(sets) == list:
        sets = [sets]
    time_str = _time_str()
    sets_str = "=?, ".join(sets)
    sets_str += "=?, time='{}'".format(time_str)

    return "UPDATE jobs SET {} WHERE {}=?".format(sets_str, where)


def _pull_and_process(args, gpu_id=''):
    delay = random.randrange(1, 50) / 10
    print("Add random delay of %f seconds to prevent job overlaps." % delay)
    time.sleep(delay)

    conn, c = _get_or_create_db(args.db_name)
    c.execute('SELECT * FROM jobs WHERE status=?', (QUEUED,))
    try:
        (id, cmd, logfile, stat, tries, _, _) = c.fetchone()
        _commit_and_close(conn, c)
    except Exception as e:
        print("Couldn't pull any new jobs." + e.message)
        _commit_and_close(conn, c)
        return

    if tries >= args.max_failures:
        print("This job has failed.")
        conn, c = _get_or_create_db(args.db_name)
        update_str = _update_str('status')
        c.execute(update_str, (FAILED, id))
        _commit_and_close(conn, c)
        return

    print("Try {}/{} of job #{}: {}".format(tries + 1, args.max_failures, id, cmd))

    if cmd.startswith('"'): cmd = cmd[1:]
    if cmd.endswith('"'): cmd = cmd[:-1]

    try:
        # first, increment try counter
        conn, c = _get_or_create_db(args.db_name)
        update_str = _update_str(['status', 'tries'])
        c.execute(update_str, (PROCESSING, tries + 1, id))
        _commit_and_close(conn, c)

        log = subprocess.PIPE
        if len(logfile):
            log = open(logfile, "w+")

        proc = subprocess.Popen(shlex.split(cmd), stdout=log, stderr=subprocess.PIPE)

        conn, c = _get_or_create_db(args.db_name)
        host = "{}:{}:{}".format(_host(), gpu_id, proc.pid)
        update_str = _update_str(['host'])
        c.execute(update_str, (host, id))
        _commit_and_close(conn, c)

        while True:
            output = proc.stderr.readline()
            if output == '' and proc.poll() is not None:
                break
            if output:
                print('ERR>> ' + output.strip())
        rc = proc.poll()

        if rc == 0:
            conn, c = _get_or_create_db(args.db_name)
            update_str = _update_str(['status'])
            c.execute(update_str, (DONE, id))
            _commit_and_close(conn, c)
            print('Job done. Process ended with return code', rc)
        else:
            raise Exception("Job failed: failed with return code: "+str(rc))
    except Exception as e:
        print(e)

        print('Job failed. Process ended')
        conn, c = _get_or_create_db(args.db_name)
        update_str = _update_str('status')
        c.execute(update_str, (QUEUED, id))
        _commit_and_close(conn, c)



def _change_status(args, mode):
    status(args)

    selector = []
    if args.all:
        selector += ALL
    elif args.done:
        selector += [DONE]
    elif args.failed:
        selector += [FAILED]
    elif args.hold:
        selector += [HOLD]
    elif args.processing:
        selector += [PROCESSING]
    elif args.queued:
        selector += [QUEUED]
    else:
        selector += ALL

    selector = "('" + "','".join(selector) + "')"

    if _get_user_confirmation(
            "Are you sure that you want to set the status of all {} jobs to {}?".format(selector, mode)):
        conn, c = _get_or_create_db(args.db_name)
        sql_cmd = "UPDATE jobs SET status='{}', tries={}, time='{}' WHERE status IN {}".format(mode, 0, _time_str(),
                                                                                               selector)
        c.execute(sql_cmd)
        _commit_and_close(conn, c)
        print("All {} jobs set to {}.".format(selector, mode))

    status(args)


def _query_gpu():
    cmd_smi = 'nvidia-smi'
    subprocess.call(cmd_smi.split())

    gpu_id = _get_user_input("\nWhich GPU should be used? [0]", '0')  # , [str(x) for x in range(10)])

    return gpu_id


def _get_user_input(prompt, default, valid_values=None):
    while True:
        sys.stdout.write(prompt + '  ')
        choice = raw_input().lower()
        if default is not None and choice == '':
            return default
        elif valid_values is None or choice in valid_values:
            return choice
        else:
            sys.stdout.write("Invalid value.")


def _get_user_confirmation(query="Are you sure?"):
    confirm = 'no'
    try:
        confirm = raw_input(query + " Enter 'yes': ")
    except KeyboardInterrupt:
        print("\nNothing happened.")
    return confirm.lower() == 'yes'


def _print_table(cols, rows, print_status=True):
    max_len_jobname = 191

    if len(rows) == 0:
        return

    if print_status:
        stats = dict()
        for s in ALL:
            stats[s] = sum(1 for x in rows if x[2] == s)

    len_cmd = min(max(len(x[1]) for x in rows) + 5, max_len_jobname + 6)
    len_host = min(max([4] + [len(x[5]) for x in rows]), max_len_jobname + 6) + 2

    str_template = "{:<5}{:<" + str(len_cmd) + "}{:<20}{:<13}{:<7}{:<" + str(len_host) + "}{:<11}"

    print()
    header = str_template.format("ID", "COMMAND", "LOGFILE", "STATUS", "TRIES", "HOST:GPU:PID", "CHANGED")
    print(header)
    print("-" * len(header))

    for row in rows:
        (id, cmd, logfile, stat, tries, host, changed) = row

        host = host or ''
        cmd = ('...' + cmd[-max_len_jobname:]) if len(cmd) > max_len_jobname else cmd
        print(str_template.format(id, cmd, logfile, stat, tries, host, changed))
    print("-" * len(header))

    if print_status:
        for s in ALL:
            print("{:<3} {}".format(stats[s], s))
    print()


def add(args):
    """
    Adds one job from the command line.
    :param args: An argparse object containing the following properties:
            job: A string containing a command line command.
            db_name: A string containing the database filename.
    :return: Nothing
    """
    job = args.job
    print('Adding', job)
    conn, c = _get_or_create_db(args.db_name)
    status = HOLD if args.hold else QUEUED
    logfile = args.logfile or ""
    _add_single_job(c, job, logfile, status)
    _commit_and_close(conn, c)


def add_list(args):
    """
    Adds a number of jobs from an external text file. The file must contain one command per line.
    :param args: An argparse object containing the following properties:
            joblist: A string containing a valid path to a text file.
            db_name: A string containing a filename for the database.
    :return: Nothing.
    """
    joblist = args.joblist
    print('Adding jobs from', joblist)

    assert os.path.exists(joblist), "Joblist file not found: " + joblist

    with open(joblist) as f:
        lines = f.readlines()

    assert len(lines) > 0, "No commands found."

    status = HOLD if args.hold else QUEUED

    conn, c = _get_or_create_db(args.db_name)
    for job in lines:
        job = job.rstrip('\n')
        _add_single_job(c, job, status)
    _commit_and_close(conn, c)

    print("Added", len(lines), "jobs.")


def status(args):
    """
    Prints the current status of the database.
    :param args: An argparse object containing the following properties:
            db_name: A string containing a filename for the database.
    :return: Nothing.
    """

    if not os.path.exists(args.db_name):
        print("No job queue. Start by adding jobs.")
        return

    conn, c = _get_or_create_db(args.db_name)

    c.execute('SELECT * FROM jobs ORDER BY status')
    rows = c.fetchall()

    c.execute("PRAGMA table_info(jobs)")
    cols = c.fetchall()
    _commit_and_close(conn, c)

    _print_table(cols, rows)


def start(args):
    """
    Start the processing loop.
    :param args: An argparse object containing the following properties:
            db_name: A string containing a filename for the database.
            max_idle_minutes: Number of minutes to idle before quitting the processing loop.
    :return: Nothing.
    """
    assert os.path.exists(args.db_name), "No joblist found in {}. Please start with adding jobs.".format(args.db_name)

    gpu_id = args.gpu
    if args.gpu is None:
        gpu_id = _query_gpu()

    os.environ['CUDA_VISIBLE_DEVICES'] = gpu_id
    print('Set CUDA_VISIBLE_DEVICES to', str(os.environ['CUDA_VISIBLE_DEVICES']))

    num_queued = _check_for_queued_jobs(args.db_name)
    begin_idle_time = datetime.datetime.now()
    end_idle_time = begin_idle_time + datetime.timedelta(seconds=args.max_idle_minutes * 60)

    while num_queued > 0 or datetime.datetime.now() < end_idle_time:
        if num_queued > 0:
            _pull_and_process(args, gpu_id)
            begin_idle_time = datetime.datetime.now()
            end_idle_time = begin_idle_time + datetime.timedelta(seconds=args.max_idle_minutes * 60)
        else:
            str_end_time = end_idle_time.strftime("%H:%M")
            str_delta = str(end_idle_time - datetime.datetime.now())[:-7]
            print("No jobs queued. Waiting for new ones until {} ({} left).".format(str_end_time, str_delta))
            time.sleep(IDLE_CHECK_INTERVAL_MIN * 60)
        num_queued = _check_for_queued_jobs(args.db_name)


def queue(args):
    """
    Set the status of all specified jobs in the database to 'queued'.
    :param args: An argparse object containing the following properties:
            db_name: A string containing a filename for the database.
            optional any of these flags as filters:
                    --done
                    --processing
                    --failed
                    --hold
    :return: Nothing.
    """
    _change_status(args, QUEUED)


def hold(args):
    """
    Set the status of all specified jobs in the database to 'hold'.
    :param args: An argparse object containing the following properties:
            db_name: A string containing a filename for the database.
            optional any of these flags as filters:
                    --done
                    --processing
                    --failed
                    --hold
    :return: Nothing.
    """
    _change_status(args, HOLD)


def remove(args):
    ids_to_remove = args.remove_job_ids

    select_by = 'job_id'

    if any(',' in x for x in ids_to_remove):
        print('Use blanks as separator, e.g.: nebulight remove 1 2 3.')
        return

    if any(x in ALL for x in ids_to_remove):
        print('Remove all jobs that are ', ' or '.join(ids_to_remove))

        select_by = 'status'

    selector = "('" + "','".join(ids_to_remove) + "')"

    conn, c = _get_or_create_db(args.db_name)

    c.execute('SELECT * FROM jobs WHERE {} IN {}'.format(select_by, selector))
    rows = c.fetchall()

    if len(rows) == 0:
        print("No jobs matched your criteria.")
        return

    c.execute("PRAGMA table_info(jobs)")
    cols = c.fetchall()

    print("I will remove the following jobs. Currently running jobs will NOT be killed.")
    _commit_and_close(conn, c)

    _print_table(cols, rows, print_status=False)

    if _get_user_confirmation():
        conn, c = _get_or_create_db(args.db_name)

        c.execute('DELETE FROM jobs WHERE {} IN {}'.format(select_by, selector))

        _commit_and_close(conn, c)

    status(args)


if __name__ == '__main__':
    options_parser = argparse.ArgumentParser(prog="Options", add_help=False)
    options_parser.add_argument("--db_name",
                                help="Choose a specific name for the job database. Default: joblist.sqlite3",
                                default="joblist.sqlite3")

    parser = argparse.ArgumentParser(prog="NebuLight")
    subparsers = parser.add_subparsers(title="Actions")

    sp = subparsers.add_parser("add", help="Add a single job from the command line to the queue.",
                               parents=[options_parser])
    sp.set_defaults(func=add)
    sp.add_argument('job', help='Command to execute.')
    sp.add_argument('--hold', help='Add with status hold, rather than queued.', action='store_true')
    sp.add_argument('--logfile', help='A file where logs will be stored.', type=str)

    sp = subparsers.add_parser("add_list", help="Add a list of jobs (one per line) from a file to the queue.",
                               parents=[options_parser])
    sp.set_defaults(func=add_list)
    sp.add_argument('joblist', help='File containing commands to execute.')
    sp.add_argument('--hold', help='Add with status hold, rather than queued.', action='store_true')
    sp.add_argument('--logfile', help='A file where logs will be stored.', action='store_true')

    sp = subparsers.add_parser("status", help="Print the current job status.", parents=[options_parser])
    sp.set_defaults(func=status)

    sp = subparsers.add_parser("start", help="Start a worker instance locally.", parents=[options_parser])
    sp.add_argument('--max_idle_minutes', help='Maximum number of minutes to wait for new jobs before quitting.',
                    default=180, type=int)
    sp.add_argument("--gpu", help="Set CUDA_VISIBLE_DEVICES environment variable before execution.")
    sp.add_argument("--max_failures", help="Maximum number of failures for job before it is abandoned.", default=3)
    sp.set_defaults(func=start)

    sp = subparsers.add_parser("queue", help="Set all jobs to 'queued'.", parents=[options_parser])
    sp.add_argument("--all", help="Re-enqueue all jobs to status 'queued'.", action='store_true')
    sp.add_argument("--done", help="Re-enqueue all done jobs to status 'queued'.", action='store_true')
    sp.add_argument("--failed", help="Re-enqueue all failed jobs to status 'queued'.", action='store_true')
    sp.add_argument("--hold", help="Re-enqueue all held jobs to status 'queued'.", action='store_true')
    sp.add_argument("--processing", help="Re-enqueue all processing jobs to status 'queued'.", action='store_true')
    sp.set_defaults(func=queue)

    sp = subparsers.add_parser("hold", help="Set all jobs to 'hold'.", parents=[options_parser])
    sp.add_argument("--all", help="Set all jobs to status 'hold'.", action='store_true')
    sp.add_argument("--done", help="Set all done jobs to status 'hold'.", action='store_true')
    sp.add_argument("--failed", help="Set all failed jobs to status 'hold'.", action='store_true')
    sp.add_argument("--hold", help="Set all held jobs to status 'hold'.", action='store_true')
    sp.add_argument("--processing", help="Set all processing jobs to status 'hold'.", action='store_true')
    sp.add_argument("--queued", help="Set all queued jobs to status 'hold'.", action='store_true')
    sp.set_defaults(func=hold)

    sp = subparsers.add_parser("remove", help="Remove jobs by their ID..", parents=[options_parser])
    sp.add_argument("remove_job_ids",
                    help="One or more job IDs to remove, separated by spaces. Or pass a status like 'done'.", nargs='+')
    sp.set_defaults(func=remove)

    #argcomplete.autocomplete(parser)
    args = parser.parse_args()

    args.func(args)
