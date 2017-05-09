# start many workers

import subprocess
import argparse
from concurrent.futures import ProcessPoolExecutor, as_completed
import time

def make_call(index):
    time.sleep(index*2)
    print('launching gpu'+str(index))

    proc = subprocess.Popen(['./nebulight.py', 'start', '--gpu=' + str(index)], stdout=subprocess.PIPE, stderr=subprocess.PIPE)

    while True:
        output = proc.stderr.readline()
        if output == '' and proc.poll() is not None:
            print('exit')
            break
        if output:
            print('>> ' + output.strip())

    return index


if __name__ == '__main__':

    parser = argparse.ArgumentParser(prog="start workers")
    parser.add_argument("--workers",
                        help="Number of workers. Default: 8",
                        default=8, type=int)

    args = parser.parse_args()

    print("starting "+ str(args.workers)+ " workers")

    executor = ProcessPoolExecutor(args.workers)

    futures = []

    for i in range(args.workers):
        futures.append(executor.submit(make_call, i))

    for x in as_completed(futures):
        print (x)
