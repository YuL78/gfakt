#*****************************************************************************
# Copyright (c) Dec 2013 Youcef Lemsafer
#
# Permission is hereby granted, free of charge, to any person obtaining a copy of
# this software and associated documentation files (the "Software"), to deal in
# the Software without restriction, including without limitation the rights to
# use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of
# the Software, and to permit persons to whom the Software is furnished to do so,
# subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS
# FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
# COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER
# IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
# CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
#*****************************************************************************
# Creation date: 2013.12.01
# Creator: Youcef Lemsafer
# What it is: A python driver for GPU enabled versions of GMP-ECM
#*****************************************************************************
import argparse
import logging
import re
import os
import threading
import subprocess
import hashlib
import queue

# *************************
GFAKT_NAME = "gfakt.py"
VERSION = "0.1.5"
# *************************

# ****************************
# Set up logging
# ****************************
logger = logging.getLogger('gfakt')
logger.setLevel(logging.DEBUG)

#*************************
# Command line definition
#*************************
cmd_parser = argparse.ArgumentParser(description='Python driver for GPU enabled versions of GMP-ECM.')
cmd_parser.add_argument('-v', '--verbose', help = 'Use verbose logging.', action='store_true', default = False)
cmd_parser.add_argument('-l', '--log_file', help = 'Log file name.', default = 'gfakt.log')
cmd_parser.add_argument('-c', '--curves', help = 'Number of curves to run.')
cmd_parser.add_argument('-d', '--devices', nargs='+', help='List of gpu devices to use.', type=int, required = True)
cmd_parser.add_argument('-one', help = 'Stop when a factor is found.', action='store_true', default = False)
cmd_parser.add_argument('-t', '--threads', help='number of CPU threads to use for stage 2.', type=int, required = True)
cmd_parser.add_argument('B1', help = 'B1 bound.')
cmd_parser.add_argument('B2', help = 'B2 bound.', nargs='?')
cmd_parser.add_argument('-N', '--numbers', help = 'List of numbers to factor.', nargs='+')
cmd_args = cmd_parser.parse_args()

# ****************************************************************************
# Returns true if file exists and is not empty otherwise returns false.
# ****************************************************************************
def file_exists_and_is_not_empty(file_name):
    try:
        with open(file_name) as file:
            file.seek(0, 2)
            if(file.tell() > 0):
                return True
    except IOError:
        return False
    return False

# ****************************************************************************
# ****************************************************************************
def get_last_gmp_ecm_exec_output(gmp_ecm_output_file, pattern):
    str = ''
    with open(gmp_ecm_output_file) as f:
        for line in f:
            if(line.startswith(pattern)):
                str = ''
            str = str + line
    return str
    

# ****************************************************************************
# Splits a file into N parts (without breaking the lines)
# file_path: path of the file to split
# n: desired number of parts
# ****************************************************************************
def split_file(file_path, n):
    logger.debug('Splitting file {0:s} into {1:d} parts'.format(file_path, n))
    file_names = []
    if( n == 1 ):
        file_names.append(file_path)
        return file_names

    with open(file_path, 'rb') as f:
        f.seek(0, os.SEEK_END)
        size_in_bytes = f.tell()
        f.seek(0, os.SEEK_SET)
        written_bytes_count = 0
        file_num = 0
        files = []
        for line in f:
            if (written_bytes_count == 0) or \
                (written_bytes_count > (size_in_bytes / n)):
                file_name  = file_path + '.{0:03d}'.format(file_num)
                file_names.append(file_name)
                s = open(file_name, 'wb')
                files.append(s)
                file_num = file_num + 1
                written_bytes_count = 0
            s.write(line)
            written_bytes_count = written_bytes_count + len(line)
        for file in files:
            file.close()
    
    return file_names


#****************************************************
# GPU work units queue
#****************************************************
gpu_wus_queue = queue.Queue()
#****************************************************
# CPU work units queue
#****************************************************
cpu_wus_queue = queue.Queue()

#****************************************************
# Class defining a GPU work unit
#****************************************************
class GpuWu:
    def __init__(self, id, number, curves, B1):
        self.id = id
        self.number = number
        self.curves = curves
        self.B1 = B1
        self.input_file = id + '.in'
        self.save_file = id + '_' + B1 + '.save'
        self.chkpnt_file = id + '_' + B1 + '.chkpnt'
        self.log_file = id + '_' + B1 + '.log'

    def __str__(self):
        return '{{id={0:s}, N={1:s}, curves={2:s}, B1={3:s}}}'\
                .format(self.id, self.number, self.curves, self.B1)


#*****************************************************
#*****************************************************
def push_gpu_wus(id, number, curves, B1):
    gpu_wu = GpuWu(id, number, curves, B1)
    # Create input file
    with open(gpu_wu.input_file, 'wb') as input_file:
        input_file.write(bytes(gpu_wu.number, 'ASCII'))
    logger.debug('Pushing GPU work unit ' + str(gpu_wu))
    gpu_wus_queue.put(gpu_wu)

#*****************************************************
#*****************************************************
class CpuWu:
    def __init__(self, id, number, B1, save_file):
        self.id = id
        self.number = number
        self.B1 = B1
        self.save_file = save_file
        self.output_file = save_file + '.out'
        self.return_code = -1
        self.process_id = -1

#*****************************************************
#*****************************************************
class GpuWuConsumer:
    def __init__(self, device_ids, cpu_threads_count):
        self.device_ids = device_ids
        self.cpu_threads_count = cpu_threads_count
        self.threads = []

    def run(self):
        # one thread per device
        for d in self.device_ids:
            t = threading.Thread(target = self.run_wus, args = (d,))
            t.start()
            self.threads.append(t)
        for t in self.threads:
            t.join()
        # Indicate end to CPU workers
        logger.debug('Pushing <EOF> CPU work unit')
        cpu_wus_queue.put(CpuWu('<EOF>', '0', '0', '<EOF>'))
        

    def run_wus(self, device_id):
        while (True):
            try:
                gpu_wu = gpu_wus_queue.get(False)
            except queue.Empty:
                return
            # Running the computation on the GPU
            logger.debug('Running on device {0:s}: {1:s}.'.format(str(device_id), str(gpu_wu)))
            cmd_line = 'gpu_ecm -v -gpu -gpudevice ' + str(device_id) \
                        + ' -gpucurves ' + str(gpu_wu.curves)   \
                       + ' -c ' + str(gpu_wu.curves)        \
                       + ' -one -inp ' + gpu_wu.input_file   \
                       + ' -save ' + gpu_wu.save_file    \
                       + ' -chkpnt ' + gpu_wu.chkpnt_file \
                       + ' ' + gpu_wu.B1 + ' 0'
            with open(gpu_wu.log_file, 'a') as output_f:
                proc = subprocess.Popen(cmd_line, stdout = output_f, stderr = output_f)
                logger.debug('[pid: {0:d}] {1:s}'.format(proc.pid, cmd_line))
                proc.wait()
                ret_code = proc.returncode
            
            logger.debug('The process [pid: {0:d}] exited with code {1:d}.'.format(proc.pid, ret_code))

            # Push result of stage 1 for processing by CPU
            if( ret_code == 0 ):
                # Split stage 1 save file and send parts for processing by available CPU threads
                # Note: the save file may be empty in case of error (some errors do not cause
                # the process to exit with non-zero return code).
                if( file_exists_and_is_not_empty(gpu_wu.save_file) ):
                    save_files = split_file(gpu_wu.save_file, self.cpu_threads_count)
                    for f in save_files:
                        cpu_wus_queue.put(CpuWu(gpu_wu.id, gpu_wu.number, gpu_wu.B1, f))
                else:
                    logger.info('The file `{0:s}\' does not exist or is empty.'.format(gpu_wu.save_file))
            else:
                if( ret_code & 2 ):
                    logger.info('Found factor in step 1:')
                elif( ret_code == 8 ):
                    logger.info('Found input number N:')
                else:
                    logger.info('Error while running GMP-ECM:')
                report = get_last_gmp_ecm_exec_output(gpu_wu.log_file, 'GMP-ECM ')
                logger.debug('\n' + report)
                if( not cmd_args.verbose ):
                    print(report)
                


# *****************************************************
# *****************************************************
class CpuWorker:
    def __init__(self, cpu_threads_count):
        self.cpu_threads_count = cpu_threads_count
        self.max_threads_sema = threading.Semaphore(cpu_threads_count)
        self.stage2_threads = []
        # Event dictionary used for terminating subprocesses:
        # Given a set of threads {t_i} working a number N
        # if one of the threads t_i finds a factor and the cofactor is PRP
        # the event self.evt[id of N] is set which causes the others threads
        # to be terminated.
        self.evt = {}

    def run(self):
        while( True ):
            cpu_wu = cpu_wus_queue.get()
            if( cpu_wu.id == '<EOF>' ):
                break
            if( cpu_wu.id not in self.evt ):
                self.evt[cpu_wu.id] = threading.Event()
            t = threading.Thread(target = self.run_stage2, args=(cpu_wu,))
            self.stage2_threads.append(t)
            t.start()
        for t in self.stage2_threads:
            t.join()

    def run_stage2(self, cpu_wu):
        with self.max_threads_sema, open(cpu_wu.output_file, 'a') as output_f:
            cmd_line = 'gpu_ecm -v' \
                    + ' -resume ' + cpu_wu.save_file \
                    + ' ' + cpu_wu.B1
            proc = subprocess.Popen(cmd_line, stdout = output_f, stderr = output_f)
            cpu_wu.process_id = proc.pid
            logger.debug('[pid:' + str(cpu_wu.process_id) + '] ' + cmd_line)
            while( True ):
                if(proc.poll() != None):
                    cpu_wu.return_code = proc.returncode
                    logger.debug('The process [pid:' + str(cpu_wu.process_id) + '] exited with code ' + str(cpu_wu.return_code))
                    # The bit 3 is set when the cofactor is PRP, the return code is 8 when the input number
                    # is found hence the following test
                    if( (cpu_wu.return_code & 8) and (cpu_wu.return_code != 8) ):
                        self.evt[cpu_wu.id].set()
                    if( cpu_wu.return_code & 2 ):
                        report = get_last_gmp_ecm_exec_output(cpu_wu.output_file, 'Resuming ')
                        logger.info('Factor found in step 2:\n{0:s}'.format(report))
                    break
                if(self.evt[cpu_wu.id].is_set()):
                    logger.debug('Killing process [pid:' + str(cpu_wu.process_id) + '].')
                    proc.kill()
                    break

    

#*****************************************************
# main
#*****************************************************
def main():
    logger.info('{0:s} version {1:s}.'.format(GFAKT_NAME, VERSION))
    logger.info('Written by Youcef Lemsafer (Dec 2013).')

    for n in cmd_args.numbers:
        number = n
        m = re.match('^(.*?):(.*)', n)
        if (m):
            id = m.group(1)
            number = m.group(2)
        else:
            id = hashlib.sha224(n.encode()).hexdigest()[-8:]
        logger.debug('Got number id={0:s}, N={1:s}'.format(id, number))
        push_gpu_wus(id, number, cmd_args.curves, cmd_args.B1)

    gpu_wus_consumer = GpuWuConsumer(cmd_args.devices, cmd_args.threads)
    gpu_wus_consumer.run()
    cpu_worker = CpuWorker(cmd_args.threads)
    cpu_worker.run()


#****************************************************
# Set up log handlers according to verbosity
log_level = logging.DEBUG if cmd_args.verbose else logging.INFO
console_handler = logging.StreamHandler()
console_handler.setLevel(log_level)
console_handler.setFormatter(logging.Formatter('|-> %(message)s'))
file_handler = logging.FileHandler(cmd_args.log_file)
file_handler.setFormatter(logging.Formatter(
                            '|-> %(asctime)-15s | %(message)s'))
# Always use DEBUG level when logging to file
file_handler.setLevel(logging.DEBUG)
logger.addHandler(console_handler)
logger.addHandler(file_handler)

#****************************************************
main()
