#!/usr/bin/env python3
""" Script to perform a collection of trace replays on SplinterDB"""

import subprocess
import shlex
import pathlib
from datetime import datetime
import sys
import time
import argparse

def write_version_info(out_dir):
    """write git version info including diff to version file"""
    git_branch_command = shlex.split("git branch --show-current")
    git_branch = subprocess.check_output(git_branch_command)
    git_command = shlex.split("git rev-list --format=%B --max-count=1 HEAD")
    git_info = subprocess.check_output(git_command)
    git_diff_command = shlex.split("git diff")
    git_diff = subprocess.check_output(git_diff_command)

    git_info_filename = out_dir + "/version"
    with open(git_info_filename, "wb") as git_info_file:
        git_info_file.write(git_branch)
        git_info_file.write(git_info)
        git_info_file.write(git_diff)

def get_io_stats(dev_name):
    """read the current io stats for the DEVICE"""
    diskstats_command = shlex.split("grep " + dev_name + " /proc/diskstats")
    diskstats = subprocess.check_output(
        diskstats_command).decode(sys.stdout.encoding).split()
    reads_completed = int(diskstats[3])
    read_bytes = int(diskstats[5]) * 512
    writes_completed = int(diskstats[7])
    write_bytes = int(diskstats[9]) * 512
    return reads_completed, read_bytes, writes_completed, write_bytes

def get_maybe_set_op_count(filename):
    """return the number of operations in the trace and cache in a .lc file"""
    trace_op_count_filename = filename + ".lc"
    trace_op_count_file = pathlib.Path(trace_op_count_filename)
    if trace_op_count_file.is_file():
        with open(trace_op_count_filename) as op_count_file:
            count = int(op_count_file.read())
    else:
        op_count_command = shlex.split("wc -l " + filename)
        count = int(subprocess.check_output(op_count_command).split()[0])
        with open(trace_op_count_filename, "w") as op_count_file:
            op_count_file.write(str(count))
    return count

def bytes_to_str(byte_count):
    """pretty print string for bytes"""
    if byte_count > 1024 * 1024 * 1024:
        return str(int(byte_count / 1024 / 1024 / 1024)) + "GiB"
    if byte_count > 1024 * 1024:
        return str(int(byte_count / 1024 / 1024)) + "MiB"
    if byte_count > 1024:
        return str(int(byte_count / 1024)) + "KiB"
    return str(int(byte_count)) + "B"

# generate ycsb filename
def get_ycsb_string(out_dir, datetime_str, threads, mem_size, value_size):
    """generate output filename for result file"""
    return "{}/results_value_{}-thr_{}-mem_{}-{}.csv".format(
        out_dir, value_size, threads, bytes_to_str(mem_size), datetime_str)

def get_workload_string(filename, threads, mem_size, value_size):
    """generate output filename for workload (trace)"""
    return filename + "-value_" + str(value_size) + "-thr_" + str(threads) + "-mem_"\
            + bytes_to_str(mem_size)



# main

# parse commmand line arguments
DESC = "SplinterDB Trace Replayer. Replays all traces in trace_list and outputs statistics to \
        output_dir. If --memory and/or --threads are set, then runs all traces for each \
        combination of thread_count and/or memory_size."
CL_PARSER = argparse.ArgumentParser(description=DESC, allow_abbrev=False)
CL_PARSER.add_argument('output_dir', help="Directory to write output to, will be created if it "\
                                          "doesn't exist. Safe to reuse.")
CL_PARSER.add_argument('trace_list', nargs="+", type=str,
                       help="List of trace file names, also used to label output files")
CL_PARSER.add_argument('--trace_dir', type=str, default=".",
                       help="Directory where traces are located")
USE_EXISTING_HELP = "Run all traces on an existing database. If unset, the first trace will \
                     create a new database and subsequent traces will run on that database."
CL_PARSER.add_argument('--use_existing', action="store_true", help=USE_EXISTING_HELP)
CL_PARSER.add_argument('--threads', nargs="+", type=int, default=[1],
                       help="List of thread counts to use, each will be run")
CL_PARSER.add_argument('--memory', nargs="+", type=int, default=[4],
                       help="List of memory sizes to use in GiB, each will be run")
CL_PARSER.add_argument('--db_size', type=int, default=400, help="Max size of database file/device.")
CL_PARSER.add_argument('--key_size', type=int, default=24, help="Size of keys in trace.")
CL_PARSER.add_argument('--message_size', type=int, default=100, help="Size of messages in trace.")
CL_PARSER.add_argument('--config', type=str, default="",
                       help="Config options to pass to SplinterDB")
CL_PARSER.add_argument('--device', type=str, default="",
                       help="Optional name of DEVICE that the db is located on for IO stats")
ARGS = CL_PARSER.parse_args()

GIB = 1024 * 1024 * 1024

# clean up variables
SAVE_DIR = ARGS.output_dir
TRACE_FILENAMES = ARGS.trace_list
TRACE_DIR = ARGS.trace_dir
USE_EXISTING = ARGS.use_existing
THREAD_COUNTS = ARGS.threads
MEM_SIZES = [x * GIB for x in ARGS.memory]
DB_SIZE = ARGS.db_size
KEY_SIZE = ARGS.key_size
MESSAGE_SIZE = ARGS.message_size
CONFIG_OPTIONS = ARGS.config
DEVICE = ARGS.device

# get date/time for labeling
DATETIME_STRING = datetime.today().strftime('%Y_%m_%d_%H:%M')

for mem in MEM_SIZES:
    for thread_count in THREAD_COUNTS:
        time.sleep(3)
        # create and open the csv file
        pathlib.Path(SAVE_DIR).mkdir(parents=True, exist_ok=True)
        ycsb_csv_name = get_ycsb_string(SAVE_DIR, DATETIME_STRING, thread_count, mem, MESSAGE_SIZE)
        with open(ycsb_csv_name, "w") as ycsb_csv:
            if DEVICE:
                ycsb_csv.write(
                    "Workload Throughput MeanLatency 99PLatency MaxLatency ReadAmp WriteAmp\n")
            else:
                ycsb_csv.write("Workload Throughput MeanLatency 99PLatency MaxLatency\n")

            for i, trace_filename in enumerate(TRACE_FILENAMES):
                # create SAVE_DIR for this run's data
                workload_string = get_workload_string(
                    trace_filename, thread_count, mem, MESSAGE_SIZE)
                results_dir = "{}/{}-{}".format(SAVE_DIR, workload_string, DATETIME_STRING)
                pathlib.Path(results_dir).mkdir(parents=True, exist_ok=True)

                # write git version info
                write_version_info(results_dir)

                # get trace filename and operation count
                trace_filename = TRACE_DIR + "/" + trace_filename
                trace_file = pathlib.Path(trace_filename)
                print(trace_filename)
                assert trace_file.is_file()
                op_count = get_maybe_set_op_count(trace_filename)

                # record disk stats to compute usage/io amp
                if DEVICE:
                    start_diskstats = get_io_stats(DEVICE)

                # prepare the workload command
                run_command = ["./bin/driver_test", "ycsb_test"]
                data_prefix = results_dir + "/data"
                run_command.append(data_prefix)
                run_command.append(trace_filename)
                run_command.append(str(thread_count))
                run_command.append(str(op_count))
                memory_mib = int(mem / 1024 / 1024)
                run_command.append(str(memory_mib))
                if USE_EXISTING or i != 0:
                    run_command.append("-e")

                # prepare the config part of the command
                run_command.extend(shlex.split(CONFIG_OPTIONS))
                run_command.append("--db-capacity-gib")
                run_command.append(str(DB_SIZE))
                run_command.append("--filter-remainder-size")
                run_command.append("4")
                run_command.append("--key-size")
                run_command.append(str(KEY_SIZE))
                run_command.append("--data-size")
                run_command.append(str(MESSAGE_SIZE + 2))

                # run the benchmark, print output if it crashes, save it otherwise
                try:
                    print("Running benchmark {} with {} threads and {} RAM".format(
                        trace_filename, thread_count, bytes_to_str(mem)))
                    print(" ".join(run_command))
                    workload_output = subprocess.check_output(run_command, stderr=subprocess.STDOUT)
                except subprocess.CalledProcessError as error:
                    print("Benchmark failed\n")
                    print(error.output.decode())
                    sys.exit(1)

                # write output to file
                output_filename = results_dir + "/output"
                with open(output_filename, "wb") as output_file:
                    output_file.write(workload_output)

                # write io stats to file
                if DEVICE:
                    end_diskstats = get_io_stats(DEVICE)
                    net_diskstats = tuple(
                        map(lambda x, y: x - y, end_diskstats, start_diskstats))
                    io_filename = results_dir + "/io"
                    with open(io_filename, "w") as io_file:
                        io_file.write("reads_completed: {}\n".format(net_diskstats[0]))
                        io_file.write("bytes_read: {}\n".format(net_diskstats[1]))
                        io_file.write("writes_completed: {}\n".format(net_diskstats[2]))
                        io_file.write("bytes_written: {}\n".format(net_diskstats[3]))

                # calculate headline statistics and write to file
                stats_filename = data_prefix + ".statistics"
                with open(stats_filename) as stats_file:
                    stats_lines = stats_file.readlines()
                    clock_time = int(stats_lines[1].split()[1])
                    throughput = float(stats_lines[5].split()[1])
                    latency = int(float(stats_lines[4].split()[1]))
                    mean_latency = int(float(stats_lines[9].split()[1]))
                    perc_latency = int(float(stats_lines[11].split()[1]))
                    max_latency = int(float(stats_lines[14].split()[1]))

                # write workload summary
                summary_filename = results_dir + "/summary"
                with open(summary_filename, "w") as summary_file:
                    summary_file.write(
                        "throughput:     {:.2f} Kops/sec\n".format(throughput / 1024))
                    summary_file.write("mean_latency:   {} ns\n".format(latency))
                    if DEVICE:
                        if trace_filename == "e":
                            logical_data_size = \
                                (0.95 * 50 + 0.05) * op_count * (KEY_SIZE + MESSAGE_SIZE)
                        else:
                            logical_data_size = op_count * (KEY_SIZE + MESSAGE_SIZE)
                        write_amp = float(net_diskstats[3]) / logical_data_size
                        summary_file.write("write_amp:      {:.2f}\n".format(write_amp))
                        read_amp = float(net_diskstats[1]) / logical_data_size
                        summary_file.write("read_amp:       {:.2f}\n".format(read_amp))
                        io_amp = write_amp + read_amp
                        summary_file.write("io_amp:         {:.2f}\n".format(io_amp))
                        io_mib = (net_diskstats[1] + net_diskstats[3]) / 1024 / 1024
                        clock_time_sec = clock_time / 1000 / 1000 / 1000
                        bandwidth_used = int(io_mib/clock_time_sec)
                        summary_file.write("bandwidth_used: {} MiB/sec\n".format(bandwidth_used))

                        # write trace_filename data to csv
                workload_name = trace_filename.title()
                if DEVICE:
                    ycsb_csv.write("{} {} {} {} {} {} {}\n".format(
                        workload_name, throughput, mean_latency,
                        perc_latency, max_latency, read_amp, write_amp))
                else:
                    ycsb_csv.write("{} {} {} {} {}\n".format(
                        workload_name, throughput, mean_latency,
                        perc_latency, max_latency))
