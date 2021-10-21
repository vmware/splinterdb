#!/usr/bin/env python3

import subprocess
import shlex
import pathlib
from datetime import datetime
import sys
import os
import pwd
import time

device = "nvme0n1p1"
base_trace_dir = "/mnt/traces/"

config_options = "--db-location /dev/nvme0n1p1 --set-O_DIRECT --unset-O_CREAT --memtable-capacity-mib 24 --fanout 8 --max-branches-per-node 24 --stats"

# get sudo user
def check_sudo_get_user():
    assert(os.geteuid() == 0)
    return os.environ["SUDO_USER"]

# write git branch, commit hash, commit message and diff to save_dir/version
def write_version_info(save_dir):
    git_branch_command = shlex.split("git branch --show-current")
    git_branch = subprocess.check_output(git_branch_command)
    git_command = shlex.split("git rev-list --format=%B --max-count=1 HEAD")
    git_info = subprocess.check_output(git_command)
    git_diff_command = shlex.split("git diff")
    git_diff = subprocess.check_output(git_diff_command)

    git_info_filename = save_dir + "/version"
    with open(git_info_filename, "wb") as git_info_file:
        git_info_file.write(git_branch)
        git_info_file.write(git_info)
        git_info_file.write(git_diff)

# get current io stats for the device and return reads, read bytes, writes
# and written bytes
def get_io_stats():
    diskstats_command = shlex.split("grep " + device + " /proc/diskstats")
    diskstats = subprocess.check_output(diskstats_command).decode(sys.stdout.encoding).split()
    reads_completed = int(diskstats[3])
    read_bytes = int(diskstats[5]) * 512
    writes_completed = int(diskstats[7])
    write_bytes = int(diskstats[9]) * 512
    return reads_completed, read_bytes, writes_completed, write_bytes

# read the op count for the trace from the .lc file if it exists and create
# one otherwise
def get_maybe_set_op_count(trace_filename):
    trace_op_count_filename = trace_filename + ".lc"
    trace_op_count_file = pathlib.Path(trace_op_count_filename)
    if (trace_op_count_file.is_file()):
        with open(trace_op_count_filename) as op_count_file:
            op_count = int(op_count_file.read())
    else:
        op_count_command = shlex.split("wc -l " + trace_filename)
        op_count = int(subprocess.check_output(op_count_command).split()[0])
        with open(trace_op_count_filename, "w") as op_count_file:
            op_count_file.write(str(op_count))
    return op_count

def bytes_to_str(byte_count):
    if (byte_count > 1024 * 1024 * 1024):
        return str(int(byte_count / 1024 / 1024 / 1024)) + "GiB"
    if (byte_count > 1024 * 1024):
        return str(int(byte_count / 1024 / 1024)) + "MiB"
    if (byte_count > 1024):
        return str(int(byte_count / 1024)) + "KiB"
    return str(int(byte_count)) + "B"

# key/value size to trace_dirname
def get_trace_dir_name(key_size, value_size):
    assert key_size == 24
    assert value_size == 100 or value_size == 1024
    if value_size == 100:
        return base_trace_dir + "80gib_24b_100b"
    else:
        return base_trace_dir + "80gib_24b_1kib"

# generate ycsb filename
def get_ycsb_string(save_dir, name, datetime_string, threads, mem, value_size):
    return "{}/{}-value_{}-thr_{}-mem_{}-{}.csv".format(save_dir, name,
            value_size, threads, bytes_to_str(mem), datetime_string)

# generate workload filename
def get_workload_string(phase, threads, mem, value_size):
    return phase + "-value_" + str(value_size) + "-thr_" + str(threads) + "-mem_" + bytes_to_str(mem)

class Benchmark:
    require_load = 1
    phases = []
    threads = []
    mem = []
    capacity = 0
    log = 1
    trace_dir_name = ""
    key_size = 24
    value_size = 100
    routing = 1

    def __init__(self, name, phases, threads, mem, capacity, key_size, value_size, routing, log=1):
        self.name = name
        assert(phases)
        self.phases = phases
        assert(threads)
        self.threads = threads
        assert(mem)
        self.mem = mem
        assert(capacity != 0)
        self.capacity = capacity
        self.trace_dir_name = get_trace_dir_name(key_size, value_size)
        trace_dir = pathlib.Path(self.trace_dir_name)
        assert(trace_dir.is_dir())
        self.key_size = key_size
        self.value_size = value_size
        self.log = log
        self.routing = routing

    def run(self, save_dir):
        # check if sudo (req for cg ops), save user to chown later
        #username = check_sudo_get_user()

        # get date/time for labeling
        datetime_string = datetime.today().strftime('%Y_%m_%d_%H:%M')

        for mem in self.mem:
            for threads in self.threads:
                time.sleep(3)
                # create and open the csv file
                pathlib.Path(save_dir).mkdir(parents=True, exist_ok=True)
                ycsb_csv_name = get_ycsb_string(save_dir, self.name,
                        datetime_string, threads, mem, self.value_size);
                ycsb_csv_file = pathlib.Path(ycsb_csv_name)
                #assert(not ycsb_csv_file.is_file())
                with open(ycsb_csv_name, "w") as ycsb_csv:
                    ycsb_csv.write("Workload Throughput MeanLatency 99PLatency MaxLatency ReadAmp WriteAmp\n")

                    for phase in self.phases:
                        # create save_dir for this run's data
                        workload_string = get_workload_string(phase, threads, mem, self.value_size)
                        results_dir = "{}/{}_{}-{}".format(save_dir, self.name, workload_string, datetime_string)
                        pathlib.Path(results_dir).mkdir(parents=True, exist_ok=True)

                        # write git version info
                        write_version_info(results_dir)

                        # get trace filename and operation count
                        trace_filename = self.trace_dir_name + "/replay_" + phase
                        trace_file = pathlib.Path(trace_filename)
                        assert(trace_file.is_file())
                        op_count = get_maybe_set_op_count(trace_filename)

                        # record disk stats to compute usage/io amp
                        start_diskstats = get_io_stats()

                        # prepare the workload command
                        run_command = ["./bin/driver_test", "ycsb_test"]
                        data_prefix = results_dir + "/data"
                        run_command.append(data_prefix)
                        run_command.append(trace_filename)
                        run_command.append(str(threads))
                        run_command.append(str(op_count))
                        memory_mib = int(mem / 1024 / 1024)
                        run_command.append(str(memory_mib))
                        if phase != "load":
                            run_command.append("-e")

                        # prepare the config part of the command
                        run_command.extend(shlex.split(config_options))
                        run_command.append("--db-capacity-gib")
                        run_command.append(str(self.capacity))
                        run_command.append("--filter-remainder-size")
                        if (self.routing):
                            run_command.append("4")
                        else:
                            run_command.append("8")
                        run_command.append("--key-size")
                        run_command.append(str(self.key_size))
                        run_command.append("--data-size")
                        run_command.append(str(self.value_size + 2))
                        if (self.log):
                            run_command.append("--log")

                        # run the benchmark, print output if it crashes, save it otherwise
                        try:
                            print("Running benchmark {}_{} with {} threads and {} RAM".format(self.name, phase, threads, bytes_to_str(mem)))
                            print(" ".join(run_command))
                            workload_output = subprocess.check_output(run_command, stderr=subprocess.STDOUT)
                        except subprocess.CalledProcessError as e:
                            print("Benchmark failed\n")
                            print(e.output.decode())
                            sys.exit(1)

                        # write output to file
                        output_filename = results_dir + "/output"
                        with open(output_filename, "wb") as output_file:
                            output_file.write(workload_output)

                        # write io stats to file
                        end_diskstats = get_io_stats()
                        net_diskstats = tuple(map(lambda x, y: x - y, end_diskstats, start_diskstats))
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
                            latency    = int(float(stats_lines[4].split()[1]))
                            mean_latency = int(float(stats_lines[9].split()[1]))
                            perc_latency = int(float(stats_lines[11].split()[1]))
                            max_latency = int(float(stats_lines[14].split()[1]))

                        # write workload summary
                        summary_filename = results_dir + "/summary"
                        with open(summary_filename, "w") as summary_file:
                            summary_file.write("throughput:     {:.2f} Kops/sec\n".format(throughput / 1024))
                            summary_file.write("mean_latency:   {} ns\n".format(latency))
                            if phase == "e":
                                logical_data_size = (0.95 * 50 + 0.05) * op_count * (self.key_size + self.value_size)
                            else:
                                logical_data_size = op_count * (self.key_size + self.value_size)
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

                        # write phase data to csv
                        workload_name = phase.title()
                        ycsb_csv.write("{} {} {} {} {} {} {}\n".format(
                            workload_name, throughput, mean_latency,
                            perc_latency, max_latency, read_amp, write_amp))

                #chown_command = shlex.split("chown -R {}:{} {}".format(username, username, save_dir))
                #subprocess.run(chown_command)


# main

gib = 1024 * 1024 * 1024
small = gib
medium = 10 * gib
kvellsize = 25 * gib
large = 100 * gib
splintersize = 4 * gib

dbsize = 400

# usage: name, [workloads], [threads], [cache_size], [db_size], key_size, value_size, use_routing_filters
# name is built into the output file names
# each workload is appended to base_trace_dir/80gib_24b_[100b,1kib]/replay_ to build the trcae filenames
# use_routing_filters should be deprecated
ycsb = Benchmark("ycsb", ["load", "a", "b", "c", "u", "d", "f", "e"], [16], [splintersize], dbsize, 24, 100, 1)

# The parameter given to run is prepended to the output filenames
ycsb.run("output");

