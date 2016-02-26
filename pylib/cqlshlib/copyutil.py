# cython: profile=True

# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import ConfigParser
import csv
import datetime
import json
import glob
import multiprocessing as mp
import os
import platform
import random
import re
import struct
import sys
import time
import traceback

from bisect import bisect_right
from calendar import timegm
from collections import defaultdict, namedtuple
from decimal import Decimal
from random import randrange
from StringIO import StringIO
from select import select
from threading import Lock
from uuid import UUID
from util import profile_on, profile_off

from cassandra.cluster import Cluster
from cassandra.cqltypes import ReversedType, UserType
from cassandra.metadata import protect_name, protect_names, protect_value
from cassandra.policies import RetryPolicy, WhiteListRoundRobinPolicy, DCAwareRoundRobinPolicy
from cassandra.query import BatchStatement, BatchType, SimpleStatement, tuple_factory
from cassandra.util import Date, Time

try:
    from cassandra.io.libevreactor import LibevConnection
except ImportError:
    LibevConnection = None

from cql3handling import CqlRuleSet
from displaying import NO_COLOR_MAP
from formatting import format_value_default, EMPTY, get_formatter
from sslhandling import ssl_settings

PROFILE_ON = False
STRACE_ON = False
IS_LINUX = platform.system() == 'Linux'

CopyOptions = namedtuple('CopyOptions', 'copy dialect unrecognized')


def safe_normpath(fname):
    """
    :return the normalized path but only if there is a filename, we don't want to convert
    an empty string (which means no file name) to a dot. Also expand any user variables such as ~ to the full path
    """
    return os.path.normpath(os.path.expanduser(fname)) if fname else fname


class OneWayChannel(object):
    """
    A one way pipe protected by two process level locks, one for reading and one for writing.
    """
    def __init__(self):
        self.reader, self.writer = mp.Pipe(duplex=False)
        self.rlock = mp.Lock()
        self.wlock = mp.Lock()

    def send(self, obj):
        with self.wlock:
            self.writer.send(obj)

    def recv(self):
        with self.rlock:
            return self.reader.recv()

    def close(self):
        self.reader.close()
        self.writer.close()


class OneWayChannels(object):
    """
    A group of one way channels.
    """
    def __init__(self, num_channels):
        self.channels = [OneWayChannel() for _ in xrange(num_channels)]
        self._readers = [ch.reader for ch in self.channels]
        self._rlocks = [ch.rlock for ch in self.channels]
        self._rlocks_by_readers = dict([(ch.reader, ch.rlock) for ch in self.channels])
        self.num_channels = num_channels

        self.recv = self.recv_select if IS_LINUX else self.recv_polling

    def recv_select(self, timeout):
        """
        Implementation of the recv method for Linux, where select is available. Receive an object from
        all pipes that are ready for reading without blocking.
        """
        readable, _, _ = select(self._readers, [], [], timeout)
        for r in readable:
            with self._rlocks_by_readers[r]:
                try:
                    yield r.recv()
                except EOFError:
                    continue

    def recv_polling(self, timeout):
        """
        Implementation of the recv method for platforms where select() is not available for pipes.
        We poll on all of the readers with a very small timeout. We stop when the timeout specified
        has been received but we may exceed it since we check all processes during each sweep.
        """
        start = time.time()
        while True:
            for i, r in enumerate(self._readers):
                with self._rlocks[i]:
                    if r.poll(0.000000001):
                        try:
                            yield r.recv()
                        except EOFError:
                            continue

            if time.time() - start > timeout:
                break

    def close(self):
        for ch in self.channels:
            try:
                ch.close()
            except:
                pass


class CopyTask(object):
    """
    A base class for ImportTask and ExportTask
    """
    def __init__(self, shell, ks, table, columns, fname, opts, protocol_version, config_file, direction):
        self.shell = shell
        self.ks = ks
        self.table = table
        self.local_dc = shell.conn.metadata.get_host(shell.hostname).datacenter
        self.fname = safe_normpath(fname)
        self.protocol_version = protocol_version
        self.config_file = config_file
        # do not display messages when exporting to STDOUT
        self.printmsg = self._printmsg if self.fname is not None or direction == 'from' else lambda _, eol='\n': None
        self.options = self.parse_options(opts, direction)

        self.num_processes = self.options.copy['numprocesses']
        if direction == 'in':
            self.num_processes += 1  # add the feeder process

        self.printmsg('Using %d child processes' % (self.num_processes,))

        self.processes = []
        self.inmsg = OneWayChannels(self.num_processes)
        self.outmsg = OneWayChannels(self.num_processes)

        self.columns = CopyTask.get_columns(shell, ks, table, columns)
        self.time_start = time.time()

    @staticmethod
    def _printmsg(msg, eol='\n'):
        sys.stdout.write(msg + eol)
        sys.stdout.flush()

    def maybe_read_config_file(self, opts, direction):
        """
        Read optional sections from a configuration file that  was specified in the command options or from the default
        cqlshrc configuration file if none was specified.
        """
        config_file = opts.pop('configfile', '')
        if not config_file:
            config_file = self.config_file

        if not os.path.isfile(config_file):
            return opts

        configs = ConfigParser.RawConfigParser()
        configs.readfp(open(config_file))

        ret = dict()
        config_sections = list(['copy', 'copy-%s' % (direction,),
                                'copy:%s.%s' % (self.ks, self.table),
                                'copy-%s:%s.%s' % (direction, self.ks, self.table)])

        for section in config_sections:
            if configs.has_section(section):
                options = dict(configs.items(section))
                self.printmsg("Reading options from %s:[%s]: %s" % (config_file, section, options))
                ret.update(options)

        # Update this last so the command line options take precedence over the configuration file options
        if opts:
            self.printmsg("Reading options from the command line: %s" % (opts,))
            ret.update(opts)

        if self.shell.debug:  # this is important for testing, do not remove
            self.printmsg("Using options: '%s'" % (ret,))

        return ret

    @staticmethod
    def clean_options(opts):
        """
        Convert all option values to valid string literals unless they are path names
        """
        return dict([(k, v.decode('string_escape') if k not in ['errfile', 'ratefile'] else v)
                     for k, v, in opts.iteritems()])

    def parse_options(self, opts, direction):
        """
        Parse options for import (COPY FROM) and export (COPY TO) operations.
        Extract from opts csv and dialect options.

        :return: 3 dictionaries: the csv options, the dialect options, any unrecognized options.
        """
        shell = self.shell
        opts = self.clean_options(self.maybe_read_config_file(opts, direction))

        dialect_options = dict()
        dialect_options['quotechar'] = opts.pop('quote', '"')
        dialect_options['escapechar'] = opts.pop('escape', '\\')
        dialect_options['delimiter'] = opts.pop('delimiter', ',')
        if dialect_options['quotechar'] == dialect_options['escapechar']:
            dialect_options['doublequote'] = True
            del dialect_options['escapechar']
        else:
            dialect_options['doublequote'] = False

        copy_options = dict()
        copy_options['nullval'] = opts.pop('null', '')
        copy_options['header'] = bool(opts.pop('header', '').lower() == 'true')
        copy_options['encoding'] = opts.pop('encoding', 'utf8')
        copy_options['maxrequests'] = int(opts.pop('maxrequests', 6))
        copy_options['pagesize'] = int(opts.pop('pagesize', 1000))
        # by default the page timeout is 10 seconds per 1000 entries
        # in the page size or 10 seconds if pagesize is smaller
        copy_options['pagetimeout'] = int(opts.pop('pagetimeout', max(10, 10 * (copy_options['pagesize'] / 1000))))
        copy_options['maxattempts'] = int(opts.pop('maxattempts', 5))
        copy_options['dtformats'] = opts.pop('datetimeformat', shell.display_time_format)
        copy_options['float_precision'] = shell.display_float_precision
        copy_options['chunksize'] = int(opts.pop('chunksize', 5000))
        copy_options['ingestrate'] = int(opts.pop('ingestrate', 200000))
        copy_options['maxbatchsize'] = int(opts.pop('maxbatchsize', 20))
        copy_options['minbatchsize'] = int(opts.pop('minbatchsize', 10))
        copy_options['reportfrequency'] = float(opts.pop('reportfrequency', 0.25))
        copy_options['consistencylevel'] = shell.consistency_level
        copy_options['decimalsep'] = opts.pop('decimalsep', '.')
        copy_options['thousandssep'] = opts.pop('thousandssep', '')
        copy_options['boolstyle'] = [s.strip() for s in opts.pop('boolstyle', 'True, False').split(',')]
        copy_options['numprocesses'] = int(opts.pop('numprocesses', self.get_num_processes()))
        copy_options['begintoken'] = opts.pop('begintoken', '')
        copy_options['endtoken'] = opts.pop('endtoken', '')
        copy_options['maxrows'] = int(opts.pop('maxrows', '-1'))
        copy_options['skiprows'] = int(opts.pop('skiprows', '0'))
        copy_options['skipcols'] = opts.pop('skipcols', '')
        copy_options['maxparseerrors'] = int(opts.pop('maxparseerrors', '-1'))
        copy_options['maxinserterrors'] = int(opts.pop('maxinserterrors', '-1'))
        copy_options['errfile'] = safe_normpath(opts.pop('errfile', 'import_%s_%s.err' % (self.ks, self.table,)))
        copy_options['ratefile'] = safe_normpath(opts.pop('ratefile', ''))
        copy_options['maxoutputsize'] = int(opts.pop('maxoutputsize', '-1'))
        copy_options['preparedstatements'] = bool(opts.pop('preparedstatements', 'true').lower() == 'true')

        self.check_options(copy_options)
        return CopyOptions(copy=copy_options, dialect=dialect_options, unrecognized=opts)

    @staticmethod
    def check_options(copy_options):
        """
        Check any options that require a sanity check beyond a simple type conversion and if required
        raise a value error:

        - boolean styles must be exactly 2, they must be different and they cannot be empty
        """
        bool_styles = copy_options['boolstyle']
        if len(bool_styles) != 2 or bool_styles[0] == bool_styles[1] or not bool_styles[0] or not bool_styles[1]:
            raise ValueError("Invalid boolean styles %s" % copy_options['boolstyle'])

    @staticmethod
    def get_num_processes():
        """
        Pick a reasonable number of child processes. We need to leave at
        least one core for the parent process.
        """
        return max(1, CopyTask.get_num_cores() - 1)

    @staticmethod
    def get_num_cores():
        """
        Return the number of cores if available.
        """
        try:
            return mp.cpu_count()
        except NotImplementedError:
            return 1

    @staticmethod
    def describe_interval(seconds):
        desc = []
        for length, unit in ((86400, 'day'), (3600, 'hour'), (60, 'minute')):
            num = int(seconds) / length
            if num > 0:
                desc.append('%d %s' % (num, unit))
                if num > 1:
                    desc[-1] += 's'
            seconds %= length
        words = '%.03f seconds' % seconds
        if len(desc) > 1:
            words = ', '.join(desc) + ', and ' + words
        elif len(desc) == 1:
            words = desc[0] + ' and ' + words
        return words

    @staticmethod
    def get_columns(shell, ks, table, columns):
        """
        Return all columns if none were specified or only the columns specified.
        Possible enhancement: introduce a regex like syntax (^) to allow users
        to specify all columns except a few.
        """
        return shell.get_column_names(ks, table) if not columns else columns

    def close(self):
        self.stop_processes()
        self.inmsg.close()
        self.outmsg.close()

    def num_live_processes(self):
        return sum(1 for p in self.processes if p.is_alive())

    @staticmethod
    def get_pid():
        return os.getpid() if hasattr(os, 'getpid') else None

    @staticmethod
    def trace_process(pid):
        if pid and STRACE_ON:
            os.system("strace -vvvv -c -o strace.{pid}.out -e trace=all -p {pid}&".format(pid=pid))

    def start_processes(self):
        for i, process in enumerate(self.processes):
            process.start()
            self.trace_process(process.pid)

        self.trace_process(self.get_pid())

    def stop_processes(self):
        for process in self.processes:
            process.terminate()

    def make_params(self):
        """
        Return a dictionary of parameters to be used by the worker processes.
        On Windows this dictionary must be pickle-able.
        """
        shell = self.shell
        return dict(ks=self.ks,
                    table=self.table,
                    local_dc=self.local_dc,
                    columns=self.columns,
                    options=self.options,
                    connect_timeout=shell.conn.connect_timeout,
                    hostname=shell.hostname,
                    port=shell.port,
                    ssl=shell.ssl,
                    auth_provider=shell.auth_provider,
                    cql_version=shell.conn.cql_version,
                    config_file=self.config_file,
                    protocol_version=self.protocol_version,
                    debug=shell.debug
                    )

    def update_params(self, params, i):
        """
        Add the communication channels to the parameters to be passed to the worker process:
            inmsg is the message queue flowing from parent to child process, so outmsg from the parent point
            of view and, vice-versa,  outmsg is the message queue flowing from child to parent, so inmsg
            from the parent point of view, hence the two are swapped below.
        """
        params['inmsg'] = self.outmsg.channels[i]
        params['outmsg'] = self.inmsg.channels[i]
        return params


class ExportWriter(object):
    """
    A class that writes to one or more csv files, or STDOUT
    """

    def __init__(self, fname, shell, columns, options):
        self.fname = fname
        self.shell = shell
        self.columns = columns
        self.options = options
        self.header = options.copy['header']
        self.max_output_size = long(options.copy['maxoutputsize'])
        self.current_dest = None
        self.num_files = 0

        if self.max_output_size > 0:
            if fname is not None:
                self.write = self._write_with_split
                self.num_written = 0
            else:
                shell.printerr("WARNING: maxoutputsize {} ignored when writing to STDOUT".format(self.max_output_size))
                self.write = self._write_without_split
        else:
            self.write = self._write_without_split

    def open(self):
        self.current_dest = self._get_dest(self.fname)
        if self.current_dest is None:
            return False

        if self.header:
            writer = csv.writer(self.current_dest.output, **self.options.dialect)
            writer.writerow(self.columns)

        return True

    def close(self):
        self._close_current_dest()

    def _next_dest(self):
        self._close_current_dest()
        self.current_dest = self._get_dest(self.fname + '.%d' % (self.num_files,))

    def _get_dest(self, source_name):
        """
        Open the output file if any or else use stdout. Return a namedtuple
        containing the out and a boolean indicating if the output should be closed.
        """
        CsvDest = namedtuple('CsvDest', 'output close')

        if self.fname is None:
            return CsvDest(output=sys.stdout, close=False)
        else:
            try:
                ret = CsvDest(output=open(source_name, 'wb'), close=True)
                self.num_files += 1
                return ret
            except IOError, e:
                self.shell.printerr("Can't open %r for writing: %s" % (source_name, e))
                return None

    def _close_current_dest(self):
        if self.current_dest and self.current_dest.close:
            self.current_dest.output.close()
            self.current_dest = None

    def _write_without_split(self, data, _):
        """
         Write the data to the current destination output.
        """
        self.current_dest.output.write(data)

    def _write_with_split(self, data, num):
        """
         Write the data to the current destination output if we still
         haven't reached the maximum number of rows. Otherwise split
         the rows between the current destination and the next.
        """
        if (self.num_written + num) > self.max_output_size:
            num_remaining = self.max_output_size - self.num_written
            last_switch = 0
            for i, row in enumerate(filter(None, data.split(os.linesep))):
                if i == num_remaining:
                    self._next_dest()
                    last_switch = i
                    num_remaining += self.max_output_size
                self.current_dest.output.write(row + '\n')

            self.num_written = num - last_switch
        else:
            self.num_written += num
            self.current_dest.output.write(data)


class ExportTask(CopyTask):
    """
    A class that exports data to .csv by instantiating one or more processes that work in parallel (ExportProcess).
    """
    def __init__(self, shell, ks, table, columns, fname, opts, protocol_version, config_file):
        CopyTask.__init__(self, shell, ks, table, columns, fname, opts, protocol_version, config_file, 'to')

        options = self.options
        self.begin_token = long(options.copy['begintoken']) if options.copy['begintoken'] else None
        self.end_token = long(options.copy['endtoken']) if options.copy['endtoken'] else None
        self.writer = ExportWriter(fname, shell, columns, options)

    def run(self):
        """
        Initiates the export by starting the worker processes.
        Then hand over control to export_records.
        """
        shell = self.shell

        if self.options.unrecognized:
            shell.printerr('Unrecognized COPY TO options: %s' % ', '.join(self.options.unrecognized.keys()))
            return

        if not self.columns:
            shell.printerr("No column specified")
            return 0

        ranges = self.get_ranges()
        if not ranges:
            return 0

        if not self.writer.open():
            return 0

        self.printmsg("\nStarting copy of %s.%s with columns %s." % (self.ks, self.table, self.columns))

        params = self.make_params()
        for i in xrange(self.num_processes):
            self.processes.append(ExportProcess(self.update_params(params, i)))

        self.start_processes()

        try:
            self.export_records(ranges)
        finally:
            self.close()

    def close(self):
        CopyTask.close(self)
        self.writer.close()

    def get_ranges(self):
        """
        return a queue of tuples, where the first tuple entry is a token range (from, to]
        and the second entry is a list of hosts that own that range. Each host is responsible
        for all the tokens in the range (from, to].

        The ring information comes from the driver metadata token map, which is built by
        querying System.PEERS.

        We only consider replicas that are in the local datacenter. If there are no local replicas
        we use the cqlsh session host.
        """
        shell = self.shell
        hostname = shell.hostname
        local_dc = self.local_dc
        ranges = dict()
        min_token = self.get_min_token()
        begin_token = self.begin_token
        end_token = self.end_token

        def make_range(prev, curr):
            """
            Return the intersection of (prev, curr) and (begin_token, end_token),
            return None if the intersection is empty
            """
            ret = (prev, curr)
            if begin_token:
                if ret[1] < begin_token:
                    return None
                elif ret[0] < begin_token:
                    ret = (begin_token, ret[1])

            if end_token:
                if ret[0] > end_token:
                    return None
                elif ret[1] > end_token:
                    ret = (ret[0], end_token)

            return ret

        def make_range_data(replicas=None):
            hosts = []
            if replicas:
                for r in replicas:
                    if r.is_up and r.datacenter == local_dc:
                        hosts.append(r.address)
            if not hosts:
                hosts.append(hostname)  # fallback to default host if no replicas in current dc
            return {'hosts': tuple(hosts), 'attempts': 0, 'rows': 0}

        if begin_token and begin_token < min_token:
            shell.printerr('Begin token %d must be bigger or equal to min token %d' % (begin_token, min_token))
            return ranges

        if begin_token and end_token and begin_token > end_token:
            shell.printerr('Begin token %d must be smaller than end token %d' % (begin_token, end_token))
            return ranges

        if shell.conn.metadata.token_map is None or min_token is None:
            ranges[(begin_token, end_token)] = make_range_data()
            return ranges

        ring = shell.get_ring(self.ks).items()
        ring.sort()

        if not ring:
            #  If the ring is empty we get the entire ring from the host we are currently connected to
            ranges[(begin_token, end_token)] = make_range_data()
        elif len(ring) == 1:
            #  If there is only one token we get the entire ring from the replicas for that token
            ranges[(begin_token, end_token)] = make_range_data(ring[0][1])
        else:
            # else we loop on the ring
            first_range_data = None
            previous = None
            for token, replicas in ring:
                if not first_range_data:
                    first_range_data = make_range_data(replicas)  # we use it at the end when wrapping around

                if token.value == min_token:
                    continue  # avoids looping entire ring

                current_range = make_range(previous, token.value)
                if not current_range:
                    continue

                ranges[current_range] = make_range_data(replicas)
                previous = token.value

            #  For the last ring interval we query the same replicas that hold the first token in the ring
            if previous is not None and (not end_token or previous < end_token):
                ranges[(previous, end_token)] = first_range_data

        if not ranges:
            shell.printerr('Found no ranges to query, check begin and end tokens: %s - %s' % (begin_token, end_token))

        return ranges

    def get_min_token(self):
        """
        :return the minimum token, which depends on the partitioner.
        For partitioners that do not support tokens we return None, in
        this cases we will not work in parallel, we'll just send all requests
        to the cqlsh session host.
        """
        partitioner = self.shell.conn.metadata.partitioner

        if partitioner.endswith('RandomPartitioner'):
            return -1
        elif partitioner.endswith('Murmur3Partitioner'):
            return -(2 ** 63)   # Long.MIN_VALUE in Java
        else:
            return None

    def send_work(self, ranges, tokens_to_send):
        i = 0
        for token_range in tokens_to_send:
            self.outmsg.channels[i].send((token_range, ranges[token_range]))
            ranges[token_range]['attempts'] += 1

            i = i + 1 if i < self.num_processes - 1 else 0

    def export_records(self, ranges):
        """
        Send records to child processes and monitor them by collecting their results
        or any errors. We terminate when we have processed all the ranges or when one child
        process has died (since in this case we will never get any ACK for the ranges
        processed by it and at the moment we don't keep track of which ranges a
        process is handling).
        """
        shell = self.shell
        processes = self.processes
        meter = RateMeter(log_fcn=self.printmsg,
                          update_interval=self.options.copy['reportfrequency'],
                          log_file=self.options.copy['ratefile'])
        total_requests = len(ranges)
        max_attempts = self.options.copy['maxattempts']

        self.send_work(ranges, ranges.keys())

        num_processes = len(processes)
        succeeded = 0
        failed = 0
        while (failed + succeeded) < total_requests and self.num_live_processes() == num_processes:
            for token_range, result in self.inmsg.recv(timeout=0.1):
                if token_range is None and result is None:  # a request has finished
                    succeeded += 1
                elif isinstance(result, Exception):  # an error occurred
                    if token_range is None:  # the entire process failed
                        shell.printerr('Error from worker process: %s' % (result))
                    else:   # only this token_range failed, retry up to max_attempts if no rows received yet,
                            # If rows were already received we'd risk duplicating data.
                            # Note that there is still a slight risk of duplicating data, even if we have
                            # an error with no rows received yet, it's just less likely. To avoid retrying on
                            # all timeouts would however mean we could risk not exporting some rows.
                        if ranges[token_range]['attempts'] < max_attempts and ranges[token_range]['rows'] == 0:
                            shell.printerr('Error for %s: %s (will try again later attempt %d of %d)'
                                           % (token_range, result, ranges[token_range]['attempts'], max_attempts))
                            self.send_work(ranges, [token_range])
                        else:
                            shell.printerr('Error for %s: %s (permanently given up after %d rows and %d attempts)'
                                           % (token_range, result, ranges[token_range]['rows'],
                                              ranges[token_range]['attempts']))
                            failed += 1
                else:  # partial result received
                    data, num = result
                    self.writer.write(data, num)
                    meter.increment(n=num)
                    ranges[token_range]['rows'] += num

        if self.num_live_processes() < len(processes):
            for process in processes:
                if not process.is_alive():
                    shell.printerr('Child process %d died with exit code %d' % (process.pid, process.exitcode))

        if succeeded < total_requests:
            shell.printerr('Exported %d ranges out of %d total ranges, some records might be missing'
                           % (succeeded, total_requests))

        self.printmsg("\n%d rows exported to %d files in %s." %
                      (meter.get_total_records(),
                       self.writer.num_files,
                       self.describe_interval(time.time() - self.time_start)))


class FilesReader(object):
    """
    A wrapper around a csv reader to keep track of when we have
    exhausted reading input files. We are passed a comma separated
    list of paths, where each path is a valid glob expression.
    We generate a source generator and we read each source one
    by one.
    """
    def __init__(self, fname, options):
        self.chunk_size = options.copy['chunksize']
        self.header = options.copy['header']
        self.max_rows = options.copy['maxrows']
        self.skip_rows = options.copy['skiprows']
        self.sources = self.get_source(fname)
        self.num_sources = 0
        self.current_source = None
        self.num_read = 0

    def get_source(self, paths):
        """
         Return a source generator. Each source is a named tuple
         wrapping the source input, file name and a boolean indicating
         if it requires closing.
        """
        def make_source(fname):
            try:
                return open(fname, 'rb')
            except IOError, e:
                self.printmsg("Can't open %r for reading: %s" % (fname, e))
                return None

        for path in paths.split(','):
            path = path.strip()
            if os.path.isfile(path):
                yield make_source(path)
            else:
                for f in glob.glob(path):
                    yield (make_source(f))

    @staticmethod
    def printmsg(msg, eol='\n'):
        sys.stdout.write(msg + eol)
        sys.stdout.flush()

    def start(self):
        self.next_source()

    @property
    def exhausted(self):
        return not self.current_source

    def next_source(self):
        """
         Close the current source, if any, and open the next one. Return true
         if there is another source, false otherwise.
        """
        self.close_current_source()
        while self.current_source is None:
            try:
                self.current_source = self.sources.next()
                if self.current_source:
                    self.num_sources += 1
            except StopIteration:
                return False

        if self.header:
            self.current_source.next()

        return True

    def close_current_source(self):
        if not self.current_source:
            return

        self.current_source.close()
        self.current_source = None

    def close(self):
        self.close_current_source()

    def read_rows(self, max_rows):
        if not self.current_source:
            return []

        rows = []
        for i in xrange(min(max_rows, self.chunk_size)):
            try:
                row = self.current_source.next()
                self.num_read += 1

                if 0 <= self.max_rows < self.num_read:
                    self.next_source()
                    break

                if self.num_read > self.skip_rows:
                    rows.append(row)

            except StopIteration:
                self.next_source()
                break

        return filter(None, rows)


class PipeReader(object):
    """
    A class for reading rows received on a pipe, this is used for reading input from STDIN
    """
    def __init__(self, inmsg, options):
        self.inmsg = inmsg
        self.chunk_size = options.copy['chunksize']
        self.header = options.copy['header']
        self.max_rows = options.copy['maxrows']
        self.skip_rows = options.copy['skiprows']
        self.num_read = 0
        self.exhausted = False
        self.num_sources = 1

    def start(self):
        pass

    def read_rows(self, max_rows):
        rows = []
        for i in xrange(min(max_rows, self.chunk_size)):
            row = self.inmsg.recv()
            if row is None:
                self.exhausted = True
                break

            self.num_read += 1
            if 0 <= self.max_rows < self.num_read:
                self.exhausted = True
                break  # max rows exceeded

            if self.header or self.num_read < self.skip_rows:
                self.header = False  # skip header or initial skip_rows rows
                continue

            rows.append(row)

        return rows


class ImportProcessResult(object):
    """
    An object sent from ImportProcess instances to the parent import task in order to indicate progress.
    """
    def __init__(self, imported=0):
        self.imported = imported


class FeedingProcessResult(object):
    """
   An object sent from FeedingProcess instances to the parent import task in order to indicate progress.
    """
    def __init__(self, sent, reader):
        self.sent = sent
        self.num_sources = reader.num_sources
        self.skip_rows = reader.skip_rows


class ImportTaskError(object):
    """
    An object send from child processes (feeder or workers) to the parent import task to indicate an error.
    """
    def __init__(self, name, msg, rows=None, attempts=1, final=True):
        self.name = name
        self.msg = msg
        self.rows = rows if rows else []
        self.attempts = attempts
        self.final = final

    def is_parse_error(self):
        """
        We treat read and parse errors as unrecoverable and we have different global counters for giving up when
        a maximum has been reached. We consider value and type errors as parse errors as well since they
        are typically non recoverable.
        """
        name = self.name
        return name.startswith('ValueError') or name.startswith('TypeError') or \
            name.startswith('ParseError') or name.startswith('IndexError') or name.startswith('ReadError')


class ImportErrorHandler(object):
    """
    A class for managing import errors
    """
    def __init__(self, task):
        self.shell = task.shell
        self.options = task.options
        self.printmsg = task.printmsg
        self.max_attempts = self.options.copy['maxattempts']
        self.max_parse_errors = self.options.copy['maxparseerrors']
        self.max_insert_errors = self.options.copy['maxinserterrors']
        self.err_file = self.options.copy['errfile']
        self.parse_errors = 0
        self.insert_errors = 0
        self.num_rows_failed = 0

        if os.path.isfile(self.err_file):
            now = datetime.datetime.now()
            old_err_file = self.err_file + now.strftime('.%Y%m%d_%H%M%S')
            self.printmsg("Renaming existing %s to %s\n" % (self.err_file, old_err_file))
            os.rename(self.err_file, old_err_file)

    def max_exceeded(self):
        if self.insert_errors > self.max_insert_errors >= 0:
            self.shell.printerr("Exceeded maximum number of insert errors %d" % self.max_insert_errors)
            return True

        if self.parse_errors > self.max_parse_errors >= 0:
            self.shell.printerr("Exceeded maximum number of parse errors %d" % self.max_parse_errors)
            return True

        return False

    def add_failed_rows(self, rows):
        self.num_rows_failed += len(rows)

        with open(self.err_file, "a") as f:
            writer = csv.writer(f, **self.options.dialect)
            for row in rows:
                writer.writerow(row)

    def handle_error(self, err):
        """
        Handle an error by printing the appropriate error message and incrementing the correct counter.
        """
        shell = self.shell

        if err.is_parse_error():
            self.parse_errors += len(err.rows)
            self.add_failed_rows(err.rows)
            shell.printerr("Failed to import %d rows: %s - %s,  given up without retries"
                           % (len(err.rows), err.name, err.msg))
        else:
            self.insert_errors += len(err.rows)
            if not err.final:
                shell.printerr("Failed to import %d rows: %s - %s,  will retry later, attempt %d of %d"
                               % (len(err.rows), err.name, err.msg, err.attempts, self.max_attempts))
            else:
                self.add_failed_rows(err.rows)
                shell.printerr("Failed to import %d rows: %s - %s,  given up after %d attempts"
                               % (len(err.rows), err.name, err.msg, err.attempts))


class ImportTask(CopyTask):
    """
    A class to import data from .csv by instantiating one or more processes
    that work in parallel (ImportProcess).
    """
    def __init__(self, shell, ks, table, columns, fname, opts, protocol_version, config_file):
        CopyTask.__init__(self, shell, ks, table, columns, fname, opts, protocol_version, config_file, 'from')

        options = self.options
        self.skip_columns = [c.strip() for c in self.options.copy['skipcols'].split(',')]
        self.valid_columns = [c for c in self.columns if c not in self.skip_columns]
        self.table_meta = self.shell.get_table_meta(self.ks, self.table)
        self.receive_meter = RateMeter(log_fcn=self.printmsg,
                                       update_interval=options.copy['reportfrequency'],
                                       log_file=options.copy['ratefile'])
        self.error_handler = ImportErrorHandler(self)
        self.feeding_result = None
        self.sent = 0

    def make_params(self):
        ret = CopyTask.make_params(self)
        ret['skip_columns'] = self.skip_columns
        ret['valid_columns'] = self.valid_columns
        return ret

    def run(self):
        shell = self.shell

        if self.options.unrecognized:
            shell.printerr('Unrecognized COPY FROM options: %s' % ', '.join(self.options.unrecognized.keys()))
            return

        if not self.valid_columns:
            shell.printerr("No column specified")
            return 0

        for c in self.table_meta.primary_key:
            if c.name not in self.valid_columns:
                shell.printerr("Primary key column '%s' missing or skipped" % (c.name,))
                return 0

        self.printmsg("\nStarting copy of %s.%s with columns %s." % (self.ks, self.table, self.valid_columns))

        try:
            params = self.make_params()

            for i in range(self.num_processes - 1):
                self.processes.append(ImportProcess(self.update_params(params, i)))

            feeder = FeedingProcess(self.outmsg.channels[-1], self.inmsg.channels[-1],
                                    self.outmsg.channels[:-1], self.fname, self.options)
            self.processes.append(feeder)

            self.start_processes()

            pr = profile_on() if PROFILE_ON else None

            self.import_records()

            if pr:
                profile_off(pr, file_name='parent_profile_%d.txt' % (os.getpid(),))

        except Exception, exc:
            shell.printerr(str(exc))
            if shell.debug:
                traceback.print_exc()
            return 0
        finally:
            self.close()

    def send_stdin_rows(self):
        """
        We need to pass stdin rows to the feeder process as it is not safe to pickle or share stdin
        directly (in case of file the child process would close it). This is a very primitive support
        for STDIN import in that we we won't start reporting progress until STDIN is fully consumed. I
        think this is reasonable.
        """
        shell = self.shell

        self.printmsg("[Use \. on a line by itself to end input]")
        for row in shell.use_stdin_reader(prompt='[copy] ', until=r'.'):
            self.outmsg.channels[-1].send(row)

        self.outmsg.channels[-1].send(None)
        if shell.tty:
            print

    def import_records(self):
        """
        Keep on running until we have stuff to receive or send and until all processes are running.
        Send data (batches or retries) up to the max ingest rate. If we are waiting for stuff to
        receive check the incoming queue.
        """
        if not self.fname:
            self.send_stdin_rows()

        while self.feeding_result is None or self.receive_meter.total_records < self.feeding_result.sent:
            self.receive_results()

            if self.error_handler.max_exceeded() or not self.all_processes_running():
                break

        if self.error_handler.num_rows_failed:
            self.shell.printerr("Failed to process %d rows; failed rows written to %s" %
                                (self.error_handler.num_rows_failed,
                                 self.error_handler.err_file))

        if not self.all_processes_running():
            self.shell.printerr("{} child process(es) died unexpectedly, aborting"
                                .format(self.num_processes - self.num_live_processes()))

        for i, _ in enumerate(self.processes):
            self.outmsg.channels[i].send(None)

        if PROFILE_ON:
            # allow time for worker processes to write profile results
            time.sleep(5)

        self.printmsg("\n%d rows imported from %d files in %s (%d skipped)." %
                      (self.receive_meter.get_total_records(),
                       self.feeding_result.num_sources if self.feeding_result else 0,
                       self.describe_interval(time.time() - self.time_start),
                       self.feeding_result.skip_rows if self.feeding_result else 0))

    def all_processes_running(self):
        return self.num_live_processes() == len(self.processes)

    def receive_results(self):
        """
        Receive results from the worker processes, which will send the number of rows imported
        or from the feeder process, which will send the number of rows sent when it has finished sending rows.
        """
        aggregate_result = ImportProcessResult()
        try:
            for result in self.inmsg.recv(timeout=0.1):
                if isinstance(result, ImportProcessResult):
                    aggregate_result.imported += result.imported
                elif isinstance(result, ImportTaskError):
                    self.error_handler.handle_error(result)
                elif isinstance(result, FeedingProcessResult):
                    self.feeding_result = result
                else:
                    raise ValueError("Unexpected result: %s" % (result,))
        finally:
            self.receive_meter.increment(aggregate_result.imported)


class FeedingProcess(mp.Process):
    """
    A process that reads from import sources and sends chunks to worker processes.
    """
    def __init__(self, inmsg, outmsg, worker_channels, fname, options):
        mp.Process.__init__(self, target=self.run)
        self.inmsg = inmsg
        self.outmsg = outmsg
        self.worker_channels = worker_channels
        self.reader = FilesReader(fname, options) if fname else PipeReader(inmsg, options)
        self.send_meter = RateMeter(log_fcn=None, update_interval=1)
        self.ingest_rate = options.copy['ingestrate']
        self.num_worker_processes = options.copy['numprocesses']
        self.chunk_id = 0

    def run(self):
        pr = profile_on() if PROFILE_ON else None

        self.inner_run()

        if pr:
            profile_off(pr, file_name='feeder_profile_%d.txt' % (os.getpid(),))

    def inner_run(self):
        """
        Send one batch per worker process to the queue unless we have exceeded the ingest rate.
        In the export case we queue everything and let the worker processes throttle using max_requests,
        here we throttle using the ingest rate in the feeding process because of memory usage concerns.
        When finished we send back to the parent process the total number of rows sent.
        """
        reader = self.reader
        reader.start()
        channels = self.worker_channels
        sent = 0

        while not reader.exhausted:
            for ch in channels:
                try:
                    max_rows = self.ingest_rate - self.send_meter.current_record
                    if max_rows <= 0:
                        self.send_meter.maybe_update(sleep=False)
                        continue

                    rows = reader.read_rows(max_rows)
                    if rows:
                        sent += self.send_chunk(ch, rows)
                except Exception, exc:
                    self.outmsg.send(ImportTaskError(exc.__class__.__name__, exc.message))

                if reader.exhausted:
                    break

        # send back to the parent process the number of rows sent to the worker processes
        self.outmsg.send(FeedingProcessResult(sent, reader))

        # wait for poison pill (None)
        self.inmsg.recv()

    def send_chunk(self, ch, rows):
        self.chunk_id += 1
        num_rows = len(rows)
        self.send_meter.increment(num_rows)
        ch.send({'id': self.chunk_id, 'rows': rows, 'imported': 0})
        return num_rows

    def close(self):
        self.reader.close()
        self.inmsg.close()
        self.outmsg.close()

        for ch in self.worker_channels:
            ch.close()


class ChildProcess(mp.Process):
    """
    An child worker process, this is for common functionality between ImportProcess and ExportProcess.
    """

    def __init__(self, params, target):
        mp.Process.__init__(self, target=target)
        self.inmsg = params['inmsg']
        self.outmsg = params['outmsg']
        self.ks = params['ks']
        self.table = params['table']
        self.local_dc = params['local_dc']
        self.columns = params['columns']
        self.debug = params['debug']
        self.port = params['port']
        self.hostname = params['hostname']
        self.connect_timeout = params['connect_timeout']
        self.cql_version = params['cql_version']
        self.auth_provider = params['auth_provider']
        self.ssl = params['ssl']
        self.protocol_version = params['protocol_version']
        self.config_file = params['config_file']

        options = params['options']
        self.time_format = options.copy['dtformats']
        self.consistency_level = options.copy['consistencylevel']
        self.decimal_sep = options.copy['decimalsep']
        self.thousands_sep = options.copy['thousandssep']
        self.boolean_styles = options.copy['boolstyle']
        self.max_attempts = options.copy['maxattempts']
        # Here we inject some failures for testing purposes, only if this environment variable is set
        if os.environ.get('CQLSH_COPY_TEST_FAILURES', ''):
            self.test_failures = json.loads(os.environ.get('CQLSH_COPY_TEST_FAILURES', ''))
        else:
            self.test_failures = None

    def printdebugmsg(self, text):
        if self.debug:
            sys.stdout.write(text + '\n')

    def close(self):
        self.printdebugmsg("Closing queues...")
        self.inmsg.close()
        self.outmsg.close()


class ExpBackoffRetryPolicy(RetryPolicy):
    """
    A retry policy with exponential back-off for read timeouts and write timeouts
    """
    def __init__(self, parent_process):
        RetryPolicy.__init__(self)
        self.max_attempts = parent_process.max_attempts
        self.printdebugmsg = parent_process.printdebugmsg

    def on_read_timeout(self, query, consistency, required_responses,
                        received_responses, data_retrieved, retry_num):
        return self._handle_timeout(consistency, retry_num)

    def on_write_timeout(self, query, consistency, write_type,
                         required_responses, received_responses, retry_num):
        return self._handle_timeout(consistency, retry_num)

    def _handle_timeout(self, consistency, retry_num):
        delay = self.backoff(retry_num)
        if delay > 0:
            self.printdebugmsg("Timeout received, retrying after %d seconds" % (delay,))
            time.sleep(delay)
            return self.RETRY, consistency
        elif delay == 0:
            self.printdebugmsg("Timeout received, retrying immediately")
            return self.RETRY, consistency
        else:
            self.printdebugmsg("Timeout received, giving up after %d attempts" % (retry_num + 1))
            return self.RETHROW, None

    def backoff(self, retry_num):
        """
        Perform exponential back-off up to a maximum number of times, where
        this maximum is per query.
        To back-off we should wait a random number of seconds
        between 0 and 2^c - 1, where c is the number of total failures.
        randrange() excludes the last value, so we drop the -1.

        :return : the number of seconds to wait for, -1 if we should not retry
        """
        if retry_num >= self.max_attempts:
            return -1

        delay = randrange(0, pow(2, retry_num + 1))
        return delay


class ExportSession(object):
    """
    A class for connecting to a cluster and storing the number
    of requests that this connection is processing. It wraps the methods
    for executing a query asynchronously and for shutting down the
    connection to the cluster.
    """
    def __init__(self, cluster, export_process):
        if LibevConnection:
            cluster.connection_class = LibevConnection
        session = cluster.connect(export_process.ks)
        session.row_factory = tuple_factory
        session.default_fetch_size = export_process.options.copy['pagesize']
        session.default_timeout = export_process.options.copy['pagetimeout']

        export_process.printdebugmsg("Created connection to %s with page size %d and timeout %d seconds per page"
                                     % (cluster.contact_points, session.default_fetch_size, session.default_timeout))

        self.cluster = cluster
        self.session = session
        self.requests = 1
        self.lock = Lock()
        self.consistency_level = export_process.consistency_level

    def add_request(self):
        with self.lock:
            self.requests += 1

    def complete_request(self):
        with self.lock:
            self.requests -= 1

    def num_requests(self):
        with self.lock:
            return self.requests

    def execute_async(self, query):
        return self.session.execute_async(SimpleStatement(query, consistency_level=self.consistency_level))

    def shutdown(self):
        self.cluster.shutdown()


class ExportProcess(ChildProcess):
    """
    An child worker process for the export task, ExportTask.
    """

    def __init__(self, params):
        ChildProcess.__init__(self, params=params, target=self.run)
        options = params['options']
        self.encoding = options.copy['encoding']
        self.float_precision = options.copy['float_precision']
        self.nullval = options.copy['nullval']
        self.max_requests = options.copy['maxrequests']

        self.hosts_to_sessions = dict()
        self.formatters = dict()
        self.options = options

    def run(self):
        try:
            self.inner_run()
        finally:
            self.close()

    def inner_run(self):
        """
        The parent sends us (range, info) on the inbound queue (inmsg)
        in order to request us to process a range, for which we can
        select any of the hosts in info, which also contains other information for this
        range such as the number of attempts already performed. We can signal errors
        on the outbound queue (outmsg) by sending (range, error) or
        we can signal a global error by sending (None, error).
        We terminate when the inbound queue is closed.
        """
        while True:
            if self.num_requests() > self.max_requests:
                time.sleep(0.001)  # 1 millisecond
                continue

            token_range, info = self.inmsg.recv()
            self.start_request(token_range, info)

    @staticmethod
    def get_error_message(err, print_traceback=False):
        if isinstance(err, str):
            msg = err
        elif isinstance(err, BaseException):
            msg = "%s - %s" % (err.__class__.__name__, err)
            if print_traceback:
                traceback.print_exc(err)
        else:
            msg = str(err)
        return msg

    def report_error(self, err, token_range=None):
        msg = self.get_error_message(err, print_traceback=self.debug)
        self.printdebugmsg(msg)
        self.outmsg.send((token_range, Exception(msg)))

    def start_request(self, token_range, info):
        """
        Begin querying a range by executing an async query that
        will later on invoke the callbacks attached in attach_callbacks.
        """
        session = self.get_session(info['hosts'], token_range)
        if session:
            metadata = session.cluster.metadata.keyspaces[self.ks].tables[self.table]
            query = self.prepare_query(metadata.partition_key, token_range, info['attempts'])
            future = session.execute_async(query)
            self.attach_callbacks(token_range, future, session)

    def num_requests(self):
        return sum(session.num_requests() for session in self.hosts_to_sessions.values())

    def get_session(self, hosts, token_range):
        """
        We return a session connected to one of the hosts passed in, which are valid replicas for
        the token range. We sort replicas by favouring those without any active requests yet or with the
        smallest number of requests. If we fail to connect we report an error so that the token will
        be retried again later.

        :return: An ExportSession connected to the chosen host.
        """
        # sorted replicas favouring those with no connections yet
        hosts = sorted(hosts,
                       key=lambda hh: 0 if hh not in self.hosts_to_sessions else self.hosts_to_sessions[hh].requests)

        errors = []
        ret = None
        for host in hosts:
            try:
                ret = self.connect(host)
            except Exception, e:
                errors.append(self.get_error_message(e))

            if ret:
                if errors:
                    self.printdebugmsg("Warning: failed to connect to some replicas: %s" % (errors,))
                return ret

        self.report_error("Failed to connect to all replicas %s for %s, errors: %s" % (hosts, token_range, errors))
        return None

    def connect(self, host):
        if host in self.hosts_to_sessions.keys():
            session = self.hosts_to_sessions[host]
            session.add_request()
            return session

        new_cluster = Cluster(
            contact_points=(host,),
            port=self.port,
            cql_version=self.cql_version,
            protocol_version=self.protocol_version,
            auth_provider=self.auth_provider,
            ssl_options=ssl_settings(host, self.config_file) if self.ssl else None,
            load_balancing_policy=WhiteListRoundRobinPolicy([host]),
            default_retry_policy=ExpBackoffRetryPolicy(self),
            compression=None,
            control_connection_timeout=self.connect_timeout,
            connect_timeout=self.connect_timeout,
            idle_heartbeat_interval=0)
        session = ExportSession(new_cluster, self)
        self.hosts_to_sessions[host] = session
        return session

    def attach_callbacks(self, token_range, future, session):
        def result_callback(rows):
            if future.has_more_pages:
                future.start_fetching_next_page()
                self.write_rows_to_csv(token_range, rows)
            else:
                self.write_rows_to_csv(token_range, rows)
                self.outmsg.send((None, None))
                session.complete_request()

        def err_callback(err):
            self.report_error(err, token_range)
            session.complete_request()

        future.add_callbacks(callback=result_callback, errback=err_callback)

    def write_rows_to_csv(self, token_range, rows):
        if not rows:
            return  # no rows in this range

        try:
            output = StringIO()
            writer = csv.writer(output, **self.options.dialect)

            for row in rows:
                writer.writerow(map(self.format_value, row))

            data = (output.getvalue(), len(rows))
            self.outmsg.send((token_range, data))
            output.close()

        except Exception, e:
            self.report_error(e, token_range)

    def format_value(self, val):
        if val is None or val == EMPTY:
            return format_value_default(self.nullval, colormap=NO_COLOR_MAP)

        ctype = type(val)
        formatter = self.formatters.get(ctype, None)
        if not formatter:
            formatter = get_formatter(ctype)
            self.formatters[ctype] = formatter

        return formatter(val, encoding=self.encoding, colormap=NO_COLOR_MAP, time_format=self.time_format,
                         float_precision=self.float_precision, nullval=self.nullval, quote=False,
                         decimal_sep=self.decimal_sep, thousands_sep=self.thousands_sep,
                         boolean_styles=self.boolean_styles)

    def close(self):
        ChildProcess.close(self)
        for session in self.hosts_to_sessions.values():
            session.shutdown()

    def prepare_query(self, partition_key, token_range, attempts):
        """
        Return the export query or a fake query with some failure injected.
        """
        if self.test_failures:
            return self.maybe_inject_failures(partition_key, token_range, attempts)
        else:
            return self.prepare_export_query(partition_key, token_range)

    def maybe_inject_failures(self, partition_key, token_range, attempts):
        """
        Examine self.test_failures and see if token_range is either a token range
        supposed to cause a failure (failing_range) or to terminate the worker process
        (exit_range). If not then call prepare_export_query(), which implements the
        normal behavior.
        """
        start_token, end_token = token_range

        if not start_token or not end_token:
            # exclude first and last ranges to make things simpler
            return self.prepare_export_query(partition_key, token_range)

        if 'failing_range' in self.test_failures:
            failing_range = self.test_failures['failing_range']
            if start_token >= failing_range['start'] and end_token <= failing_range['end']:
                if attempts < failing_range['num_failures']:
                    return 'SELECT * from bad_table'

        if 'exit_range' in self.test_failures:
            exit_range = self.test_failures['exit_range']
            if start_token >= exit_range['start'] and end_token <= exit_range['end']:
                sys.exit(1)

        return self.prepare_export_query(partition_key, token_range)

    def prepare_export_query(self, partition_key, token_range):
        """
        Return a query where we select all the data for this token range
        """
        pk_cols = ", ".join(protect_names(col.name for col in partition_key))
        columnlist = ', '.join(protect_names(self.columns))
        start_token, end_token = token_range
        query = 'SELECT %s FROM %s.%s' % (columnlist, protect_name(self.ks), protect_name(self.table))
        if start_token is not None or end_token is not None:
            query += ' WHERE'
        if start_token is not None:
            query += ' token(%s) > %s' % (pk_cols, start_token)
        if start_token is not None and end_token is not None:
            query += ' AND'
        if end_token is not None:
            query += ' token(%s) <= %s' % (pk_cols, end_token)
        return query


class ParseError(Exception):
    """ We failed to parse an import record """
    pass


class ImportConversion(object):
    """
    A class for converting strings to values when importing from csv, used by ImportProcess,
    the parent.
    """
    def __init__(self, parent, table_meta, statement=None):
        self.ks = parent.ks
        self.table = parent.table
        self.columns = parent.valid_columns
        self.nullval = parent.nullval
        self.printdebugmsg = parent.printdebugmsg
        self.decimal_sep = parent.decimal_sep
        self.thousands_sep = parent.thousands_sep
        self.boolean_styles = parent.boolean_styles
        self.time_format = parent.time_format

        self.table_meta = table_meta
        self.primary_key_indexes = [self.columns.index(col.name) for col in self.table_meta.primary_key]
        self.partition_key_indexes = [self.columns.index(col.name) for col in self.table_meta.partition_key]

        if statement is None:
            self.use_prepared_statements = False
            statement = self._get_primary_key_statement(parent, table_meta)
        else:
            self.use_prepared_statements = True

        self.proto_version = statement.protocol_version

        # the cql types and converters for the prepared statement, either the full statement or only the primary keys
        self.cqltypes = [c.type for c in statement.column_metadata]
        self.converters = [self._get_converter(c.type) for c in statement.column_metadata]

        # the cql types for the entire statement, these are the same as the types above but
        # only when using prepared statements
        self.coltypes = [table_meta.columns[name].typestring for name in parent.valid_columns]
        # these functions are used for non-prepared statements to protect values with quotes if required
        self.protectors = [protect_value if t in ('ascii', 'text', 'timestamp', 'date', 'time', 'inet') else lambda v: v
                           for t in self.coltypes]

    @staticmethod
    def _get_primary_key_statement(parent, table_meta):
        """
        We prepare a query statement to find out the types of the partition key columns so we can
        route the update query to the correct replicas. As far as I understood this is the easiest
        way to find out the types of the partition columns, we will never use this prepared statement
        """
        where_clause = ' AND '.join(['%s = ?' % (protect_name(c.name)) for c in table_meta.partition_key])
        select_query = 'SELECT * FROM %s.%s WHERE %s' % (protect_name(parent.ks),
                                                         protect_name(parent.table),
                                                         where_clause)
        return parent.session.prepare(select_query)

    def _get_converter(self, cql_type):
        """
        Return a function that converts a string into a value the can be passed
        into BoundStatement.bind() for the given cql type. See cassandra.cqltypes
        for more details.
        """
        def unprotect(v):
            if v is not None:
                return CqlRuleSet.dequote_value(v)

        def convert(t, v):
            return converters.get(t.typename, convert_unknown)(unprotect(v), ct=t)

        def convert_blob(v, **_):
            return bytearray.fromhex(v[2:])

        def convert_text(v, **_):
            return v

        def convert_uuid(v, **_):
            return UUID(v)

        def convert_bool(v, **_):
            return True if v.lower() == self.boolean_styles[0].lower() else False

        def get_convert_integer_fcn(adapter=int):
            """
            Return a slow and a fast integer conversion function depending on self.thousands_sep
            """
            if self.thousands_sep:
                return lambda v, ct=cql_type: adapter(v.replace(self.thousands_sep, ''))
            else:
                return lambda v, ct=cql_type: adapter(v)

        def get_convert_decimal_fcn(adapter=float):
            """
            Return a slow and a fast decimal conversion function depending on self.thousands_sep and self.decimal_sep
            """
            if self.thousands_sep and self.decimal_sep:
                return lambda v, ct=cql_type: adapter(v.replace(self.thousands_sep, '').replace(self.decimal_sep, '.'))
            elif self.thousands_sep:
                return lambda v, ct=cql_type: adapter(v.replace(self.thousands_sep, ''))
            elif self.decimal_sep:
                return lambda v, ct=cql_type: adapter(v.replace(self.decimal_sep, '.'))
            else:
                return lambda v, ct=cql_type: adapter(v)

        def split(val, sep=','):
            """
            Split into a list of values whenever we encounter a separator but
            ignore separators inside parentheses or single quotes, except for the two
            outermost parentheses, which will be ignored. We expect val to be at least
            2 characters long (the two outer parentheses).
            """
            ret = []
            last = 1
            level = 0
            quote = False
            for i, c in enumerate(val):
                if c == '{' or c == '[' or c == '(':
                    level += 1
                elif c == '}' or c == ']' or c == ')':
                    level -= 1
                elif c == '\'':
                    quote = not quote
                elif c == sep and level == 1 and not quote:
                    ret.append(val[last:i])
                    last = i + 1
            else:
                if last < len(val) - 1:
                    ret.append(val[last:-1])

            return ret

        # this should match all possible CQL datetime formats
        p = re.compile("(\d{4})\-(\d{2})\-(\d{2})\s?(?:'T')?" +  # YYYY-MM-DD[( |'T')]
                       "(?:(\d{2}):(\d{2})(?::(\d{2}))?)?" +  # [HH:MM[:SS]]
                       "(?:([+\-])(\d{2}):?(\d{2}))?")  # [(+|-)HH[:]MM]]

        def convert_datetime(val, **_):
            try:
                tval = time.strptime(val, self.time_format)
                return timegm(tval) * 1e3  # scale seconds to millis for the raw value
            except ValueError:
                pass  # if it's not in the default format we try CQL formats

            m = p.match(val)
            if not m:
                raise ValueError("can't interpret %r as a date, specified time format is %s" % (val, self.time_format))

            # https://docs.python.org/2/library/time.html#time.struct_time
            tval = time.struct_time((int(m.group(1)), int(m.group(2)), int(m.group(3)),  # year, month, day
                                    int(m.group(4)) if m.group(4) else 0,  # hour
                                    int(m.group(5)) if m.group(5) else 0,  # minute
                                    int(m.group(6)) if m.group(6) else 0,  # second
                                    0, 1, -1))  # day of week, day of year, dst-flag

            if m.group(7):
                offset = (int(m.group(8)) * 3600 + int(m.group(9)) * 60) * int(m.group(7) + '1')
            else:
                offset = -time.timezone

            # scale seconds to millis for the raw value
            return (timegm(tval) + offset) * 1e3

        def convert_date(v, **_):
            return Date(v)

        def convert_time(v, **_):
            return Time(v)

        def convert_tuple(val, ct=cql_type):
            return tuple(convert(t, v) for t, v in zip(ct.subtypes, split(val)))

        def convert_list(val, ct=cql_type):
            return list(convert(ct.subtypes[0], v) for v in split(val))

        def convert_set(val, ct=cql_type):
            return frozenset(convert(ct.subtypes[0], v) for v in split(val))

        def convert_map(val, ct=cql_type):
            """
            We need to pass to BoundStatement.bind() a dict() because it calls iteritems(),
            except we can't create a dict with another dict as the key, hence we use a class
            that adds iteritems to a frozen set of tuples (which is how dict are normally made
            immutable in python).
            """
            class ImmutableDict(frozenset):
                iteritems = frozenset.__iter__

            return ImmutableDict(frozenset((convert(ct.subtypes[0], v[0]), convert(ct.subtypes[1], v[1]))
                                 for v in [split('{%s}' % vv, sep=':') for vv in split(val)]))

        def convert_user_type(val, ct=cql_type):
            """
            A user type is a dictionary except that we must convert each key into
            an attribute, so we are using named tuples. It must also be hashable,
            so we cannot use dictionaries. Maybe there is a way to instantiate ct
            directly but I could not work it out.
            """
            vals = [v for v in [split('{%s}' % vv, sep=':') for vv in split(val)]]
            ret_type = namedtuple(ct.typename, [unprotect(v[0]) for v in vals])
            return ret_type(*tuple(convert(t, v[1]) for t, v in zip(ct.subtypes, vals)))

        def convert_single_subtype(val, ct=cql_type):
            return converters.get(ct.subtypes[0].typename, convert_unknown)(val, ct=ct.subtypes[0])

        def convert_unknown(val, ct=cql_type):
            if issubclass(ct, UserType):
                return convert_user_type(val, ct=ct)
            elif issubclass(ct, ReversedType):
                return convert_single_subtype(val, ct=ct)

            self.printdebugmsg("Unknown type %s (%s) for val %s" % (ct, ct.typename, val))
            return val

        converters = {
            'blob': convert_blob,
            'decimal': get_convert_decimal_fcn(adapter=Decimal),
            'uuid': convert_uuid,
            'boolean': convert_bool,
            'tinyint': get_convert_integer_fcn(),
            'ascii': convert_text,
            'float': get_convert_decimal_fcn(),
            'double': get_convert_decimal_fcn(),
            'bigint': get_convert_integer_fcn(adapter=long),
            'int': get_convert_integer_fcn(),
            'varint': get_convert_integer_fcn(),
            'inet': convert_text,
            'counter': get_convert_integer_fcn(adapter=long),
            'timestamp': convert_datetime,
            'timeuuid': convert_uuid,
            'date': convert_date,
            'smallint': get_convert_integer_fcn(),
            'time': convert_time,
            'text': convert_text,
            'varchar': convert_text,
            'list': convert_list,
            'set': convert_set,
            'map': convert_map,
            'tuple': convert_tuple,
            'frozen': convert_single_subtype,
        }

        return converters.get(cql_type.typename, convert_unknown)

    def convert_row(self, row):
        """
        Convert the row into a list of parsed values if using prepared statements, else simply apply the
        protection functions to escape values with quotes when required. Also check on the row length and
        make sure primary partition key values aren't missing.
        """
        converters = self.converters if self.use_prepared_statements else self.protectors

        if len(row) != len(converters):
            raise ParseError('Invalid row length %d should be %d' % (len(row), len(converters)))

        for i in self.primary_key_indexes:
            if row[i] == self.nullval:
                raise ParseError(self.get_null_primary_key_message(i))

        try:
            return [conv(val) for conv, val in zip(converters, row)]
        except Exception, e:
            raise ParseError(e.message)

    def get_null_primary_key_message(self, idx):
        message = "Cannot insert null value for primary key column '%s'." % (self.columns[idx],)
        if self.nullval == '':
            message += " If you want to insert empty strings, consider using" \
                       " the WITH NULL=<marker> option for COPY."
        return message

    def get_row_partition_key_values_fcn(self):
        """
        Return a function to convert a row into a string composed of the partition key values serialized
        and binary packed (the tokens on the ring). Depending on whether we are using prepared statements, we
        may have to convert the primary key values first, so we have two different serialize_value implementations.
        We also return different functions depending on how many partition key indexes we have (single or multiple).
        See also BoundStatement.routing_key.
        """
        def serialize_value_prepared(n, v):
            return self.cqltypes[n].serialize(v, self.proto_version)

        def serialize_value_not_prepared(n, v):
            return self.cqltypes[n].serialize(self.converters[n](v), self.proto_version)

        partition_key_indexes = self.partition_key_indexes
        serialize = serialize_value_prepared if self.use_prepared_statements else serialize_value_not_prepared

        def serialize_row_single(row):
            return serialize(partition_key_indexes[0], row[partition_key_indexes[0]])

        def serialize_row_multiple(row):
            pk_values = []
            for i in partition_key_indexes:
                val = serialize(i, row[i])
                l = len(val)
                pk_values.append(struct.pack(">H%dsB" % l, l, val, 0))
            return b"".join(pk_values)

        if len(partition_key_indexes) == 1:
            return serialize_row_single
        return serialize_row_multiple


class TokenMap(object):
    """
    A wrapper around the metadata token map to speed things up by caching ring token *values* and
    replicas. It is very important that we use the token values, which are primitive types, rather
    than the tokens classes when calling bisect_right() in split_batches(). If we use primitive values,
    the bisect is done in compiled code whilst with token classes each comparison requires a call
    into the interpreter to perform the cmp operation defined in Python. A simple test with 1 million bisect
    operations on an array of 2048 tokens was done in 0.37 seconds with primitives and 2.25 seconds with
    token classes. This is significant for large datasets because we need to do a bisect for each single row,
    and if VNODES are used, the size of the token map can get quite large too.
    """
    def __init__(self, ks, hostname, local_dc, session):

        self.ks = ks
        self.hostname = hostname
        self.local_dc = local_dc
        self.metadata = session.cluster.metadata

        self._initialize_ring()

        # Note that refresh metadata is disabled by default and we currenlty do not intercept it
        # If hosts are added, removed or moved during a COPY operation our token map is no longer optimal
        # However we can cope with hosts going down and up since we filter for replicas that are up when
        # making each batch

    def _initialize_ring(self):
        token_map = self.metadata.token_map
        if token_map is None:
            self.ring = [0]
            self.replicas = [(self.metadata.get_host(self.hostname),)]
            self.pk_to_token_value = lambda pk: 0
            return

        token_map.rebuild_keyspace(self.ks, build_if_absent=True)
        tokens_to_hosts = token_map.tokens_to_hosts_by_ks.get(self.ks, None)
        from_key = token_map.token_class.from_key

        self.ring = [token.value for token in token_map.ring]
        self.replicas = [tuple(tokens_to_hosts[token]) for token in token_map.ring]
        self.pk_to_token_value = lambda pk: from_key(pk).value

    @staticmethod
    def get_ring_pos(ring, val):
        idx = bisect_right(ring, val)
        return idx if idx < len(ring) else 0

    def filter_replicas(self, hosts):
        shuffled = tuple(sorted(hosts, key=lambda k: random.random()))
        return filter(lambda r: r.is_up and r.datacenter == self.local_dc, shuffled) if hosts else ()


class FastTokenAwarePolicy(DCAwareRoundRobinPolicy):
    """
    Send to any replicas attached to the query, or else fall back to DCAwareRoundRobinPolicy
    """

    def __init__(self, local_dc='', used_hosts_per_remote_dc=0):
        DCAwareRoundRobinPolicy.__init__(self, local_dc, used_hosts_per_remote_dc)

    def make_query_plan(self, working_keyspace=None, query=None):
        """
        Extend TokenAwarePolicy.make_query_plan() so that we choose the same replicas in preference
        and most importantly we avoid repeating the (slow) bisect
        """
        replicas = query.replicas if hasattr(query, 'replicas') else []
        for r in replicas:
            yield r

        for r in DCAwareRoundRobinPolicy.make_query_plan(self, working_keyspace, query):
            if r not in replicas:
                yield r


class ImportProcess(ChildProcess):

    def __init__(self, params):
        ChildProcess.__init__(self, params=params, target=self.run)

        self.skip_columns = params['skip_columns']
        self.valid_columns = params['valid_columns']
        self.skip_column_indexes = [i for i, c in enumerate(self.columns) if c in self.skip_columns]

        options = params['options']
        self.nullval = options.copy['nullval']
        self.max_attempts = options.copy['maxattempts']
        self.min_batch_size = options.copy['minbatchsize']
        self.max_batch_size = options.copy['maxbatchsize']
        self.use_prepared_statements = options.copy['preparedstatements']
        self.dialect_options = options.dialect
        self._session = None
        self.query = None
        self.conv = None
        self.make_statement = None

    @property
    def session(self):
        if not self._session:
            cluster = Cluster(
                contact_points=(self.hostname,),
                port=self.port,
                cql_version=self.cql_version,
                protocol_version=self.protocol_version,
                auth_provider=self.auth_provider,
                load_balancing_policy=FastTokenAwarePolicy(local_dc=self.local_dc),
                ssl_options=ssl_settings(self.hostname, self.config_file) if self.ssl else None,
                default_retry_policy=ExpBackoffRetryPolicy(self),
                compression=None,
                control_connection_timeout=self.connect_timeout,
                connect_timeout=self.connect_timeout,
                idle_heartbeat_interval=0)

            if LibevConnection:
                cluster.connection_class = LibevConnection

            self._session = cluster.connect(self.ks)
            self._session.default_timeout = None
        return self._session

    def run(self):
        try:
            pr = profile_on() if PROFILE_ON else None

            self.inner_run(*self.make_params())

            if pr:
                profile_off(pr, file_name='worker_profile_%d.txt' % (os.getpid(),))

        except Exception, exc:
            if self.debug:
                traceback.print_exc(exc)

        finally:
            self.close()

    def close(self):
        if self._session:
            self._session.cluster.shutdown()
        ChildProcess.close(self)

    def make_params(self):
        metadata = self.session.cluster.metadata
        table_meta = metadata.keyspaces[self.ks].tables[self.table]

        prepared_statement = None
        is_counter = ("counter" in [table_meta.columns[name].typestring for name in self.valid_columns])
        if is_counter:
            query = 'UPDATE %s.%s SET %%s WHERE %%s' % (protect_name(self.ks), protect_name(self.table))
            make_statement = self.wrap_make_statement(self.make_counter_batch_statement)
        elif self.use_prepared_statements:
            query = 'INSERT INTO %s.%s (%s) VALUES (%s)' % (protect_name(self.ks),
                                                            protect_name(self.table),
                                                            ', '.join(protect_names(self.valid_columns),),
                                                            ', '.join(['?' for _ in self.valid_columns]))

            query = self.session.prepare(query)
            query.consistency_level = self.consistency_level
            prepared_statement = query
            make_statement = self.wrap_make_statement(self.make_prepared_batch_statement)
        else:
            query = 'INSERT INTO %s.%s (%s) VALUES (%%s)' % (protect_name(self.ks),
                                                             protect_name(self.table),
                                                             ', '.join(protect_names(self.valid_columns),))
            make_statement = self.wrap_make_statement(self.make_non_prepared_batch_statement)

        conv = ImportConversion(self, table_meta, prepared_statement)
        tm = TokenMap(self.ks, self.hostname, self.local_dc, self.session)
        return query, conv, tm, make_statement

    def inner_run(self, query, conv, tm, make_statement):
        """
        Main run method. Note that we bind self methods that are called inside loops
        for performance reasons.
        """
        self.query = query
        self.conv = conv
        self.make_statement = make_statement

        convert_rows = self.convert_rows
        split_into_batches = self.split_into_batches
        result_callback = self.result_callback
        err_callback = self.err_callback
        session = self.session

        while True:
            chunk = self.inmsg.recv()
            if chunk is None:
                break

            try:
                chunk['rows'] = convert_rows(conv, chunk)
                for replicas, batch in split_into_batches(chunk, conv, tm):
                    statement = make_statement(query, conv, chunk, batch, replicas)
                    future = session.execute_async(statement)
                    future.add_callbacks(callback=result_callback, callback_args=(batch, chunk),
                                         errback=err_callback, errback_args=(batch, chunk, replicas))

            except Exception, exc:
                self.report_error(exc, chunk, chunk['rows'])

    def wrap_make_statement(self, inner_make_statement):
        def make_statement(query, conv, chunk, batch, replicas):
            try:
                return inner_make_statement(query, conv, batch, replicas)
            except Exception, exc:
                print "Failed to make batch statement: {}".format(exc)
                self.report_error(exc, chunk, batch['rows'])
                return None

        def make_statement_with_failures(query, conv, chunk, batch, replicas):
            failed_batch = self.maybe_inject_failures(batch)
            if failed_batch:
                return failed_batch
            return make_statement(query, conv, chunk, batch, replicas)

        return make_statement_with_failures if self.test_failures else make_statement

    def make_counter_batch_statement(self, query, conv, batch, replicas):
        statement = BatchStatement(batch_type=BatchType.COUNTER, consistency_level=self.consistency_level)
        statement.replicas = replicas
        statement.keyspace = self.ks
        for row in batch['rows']:
            where_clause = []
            set_clause = []
            for i, value in enumerate(row):
                if i in conv.primary_key_indexes:
                    where_clause.append("%s=%s" % (self.valid_columns[i], value))
                else:
                    set_clause.append("%s=%s+%s" % (self.valid_columns[i], self.valid_columns[i], value))

            full_query_text = query % (','.join(set_clause), ' AND '.join(where_clause))
            statement.add(full_query_text)
        return statement

    def make_prepared_batch_statement(self, query, _, batch, replicas):
        """
        Return a batch statement. This is an optimized version of:

            statement = BatchStatement(batch_type=BatchType.UNLOGGED, consistency_level=self.consistency_level)
            for row in batch['rows']:
                statement.add(query, row)

        We could optimize further by removing bound_statements altogether but we'd have to duplicate much
        more driver's code (BoundStatement.bind()).
        """
        statement = BatchStatement(batch_type=BatchType.UNLOGGED, consistency_level=self.consistency_level)
        statement.replicas = replicas
        statement.keyspace = self.ks
        statement._statements_and_parameters = [(True, query.query_id, query.bind(r).values) for r in batch['rows']]
        return statement

    def make_non_prepared_batch_statement(self, query, _, batch, replicas):
        statement = BatchStatement(batch_type=BatchType.UNLOGGED, consistency_level=self.consistency_level)
        statement.replicas = replicas
        statement.keyspace = self.ks
        statement._statements_and_parameters = [(False, query % (','.join(r),), ()) for r in batch['rows']]
        return statement

    def convert_rows(self, conv, chunk):
        """
        Return converted rows and report any errors during conversion.
        """
        def filter_row_values(row):
            return [v for i, v in enumerate(row) if i not in self.skip_column_indexes]

        if self.skip_column_indexes:
            rows = [filter_row_values(r) for r in list(csv.reader(chunk['rows'], **self.dialect_options))]
        else:
            rows = list(csv.reader(chunk['rows'], **self.dialect_options))

        errors = defaultdict(list)

        def convert_row(r):
            try:
                return conv.convert_row(r)
            except ParseError, err:
                errors[err.message].append(r)
                return None

        converted_rows = filter(None, [convert_row(r) for r in rows])

        if errors:
            for msg, rows in errors.iteritems():
                self.outmsg.send(ImportTaskError(ParseError.__name__, msg, rows))
                self.update_chunk(rows, chunk)
        return converted_rows

    def maybe_inject_failures(self, batch):
        """
        Examine self.test_failures and see if token_range is either a token range
        supposed to cause a failure (failing_range) or to terminate the worker process
        (exit_range). If not then call prepare_export_query(), which implements the
        normal behavior.
        """
        if 'failing_batch' in self.test_failures:
            failing_batch = self.test_failures['failing_batch']
            if failing_batch['id'] == batch['id']:
                if batch['attempts'] < failing_batch['failures']:
                    statement = SimpleStatement("INSERT INTO badtable (a, b) VALUES (1, 2)",
                                                consistency_level=self.consistency_level)
                    return statement

        if 'exit_batch' in self.test_failures:
            exit_batch = self.test_failures['exit_batch']
            if exit_batch['id'] == batch['id']:
                sys.exit(1)

        return None  # carry on as normal

    @staticmethod
    def make_batch(batch_id, rows, attempts=1):
        return {'id': batch_id, 'rows': rows, 'attempts': attempts}

    def split_into_batches(self, chunk, conv, tm):
        """
        Batch rows by ring position or replica.
        If there are at least min_batch_size rows for a ring position then split these rows into
        groups of max_batch_size and send a batch for each group, using all replicas for this ring position.
        Otherwise, we are forced to batch by replica, and here unfortunately we can only choose one replica to
        guarantee common replicas across partition keys. We are typically able
        to batch by ring position for small clusters or when VNODES are not used. For large clusters with VNODES
        it may not be possible, in this case it helps to increase the CHUNK SIZE but up to a limit, otherwise
        we may choke the cluster.
        """

        rows_by_ring_pos = defaultdict(list)
        errors = defaultdict(list)

        min_batch_size = self.min_batch_size
        max_batch_size = self.max_batch_size
        ring = tm.ring

        get_row_partition_key_values = conv.get_row_partition_key_values_fcn()
        pk_to_token_value = tm.pk_to_token_value
        get_ring_pos = tm.get_ring_pos
        make_batch = self.make_batch

        for row in chunk['rows']:
            try:
                pk = get_row_partition_key_values(row)
                rows_by_ring_pos[get_ring_pos(ring, pk_to_token_value(pk))].append(row)
            except Exception, e:
                errors[e.message].append(row)

        if errors:
            for msg, rows in errors.iteritems():
                self.outmsg.send(ImportTaskError(ParseError.__name__, msg, rows))
                self.update_chunk(rows, chunk)

        replicas = tm.replicas
        filter_replicas = tm.filter_replicas
        rows_by_replica = defaultdict(list)
        for ring_pos, rows in rows_by_ring_pos.iteritems():
            if len(rows) > min_batch_size:
                for i in xrange(0, len(rows), max_batch_size):
                    yield filter_replicas(replicas[ring_pos]), make_batch(chunk['id'], rows[i:i + max_batch_size])
            else:
                # select only the first valid replica to guarantee more overlap or none at all
                rows_by_replica[filter_replicas(replicas[ring_pos])[:1]].extend(rows)

        # Now send the batches by replica
        for replicas, rows in rows_by_replica.iteritems():
            for i in xrange(0, len(rows), max_batch_size):
                yield replicas, make_batch(chunk['id'], rows[i:i + max_batch_size])

    def result_callback(self, _, batch, chunk):
        self.update_chunk(batch['rows'], chunk)

    def err_callback(self, response, batch, chunk, replicas):
        err_is_final = batch['attempts'] >= self.max_attempts
        self.report_error(response, chunk, batch['rows'], batch['attempts'], err_is_final)
        if not err_is_final:
            batch['attempts'] += 1
            statement = self.make_statement(self.query, self.conv, chunk, batch, replicas)
            future = self.session.execute_async(statement)
            future.add_callbacks(callback=self.result_callback, callback_args=(batch, chunk),
                                 errback=self.err_callback, errback_args=(batch, chunk, replicas))

    def report_error(self, err, chunk, rows=None, attempts=1, final=True):
        if self.debug:
            traceback.print_exc(err)
        self.outmsg.send(ImportTaskError(err.__class__.__name__, err.message, rows, attempts, final))
        if final:
            self.update_chunk(rows, chunk)

    def update_chunk(self, rows, chunk):
        num_chunk_rows = len(chunk['rows'])
        chunk['imported'] += len(rows)
        if chunk['imported'] == num_chunk_rows:
            self.outmsg.send(ImportProcessResult(num_chunk_rows))


class RateMeter(object):

    def __init__(self, log_fcn, update_interval=0.25, log_file=''):
        self.log_fcn = log_fcn  # the function for logging, may be None to disable logging
        self.update_interval = update_interval  # how often we update in seconds
        self.log_file = log_file  # an optional file where to log statistics in addition to stdout
        self.start_time = time.time()  # the start time
        self.last_checkpoint_time = self.start_time  # last time we logged
        self.current_rate = 0.0  # rows per second
        self.current_record = 0  # number of records since we last updated
        self.total_records = 0   # total number of records

        if os.path.isfile(self.log_file):
            os.unlink(self.log_file)

    def increment(self, n=1):
        self.current_record += n
        self.maybe_update()

    def maybe_update(self, sleep=False):
        if self.current_record == 0:
            return

        new_checkpoint_time = time.time()
        time_difference = new_checkpoint_time - self.last_checkpoint_time
        if time_difference >= self.update_interval:
            self.update(new_checkpoint_time)
            self.log_message()
        elif sleep:
            remaining_time = time_difference - self.update_interval
            if remaining_time > 0.000001:
                time.sleep(remaining_time)

    def update(self, new_checkpoint_time):
        time_difference = new_checkpoint_time - self.last_checkpoint_time
        if time_difference >= 1e-09:
            self.current_rate = self.get_new_rate(self.current_record / time_difference)

        self.last_checkpoint_time = new_checkpoint_time
        self.total_records += self.current_record
        self.current_record = 0

    def get_new_rate(self, new_rate):
        """
         return the rate of the last period: this is the new rate but
         averaged with the last rate to smooth a bit
        """
        if self.current_rate == 0.0:
            return new_rate
        else:
            return (self.current_rate + new_rate) / 2.0

    def get_avg_rate(self):
        """
         return the average rate since we started measuring
        """
        time_difference = time.time() - self.start_time
        return self.total_records / time_difference if time_difference >= 1e-09 else 0

    def log_message(self):
        if not self.log_fcn:
            return

        output = 'Processed: %d rows; Rate: %7.0f rows/s; Avg. rate: %7.0f rows/s\r' % \
                 (self.total_records, self.current_rate, self.get_avg_rate())
        self.log_fcn(output, eol='\r')
        if self.log_file:
            with open(self.log_file, "a") as f:
                f.write(output + '\n')

    def get_total_records(self):
        self.update(time.time())
        self.log_message()
        return self.total_records
