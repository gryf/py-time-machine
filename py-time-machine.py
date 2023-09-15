#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Rsync Time Machine Style backup.

Inspired by rsync-time-machine https://github.com/infinet/rsync-time-machine
"""
import argparse
import datetime
import fcntl
import getpass
import hashlib
import logging
import os
import re
import shutil
import subprocess
import sys

import yaml
import yaml.error

ONEK = 1024.0
ONEM = 1048576.0
ONEG = 1073741824.0
ONET = 1099511627776.0
ONEDAY = datetime.timedelta(days=1)

XDG_CONF_DIR = os.getenv('XDG_CONFIG_HOME', os.path.expanduser('~/.config'))
CONF_FILES = [os.path.join(XDG_CONF_DIR, 'py-time-machine.yaml'),
              '/etc/py-time-machine.yaml']

if os.getuid() == 0:
    LOG_FILE = '/var/log/py-time-machine.log'
else:
    LOG_FILE = '/tmp/py-time-machine.log'
REMOTE_PAT = re.compile(r'^((?P<user>[A-Za-z0-9\._%\+\-]+)@)?'
                        r'((?P<host>[A-Za-z0-9.\-]+)\:)'
                        r'(?P<path>.+$)')

RSYNC_ARGS = ('--delete',
              '--delete-excluded',
              '--group',
              '--hard-links',
              '--itemize-changes',
              '--links',
              '--numeric-ids',
              '--one-file-system',
              '--owner',
              '--perms',
              '--progress',
              '--recursive',
              '--relative',
              '--times',
              '-D',
              '-q')

RSYNC_EXIT_CODE = {0: 'Success',
                   1: 'Syntax or usage error',
                   2: 'Protocol incompatibility',
                   3: 'Errors selecting input/output files, dirs',
                   4: 'Requested action not supported: an attempt was made to '
                   'manipulate 64-bit files on a platform that cannot support '
                   'them; or an option was specified that is supported by the '
                   'client and not by the server.',
                   5: 'Error starting client-server protocol',
                   6: 'Daemon unable to append to log-file',
                   10: 'Error in socket I/O',
                   11: 'Error in file I/O',
                   12: 'Error in rsync protocol data stream',
                   13: 'Errors with program diagnostics',
                   14: 'Error in IPC code',
                   20: 'Received SIGUSR1 or SIGINT',
                   21: 'Some error returned by waitpid()',
                   22: 'Error allocating core memory buffers',
                   23: 'Partial transfer due to error',
                   24: 'Partial transfer due to vanished source files',
                   25: 'The --max-delete limit stopped deletions',
                   30: 'Timeout in data send/receive',
                   35: 'Timeout waiting for daemon connection'}

PAT_STAT = re.compile(r'.*Namelen: (?P<namemax>\d+).*'
                      r'Block size: (?P<bsize>\d+)\s+'
                      r'Fundamental block size: (?P<frsize>\d+).*'
                      r'Blocks: Total: (?P<blocks>\d+)\s+'
                      r'Free: (?P<bfree>\d+)\s+'
                      r'Available: (?P<bavail>\d+).*'
                      r'Inodes: Total: (?P<files>\d+)\s+'
                      r'Free: (?P<ffree>\d+)', re.DOTALL | re.MULTILINE)


class PyTimeMachine:

    def __init__(self, args):
        self.sources = []
        self.destination = ''
        self.exclude = []
        self.min_space = 1024
        self.min_inodes = 100000
        self.smart_remove = {'keep_all': 1,
                             'keep_one_per_day': 7,
                             'keep_one_per_week': 4,
                             'keep_one_per_month': 12}
        self.rsh_command = None

        self._configfile = None
        if args.config:
            self._configfile = args.config
        self._is_src_remote = None
        self._is_dst_remote = None
        self._fl = None

        self._check_inodes = True

        self._setup_logger(args.log)

    def _setup_logger(self, logfilename):
        handlers = [logging.StreamHandler(sys.stdout)]
        if logfilename:
            handlers.append(logging.FileHandler(logfilename, mode="a"))

        logging.basicConfig(handlers=handlers,
                            level=logging.DEBUG,
                            format="%(asctime)s - %(levelname)s - %(message)s",
                            datefmt="%Y-%m-%d %H:%M:%S")

    def run(self):
        self._read_config()
        self._flock_exclusive()
        try:
            logging.info("Start backup to %s", self.destination)
            self._create_dest_directory()
            stat_before = self._check_freespace()
            self._take_snapshot()
            logging.info('Filesystem before backup:')
            self._print_fs_stat(stat_before)
            logging.info('Filesystem after backup:')
            self._print_fs_stat(self._get_stat())
            logging.info("All done")
        finally:
            self._flock_release()

    def _flock_exclusive(self):
        """lock so only one snapshot of current config is running"""
        m = hashlib.md5()
        m.update(self.destination.encode('utf-8'))
        self._lock_fname = '/tmp/time-machine-%s.lock' % m.hexdigest()
        self._fl = os.open(self._lock_fname, os.O_CREAT | os.O_TRUNC |
                           os.O_WRONLY, 0o0600)
        try:
            fcntl.lockf(self._fl, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except IOError:
            logging.error('Error: cannot obtain lock, there maybe another '
                          'time-machine is running')
            logging.info('Backup task aborted!')
            sys.exit(2)

    def _flock_release(self):
        fcntl.lockf(self._fl, fcntl.LOCK_UN)
        os.close(self._fl)
        os.remove(self._lock_fname)

    def _create_dest_directory(self):
        if self.is_dst_remote:
            if _run(self._dst_cmd[:] + ['ls', self._dst_path]).returncode != 0:
                _run(self._dst_cmd[:] + ['mkdir', '-p', self._dst_path])
        else:
            if not os.path.exists(self.destination):
                os.makedirs(self.destination)

    @property
    def is_dst_remote(self):
        if self._is_dst_remote is None:
            match = REMOTE_PAT.match(self.destination)
            if match:
                match = match.groupdict()
                if not match['user']:
                    match['user'] = getpass.getuser()
                self._dst_cmd = ['ssh', match['user'] + '@' + match['host']]
                self._dst_rsync_partial = match['user'] + '@' + match['host']
                self._dst_path = match['path']
                self._is_dst_remote = True
            else:
                self._dst_path = self.destination
                self._is_dst_remote = False
        return self._is_dst_remote

    def _get_stat(self):
        if self.is_dst_remote:
            res = _run(self._dst_cmd[:] + ['stat', '-f', self._dst_path])
            stat = PAT_STAT.match(res.stdout).groupdict()
            for key in stat:
                stat[key] = int(stat[key])
        else:
            stat = os.statvfs(self.destination)
            stat = {'ffree': stat.f_favail,
                    'files': stat.f_files,
                    'bavail': stat.f_bavail,
                    'bsize': stat.f_bsize,
                    'blocks': stat.f_blocks}
        return stat

    def _find_snapshots(self):
        if self.is_dst_remote:
            res = _run(self._dst_cmd[:] + ['ls', '-1', '--color=none',
                                           self._dst_path]).stdout.split('\n')
        else:
            res = os.listdir(self.destination)

        snapshots = []
        for entry in res:
            try:
                dt = datetime.datetime.strptime(entry, "%Y-%m-%d_%H:%M:%S_GMT")
                snapshots.append((dt, os.path.join(self._dst_path, entry)))
            except ValueError:
                pass

        snapshots.sort()
        return snapshots

    def _take_snapshot(self):
        snapshots = self._find_snapshots()

        now = datetime.datetime.utcnow()
        backup_dst = os.path.join(self._dst_path,
                                  now.strftime("%Y-%m-%d_%H:%M:%S_GMT"))
        args = [x for x in RSYNC_ARGS]

        if self.rsh_command:
            args.extend(['-e', self.rsh_command])

        exclude_patterns = ['--exclude=%s' % x for x in self.exclude]
        args.extend(exclude_patterns)

        latest = os.path.join(self._dst_path, 'latest')
        latest_stat = self._get_file_stat(latest)
        if len(snapshots) > 0 and latest_stat['exists']:
            last_snapshot = latest_stat['target']
            logging.info('Copying last snapshot.')
            if self.is_dst_remote:
                res = _run(self._dst_cmd[:] + ['cp', '-arl', last_snapshot,
                                               backup_dst])
            else:
                res = _run(['cp', '-arl', last_snapshot, backup_dst])

            if res.returncode != 0:
                logging.error('Unable to clone last snapshot, abort.')
                exit(2)

        elif len(snapshots) > 0 and not latest_stat['exists']:
            if latest_stat['islink']:
                if self.is_dst_remote:
                    _run(self._dst_cmd[:] + ['rm', latest])
                else:
                    os.remove(latest)
                logging.error('Error, the "latest" symbol link is broken and '
                              'will be recreated empty for next backup...')
            else:
                logging.error('Error, cannot find the last snapshot, maybe '
                              'the "latest" symbol link has been deleted. We '
                              'will recreate it empty for next backup...')
            if self.is_dst_remote:
                _run(self._dst_cmd[:] + ['mkdir', '-p', backup_dst])
                _run(self._dst_cmd[:] + ['ln', '-s', backup_dst, latest])
            else:
                os.mkdir(backup_dst)
                os.symlink(backup_dst, latest)
            exit(2)

        else:  # len(snapshots) == 0
            if self.is_dst_remote:
                _run(self._dst_cmd[:] + ['rm', latest])
            elif os.path.lexists(latest):
                os.remove(latest)

        if self.is_dst_remote:
            _run(self._dst_cmd[:] + ['mkdir', '-p', backup_dst])
        elif not os.path.exists(backup_dst):
            os.mkdir(backup_dst)

        args.extend(self.sources)
        if self.is_dst_remote:
            args.append(self._dst_rsync_partial + ':' + backup_dst)
        else:
            args.append(backup_dst)

        self._run_rsync(args)

        if self.is_dst_remote:
            _run(self._dst_cmd[:] + ['rm', latest])
            _run(self._dst_cmd[:] + ['ln', '-s', backup_dst, latest])
        elif os.path.exists(latest):
            os.remove(latest)
            os.symlink(backup_dst, latest)

        self._smart_remove(snapshots)

    # function from Back In Time
    def _keep_all(self, snapshots, min_date, max_date):
        """
        Add all snapshots between min_date and max_date to keep_snapshots

        Args:
            snapshots (list):  [(dt1, snapshot_path1), ...]
            min_date (datetime.datetime):   minimum date for snapshots to keep
            max_date (datetime.datetime):   maximum date for snapshots to keep

        Returns:
            list: list of snapshots that should be kept
        """
        res = []
        for (dt, spath) in snapshots:
            if min_date <= dt and dt <= max_date:
                res.append(spath)
        return res

    # function from Back In Time
    def _keep_last(self, snapshots, min_date, max_date):
        """
        Add only the lastest snapshots between min_date and max_date to
        keep_snapshots.

        Args:
            snapshots (list):  [(dt1, snapshot_path1), ...]
            min_date (datetime.datetime):   minimum date for snapshots to keep
            max_date (datetime.datetime):   maximum date for snapshots to keep

        Returns:
            string: the snapshot to be kept
        """
        res = []
        for (dt, spath) in snapshots:
            if min_date <= dt and dt < max_date:
                res.append((dt, spath))

        if res:
            res.sort()
            return [res[-1][1]]
        else:
            return []

    # function from Back In Time
    def inc_month(self, dt):
        """
        First day of next month of ``date`` with respect on new years. So if
        ``date`` is December this will return 1st of January next year.

        Args:
            date (datetime.date):   old date that should be increased

        Returns:
            datetime.date:          1st day of next month
        """
        y = dt.year
        m = dt.month + 1
        if m > 12:
            m = 1
            y = y + 1
        return datetime.datetime(y, m, 1)

    # function from Back In Time
    def dec_month(self, dt):
        """
        First day of previous month of ``date`` with respect on previous years.
        So if ``date`` is January this will return 1st of December previous
        year.

        Args:
            date (datetime.date):   old date that should be decreased

        Returns:
            datetime.date:          1st day of previous month
        """
        y = dt.year
        m = dt.month - 1
        if m < 1:
            m = 12
            y = y - 1
        return datetime.datetime(y, m, 1)

    # function from Back In Time
    def _smart_remove(self, snapshots):
        """
        Remove old snapshots based on configurable intervals.

        Args:
            keep_all (int):                 keep all snapshots for the
                                            last ``keep_all`` days
            keep_one_per_day (int):         keep one snapshot per day for the
                                            last ``keep_one_per_day`` days
            keep_one_per_week (int):        keep one snapshot per week for the
                                            last ``keep_one_per_week`` weeks
            keep_one_per_month (int):       keep one snapshot per month for the
                                            last ``keep_one_per_month`` months
        """
        if len(snapshots) <= 1:
            logging.info("There is only one snapshots, so keep it")
            return

        now = datetime.datetime.utcnow()
        keep_all = self.smart_remove['keep_all']
        keep_one_per_day = self.smart_remove['keep_one_per_day']
        keep_one_per_week = self.smart_remove['keep_one_per_week']
        keep_one_per_month = self.smart_remove['keep_one_per_month']

        # utc 00:00:00
        today = datetime.datetime(now.year, now.month, now.day, 0, 0, 0)
        snapshots.sort()

        # keep the last snapshot
        keep_snapshots = [snapshots[-1][1]]

        # keep all for the last keep_all days x 24 hours
        if keep_all > 0:
            tmp = self._keep_all(snapshots,
                                 now - datetime.timedelta(days=keep_all), now)
            keep_snapshots.extend(tmp)

        # keep one per days for the last keep_one_per_day days
        if keep_one_per_day > 0:
            for _ in range(0, keep_one_per_day):
                tmp = self._keep_last(snapshots, today, today + ONEDAY)
                keep_snapshots.extend(tmp)
                today -= ONEDAY

        # keep one per week for the last keep_one_per_week weeks
        if keep_one_per_week > 0:
            d = today - datetime.timedelta(days=today.weekday() + 1)
            for _ in range(0, keep_one_per_week):
                tmp = self._keep_last(snapshots, d,
                                      d + datetime.timedelta(days=8))
                keep_snapshots.extend(tmp)
                d -= datetime.timedelta(days=7)

        # keep one per month for the last keep_one_per_month months
        if keep_one_per_month > 0:
            d1 = datetime.datetime(now.year, now.month, 1)
            d2 = self.inc_month(d1)
            for i in range(0, keep_one_per_month):
                tmp = self._keep_last(snapshots, d1, d2)
                keep_snapshots.extend(tmp)
                d2 = d1
                d1 = self.dec_month(d1)

        # keep one per year for all years
        first_year = snapshots[0][0].year

        for i in range(first_year, now.year + 1):
            tmp = self._keep_last(snapshots, datetime.datetime(i, 1, 1),
                                  datetime.datetime(i + 1, 1, 1))
            keep_snapshots.extend(tmp)

        tmp = set(keep_snapshots)
        keep_snapshots = tmp
        del_snapshots = []
        for dt, s in snapshots:
            if s in keep_snapshots:
                continue

            del_snapshots.append(s)

        if not del_snapshots:
            logging.info('No snapshot to remove')
            return

        for snapshot in del_snapshots:
            logging.info('Delete snapshot %s', snapshot)

            if self.is_dst_remote:
                _run(self._dst_cmd[:] + ['rm', '-fr', snapshot])
            else:
                shutil.rmtree(snapshot)

    def _run_rsync(self, args):
        cmd = ['rsync']
        cmd.extend(args)
        logging.info('running cmd: %s', ' '.join(cmd))
        try:
            res = _run(cmd)
            if res.returncode == 0:
                logging.info('Rsynced successfully')
            else:
                logging.error('Rsync Error %d, %s', res.returncode,
                              RSYNC_EXIT_CODE[res.returncode])
        except Exception:
            logging.error('Rsync Exception')

    def _get_file_stat(self, latest):
        result = {'exists': False, 'broken': False, 'islink': False,
                  'target': None}

        if self.is_dst_remote:
            res = _run(self._dst_cmd[:] + ['readlink', latest])
            if res.returncode == 0:
                result['target'] = res.stdout.strip()
            if _run(self._dst_cmd[:] +
                    [f'[ -e "{latest}" ]']).returncode != 0:
                result['broken'] = True
            else:
                result['exists'] = True
            if _run(self._dst_cmd[:] +
                    [f'[ -L "{latest}" ]']).returncode == 0:
                result['exists'] = True
                result['islink'] = True
            return result

        if os.path.islink(latest):
            result['islink'] = os.path.islink(latest)
            result['exists'] = os.path.islink(latest)
            result['broken'] = not os.path.exists(latest)
            result['target'] = os.path.join(self._dst_path,
                                            os.readlink(latest))
        else:
            result['exists'] = os.path.islink(latest)
        return result

    def _check_freespace(self):
        """abort backup if not enough free space or inodes"""
        stat = self._get_stat()

        # Linux btrfs does not report total and free inodes. Ignore inodes
        # check
        if stat['files'] == 0:
            self._check_inodes = False
        else:
            inodes_free = stat['ffree']
            if inodes_free < self.min_inodes:
                logging.error('Error: not enough inodes, the backup '
                              'filesystem has %d free inodes, the minimum '
                              'requirement is %d', inodes_free,
                              self.min_inodes)
                logging.info('Backup task aborted!')
                sys.exit(2)

        space_free = stat['bavail'] * stat['bsize'] / ONEM
        if space_free < self.min_space:
            logging.error('Error: not enough space, the backup filesystem has '
                          '%.0f MB free space, the minimum requirement is %d '
                          'MB', space_free, self.min_space)
            logging.info('Backup task aborted!')
            sys.exit(2)
        return stat

    def _print_fs_stat(self, stat):

        def humanize_bytes(n):
            n = int(n)
            if n >= ONET:
                ret = '%.2f TB' % (n / ONET)
            elif n >= ONEG:
                ret = '%.2f GB' % (n / ONEG)
            elif n >= ONEM:
                ret = '%.0f MB' % (n / ONEM)
            elif n >= ONEK:
                ret = '%.0f KB' % (n / ONEK)
            else:
                ret = '%d Bytes' % n

            return ret

        def humanize_inodes(n):
            n = int(n)
            if n >= ONET:
                ret = '%.0f T' % (n / ONET)
            elif n >= ONEG:
                ret = '%.0f G' % (n / ONEG)
            elif n >= ONEM:
                ret = '%.0f M' % (n / ONEM)
            elif n >= ONEK:
                ret = '%.0f K' % (n / ONEK)
            else:
                ret = '%d ' % n

            return ret

        space_free = stat['bavail'] * stat['bsize']
        space_total = stat['blocks'] * stat['bsize']
        space_used = (space_total - space_free) * 100.0 / space_total
        logging.info('    free space: %s, %.1f%% used',
                     humanize_bytes(space_free), space_used)
        if self._check_inodes:
            inodes_free = stat['ffree']
            inodes_used = (stat['files'] -
                           stat['ffree']) * 100.0 / stat['files']
            logging.info('    free inodes: %s, %.1f%% used',
                         humanize_inodes(inodes_free), inodes_used)

    def _read_config(self):
        conf_files = CONF_FILES[:]
        if self._configfile:
            conf_files = [self._configfile]
            try:
                with open(self._configfile) as fobj:
                    fobj.read()
            except OSError as ex:
                pass
                logging.error('Cannot access file %s: %s', self._configfile,
                              ex.strerror)
                sys.exit(1)

        conf_dict = None
        for fname in conf_files:
            if not os.path.exists(fname):
                # lets try another location
                continue

            try:
                with open(fname) as fobj:
                    conf_dict = yaml.safe_load(fobj)
            except OSError as ex:
                logging.error('Cannot access file %s: %s', self._configfile,
                              ex.strerror)
                sys.exit(1)

            except yaml.error.YAMLError as ex:
                logging.error('Error loading config file:\n%s', str(ex))
                sys.exit(1)

            break

        if conf_dict is None:
            logging.error('Cannot find config file. Provide one with the '
                          '`--config` argument, or populate it in the common '
                          'locations:\n - %s\n - %s', CONF_FILES[0],
                          CONF_FILES[1])
            sys.exit(2)

        # check required options
        if not all((conf_dict.get('source'), conf_dict.get('destination'))):
            logging.error('Invalid config file: no source and/or destination '
                          'keys found')
            sys.exit(1)

        self.sources = conf_dict['source']
        if isinstance(self.sources, str):
            self.sources = [self.sources]

        self.destination = conf_dict['destination']
        if not isinstance(self.destination, str):
            logging.error('Invalid config file: Destination should be single '
                          'string.')
            sys.exit(1)

        self.exclude = conf_dict.get('exclude', [])
        if isinstance(self.exclude, str):
            self.exclude = [self.exclude]

        sr = conf_dict.get('smart_remove', {})
        for key in ('keep_all', 'keep_one_per_day', 'keep_one_per_week',
                    'keep_one_per_month'):
            if sr.get(key):
                self.smart_remove[key] = sr[key]

        sr = conf_dict.get('free_space', {})
        for key in ('min_space', 'min_inodes'):
            if sr.get(key):
                setattr(self, key, sr[key])

        if conf_dict.get('rsh_command'):
            self.rsh_command = conf_dict['rsh_command']


def _run(command):
    return subprocess.run(command, capture_output=True, encoding='utf-8')


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--config', help='alternative config file')
    parser.add_argument('-l', '--log', help='log to provided file')

    args = parser.parse_args()

    ptm = PyTimeMachine(args)
    t_start = datetime.datetime.now()
    ptm.run()
    t_used = datetime.datetime.now() - t_start
    logging.info('Backup runtime: %s', str(t_used).split('.')[0])

    sys.exit(0)


if __name__ == "__main__":
    main()
