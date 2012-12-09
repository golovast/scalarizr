from __future__ import with_statement

import os
import re
import sys
import glob
import string
import shutil
import logging
import subprocess

from scalarizr import linux, storage2
from scalarizr.linux import coreutils, pkgmgr
from scalarizr.node import __node__
from scalarizr.services import mysql as mysql_svc
from scalarizr.services import backup
from scalarizr.libs import bases
from scalarizr.storage2.cloudfs import LargeTransfer
from scalarizr.libs import metaconf




LOG = logging.getLogger(__name__)


class Error(Exception):
	pass


__behavior__ = 'percona' \
			if 'percona' in __node__['behavior'] \
			else 'mysql2'


__mysql__ = __node__[__behavior__]
__mysql__.update({
	'behavior': __behavior__,
	'port': 3306,
	'storage_dir': '/mnt/dbstorage',
	'data_dir': '/mnt/dbstorage/mysql-data',
	'binlog_dir': '/mnt/dbstorage/mysql-misc',
	'tmp_dir': '/mnt/dbstorage/tmp',
	'root_user': 'scalr',
	'repl_user': 'scalr_repl',
	'stat_user': 'scalr_stat',
	'pma_user': 'pma',
	'debian.cnf': '/etc/mysql/debian.cnf',
	'my.cnf': '/etc/my.cnf' if linux.os['family'] in ('RedHat', 'Oracle') else '/etc/mysql/my.cnf',
	#'mysqld_exec': util.try_exec('/usr/sbin/mysqld', '/usr/libexec/mysqld')
	'mysqldump_chunk_size': 200 * 1024 * 1024,
	'stop_slave_timeout': 180,
	'change_master_timeout': 60,
	'defaults': {
		'datadir': '/var/lib/mysql',
		'log_bin': 'mysql_bin'
	}
})


class MySQLSnapBackup(backup.SnapBackup):
	def __init__(self, **kwds):
		super(MySQLSnapBackup, self).__init__(**kwds)
		self.on(
			freeze=self.freeze,
			unfreeze=self.unfreeze
		)
		self._mysql_init = mysql_svc.MysqlInitScript()

	def _client(self):
		return mysql_svc.MySQLClient(
					__mysql__['root_user'],
					__mysql__['root_password'])


	def freeze(self, volume, state):
		self._mysql_init.start()
		client = self._client()
		client.lock_tables()
		(log_file, log_pos) = client.master_status()

		upd = {'log_file': log_file, 'log_pos': log_pos}
		state.update(upd)
		self.tags.update(upd)


	def unfreeze(self, *args):
		client = self._client()
		client.unlock_tables()

		
class MySQLSnapRestore(backup.SnapRestore):
	def __init__(self, **kwds):
		super(MySQLSnapRestore, self).__init__(**kwds)
		self.on(complete=self.complete)

	def complete(self, volume):
		vol = storage2.volume(volume)
		vol.mpoint = __mysql__['storage_dir']
		vol.mount()


backup.backup_types['snap_mysql'] = MySQLSnapBackup
backup.restore_types['snap_mysql'] = MySQLSnapRestore


class XtrabackupMixin(object):
	def __init__(self):
		self.error_messages.update({
			'invalid_backup_type': "Invalid backup type: %s. "
								"Supported values are 'full' | 'incremental'"
		})

	def _check_backup_type(self):
		msg = self.error_messages['invalid_backup_type'] % self.backup_type
		assert self.backup_type in ('full', 'incremental'), msg


class XtrabackupBackup(XtrabackupMixin, backup.Backup):

	def __init__(self, 
				backup_type='full', 
				from_lsn=None,
				backup_dir='/mnt/dbbackup',
				volume=None,
				**kwds):
		'''
		:type backup_type: string
		:param backup_type: Xtrabackup type. Valid values are
			* full
			* incremental

		:type from_lsn: int
		:param from_lsn: Log sequence number to start from

		:type backup_dir: string
		:param backup_dir: Directory to store backup files

		:type volume: :class:`scalarizr.storage2.volumes.base.Volume` or dict
		:param volume: A volume object or configuration to ensure and mount 
			to 'backup_dir'. After backup completion it will be snapshotted 
			and snapshot will be available in Restore configuration
		'''
		backup.Backup.__init__(self, 
				backup_type=backup_type, from_lsn=from_lsn,
				backup_dir=backup_dir, volume=volume, **kwds)
		XtrabackupMixin.__init__(self)


	def _run(self):
		self._check_backup_type()
		if self.volume:
			self.volume = storage2.volume(self.volume)
			if self.tags:
				self.volume.tags = self.tags
			self.volume.mpoint = self.backup_dir
			self.volume.ensure(mount=True, mkfs=True)
		elif not os.path.exists(self.backup_dir):
			os.makedirs(self.backup_dir)

		kwds = {}
		if self.backup_type == 'incremental':
			from_lsn = self.from_lsn
			if not from_lsn:
				checkpoints = self._checkpoints()
				from_lsn = checkpoints['to_lsn']
			kwds.update({
				'incremental': True,
				'incremental_lsn': from_lsn
			})
		elif 'full' == self.backup_type and self.volume:
			coreutils.clean_dir(self.backup_dir)

		exc_info = None
		try:
			LOG.info('Creating %s xtrabackup', self.backup_type)
			innobackupex(self.backup_dir, 
					user=__mysql__['root_user'], 
					password=__mysql__['root_password'],
					**kwds)
			log_file, log_pos = self._binlog_info()
			chkpoints = self._checkpoints()
			to_lsn = chkpoints['to_lsn']
			from_lsn = chkpoints['from_lsn']
			snapshot = None
		except:
			exc_info = sys.exc_info()
		finally:
			if self.volume:
				try:
					self.volume.detach()
				except:
					msg = 'Failed to detach backup volume: %s'
					LOG.warn(msg, sys.exc_info()[1])
		if exc_info:
			raise exc_info[0], exc_info[1], exc_info[2]
		if self.volume:
			snapshot = self.volume.snapshot(
						self.description or 'MySQL xtrabackup', 
						self.tags)

		return backup.restore(
				type='xtrabackup', 
				log_file=log_file, 
				log_pos=log_pos,
				from_lsn=from_lsn,
				to_lsn=to_lsn,
				backup_type=self.backup_type,
				backup_dir=self.backup_dir,
				volume=self.volume.clone(),				 
				snapshot=snapshot)


	def _latest_backup_dir(self):
		try:
			dirs = filter(lambda x: not x.startswith('.'), os.listdir(self.backup_dir))
			name = sorted(dirs)[0]
		except IndexError:
			msg = 'Failed to find any previous backup in %s'
			raise Error(msg, self.backup_dir)
		else:
			return os.path.join(self.backup_dir, name) 	

	
	def _checkpoints(self, filename=None):
		if not filename:
			filename = self._latest_backup_dir() + '/xtrabackup_checkpoints'
		ret = {}
		for line in open(filename):
			key, value = line.split('=')
			ret[key.strip()] = value.strip()
		return ret


	def _binlog_info(self, filename=None):
		if not filename:
			filename = self._latest_backup_dir() + '/xtrabackup_binlog_info'
		return map(string.strip, open(filename).read().strip().split('\t'))


class XtrabackupRestore(XtrabackupMixin, backup.Restore):
	'''
	Example:
		rst = backup.restore(
					type='xtrabackup', 
					snapshot=dict(type='ebs', id='snap-12345678'))
	'''

	def __init__(self, 
				log_file=None,
				log_pos=None,
				from_lsn=None,
				to_lsn=None,
				backup_type=None,
				backup_dir='/mnt/dbbackup',
				volume=None,
				snapshot=None,
				**kwds):
		'''
		:type log_file: string
		:param log_file: MySQL binary log file (e.g. binlog.000003)

		:type log_pos: int
		:param log_pos: MySQL binary log file position (e.g. 126)

		:type from_lsn: int
		:param from_lsn: InnoDB start log sequence number

		:type to_lsn: int
		:param to_lsn: InnoDB end log sequence number
		
		:type backup_type: string
		:param backup_type: Xtrabackup type. Valid values are
			* full
			* incremental
		
		:type backup_dir: string
		:param backup_dir: Directory to store backup files

		:type volume: :class:`scalarizr.storage2.volumes.base.Volume` or dict
		:param volume: A volume object or configuration to ensure and mount 
			to 'backup_dir'.

		:type snapshot: :class:`scalarizr.storage2.volumes.base.Snapshot` 
			or dict
		:param snapshot: A snapshot object to restore backup Volume from
		'''
		backup.Restore.__init__(self, 
				log_file=log_file, log_pos=log_pos, from_lsn=from_lsn,
				to_lsn=to_lsn, backup_type=backup_type, backup_dir=backup_dir,
				volume=volume, snapshot=snapshot, **kwds)
		XtrabackupMixin.__init__(self)
		self.features['master_binlog_reset'] = True
		self._mysql_init = mysql_svc.MysqlInitScript()
		self._data_dir = None
		self._binlog_dir = None
		self._log_bin = None

	def _run(self):
		if self.backup_type:
			self._check_backup_type()
		rst_volume = None
		exc_info = None
		'''
		# Create custom my.cnf
		# XXX: it's not a good think to do, but we should create this hacks, 
		# cause when handler calls restore.run() my.cnf is not patched yet 
		shutil.copy(__mysql__['my.cnf'], '/tmp/my.cnf')
		mycnf = metaconf.Configuration('mysql')
		mycnf.read('/tmp/my.cnf')
		try:
			mycnf.options('mysqld')
		except metaconf.NoPathError:
			mycnf.add('mysqld')
		mycnf.set('mysqld/datadir', __mysql__['data_dir'])
		mycnf.set('mysqld/log-bin', __mysql__['binlog_dir'])
		mycnf.write('/tmp/my.cnf')
		'''
		
		my_defaults = my_print_defaults('mysqld')
		rst_volume = None
		self._data_dir = os.path.normpath(my_defaults['datadir'])
		LOG.info('_run: datadir is "%s"' % self._data_dir)
		self._log_bin = os.path.normpath(my_defaults['log_bin'])
		if self._log_bin.startswith('/'):
			self._binlog_dir = os.path.dirname(self._log_bin)
		
		try:
			if self.snapshot:
				LOG.info('Creating restore volume from snapshot')
				if self.volume:
					# Clone volume object
					self.volume = storage2.volume(self.volume)
					rst_volume = self.volume.clone()
					rst_volume.snap = self.snapshot
				else:
					self.snapshot = storage2.snapshot(self.snapshot)
					rst_volume = storage2.volume(type=self.snapshot.type, 
											snap=self.snapshot)
				rst_volume.tags.update({'tmp': 1})
				rst_volume.mpoint = self.backup_dir
				rst_volume.ensure(mount=True)

	
			if not os.listdir(self.backup_dir):
				msg = 'Failed to find any backups in %s'
				raise Error(msg, self.backup_dir)
			
			backups = sorted(os.listdir(self.backup_dir))
			LOG.info('Preparing the base backup')
			base = backups.pop(0)
			target_dir = os.path.join(self.backup_dir, base)
			innobackupex(target_dir, 
						apply_log=True, 
						redo_only=True,
						user=__mysql__['root_user'],
						password=__mysql__['root_password'])
			for inc in backups:
				LOG.info('Preparing incremental backup %s', inc)
				innobackupex(target_dir,
							apply_log=True, 
							redo_only=True, 
							incremental_dir=os.path.join(self.backup_dir, inc),
							user=__mysql__['root_user'],
							password=__mysql__['root_password'])
			LOG.info('Preparing the full backup')
			innobackupex(target_dir, 
						apply_log=True, 
						user=__mysql__['root_user'],
						password=__mysql__['root_password'])
			
			LOG.info('Copying backup to datadir')
			self._mysql_init.stop()
			self._start_copyback()
			try:
				innobackupex(target_dir, copy_back=True)
				coreutils.chown_r(self._data_dir, 
								'mysql', 'mysql')
				self._mysql_init.start()
				self._commit_copyback()
			except:
				self._rollback_copyback()
				raise
		except:
			exc_info = sys.exc_info()
		finally:
			if rst_volume:
				LOG.info('Destroying restore volume')
				try:
					rst_volume.destroy(force=True)
				except:
					msg = 'Failed to destroy volume %s: %s'
					LOG.warn(msg, rst_volume.id, sys.exc_info()[1])
		if exc_info:
			raise exc_info[0], exc_info[1], exc_info[2]


	def _start_copyback(self):
		src = self._data_dir
		dst = src + '.bak'
		LOG.debug('Backup %s -> %s', src, dst)
		os.rename(src, dst)
		if self._binlog_dir:
			for name in glob.glob(self._log_bin + '*'):
				src = os.path.join(self._binlog_dir, name)
				dst = src + '.bak'
				LOG.debug('Backup %s -> %s', src, dst)
				os.rename(src, dst)
		os.makedirs(self._data_dir)				
		
	
	def _commit_copyback(self):
		shutil.rmtree(self._data_dir + '.bak')
		if self._binlog_dir:
			for name in glob.glob(self._log_bin + '*.bak'):
				LOG.debug('Remove %s' % os.path.join(self._binlog_dir, name))
				os.remove(os.path.join(self._binlog_dir, name))
	
	
	def _rollback_copyback(self):
		if os.path.exists(self._data_dir):
			shutil.rmtree(self._data_dir)
		os.rename(self._data_dir + '.bak', self._data_dir)
		if self._binlog_dir:
			for name in glob.glob(self._log_bin + '*.bak'):
				dstname = os.path.splitext(name)[0]
				shutil.move(os.path.join(self._binlog_dir, name), 
							os.path.join(self._binlog_dir, dstname))


backup.backup_types['xtrabackup'] = XtrabackupBackup
backup.restore_types['xtrabackup'] = XtrabackupRestore		


class MySQLDumpBackup(backup.Backup):
	'''
	Example:
		bak = backup.backup(
				type='mysqldump',
				cloudfs_dir='s3://scalr-1a8f341e/backups/mysql/1265/')
		bak.run()
	'''

	def __init__(self,
				cloudfs_dir=None,
				file_per_database=True,
				chunk_size=None,
				**kwds):
		super(MySQLDumpBackup, self).__init__(cloudfs_dir=cloudfs_dir, 
				file_per_database=file_per_database, 
				chunk_size=chunk_size or __mysql__['mysqldump_chunk_size'],
				**kwds)
		self.features.update({
			'start_slave': False
		})


	def _run(self):
		client = mysql_svc.MySQLClient(
					__mysql__['root_user'],
					__mysql__['root_password'])
		self._databases = client.list_databases()
		transfer = LargeTransfer(self._gen_src, self._gen_dst, 'upload', 
								tar_it=False, chunk_size=self.chunk_size)
		transfer.run()
		return backup.restore(type='mysqldump', 
						files=transfer.result()['completed'])


	def _gen_src(self):
		if self.file_per_database:
			for db_name in self._databases:
				self._current_db = db_name
				cmd = linux.build_cmd_args(
					executable='/usr/bin/mysqldump',
					params=__mysql__['mysqldump_options'].split() + [db_name])
				mysql_dump = subprocess.Popen(cmd, bufsize=-1, 
								stdout=subprocess.PIPE, stderr=subprocess.PIPE)
				yield mysql_dump.stdout
		else:
			cmd = linux.build_cmd_args(
				executable='/usr/bin/mysqldump',
				params=__mysql__['mysqldump_options'].split() + ['--all-databases'])
			mysql_dump = subprocess.Popen(cmd, bufsize=-1, 
							stdout=subprocess.PIPE, stderr=subprocess.PIPE)
			yield mysql_dump.stdout


	def _gen_dst(self):
		while True:
			if self.file_per_database:
				yield os.path.join(self.cloudfs_dir, self._current_db)
			else:
				yield os.path.join(self.cloudfs_dir, 'mysql')


backup.backup_types['mysqldump'] = MySQLDumpBackup


class User(bases.ConfigDriven):
	def __init__(self, user=None, password=None, privileges='*'):
		pass


	def ensure(self):
		pass


	def exists(self):
		pass


	def delete(self):
		pass


def innobackupex(*params, **long_kwds):
	if not os.path.exists('/usr/bin/innobackupex'):
		pkgmgr.installed('percona-xtrabackup')
	return linux.system(linux.build_cmd_args(
			executable='/usr/bin/innobackupex', 
			long=long_kwds, 
			params=params))
		
		
def my_print_defaults(*option_groups):
	out = linux.system(linux.build_cmd_args(
			executable='/usr/bin/my_print_defaults', 
			params=option_groups))[0]
	ret = {}
	for line in out.splitlines():
		cols = line.split('=')
		ret[cols[0][2:]] = cols[1] if len(cols) > 1 else True
	for key in __mysql__['defaults']:
		if key not in ret:
			ret[key] = __mysql__['defaults'][key]
	return ret


def mysqldump(*databases, **long_kwds):
	output = long_kwds.pop('output', None)
	cmd = linux.build_cmd_args(
			executable='/usr/bin/mysqldump',
			long=long_kwds,
			params=databases)
	kwds = {}
	if output:
		kwds['stdout'] = open(output, 'w+')
	return linux.system(cmd, **kwds)


def mysqlbinlog(log_file, **log_kwds):
	return linux.system(linux.build_cmd_args(
			executable='/usr/bin/mysqlbinlog',
			long=log_kwds,
			params=[log_file]))


def mysqlbinlog_head():
	'''
	Returns the first binary log file position
	Example:
		>> binlog_head()
		>> ('binlog.000001', 107)
	'''
	my_defaults = my_print_defaults('mysqld')
	binlog_dir = os.path.dirname(my_defaults['log_bin']) \
				if my_defaults['log_bin'][0] == '/' \
				else my_defaults['datadir']
	binlog_index = os.path.join(binlog_dir, 
					os.path.basename(my_defaults['log_bin'])) + '.index'
	with open(binlog_index) as fp:
		binlog_1 = fp.readline().strip()
		binlog_1 = os.path.join(binlog_dir, binlog_1)
	# FORMAT_DESCRIPTION_EVENT minimum length
	# @see http://dev.mysql.com/doc/internals/en/binary-log-versions.html
	stop_position = 91 
	out = mysqlbinlog(binlog_1, verbose=True, 
					stop_position=stop_position)[0]
	end_log_pos_re = re.compile(r'end_log_pos\s+(\d+)')
	for line in out.splitlines():
		m = end_log_pos_re.search(line) # must be search?
		if m:
			return (os.path.basename(binlog_1), m.group(1))

	msg = 'Failed to read FORMAT_DESCRIPTION_EVENT ' \
			'at the top of the %s' % binlog_1
	raise Error(msg)

	
