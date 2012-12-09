'''
Created on Nov 15, 2011

@author: dmitry
'''

from __future__ import with_statement
import os
import sys
import time
import shutil
import logging
import glob
import tarfile
import tempfile

# Core
from scalarizr.bus import bus
from scalarizr.messaging import Messages
from scalarizr.handlers import ServiceCtlHandler, DbMsrMessages, HandlerError, prepare_tags, operation
import scalarizr.services.mysql as mysql_svc
from scalarizr.service import CnfController, _CnfManifest
from scalarizr.services import ServiceError
from scalarizr.platform import UserDataOptions
from scalarizr.util import system2, disttool, firstmatched, initdv2, software, cryptotool, filetool
from scalarizr.storage import transfer

from scalarizr import storage2
from scalarizr.linux import iptables	
from scalarizr.services import backup
from scalarizr.services import mysql2 as mysql2_svc  # backup/restore providers
from scalarizr.node import __node__
from scalarizr.services import make_backup_steps

# Libs
from scalarizr.libs.metaconf import Configuration, NoPathError


LOG = logging.getLogger(__name__)

SU_EXEC = '/bin/su'
BASH = '/bin/bash'

__mysql__ = mysql2_svc.__mysql__


'''
OPT_ROOT_PASSWORD 		= "root_password"
OPT_REPL_PASSWORD 		= "repl_password"
OPT_STAT_PASSWORD   	= "stat_password"
OPT_REPLICATION_MASTER  = "replication_master"

OPT_LOG_FILE 			= "log_file"
OPT_LOG_POS				= "log_pos"

OPT_VOLUME_CNF			= 'volume_config'
OPT_SNAPSHOT_CNF		= 'snapshot_config'

CHANGE_MASTER_TIMEOUT   = '60'
'''
'''
# Mysql storage constants
STORAGE_PATH 			= "/mnt/dbstorage"
STORAGE_TMP_DIR 		= "tmp"
STORAGE_VOLUME_CNF 		= 'mysql.json'
STORAGE_SNAPSHOT_CNF 	= 'mysql-snap.json'

# System users
ROOT_USER 				= "scalr"
REPL_USER 				= "scalr_repl"
STAT_USER 				= "scalr_stat"
PMA_USER 				= "pma"

BACKUP_CHUNK_SIZE 		= 200*1024*1024
STOP_SLAVE_TIMEOUT		= 180
DEFAULT_DATADIR			= "/var/lib/mysql"
DEBIAN_CNF_PATH			= "/etc/mysql/debian.cnf"

DATA_DIR = os.path.join(STORAGE_PATH, mysql_svc.STORAGE_DATA_DIR)
'''

PRIVILEGES = {
	__mysql__['repl_user']: ('Repl_slave_priv', ), 
	__mysql__['stat_user']: ('Repl_client_priv', )
}
	
	
class MysqlMessages:
	
	CREATE_PMA_USER = "Mysql_CreatePmaUser"
	"""
	@ivar pma_server_ip: User host
	@ivar farm_role_id
	@ivar root_password
	"""
	
	CREATE_PMA_USER_RESULT = "Mysql_CreatePmaUserResult"
	"""
	@ivar status: ok|error
	@ivar last_error
	@ivar pma_user
	@ivar pma_password
	@ivar farm_role_id
	"""

	CONVERT_VOLUME = "ConvertVolume"
	"""
	@ivar volume: volume configuration
	"""

	CONVERT_VOLUME_RESULT = "ConvertVolumeResult"
	"""
	@ivar status: ok|error
	@ivar last_error
	@ivar volume: converted volume configuration
	"""



def get_handlers():
	return [MysqlHandler()]
	
	
class DBMSRHandler(ServiceCtlHandler):
	pass

initdv2.explore(__mysql__['behavior'], mysql_svc.MysqlInitScript)

class MysqlCnfController(CnfController):
	_mysql_version = None
	_merged_manifest = None	
	_cli = None
	
	def __init__(self):
		self._init_script = initdv2.lookup(__mysql__['behavior'])
		definitions = {'ON':'1', 'TRUE':'1','OFF':'0','FALSE':'0'}
		CnfController.__init__(self, 
				__mysql__['behavior'], 
				mysql_svc.MYCNF_PATH, 
				'mysql', 
				definitions) #TRUE,FALSE

	@property
	def root_client(self):
		if not self._cli:
			self._cli = mysql_svc.MySQLClient(
						__mysql__['root_user'], 
						__mysql__['root_password'])	
		return self._cli
	
	
	@property
	def _manifest(self):
		f_manifest = CnfController._manifest
		base_manifest = f_manifest.fget(self)		
		path = self._manifest_path

		s = {}
		out = None
		
		if not self._merged_manifest:
			cmd = '%s --no-defaults --verbose --help' % mysql_svc.MYSQLD_PATH
			out = system2('%s - mysql -s %s -c "%s"' % (SU_EXEC, BASH, cmd),shell=True, raise_exc=False,silent=True)[0]
			
		if out:
			raw = out.split('------------------------------------------------- ------------------------')
			if raw:
				a = raw[-1].split('\n')
				if len(a) > 5:
					b = a[1:-5]
					for item in b:
						c = item.split()
						if len(c) > 1:
							key = c[0]
							val = ' '.join(c[1:])
							s[key.strip()] = val.strip()
		
		if s:	
			m_config = Configuration('ini')
			if os.path.exists(path):
				m_config.read(path)		
				
			for variable in base_manifest:
				name = variable.name
				dv_path = './%s/default-value' % name
				
				try:
					old_value =  m_config.get(dv_path)
					if name in s:
						new_value = s[name] 
					else:
						name = name.replace('_','-')
						if name in s:
							new_value = self.definitions[s[name]] if s[name] in self.definitions else s[name]
							if old_value != new_value and new_value != '(No default value)':
								LOG.debug('Replacing %s default value %s with precompiled value %s' % (name, old_value, new_value))
								m_config.set(path=dv_path, value=new_value, force=True)
				except NoPathError, e:
					pass
			m_config.write(path)
					
		self._merged_manifest = _CnfManifest(path)
		return self._merged_manifest

			
	def get_system_variables(self):
		vars = CnfController.get_system_variables(self)
		LOG.debug('Variables from config: %s' % str(vars))
		if self._init_script.running:
			cli_vars = self.root_client.show_global_variables()
			LOG.debug('Variables from cli: %s' % str(cli_vars))
			vars.update(cli_vars)
		return vars
	
	def apply_preset(self, preset):
		
		CnfController.apply_preset(self, preset)
	
	def _before_apply_preset(self):
		self.sendline = ''
		
	def _after_set_option(self, option_spec, value):
		LOG.debug('callback "_after_set_option": %s %s (Need restart: %s)' 
				% (option_spec, value, option_spec.need_restart))
		
		if value != option_spec.default_value and not option_spec.need_restart:
			LOG.debug('Preparing to set run-time variable %s to %s' % (option_spec.name, value))
			self.sendline += 'SET GLOBAL %s = %s; ' % (option_spec.name, value)
			

	def _after_remove_option(self, option_spec):
		if option_spec.default_value and not option_spec.need_restart:
			LOG.debug('Preparing to set run-time variable %s to default [%s]' 
						% (option_spec.name,option_spec.default_value))
			'''
			when removing mysql options DEFAULT keyword must be used instead of
			self.sendline += 'SET GLOBAL %s = %s; ' % (option_spec.name, option_spec.default_value)
			'''
			self.sendline += 'SET GLOBAL %s = DEFAULT; ' % (option_spec.name)
	
	def _after_apply_preset(self):
		if not self._init_script.running:
			LOG.info('MySQL isn`t running, skipping process of applying run-time variables')
			return
		
		if self.sendline and self.root_client.test_connection():
			LOG.debug(self.sendline)
			try:
				self.root_client.fetchone(self.sendline)
			except BaseException, e:
				LOG.error('Cannot set global variables: %s' % e)
			else:
				LOG.debug('All global variables has been set.')
			finally:
				#temporary fix for percona55 backup issue (SCALARIZR-435)
				self.root_client.reconnect()
		elif not self.sendline:
			LOG.debug('No global variables changed. Nothing to set.')
		elif not self.root_client.test_connection():
			LOG.debug('No connection to MySQL. Skipping SETs.')

	
	def _get_version(self):
		if not self._mysql_version:
			info = software.software_info('mysql')
			self._mysql_version = info.version
		return self._mysql_version


class MysqlHandler(DBMSRHandler):
	
	
	def __init__(self):
		self.mysql = mysql_svc.MySQL()
		ServiceCtlHandler.__init__(self, 
				__mysql__['behavior'], 
				self.mysql.service, 
				MysqlCnfController())



		bus.on(init=self.on_init, reload=self.on_reload)
		bus.define_events(
			'before_mysql_data_bundle',
			'mysql_data_bundle',
			# @param host: New master hostname 
			'before_mysql_change_master',
			# @param host: New master hostname 
			# @param log_file: log file to start from 
			# @param log_pos: log pos to start from 
			'mysql_change_master'
			'before_slave_promote_to_master',
			'slave_promote_to_master'
		)
		
		self._phase_mysql = 'Configure MySQL'
		self._phase_data_bundle = self._op_data_bundle = 'MySQL data bundle'
		self._phase_backup = self._op_backup = 'MySQL backup'
		self._step_upload_to_cloud_storage = 'Upload data to cloud storage'
		self._step_accept_scalr_conf = 'Accept Scalr configuration'
		self._step_patch_conf = 'Patch my.cnf configuration file'
		self._step_create_storage = 'Create storage'
		self._step_move_datadir = 'Move data directory to storage'
		self._step_create_users = 'Create Scalr users'
		self._step_restore_users = 'Restore Scalr users'
		self._step_create_data_bundle = 'Create data bundle'
		self._step_change_replication_master = 'Change replication Master'
		self._step_innodb_recovery = 'InnoDB recovery'
		self._step_collect_hostup_data = 'Collect HostUp data'
		
		
		self.on_reload()	

		
	def accept(self, message, queue, behaviour=None, platform=None, os=None, dist=None):
		return __mysql__['behavior'] in behaviour and (
					message.name == DbMsrMessages.DBMSR_NEW_MASTER_UP
				or 	message.name == DbMsrMessages.DBMSR_PROMOTE_TO_MASTER
				or 	message.name == DbMsrMessages.DBMSR_CREATE_DATA_BUNDLE
				or 	message.name == DbMsrMessages.DBMSR_CREATE_BACKUP
				or  message.name == Messages.UPDATE_SERVICE_CONFIGURATION
				or  message.name == Messages.BEFORE_HOST_TERMINATE
				or  message.name == MysqlMessages.CREATE_PMA_USER
				or	message.name == MysqlMessages.CONVERT_VOLUME)
		
	
	def get_initialization_phases(self, hir_message):
		if __mysql__['behavior'] in hir_message.body:
			steps = [self._step_accept_scalr_conf, 
					self._step_create_storage]
			if hir_message.body[__mysql__['behavior']]['replication_master'] == '1':
				steps.append(self._step_create_data_bundle)
			else:
				steps.append(self._step_change_replication_master)
			steps.append(self._step_collect_hostup_data)
		
			
			return {'before_host_up': [{
				'name': self._phase_mysql, 
				'steps': steps
			}]}
	
			
	def on_reload(self):
		LOG.debug("on_reload")
		self._queryenv = bus.queryenv_service
		self._platform = bus.platform
		#self._cnf = bus.cnf
		#ini = self._cnf.rawini
		#self._role_name = ini.get(config.SECT_GENERAL, config.OPT_ROLE_NAME)
		#self._volume_config_path  = self._cnf.private_path(os.path.join('storage', STORAGE_VOLUME_CNF))
		#self._snapshot_config_path = self._cnf.private_path(os.path.join('storage', STORAGE_SNAPSHOT_CNF))


	def on_init(self):	
		LOG.debug("on_init")	
		bus.on("host_init_response", self.on_host_init_response)
		bus.on("before_host_up", self.on_before_host_up)
		bus.on("before_reboot_start", self.on_before_reboot_start)
		bus.on("before_reboot_finish", self.on_before_reboot_finish)
				
		if __node__['state'] == 'bootstrapping':
			self._insert_iptables_rules()
		
		elif __node__['state'] == 'running':
			vol = storage2.volume(__mysql__['volume'])
			if not vol.tags:
				vol.tags = self.resource_tags()
			vol.ensure(mount=True)
			__mysql__['volume'] = vol
			if int(__mysql__['replication_master']):
				LOG.debug("Checking Scalr's %s system users presence", 
						__mysql__['behavior'])
				creds = self.get_user_creds()
				self.create_users(**creds)
				
			'''
			# Creating self.storage_vol object from configuration
			storage_conf = Storage.restore_config(self._volume_config_path)
			storage_conf['tags'] = self.mysql_tags
			self.storage_vol = Storage.create(storage_conf)
			if not self.storage_vol.mounted():
				self.storage_vol.mount()
			
			if self.is_replication_master:
				LOG.debug("Checking Scalr's %s system users presence." % __mysql__['behavior'])
				creds = self.get_user_creds()
				self.create_users(**creds)
			'''
	

	def on_host_init_response(self, message):
		"""
		Check mysql data in host init response
		@type message: scalarizr.messaging.Message
		@param message: HostInitResponse
		"""
		LOG.debug("on_host_init_response")
		
		with bus.initialization_op as op:
			with op.phase(self._phase_mysql):
				with op.step(self._step_accept_scalr_conf):
		
					if not message.body.has_key(__mysql__['behavior']):
						msg = "HostInitResponse message for MySQL behavior " \
								"must have '%s' property" % __mysql__['behavior']
						raise HandlerError(msg)
					

					# Apply MySQL data from HIR
					md = getattr(message, __mysql__['behavior']).copy()					

					md['compat_prior_backup_restore'] = False
					if md.get('volume'):
						# New format
						md['volume'] = storage2.volume(md['volume'])
						if 'backup' in md:
							md['backup'] = backup.backup(md['backup'])
						if 'restore' in md:
							md['restore'] = backup.restore(md['restore'])

					else:
						# Compatibility transformation
						# - volume_config -> volume
						# - master n'th start, type=ebs - del snapshot_config
						# - snapshot_config + log_file + log_pos -> restore
						# - create backup on master 1'st start
						md['compat_prior_backup_restore'] = True
						if md.get('volume_config'):
							md['volume'] = storage2.volume(
									md.pop('volume_config'))
						else:
							md['volume'] = storage2.volume(
									type=md['snapshot_config']['type'])

						if md['volume'].device and \
									md['volume'].type in ('ebs', 'raid'):
							md.pop('snapshot_config', None)

						if md.get('snapshot_config'):
							md['restore'] = backup.restore(
									type='snap_mysql', 
									snapshot=md.pop('snapshot_config'),
									volume=md['volume'],
									log_file=md.pop('log_file'),
									log_pos=md.pop('log_pos'))
						elif int(md['replication_master']) and \
									not md['volume'].device:
							md['backup'] = backup.backup(
									type='snap_mysql',
									volume=md['volume'])

					__mysql__.update(md)

					LOG.debug('__mysql__: %s', md)
					LOG.debug('volume in __mysql__: %s', 'volume' in __mysql__)
					LOG.debug('restore in __mysql__: %s', 'restore' in __mysql__)
					LOG.debug('backup in __mysql__: %s', 'backup' in __mysql__)
					
					__mysql__['volume'].mpoint = __mysql__['storage_dir']
					__mysql__['volume'].tags = self.resource_tags()
					if 'backup' in __mysql__:
						__mysql__['backup'].tags = self.resource_tags()

	
	def on_before_host_up(self, message):
		LOG.debug("on_before_host_up")
		"""
		Configure MySQL __mysql__['behavior']
		@type message: scalarizr.messaging.Message		
		@param message: HostUp message
		"""
		
		self.generate_datadir()
		self.mysql.service.stop('Configuring MySQL')
		repl = 'master' if int(__mysql__['replication_master']) else 'slave'
		bus.fire('before_mysql_configure', replication=repl)
		if repl == 'master':
			self._init_master(message)	
		else:
			self._init_slave(message)
		# Force to resave volume settings
		__mysql__['volume'] = storage2.volume(__mysql__['volume'])
		bus.fire('service_configured', service_name=__mysql__['behavior'], replication=repl)


	def on_BeforeHostTerminate(self, message):
		LOG.debug('Handling BeforeHostTerminate message from %s' % message.local_ip)
		#assert message.local_ip

		if message.local_ip == __node__['private_ip']:
			self.mysql.service.stop(reason='Server will be terminated')
			LOG.info('Detaching MySQL storage')
			vol = storage2.volume(__mysql__['volume'])
			vol.detach()
			if not int(__mysql__['replication_master']):
				LOG.info('Destroying volume %s', vol.id)
				vol.destroy(remove_disks=True)
				LOG.info('Volume %s has been destroyed.' % vol.id)	
	
	
	def on_Mysql_CreatePmaUser(self, message):
		LOG.debug("on_Mysql_CreatePmaUser")
		assert message.pma_server_ip
		assert message.farm_role_id
		
		try:
			# Operation allowed only on Master server
			if not int(__mysql__['replication_master']):
				msg = 'Cannot add pma user on slave. ' \
						'It should be a Master server'
				raise HandlerError(msg)
			
			pma_server_ip = message.pma_server_ip
			farm_role_id  = message.farm_role_id
			pma_password = cryptotool.pwgen(20)
			LOG.info("Adding phpMyAdmin system user")
			
			if  self.root_client.user_exists(__mysql__['pma_user'], pma_server_ip):
				LOG.info('PhpMyAdmin system user already exists. Removing user.')
				self.root_client.remove_user(__mysql__['pma_user'], pma_server_ip)

			self.root_client.create_user(__mysql__['pma_user'], pma_server_ip, 
										pma_password, privileges=None)
			LOG.info('PhpMyAdmin system user successfully added')
			
			# Notify Scalr
			self.send_message(MysqlMessages.CREATE_PMA_USER_RESULT, dict(
				status       = 'ok',
				pma_user	 = __mysql__['pma_user'],
				pma_password = pma_password,
				farm_role_id = farm_role_id,
			))
			
		except (Exception, BaseException), e:
			LOG.exception(e)
			
			# Notify Scalr about error
			self.send_message(MysqlMessages.CREATE_PMA_USER_RESULT, dict(
				status		= 'error',
				last_error	=  str(e).strip(),
				farm_role_id = farm_role_id
			))

	def on_DbMsr_CreateBackup(self, message):
		LOG.debug("on_DbMsr_CreateBackup")

		'''
		bak = backup.backup(
				type='mysqldump', 
				file_per_database=True,
				tmpdir=__mysql__['tmp_dir'],
				cloudfsdir=self._platform.scalrfs.backups(__mysql__['behavior']),
				chunk_size=__mysql__['mysqldump_chunk_size'])
		restore = None

		try:
			op = operation(name=self._op_backup, phases=[{
				'name': self._phase_backup, 
				'steps': [self._phase_backup]
			}])
			op.define()
			with op.phase(self._phase_backup):
				with op.step(self._phase_backup):
					restore = bak.run()
					
					#- type: mysqldump
					#- files:
					#  - size: 1234567
		            #  - path: s3://farm-2121-44/backups/mysql/20120314.tar.gz.part0
					#  - size: 3524567
		            #  - path: s3://farm-2121-44/backups/mysql/20120314.tar.gz.part1
					#result = list(dict(path=path, size=size) for path, size in zip(cloud_files, sizes))								
			op.ok(data=restore.files)
	
			# Notify Scalr
			self.send_message(DbMsrMessages.DBMSR_CREATE_BACKUP_RESULT, dict(
				db_type = __mysql__['behavior'],
				status = 'ok',
				backup_parts = restore.files
			))
		except:
			exc = sys.exc_info()[1]
			LOG.exception(exc)
			
			# Notify Scalr about error
			self.send_message(DbMsrMessages.DBMSR_CREATE_BACKUP_RESULT, dict(
				db_type = __mysql__['behavior'],
				status = 'error',
				last_error = str(exc)
			))

		'''


		tmp_basedir = __mysql__['tmp_dir']
		if not os.path.exists(tmp_basedir):
			os.makedirs(tmp_basedir)		
		# Retrieve password for scalr mysql user
		backup_path = None
		tmpdir = None
		try:
			# Get databases list
			databases = self.root_client.list_databases()
			
			op = operation(name=self._op_backup, phases=[{
				'name': self._phase_backup
			}])
			op.define()			

			with op.phase(self._phase_backup):
				# Dump all databases
				LOG.info("Dumping all databases")
				tmpdir = tempfile.mkdtemp(dir=tmp_basedir)

				backup_filename = 'mysql-backup-%s.tar.gz' % time.strftime('%Y-%m-%d-%H:%M:%S') 
				backup_path = os.path.join(tmpdir, backup_filename)
				
				# Creating archive 
				backup = tarfile.open(backup_path, 'w:gz')
				mysqldump = mysql_svc.MySQLDump(root_user=__mysql__['root_user'],
									root_password=__mysql__['root_password'])
				dump_options = __mysql__['mysqldump_options'].split(' ')

				def _single_backup(db_name):
					dump_path = os.path.join(tmpdir, db_name + '.sql') 
					mysqldump.create(db_name, dump_path, dump_options)
					backup.add(dump_path, os.path.basename(dump_path))

				make_backup_steps(databases, op, _single_backup)
						
				backup.close()
				
			with op.step(self._step_upload_to_cloud_storage):
				# Creating list of full paths to archive chunks
				if os.path.getsize(backup_path) > __mysql__['mysqldump_chunk_size']:
					parts = [os.path.join(tmpdir, file) for file in filetool.split(backup_path, backup_filename, __mysql__['mysqldump_chunk_size'] , tmpdir)]
				else:
					parts = [backup_path]
				sizes = [os.path.getsize(file) for file in parts]
						
				cloud_storage_path = self._platform.scalrfs.backups('mysql')
				LOG.info("Uploading backup to cloud storage (%s)", cloud_storage_path)
				trn = transfer.Transfer()
				cloud_files = trn.upload(parts, cloud_storage_path)
				LOG.info("Mysql backup uploaded to cloud storage under %s/%s", 
								cloud_storage_path, backup_filename)

			result = list(dict(path=path, size=size) for path, size in zip(cloud_files, sizes))								
			op.ok(data=result)
			
			# Notify Scalr
			self.send_message(DbMsrMessages.DBMSR_CREATE_BACKUP_RESULT, dict(
				db_type = __mysql__['behavior'],
				status = 'ok',
				backup_parts = result
			))
						
		except (Exception, BaseException), e:
			LOG.exception(e)
			
			# Notify Scalr about error
			self.send_message(DbMsrMessages.DBMSR_CREATE_BACKUP_RESULT, dict(
				db_type = __mysql__['behavior'],
				status = 'error',
				last_error = str(e)
			))
			
		finally:
			if tmpdir:
				shutil.rmtree(tmpdir, ignore_errors=True)
			if backup_path and os.path.exists(backup_path):
				os.remove(backup_path)	


	def on_DbMsr_CreateDataBundle(self, message):
		LOG.debug("on_DbMsr_CreateDataBundle")

		try:
			op = operation(name=self._op_data_bundle, phases=[{
				'name': self._phase_data_bundle, 
				'steps': [self._step_create_data_bundle]
			}])
			op.define()

			with op.phase(self._phase_data_bundle):
				with op.step(self._step_create_data_bundle):
		
					bus.fire('before_mysql_data_bundle')

					backup_info = message.body.get(__mysql__['behavior'], {})

					compat_prior_backup_restore = 'backup' not in backup_info
					if compat_prior_backup_restore:
						bak = backup.backup(
								type='snap_mysql',
								volume=__mysql__['volume'])
					else:
						bak = backup.backup(backup_info['backup'])
					restore = bak.run()
					
					'''
					# Creating snapshot
					snap, log_file, log_pos = self._create_snapshot(ROOT_USER, self.root_password, tags=self.mysql_tags)
					used_size = firstmatched(lambda r: r.mpoint == STORAGE_PATH, filetool.df()).used
					bus.fire('mysql_data_bundle', snapshot_id=snap.id)			
					'''
				
					# Notify scalr
					msg_data = {
						'db_type': __mysql__['behavior'],
						'status': 'ok',
						__mysql__['behavior']: {}
					}
					if compat_prior_backup_restore:
						msg_data[__mysql__['behavior']].update({
							'snapshot_config': dict(restore.snapshot),
							'log_file': restore.log_file,
							'log_pos': restore.log_pos,
						})
					else:
						msg_data[__mysql__['behavior']].update({						
							'restore': dict(restore)
						})
					self.send_message(DbMsrMessages.DBMSR_CREATE_DATA_BUNDLE_RESULT, msg_data)
			op.ok()
			
		except (Exception, BaseException), e:
			LOG.exception(e)
			
			# Notify Scalr about error
			self.send_message(DbMsrMessages.DBMSR_CREATE_DATA_BUNDLE_RESULT, dict(
				db_type = __mysql__['behavior'],
				status		='error',
				last_error	= str(e)
			))
	
	
	def on_DbMsr_PromoteToMaster(self, message):
		"""
		Promote slave to master
		"""
		LOG.debug("on_DbMsr_PromoteToMaster")
		#assert message.body['volume_config']
		#assert message.mysql2
		mysql2 = message.body[__mysql__['behavior']]
		#assert mysql2['root_password']
		#assert mysql2['repl_password']
		#assert mysql2['stat_password']

		
		if int(__mysql__['replication_master']):
			LOG.warning('Cannot promote to master. Already master')
			return
		LOG.info('Starting Slave -> Master promotion')
			
		bus.fire('before_slave_promote_to_master')

		__mysql__['compat_prior_backup_restore'] = mysql2.get('volume_config') or mysql2.get('snapshot_config')
		new_vol	= None
		if mysql2.get('volume_config'):
			new_vol = storage2.volume(mysql2.get('volume_config'))
					
		try:
			if new_vol and new_vol.type not in ('eph', 'lvm'):
				if self.mysql.service.running:
					self.root_client.stop_slave()

					self.mysql.service.stop('Swapping storages to promote slave to master')
				
				# Unplug slave storage and plug master one
				#old_conf = self.storage_vol.detach(force=True) # ??????
				old_vol = storage2.volume(__mysql__['volume'])				
				try:
					old_vol.umount()
					#master_vol = self._take_master_volume(master_vol_id)
					new_vol.mpoint = __mysql__['storage_dir']				
					new_vol.ensure(mount=True)				
					#new_storage_vol = self._plug_storage(STORAGE_PATH, master_storage_conf)				
					# Continue if master storage is a valid MySQL storage 
					if self._storage_valid():
						# Patch configuration files 
						self.mysql.move_mysqldir_to(__mysql__['storage_dir'])
						self.mysql._init_replication(master=True)
						# Set read_only option
						self.mysql.my_cnf.read_only = False
						self.mysql.service.start()
						# Update __mysql__['behavior'] configuration
						__mysql__.update({
							'replication_master': 1,
							'root_password': mysql2['root_password'],
							'repl_password': mysql2['repl_password'],
							'stat_password': mysql2['stat_password'],
							'volume': new_vol
						})

						try:
							old_vol.destroy(remove_disks=True)
						except:
							LOG.warn('Failed to destroy old MySQL volume %s: %s', 
										old_vol.id, sys.exc_info()[1])
						'''
						updates = {
							OPT_ROOT_PASSWORD : mysql2['root_password'],
							OPT_REPL_PASSWORD : mysql2['repl_password'],
							OPT_STAT_PASSWORD : mysql2['stat_password'],
							OPT_REPLICATION_MASTER 	: "1"
						}
						self._update_config(updates)
						Storage.backup_config(new_storage_vol.config(), self._volume_config_path) 
						'''

						# Send message to Scalr
						msg_data = {
							'status': 'ok',
							'db_type': __mysql__['behavior'],
							__mysql__['behavior']: {}
						} 
						if __mysql__['compat_prior_backup_restore']:
							msg_data[__mysql__['behavior']].update({
								'volume_config': dict(__mysql__['volume'])
							})
						else:
							msg_data[__mysql__['behavior']].update({
								'volume': dict(__mysql__['volume'])
							})
		
						self.send_message(
								DbMsrMessages.DBMSR_PROMOTE_TO_MASTER_RESULT, 
								msg_data)
					else:
						msg = "%s is not a valid MySQL storage" % __mysql__['data_dir']
						raise HandlerError(msg)
				except:
					self.mysql.service.stop('Detaching new volume')
					new_vol.detach()
					old_vol.mount()
					raise
			else:
				# Set read_only option
				self.mysql.my_cnf.read_only = False
				self.mysql.service.start()

				self.root_client.stop_slave()
				self.root_client.reset_master()
				self.mysql.flush_logs(__mysql__['data_dir'])
				
				__mysql__.update({
					'replication_master': 1,
					'root_password': mysql2['root_password'],
					'repl_password': mysql2['repl_password'],
					'stat_password': mysql2['stat_password'],
				})
				'''
				updates = {
					OPT_ROOT_PASSWORD : mysql2['root_password'],
					OPT_REPL_PASSWORD : mysql2['repl_password'],
					OPT_STAT_PASSWORD : mysql2['stat_password'],
					OPT_REPLICATION_MASTER 	: "1"
				}
				self._update_config(updates)
				'''

				if mysql2.get('backup'):
					bak = backup.backup(**mysql2.get('backup'))
				else:
					bak = backup.backup(
							type='snap_mysql', 
							volume=__mysql__['volume'])
				restore = bak.run()
				'''				
				snap, log_file, log_pos = self._create_snapshot(ROOT_USER, mysql2['root_password'], tags=self.mysql_tags)
				Storage.backup_config(snap.config(), self._snapshot_config_path)
				'''				

				# Send message to Scalr
				msg_data = dict(
					status="ok",
					db_type = __mysql__['behavior']
				)
				if __mysql__['compat_prior_backup_restore']:
					msg_data[__mysql__['behavior']] = {
						'log_file': restore.log_file,
						'log_pos': restore.log_pos,
						'snapshot_config': dict(restore.snapshot),
						'volume_config': dict(__mysql__['volume'])
					}
				else:
					msg_data[__mysql__['behavior']] = {
						'restore': dict(restore),
						'volume': dict(__mysql__['volume'])
					}
				
				self.send_message(DbMsrMessages.DBMSR_PROMOTE_TO_MASTER_RESULT, msg_data)
				LOG.info('Promotion completed')							
				
			bus.fire('slave_promote_to_master')
			
		except (Exception, BaseException), e:
			LOG.exception(e)
			
			msg_data = dict(
				db_type = __mysql__['behavior'],
				status="error",
				last_error=str(e))
			self.send_message(DbMsrMessages.DBMSR_PROMOTE_TO_MASTER_RESULT, msg_data)
			
			# Change back read_only option 
			self.mysql.my_cnf.read_only = True
			
			# Start MySQL
			self.mysql.service.start()

		

		
	def on_DbMsr_NewMasterUp(self, message):

		assert message.body.has_key("db_type")
		assert message.body.has_key("local_ip")
		assert message.body.has_key("remote_ip")
		assert message.body.has_key(__mysql__['behavior'])
	
		mysql2 = message.body[__mysql__['behavior']]
		
		if int(__mysql__['replication_master']):
			LOG.debug('Skip NewMasterUp. My replication role is master')
			return
		
		host = message.local_ip or message.remote_ip
		LOG.info("Switching replication to a new MySQL master %s", host)
		bus.fire('before_mysql_change_master', host=host)			

		LOG.debug("__mysql__['volume']: %s", __mysql__['volume'])
		
		if __mysql__['volume'].type in ('eph', 'lvm'):
			if 'restore' in mysql2:
				restore = backup.restore(**mysql2['restore'])
			else:
				# snap_mysql restore should update MySQL volume, and delete old one
				restore = backup.restore(
							type='snap_mysql',
							log_file=mysql2['log_file'],
							log_pos=mysql2['log_pos'],
							volume=__mysql__['volume'],
							snapshot=mysql2['snapshot_config'])
			# XXX: ugly
			if __mysql__['volume'].type == 'eph':
				self.mysql.service.stop('Swapping storages to reinitialize slave')
				'''
				LOG.info('Reinitializing Slave from the new snapshot %s (log_file: %s log_pos: %s)', 
						restore.snapshot['id'], restore.log_file, restore.log_pos)
				new_vol = restore.run()
				self.mysql.service.stop('Swapping storages to reinitialize slave')
			
				LOG.debug('Destroing old storage')
				vol = storage.volume(**__mysql__['volume'])
				vol.destroy(remove_disks=True)
				LOG.debug('Storage destoyed')

				'''
			log_file = restore.log_file
			log_pos = restore.log_pos
			restore.run()
			
			'''
			LOG.debug('Plugging new storage')
			vol = Storage.create(snapshot=snap_config.copy(), tags=self.mysql_tags)
			self._plug_storage(STORAGE_PATH, vol)
			LOG.debug('Storage plugged')

			Storage.backup_config(vol.config(), self._volume_config_path)
			Storage.backup_config(snap_config, self._snapshot_config_path)
			self.storage_vol = vol
			'''
			
			self.mysql.service.start()
		else:
			LOG.debug("Stopping slave i/o thread")
			self.root_client.stop_slave_io_thread()
			LOG.debug("Slave i/o thread stopped")
			
			LOG.debug("Retrieving current log_file and log_pos")
			status = self.root_client.slave_status()
			log_file = status['Master_Log_File']
			log_pos = status['Read_Master_Log_Pos']
			LOG.debug("Retrieved log_file=%s, log_pos=%s", log_file, log_pos)


		self._change_master(
			host=host, 
			user=__mysql__['repl_user'], 
			password=mysql2['repl_password'],
			log_file=log_file, 
			log_pos=log_pos
		)
			
		LOG.debug("Replication switched")
		bus.fire('mysql_change_master', host=host, log_file=log_file, log_pos=log_pos)


	def on_ConvertVolume(self, message):
		try:
			if __node__['state'] != 'running':
				raise HandlerError('scalarizr is not in "running" state')

			old_volume = storage2.volume(__mysql__['volume'])
			new_volume = storage2.volume(message.volume)

			if old_volume.type != 'eph' or new_volume.type != 'lvm':
				raise HandlerError('%s to %s convertation unsupported.' %
								   (old_volume.type, new_volume.type))

			new_volume.ensure()
			__mysql__.update({'volume': new_volume})
		except:
			e = sys.exc_info()[1]
			LOG.error('Volume convertation failed: %s' % e)
			self.send_message(MysqlMessages.CONVERT_VOLUME_RESULT,
					dict(status='error', last_error=str(e)))


		
	def on_before_reboot_start(self, *args, **kwargs):
		self.mysql.service.stop('Instance is going to reboot')

	
	def on_before_reboot_finish(self, *args, **kwargs):
		self._insert_iptables_rules()


	def generate_datadir(self):
		try:
			datadir = mysql2_svc.my_print_defaults('mysqld').get('datadir')
			if datadir and \
				os.path.isdir(datadir) and \
				not os.path.isdir(os.path.join(datadir, 'mysql')):
				self.mysql.service.start()
				self.mysql.service.stop('Autogenerating datadir')				
		except:
			#TODO: better error handling
			pass		
	

	def _storage_valid(self):
		binlog_base = os.path.join(__mysql__['storage_dir'], mysql_svc.STORAGE_BINLOG)
		return os.path.exists(__mysql__['data_dir']) and glob.glob(binlog_base + '*')


	def _change_selinux_ctx(self):
		chcon = software.whereis('chcon')
		if disttool.is_rhel() and chcon:
			LOG.debug('Changing SELinux file security context for new mysql datadir')
			system2((chcon[0], '-R', '-u', 'system_u', '-r',
					'object_r', '-t', 'mysqld_db_t', 
					os.path.dirname(__mysql__['storage_dir'])), raise_exc=False)
	
		
	def _init_master(self, message):
		"""
		Initialize MySQL master
		@type message: scalarizr.messaging.Message 
		@param message: HostUp message
		"""
		LOG.info("Initializing MySQL master")
		
		with bus.initialization_op as op:
			with op.step(self._step_create_storage):
				if 'restore' in __mysql__ and \
						__mysql__['restore'].type == 'snap_mysql':
					__mysql__['restore'].run()
				else:
					__mysql__['volume'].ensure(mount=True, mkfs=True)
					LOG.debug('MySQL volume config after ensure: %s', dict(__mysql__['volume']))
					
				self.mysql.flush_logs(__mysql__['data_dir'])
		
			with op.step(self._step_move_datadir):
				storage_valid = self._storage_valid()				
				user_creds = self.get_user_creds()
		
				datadir = mysql2_svc.my_print_defaults('mysqld').get('datadir', '/var/lib/mysql')
				self.mysql.my_cnf.datadir = datadir

		
				if not storage_valid and datadir.find(__mysql__['data_dir']) == 0:
					# When role was created from another mysql role it contains modified my.cnf settings 
					self.mysql.my_cnf.datadir = '/var/lib/mysql'
					self.mysql.my_cnf.log_bin = None
				
				# Patch configuration
				self.mysql.my_cnf.expire_logs_days = 10
				self.mysql.my_cnf.skip_locking = False				
				self.mysql.move_mysqldir_to(__mysql__['storage_dir'])
				self._change_selinux_ctx()

		
			with op.step(self._step_patch_conf):
				# Init replication
				self.mysql._init_replication(master=True)
				
			if 'restore' in __mysql__ and \
					__mysql__['restore'].type == 'xtrabackup':
				__mysql__['restore'].run()
				if __mysql__['restore'].features['master_binlog_reset']:
					self.mysql.service.start()
					self.mysql.service.stop()
					log_file, log_pos = mysql2_svc.mysqlbinlog_head()
					__mysql__['restore'].log_file = log_file
					__mysql__['restore'].log_pos = log_pos
			
		
		# If It's 1st init of mysql master storage
		if not storage_valid:
			with op.step(self._step_create_users):			
				if os.path.exists(__mysql__['debian.cnf']):
					LOG.debug("Copying debian.cnf file to mysql storage")
					shutil.copy(__mysql__['debian.cnf'], __mysql__['storage_dir'])	
						
				# Add system users	
				self.create_users(**user_creds)
			
		# If volume has mysql storage directory structure (N-th init)
		else:
			with op.step(self._step_innodb_recovery):
				self._copy_debian_cnf_back()
				if 'restore' in __mysql__ \
					and __mysql__['restore'].type != 'xtrabackup':
					self._innodb_recovery()	
					self.mysql.service.start()
					
		with op.step(self._step_create_data_bundle):
			if 'backup' in __mysql__:				
				__mysql__['restore'] = __mysql__['backup'].run()

		with op.step(self._step_collect_hostup_data):
			# Update HostUp message
			md = dict(
				replication_master=__mysql__['replication_master'],
				root_password=__mysql__['root_password'],
				repl_password=__mysql__['repl_password'],
				stat_password=__mysql__['stat_password'],
			)
			if __mysql__['compat_prior_backup_restore']:
				if 'restore' in __mysql__:
					md.update(dict(					
							log_file=__mysql__['restore'].log_file,
							log_pos=__mysql__['restore'].log_pos,
							snapshot_config=dict(__mysql__['restore'].snapshot)))
				elif 'log_file' in __mysql__:
					md.update(dict(
							log_file=__mysql__['log_file'],
							log_pos=__mysql__['log_pos']))
				md.update(dict(
							volume_config=dict(__mysql__['volume'])))
			else:
				md.update(dict(
					volume=dict(__mysql__['volume'])
				))
				for key in ('backup', 'restore'):
					if key in __mysql__:
						md[key] = dict(__mysql__[key])						
					
				
			message.db_type = __mysql__['behavior']
			setattr(message, __mysql__['behavior'], md)

			
			

	def _init_slave(self, message):
		"""
		Initialize MySQL slave
		@type message: scalarizr.messaging.Message 
		@param message: HostUp message
		"""
		LOG.info("Initializing MySQL slave")
		
		with bus.initialization_op as op:
			with op.step(self._step_create_storage):
				if 'restore' in __mysql__ and \
						__mysql__['restore'].type == 'snap_mysql':
					__mysql__['restore'].run()
				else:
					__mysql__['volume'].ensure(mount=True, mkfs=True)
				'''
				if not self._storage_valid():
					LOG.debug("Initialize slave storage")
					__mysql__['restore'].run()
				else:
					__mysql__['volume'].ensure(mount=True, mkfs=True)
				'''
		
			with op.step(self._step_patch_conf):		
				self.mysql.service.stop('Required by Slave initialization process')			
				self.mysql.flush_logs(__mysql__['data_dir'])
				
				# Change configuration files
				LOG.info("Changing configuration files")
				self.mysql.my_cnf.datadir = __mysql__['data_dir']
				self.mysql.my_cnf.skip_locking = False
				self.mysql.my_cnf.expire_logs_days = 10
				self.mysql.my_cnf.read_only = True
	
			with op.step(self._step_move_datadir):
				self.mysql.move_mysqldir_to(__mysql__['storage_dir'])
				self._change_selinux_ctx()
				self.mysql._init_replication(master=False)
				self._copy_debian_cnf_back()
				
			if 'restore' in __mysql__ and \
					__mysql__['restore'].type == 'xtrabackup':
				__mysql__['restore'].run()
				
			with op.step(self._step_innodb_recovery):
				if 'restore' in __mysql__ \
						and __mysql__['restore'].type != 'xtrabackup':
					self._innodb_recovery()
					self.mysql.service.start()
			
			with op.step(self._step_change_replication_master):
				# Change replication master 
				LOG.info("Requesting master server")
				master_host = self.get_master_host()
	
				self._change_master( 
						host=master_host, 
						user=__mysql__['repl_user'], 
						password=__mysql__['repl_password'],
						log_file=__mysql__['restore'].log_file, 
						log_pos=__mysql__['restore'].log_pos)
			
			with op.step(self._step_collect_hostup_data):
				# Update HostUp message
				#message.mysql = self._compat_storage_data(self.storage_vol)
				message.db_type = __mysql__['behavior']

		
	def get_master_host(self):
		master_host = None
		while not master_host:
			try:
				master_host = list(host 
					for host in self._queryenv.list_roles(behaviour=__mysql__['behavior'])[0].hosts 
					if host.replication_master)[0]
			except IndexError:
				LOG.debug("QueryEnv respond with no mysql master. " + 
						"Waiting %d seconds before the next attempt", 5)
			time.sleep(5)
		LOG.debug("Master server obtained (local_ip: %s, public_ip: %s)",
				master_host.internal_ip, master_host.external_ip)
		return master_host.internal_ip or master_host.external_ip

			
	def _copy_debian_cnf_back(self):
		debian_cnf = os.path.join(__mysql__['storage_dir'], 'debian.cnf')
		if disttool.is_debian_based() and os.path.exists(debian_cnf):
			LOG.debug("Copying debian.cnf from storage to mysql configuration directory")
			shutil.copy(debian_cnf, '/etc/mysql/')
			
			
	@property
	def root_client(self):
		return mysql_svc.MySQLClient(
					__mysql__['root_user'], 
					__mysql__['root_password'])
	
	'''
	def _compat_storage_data(self, vol=None, snap=None):
		ret = dict()
		if bus.scalr_version >= (2, 2):
			if vol:
				ret['volume_config'] = vol.config() if not isinstance(vol, dict) else vol
			if snap:
				ret['snapshot_config'] = snap.config() if not isinstance(snap, dict) else snap
		else:
			if vol:
				ret['volume_id'] = vol.config()['id'] if not isinstance(vol, dict) else vol['id']
			if snap:
				ret['snapshot_id'] = snap.config()['id'] if not isinstance(snap, dict) else snap['id']
		return ret
	'''		

	def _innodb_recovery(self, storage_path=None):
		storage_path = storage_path or __mysql__['storage_dir']
		binlog_path	= os.path.join(storage_path, mysql_svc.STORAGE_BINLOG)		
		data_dir = os.path.join(storage_path, mysql_svc.STORAGE_DATA_DIR),
		pid_file = os.path.join(storage_path, 'mysql.pid')
		socket_file = os.path.join(storage_path, 'mysql.sock')
		mysqld_safe_bin	= software.whereis('mysqld_safe')[0]
		
		LOG.info('Performing InnoDB recovery')
		mysqld_safe_cmd = (mysqld_safe_bin, 
			'--socket=%s' % socket_file, 
			'--pid-file=%s' % pid_file, 
			'--datadir=%s' % data_dir,
			'--log-bin=%s' % binlog_path, 
			'--skip-networking', 
			'--skip-grant', 
			'--bootstrap', 
			'--skip-slave-start')
		system2(mysqld_safe_cmd, stdin="select 1;")	

		
	def _insert_iptables_rules(self):
		if iptables.enabled():
			iptables.FIREWALL.ensure([
				{"jump": "ACCEPT", "protocol": "tcp", "match": "tcp", "dport": "3306"},
			])
		
		'''
		ipt = iptables.IpTables()
		if ipt.usable():
			ipt.insert_rule(None, iptables.RuleSpec(dport=mysql_svc.MYSQL_DEFAULT_PORT, 
												jump='ACCEPT', protocol=iptables.P_TCP))	
		'''

	
	def get_user_creds(self):
		options = {
			__mysql__['root_user']: 'root_password', 
			__mysql__['repl_user']: 'repl_password', 
			__mysql__['stat_user']: 'stat_password'}
		creds = {}
		for login, opt_pwd in options.items():
			password = __mysql__[opt_pwd]
			if not password:
				password = cryptotool.pwgen(20)
				__mysql__[opt_pwd] = password
			creds[login] = password
		return creds
	
	
	def create_users(self, **creds):
		users = {}
		root_cli = mysql_svc.MySQLClient(__mysql__['root_user'], creds[__mysql__['root_user']])
		local_root = mysql_svc.MySQLUser(root_cli, __mysql__['root_user'], creds[__mysql__['root_user']], host='localhost')

		if not self.mysql.service.running:
			self.mysql.service.start()
			
			try:
				if not local_root.exists() or not local_root.check_password():
					users.update({'local_root': local_root})
					self.mysql.service.stop('creating users')
					self.mysql.service.start_skip_grant_tables()
				else:
					LOG.debug('User %s exists and has correct password' % __mysql__['root_user'])
			except ServiceError, e:
				if 'Access denied for user' in str(e):
					users.update({'local_root': local_root})
					self.mysql.service.stop('creating users')
					self.mysql.service.start_skip_grant_tables()
				
		for login, password in creds.items():
			user = mysql_svc.MySQLUser(root_cli, login, password, host='%', privileges=PRIVILEGES.get(login, None))
			users[login] = user
			
		for login, user in users.items():
			if not user.exists():
				LOG.debug('User %s not found. Recreating.' % login)
				user.create()
			elif not user.check_password():
				LOG.warning('Password for user %s was changed. Recreating.' %  login)
				user.remove()
				user.create()
			users[login] = user
			
		self.mysql.service.stop_skip_grant_tables()	
		self.mysql.service.start()
		return users
		

	'''
	def _update_config(self, data): 
		#XXX: I just don't like it
		#ditching empty data
		updates = dict((k, v or '') for k, v in data.items())
		self._cnf.update_ini(CNF_SECTION, {CNF_SECTION : updates})
		


	def _plug_storage(self, mpoint, vol):
		vol.tags = self.mysql_tags
		vol.mpoint = mpoint
		vol.ensure(mount=True, mkfs=True)
		return vol
	

	def _create_snapshot(self, root_user, root_password, tags=None):
		snap, log_file, log_pos = self.data_bundle.create(tags)
		wait_until(lambda: snap.status() != snap.QUEUED)
		if snap.state == snap.FAILED:
			raise HandlerError('MySQL storage snapshot creation failed. See log for more details')
		
		LOG.info('MySQL data bundle created\n  snapshot: %s\n  log_file: %s\n  log_pos: %s', 
						snap.id, log_file, log_pos)
		return snap, log_file, log_pos
	
	
	def _get_ini_options(self, *args):
		ret = []
		for opt in args:
			try:
				ret.append(self._cnf.rawini.get(CNF_SECTION, opt))
			except ConfigParser.Error:
				err = 'Required configuration option is missed in mysql.ini: %s' % opt
				raise HandlerError(err)
		return tuple(ret)
	'''

	def _data_bundle_description(self):
		pl = bus.platform
		return 'MySQL data bundle (farm: %s role: %s)' % (
					pl.get_user_data(UserDataOptions.FARM_ID), 
					pl.get_user_data(UserDataOptions.ROLE_NAME))


	def _datadir_size(self):
		stat = os.statvfs(__mysql__['storage_dir'])
		return stat.f_bsize * stat.f_blocks / 1024 / 1024 / 1024 + 1
		

	def _change_master(self, host, user, password, log_file, log_pos, timeout=None):
		
		LOG.info("Changing replication Master to server %s (log_file: %s, log_pos: %s)", host, log_file, log_pos)
		
		timeout = timeout or int(__mysql__['change_master_timeout'])
		
		# Changing replication master
		self.root_client.stop_slave()
		self.root_client.change_master_to(host, user, password, log_file, log_pos)
		
		# Starting slave
		result = self.root_client.start_slave()
		LOG.debug('Start slave returned: %s' % result)
		if result and 'ERROR' in result:
			raise HandlerError('Cannot start mysql slave: %s' % result)

		time_until = time.time() + timeout
		status = None
		while time.time() <= time_until:
			status = self.root_client.slave_status()
			if status['Slave_IO_Running'] == 'Yes' and \
				status['Slave_SQL_Running'] == 'Yes':
				break
			time.sleep(5)
		else:
			if status:
				if not status['Last_Error']:
					logfile = firstmatched(lambda p: os.path.exists(p), 
										('/var/log/mysqld.log', '/var/log/mysql.log'))
					if logfile:
						gotcha = '[ERROR] Slave I/O thread: '
						size = os.path.getsize(logfile)
						fp = open(logfile, 'r')
						try:
							fp.seek(max((0, size - 8192)))
							lines = fp.read().split('\n')
							for line in lines:
								if gotcha in line:
									status['Last_Error'] = line.split(gotcha)[-1]
						finally:
							fp.close()
				
				msg = "Cannot change replication Master server to '%s'. "  \
						"Slave_IO_Running: %s, Slave_SQL_Running: %s, " \
						"Last_Errno: %s, Last_Error: '%s'" % (
						host, status['Slave_IO_Running'], status['Slave_SQL_Running'],
						status['Last_Errno'], status['Last_Error'])
				raise HandlerError(msg)
			else:
				raise HandlerError('Cannot change replication master to %s' % (host))

				
		LOG.debug('Replication master is changed to host %s', host)		


	def resource_tags(self):
		return prepare_tags(__mysql__['behavior'], 
				db_replication_role=__mysql__['replication_master'])
