'''
Created on April 18th, 2011

@author: Dmytro Korsakov
'''
from __future__ import with_statement

import os
import glob
import time
import shutil
import tarfile
import logging
import tempfile

from scalarizr import config
from scalarizr.bus import bus
from scalarizr.messaging import Messages
from scalarizr.config import BuiltinBehaviours, ScalarizrState
from scalarizr.handlers import ServiceCtlHandler, HandlerError, DbMsrMessages, Handler
from scalarizr.util.filetool import split, rchown
from scalarizr.util import system2, wait_until, disttool, software, filetool, cryptotool
from scalarizr.storage import Storage, Snapshot, StorageError, Volume, transfer
from scalarizr.services.postgresql import PostgreSql, PSQL, ROOT_USER, PG_DUMP, PgUser, SU_EXEC
from scalarizr.linux import iptables
from scalarizr.handlers import operation, prepare_tags
from scalarizr.services import make_backup_steps


BEHAVIOUR = SERVICE_NAME = CNF_SECTION = BuiltinBehaviours.POSTGRESQL

PG_SOCKET_DIR 				= '/var/run/postgresql/'
STORAGE_PATH 				= "/mnt/pgstorage"
STORAGE_VOLUME_CNF 			= 'postgresql.json'
STORAGE_SNAPSHOT_CNF 		= 'postgresql-snap.json'

OPT_PG_VERSION				= 'pg_version'
OPT_VOLUME_CNF				= 'volume_config'
OPT_SNAPSHOT_CNF			= 'snapshot_config'
OPT_ROOT_USER				= 'root_user'
OPT_ROOT_PASSWORD 			= "root_password"
OPT_ROOT_SSH_PUBLIC_KEY 	= "root_ssh_public_key"
OPT_ROOT_SSH_PRIVATE_KEY	= "root_ssh_private_key"
OPT_CURRENT_XLOG_LOCATION	= 'current_xlog_location'
OPT_REPLICATION_MASTER 		= "replication_master"

BACKUP_CHUNK_SIZE 		= 200*1024*1024

POSTGRESQL_DEFAULT_PORT	= 5432

		
def get_handlers():
	return (PostgreSqlHander(), )


SSH_KEYGEN_SELINUX_MODULE = """
module local 1.0;

require {
	type initrc_tmp_t;
	type ssh_keygen_t;
	type initrc_t;
	type etc_runtime_t;
	class tcp_socket { read write };
	class file { read write getattr };
}

#============= ssh_keygen_t ==============
allow ssh_keygen_t etc_runtime_t:file { read write getattr };
allow ssh_keygen_t initrc_t:tcp_socket { read write };
allow ssh_keygen_t initrc_tmp_t:file { read write };
"""



class PostgreSqlHander(ServiceCtlHandler):	
	_logger = None
		
	_queryenv = None
	""" @type _queryenv: scalarizr.queryenv.QueryEnvService	"""
	
	_platform = None
	""" @type _platform: scalarizr.platform.Ec2Platform """
	
	_cnf = None
	''' @type _cnf: scalarizr.config.ScalarizrCnf '''
	
	storage_vol = None	
		
		
	def accept(self, message, queue, behaviour=None, platform=None, os=None, dist=None):
		return BEHAVIOUR in behaviour and (
					message.name == DbMsrMessages.DBMSR_NEW_MASTER_UP
				or 	message.name == DbMsrMessages.DBMSR_PROMOTE_TO_MASTER
				or 	message.name == DbMsrMessages.DBMSR_CREATE_DATA_BUNDLE
				or 	message.name == DbMsrMessages.DBMSR_CREATE_BACKUP
				or  message.name == Messages.UPDATE_SERVICE_CONFIGURATION
				or  message.name == Messages.HOST_INIT
				or  message.name == Messages.BEFORE_HOST_TERMINATE
				or  message.name == Messages.HOST_UP
				or  message.name == Messages.HOST_DOWN)	

	
	def get_initialization_phases(self, hir_message):
		if BEHAVIOUR in hir_message.body:
			steps = [self._step_accept_scalr_conf, self._step_create_storage]
			if hir_message.body[BEHAVIOUR]['replication_master'] == '1':
				steps += [self._step_init_master, self._step_create_data_bundle]
			else:
				steps += [self._step_init_slave]
			steps += [self._step_collect_host_up_data]
			
			return {'before_host_up': [{
				'name': self._phase_postgresql, 
				'steps': steps
			}]}
	
	
	def __init__(self):
		self._logger = logging.getLogger(__name__)
		self._service_name = SERVICE_NAME
		Handler.__init__(self)
		bus.on("init", self.on_init)
		bus.define_events(
			'before_postgresql_data_bundle',
			
			'postgresql_data_bundle',
			
			# @param host: New master hostname 
			'before_postgresql_change_master',
			
			# @param host: New master hostname 
			'postgresql_change_master',
			
			'before_slave_promote_to_master',
			
			'slave_promote_to_master'
		)	
		
		self._phase_postgresql = 'Configure PostgreSQL'
		self._phase_data_bundle = self._op_data_bundle = 'PostgreSQL data bundle'
		self._phase_backup = self._op_backup = 'PostgreSQL backup'
		self._step_upload_to_cloud_storage = 'Upload data to cloud storage'
		self._step_accept_scalr_conf = 'Accept Scalr configuration'
		self._step_patch_conf = 'Patch configuration files'
		self._step_create_storage = 'Create storage'
		self._step_init_master = 'Initialize Master'
		self._step_init_slave = 'Initialize Slave'
		self._step_create_data_bundle = 'Create data bundle'
		self._step_change_replication_master = 'Change replication Master'
		self._step_collect_host_up_data = 'Collect HostUp data'
		
		self.on_reload()		


	def on_init(self):		
		#temporary fix for starting-after-rebundle issue
		if not os.path.exists(PG_SOCKET_DIR):
			os.makedirs(PG_SOCKET_DIR)
			rchown(user='postgres', path=PG_SOCKET_DIR)
			
		bus.on("host_init_response", self.on_host_init_response)
		bus.on("before_host_up", self.on_before_host_up)
		bus.on("before_reboot_start", self.on_before_reboot_start)
		bus.on("before_reboot_finish", self.on_before_reboot_finish)
		
		if self._cnf.state == ScalarizrState.BOOTSTRAPPING:
			self._insert_iptables_rules()
			
			if disttool.is_redhat_based():		
					
				checkmodule_paths = software.whereis('checkmodule')
				semodule_package_paths = software.whereis('semodule_package')
				semodule_paths = software.whereis('semodule')
			
				if all((checkmodule_paths, semodule_package_paths, semodule_paths)):
					
					filetool.write_file('/tmp/sshkeygen.te',
								SSH_KEYGEN_SELINUX_MODULE, logger=self._logger)
					
					self._logger.debug('Compiling SELinux policy for ssh-keygen')
					system2((checkmodule_paths[0], '-M', '-m', '-o',
							 '/tmp/sshkeygen.mod', '/tmp/sshkeygen.te'), logger=self._logger)
					
					self._logger.debug('Building SELinux package for ssh-keygen')
					system2((semodule_package_paths[0], '-o', '/tmp/sshkeygen.pp',
							 '-m', '/tmp/sshkeygen.mod'), logger=self._logger)
					
					self._logger.debug('Loading ssh-keygen SELinux package')					
					system2((semodule_paths[0], '-i', '/tmp/sshkeygen.pp'), logger=self._logger)
				
		
		if self._cnf.state == ScalarizrState.RUNNING:

			storage_conf = Storage.restore_config(self._volume_config_path)
			storage_conf['tags'] = self.postgres_tags
			self.storage_vol = Storage.create(storage_conf)
			if not self.storage_vol.mounted():
				self.storage_vol.mount()
			
			self.postgresql.service.start()
			self.accept_all_clients()
			
			self._logger.debug("Checking presence of Scalr's PostgreSQL root user.")
			root_password = self.root_password
			
			if not self.postgresql.root_user.exists():
				self._logger.debug("Scalr's PostgreSQL root user does not exist. Recreating")
				self.postgresql.root_user = self.postgresql.create_user(ROOT_USER, root_password)
			else:
				try:
					self.postgresql.root_user.check_system_password(root_password)
					self._logger.debug("Scalr's root PgSQL user is present. Password is correct.")				
				except ValueError:
					self._logger.warning("Scalr's root PgSQL user was changed. Recreating.")
					self.postgresql.root_user.change_system_password(root_password)
					
			if self.is_replication_master:	
				#ALTER ROLE cannot be executed in a read-only transaction
				self._logger.debug("Checking password for pg_role scalr.")		
				if not self.postgresql.root_user.check_role_password(root_password):
					self._logger.warning("Scalr's root PgSQL role was changed. Recreating.")
					self.postgresql.root_user.change_role_password(root_password)
			

	def on_reload(self):
		self._queryenv = bus.queryenv_service
		self._platform = bus.platform
		self._cnf = bus.cnf
		ini = self._cnf.rawini
		self._role_name = ini.get(config.SECT_GENERAL, config.OPT_ROLE_NAME)
		self._storage_path = STORAGE_PATH
		self._tmp_path = os.path.join(self._storage_path, 'tmp')
		
		self._volume_config_path  = self._cnf.private_path(os.path.join('storage', STORAGE_VOLUME_CNF))
		self._snapshot_config_path = self._cnf.private_path(os.path.join('storage', STORAGE_SNAPSHOT_CNF))
		
		self.pg_keys_dir = self._cnf.private_path('keys')
		self.postgresql = PostgreSql(self.version, self.pg_keys_dir)
		
		
	@property
	def version(self):
		ver = None
		if self._cnf.rawini.has_option(CNF_SECTION, OPT_PG_VERSION):
			ver = self._cnf.rawini.get(CNF_SECTION, OPT_PG_VERSION)
			
		if not ver:
			try:
				path_list = glob.glob('/var/lib/p*sql/9.*')
				path_list.sort()
				path = path_list[-1]
				ver = os.path.basename(path)
			except IndexError:
				self._logger.warning('Postgresql default directory not found. Assuming that PostgreSQL 9.0 is installed.')
				ver = '9.0'
			finally:
				self._update_config({OPT_PG_VERSION : ver})
		return ver
		
	
	def on_HostInit(self, message):
		if message.local_ip != self._platform.get_private_ip() and message.local_ip in self.pg_hosts:
			self._logger.debug('Got new slave IP: %s. Registering in pg_hba.conf' % message.local_ip)
			self.postgresql.register_slave(message.local_ip)
			

	def on_HostUp(self, message):
		if message.local_ip == self._platform.get_private_ip():
			self.accept_all_clients()
		elif message.local_ip in self.farm_hosts:
			self.postgresql.register_client(message.local_ip)
		
	
	
	def on_HostDown(self, message):
		if  message.local_ip != self._platform.get_private_ip():
			self.postgresql.unregister_client(message.local_ip)
			if self.is_replication_master and self.farmrole_id == message.farm_role_id:
				self.postgresql.unregister_slave(message.local_ip)
	
	@property			
	def farm_hosts(self):
		list_roles = self._queryenv.list_roles()
		servers = []
		for serv in list_roles:
			for host in serv.hosts :
				servers.append(host.internal_ip or host.external_ip)
		self._logger.debug("QueryEnv returned list of servers within farm: %s" % servers)
		return servers				
		
		
	@property
	def pg_hosts(self):
		'''
		All pg instances including those in Initializing state
		'''
		list_roles = self._queryenv.list_roles(behaviour=BEHAVIOUR, with_init=True)
		servers = []
		for pg_serv in list_roles:
			for pg_host in pg_serv.hosts:
				servers.append(pg_host.internal_ip or pg_host.external_ip)
		self._logger.debug("QueryEnv returned list of %s servers: %s" % (BEHAVIOUR, servers))
		return servers
	
	
	def accept_all_clients(self):
		farm_hosts = self.farm_hosts
		for ip in farm_hosts:
				self.postgresql.register_client(ip, force=False)
		if farm_hosts:
			self.postgresql.service.reload('Granting access to all servers within farm.', force=True)
				
	
	@property
	def root_password(self):
		password = None 
		
		opt_pwd = '%s_password' % ROOT_USER
		if self._cnf.rawini.has_option(CNF_SECTION, opt_pwd):
			password = self._cnf.rawini.get(CNF_SECTION, opt_pwd)
		return password


	@property	
	def farmrole_id(self):
		id = None
		if self._cnf.rawini.has_option(config.SECT_GENERAL, config.OPT_FARMROLE_ID):
			id = self._cnf.rawini.get(config.SECT_GENERAL, config.OPT_FARMROLE_ID)
		return id
	
			
	def store_password(self, name, password):
		opt_user_password = '%s_password' % name
		self._cnf.update_ini(BEHAVIOUR, {CNF_SECTION: {opt_user_password:password}})
			
			
	@property
	def is_replication_master(self):
		value = self._cnf.rawini.get(CNF_SECTION, OPT_REPLICATION_MASTER)
		self._logger.debug('Got %s : %s' % (OPT_REPLICATION_MASTER, value))
		return True if int(value) else False
				
				
	@property
	def postgres_tags(self):
		return prepare_tags(BEHAVIOUR, db_replication_role=self.is_replication_master)
		
				
	def on_host_init_response(self, message):
		"""
		Check postgresql data in host init response
		@type message: scalarizr.messaging.Message
		@param message: HostInitResponse
		"""
		
		with bus.initialization_op as op:
			with op.phase(self._phase_postgresql):
				with op.step(self._step_accept_scalr_conf):
		
					if not message.body.has_key(BEHAVIOUR) or message.db_type != BEHAVIOUR:
						raise HandlerError("HostInitResponse message for PostgreSQL behaviour must have 'postgresql' property and db_type 'postgresql'")
					
					'''
					if message.postgresql[OPT_REPLICATION_MASTER] != '1'  and \
							(not message.body.has_key(OPT_ROOT_SSH_PUBLIC_KEY) or not 
							message.body.has_key(OPT_ROOT_SSH_PRIVATE_KEY)):
						raise HandlerError("HostInitResponse message for PostgreSQL slave must contain both public and private ssh keys")
					'''
					
					dir = os.path.dirname(self._volume_config_path)
					if not os.path.exists(dir):
						os.makedirs(dir)
					
					postgresql_data = message.postgresql.copy()
			
					root = PgUser(ROOT_USER, self.pg_keys_dir)
					root.store_keys(postgresql_data[OPT_ROOT_SSH_PUBLIC_KEY], postgresql_data[OPT_ROOT_SSH_PRIVATE_KEY])
					del postgresql_data[OPT_ROOT_SSH_PUBLIC_KEY]
					del postgresql_data[OPT_ROOT_SSH_PRIVATE_KEY]		
					
					for key, file in ((OPT_VOLUME_CNF, self._volume_config_path), 
									(OPT_SNAPSHOT_CNF, self._snapshot_config_path)):
						if os.path.exists(file):
							os.remove(file)
						
						if key in postgresql_data:
							if postgresql_data[key]:
								Storage.backup_config(postgresql_data[key], file)
							del postgresql_data[key]
					
					root_user= postgresql_data[OPT_ROOT_USER] or ROOT_USER
					postgresql_data['%s_password' % root_user] = postgresql_data.get(OPT_ROOT_PASSWORD) or cryptotool.pwgen(10)
					del postgresql_data[OPT_ROOT_PASSWORD]
					
					self._logger.debug("Update postgresql config with %s", postgresql_data)
					self._update_config(postgresql_data)


	def on_before_host_up(self, message):
		"""
		Configure PostgreSQL behaviour
		@type message: scalarizr.messaging.Message		
		@param message: HostUp message
		"""

		repl = 'master' if self.is_replication_master else 'slave'
		#bus.fire('before_postgresql_configure', replication=repl)
		
		if self.is_replication_master:
			self._init_master(message)									  
		else:
			self._init_slave(message)	
		bus.fire('service_configured', service_name=SERVICE_NAME, replication=repl)
					
				
	def on_before_reboot_start(self, *args, **kwargs):
		"""
		Stop MySQL and unplug storage
		"""
		self.postgresql.service.stop('rebooting')


	def on_before_reboot_finish(self, *args, **kwargs):
		self._insert_iptables_rules()


	def on_BeforeHostTerminate(self, message):
		self._logger.info('Handling BeforeHostTerminate message from %s' % message.local_ip)
		if message.local_ip == self._platform.get_private_ip():
			self._logger.info('Stopping %s service' % BEHAVIOUR)
			self.postgresql.service.stop('Server will be terminated')
			if not self.is_replication_master:
				self._logger.info('Destroying volume %s' % self.storage_vol.id)
				self.storage_vol.destroy(remove_disks=True)
				self._logger.info('Volume %s has been destroyed.' % self.storage_vol.id)


	def on_DbMsr_CreateDataBundle(self, message):
		
		try:
			op = operation(name=self._op_data_bundle, phases=[{
				'name': self._phase_data_bundle, 
				'steps': [self._step_create_data_bundle]
			}])
			op.define()
			
			with op.phase(self._phase_data_bundle):
				with op.step(self._step_create_data_bundle):
					
					bus.fire('before_postgresql_data_bundle')
					# Retrieve password for scalr postgresql root user
					# Creating snapshot		
					snap = self._create_snapshot()
					used_size = int(system2(('df', '-P', '--block-size=M', self._storage_path))[0].split('\n')[1].split()[2][:-1])
					bus.fire('postgresql_data_bundle', snapshot_id=snap.id)			
					
					# Notify scalr
					msg_data = dict(
						db_type 	= BEHAVIOUR,
						used_size	= '%.3f' % (float(used_size) / 1000,),
						status		= 'ok'
					)
					msg_data[BEHAVIOUR] = self._compat_storage_data(snap=snap)
					self.send_message(DbMsrMessages.DBMSR_CREATE_DATA_BUNDLE_RESULT, msg_data)

			op.ok()

		except (Exception, BaseException), e:
			self._logger.exception(e)
			
			# Notify Scalr about error
			self.send_message(DbMsrMessages.DBMSR_CREATE_DATA_BUNDLE_RESULT, dict(
				db_type 	= BEHAVIOUR,
				status		='error',
				last_error	= str(e)
			))
			

	def on_DbMsr_PromoteToMaster(self, message):
		"""
		Promote slave to master
		@type message: scalarizr.messaging.Message
		@param message: postgresql_PromoteToMaster
		"""
		
		if message.db_type != BEHAVIOUR:
			self._logger.error('Wrong db_type in DbMsr_PromoteToMaster message: %s' % message.db_type)
			return
		
		if self.is_replication_master:
			self._logger.warning('Cannot promote to master. Already master')
			return
		
		bus.fire('before_slave_promote_to_master')
		
		master_storage_conf = message.body.get('volume_config')
		tx_complete = False	
		old_conf 		= None
		new_storage_vol	= None		
					
		try:
						
			msg_data = dict(
					db_type=BEHAVIOUR, 
					status="ok",
			)
			
			self.postgresql.stop_replication()
			
			if master_storage_conf and master_storage_conf['type'] != 'eph':

				self.postgresql.service.stop('Unplugging slave storage and then plugging master one')

				old_conf = self.storage_vol.detach(force=True) # ??????
				new_storage_vol = self._plug_storage(self._storage_path, master_storage_conf)	
							
				# Continue if master storage is a valid postgresql storage 
				if not self.postgresql.cluster_dir.is_initialized(self._storage_path):
					raise HandlerError("%s is not a valid postgresql storage" % self._storage_path)
				
				Storage.backup_config(new_storage_vol.config(), self._volume_config_path) 
				msg_data[BEHAVIOUR] = self._compat_storage_data(vol=new_storage_vol)
				
			slaves = [host.internal_ip for host in self._get_slave_hosts()]		
			self.postgresql.init_master(self._storage_path, self.root_password, slaves)
			self._update_config({OPT_REPLICATION_MASTER : "1"})	
				
			if not master_storage_conf or master_storage_conf['type'] == 'eph':									
				snap = self._create_snapshot()
				Storage.backup_config(snap.config(), self._snapshot_config_path)
				msg_data[BEHAVIOUR] = self._compat_storage_data(self.storage_vol, snap)
				
			msg_data[BEHAVIOUR].update({OPT_CURRENT_XLOG_LOCATION: None})		
			self.send_message(DbMsrMessages.DBMSR_PROMOTE_TO_MASTER_RESULT, msg_data)	
								
			tx_complete = True
			bus.fire('slave_promote_to_master')
			
		except (Exception, BaseException), e:
			self._logger.exception(e)
			if new_storage_vol:
				new_storage_vol.detach()
			# Get back slave storage
			if old_conf:
				self._plug_storage(self._storage_path, old_conf)
			
			self.send_message(DbMsrMessages.DBMSR_PROMOTE_TO_MASTER_RESULT, dict(
				db_type=BEHAVIOUR, 															
				status="error",
				last_error=str(e)
			))

			# Start postgresql
			self.postgresql.service.start()
		
		if tx_complete and master_storage_conf and master_storage_conf['type'] != 'eph':
			# Delete slave EBS
			self.storage_vol.destroy(remove_disks=True)
			self.storage_vol = new_storage_vol
			Storage.backup_config(self.storage_vol.config(), self._volume_config_path)

	
	def on_DbMsr_NewMasterUp(self, message):
		"""
		Switch replication to a new master server
		@type message: scalarizr.messaging.Message
		@param message:  DbMsr_NewMasterUp
		"""
		if not message.body.has_key(BEHAVIOUR) or message.db_type != BEHAVIOUR:
			raise HandlerError("DbMsr_NewMasterUp message for PostgreSQL behaviour must have 'postgresql' property and db_type 'postgresql'")
		
		postgresql_data = message.postgresql.copy()
		
		if self.is_replication_master:
			self._logger.debug('Skipping NewMasterUp. My replication role is master')	
			return 
		
		host = message.local_ip or message.remote_ip
		self._logger.info("Switching replication to a new postgresql master %s", host)
		bus.fire('before_postgresql_change_master', host=host)			
		
		if OPT_SNAPSHOT_CNF in postgresql_data and postgresql_data[OPT_SNAPSHOT_CNF]['type'] != 'eph':
			snap_data = postgresql_data[OPT_SNAPSHOT_CNF]
			self._logger.info('Reinitializing Slave from the new snapshot %s', 
					snap_data['id'])
			self.postgresql.service.stop()
			
			self._logger.debug('Destroying old storage')
			self.storage_vol.destroy()
			self._logger.debug('Storage destroyed')
			
			self._logger.debug('Plugging new storage')
			vol = Storage.create(snapshot=snap_data.copy(), tags=self.postgres_tags)
			self._plug_storage(self._storage_path, vol)
			self._logger.debug('Storage plugged')
			
			Storage.backup_config(vol.config(), self._volume_config_path)
			Storage.backup_config(snap_data, self._snapshot_config_path)
			self.storage_vol = vol
		
		self.postgresql.init_slave(self._storage_path, host, POSTGRESQL_DEFAULT_PORT, self.root_password)
			
		self._logger.debug("Replication switched")
		bus.fire('postgresql_change_master', host=host)

	def on_DbMsr_CreateBackup(self, message):
		#TODO: Think how to move the most part of it into Postgresql class 
		# Retrieve password for scalr mysql user
		tmpdir = backup_path = None
		try:
			# Get databases list
			psql = PSQL(user=self.postgresql.root_user.name)
			databases = psql.list_pg_databases()
			if 'template0' in databases:
				databases.remove('template0')
			
			
			op = operation(name=self._op_backup, phases=[{
				'name': self._phase_backup
			}])
			op.define()			
			
			with op.phase(self._phase_backup):
			
				if not os.path.exists(self._tmp_path):
					os.makedirs(self._tmp_path)
					
				# Defining archive name and path
				backup_filename = time.strftime('%Y-%m-%d-%H:%M:%S')+'.tar.gz'
				backup_path = os.path.join(self._tmp_path, backup_filename)
				
				# Creating archive 
				backup = tarfile.open(backup_path, 'w:gz')
	
				# Dump all databases
				self._logger.info("Dumping all databases")
				tmpdir = tempfile.mkdtemp(dir=self._tmp_path)		
				rchown(self.postgresql.root_user.name, tmpdir)

				def _single_backup(db_name):
					dump_path = tmpdir + os.sep + db_name + '.sql'
					pg_args = '%s %s --no-privileges -f %s' % (PG_DUMP, db_name, dump_path)
					su_args = [SU_EXEC, '-', self.postgresql.root_user.name, '-c', pg_args]
					err = system2(su_args)[1]
					if err:
						raise HandlerError('Error while dumping database %s: %s' % (db_name, err))
					backup.add(dump_path, os.path.basename(dump_path))	

				make_backup_steps(databases, op, _single_backup)						

				backup.close()
				
				with op.step(self._step_upload_to_cloud_storage):
					# Creating list of full paths to archive chunks
					if os.path.getsize(backup_path) > BACKUP_CHUNK_SIZE:
						parts = [os.path.join(tmpdir, file) for file in split(backup_path, backup_filename, BACKUP_CHUNK_SIZE , tmpdir)]
					else:
						parts = [backup_path]
					sizes = [os.path.getsize(file) for file in parts]
						
					cloud_storage_path = self._platform.scalrfs.backups(BEHAVIOUR)
					self._logger.info("Uploading backup to cloud storage (%s)", cloud_storage_path)
					trn = transfer.Transfer()
					cloud_files = trn.upload(parts, cloud_storage_path)
					self._logger.info("Postgresql backup uploaded to cloud storage under %s/%s", 
									cloud_storage_path, backup_filename)
			
			result = list(dict(path=path, size=size) for path, size in zip(cloud_files, sizes))
			op.ok(data=result)
				
			# Notify Scalr
			self.send_message(DbMsrMessages.DBMSR_CREATE_BACKUP_RESULT, dict(
				db_type = BEHAVIOUR,
				status = 'ok',
				backup_parts = result
			))
						
		except (Exception, BaseException), e:
			self._logger.exception(e)
			
			# Notify Scalr about error
			self.send_message(DbMsrMessages.DBMSR_CREATE_BACKUP_RESULT, dict(
				db_type = BEHAVIOUR,
				status = 'error',
				last_error = str(e)
			))
			
		finally:
			if tmpdir:
				shutil.rmtree(tmpdir, ignore_errors=True)
			if backup_path and os.path.exists(backup_path):
				os.remove(backup_path)				
		
								
	def _init_master(self, message):
		"""
		Initialize postgresql master
		@type message: scalarizr.messaging.Message 
		@param message: HostUp message
		"""
		
		self._logger.info("Initializing PostgreSQL master")
		
		with bus.initialization_op as op:
			with op.step(self._step_create_storage):		
		
				# Plug storage
				volume_cnf = Storage.restore_config(self._volume_config_path)
				try:
					snap_cnf = Storage.restore_config(self._snapshot_config_path)
					volume_cnf['snapshot'] = snap_cnf
				except IOError:
					pass
				self.storage_vol = self._plug_storage(mpoint=self._storage_path, vol=volume_cnf)
				Storage.backup_config(self.storage_vol.config(), self._volume_config_path)		
				
			with op.step(self._step_init_master):
				self.postgresql.init_master(mpoint=self._storage_path, password=self.root_password)
				
				msg_data = dict()
				msg_data.update({OPT_REPLICATION_MASTER 		: 	str(int(self.is_replication_master)),
									OPT_ROOT_USER				:	self.postgresql.root_user.name,
									OPT_ROOT_PASSWORD			:	self.root_password,
									OPT_CURRENT_XLOG_LOCATION	: 	None})	
					
			with op.step(self._step_create_data_bundle):
				# Create snapshot
				snap = self._create_snapshot()
				Storage.backup_config(snap.config(), self._snapshot_config_path)
			
			with op.step(self._step_collect_host_up_data):
				# Update HostUp message 
				msg_data.update(self._compat_storage_data(self.storage_vol, snap))
					
				if msg_data:
					message.db_type = BEHAVIOUR
					message.postgresql = msg_data.copy()
					message.postgresql.update({
									OPT_ROOT_SSH_PRIVATE_KEY	: 	self.postgresql.root_user.private_key, 
									OPT_ROOT_SSH_PUBLIC_KEY 	: 	self.postgresql.root_user.public_key
									})
					try:
						del msg_data[OPT_SNAPSHOT_CNF], msg_data[OPT_VOLUME_CNF]
					except KeyError:
						pass 
					self._update_config(msg_data)
	
	def _get_master_host(self):
		master_host = None
		self._logger.info("Requesting master server")
		while not master_host:
			try:
				master_host = list(host 
					for host in self._queryenv.list_roles(behaviour=BEHAVIOUR)[0].hosts 
					if host.replication_master)[0]
			except IndexError:
				self._logger.debug("QueryEnv respond with no postgresql master. " + 
						"Waiting %d seconds before the next attempt", 5)
				time.sleep(5)
		return master_host
	
	def _get_slave_hosts(self):
		self._logger.info("Requesting standby servers")
		return list(host for host in self._queryenv.list_roles(behaviour=BEHAVIOUR)[0].hosts 
				if not host.replication_master)
				
	def _init_slave(self, message):
		"""
		Initialize postgresql slave
		@type message: scalarizr.messaging.Message 
		@param message: HostUp message
		"""
		self._logger.info("Initializing postgresql slave")
		
		with bus.initialization_op as op:
			with op.step(self._step_create_storage):
				self._logger.debug("Initialize slave storage")
				self.storage_vol = self._plug_storage(self._storage_path, 
						dict(snapshot=Storage.restore_config(self._snapshot_config_path)))			
				Storage.backup_config(self.storage_vol.config(), self._volume_config_path)
			
			with op.step(self._step_init_slave):
				# Change replication master 
				master_host = self._get_master_host()
						
				self._logger.debug("Master server obtained (local_ip: %s, public_ip: %s)",
						master_host.internal_ip, master_host.external_ip)
				
				host = master_host.internal_ip or master_host.external_ip
				self.postgresql.init_slave(self._storage_path, host, POSTGRESQL_DEFAULT_PORT, self.root_password)
			
			with op.step(self._step_collect_host_up_data):
				# Update HostUp message
				message.postgresql = self._compat_storage_data(self.storage_vol)
				message.db_type = BEHAVIOUR


	def _update_config(self, data): 
		#XXX: I just don't like it
		#ditching empty data
		updates = dict()
		for k,v in data.items():
			if v: 
				updates[k] = v
		
		self._cnf.update_ini(BEHAVIOUR, {CNF_SECTION: updates})


	def _plug_storage(self, mpoint, vol):
		if not isinstance(vol, Volume):
			vol['tags'] = self.postgres_tags
			vol = Storage.create(vol)

		try:
			if not os.path.exists(mpoint):
				os.makedirs(mpoint)
			if not vol.mounted():
				vol.mount(mpoint)
		except StorageError, e:
			''' XXX: Crapy. We need to introduce error codes from fstool ''' 
			if 'you must specify the filesystem type' in str(e):
				vol.mkfs()
				vol.mount(mpoint)
			else:
				raise
		return vol


	def _create_snapshot(self):
		self._logger.info("Creating PostgreSQL data bundle")
		psql = PSQL()
		if self.postgresql.service.running:
			psql.start_backup()
		
		system2('sync', shell=True)
		# Creating storage snapshot
		snap = self._create_storage_snapshot()
		if self.postgresql.service.running:
			psql.stop_backup()
		
		wait_until(lambda: snap.state in (Snapshot.CREATED, Snapshot.COMPLETED, Snapshot.FAILED))
		if snap.state == Snapshot.FAILED:
			raise HandlerError('postgresql storage snapshot creation failed. See log for more details')
		
		self._logger.info('PostgreSQL data bundle created\n  snapshot: %s', snap.id)
		return snap


	def _create_storage_snapshot(self):
		try:
			return self.storage_vol.snapshot(tags=self.postgres_tags)
		except StorageError, e:
			self._logger.error("Cannot create PostgreSQL data snapshot. %s", e)
			raise
		

	def _compat_storage_data(self, vol=None, snap=None):
		ret = dict()
		if vol:
			ret['volume_config'] = vol.config()
		if snap:
			ret['snapshot_config'] = snap.config()
		return ret


	def _insert_iptables_rules(self):
		if iptables.enabled():
			iptables.FIREWALL.ensure([
				{"jump": "ACCEPT", "protocol": "tcp", "match": "tcp", "dport": str(POSTGRESQL_DEFAULT_PORT)},
			])

		"""
		iptables = IpTables()
		if iptables.enabled():
			iptables.insert_rule(None, RuleSpec(dport=POSTGRESQL_DEFAULT_PORT, 
											jump='ACCEPT', protocol=P_TCP))
		"""
