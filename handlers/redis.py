'''
Created on Aug 12, 2011

@author: Dmytro Korsakov
'''
from __future__ import with_statement

import os
import time
import shutil
import tarfile
import tempfile
import logging


from scalarizr import config
from scalarizr.bus import bus
from scalarizr import handlers
from scalarizr.messaging import Messages
from scalarizr.util import system2, wait_until, cryptotool, software, initdv2
from scalarizr.util.filetool import split
from scalarizr.services import redis
from scalarizr.service import CnfController
from scalarizr.config import BuiltinBehaviours, ScalarizrState
from scalarizr.handlers import ServiceCtlHandler, HandlerError, DbMsrMessages
from scalarizr.storage import Storage, Snapshot, StorageError, Volume, transfer
from scalarizr.libs.metaconf import Configuration, NoPathError
from scalarizr.handlers import operation, prepare_tags


BEHAVIOUR = SERVICE_NAME = CNF_SECTION = BuiltinBehaviours.REDIS

STORAGE_PATH 				= '/mnt/redisstorage'
STORAGE_VOLUME_CNF 			= 'redis.json'
STORAGE_SNAPSHOT_CNF 		= 'redis-snap.json'

OPT_REPLICATION_MASTER  	= 'replication_master'
OPT_PERSISTENCE_TYPE		= 'persistence_type'
OPT_MASTER_PASSWORD			= "master_password"
OPT_VOLUME_CNF				= 'volume_config'
OPT_SNAPSHOT_CNF			= 'snapshot_config'
OPT_USE_PASSWORD            = 'use_password'

REDIS_CNF_PATH				= 'cnf_path'
UBUNTU_CONFIG_PATH			= '/etc/redis/redis.conf'
CENTOS_CONFIG_PATH			= '/etc/redis.conf'

BACKUP_CHUNK_SIZE 			= 200*1024*1024


LOG = logging.getLogger(__name__)


initdv2.explore(SERVICE_NAME, redis.RedisInitScript)


def get_handlers():
	return (RedisHandler(), )


class RedisHandler(ServiceCtlHandler, handlers.FarmSecurityMixin):

	_queryenv = None
	""" @type _queryenv: scalarizr.queryenv.QueryEnvService	"""

	_platform = None
	""" @type _platform: scalarizr.platform.Ec2Platform """

	_cnf = None
	''' @type _cnf: scalarizr.config.ScalarizrCnf '''
	
	storage_vol = None	
	default_service = None
		
	@property
	def is_replication_master(self):
		value = 0
		if self._cnf.rawini.has_section(CNF_SECTION) and self._cnf.rawini.has_option(CNF_SECTION, OPT_REPLICATION_MASTER):
			value = self._cnf.rawini.get(CNF_SECTION, OPT_REPLICATION_MASTER)
			LOG.debug('Got %s : %s' % (OPT_REPLICATION_MASTER, value))
		return True if int(value) else False


	@property
	def redis_tags(self):
		return prepare_tags(BEHAVIOUR, db_replication_role=self.is_replication_master)


	@property
	def persistence_type(self):
		value = 'snapshotting'
		if self._cnf.rawini.has_section(CNF_SECTION) and self._cnf.rawini.has_option(CNF_SECTION, OPT_PERSISTENCE_TYPE):
			value = self._cnf.rawini.get(CNF_SECTION, OPT_PERSISTENCE_TYPE)
			LOG.debug('Got %s : %s' % (OPT_PERSISTENCE_TYPE, value))
		return value


	def accept(self, message, queue, behaviour=None, platform=None, os=None, dist=None):
		return BEHAVIOUR in behaviour and (
		message.name == DbMsrMessages.DBMSR_NEW_MASTER_UP
		or 	message.name == DbMsrMessages.DBMSR_PROMOTE_TO_MASTER
		or 	message.name == DbMsrMessages.DBMSR_CREATE_DATA_BUNDLE
		or 	message.name == DbMsrMessages.DBMSR_CREATE_BACKUP
		or  message.name == Messages.UPDATE_SERVICE_CONFIGURATION
		or  message.name == Messages.BEFORE_HOST_TERMINATE
		or  message.name == Messages.HOST_INIT)


	def get_initialization_phases(self, hir_message):
		if BEHAVIOUR in hir_message.body:

			steps = [self._step_accept_scalr_conf, self._step_create_storage]
			if hir_message.body[BEHAVIOUR]['replication_master'] == '1':
				steps += [self._step_init_master, self._step_create_data_bundle]
			else:
				steps += [self._step_init_slave]
			steps += [self._step_collect_host_up_data]

			return {'before_host_up': [{
			                           'name': self._phase_redis,
			                           'steps': steps
			                           }]}


	def __init__(self):
		handlers.FarmSecurityMixin.__init__(self, ["%s:%s" %
			 (redis.DEFAULT_PORT, redis.DEFAULT_PORT+16)])
		ServiceCtlHandler.__init__(self, SERVICE_NAME, cnf_ctl=RedisCnfController())
		bus.on("init", self.on_init)
		bus.define_events(
			'before_%s_data_bundle' % BEHAVIOUR,

			'%s_data_bundle' % BEHAVIOUR,

			# @param host: New master hostname 
			'before_%s_change_master' % BEHAVIOUR,

			# @param host: New master hostname 
			'%s_change_master' % BEHAVIOUR,

			'before_slave_promote_to_master',

			'slave_promote_to_master'
		)

		self._phase_redis = 'Configure Redis'
		self._phase_data_bundle = self._op_data_bundle = 'Redis data bundle'
		self._phase_backup = self._op_backup = 'Redis backup'
		self._step_copy_database_file = 'Copy database file'
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

		bus.on("host_init_response", self.on_host_init_response)
		bus.on("before_host_up", self.on_before_host_up)
		bus.on("before_reboot_start", self.on_before_reboot_start)
		bus.on("before_reboot_finish", self.on_before_reboot_finish)

		if self._cnf.state == ScalarizrState.RUNNING:

			storage_conf = Storage.restore_config(self._volume_config_path)
			storage_conf['tags'] = self.redis_tags
			self.storage_vol = Storage.create(storage_conf)
			if not self.storage_vol.mounted():
				self.storage_vol.mount()

			self.redis_instances = redis.RedisInstances(self.is_replication_master, self.persistence_type)
			self.redis_instances.init_processes(ports=[redis.DEFAULT_PORT,], passwords=[self.get_main_password(),])
			self.redis_instances.start()
			self._init_script = self.redis_instances.get_default_process()


	def on_reload(self):
		self._queryenv = bus.queryenv_service
		self._platform = bus.platform
		self._cnf = bus.cnf
		ini = self._cnf.rawini
		self._role_name = ini.get(config.SECT_GENERAL, config.OPT_ROLE_NAME)

		self._storage_path = STORAGE_PATH

		self._volume_config_path  = self._cnf.private_path(os.path.join('storage', STORAGE_VOLUME_CNF))
		self._snapshot_config_path = self._cnf.private_path(os.path.join('storage', STORAGE_SNAPSHOT_CNF))
		
		self.default_service = initdv2.lookup(SERVICE_NAME)
		
		
	def on_host_init_response(self, message):
		"""
		Check redis data in host init response
		@type message: scalarizr.messaging.Message
		@param message: HostInitResponse
		"""
		with bus.initialization_op as op:
			with op.phase(self._phase_redis):
				with op.step(self._step_accept_scalr_conf):

					if not message.body.has_key(BEHAVIOUR) or message.db_type != BEHAVIOUR:
						raise HandlerError("HostInitResponse message for %s behaviour must have '%s' property and db_type '%s'"
						                   % (BEHAVIOUR, BEHAVIOUR, BEHAVIOUR))

					config_dir = os.path.dirname(self._volume_config_path)
					if not os.path.exists(config_dir):
						os.makedirs(config_dir)

					redis_data = message.redis.copy()
					LOG.info('Got Redis part of HostInitResponse: %s' % redis_data)

					'''
					XXX: following line enables support for old scalr installations
					use_password shoud be set by postinstall script for old servers
					'''
					redis_data[OPT_USE_PASSWORD] = redis_data.get(OPT_USE_PASSWORD, '1')

					for key, config_file in ((OPT_VOLUME_CNF, self._volume_config_path),
					                         (OPT_SNAPSHOT_CNF, self._snapshot_config_path)):
						if os.path.exists(config_file):
							os.remove(config_file)

						if key in redis_data:
							if redis_data[key]:
								Storage.backup_config(redis_data[key], config_file)
							del redis_data[key]

					LOG.debug("Update redis config with %s", redis_data)
					self._update_config(redis_data)

					if self.default_service.running:
						self.default_service.stop('Treminating default redis instance')
						
					self.redis_instances = redis.RedisInstances(self.is_replication_master, self.persistence_type)
					self.redis_instances.init_processes(ports=[redis.DEFAULT_PORT,], passwords=[self.get_main_password(),])


	def on_before_host_up(self, message):
		"""
		Configure redis behaviour
		@type message: scalarizr.messaging.Message		
		@param message: HostUp message
		"""

		repl = 'master' if self.is_replication_master else 'slave'

		if self.is_replication_master:
			self._init_master(message)
		else:
			self._init_slave(message)
		self._init_script = self.redis_instances.get_default_process()
		bus.fire('service_configured', service_name=SERVICE_NAME, replication=repl)


	def on_before_reboot_start(self, *args, **kwargs):
		self.redis_instances.save_all()


	def on_before_reboot_finish(self, *args, **kwargs):
		if self.default_service.running:
			self.default_service.stop('Treminating default redis instance')


	def on_BeforeHostTerminate(self, message):
		LOG.info('Handling BeforeHostTerminate message from %s' % message.local_ip)
		if message.local_ip == self._platform.get_private_ip():
			LOG.info('Dumping redis data on disk')
			self.redis_instances.save_all()
			LOG.info('Stopping %s service' % BEHAVIOUR)
			self.redis_instances.stop('Server will be terminated')
			if not self.is_replication_master:
				LOG.info('Destroying volume %s' % self.storage_vol.id)
				self.storage_vol.destroy(remove_disks=True)
				LOG.info('Volume %s was destroyed.' % self.storage_vol.id)


	def on_DbMsr_CreateDataBundle(self, message):

		try:
			op = operation(name=self._op_data_bundle, phases=[{
			                                                  'name': self._phase_data_bundle,
			                                                  'steps': [self._step_create_data_bundle]
			                                                  }])
			op.define()


			with op.phase(self._phase_data_bundle):
				with op.step(self._step_create_data_bundle):

					bus.fire('before_%s_data_bundle' % BEHAVIOUR)
					# Creating snapshot		
					snap = self._create_snapshot()
					used_size = int(system2(('df', '-P', '--block-size=M', self._storage_path))[0].split('\n')[1].split()[2][:-1])
					bus.fire('%s_data_bundle' % BEHAVIOUR, snapshot_id=snap.id)

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
			LOG.exception(e)

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
		@param message: redis_PromoteToMaster
		"""

		if message.db_type != BEHAVIOUR:
			LOG.error('Wrong db_type in DbMsr_PromoteToMaster message: %s' % message.db_type)
			return

		if self.is_replication_master:
			LOG.warning('Cannot promote to master. Already master')
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

			if master_storage_conf and master_storage_conf['type'] != 'eph':

				self.redis_instances.stop('Unplugging slave storage and then plugging master one')

				old_conf = self.storage_vol.detach(force=True) # ??????
				new_storage_vol = self._plug_storage(self._storage_path, master_storage_conf)	
				
				'''
				#This code was removed because redis master storage can be empty yet valid
				for r in self.redis_instances:
					# Continue if master storage is a valid redis storage 
					if not r.working_directory.is_initialized(self._storage_path):
						raise HandlerError("%s is not a valid %s storage" % (self._storage_path, BEHAVIOUR))

				Storage.backup_config(new_storage_vol.config(), self._volume_config_path)
				'''
				
				Storage.backup_config(new_storage_vol.config(), self._volume_config_path) 
				msg_data[BEHAVIOUR] = self._compat_storage_data(vol=new_storage_vol)

			self.redis_instances.init_as_masters(self._storage_path)
			self._update_config({OPT_REPLICATION_MASTER : "1"})

			if not master_storage_conf or master_storage_conf['type'] == 'eph':

				snap = self._create_snapshot()
				Storage.backup_config(snap.config(), self._snapshot_config_path)
				msg_data[BEHAVIOUR] = self._compat_storage_data(self.storage_vol, snap)

			self.send_message(DbMsrMessages.DBMSR_PROMOTE_TO_MASTER_RESULT, msg_data)

			tx_complete = True
			bus.fire('slave_promote_to_master')

		except (Exception, BaseException), e:
			LOG.exception(e)
			if new_storage_vol and not new_storage_vol.detached:
				new_storage_vol.detach()
			# Get back slave storage
			if old_conf:
				self._plug_storage(self._storage_path, old_conf)

			self.send_message(DbMsrMessages.DBMSR_PROMOTE_TO_MASTER_RESULT, dict(
				db_type=BEHAVIOUR,
				status="error",
				last_error=str(e)
			))

			# Start redis
			self.redis_instances.start()

		if tx_complete and master_storage_conf and master_storage_conf['type'] != 'eph':
			# Delete slave EBS
			self.storage_vol.destroy(remove_disks=True)
			self.storage_vol = new_storage_vol
			Storage.backup_config(self.storage_vol.config(), self._volume_config_path)



	def on_DbMsr_NewMasterUp(self, message):
		"""
		Switch replication to a new master server
		@type message: scalarizr.messaging.Message
		@param message:  DbMsr__NewMasterUp
		"""
		if not message.body.has_key(BEHAVIOUR) or message.db_type != BEHAVIOUR:
			raise HandlerError("DbMsr_NewMasterUp message for %s behaviour must have '%s' property and db_type '%s'" %
			                   BEHAVIOUR, BEHAVIOUR, BEHAVIOUR)

		if self.is_replication_master:
			LOG.debug('Skipping NewMasterUp. My replication role is master')
			return

		host = message.local_ip or message.remote_ip
		LOG.info("Switching replication to a new %s master %s"% (BEHAVIOUR, host))
		bus.fire('before_%s_change_master' % BEHAVIOUR, host=host)

		self.redis_instances.init_as_slaves(self._storage_path, host)
		self.redis_instances.wait_for_sync()

		LOG.debug("Replication switched")
		bus.fire('%s_change_master' % BEHAVIOUR, host=host)


	def on_DbMsr_CreateBackup(self, message):
		tmpdir = backup_path = None
		try:
			op = operation(name=self._op_backup, phases=[{
			                                             'name': self._phase_backup,
			                                             'steps': [self._step_copy_database_file,
			                                                       self._step_upload_to_cloud_storage]
			                                             }])
			op.define()

			with op.phase(self._phase_backup):

				with op.step(self._step_copy_database_file):
					# Flush redis data on disk before creating backup
					LOG.info("Dumping Redis data on disk")
					self.redis_instances.save_all()

					# Dump all databases
					LOG.info("Dumping all databases")
					tmpdir = tempfile.mkdtemp()

					# Defining archive name and path
					backup_filename = time.strftime('%Y-%m-%d-%H:%M:%S')+'.tar.gz'
					backup_path = os.path.join('/tmp', backup_filename)
					dbs = [r.db_path for r in self.redis_instances]

					# Creating archive 
					backup = tarfile.open(backup_path, 'w:gz')

					for src_path in dbs:
						fname = os.path.basename(src_path)
						dump_path = os.path.join(tmpdir, fname)
						if not os.path.exists(src_path):
							LOG.info('%s DB file %s does not exist. Nothing to backup.' % (BEHAVIOUR, src_path))
						else:
							shutil.copyfile(src_path, dump_path)
							backup.add(dump_path, fname)
					backup.close()

					# Creating list of full paths to archive chunks
					if os.path.getsize(backup_path) > BACKUP_CHUNK_SIZE:
						parts = [os.path.join(tmpdir, file) for file in split(backup_path, backup_filename, BACKUP_CHUNK_SIZE , tmpdir)]
					else:
						parts = [backup_path]
					sizes = [os.path.getsize(file) for file in parts]

				with op.step(self._step_upload_to_cloud_storage):

					cloud_storage_path = self._platform.scalrfs.backups(BEHAVIOUR)
					LOG.info("Uploading backup to cloud storage (%s)", cloud_storage_path)
					trn = transfer.Transfer()
					cloud_files = trn.upload(parts, cloud_storage_path)
					LOG.info("%s backup uploaded to cloud storage under %s/%s" %
					         (BEHAVIOUR, cloud_storage_path, backup_filename))

			result = list(dict(path=path, size=size) for path, size in zip(cloud_files, sizes))
			op.ok(data=result)

			# Notify Scalr
			self.send_message(DbMsrMessages.DBMSR_CREATE_BACKUP_RESULT, dict(
				db_type = BEHAVIOUR,
				status = 'ok',
				backup_parts = result
			))

		except (Exception, BaseException), e:
			LOG.exception(e)

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
		Initialize redis master
		@type message: scalarizr.messaging.Message 
		@param message: HostUp message
		"""

		with bus.initialization_op as op:
			with op.step(self._step_create_storage):

				LOG.info("Initializing %s master" % BEHAVIOUR)

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
				password = self.get_main_password()
				ri = self.redis_instances.get_instance(port=redis.DEFAULT_PORT)
				ri.init_master(mpoint=self._storage_path)

				msg_data = dict()
				msg_data.update({OPT_REPLICATION_MASTER 		: 	'1',
				                 OPT_MASTER_PASSWORD			:	password})

			with op.step(self._step_create_data_bundle):
				# Create snapshot
				snap = self._create_snapshot()
				Storage.backup_config(snap.config(), self._snapshot_config_path)

			with op.step(self._step_collect_host_up_data):
				# Update HostUp message 
				msg_data.update(self._compat_storage_data(self.storage_vol, snap))

				if msg_data:
					message.db_type = BEHAVIOUR
					message.redis = msg_data.copy()
					try:
						del msg_data[OPT_SNAPSHOT_CNF], msg_data[OPT_VOLUME_CNF]
					except KeyError:
						pass
					self._update_config(msg_data)

	@property
	def using_password(self):
		if not self._cnf.rawini.has_option(CNF_SECTION, OPT_USE_PASSWORD):
			self._update_config({OPT_USE_PASSWORD:'1'})
		val = self._cnf.rawini.get(CNF_SECTION, OPT_USE_PASSWORD)
		return True if int(val) else False


	def get_main_password(self):
		password = None
		if self.using_password:
			if self._cnf.rawini.has_option(CNF_SECTION, OPT_MASTER_PASSWORD):
				password = self._cnf.rawini.get(CNF_SECTION, OPT_MASTER_PASSWORD)
			if not password:
				password = cryptotool.pwgen(20)
				self._update_config({OPT_MASTER_PASSWORD:password})
		return password

	def _get_master_host(self):
		master_host = None
		LOG.info("Requesting master server")
		while not master_host:
			try:
				master_host = list(host 
					for host in self._queryenv.list_roles(behaviour=BEHAVIOUR)[0].hosts 
					if host.replication_master)[0]
			except IndexError:
				LOG.debug("QueryEnv respond with no %s master. " % BEHAVIOUR +
				          "Waiting %d seconds before the next attempt" % 5)
				time.sleep(5)
		return master_host


	def _init_slave(self, message):
		"""
		Initialize redis slave
		@type message: scalarizr.messaging.Message 
		@param message: HostUp message
		"""
		LOG.info("Initializing %s slave" % BEHAVIOUR)

		with bus.initialization_op as op:
			with op.step(self._step_create_storage):

				LOG.debug("Initializing slave storage")
				self.storage_vol = self._plug_storage(self._storage_path,
					dict(snapshot=Storage.restore_config(self._snapshot_config_path)))
				Storage.backup_config(self.storage_vol.config(), self._volume_config_path)

			with op.step(self._step_init_slave):
				# Change replication master 
				master_host = self._get_master_host()

				LOG.debug("Master server obtained (local_ip: %s, public_ip: %s)",
					master_host.internal_ip, master_host.external_ip)

				host = master_host.internal_ip or master_host.external_ip
				instance = self.redis_instances.get_instance(port=redis.DEFAULT_PORT)
				instance.init_slave(self._storage_path, host, redis.DEFAULT_PORT)
				op.progress(50)
				instance.wait_for_sync()

			with op.step(self._step_collect_host_up_data):
				# Update HostUp message
				message.redis = self._compat_storage_data(self.storage_vol)
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
			vol['tags'] = self.redis_tags
			vol = Storage.create(vol)

		try:
			if not os.path.exists(mpoint):
				os.makedirs(mpoint)
			if not vol.mounted():
				vol.mount(mpoint)
		except StorageError, e:
			if 'you must specify the filesystem type' in str(e):
				vol.mkfs()
				vol.mount(mpoint)
			else:
				raise
		return vol


	def _create_snapshot(self):
		LOG.info("Creating Redis data bundle")
		system2('sync', shell=True)
		# Creating storage snapshot
		snap = self._create_storage_snapshot()

		wait_until(lambda: snap.state in (Snapshot.CREATED, Snapshot.COMPLETED, Snapshot.FAILED))
		if snap.state == Snapshot.FAILED:
			raise HandlerError('%s storage snapshot creation failed. See log for more details' % BEHAVIOUR)

		LOG.info('Redis data bundle created\n  snapshot: %s', snap.id)
		return snap


	def _create_storage_snapshot(self):
		LOG.info("Dumping Redis data on disk")
		self.redis_instances.save_all()
		try:
			return self.storage_vol.snapshot(tags=self.redis_tags)
		except StorageError, e:
			LOG.error("Cannot create %s data snapshot. %s", (BEHAVIOUR, e))
			raise


	def _compat_storage_data(self, vol=None, snap=None):
		ret = dict()
		if vol:
			ret['volume_config'] = vol.config()
		if snap:
			ret['snapshot_config'] = snap.config()
		return ret


class RedisCnfController(CnfController):

	def __init__(self):
		cnf_path = redis.get_redis_conf_path()
		CnfController.__init__(self, BEHAVIOUR, cnf_path, 'redis', {'1':'yes', '0':'no'})


	@property
	def _software_version(self):
		return software.software_info('redis').version


	def get_main_password(self):
		password = None
		cnf = bus.cnf
		if cnf.rawini.has_option(CNF_SECTION, OPT_MASTER_PASSWORD):
			password = cnf.rawini.get(CNF_SECTION, OPT_MASTER_PASSWORD)
		return password


	def _after_apply_preset(self):
		password = self.get_main_password()
		cli = redis.RedisCLI(password)
		cli.bgsave()
		
