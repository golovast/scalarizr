import os
import uuid

from scalarizr import storage2
from scalarizr.libs import bases
from scalarizr.linux import coreutils, mount as mod_mount


LOG = storage2.LOG


class Base(bases.ConfigDriven):

	def __init__(self, 
				version='2.0', 
				type='base', 
				id=None, 
				tags=None,
				**kwds):
		super(Base, self).__init__(
				version=version, type=type, 
				id=id, tags=tags or {}, **kwds)
		self.error_messages.update({
			'restore_unsupported': 'Restores from snapshot not supported '
									'by this volume type: %s',
		})


	def _genid(self, prefix=''):
		return '%s%s-%s' % (prefix, self.type, uuid.uuid4().hex[0:8])		


class Volume(Base):
	MAX_SIZE = None
	
	def __init__(self, 
				device=None, 
				fstype='ext3', 
				fscreated=False, 
				mpoint=None, 
				snap=None,
				**kwds):
		super(Volume, self).__init__(
				device=device,
				fstype=fstype,
				fscreated=fscreated,
				mpoint=mpoint,
				snap=snap,
				**kwds)
		self.features.update({'restore': True})
		

	def ensure(self, mount=False, mkfs=False, fstab=False, **updates):
		if not self.features['restore']:
			self._check_restore_unsupported()
		if self.snap and isinstance(self.snap, Snapshot):
			self.snap = self.snap.config()
		self._ensure()
		self._check_attr('device')
		if not self.id:
			self.id = self._genid('vol-')
		if mount:
			try:
				LOG.debug('Mounting: %s', self.id)
				self.mount()
			except mod_mount.NoFileSystem:
				if mkfs:
					LOG.debug('Creating %s filesystem: %s', self.fstype, self.id)
					self.mkfs()
					self.mount()
				else:
					raise
			if fstab and self.device not in mod_mount.fstab():
				LOG.debug('Adding to fstab: %s', self.id)
				mod_mount.fstab().add(self.device, self.mpoint, self.fstype)
		return self.config()
	
	
	def snapshot(self, description=None, tags=None, **kwds):
		return self._snapshot(description, tags, **kwds)


	def destroy(self, force=False, **kwds):
		if self.device:
			self.detach(force, **kwds)
		self._destroy(force, **kwds)


	def detach(self, force=False, **kwds):
		if not self.device:
			return
		self.umount()
		self._detach(force, **kwds)


	def mount(self):
		self._check(mpoint=True)
		mounted_to = self.mounted_to()
		if mounted_to == self.mpoint:
			return
		elif mounted_to: 
			self.umount()
		if not os.path.exists(self.mpoint):
			os.makedirs(self.mpoint)
		mod_mount.mount(self.device, self.mpoint)


	def umount(self):
		self._check()
		mod_mount.umount(self.device)


	def mounted_to(self):
		self._check()
		try:
			return mod_mount.mounts()[self.device].mpoint
		except KeyError:
			return False


	def is_fs_created(self):
		self._check()
		try:
			device_attrs = coreutils.blkid(self.device)
		except:
			return False

		fstype = device_attrs.get('type')

		if fstype is None:
			return False
		else:
			self.fstype = fstype
			return True


	def mkfs(self):
		self._check()
		if self.fscreated:
			raise storage2.OperationError(
					'fscreated flag is active. Filesystem creation denied '
					'to preserve the original filesystem. If you wish to '
					'proceed anyway set fscreated=False and retry')
		fs = storage2.filesystem(self.fstype)
		LOG.info('Creating filesystem on %s', self.device)
		fs.mkfs(self.device)
		self.fscreated = True


	def clone(self):
		config = self.initial_config.copy()
		config.pop('id', None)		
		config.pop('fscreated', None)
		self._clone(config)
		return storage2.volume(config)


	def _check(self, fstype=True, device=True, **kwds):
		if fstype:
			self._check_attr('fstype')
		if device:
			self._check_attr('device')
		for name in kwds:
			self._check_attr(name)

	
	def _check_attr(self, name):
		assert hasattr(self, name) and getattr(self, name) is not None,  \
				self.error_messages['empty_attr'] % name


	def _check_restore_unsupported(self):
		if self.snap:
			msg = self.error_messages['restore_unsupported'] % self.type
			#FIXME: eph volume in NewMasterUp raises error here
			LOG.debug(msg)
			LOG.debug('Some details: features=%s, config=%s', self.features, self.config())
			#raise NotImplementedError(msg)
		
	
	def _ensure(self):
		pass
	
	
	def _snapshot(self, description, tags, **kwds):
		pass


	def _detach(self, force, **kwds):
		pass

	
	def _destroy(self, force, **kwds):
		pass

	def _clone(self, config):
		pass
	

storage2.volume_types['base'] = Volume	
	
	
class Snapshot(Base):
	QUEUED = 'queued'
	IN_PROGRESS = 'in-progress'
	COMPLETED = 'completed'
	FAILED = 'failed'
	UNKNOWN = 'unknown'

	def __init__(self, **kwds):
		super(Snapshot, self).__init__(**kwds)
		if not self._config.get('id'):
			self._config['id'] = self._genid('snap-')
	
	
	def restore(self):
		vol = storage2.volume(type=self.type, snap=self)
		vol.ensure()
		return vol
	
	
	def destroy(self):
		return self._destroy()
	
	
	def status(self):
		return self._status()
	

	def _destroy(self):
		pass
	
	def _status(self):
		pass
	

storage2.snapshot_types['base'] = Snapshot
