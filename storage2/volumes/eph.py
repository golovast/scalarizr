from __future__ import with_statement

import os
import sys
import logging
import tempfile
import urlparse

from scalarizr import storage2
from scalarizr.libs import metaconf
from scalarizr.util import filetool
from scalarizr.storage2 import cloudfs
from scalarizr.storage2.volumes import base

# until transfer will be ready
from scalarizr.storage import eph as old_eph


LOG = logging.getLogger(__name__)


class EphVolume(base.Volume):
	
	def __init__(self, 
				vg=None, 
				disk=None, 
				size=None, 
				cloudfs_dir=None, 
				**kwds):
		'''
		:type vg:
		:param vg:

		:type disk:
		:param disk:

		:type size:
		:param size:

		:type cloudfs_dir:
		:param cloudfs_dir:
		'''
		# Compatibility with 1.0
		snap_backend = kwds.pop('snap_backend', None)
		if snap_backend:
			cloudfs_dir = snap_backend['path'] \
						if isinstance(snap_backend, dict) \
						else snap_backend
			if not cloudfs_dir.endswith('/'):
				cloudfs_dir += '/'
		kwds.pop('lvm_group_cfg', None)

		super(EphVolume, self).__init__(vg=vg, disk=disk, size=size,
				cloudfs_dir=cloudfs_dir, **kwds)

		self._transfer = None
		self._lvm_volume = None


	def _ensure(self):
		# snap should be applied after layout: download and extract data.
		# this could be done on already ensured volume. 
		# Example: resync slave data

		if not self._lvm_volume:
			if isinstance(self.disk, basestring) and \
					self.disk.startswith('/dev/sd'):
				self.disk = storage2.volume(
						type='ec2_ephemeral', 
						name='ephemeral0')
			self._lvm_volume = storage2.volume(
					type='lvm',
					pvs=[self.disk],
					size=self.size + 'VG',
					vg=self.vg,
					name='data')

		self._lvm_volume.ensure()
		self.device = self._lvm_volume.device

		if self.snap:
			self.snap = storage2.snapshot(self.snap)
			self.mkfs()
			tmp_mpoint = not self.mpoint
			if tmp_mpoint:
				tmp_mpoint = tempfile.mkdtemp()
				self.mpoint = tmp_mpoint

			transfer = cloudfs.LargeTransfer(self.snap.path, self.mpoint + '/')
			try:
				self.mount()
				if hasattr(self.snap, 'size'):
					df_info = filetool.df()
					df = filter(lambda x: x.mpoint == self.mpoint, df_info)[0]
					if df.free < self.snap.size:
						raise storage2.StorageError('Not enough free space'
								' on device %s to restore snapshot.' %
								self.device)

				transfer.run()
			except:
				e = sys.exc_info()[1]
				raise storage2.StorageError("Snapshot restore error: %s" % e)
			finally:
				try:
					self.umount()
				finally:
					if tmp_mpoint:
						self.mpoint = None
						os.rmdir(tmp_mpoint)

			self.snap = None


	def _snapshot(self, description, tags, **kwds):
		lvm_snap = self._lvm_volume.lvm_snapshot(size='100%FREE')
		try:
			snap = storage2.snapshot(type='eph')
			snap.path = os.path.join(os.path.join(
							self.cloudfs_dir, snap.id + '.manifest.ini'))

			lvm_snap_vol = storage2.volume(
							device=lvm_snap.device,
							mpoint=tempfile.mkdtemp())
			lvm_snap_vol.ensure(mount=True)

			df_info = filetool.df()
			df = filter(lambda x: x.mpoint == lvm_snap_vol.mpoint, df_info)

			snap.size = df[0].used

			try:
				transfer = cloudfs.LargeTransfer(
								src=lvm_snap_vol.mpoint + '/',
								dst=snap.path,
								tar_it=True,
								gzip_it=True,
								tags=tags)
				transfer.run()
			finally:
				lvm_snap_vol.umount()
				os.rmdir(lvm_snap_vol.mpoint)
		finally:
			lvm_snap.destroy()

		return snap


	def _destroy(self, force, **kwds):
		self._lvm_volume.destroy(force=force)
		self.device = None


	def _detach(self, force, **kwds):
		self._lvm_volume.detach(force=force, **kwds)


class EphSnapshot(base.Snapshot):

	def _destroy(self):
		self._check_attr('path')
		scheme = urlparse.urlparse(self.path).scheme
		storage_drv = cloudfs.cloudfs(scheme)

		base_url = os.path.dirname(self.path)
		manifest_path = tempfile.mktemp()
		try:
			with open(manifest_path, 'w') as f:
				storage_drv.get(self.path, f)

			c = metaconf.Configuration('ini')
			c.read(manifest_path)
			for chunk in c.children('./chunks/'):
				chunk_path = os.path.join(base_url, chunk)
				storage_drv.delete(chunk_path)
			storage_drv.delete(self.path)
			self.path = None
		finally:
			os.remove(manifest_path)


	def _status(self):
		return self.UNKNOWN


class EphVolumeAdapter(EphVolume):
	
	def __init__(self, **kwds):
		super(EphVolumeAdapter, self).__init__(**kwds)
		self.snap_backend = self._config.pop('cloudfs_dir')
		self._eph_vol = None
		self._eph_pvd = old_eph.EphVolumeProvider()
		
		
	def _ensure(self):
		if self.snap:
			config = self.snap \
					if isinstance(self.snap, dict) \
					else self.snap.config()
		else:	
			config = self.config()
		disk = storage2.volume(config['disk'])
		if disk.device and disk.device.startswith('/dev/sd'):
			disk = storage2.volume(
					type='ec2_ephemeral', 
					name='ephemeral0')
		disk.ensure()
		self.disk = config['disk'] = disk

		if self.snap:
			if self._eph_vol:
				self._eph_vol.detach(force=True)
			self._eph_vol = self._eph_pvd.create_from_snapshot(**config)
			self.snap = None
		else:
			self._eph_vol = self._eph_pvd.create(**config)
		
		self.device = self._eph_vol.device
		
		
	def _snapshot(self, description, tags, **kwds):
		conf = self._eph_vol.config()
		del conf['id']
		eph_snap = self._eph_pvd.snapshot_factory(description, **conf)		
		eph_snap = self._eph_pvd.create_snapshot(self._eph_vol, eph_snap, **kwds)
		
		snap = storage2.snapshot(type='eph')
		snap._config.update(eph_snap.config())
		snap._eph_pvd = self._eph_pvd
		return snap
	
	
	def _destroy(self, force, **kwds):
		self._eph_pvd.destroy(self._eph_vol, force, **kwds)	
	

	def _detach(self, force, **kwds):
		self._eph_pvd.detach(self._eph_vol, force)


class EphSnapshotAdapter(base.Snapshot):
	_eph_pvd = None
	
	def _status(self):
		return self._eph_pvd.get_snapshot_state(self)
		

#storage2.volume_types['eph'] = EphVolume
storage2.volume_types['eph'] = EphVolumeAdapter
#storage2.snapshot_types['eph'] = EphSnapshot
storage2.snapshot_types['eph'] = EphSnapshotAdapter

