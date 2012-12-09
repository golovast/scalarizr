from __future__ import with_statement

__author__ = 'Nick Demyanchuk'

import os
import re
import base64
import logging
import tempfile

from scalarizr import storage2, util
from scalarizr.linux import mdadm, lvm2, coreutils
from scalarizr.storage2.volumes import base


LOG = logging.getLogger(__name__)


class RaidVolume(base.Volume):


	lv_re = re.compile(r'Logical volume "([^\"]+)" created')

	
	def __init__(self, 
				disks=None, raid_pv=None, level=None, lvm_group_cfg=None, 
				vg=None, pv_uuid=None, **kwds):
		'''
		:type disks: list
		:param disks: Raid disks

		:type raid_pv: string
		:param raid_pv: Raid device name (e.g: /dev/md0)

		:type level: int
		:param level: Raid level. Valid values are
			* 0
			* 1
			* 5
			* 10

		:type lvm_group_cfg: string
		:param lvm_group_cfg: LVM volume group configuration (base64 encoded)

		:type vg: string
		:param vg: LVM volume group name

		:type pv_uuid: string
		:param pv_uuid: Mdadm device physical volume id
		'''
		# Backward compatibility with old storage
		if vg is not None:
			vg = os.path.basename(vg)
		self._v1_compat = False
		super(RaidVolume, self).__init__(disks=disks or [],
				raid_pv=raid_pv, level=level and int(level), 
				lvm_group_cfg=lvm_group_cfg,
				vg=vg, pv_uuid=pv_uuid, **kwds)


	def _ensure(self):
		self._v1_compat = self.snap and len(self.snap['disks']) and \
						isinstance(self.snap['disks'][0], dict) and \
						'snapshot' in self.snap['disks'][0]
		if self.snap:
			disks = []
			try:
				# @fixme: pv should be based on a disk config with snapshot taste
				# @todo: create disks concurrently
				for disk_snap in self.snap['disks']:
					if self._v1_compat:
						disk_snap = disk_snap['snapshot']
					snap = storage2.snapshot(disk_snap)
					disks.append(snap.restore())
			except:
				for disk in disks:
					disk.destroy()
				raise

			self.disks = disks

			if self._v1_compat:
				# is some old snapshots /dev/vgname occured
				self.vg = os.path.basename(self.snap['vg'])
			else:
				self.vg = self.snap['vg']
			self.level = int(self.snap['level'])
			self.pv_uuid = self.snap['pv_uuid']
			self.lvm_group_cfg = self.snap['lvm_group_cfg']

			self.snap = None

		self._check_attr('level')
		self._check_attr('vg')
		self._check_attr('disks')

		assert int(self.level) in (0,1,5,10),\
									'Unknown raid level: %s' % self.level

		disks = []
		for disk in self.disks:
			disk = storage2.volume(disk)
			disk.ensure()
			disks.append(disk)
		self.disks = disks

		disks_devices = [disk.device for disk in self.disks]

		if self.lvm_group_cfg:
			try:
				raid_device = mdadm.mdfind(*disks_devices)
			except storage2.StorageError:
				raid_device = mdadm.findname()
				if self.level in (1, 10):
					for disk in disks_devices:
						mdadm.mdadm('misc', None, disk,
									zero_superblock=True, force=True)

					try:
						kwargs = dict(force=True, metadata='default',
									  level=self.level, assume_clean=True,
									  raid_devices=len(disks_devices))
						mdadm.mdadm('create', raid_device, *disks_devices, **kwargs)
					except:
						if self.level == 10 and self._v1_compat:
							self._v1_repair_raid10(raid_device)
						else:
							raise
				else:
					mdadm.mdadm('assemble', raid_device, *disks_devices)

				mdadm.mdadm('misc', raid_device, wait=True)

			# Restore vg config
			vg_restore_file = tempfile.mktemp()
			with open(vg_restore_file, 'w') as f:
				f.write(base64.b64decode(self.lvm_group_cfg))

			# Ensure RAID physical volume
			try:
				lvm2.pvs(raid_device)
			except:
				lvm2.pvcreate(raid_device, uuid=self.pv_uuid, 
							restorefile=vg_restore_file)
			finally:
				lvm2.vgcfgrestore(self.vg, file=vg_restore_file)
				os.remove(vg_restore_file)
				

			# Check that logical volume exists
			lv_infos = lvm2.lvs(self.vg)
			if not lv_infos:
				raise storage2.StorageError(
					'No logical volumes found in %s vol. group')
			lv_name = lv_infos.popitem()[1].lv_name
			self.device = lvm2.lvpath(self.vg, lv_name)

			# Activate volume group
			lvm2.vgchange(self.vg, available='y')

			# Wait for logical volume device file
			util.wait_until(lambda: os.path.exists(self.device),
						timeout=120, logger=LOG,
						error_text='Logical volume %s not found' % self.device)

		else:
			raid_device = mdadm.findname()
			kwargs = dict(force=True, level=self.level, assume_clean=True,
						  raid_devices=len(disks_devices), metadata='default')
			mdadm.mdadm('create', raid_device, *disks_devices, **kwargs)
			mdadm.mdadm('misc', raid_device, wait=True)

			lvm2.pvcreate(raid_device, force=True)
			self.pv_uuid = lvm2.pvs(raid_device)[raid_device].pv_uuid

			lvm2.vgcreate(self.vg, raid_device)

			out, err = lvm2.lvcreate(self.vg, extents='100%FREE')[:2]
			try:
				clean_out = out.strip().split('\n')[-1].strip()
				vol = re.match(self.lv_re, clean_out).group(1)
				self.device = lvm2.lvpath(self.vg, vol)
			except:
				e = 'Logical volume creation failed: %s\n%s' % (out, err)
				raise Exception(e)

			self.lvm_group_cfg = lvm2.backup_vg_config(self.vg)

		self.raid_pv = raid_device


	def _v1_repair_raid10(self, raid_device):
		'''
		Situation is the following:
		raid10 creation from the only half of snapshots failed.
		'''
		disks_devices = [disk.device for disk in self.disks]
		missing_devices = disks_devices[::2]
		md0_devices = [disks_devices[i] if i % 2 else 'missing' \
						for i in range(0, len(disks_devices))]
		

		# Stop broken raid
		if os.path.exists('/dev/md127'):
			mdadm.mdadm('stop', '/dev/md127', force=True)

		# Create raid with missing disks
		kwargs = dict(force=True, metadata='default',
					  level=self.level, assume_clean=True,
					  raid_devices=len(disks_devices))
		mdadm.mdadm('create', raid_device, *md0_devices, **kwargs)
		mdadm.mdadm('misc', raid_device, wait=True)

		# Add missing devices one by one
		for device in missing_devices:
			mdadm.mdadm('add', raid_device, device)
		mdadm.mdadm('misc', raid_device, wait=True)


	def _detach(self, force, **kwds):
		self.lvm_group_cfg = lvm2.backup_vg_config(self.vg)
		lvm2.vgremove(self.vg, force=True)
		self.device = None
		lvm2.pvremove(self.raid_pv, force=True)

		mdadm.mdadm('misc', None, self.raid_pv, stop=True, force=True)
		try:
			mdadm.mdadm('manage', None, self.raid_pv, remove=True, force=True)
		except (Exception, BaseException), e:
			if not 'No such file or directory' in str(e):
				raise

		try:
			os.remove(self.raid_pv)
		except:
			pass

		self.raid_pv = None

		for disk in self.disks:
			disk.detach(force=force)


	def _snapshot(self, description, tags, **kwds):
		coreutils.sync()
		lvm2.dmsetup('suspend', self.device)
		try:
			description = 'Raid%s disk ${index}%s' % (self.level, \
							'. %s' % description if description else '')
			disks_snaps = storage2.concurrent_snapshot(
				volumes=self.disks,
				description=description,
				tags=tags, **kwds
			)

			return storage2.snapshot(
				type='raid',
				disks=disks_snaps,
				lvm_group_cfg=lvm2.backup_vg_config(self.vg),
				level=self.level,
				pv_uuid=self.pv_uuid,
				vg=self.vg
			)
		finally:
			lvm2.dmsetup('resume', self.device)


	def _destroy(self, force, **kwds):
		remove_disks = kwds.get('remove_disks')
		if remove_disks:
			for disk in self.disks:
				disk.destroy(force=force)
			self.disks = []


class RaidSnapshot(base.Snapshot):

	def __init__(self, **kwds):
		super(RaidSnapshot, self).__init__(**kwds)
		self.disks = map(storage2.snapshot, self.disks)


	def _destroy(self):
		for disk in self.disks:
			disk.destroy()


	def _status(self):
		if all((snap.status() == self.COMPLETED for snap in self.disks)):
			return self.COMPLETED
		elif any((snap.status() == self.FAILED for snap in self.disks)):
			return self.FAILED
		elif any((snap.status() == self.IN_PROGRESS for snap in self.disks)):
			return self.IN_PROGRESS
		return self.UNKNOWN


storage2.volume_types['raid'] = RaidVolume
storage2.snapshot_types['raid'] = RaidSnapshot
