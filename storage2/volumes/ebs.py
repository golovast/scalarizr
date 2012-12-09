from __future__ import with_statement

import sys
import os
import glob
import string
import logging
import threading

import boto.ec2.snapshot
import boto.ec2.volume
import boto.exception

from scalarizr import linux
from scalarizr import storage2
from scalarizr import util
from scalarizr.node import __node__
from scalarizr.storage2.volumes import base
from scalarizr.linux import coreutils


LOG = logging.getLogger(__name__)


def name2device(name):
	if name.startswith('/dev/xvd'):
		return name
	if storage2.RHEL_DEVICE_ORDERING_BUG or os.path.exists('/dev/xvda1'):
		name = name.replace('/sd', '/xvd')
	if storage2.RHEL_DEVICE_ORDERING_BUG:
		name = name[0:8] + chr(ord(name[8])+4) + name[9:]
	return name


def device2name(device):
	if device.startswith('/dev/sd'):
		return device
	elif storage2.RHEL_DEVICE_ORDERING_BUG:
		device = device[0:8] + chr(ord(device[8])-4) + device[9:]
	return device.replace('/xvd', '/sd')


class FreeDeviceLetterMgr(object):

	def __init__(self):
		# Workaround: rhel 6 returns "Null body" when attach to /dev/sdf
		s = 7 if linux.os['release'] and linux.os.redhat else 5
		self._all = list(string.ascii_lowercase[s:16])
		self._acquired = set()
		self._lock = threading.Lock()
		self._local = threading.local()	

	
	def __enter__(self):
		with self._lock:
			detached = __node__['ec2']['t1micro_detached_ebs'] or list()
			detached = set(name[-1] for name in detached)
		letters = list(set(self._all) - self._acquired - detached)
		for l in letters:
			pattern = name2device('/dev/sd' + l) + '*'
			if not glob.glob(pattern):
				with self._lock:
					if not l in self._acquired:
						self._acquired.add(l)
						self._local.letter = l
						return self
		msg = 'No free letters for block device name remains'
		raise storage2.StorageError(msg)

	def get(self):
		return self._local.letter
	
	def __exit__(self, *args):
		if hasattr(self._local, 'letter'):
			self._acquired.remove(self._local.letter)
			del self._local.letter


class EbsMixin(object):
	
	_conn = None	
	
	def __init__(self):
		self.error_messages.update({
			'no_connection': 'EC2 connection should be available '
							'to perform this operation'								
		})

	
	def _ebs_snapshot(self, snapshot):
		if isinstance(snapshot, basestring):
			ret = boto.ec2.snapshot.Snapshot(self._conn)
			ret.id = snapshot
			return ret
		return snapshot

	
	def _ebs_volume(self, volume):
		if isinstance(volume, basestring):
			ret = boto.ec2.volume.Volume(self._conn)
			ret.id = volume
			return ret
		return volume


	def _check_ec2(self):
		self._check_attr('id')
		self._conn = self._connect_ec2()
		assert self._conn, self.error_messages['no_connection']


	def _connect_ec2(self):
		try:
			return __node__['ec2']['connect_ec2']()
		except:
			if sys.exc_type.__name__ not \
				in ('AttributeError', 'NoAuthHandlerFound'):
				raise
		


	def _avail_zone(self):
		return __node__['ec2']['avail_zone']


	def _instance_id(self):
		return __node__['ec2']['instance_id']


	def _instance_type(self):
		return __node__['ec2']['instance_type']


class EbsVolume(base.Volume, EbsMixin):
	
	_free_device_letter_mgr = FreeDeviceLetterMgr()
	_global_timeout = 3600
	
	def __init__(self, 
				name=None,
				avail_zone=None,
				size=None,
				volume_type='standard',
				iops=None,
				**kwds):
		base.Volume.__init__(self, name=name, avail_zone=avail_zone, 
				size=size and int(size) or None, 
				volume_type=volume_type, iops=iops, **kwds)	
		EbsMixin.__init__(self)
		self.error_messages.update({
			'no_id_or_conn': 'Volume has no ID and EC2 connection '
							'required for volume construction is not available'
		})	

		
	def _clone(self, config):
		config.pop('device', None)
		config.pop('avail_zone', None)

		
	def _ensure(self):
		'''
		Algo:
		
		if id:
			ebs = get volume
			if ebs in different zone:
				create snapshot
				del id
				
		if not id:
			ebs = create volume
		
		if not ebs is in-use by this server:
			if attaching or detaching:
				wait for state change
			if in-use:
				detach volume
			attach volume
		'''
		
		self._conn = self._connect_ec2()
		assert self._conn or self.id, self.error_messages['no_id_or_conn']
		
		if self._conn:
			zone = self._avail_zone()			
			snap = name = None
			size = self.size() if callable(self.size) else self.size
			
			if self.id:
				ebs = self._conn.get_all_volumes([self.id])[0]
				if ebs.zone != zone:
					LOG.warn('EBS volume %s is in the different '
							'availability zone (%s). Snapshoting it '
							'and create a new EBS volume in %s', 
							ebs.id, ebs.zone, zone)
					snap = self._create_snapshot(self.id).id
					self.id = ebs = None
				else:
					size = ebs.size
			elif self.snap:
				snap = self.snap['id']
				if not self.tags:
					self.tags = self.snap.get('tags', {})
			if not self.id:
				ebs = self._create_volume(
						zone=zone, 
						size=self.size, 
						snapshot=snap,
						volume_type=self.volume_type, 
						iops=self.iops,
						tags=self.tags)
				size = ebs.size
		
			if not (ebs.volume_state() == 'in-use' and  \
					ebs.attach_data.instance_id == self._instance_id()):
				if ebs.attachment_state() in ('attaching', 'detaching'):
					self._wait_attachment_state_change(ebs)
				if ebs.attachment_state() == 'attached':
					self._detach_volume(ebs)
				with self._free_device_letter_mgr:
					name = '/dev/sd%s' % self._free_device_letter_mgr.get()
					self._attach_volume(ebs, name)
					
			else:
				name = ebs.attach_data.device
			
			self._config.update({
				'id': ebs.id,
				'name': name,
				'avail_zone': zone,
				'size': size,
				'snap': None
			})
			
		if self.name:
			self.device = name2device(self.name)			
			


	def _snapshot(self, description, tags, **kwds):
		'''
		@type nowait: bool
		@param nowait: Wait for snapshot completion. Default: True
		'''
		
		self._check_ec2()
		snapshot = self._create_snapshot(self.id, description, tags, 
										kwds.get('nowait', True))
		return storage2.snapshot(
				type='ebs', 
				id=snapshot.id, 
				description=snapshot.description,
				tags=tags)


	def _detach(self, force, **kwds):
		self._check_ec2()
		self._detach_volume(self.id, force)
		if self._instance_type() == 't1.micro':
			detached = __node__['ec2']['t1micro_detached_ebs'] or list()
			detached.append(self.name)
			__node__['ec2']['t1micro_detached_ebs'] = detached

	
	def _destroy(self, force, **kwds):
		self._check_ec2()
		self._conn.delete_volume(self.id)

		
	def _create_volume(self, zone=None, size=None, snapshot=None, 
					volume_type=None, iops=None, tags=None):
		LOG.debug('Creating EBS volume (zone: %s size: %s snapshot: %s '
				'volume_type: %s iops: %s)', zone, size, snapshot,
				volume_type, iops) 
		if snapshot:
			self._wait_snapshot(snapshot)
		ebs = self._conn.create_volume(size, zone, snapshot, volume_type, iops)
		LOG.debug('EBS volume %s created', ebs.id)
		
		LOG.debug('Checking that EBS volume %s is available', ebs.id)
		msg = "EBS volume %s is not in 'available' state. " \
				"Timeout reached (%s seconds)" % (
				ebs.id, self._global_timeout)
		util.wait_until(
			lambda: ebs.update() == "available", 
			logger=LOG, timeout=self._global_timeout,
			error_text=msg
		)
		LOG.debug('EBS volume %s available', ebs.id)
		
		if tags:
			try:
				LOG.debug('Applying tags to EBS volume %s (tags: %s)', ebs.id, tags)
				self._conn.create_tags([ebs.id], tags)
			except:
				LOG.warn('Cannot apply tags to EBS volume %s. Error: %s', 
						ebs.id, sys.exc_info()[1])
		return ebs
	
	
	def _create_snapshot(self, volume, description=None, tags=None, nowait=False):
		LOG.debug('Creating snapshot of EBS volume %s', volume)
		coreutils.sync()		
		snapshot = self._conn.create_snapshot(volume, description)
		LOG.debug('Snapshot %s created for EBS volume %s', snapshot.id, volume)
		if tags:
			try:
				LOG.debug('Applying tags to EBS snapshot %s (tags: %s)', 
						snapshot.id, tags)
				self._conn.create_tags([snapshot.id], tags)
			except:
				LOG.warn('Cannot apply tags to EBS snapshot %s. Error: %s', 
						snapshot.id, sys.exc_info()[1])
		if not nowait:
			self._wait_snapshot(snapshot)	
		return snapshot		
	
	
	def _attach_volume(self, volume, device_name=None):
		ebs = self._ebs_volume(volume)
		
		LOG.debug('Attaching EBS volume %s (device: %s)', ebs.id, device_name)
		ebs.attach(self._instance_id(), device_name)
		LOG.debug('Checking that EBS volume %s is attached', ebs.id)
		msg = "EBS volume %s wasn't attached. Timeout reached (%s seconds)" % (
				ebs.id, self._global_timeout)
		util.wait_until(
			lambda: ebs.update() and ebs.attachment_state() == 'attached', 
			logger=LOG, timeout=self._global_timeout,
			error_text=msg
		)
		LOG.debug('EBS volume %s attached', ebs.id)
		
		device = name2device(device_name)
		LOG.debug('EBS device name %s is mapped to %s in operation system', 
				device_name, device)
		LOG.debug('Checking that device %s is available', device)
		msg = 'Device %s is not available in operation system. ' \
				'Timeout reached (%s seconds)' % (
				device, self._global_timeout)
		util.wait_until(
			lambda: os.access(device, os.F_OK | os.R_OK), 
			sleep=1, logger=LOG, timeout=self._global_timeout,
			error_text=msg
		)
		LOG.debug('Device %s is available', device)
	
	
	def _detach_volume(self, volume, force=False):
		ebs = self._ebs_volume(volume)
		LOG.debug('Detaching EBS volume %s', ebs.id)
		try:
			ebs.detach(force)
		except boto.exception.BotoServerError, e:
			if e.code != 'IncorrectState':
				raise
		LOG.debug('Checking that EBS volume %s is available', ebs.id)
		msg = "EBS volume %s is not in 'available' state. " \
				"Timeout reached (%s seconds)" % (
				ebs.id, self._global_timeout)
		util.wait_until(
			lambda: ebs.update() == 'available', 
			logger=LOG, timeout=self._global_timeout,
			error_text=msg
		)
		LOG.debug('EBS volume %s is available', ebs.id)		
	
	
	def _wait_attachment_state_change(self, volume):
		ebs = self._ebs_volume(volume)
		msg = 'EBS volume %s hangs in attaching state. ' \
				'Timeout reached (%s seconds)' % (ebs.id, self._global_timeout)
		util.wait_until(
			lambda: ebs.update() and ebs.attachment_state() != 'attaching',
			logger=LOG, timeout=self._global_timeout,
			error_text=msg
		)
	
	
	def _wait_snapshot(self, snapshot):
		snapshot = self._ebs_snapshot(snapshot)
		LOG.debug('Checking that EBS snapshot %s is completed', snapshot.id)
		msg = "EBS snapshot %s wasn't completed. " \
				"Timeout reached (%s seconds)" % (
				snapshot.id, self._global_timeout)
		util.wait_until(
			lambda: snapshot.update() and snapshot.status != 'pending', 
			logger=LOG,
			error_text=msg
		)
		if snapshot.status == 'error':
			msg = 'Snapshot %s creation failed. AWS status is "error"' % snapshot.id
			raise storage2.StorageError(msg)
		elif snapshot.status == 'completed':
			LOG.debug('Snapshot %s completed', snapshot.id)		


class EbsSnapshot(EbsMixin, base.Snapshot):

	#error_messages = base.Snapshot.error_messages.copy()
	
	_status_map = {
		'pending': base.Snapshot.IN_PROGRESS,
		'completed': base.Snapshot.COMPLETED,
		'error': base.Snapshot.FAILED
	}
	
	
	def __init__(self, **kwds):
		base.Snapshot.__init__(self, **kwds)
		EbsMixin.__init__(self)			

	def _status(self):
		self._check_ec2()
		snapshot = self._ebs_snapshot(self.id)
		snapshot.update()
		return self._status_map[snapshot.status]
	

	def _destroy(self):
		self._check_ec2()
		self._conn.delete_snapshot(self.id)


storage2.volume_types['ebs'] = EbsVolume
storage2.snapshot_types['ebs'] = EbsSnapshot

		




