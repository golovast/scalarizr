'''
Created on Aug 27, 2012

@author: marat
'''

from __future__ import with_statement

import re
import collections

from scalarizr import linux


class NoFileSystem(linux.LinuxError):
	pass


_MountEntry = collections.namedtuple('_MountEntry', 
				'device mpoint fstype options dump fsck_order')


class _Mounts(object):
	filename = '/proc/mounts'	
	_entries = None
	_entry_re = None

	
	def __init__(self, filename=None):
		if filename:
			self.filename = filename
		self._entry_re = re.compile("\\s+")
		self._reload()


	def _reload(self):
		self._entries = []
		for line in open(self.filename):
			if line[0] != "#":
				m = filter(None, self._entry_re.split(line))
				if m:
					self._entries.append(_MountEntry(*m))

	
	def __getitem__(self, device_or_mpoint):
		self._reload()
		for entry in self._entries:
			if entry.device == device_or_mpoint or \
				entry.mpoint == device_or_mpoint:
				return entry
		raise KeyError(device_or_mpoint)


	def __contains__(self, device_or_mpoint):
		self._reload()
		return any([entry.device == device_or_mpoint or 
				entry.mpoint == device_or_mpoint 
				for entry in self._entries])
		

class _Fstab(_Mounts):
	filename = '/etc/fstab'


	def add(self, device, mpoint, fstype, options='auto', dump=0, fsck_order=0):
		with open(self.filename, 'a+') as fp:
			line = ' '.join(map(str, (device, mpoint, fstype, options, dump, fsck_order)))
			fp.write('\n')
			fp.write(line)
			
	def remove(self, device_or_mpoint):
		raise NotImplementedError()
		

def mounts():
	return _Mounts()


def fstab():
	return _Fstab()	


def mount(device, mpoint, *short_args, **long_kwds):
	args = linux.build_cmd_args(
		executable='/bin/mount',
		short=short_args, 
		long=long_kwds, 
		params=(device, mpoint)
	)
	try:
		msg = 'Cannot mount %s -> %s' % (device, mpoint)
		linux.system(args, error_text=msg)
	except linux.LinuxError, e:
		if 'you must specify the filesystem type' in e.err:
			raise NoFileSystem(device)
		raise


def umount(device_or_mpoint, **long_kwds):
	args = linux.build_cmd_args(
			executable='/bin/umount',
			short=('-f', device_or_mpoint),
			long=long_kwds)
	try:
		linux.system(args, error_text='Cannot umount %s' % device_or_mpoint)
	except linux.LinuxError, e:
		if 'not mounted' in e.err or 'not found' in e.err:
			return
		raise

