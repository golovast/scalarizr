'''
Created on May 10, 2010

@author: marat
@author: Dmytro Korsakov
@deprecated: scalarizr.linux.mount is replacement.
@ported 
'''

from scalarizr.util import disttool, system2
import subprocess
import re
import os


class FstoolError(BaseException):
	NO_FS = -100
	CANNOT_MOUNT = -101
	CANNOT_UMOUNT = -102
	CANNOT_CREATE_FS = -103
	
	message = None
	code = None
	
	def __init__(self, *args):
		BaseException.__init__(self, *args)
		self.message = args[0]
		try:
			self.code = args[1]
		except IndexError:
			pass


class Fstab:
	'''
	Utility for /etc/fstab
	@see http://www.debianhelp.co.uk/fstab.htm
	'''
	LOCATION = None
	filename = None	
	_entries = None
	_re = None
	loaded = False
	
	def __init__(self, filename=None, autoload=False):
		self.filename = filename if not filename is None else self.LOCATION
		self._entries = []
		self._re = re.compile("\\s+")
		if autoload:
			self.reload()
		
	def reload(self):
		self._entries = []
		f = open(self.filename, "r")

		for line in f:
			if line[0:1] == "#":
				continue
			m = filter(None, self._re.split(line))
			if m: 
				if len(m) < 6:
					m += [None] * (6 - len(m))
				self._entries.append(TabEntry(*m))
		f.close()
		self.loaded = True
		
	def list_entries(self, reload=False):
		if not self._entries or reload:
			self.reload()
		return list(self._entries)

	def contains(self, devname=None, mpoint=None, reload=False):
		eq = dict(devname=devname, mpoint=mpoint)
		for entry in self.list_entries(reload):
			if self._cmp(entry, eq):
				return True
		return False
			
	def _cmp(self, entry, eq):
		return all(list(getattr(entry, k) == v for k, v in eq.items() if v))	
		
	def find(self, devname=None, mpoint=None, fstype=None, reload=False):
		eq = dict(devname=devname, mpoint=mpoint, fstype=fstype)
		return list(entry for entry in self.list_entries(reload) if self._cmp(entry, eq))

	def append(self, devname, mpoint, fstype="auto", options="defaults", dump=0, fsckorder=0, autosave=True):
		if not self._entries:
			self.reload()
		self._entries.append(TabEntry(devname, mpoint, fstype, options, dump, fsckorder))
		if autosave:
			self.save()
	
	def remove(self, devname=None, mpoint=None, fstype=None, reload=False, autosave=True):
		ent = self.find(devname, mpoint, fstype, reload)
		if len(ent):
			self._entries.remove(ent[0])
			if autosave:
				self.save()
			return True
		return False
	
	def save(self):
		fp = None
		try:
			fp = open(self.filename, "w")
			fp.write(str(self))
		finally:
			if fp:
				fp.close()
		
	
	def __str__(self):
		lens = [[], [], [], [], [], []]
		if not self.loaded:
			self.reload()
		
		# Calculate length of each cell
		for entry in self._entries:
			i = 0
			for val in entry.values():
				lens[i].append(len(str(val)))
				i += 1
				
		# Calculate max length		
		lens = list(max(v) for v in lens)		
		
		# Build beautiful fstab
		ret = ''
		for entry in self._entries:
			i = 0
			for val in entry.values():
				ret += val + ' '*(lens[i] - len(val) + 2)
				i += 1
			ret += '\n'
			
		return ret
	

class Mtab(Fstab):
	'''
	Utility for /etc/mtab
	'''
	LOCAL_FS_TYPES = None	

		
class TabEntry(object):
	devname = None
	mpoint = None
	fstype = None
	options = None
	dump = None
	fsckorder = None	
	
	def __init__(self, devname, mpoint, fstype='auto', options='default', dump=0, fsckorder=0):
		self.devname = devname
		self.mpoint = mpoint
		self.fstype = fstype
		self.options = options
		self.dump = dump
		self.fsckorder = fsckorder
		
	def values(self):
		ret = [self.devname, self.mpoint, self.fstype, self.options, \
			 self.dump if self.dump is not None else '', \
			 self.fsckorder if self.fsckorder is not None else '']
		return ['%s' % (v,) for v in ret]
	
		
	def __str__(self):
		return "%s\t%s\t%s\t%s\t%s\t%s" % (
			self.devname, self.mpoint, self.fstype, self.options, 
			self.dump if self.dump is not None else '', 
			self.fsckorder if self.fsckorder is not None else '' 
		)

		
if disttool.is_linux():
	Fstab.LOCATION = "/etc/fstab"	
	Mtab.LOCATION = "/etc/mtab"
	Mtab.LOCAL_FS_TYPES = ('ext2', 'ext3', 'xfs', 'jfs', 'reiserfs', 'tmpfs', 'sysfs', 'proc')
	
elif disttool.is_sun():
	Fstab.LOCATION = "/etc/vfstab"	
	Mtab.LOCATION = "/etc/mnttab"
	Mtab.LOCAL_FS_TYPES = ('ext2', 'ext3', 'xfs', 'jfs', 'reiserfs', 'tmpfs', 
		'ufs', 'sharefs', 'dev', 'devfs', 'ctfs', 'mntfs',
		'proc', 'lofs',   'objfs', 'fd', 'autofs')
	

def mount (device, mpoint = '/mnt', options=None, make_fs=False, fstype='ext3', auto_mount=False):
	if not os.path.exists(mpoint):
		os.makedirs(mpoint)
	
	options = options or ('-t', 'auto')
	
	if make_fs:
		mkfs(device,fstype)
	
	out, code = system2(['mount'] + list(options) + [device, mpoint], stderr=subprocess.STDOUT, raise_error=False)[0::2]
	if code:
		if out.find("you must specify the filesystem type") != -1:
			raise FstoolError("No filesystem found on device '%s'" % (device), FstoolError.NO_FS)
		raise FstoolError(out)
	
	if " ".join(options).find("loop") == -1:
		mtab = Mtab()		
		if not mtab.contains(device):
			raise FstoolError("Cannot mount device '%s'. %s" % (device, out), FstoolError.CANNOT_MOUNT)
	
	if auto_mount:
		fstab = Fstab()
		if not fstab.contains(device, mpoint=mpoint, reload=True):
			opts = "defaults"
			if disttool.is_ubuntu() and disttool.version_info() >= (10, 4):
				opts += ',comment=cloudconfig,nobootwait'
			fstab.append(device, mpoint, options=opts)

def umount(device=None, mpoint=None, options=None, clean_fstab = False):
	dev = device or mpoint
	if not os.path.exists(dev):
		raise FstoolError("Path doesn't exists %s" % (dev), FstoolError.CANNOT_UMOUNT)
	
	options = options or ()
	
	out, returncode = system2(['umount']+list(options)+[dev], stderr=subprocess.STDOUT, raise_error=False)[0::2]
	if returncode and not 'not mounted' in out:
		raise FstoolError("Cannot unmount %s. %s" % (dev, out), FstoolError.CANNOT_UMOUNT)
	
	if clean_fstab:
		fstab = Fstab()
		fstab.remove(device, mpoint)
	
	
def mkfs(device, fstype = 'ext3'):
	out, retcode = system2(['/sbin/mkfs', '-t', fstype, '-F', device], stderr=subprocess.STDOUT, raise_error=False)[0::2]
	if retcode:
		raise FstoolError("Cannot create file system on device '%s'. %s" % (device, out), 
				FstoolError.CANNOT_CREATE_FS)
		
def get_mysql_device():
	import string
	o_z = string.ascii_lowercase[14:]
	for letter in o_z:
		device_name = 'dev/sd'+letter
		if not os.path.isfile(device_name):
			return device_name
	return None
