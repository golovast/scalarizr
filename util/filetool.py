'''
Created on Jun 22, 2010

@author: marat
'''
from __future__ import with_statement


from scalarizr.util import system2
from scalarizr.util import disttool
import os
import shutil
import pwd
import math
import logging
import time

try:
	from collections import namedtuple
except ImportError:
	from scalarizr.externals.collections import namedtuple


from threading import local



class ConfigurationFile(object):

	BACKUP_BASE_DIR = '/var/lib/scalarizr/backup'
	
	def __init__(self, path):
		self.path = path
		self.backup_dir = os.path.join(self.BACKUP_BASE_DIR, self.path[1:])
		self.local = local()
		
	def __str__(self):
		raise NotImplementedError()

	def __enter__(self):
		self.local.last_trans_id = str(time.time())
		if not os.path.exists(self.backup_dir):
			os.makedirs(self.backup_dir)
		shutil.copy(self.path, os.path.join(self.backup_dir, self.local.last_trans_id))
		with open(self.path, 'w+') as fp:
			fp.write(str(self))
		return self

	def __exit__(self, *args):
		if args[0] and hasattr(self.local, 'last_trans_id'):
			backup_path = os.path.join(self.backup_dir, self.local.last_trans_id)
			shutil.copy(backup_path, self.path)

	def trans(self, enter=None, exit=None):
		#raise NotImplementedError()
		return self

	def reload(self):
		#raise NotImplementedError()
		pass

BUFFER_SIZE = 1024 * 1024	# Buffer size in bytes.
PART_SUFFIX = '.part.'	

def split(filename, part_name_prefix, chunk_size, dest_dir):
	logger = logging.getLogger(__name__)
	f = None
	try:
		try:
			f = open(filename, "rb")
		except (OSError,IOError):
			logger.error("Cannot open file to split '%s'", filename)
			raise
		
		# Create the part file upfront to catch any creation/access errors
		# before writing out data.
		num_parts = int(math.ceil(float(os.path.getsize(filename))/chunk_size))
		part_names = []
		logger.debug("Splitting file '%s' into %d chunks", filename, num_parts)
		for i in range(num_parts):
			part_name_suffix = PART_SUFFIX + str(i).rjust(2, "0")
			part_name = part_name_prefix + part_name_suffix
			part_names.append(part_name)
			
			part_filename = os.path.join(dest_dir, part_name)
			try:
				touch(part_filename)
			except OSError:
				logger.error("Cannot create part file '%s'", part_filename)
				raise
					
		# Write parts to files.
		for part_name in part_names:
			part_filename = os.path.join(dest_dir, part_name)
			cf = open(part_filename, "wb")
			try:
				logger.debug("Writing chunk '%s'", part_filename)
				_write_chunk(f, cf, chunk_size)
			except OSError:
				logger.error("Cannot write chunk file '%s'", part_filename)
				raise
			
		return part_names
	finally:
		if f is not None:
			f.close()

	
def _write_chunk(source_file, chunk_file, chunk_size):
	bytes_written = 0  # Bytes written.
	bytes_left = chunk_size	# Bytes left to write in this chunk.
	
	while bytes_left > 0:
		size = BUFFER_SIZE if BUFFER_SIZE < bytes_left else bytes_left
		buf = source_file.read(size)
		chunk_file.write(buf)
		bytes_written += len(buf)
		bytes_left = chunk_size - bytes_written
		if len(buf) < size:
			bytes_left = 0 # EOF


def touch(filename):
	open(filename, "w+").close()

def truncate(filename):
	f = open(filename, "w+")
	f.truncate(0)
	f.close()

def remove(filename):
	if os.path.exists(filename):
		os.remove(filename)

def read_file(filename, msg = None, error_msg="Cannot read from ", logger = None):
	if not logger:
		logger = logging.getLogger(__name__)
	
	logger.debug(msg) if msg else logger.debug("Reading file %s", filename)

	file = None
	data = None	
	if os.path.isfile(filename):
		try:
			file = open(filename,'r')
			data = file.read()
			return data
		except IOError, e:
			logger.error(error_msg, filename, " : ", str(e))
			return None
		finally:
			if file and not file.closed:
				file.close()
				
	else:
		logger.error("File %s does not exist", filename)
		return None


def write_file(filename, data, mode = 'w', msg = None, error_msg="Cannot write to ", logger = None):
	if not logger:
		logger = logging.getLogger(__name__)
	logger.debug(msg) if msg else logger.debug("Writing file %s", filename)
	if not os.path.isfile(filename):
		#logger.debug("File %s does not exist. Trying to create.", filename)
		dir = os.path.dirname(filename)
		if not os.path.isdir(dir):
			logger.debug("Parent directory %s does not exist. Creating", dir)
			os.makedirs(dir)

	file = None
	try:
		file = open(filename, mode)
		file.write(data)
		return True
	except IOError, e:
		logger.error('%s %s : %s', error_msg, filename, e)
	finally:
		if file and not file.closed:
			file.close()
		
	return False


class Rsync(object):
	"""
	Wrapper for rsync
	"""
	
	EXECUTABLE = "rsync"
	_options = None
	_src = None
	_dst = None
	_executable = None
	_quiet = None
	
	def __init__(self, executable=None):
		self._executable = executable if executable is not None else Rsync.EXECUTABLE
		self._options = []
		self._src = self._dst = None
		self._quiet = False
		
	def archive(self):
		self._options.append('-rlpgoD')
		return self
	
	def times(self):
		self._options.append('-t')
		return self
	
	def recursive(self):
		self._options.append('-r')
		return self
		
	def sparse(self):
		self._options.append('-S')
		return self

	def links(self):
		self._options.append('-l')
		return self
	
	def verbose(self):
		self._options.append('-v')
		return self
		
	def dereference(self):
		self._options.append('-L')
		return self
	
	def xattributes(self):
		self._options.append('-X')
		return self

	def hardlinks(self):
		self._options.append('-H')
		return self

	def exclude(self, files):
		for file in files:
			self._options.append('--exclude=%s' % file)
		return self

	def version(self):
		self._options.append("--version")
		return self
	
	def delete(self):
		self._options.append("--delete")
		return self
		
	def source(self, path):
		self._src = path
		return self
		
	def dest(self, path):
		self._dst = path
		return self
		
	def quietly(self):
		self._quiet = True
		return self
	
	def _sync(self):
		system2(['sync'])
	
	def execute(self):
		self._sync()
		rsync_cmd = [self._executable] + self._options + [self._src, self._dst]
		out, err, returncode = system2(rsync_cmd, raise_exc=False)
		self._sync()
		return out, err, returncode
		
	def __str__(self):
		ret = "sync && %(executable)s %(options)s %(src)s %(dst)s %(quiet)s" % dict(
			executable=self._executable,
			options=" ".join(self._options),
			src=self._src,
			dst=self._dst,
			quiet="2>&1 > /dev/null && sync" if self._quiet else "&& sync"
		)
		return ret.strip()

	@staticmethod
	def usable():
		"""
		@todo: implement
		"""
		return True


class Tar:
	EXECUTABLE = "/usr/sfw/bin/gtar" if disttool.is_sun() else "tar"
	
	_executable = None
	_options = None
	_files = None
	
	def __init__(self, executable=None):
		self._executable = executable if executable is not None else self.EXECUTABLE
		self._options = []
		self._files = []
		
	def version(self):
		self._options.append("--version")
		return self
	
	def verbose(self):
		self._options.append("-v")
		return self
	
	def create(self):
		self._options.append("-c")
		return self
		
	def bzip2(self):
		self._options.append("-j")
		return self

	def diff(self):
		self._options.append("-d")
		return self
		
	def gzip(self):
		self._options.append("-z")
		return self

	def extract(self):
		self._options.append("-x")
		return self

	def update(self):
		self._options.append("-u")
		return self

	def sparse(self):
		self._options.append("-S")
		return self

	def dereference(self):
		self._options.append("-h")
		return self

	def archive(self, filename):
		self._options.append("-f " + filename if filename is not None else "-")
		return self
	
	def chdir(self, dir):
		self._options.append("-C " + dir)
		return self
	
	def add(self, filename, dir=None):
		item = filename if dir is None else "-C "+dir+" "+filename
		self._files.append(item)
		return self
	
	def __str__(self):
		ret = "%(executable)s %(options)s %(files)s" % dict(
			executable=self._executable,
			options=" ".join(self._options),
			files=" ".join(self._files)
		)
		return ret.strip()
	
_dfrow = namedtuple('df', 'device, size, used, free, mpoint')

def _parse_int(value):
	try:
		return int(value)
	except ValueError:
		return None

def df():
	out = system2(('df', '-Pk'))[0]
	return [_dfrow(
				line[0], _parse_int(line[1]), _parse_int(line[2]), 
				_parse_int(line[3]), line[-1]) 
				for line in map(str.split, out.splitlines()[1:])]



def rchown(user, path):
	#log "chown -r %s %s" % (user, path)
	user = pwd.getpwnam(user)	
	os.chown(path, user.pw_uid, user.pw_gid)
	try:
		for root, dirs, files in os.walk(path):
			for dir in dirs:
				os.chown(os.path.join(root , dir), user.pw_uid, user.pw_gid)
			for file in files:
				if os.path.exists(os.path.join(root, file)): #skipping dead links
					os.chown(os.path.join(root, file), user.pw_uid, user.pw_gid)
	except OSError, e:
		#log 'Cannot chown directory %s : %s' % (path, e)	
		pass