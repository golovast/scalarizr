from __future__ import with_statement

import re
import os
import sys
import Queue
import urlparse
import itertools
import tempfile
import inspect
import subprocess
import threading
import logging
import ConfigParser
import time
import hashlib
import uuid
from copy import copy
if sys.version_info[0:2] >= (2, 7):
	from collections import OrderedDict
else:
	from scalarizr.externals.collections import OrderedDict
try:
	import json
except ImportError:
	import simplejson as json

from scalarizr import storage2
from scalarizr.libs import bases
from scalarizr.linux import coreutils


LOG = logging.getLogger(__name__)


class BaseTransfer(bases.Task):
	
	def __init__(self, src=None, dst=None, **kwds):
		'''
		:type src: string / generator / iterator
		:param src: Transfer source

		:type dst: string / generator / iterator
		:param dst: Transfer destination
		'''
		if callable(src):
			src = (item for item in src())
		else:
			if not hasattr(src, '__iter__'):
				src = [src]
			src = iter(src)
		if callable(dst):
			dst = (item for item in dst())
		elif not hasattr(dst, '__iter__'):
			dst = itertools.repeat(dst)
		else:
			dst = iter(dst)	
	
		super(BaseTransfer, self).__init__(src=src, dst=dst, **kwds)
		self.define_events('transfer_start', 'transfer_error', 'transfer_complete')
		

class FileTransfer(BaseTransfer):

	_url_re = re.compile(r'^[\w-]+://')

	def __init__(self, num_workers=4, retries=3, multipart=False, **kwds):
		'''
		:type num_workers: int
		:param num_workers: Number of worker threads

		:type retries: int
		:param retries: Max retries to transfer one file

		:type multipart: bool
		:param multipart: Use multipart uploading functionality of the underlying
			driver. :param:`src` should be a chunks iterator / generator

		:event transfer_start: Fired when started transfer of the particular file

		:evtype src: string
		:evparam src: Full source path

		:evtype dst: string
		:evparam dst: Full destination path

		:event transfer_complete: Fired when file transfer complete

		:event transfer_error: Fired when file transfer failed	

		@param: src transfer source path
			- str file or directory path. directory processed recursively
			- list of path strings 
			- generator function that will produce path strings 

		@param: dst transfer destination path
			- str file or directory path
			- list of path strings			
			- generator function that will produce path strings


		# XXX(marat): We should extend class from Observable
		@param: listener function or object to call when file transfer is 
		started, finished, failed, restarted.
			on_start: fn(src, dst, state='start')
			on_complete: fn(src, dst, state='complete', retry=1, transfered_bytes=1892331)
			on_error: fn(src, dst, state='error', retry=1, exc_info=(3 items tuple))

		Examples:

			Upload pathes
				Transfer('/mnt/backups/daily.tar.gz', 's3://backups/mysql/2012-09-05/', 'upload')

			Upload generator
				def files():
					yield 'part.1'
					yield 'part.2'
				Transfer(files, 's3://images/ubuntu12.04.1/', 'upload')

			Download both generators
				def src():
					yield 's3://backups/mysql/daily.tar.gz'
					yield 'rackspace-cloudfiles://backups/mysql/daily.tar.gz'

				def dst():
					yield '/backups/daily-from-s3.tar.gz'
					yield '/backups/daily-from-cloudfiles.tar.gz'


		Usage:
			==========  ==========  ==========  ====================================
			SRC         DST         MULTIPART   EXPECTED INPUT
			==========  ==========  ==========  ====================================
			str         str         -           src: path, dst: path
			iter        str         False       src: paths, dst: path
			iter        str         True        src: chunks of a file, dst: path
			iter        iter        -           src: paths, dst: paths
			==========  ==========  ==========  ====================================

		'''
		if not (not isinstance(kwds["src"], basestring) and
				isinstance(kwds["dst"], basestring)) and multipart:
			multipart = False

		super(FileTransfer, self).__init__(num_workers=num_workers, 
						retries=retries, multipart=multipart, **kwds)

		self._completed = []
		self._failed = []
		self._retries_queue = Queue.Queue()
		self._stop_all = threading.Event()
		self._stopped = threading.Event()
		self._gen_lock = threading.RLock()
		self._worker_lock = threading.Lock()
		self._upload_id = None
		self._chunk_num = -1
		self._multipart_result = None

		
	def _job_generator(self):
		no_more = False
		while True:
			try:
				yield self._retries_queue.get_nowait()
				continue
			except Queue.Empty:
				if no_more:
					raise StopIteration
			try:
				with self._gen_lock:
					src = self.src.next()
					dst = self.dst.next()
					retry = 0
					if self.multipart:
						self._chunk_num += 1
					LOG.debug("FileTransfer yield %s %s %s %s" % (src, dst, retry, self._chunk_num))
					yield src, dst, retry, self._chunk_num
			except StopIteration:
				no_more = True
			
			
	def _is_remote_path(self, path):
		return isinstance(path, basestring) and self._url_re.match(path)
				
				
	def _worker(self):
		driver = None
		for src, dst, retry, chunk_num in self._job_generator():
			self.fire('transfer_start', src, dst, retry, chunk_num)
			try:
				uploading = self._is_remote_path(dst) and os.path.isfile(src)
				downloading = self._is_remote_path(src) and not self._is_remote_path(dst)
				assert not (uploading and downloading)
				assert uploading or downloading

				rem, loc = (dst, src) if uploading else (src, dst)
				if not driver:
					driver = cloudfs(urlparse.urlparse(rem).scheme)
				with self._worker_lock:
					if self.multipart and not self._upload_id:
						chunk_size = os.path.getsize(loc)
						self._upload_id = driver.multipart_init(rem, chunk_size)

				if uploading:
					if self.multipart:
						driver.multipart_put(self._upload_id, chunk_num, src)
					else:
						driver.put(src, dst)
					self._completed.append({
							'src': src,
							'dst': dst,
							'chunk_num': chunk_num,
							'size': os.path.getsize(src)})
				else:
					driver.get(src, dst)
					self._completed.append({
							'src': src,
							'dst': dst,
							'size': os.path.getsize(dst)})
				self.fire('transfer_complete', src, dst, retry, chunk_num)

			except AssertionError:
				self.fire('transfer_error', src, dst, retry, chunk_num, 
							sys.exc_info())
			except:
				retry += 1
				if retry <= self.retries:
					self._retries_queue.put((src, dst, retry, chunk_num))
				else:
					self._failed.append({
							'src': src,
							'dst': dst,
							'exc_info': sys.exc_info()})
				self.fire('transfer_error', src, dst, retry, chunk_num, 
							sys.exc_info())
			finally:
				if self._stop_all.isSet():
					with self._worker_lock:
						if self.multipart and self._upload_id:
							driver.multipart_abort(self._upload_id)
							self._upload_id = None
					break

		with self._worker_lock:
			# 'if driver' condition prevents threads that did nothing from
			# entering (could happen in case num_workers > chunks)
			if driver and self.multipart and self._upload_id:
				self._multipart_result = driver.multipart_complete(self._upload_id)
				self._upload_id = None


	def _run(self):
		self._stop_all.clear()
		self._stopped.clear()
		try:
			# Starting threads
			pool = []
			for n in range(self.num_workers):
				worker = threading.Thread(
							name='transfer-worker-%s' % n, 
							target=self._worker)
				LOG.debug("Starting worker '%s'", worker.getName())
				worker.start()
				pool.append(worker)
			# Join workers
			for worker in pool:
				worker.join()
				LOG.debug("Worker '%s' finished", worker.getName())
			return {
				'completed': self._completed,
				'failed': self._failed,
				'multipart_result': self._multipart_result,
			}
		finally:
			self._stopped.set()


	def kill(self, timeout=None):
		self._stop_all.set()
		self._stopped.wait(timeout)



class LargeTransfer(bases.Task):
	'''
	SQL dump. File-per-database.
	---------------------------

	def src_gen():
		yield stream = mysqldump ${database}
	def dst_gen():
		yield ${database}

	s3://.../${transfer_id}/manifest.json
							${database_1}.gz.00
							${database_1}.gz.01
							${database_2}.gz.00
							${database_2}.gz.01
							${database_2}.gz.02


	Directory backup
	----------------

	src = '/mnt/dbbackup'
	dst = 's3://backup/key1/key2/'

	s3://backup/key1/key2/${transfer_id}/manifest.json
	s3://backup/key1/key2/${transfer_id}/part.gz.00
	s3://backup/key1/key2/${transfer_id}/part.gz.01
	s3://backup/key1/key2/${transfer_id}/part.gz.02


	Directory restore
	-----------------

	src = s3://backup/key1/key2/eph-snap-12345678/manifest.json
	dst = /mnt/dbbackup/

	1. Download manifest 
	2. <chunk downloader> | funzip | tar -x -C /mnt/dbbackup
	'''

	# NOTE: use directory dst for uploading.
	UPLOAD = 'upload'
	DOWNLOAD = 'download'

	pigz_bin = '/usr/bin/pigz'
	gzip_bin = '/bin/gzip'

	def __init__(self, src, dst, direction,
				transfer_id=None,
				tar_it=True,
				gzip_it=True, 
				chunk_size=100, 
				try_pigz=True,
				manifest='manifest.json',
				description='',
				tags=None,
				**kwds):
		'''
		@param src: transfer source path
			- str file or directory path. 
			- file-like object (stream)
			- generator function
		'''
		url_re = re.compile(r'^[\w-]+://')
		if isinstance(src, basestring) and url_re.match(src):
			self._up = False
		elif isinstance(dst, basestring) and url_re.match(dst):
			self._up = True
		else:
			raise ValueError('Eather src or dst should be URL-like string')
		if self._up and os.path.isdir(src) and not tar_it:
			raise ValueError('Passed src is a directory. tar_it=True expected')
		if self._up:
			if callable(src):
				src = (item for item in src())
			else:
				if not hasattr(src, '__iter__'):
					src = [src]
				src = iter(src)
			if callable(dst):
				dst = (item for item in dst())
			elif not hasattr(dst, '__iter__'):
				dst = itertools.repeat(dst)
			else:
				dst = iter(dst)

		super(LargeTransfer, self).__init__()

		self.description = description
		self.tags = tags
		self.multipart = kwds.get("multipart")
		self.direction = direction
		self.src = src
		self.dst = dst
		self.tar_it = tar_it
		self.gzip_it = gzip_it
		self.chunk_size = chunk_size
		self.try_pigz = try_pigz
		if transfer_id is None:
			self.transfer_id = uuid.uuid4().hex
		else:
			self.transfer_id = transfer_id
		self.manifest = manifest
		self._transfer = FileTransfer(src=self._src_generator,
								dst=self._dst_generator, **kwds)
		self._tranzit_vol = storage2.volume(
								type='tmpfs',
								mpoint=tempfile.mkdtemp())
		self._chunk_num = -1
		self._given_chunks = OrderedDict()
		self._restoration_queue = Queue.Queue()
		self._dl_lock = threading.Lock()

		events = self._transfer.list_events()
		self.define_events(*events)
		for ev in events:
			self._transfer.on(ev, self._proxy_event(ev))
		


	def _src_generator(self):
		'''
		Compress, split, yield out
		'''
		if self.direction == self.UPLOAD:
			# Tranzit volume size is chunk for each worker
			# and Ext filesystem overhead

			# manifest is not used in multipart uploads and is left
			# here to avoid writing ifs everywhere
			manifest = Manifest()
			manifest["description"] = self.description
			if self.tags:
				manifest["tags"] = self.tags

			def delete_uploaded_chunk(src, dst, retry, chunk_num):
				os.remove(src)
			self._transfer.on(transfer_complete=delete_uploaded_chunk)

			for src in self.src:
				fileinfo = {
					"name": '',
					"tar": False,
					"gzip": False,
					"chunks": [],
				}
				prefix = self._tranzit_vol.mpoint
				stream = None
				cmd = tar = gzip = None

				if hasattr(src, 'read'):
					# leaving fileinfo["name"] == ''
					stream = src
					if hasattr(stream, 'name'):
						name = stream.name
					else:
						name = 'stream-%s' % hash(stream)
					prefix = os.path.join(prefix, name) + '.'
				elif os.path.isdir(src):
					name = os.path.basename(src.rstrip('/'))
					fileinfo["name"] = name
					fileinfo["tar"] = True
					prefix = os.path.join(prefix, name) + '.tar.'

					tar = cmd = subprocess.Popen(
									['/bin/tar', 'cp', '-C', src, '.'],
									#['/bin/tar', 'cp', src],
									stdout=subprocess.PIPE,
									stderr=subprocess.PIPE,
									close_fds=True)
					stream = tar.stdout
				elif os.path.isfile(src):
					name = os.path.basename(src)
					fileinfo["name"] = name
					prefix = os.path.join(prefix, name) + '.'

					stream = open(src)
				else:
					raise ValueError('Unsupported src: %s' % src)

				if self.gzip_it:
					fileinfo["gzip"] = True
					prefix += 'gz.'
					gzip = cmd = subprocess.Popen(
								[self._gzip_bin(), '-5'],
								stdin=stream,
								stdout=subprocess.PIPE,
								stderr=subprocess.PIPE,
								close_fds=True)
					if tar:
						# Allow tar to receive SIGPIPE if gzip exits.
						tar.stdout.close()
					stream = gzip.stdout

				for filename, md5sum in self._split(stream, prefix):
					fileinfo["chunks"].append((os.path.basename(filename), md5sum))
					LOG.debug("LargeTransfer yield %s" % filename)
					yield filename
				if cmd:
					cmd.communicate()

				manifest["files"].append(fileinfo)

			# send manifest to file transfer
			if not self.multipart:
				LOG.debug("Manifest: %s" % manifest.data)
				manifest_f = os.path.join(self._tranzit_vol.mpoint, self.manifest)
				manifest.write(manifest_f)
				LOG.debug("LargeTransfer yield %s" % manifest_f)
				yield manifest_f

		elif self.direction == self.DOWNLOAD:
			def transfer_kill(*args, **kwargs):
				self._transfer.kill()
				# TODO: rethink killing
				#? kill self._restorer?
			self._transfer.on(transfer_error=transfer_kill())

			# The first yielded object will be the manifest, so
			# catch_manifest is a listener that's supposed to trigger only
			# once and unsubscribe itself.
			manifest_ready = threading.Event()
			def wait_manifest(src, dst, retry, chunk_num):
				self._transfer.un('transfer_complete', wait_manifest)
				manifest_ready.set()
			self._transfer.on(transfer_complete=wait_manifest)

			manifest_path = self.src.next()
			yield manifest_path

			manifest_ready.wait()
			# we should have the manifest on the tmpfs by now
			manifest_local = os.path.join(self._tranzit_vol.mpoint,
				os.path.basename(manifest_path))
			manifest = Manifest(manifest_local)
			os.remove(manifest_local)
			remote_path = os.path.dirname(manifest_path)
			self.files = copy(manifest["files"])

			# add ready and done events to each chunk without breaking the
			# chunk order
			for file in self.files:
				file["chunks"] = OrderedDict([(
					basename, {
						"md5sum": md5sum,
						"downloaded": threading.Event(),
						"processed": threading.Event()
					}
				) for basename, md5sum in file["chunks"]])

			# launch restorer
			self._restorer = threading.Thread(target=self._dl_restorer)
			self._restorer.start()

			def wait_chunk(src, dst, retry, chunk_num):
				basename = os.path.basename(dst)
				for file in self.files:
					if basename in file["chunks"]:
						chunk = file["chunks"][basename]

				chunk["downloaded"].set()
				chunk["processed"].wait()  # TODO: avoid infinite waiting
				os.remove(dst)
			self._transfer.on(transfer_complete=wait_chunk)

			for file in self.files:
				for chunk in file["chunks"]:
					yield os.path.join(remote_path, chunk)


	def _dst_generator(self):
		if self.direction == self.UPLOAD:
			# last yield for manifest
			# NOTE: this only works if dst is a dir
			for dst in self.dst:
				# has sense only if not multipart
				self._upload_res = os.path.join(dst, self.transfer_id, self.manifest)

				yield os.path.join(dst, self.transfer_id)
		else:
			while True:
				yield self._tranzit_vol.mpoint


	"""
	def _split(self, stream, prefix):
		buf_size = 4096
		chunk_size = self.chunk_size * 1024 * 1024
		read_bytes = 0
		fp = None

		def next_chunk(chunk_num=-1):
			chunk_num += 1
			return open(prefix + '%03d' % chunk_num, 'w'), hashlib.md5(), chunk_num
		fp, md5sum, chunk_num = next_chunk()

		while True:
			LOG.debug("%s %s %s" % (fp, md5sum.hexdigest(), chunk_num))
			size = min(buf_size, chunk_size - read_bytes)
			bytes = stream.read(size)
			if not bytes:
				if fp:
					fp.close()
				LOG.debug("BREAK")
				break
			read_bytes += len(bytes)
			fp.write(bytes)
			md5sum.update(bytes)
			if read_bytes == chunk_size:
				fp.close()
				yield fp.name, md5sum.hexdigest()
				fp, md5sum, chunk_num = next_chunk(chunk_num)
	"""


	def _split(self, stream, prefix):
		buf_size = 4096
		chunk_size = self.chunk_size * 1024 * 1024

		for chunk_n in itertools.count():
			chunk_name = prefix + '%03d' % chunk_n
			chunk_capacity = chunk_size
			chunk_md5 = hashlib.md5()

			with open(chunk_name, 'w') as chunk:
				while chunk_capacity:
					bytes = stream.read(min(buf_size, chunk_capacity))
					if not bytes:
						break
					chunk.write(bytes)
					chunk_capacity -= len(bytes)
					chunk_md5.update(bytes)

			if chunk_capacity != chunk_size:  # non-empty chunk
				yield chunk_name, chunk_md5.hexdigest()
			else:  # empty chunk
				os.remove(chunk_name)
			if chunk_capacity:  # empty or half-empty chunk, meaning stream
								# is empty (if stream.closed?)
				break


	def _gzip_bin(self):
		if self.try_pigz and os.path.exists(self.pigz_bin):
			return self.pigz_bin
		return self.gzip_bin


	def _proxy_event(self, event):
		def proxy(*args, **kwds):
			self.fire(event, *args, **kwds)
		return proxy


	def _dl_restorer(self):
		buf_size = 4096

		for file in self.files:
			dst = self.dst.next()

			# create 'cmd' and 'stream'
			if not file["tar"] and not file["gzip"]:
				cmd = None
				stream = open(dst, 'w')  #? use file["name"]
			elif file["tar"] and file["gzip"]:
				# unzip from pipe
				# from man unzip:
				# Archives read from standard input are not yet supported,
				# except with funzip (and then only the first member of the
				# archive can be extracted).
				unzip = subprocess.Popen(["/usr/bin/funzip"],
					stdin=subprocess.PIPE, stdout=subprocess.PIPE)
				cmd = subprocess.Popen(['/bin/tar', '-x', '-C', dst],
					stdin=unzip.stdout)
				unzip.stdout.close()
				stream = unzip.stdin
			elif file["tar"]:
				cmd = subprocess.Popen(['/bin/tar', '-x', '-C', dst],
					stdin=subprocess.PIPE)
				stream = cmd.stdin
			elif file["gzip"]:
				unzip = subprocess.Popen(["/usr/bin/funzip"],
					stdin=subprocess.PIPE, stdout=subprocess.PIPE)
				cmd = subprocess.Popen(["/usr/bin/tee", dst],
					stdin=unzip.stdout)
				unzip.stdout.close()
				stream = unzip.stdin

			for chunk, info in file["chunks"].iteritems():
				info["downloaded"].wait()

				location = os.path.join(self._tranzit_vol.mpoint, chunk)
				with open(location) as fd:
					while True:
						bytes = fd.read(buf_size)
						if not bytes:
							break
						stream.write(bytes)
						#? stream.flush()

				info["processed"].set()  # this leads to chunk removal

			stream.close()
			if cmd:
				cmd.communicate()


	def _run(self):
		LOG.debug("Creating tmpfs")
		self._tranzit_vol.size = int(self.chunk_size * self._transfer.num_workers * 1.1)
		self._tranzit_vol.ensure(mkfs=True)
		try:
			res = self._transfer.run()

			if self.direction == self.DOWNLOAD:
				self._restorer.join()
			elif self.direction == self.UPLOAD:
				if self.multipart:
					return res["multipart_result"]
				else:
					return self._upload_res
		finally:
			LOG.debug("Destroying tmpfs")
			self._tranzit_vol.destroy()
			coreutils.remove(self._tranzit_vol.mpoint)


class Manifest(object):
	"""
	manifest.json
	-------------

	{
		version: 2.0,
		description,
		tags,
		created_at,
		files: [
			{
				name,
				tar: true,
				gzip: true,
				chunks: [(basename001, md5sum)]
			}
		]
	}


	Supports reading of old ini-manifests and represents their data in the
	new-manifest style.

	Make sure to write to a file with '.json' extension.
	"""

	def __init__(self, filename=None):
		self.reset()
		if filename:
			self.read(filename)

	def reset(self):
		self.data = {
			"version": 2.0,
			"description": '',
			"tags": {},
			"files": [],
		}

	def read(self, filename):
		if filename.endswith(".json"):
			self.data = self._read_json(filename)
		elif filename.endswith(".ini"):
			self.data = self._read_ini(filename)
		else:
			raise TypeError(".json or .ini manifests only")
		return self

	def write(self, filename):
		self.data["created_at"] = time.strftime("%Y-%m-%d %H:%M:%S")
		with open(filename, 'w') as fd:
			fd.write(json.dumps(self.data) + '\n')

	def _read_json(self, filename):
		with open(filename) as fd:
			return json.load(fd)

	def _read_ini(self, filename):
		"""
		Backward compatibility with the old ini manifests.


		manifest.ini
		------------

		[snapshot]
		description = description here
		created_at = datetime
		pack_method = "pigz"

		[chunks]
		${database_1}.gz.part00 = md5sum
		${database_1}.gz.part01 = md5sum


		Chunks are parts of a single directory packed with tar and gz. They
		were stored unordered.
		"""
		parser = ConfigParser.ConfigParser()
		parser.read(filename)

		# get name using the first chunk name
		chunkname = parser.options("chunks")[0]
		chunkname = chunkname.rsplit('.', 1)[0]  # strip part number
		if chunkname.endswith(".tar.gz"):  # this should always be true
			name = chunkname[:-7]
		else:
			name = chunkname  # just in case
		tar = True
		gzip = True

		chunks = parser.items("chunks")
		chunks.sort()

		return {
			"version": 1.0,
			"description": parser.get("snapshot", "description"),
			"tags": {},
			"created_at": parser.get("snapshot", "created_at"),
			"files": [
				{
					"name": name,
					"tar": tar,
					"gzip": gzip,
					"chunks": chunks,
				}
			]
		}

	def __getitem__(self, item):
		return self.data.__getitem__(item)

	def __setitem__(self, key, value):
		return self.data.__setitem__(key, value)

	def __delitem__(self, key):
		return self.data.__delitem__(key)

	def __iter__(self):
		return self.data.__iter__()

	def __contains__(self, value):
		return self.data.__contains__(value)


def cloudfs(fstype, imported={}, **driver_kwds):
	if fstype not in imported:
		imported[fstype] = __import__('scalarizr.storage2.cloudfs.%s' % fstype,
			globals(), locals(), ["__cloudfs__"], -1).__cloudfs__
	return imported[fstype](**driver_kwds)


class CloudFileSystem(object):

	features = {
		'multipart': False
	}

	def parseurl(self, url):
		"""
		{
			'bucket',
			'path'

		}
		{
			'container',
			'object'

		}
		"""
		raise NotImplementedError()

	def ls(self, path):
		raise NotImplementedError()

	def stat(self, path):
		'''
		size in bytes
		type = dir | file | container
		'''
		raise NotImplementedError()

	def put(self, src, path):
		raise NotImplementedError()

	def multipart_init(self, path, part_size):
		'''
		Returns upload_id
		'''
		raise NotImplementedError()

	def multipart_put(self, upload_id, src):
		raise NotImplementedError()

	def multipart_complete(self, upload_id):
		raise NotImplementedError()

	def multipart_abort(self, upload_id):
		raise NotImplementedError()

	def get(self, path, dst):
		raise NotImplementedError()

	def delete(self, path):
		raise NotImplementedError()


'''
bak = backup.backup(
		type='mysqldump', 
		file_per_database=True, 
		cloudfs_dir='glacier://Vault_1/')
rst = buk.run()
print rst
>>> {
	type: mysqldump
	files: [{
		size: 14503104
		path: glacier://Vault_1/?avail_zone=us-east-1&archive_id=NkbByEejwEggmBz2fTHgJrg0XBoDfjP4q6iu87-TjhqG6eGoOY9Z8i1_AUyUsuhPAdTqLHy8pTl5nfCFJmDl2yEZONi5L26Omw12vcs01MNGntHEQL8MBfGlqrEXAMPLEArchiveId
	}]

Inside LargeTransfer:

pack into single tar | gzip | split | Transfer(generator, 'glacier://Vault_1/', multipart=True)



'''
