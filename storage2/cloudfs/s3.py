__author__ = 'vladimir'

import urlparse
import logging
import os
import sys

from boto.s3.key import Key
from boto.exception import S3ResponseError

from scalarizr.bus import bus


class TransferError(Exception):  #? BaseException
	pass


class S3FileSystem(object):
	schema	= 's3'
	urlparse.uses_netloc.append(schema)

	acl	= None

	_logger = None
	_bucket = None

	def __init__(self, acl='aws-exec-read'):
		self._logger = logging.getLogger(__name__)
		self.acl = acl

	def parse_url(self, url):
		o = urlparse.urlparse(url)
		if o.scheme != self.schema:
			raise TransferError('Wrong schema')
		return o.hostname, o.path[1:]

	def ls(self, remote_path):
		bucket_name, key_name = self.parse_url(remote_path)
		connection = self._get_connection()
		bkt = connection.get_bucket(bucket_name, validate=False)
		files = [self._format_path(self._bucket.name, key.name) for key in bkt.list(prefix=key_name)]
		return tuple(files)

	def stat(self, path):
		raise NotImplementedError()

	def put(self, local_path, remote_path):
		self._logger.info("Uploading '%s' to S3 under '%s'", local_path, remote_path)
		bucket_name, key_name = self.parse_url(remote_path)
		key_name = os.path.join(key_name, os.path.basename(local_path))

		try:
			connection = self._get_connection()

			if not self._bucket_check_cache(bucket_name):
				try:
					bck = connection.get_bucket(bucket_name)
				except S3ResponseError, e:
					if e.code == 'NoSuchBucket':
						pl = bus.platform
						try:
							location = location_from_region(pl.get_region())
						except:
							location = ''
						bck = connection.create_bucket(
							bucket_name,
							location=location,
							policy=self.acl
						)
					else:
						raise
				# Cache bucket
				self._bucket = bck

			file = None
			try:
				key = Key(self._bucket)
				key.name = key_name
				file = open(local_path, "rb")
				self._logger.debug("Actually uploading %s" % file)
				key.set_contents_from_file(file, policy=self.acl)  # TODO: hangs sometimes
				return self._format_path(bucket_name, key_name)
			finally:
				if file:
					file.close()

		except:
			self._logger.debug("except")
			exc = sys.exc_info()
			raise TransferError, exc[1], exc[2]

	def get(self, remote_path, local_path):
		self._logger.info('Downloading %s from S3 to %s' % (remote_path, local_path))
		bucket_name, key_name = self.parse_url(remote_path)
		dest_path = os.path.join(local_path, os.path.basename(remote_path))

		try:
			connection = self._get_connection()

			try:
				if not self._bucket_check_cache(bucket_name):
					self._bucket = connection.get_bucket(bucket_name, validate=False)
				key = self._bucket.get_key(key_name)
				if key is None: raise TransferError("Key is None. No such key?")  ###
			except S3ResponseError, e:
				if e.code in ('NoSuchBucket', 'NoSuchKey'):
					raise TransferError("S3 path '%s' not found" % remote_path)
				raise

			key.get_contents_to_filename(dest_path)
			return dest_path

		except:
			exc = sys.exc_info()
			raise TransferError, exc[1], exc[2]

	def delete(self, path):
		raise NotImplementedError()

	def _bucket_check_cache(self, bucket):
		if self._bucket and self._bucket.name != bucket:
			self._bucket = None
		return self._bucket

	def _get_connection(self):
		pl = bus.platform
		return pl.new_s3_conn()

	def _format_path(self, bucket, key):
		return '%s://%s/%s' % (self.schema, bucket, key)


def location_from_region(region):
	if region == 'us-east-1' or not region:
		return ''
	elif region == 'eu-west-1':
		return 'EU'
	else:
		return region


__cloudfs__ = S3FileSystem
