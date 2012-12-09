__author__ = 'Nick Demyanchuk'

import os
import sys
import urlparse
import logging

from apiclient.http import MediaFileUpload, MediaIoBaseDownload

from scalarizr.bus import bus
from scalarizr.storage import transfer


LOG = logging.getLogger(__name__)
CHUNK_SIZE = 2*1024*1024

class GoogleCSTransferProvider(transfer.TransferProvider):

	schema = 'gcs'
	urlparse.uses_netloc.append(schema)

	def list(self, remote_path):
		bucket, path = self._parse_path(remote_path)

		req = self.cloudstorage.objects().list(
			bucket=bucket, prefix=path, delimiter='/'
		)
		resp = req.execute()
		return tuple('gcs://%s' % o['name'] for o in resp['items'])


	def get(self, remote_path, local_path):
		LOG.debug('Downloading %s from cloud storage (local path: %s)', remote_path, local_path)
		bucket, name = self._parse_path(remote_path)
		local_path = os.path.join(local_path, os.path.basename(remote_path))
		f = open(local_path, 'w')
		request = self.cloudstorage.objects().get_media(
			bucket=bucket, object=name)
		media = MediaIoBaseDownload(f, request, chunksize=CHUNK_SIZE)
		done = False

		while not done:
			status, done = media.next_chunk()
			if status:
				LOG.debug('Downloaded %d%%' % int(status.progress() * 100))

		LOG.debug('Download complete.')
		return local_path


	def put(self, local_path, remote_path):
		LOG.debug('Uploading %s to cloud storage (remote path: %s)', local_path, remote_path)

		filename = os.path.basename(local_path)
		bucket, name = self._parse_path(remote_path)
		name = os.path.join(name, filename)

		buckets = self._list_buckets()
		if bucket not in buckets:
			self._create_bucket(bucket)

		media = MediaFileUpload(local_path,
			'application/octet-stream',
			resumable=True)

		response = None
		req = self.cloudstorage.objects().insert(
			bucket=bucket, name=name, media_body=media
		)
		last_progress = 0
		while response is None:
			status, response = req.next_chunk()
			if status:
				percentage = int(status.progress() * 100)
				if percentage - last_progress >= 10:
					LOG.debug("Uploaded %d%%." % percentage)
					last_progress = percentage
		LOG.debug('Upload completed.')
		return 'gcs://%s' % os.path.join(bucket, name)



	def _parse_path(self, path):
		o = urlparse.urlparse(path)
		if o.scheme != self.schema:
			raise transfer.TransferError('Wrong schema')
		return o.hostname, o.path[1:]


	def _list_buckets(self):
		pl = bus.platform
		proj_id = pl.get_numeric_project_id()

		req = self.cloudstorage.buckets().list(projectId=proj_id)
		resp = req.execute()
		if 'items' not in resp:
			return []
		buckets = [b['id'] for b in resp['items']]
		return buckets


	def _create_bucket(self, bucket_name):
		pl = bus.platform
		proj_id = pl.get_numeric_project_id()

		req_body = dict(id=bucket_name, projectId=proj_id)
		req = self.cloudstorage.buckets().insert(body=req_body)
		try:
			req.execute()
		except:
			e = sys.exc_info()[1]
			if not 'You already own this bucket' in str(e):
				raise


	@property
	def cloudstorage(self):
		pl = bus.platform
		return pl.new_storage_client()