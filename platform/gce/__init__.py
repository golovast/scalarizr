from __future__ import with_statement

__author__ = 'Nick Demyanchuk'

import os
import base64
import logging
import urllib2
import httplib2
import threading

try:
	import json
except ImportError:
	import simplejson as json

from oauth2client.client import SignedJwtAssertionCredentials
from apiclient.discovery import build

from scalarizr.platform import Platform
from scalarizr.storage.transfer import Transfer
from scalarizr.platform.gce.storage import GoogleCSTransferProvider


Transfer.explore_provider(GoogleCSTransferProvider)

COMPUTE_RW_SCOPE = 'https://www.googleapis.com/auth/compute'
STORAGE_FULL_SCOPE = 'https://www.googleapis.com/auth/devstorage.full_control'


LOG = logging.getLogger(__name__)

def get_platform():
	return GcePlatform()


class GoogleServiceManager(object):
	"""
	Manages 1 service connection per thread
	Works out dead threads' connections
	"""

	def __init__(self, pl, s_name, s_ver, *scope):
		self.pl = pl
		self.s_name= s_name
		self.s_ver = s_ver
		self.scope = list(scope)
		self.map = {}
		self.lock = threading.Lock()
		self.pool = []

	def get_service(self):
		current_thread = threading.current_thread()
		with self.lock:
			if not current_thread in self.map:
				# Check other threads
				for t, s in self.map.items():
					if not t.is_alive():
						self.pool.append(s)
						del self.map[t]

				if self.pool:
					s = self.pool.pop()
					self.map[current_thread] = s
					return s

				http = self._get_auth()
				s = build(self.s_name, self.s_ver, http=http)
				self.map[current_thread] = s

			return self.map[current_thread]


	def _get_auth(self):
		http = httplib2.Http()
		email = self.pl.get_access_data('service_account_name')
		pk = base64.b64decode(self.pl.get_access_data('key'))
		cred = SignedJwtAssertionCredentials(email, pk, scope=self.scope)
		return cred.authorize(http)



class GcePlatform(Platform):
	metadata_url = 'http://metadata.google.internal/0.1/meta-data/'
	_metadata = None

	def __init__(self):
		Platform.__init__(self)
		self.compute_svc_mgr = GoogleServiceManager(
			self, 'compute', 'v1beta12', COMPUTE_RW_SCOPE, STORAGE_FULL_SCOPE)

		self.storage_svs_mgr = GoogleServiceManager(
			self, 'storage', 'v1beta1', STORAGE_FULL_SCOPE)


	def get_user_data(self, key=None):
		if self._userdata is None:
			self._userdata = dict()
			resp = self._get_metadata('attributes/')
			keys = resp.strip().split()
			for k in keys:
				value = self._get_metadata('attributes/%s' % k)
				self._userdata[k] = value

		return self._userdata.get(key) if key else self._userdata


	def _get_metadata(self, key):
		if self._metadata is None:
			self._metadata = dict()

		if not key in self._metadata:
			key_url = os.path.join(self.metadata_url, key)
			resp = urllib2.urlopen(key_url)
			self._metadata[key] = resp.read()

		return self._metadata[key]


	def get_public_ip(self):
		network = self._get_metadata('network')
		network = json.loads(network)
		return network['networkInterface'][0]['accessConfiguration'][0]['externalIp']


	def get_private_ip(self):
		network = self._get_metadata('network')
		network = json.loads(network)
		return network['networkInterface'][0]['ip']


	def get_project_id(self):
		return self._get_metadata('project-id')


	def get_zone(self):
		return self._get_metadata('zone')


	def get_numeric_project_id(self):
		return self._get_metadata('numeric-project-id')


	def get_machine_type(self):
		return self._get_metadata('machine-type')


	def get_instance_id(self):
		return self._get_metadata('instance-id')


	def get_hostname(self):
		return self._get_metadata('hostname')


	def get_image(self):
		return self._get_metadata('image')


	def new_compute_client(self):
		return self.compute_svc_mgr.get_service()


	def new_storage_client(self):
		return self.storage_svs_mgr.get_service()
