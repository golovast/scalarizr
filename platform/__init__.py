'''
Created on Dec 24, 2009

@author: marat
'''
from scalarizr.bus import bus
from scalarizr.util.filetool import read_file
import os
import re
import socket
import urllib2
import logging
import ConfigParser

class PlatformError(BaseException):
	pass

class UserDataOptions:
	FARM_ID = "farmid"
	SERVER_ID = "serverid"	
	ROLE_NAME = "realrolename"
	BEHAVIOUR = 'behaviors'
	CRYPTO_KEY = "szr_key"
	QUERYENV_URL = "queryenv_url"
	MESSAGE_SERVER_URL = "p2p_producer_endpoint"
	FARM_HASH = "hash"
	CLOUD_STORAGE_PATH = 'cloud_storage_path'
	ENV_ID = 'env_id'
	FARMROLE_ID = 'farm_roleid'
	ROLE_ID = 'roleid'
	REGION = 'region'

class PlatformFactory(object):
	_platforms = {}
	
	def new_platform(self, name):
		if not self._platforms.has_key(name):
			pl = __import__("scalarizr.platform." + name, globals(), locals(), fromlist=["get_platform"])
			self._platforms[name] = pl.get_platform()

		return self._platforms[name];


class PlatformFeatures:
	VOLUMES		= 'volumes'
	SNAPSHOTS	= 'snapshots'


class Platform():
	name = None
	_arch = None
	_access_data = None
	_userdata = None
	
	features = []
	scalrfs = None			
	
	def __init__(self):
		self.scalrfs = self._scalrfs(self)
	
	def get_private_ip(self):
		return self.get_public_ip()
	
	def get_public_ip(self):
		return socket.gethostbyname(socket.gethostname())
	
	def get_user_data(self, key=None):
		cnf = bus.cnf
		if self._userdata is None:
			path = cnf.private_path('.user-data')
			if os.path.exists(path):
				rawmeta = read_file(path)
				if not rawmeta:
					raise PlatformError("Empty user-data")
				self._userdata = self._parse_user_data(rawmeta)
		if key and self._userdata:
			return self._userdata[key] if key in self._userdata else None
		else:
			return self._userdata

	def set_access_data(self, access_data):
		self._access_data = access_data
	
	def get_access_data(self, prop=None):
		if prop:
			try:
				return self._access_data[prop]
			except TypeError, KeyError:
				raise PlatformError("Platform access data property '%s' doesn't exists" % (prop,))
		else:
			return self._access_data
		
	def clear_access_data(self):
		self._access_data = None
	
	def get_architecture(self):
		"""
		@return Architectures 
		"""
		if self._arch is None:
			uname = os.uname()
			if re.search("^i\\d86$", uname[4]):
				self._arch = Architectures.I386
			elif re.search("^x86_64$", uname[4]):
				self._arch = Architectures.X86_64
			else:
				self._arch = Architectures.UNKNOWN
		return self._arch
	
	@property
	def cloud_storage_path(self):
		try:
			return bus.cnf.rawini.get('general', 'cloud_storage_path')
		except ConfigParser.NoOptionError:
			return ''
	
	def _parse_user_data(self, raw_userdata):
		userdata = {}
		for k, v in re.findall("([^=]+)=([^;]*);?", raw_userdata):
			userdata[k] = v
		return userdata
	
	
	class _scalrfs(object):
		
		def __init__(self, platform):
			self.platform = platform
			self.ini = bus.cnf.rawini
		
		
		def root(self):
			scalr_id = ''
			if bus.queryenv_version >= (2012, 7, 1):
				queryenv = bus.queryenv_service
				scalr_id = queryenv.get_global_config()['params'].get('scalr.id', '')
			if scalr_id:
				scalr_id = '-' + scalr_id
			if bus.scalr_version >= (3, 1, 0):
				return '%s://scalr%s-%s-%s' % (
					self.platform.cloud_storage_path.split('://')[0],
					scalr_id,
					self.ini.get('general', 'env_id'),
				    self.ini.get('general', 'region')
				)
			else:
				return self.platform.cloud_storage_path

		
		def images(self):
			if bus.scalr_version >= (3, 1, 0):
				return os.path.join(self.root(), 'images')
			else:
				return '%s://scalr2-images-%s-%s' % (
					self.platform.cloud_storage_path.split('://')[0],
					self.ini.get('general', 'region'),
					self.platform.get_account_id()
				)

		
		def backups(self, service):
			if bus.scalr_version >= (3, 1, 0):
				path = 'backups/%s/%s/%s-%s' % (
					self.ini.get('general', 'farm_id'),
					service,
					self.ini.get('general', 'farm_role_id'),
					self.ini.get('general', 'role_name')
				)
				return os.path.join(self.root(), path)
			else:
				return os.path.join(self.root(), '%s-backup' % service)
	

class Ec2LikePlatform(Platform):
	
	_meta_url = "http://169.254.169.254/"
	_userdata_key = 'latest/user-data'
	_metadata_key = 'latest/meta-data'
	_metadata = {}
	_userdata = None
	
	def __init__(self):
		Platform.__init__(self)
		self._logger = logging.getLogger(__name__)
		self._cnf = bus.cnf
	
	def _get_property(self, name):
		if not self._metadata.has_key(name):
			full_name = os.path.join(self._metadata_key, name)
			self._metadata[name] = self._fetch_metadata(full_name)
		return self._metadata[name]
	
	def get_user_data(self, key=None):
		if self._userdata is None:
			raw_userdata = self._fetch_metadata(self._userdata_key)
			self._userdata = self._parse_user_data(raw_userdata)			
		if key:
			return self._userdata[key] if key in self._userdata else None
		else:
			return self._userdata

	def _fetch_metadata(self, key):
		url = self._meta_url + key
		try:
			r = urllib2.urlopen(url)
			return r.read().strip()
		except IOError, e:
			if isinstance(e, urllib2.HTTPError):
				if e.code == 404:
					return ""
			raise PlatformError("Cannot fetch %s metadata url '%s'. Error: %s" % (self.name, url, e))
		
	def get_private_ip(self):
		return self._get_property("local-ipv4")
	
	def get_public_ip(self):
		return self._get_property("public-ipv4")
	
	def get_public_hostname(self):
		return self._get_property("public-hostname")
	
	def get_instance_id(self):
		return self._get_property("instance-id")
	
	def get_instance_type(self):
		return self._get_property("instance-type")
	
	def get_ami_id(self):
		return self._get_property("ami-id")

	def get_ancestor_ami_ids(self):
		return self._get_property("ancestor-ami-ids").split("\n")
	
	def get_kernel_id(self):
		return self._get_property("kernel-id")
	
	def get_ramdisk_id(self):
		return self._get_property("ramdisk-id")
	
	def get_avail_zone(self):
		return self._get_property("placement/availability-zone")
	
	def get_region(self):
		return self.get_avail_zone()[0:-1]
	
	def get_block_device_mapping(self):
		keys = self._get_property("block-device-mapping").split("\n")
		ret = {}
		for key in keys:
			ret[key] = self._get_property("block-device-mapping/" + key)
		return ret
	
	def block_devs_mapping(self):
		keys = self._get_property("block-device-mapping").split("\n")
		ret = list()
		for key in keys:
			ret.append((key, self._get_property("block-device-mapping/" + key)))
		return ret
		
	def get_ssh_pub_key(self):
		return self._get_property("public-keys/0/openssh-key")

class Architectures:
	I386 = "i386"
	X86_64 = "x86_64"
	UNKNOWN = "unknown"
