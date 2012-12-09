from __future__ import with_statement

import os
import re
import ConfigParser
import sys
try:
	import json
except ImportError:
	import simplejson as json 
import logging

LOG = logging.getLogger(__name__)

base_dir = '/etc/scalr'
private_dir = base_dir + '/private.d'
public_dir = base_dir + '/public.d'
storage_dir = private_dir + '/storage'


class Store(dict):
	def __len__(self):
		return 1

	def __repr__(self):
		return '<%s at %s>' % (type(self).__name__, hex(id(self)))

	def __contains__(self, key):
		try:
			self.__getitem__(key)
			return True
		except KeyError:
			return False

	def update(self, *args, **kwds):
		if args:
			if len(args) == 1 and isinstance(args[0], dict):
				kwds = args[0]
			else:
				kwds = dict(args)
		for key, value in kwds.items():
			self.__setitem__(key, value)
		


class Compound(Store):
	def __init__(self, patterns=None):
		self.__re_map = {}
		self.__plain_map = {}
		patterns = patterns or {}
		for pattern, store in patterns.items():
			keys = pattern.split(',')
			for key in keys:
				if '*' in key:
					key = re.compile(r'^%s$' % key.replace('*', '.+'))
					self.__re_map[key] = store
				elif isinstance(store, Store):
					self.__plain_map[key] = store
				else:
					dict.__setitem__(self, key, store)


	def __setitem__(self, key, value):
		store = self.__find_store(key)
		if store:
			store.__setitem__(key, value)
		else:
			dict.__setitem__(self, key, value)


	def __getitem__(self, key):
		store = self.__find_store(key)
		if store:
			if isinstance(store, Compound):
				return store
			else:
				return store.__getitem__(key)
		else:
			return dict.__getitem__(self, key)


	def __find_store(self, key):
		if key in self.__plain_map:
			return self.__plain_map[key]
		else:
			for rkey, store in self.__re_map.items():
				if rkey.match(key):
					return store

		

class Json(Store):
	def __init__(self, filename, fn):
		'''
		Example:
		jstore = Json('/etc/scalr/private.d/storage/mysql.json', 
					'scalarizr.storage2.volume')
		'''
		self.filename = filename
		self.fn = fn
		self._obj = None

	def __getitem__(self, key):
		if not self._obj:
			try:
				with open(self.filename, 'r') as fp:
					kwds = json.load(fp)
			except:
				raise KeyError(key)
			else:
				if isinstance(self.fn, basestring):
					self.fn = _import(self.fn)
				LOG.debug('getitem: %s', key)
				self._obj = self.fn(**kwds)
		return self._obj


	def __setitem__(self, key, value):
		self._obj = value
		if hasattr(value, 'config'):
			vv = dict(value)
		else:
			vv = value
		dirname = os.path.dirname(self.filename)
		if not os.path.exists(dirname):
			os.makedirs(dirname)
		with open(self.filename, 'w+') as fp:
			json.dump(vv, fp)


class Ini(Store):
	def __init__(self, filename, section, mapping=None):
		self.filename = filename
		self.section = section
		self.ini = None
		self.mapping = mapping or {}


	def _reload(self):
		self.ini = ConfigParser.ConfigParser()
		if os.path.exists(self.filename):
			self.ini.read(self.filename)


	def __getitem__(self, key):
		try:
			self._reload()
			if key in self.mapping:
				key = self.mapping[key]
			return self.ini.get(self.section, key)
		except ConfigParser.Error:
			raise KeyError(key)


	def __setitem__(self, key, value):
		self._reload()
		if value is None:
			value = ''
		elif isinstance(value, bool):
			value = str(int(value))
		else:
			value = str(value)
		if not self.ini.has_section(self.section):
			self.ini.add_section(self.section)
		if key in self.mapping:
			key = self.mapping[key]
		self.ini.set(self.section, key, value)
		with open(self.filename, 'w+') as fp:
			self.ini.write(fp)	


class IniOption(Ini):
	def __init__(self, filename, section, option, 
			getfilter=None, setfilter=None):
		self.option = option
		self.getfilter = getfilter
		self.setfilter = setfilter
		super(IniOption, self).__init__(filename, section)


	def __getitem__(self, key):
		value = super(IniOption, self).__getitem__(self.option)
		if self.getfilter:
			return self.getfilter(value)
		return value


	def __setitem__(self, key, value):
		if self.setfilter:
			value = self.setfilter(value)
		super(IniOption, self).__setitem__(self.option, value)


class File(Store):
	def __init__(self, filename):
		self.filename = filename


	def __getitem__(self, key):
		try:
			with open(self.filename) as fp:
				return fp.read().strip()
		except:
			raise KeyError(key)


	def __setitem__(self, key, value):
		with open(self.filename, 'w+') as fp:
			fp.write(str(value).strip())


class BoolFile(Store):
	def __init__(self, filename):
		self.filename = filename


	def __getitem__(self, key):
		return os.path.isfile(self.filename)


	def __setitem__(self, key, value):
		if value:
			open(self.filename, 'w+').close()
		else:
			if os.path.isfile(self.filename):
				os.remove(self.filename)


class State(Store):
	def __init__(self, key):
		self.key = key

	def __getitem__(self, key):
		from scalarizr.config import STATE
		return STATE[self.key]

	def __setitem__(self, key, value):
		from scalarizr.config import STATE
		STATE[self.key] = value


class Attr(Store):
	def __init__(self, module, attr):
		self.module = module
		self.attr = attr
		self.getter = None


	def __getitem__(self, key):
		try:
			if isinstance(self.module, basestring):
				self.module = _import(self.module)
			if not self.getter:
				def getter():
					path = self.attr.split('.')
					base = self.module
					for name in path[:-1]:
						base = getattr(base, name)
					return getattr(base, path[-1])
				self.getter = getter
		except:
			raise KeyError(key) 
		return self.getter()


class Call(Attr):
	def __getitem__(self, key):
		attr = Attr.__getitem__(self, key)
		return attr()	


def _import(objectstr):
	try:
		__import__(objectstr)
		return sys.modules[objectstr]
	except ImportError:
		module_s, _, attr = objectstr.rpartition('.')
		__import__(module_s)
		try:
			return getattr(sys.modules[module_s], attr)
		except (KeyError, AttributeError):
			raise ImportError('No module named %s' % attr)


__node__ = {
	'server_id,role_id,farm_id,farm_role_id,env_id': 
				Ini(private_dir + '/config.ini', 'general'),
	'platform': Ini(public_dir + '/config.ini', 'general'),
	'behavior': IniOption(
						private_dir + '/config.ini', 'general', 'behaviour',
						lambda val: val.strip().split(','),
						lambda val: ','.join(val)),
	'public_ip': Call('scalarizr.bus', 'bus.platform.get_public_ip'),
	'private_ip': Call('scalarizr.bus', 'bus.platform.get_private_ip'),
	'state': File(private_dir + '/.state'),
	'rebooted': BoolFile(private_dir + '/.reboot'),
	'halted': BoolFile(private_dir + '/.halt')
}
for behavior in ('mysql', 'mysql2', 'percona'):
	section = 'mysql2' if behavior == 'percona' else behavior
	__node__[behavior] = Compound({
		'volume,volume_config': 
				Json('%s/storage/%s.json' % (private_dir, 'mysql'), 
					'scalarizr.storage2.volume'),
		'*_password,log_*,replication_master': 
				Ini('%s/%s.ini' % (private_dir, behavior), section),
		'mysqldump_options': 
				Ini('%s/%s.ini' % (public_dir, behavior), section)
	})
__node__['ec2'] = Compound({
	't1micro_detached_ebs': State('ec2.t1micro_detached_ebs'),
	'hostname_as_pubdns': 
				Ini('%s/%s.ini' % (public_dir, 'ec2'), 'ec2'),
	'ami_id': Call('scalarizr.bus', 'bus.platform.get_ami_id'),
	'kernel_id': Call('scalarizr.bus', 'bus.platform.get_kernel_id'),
	'ramdisk_id': Call('scalarizr.bus', 'bus.platform.get_ramdisk_id'),
	'instance_id': Call('scalarizr.bus', 'bus.platform.get_instance_id'),
	'instance_type': Call('scalarizr.bus', 'bus.platform.get_instance_type'),
	'avail_zone': Call('scalarizr.bus', 'bus.platform.get_avail_zone'),
	'region': Call('scalarizr.bus', 'bus.platform.get_region'),
	'connect_ec2': Attr('scalarizr.bus', 'bus.platform.new_ec2_conn'),
	'connect_s3': Attr('scalarizr.bus', 'bus.platform.new_s3_conn')
})
__node__['scalr'] = Compound({
	'version': File(private_dir + '/.scalr-version'),
	'id': Ini(private_dir + '/config.ini', 'general', {'id': 'scalr_id'})
})
__node__ = Compound(__node__)
#__node__ = dict() # XXX: added for unittests


