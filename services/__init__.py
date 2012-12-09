'''
Created on Jul 7, 2011

@author: shaitanich
'''

import os
import logging

from scalarizr.libs.metaconf import Configuration, NoPathError
from scalarizr.handlers import operation


LOG = logging.getLogger(__name__)


def lazy(init):
	def wrapper(cls, *args, **kwargs):
		obj = init(cls, *args, **kwargs)
		return LazyInitScript(obj)
	return wrapper


class LazyInitScript(object):
	
	_script = None
	reload_queue = None
	restart_queue = None
	
	def __getattr__(self, name):
		return getattr(self._script, name) 
	
	def __init__(self, script):
		self._script = script
		self.reload_queue = []
		self.restart_queue = []

	def start(self):
		try:
			if not self._script.running:
				self._script.start()
			elif self.restart_queue:
				reasons = ' '.join([req+',' for req in self.restart_queue])[:-1]
				self._script.restart(reasons)	
			elif self.reload_queue:
				reasons = ' '.join([req+',' for req in self.reload_queue])[:-1]
				self._script.reload(reasons)		
		finally:
			self.restart_queue = []
			self.reload_queue = []	
		
	def stop(self, reason=None):
		if self._script.running:
			try:
				LOG.info('Stopping service: %s' % reason)
				self._script.stop(reason)
			finally:
				self.restart_queue = []
				self.reload_queue = []	

	def restart(self, reason=None, force=False):
		if force:
			self._script.restart(reason)
		elif  self._script.running:
			self.restart_queue.append(reason)
		
	def reload(self, reason=None, force=False):
		if force:
			self._script.reload(reason)
		elif self._script.running:
			self.reload_queue.append(reason)
	
	@property		
	def running(self):
		return self._script.running
	
	def status(self):
		return self._script.status()
			

class BaseService(object):

	_objects = None
	
	def _set(self, key, obj):
		self._objects[key] = obj
		
	def _get(self, key, callback, *args, **kwargs):
		if not self._objects.has_key(key):
			self._set(key, callback(*args, **kwargs))
		return self._objects[key]	
	
	
class BaseConfig(object):
	'''
	Parent class for object representations of postgresql.conf and recovery.conf which fortunately both have similar syntax
	'''
	
	autosave = None
	path = None
	data = None
	config_name = None
	config_type = None
	comment_empty = False
	
	def __init__(self, path, autosave=True):
		self._logger = logging.getLogger(__name__)
		self.autosave = autosave
		self.path = path
		
	@classmethod
	def find(cls, config_dir):
		return cls(os.path.join(config_dir.path, cls.config_name))
		
	def set(self, option, value):
		if not self.data:
			self.data = Configuration(self.config_type)
			if os.path.exists(self.path):
				self.data.read(self.path)
		if value:
			self.data.set(option,str(value), force=True)
		elif self.comment_empty: 
			self.data.comment(option)
		if self.autosave:
			self.save_data()
			self.data = None
			
	def set_path_type_option(self, option, path):
		if not os.path.exists(path):
			raise ValueError('%s %s does not exist' % (option, path))
		self.set(option, path)		
		
	def set_numeric_option(self, option, number):
		try:
			assert number is None or type(number) is int
		except AssertionError:
			raise ValueError('%s must be a number (got %s instead)' % (option, number))
		
		is_numeric = type(number) is int
		self.set(option, str(number) if is_numeric else None)

					
	def get(self, option):
		if not self.data:
			self.data =  Configuration(self.config_type)
			if os.path.exists(self.path):
				self.data.read(self.path)	
		try:
			value = self.data.get(option)	
		except NoPathError:
			try:
				value = getattr(self, option+'_default')
			except AttributeError:
				value = None
		if self.autosave:
			self.data = None
		return value
	
	def get_numeric_option(self, option):
		value = self.get(option)
		try:
			assert value is None or int(value)
		except AssertionError:
			raise ValueError('%s must be a number (got %s instead)' % (option, type(value)))
		return value if value is None else int(value)
	
	def save_data(self):
		if self.data:
			self.data.write(self.path)			
			
class ServiceError(BaseException):
	pass


def backup_step_msg(str_or_tuple):
	if isinstance(str_or_tuple, str):
		return "Backup '%s'" % str_or_tuple

	start = str_or_tuple[0]
	end = str_or_tuple[1]
	num = str_or_tuple[2]
	if start+1 != end:
		return 'Backup %d-%d of %d databases' % (start+1, end, num)
	else:
		return 'Backup last database'
		

# number of databases backuped in single step
backup_num_databases_in_step = 10  

def backup_databases_iterator(databases):
	page_size = backup_num_databases_in_step
	num_db = len(databases)
	if num_db >= page_size:
		for start in xrange(0, num_db, page_size):
			end = start + page_size
			if end > num_db:
				end = num_db
			yield (databases[start:end], backup_step_msg((start, end, num_db)))
	else:
		for db_name in databases:
			yield ([db_name], backup_step_msg(db_name))

def make_backup_steps(db_list, _operation, _single_backup_fun):
	for db_portion, step_msg in backup_databases_iterator(db_list):
		with _operation.step(step_msg):
			for db_name in db_portion:
				_single_backup_fun(db_name)