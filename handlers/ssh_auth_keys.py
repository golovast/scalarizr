
from __future__ import with_statement

from scalarizr.bus import bus
from scalarizr.handlers import Handler
from scalarizr.messaging import Messages
from scalarizr.util import firstmatched
from scalarizr.util.filetool import read_file, write_file

import re
import os
import logging
from scalarizr.util import disttool
from scalarizr.util.initdv2 import ParametrizedInitScript

class UpdateSshAuthorizedKeysError(BaseException):
	pass

def get_handlers ():
	return [SSHKeys()]

class SSHKeys(Handler):
	sshd_config_path = '/etc/ssh/sshd_config'
	authorized_keys_file = '/root/.ssh/authorized_keys'
	
	_logger = None
	_sshd_init = None
	
	def __init__(self):
		self._logger = logging.getLogger(__name__)
		
		if disttool.is_redhat_based():
			init_script = ('/sbin/service', 'sshd')
		elif disttool.is_ubuntu() and disttool.version_info() >= (10, 4):
			init_script = ('/usr/sbin/service', 'ssh')
		else:
			init_script = firstmatched(os.path.exists, ('/etc/init.d/ssh', '/etc/init.d/sshd'))
		self._sshd_init = ParametrizedInitScript('sshd', init_script)
		
		bus.on(init=self.on_init)
		
	def on_init(self):
		self._setup_sshd_config()
		
	def _setup_sshd_config(self):
		# Enable public key authentification
		sshd_config = open(self.sshd_config_path)
		lines = sshd_config.readlines()
		sshd_config.close()
		
		variables = {
			'RSAAuthentication' : 'yes',
			'PubkeyAuthentication' : 'yes',
			'AuthorizedKeysFile' :	'.ssh/authorized_keys'
		}
		regexps = {}
		for key, value in variables.items():
			regexps[key] = re.compile(r'^%s\s+%s' % (key, value))
			
		new_lines = []
		for line in lines:
			for key, regexp in regexps.items():
				if regexp.search(line):
					self._logger.debug('Found %s', regexp)
					if key in variables:
						del variables[key]
				elif line.startswith(key) and key in variables:
					# second condition is a workaround over duplicate options in sshd_config 
					self._logger.debug('Update %s option %s: %s', self.sshd_config_path, key, variables[key])
					line = '%s %s\n' % (key, variables[key])
					if key in variables:
						del variables[key]
			new_lines.append(line)
		for key, value in variables.items():
			self._logger.debug('Update %s option %s: %s', self.sshd_config_path, key, value)
			new_lines.append('%s %s\n' % (key, value))
			
		if new_lines != lines:	
			self._logger.debug('Writing new %s', self.sshd_config_path)
			fp = open(self.sshd_config_path, 'w')
			fp.write(''.join(new_lines))
			fp.close()
			self._sshd_init.restart()
		
		
		# Setup .ssh directory structure
		ssh_dir = os.path.dirname(self.authorized_keys_file)
		if not os.path.exists(ssh_dir):
			self._logger.debug('Creating %s', ssh_dir)
			os.makedirs(ssh_dir)
		os.chmod(ssh_dir, 0700)
		
		if not os.path.exists(self.authorized_keys_file):
			self._logger.debug('Creating empty authrized keys file %s', self.authorized_keys_file)
			open(self.authorized_keys_file, 'w').close()
		os.chmod(self.authorized_keys_file, 0600)
		
	def on_UpdateSshAuthorizedKeys(self, message):
		if not message.add and not message.remove:
			self._logger.debug('Empty key lists in message. Nothing to do.')
			return
		
		self._setup_sshd_config()

		ak = self._read_ssh_keys_file()
		if message.add:
			for key in message.add:
				ak = self._add_key(ak, key)
		if message.remove:
			for key in message.remove:
				ak = self._remove_key(ak, key)
				
		if ak:
			ak = self._check(ak)
			self._write_ssh_keys_file(ak)
	
	def _read_ssh_keys_file(self):
		content = read_file(self.authorized_keys_file, 
						msg='Reading autorized keys from %s' % self.authorized_keys_file, 
						logger=self._logger)
		if content == None:
			raise UpdateSshAuthorizedKeysError('Unable to read ssh keys from %s' % self.authorized_keys_file)
		return content
	
	def _write_ssh_keys_file(self, content):
		ret = write_file(self.authorized_keys_file, content, msg='Writing authorized keys', logger=self._logger)
		if not ret:
			raise UpdateSshAuthorizedKeysError('Unable to write ssh keys to %s' % self.authorized_keys_file)
		os.chmod(self.authorized_keys_file, 0600)
	
	def _add_key(self, content, key):
		if not key in content:
			return content + '\n%s\n' % key
		else:
			self._logger.debug('Key already exists in %s' % self.authorized_keys_file)
			return content
	
	def _remove_key(self, content, key):
		if content:
			return content.replace(key, '')
		else: 
			self._logger.debug('No keys found. Keys file %s is probably empty' % self.authorized_keys_file)
			return content
		
	def _check(self, content):
		while '\n\n' in content:
			content = content.replace('\n\n', '\n')
		if not content.endswith('\n'):
			content += '\n'
		return content
		
	def accept(self, message, queue, behaviour=None, platform=None, os=None, dist=None):
		return (message.name == Messages.UPDATE_SSH_AUTHORIZED_KEYS)
	