'''
Created on Mar 2, 2010

@author: marat
'''
from scalarizr.bus import bus
from scalarizr.handlers import Handler
from scalarizr.util import system2, filetool, disttool
import logging
import os, re, sys
import ConfigParser
from scalarizr.util.fstool import Mtab, Fstab, mount


def get_handlers ():
	return [Ec2LifeCycleHandler()]

class Ec2LifeCycleHandler(Handler):
	_logger = None
	_platform = None
	"""
	@ivar scalarizr.platform.ec2.Ec2Platform:
	"""
	
	def __init__(self):
		self._logger = logging.getLogger(__name__)
		bus.on(init=self.on_init, reload=self.on_reload)
		self.on_reload()		
	
	def on_init(self, *args, **kwargs):
		bus.on("before_hello", self.on_before_hello)		
		bus.on("before_host_init", self.on_before_host_init)
		bus.on("before_restart", self.on_before_restart)

		msg_service = bus.messaging_service
		producer = msg_service.get_producer()
		producer.on("before_send", self.on_before_message_send)
		
		# Set the hostname to this instance's public hostname
		cnf = bus.cnf
		try:
			hostname_as_pubdns = int(cnf.rawini.get('ec2', 'hostname_as_pubdns'))
		except ConfigParser.Error:
			hostname_as_pubdns = True
		if hostname_as_pubdns:
			system2("hostname " + self._platform.get_public_hostname(), shell=True)		
		
		if disttool.is_ubuntu():
			# Ubuntu cloud-init scripts may disable root ssh login
			for path in ('/etc/ec2-init/ec2-config.cfg', '/etc/cloud/cloud.cfg'):
				if os.path.exists(path):
					c = filetool.read_file(path)
					c = re.sub(re.compile(r'^disable_root[^:=]*([:=]).*', re.M), r'disable_root\1 0', c)
					filetool.write_file(path, c)
			
		# Add server ssh public key to authorized_keys
		authorized_keys_path = "/root/.ssh/authorized_keys"
		if os.path.exists(authorized_keys_path):
			c = filetool.read_file(authorized_keys_path)
			ssh_key = self._platform.get_ssh_pub_key()
			idx = c.find(ssh_key)
			if idx == -1:
				if c and c[-1] != '\n':
					c += '\n'
				c += ssh_key + "\n"
				self._logger.debug("Add server ssh public key to authorized_keys")
				filetool.write_file(authorized_keys_path, c)
			elif idx > 0 and c[idx-1] != '\n':
				c = c[0:idx] + '\n' + c[idx:]
				self._logger.warn('Adding new-line character before server SSH key in authorized_keys file')
				filetool.write_file(authorized_keys_path, c)
				
		# Mount ephemeral devices
		# Seen on eucalyptus: 
		# 	- fstab contains invalid fstype and `mount -a` fails  
		mtab = Mtab()
		fstab = Fstab()
		for device in self._platform.instance_store_devices:
			if os.path.exists(device) and fstab.contains(device) and not mtab.contains(device):
				entry = fstab.find(device)[0]
				try:
					mount(device, entry.mpoint, ('-o', entry.options))
				except:
					self._logger.warn(sys.exc_info()[1])

	def on_reload(self):
		self._platform = bus.platform		
	
	def on_before_hello(self, message):
		"""
		@param message: Hello message
		"""
		
		message.aws_instance_id = self._platform.get_instance_id()
		message.aws_instance_type = self._platform.get_instance_type()		
		message.aws_ami_id = self._platform.get_ami_id()
		message.aws_avail_zone = self._platform.get_avail_zone()


	def on_before_host_init(self, message):
		"""
		@param message: HostInit message
		"""

		message.ssh_pub_key = self._platform.get_ssh_pub_key()

	def on_before_restart(self, message):
		"""
		@param message: Restart message
		@type message: scalarizr.messaging.Message 
		"""
		
		"""
		@todo Update ips, reset platform meta-data
 		@see http://docs.amazonwebservices.com/AWSEC2/latest/DeveloperGuide/index.html?Concepts_BootFromEBS.html#Stop_Start
		"""
		pass

	def on_before_message_send(self, queue, message):
		"""
		@todo: add aws specific here
		"""
		
		pass
	
