__author__ = 'Nick Demyanchuk'

from scalarizr.bus import bus
from scalarizr import handlers


def get_handlers ():
	return [GceLifeCycle()]

class GceLifeCycle(handlers.Handler):

	def __init__(self):
		bus.on(init=self.on_init)


	def on_init(self, *args, **kwargs):
		bus.on(before_hello=self.on_before_hello)


	def on_before_hello(self, message):
		"""
		@param message: Hello message
		"""

		pl = bus.platform
		message.body['gce'] = {
			'serverId': pl.get_instance_id(),
			'cloudLocation ': pl.get_zone(),
			'serverName': pl.get_hostname().split('.')[0],
			'machineType': pl.get_machine_type()
		}

