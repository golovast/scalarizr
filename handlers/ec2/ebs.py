'''
Created on Mar 1, 2010

@author: marat
'''

from scalarizr.handlers.block_device import BlockDeviceHandler
from scalarizr.platform.ec2 import ebstool


def get_handlers ():
	return [EbsHandler()]

class EbsHandler(BlockDeviceHandler):

	def __init__(self):
		BlockDeviceHandler.__init__(self, 'ebs')

	def get_devname(self, devname):
		return ebstool.get_ebs_devname(devname)
