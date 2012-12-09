'''
Created on Nov 2, 2011

@author: marat
'''


from scalarizr.handlers.block_device import BlockDeviceHandler


def get_handlers ():
	return [BlockDeviceHandler('csvol')]
