'''
Created on Jun 26, 2012

@author: marat
'''

from BaseHTTPServer import HTTPServer
import threading
import sys

try:
	import selectmodule as select
except ImportError:
	import select
	
class HTTPServer25(HTTPServer):
	
	def __init__(self, server_address, RequestHandlerClass):
		HTTPServer.__init__(self, server_address, RequestHandlerClass)
		self.__is_shut_down = threading.Event()
		self.__serving = False
	
	def serve_forever(self, poll_interval=0.5):
		self.__serving = True
		self.__is_shut_down.clear()
		while self.__serving:
			# XXX: Consider using another file descriptor or
			# connecting to the socket to wake this up instead of
			# polling. Polling reduces our responsiveness to a
			# shutdown request and wastes cpu at all other times.
			r, w, e = select.select([self], [], [], poll_interval)
			if r:
				self.handle_request()
		
		self.__is_shut_down.set()
	
	def shutdown(self):
		self.__serving = False
		self.__is_shut_down.wait()

		
		
def patch():
	import BaseHTTPServer
	BaseHTTPServer.HTTPServer = HTTPServer25

	import scalarizr.externals.logging as logging
	import scalarizr.externals.logging.config as logging_config
	import scalarizr.externals.logging.handlers as logging_handlers
	
	sys.modules['logging'] = logging
	sys.modules['logging.config'] = logging_config
	sys.modules['logging.handlers'] = logging_handlers
	 