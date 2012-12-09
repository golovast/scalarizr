'''
Created on 22.01.2010

@author: marat
@author: Dmytro Korsakov
'''
import re
import logging
import logging.config
import threading
import traceback
import cStringIO
import string
import os

from scalarizr.bus import bus
from scalarizr.config import ScalarizrState
from scalarizr.messaging import Queues, Messages


INTERVAL_RE = re.compile('((?P<minutes>\d+)min\s?)?((?P<seconds>\d+)s)?')

class RotatingFileHandler(logging.handlers.RotatingFileHandler):
	def __init__(self, filename, mode, maxBytes, backupCount, chmod = 0600):
		logging.handlers.RotatingFileHandler.__init__(self, filename, mode, maxBytes, backupCount)
		try:
			os.chown(self.baseFilename, os.getuid(), os.getgid())
			os.chmod(self.baseFilename, chmod)
		except OSError:
			pass
		
		
class MessagingHandler(logging.Handler):
	
	num_entries = None
	send_interval = None
	
	_sender_thread = None
	_send_event = None
	_stop_event = None
	_initialized = False
	
	_messaging_enabled = False
	_msgsrv_subscribed = False
	_sending_message = False
	
	def __init__(self, num_entries = 20, send_interval = '30s'):
		logging.Handler.__init__(self)	
		
		m = INTERVAL_RE.match(send_interval)
		self.send_interval = (int(m.group('seconds') or 0) + 60*int(m.group('minutes') or 0)) or 1
		self.num_entries = num_entries
		self._logger = logging.getLogger(__name__)
		bus.on("shutdown", self.on_shutdown)
		
	def __del__(self):
		if self._send_event:
			self._stop_event.set()
			self._send_event.set()
			
	def _init(self):
		self.entries = []		
		
		self._send_event = threading.Event()
		self._stop_event = threading.Event()
		self._lock = threading.Lock()
		
		self._sender_thread = threading.Thread(target=self._sender)
		self._sender_thread.setDaemon(True)
		self._sender_thread.start()		

		self._initialized = True		

	def _enable_messaging(self):
		self._logger.debug('Enabling log messaging')
		self._messaging_enabled = True
		# Stop listening messaging events
		m = bus.messaging_service; 
		if m:
			producer = m.get_producer() 
			consumer = m.get_consumer()
			producer.un('send', self.on_out_message_send)
			if self.on_in_message_received in consumer.listeners:
				consumer.listeners.remove(self.on_in_message_received)

	def _subscribe_msgsrv(self):
		self._msgsrv_subscribed = True
		m = bus.messaging_service; producer = m.get_producer(); consumer = m.get_consumer()
		producer.on('send', self.on_out_message_send)
		consumer.listeners.append(self.on_in_message_received)

	def on_out_message_send(self, queue, message):
		self._logger.debug('Handled on_out_message_send message: %s', message.name)
		if message.name != Messages.HOST_INIT:
			self._enable_messaging()
	
	def on_in_message_received(self, message, queue):
		self._logger.debug('Handled on_in_message_received message: %s', message.name)		
		if message.name == Messages.HOST_INIT_RESPONSE:
			self._enable_messaging()


	def emit(self, record):
		if not self._messaging_enabled:
			'''
			Logging via scalarizr messaging is not enabled by startup. 
			It'is related with Scalarizr one-time crypto key:
			Crypto key updated on ->HostInit, and we know Scalr 100% obtained it on <-HostInitResponse
			'''
			cnf = bus.cnf
			if cnf and cnf.state == ScalarizrState.RUNNING:
				self._enable_messaging()
			elif bus.messaging_service and not self._msgsrv_subscribed:
				self._subscribe_msgsrv()
		
		if self._sending_message and record.name.startswith('scalarizr.messaging'):
			# Skip all transport logs 
			return
				
		if not self._initialized:
			self._init()
		
		msg = str(record.msg) % record.args if record.args else str(record.msg)
		
		stack_trace = None
		if record.exc_info:
			output = cStringIO.StringIO()
			traceback.print_tb(record.exc_info[2], file=output)
			stack_trace =  output.getvalue()
			output.close()			

		ent = dict(
			name = record.name,
			level = record.levelname,
			pathname = record.pathname,
			lineno = record.lineno,
			msg = msg,
			stack_trace = stack_trace
		)
		
		self._lock.acquire()		
		try:
			self.entries.append(ent)
			if self._time_has_come():
				self._send_event.set()
		finally:
			self._lock.release()
		
	def _time_has_come(self):
		return len(self.entries) >= self.num_entries
	
	def on_shutdown(self):
		self._stop_event.set()
		self._send_message()
		self._sender_thread.join()
			
	def _send_message(self):
		if not bus.messaging_service:
			return
		
		entries = ()
		self._lock.acquire()
		try:
			entries = self.entries[:]
			self.entries = []
		finally:
			self._lock.release()
		
		try:
			if entries:
				msg_service = bus.messaging_service
				message = msg_service.new_message(Messages.LOG)
				message.body["entries"] = entries
				self._sending_message = True
				msg_service.get_producer().send(Queues.LOG, message)
		except (BaseException, Exception):
			# silently
			pass	
		finally:
			self._sending_message = False
	
	def _sender(self):
		while not self._stop_event.isSet():
			try:
				for i in range(self.send_interval * 10):
					self._send_event.wait(0.1)
					if self._stop_event.isSet():
						return
				if self._messaging_enabled and self.entries:
					self._send_message()
			finally:
				self._send_event.clear()

	
def fix_py25_handler_resolving():
	
	def _resolve(name):
		"""Resolve a dotted name to a global object."""
		name = string.split(name, '.')
		used = name.pop(0)
		found = __import__(used)
		for n in name:
			used = used + '.' + n
			try:
				found = getattr(found, n)
			except AttributeError:
				__import__(used)
				found = getattr(found, n)
		return found
	
	def _strip_spaces(alist):
		return map(lambda x: string.strip(x), alist)
	
	def _install_handlers(cp, formatters):
		"""Install and return handlers"""
		hlist = cp.get("handlers", "keys")
		if not len(hlist):
			return {}
		hlist = string.split(hlist, ",")
		hlist = _strip_spaces(hlist)
		handlers = {}
		fixups = [] #for inter-handler references
		for hand in hlist:
			sectname = "handler_%s" % hand
			klass = cp.get(sectname, "class")
			opts = cp.options(sectname)
			if "formatter" in opts:
				fmt = cp.get(sectname, "formatter")
			else:
				fmt = ""
			try:
				klass = eval(klass, vars(logging))
			except (AttributeError, NameError):
				klass = _resolve(klass)
			args = cp.get(sectname, "args")
			args = eval(args, vars(logging))
			h = klass(*args)
			if "level" in opts:
				level = cp.get(sectname, "level")
				h.setLevel(logging._levelNames[level])
			if len(fmt):
				h.setFormatter(formatters[fmt])
			if issubclass(klass, logging.handlers.MemoryHandler):
				if "target" in opts:
					target = cp.get(sectname,"target")
				else:
					target = ""
				if len(target): #the target handler may not be loaded yet, so keep for later...
					fixups.append((h, target))
			handlers[hand] = h
			
		#now all handlers are loaded, fixup inter-handler references...
		for h, t in fixups:
			h.setTarget(handlers[t])
		return handlers

	logging.config._install_handlers = _install_handlers
	logging.config._resolve = _resolve
	logging.config._strip_spaces = _strip_spaces	
			