'''
Created on Jan 6, 2010

@author: marat
@author: Dmytro Korsakov
@author: spike
'''

from __future__ import with_statement

# Core components
from scalarizr.bus import bus
from scalarizr.config import Configurator, BuiltinBehaviours, ScalarizrState
from scalarizr.service import CnfController
from scalarizr.handlers import HandlerError, ServiceCtlHandler
from scalarizr.messaging import Messages

# Libs
from scalarizr.libs.metaconf import Configuration, NoPathError
from scalarizr.util import system2, cached, firstmatched,\
	validators, software, initdv2, disttool
from scalarizr.linux import iptables
from scalarizr.util.filetool import read_file, write_file

# Stdlibs
import os, logging, shutil, re, time
from telnetlib import Telnet
from datetime import datetime
import ConfigParser
import cStringIO


BEHAVIOUR = SERVICE_NAME = BuiltinBehaviours.WWW
CNF_NAME = BEHAVIOUR
CNF_SECTION = BEHAVIOUR

BIN_PATH = 'binary_path'
APP_PORT = 'app_port'
HTTPS_INC_PATH = 'https_include_path'
APP_INC_PATH = 'app_include_path'
UPSTREAM_APP_ROLE = 'upstream_app_role'

class NginxInitScript(initdv2.ParametrizedInitScript):
	_nginx_binary = None
	
	def __init__(self):
		cnf = bus.cnf; ini = cnf.rawini
		self._nginx_binary = ini.get(CNF_SECTION, BIN_PATH)
		

		pid_file = None
		'''
		Saw on 8.04:
		--pid-path=/var/run/nginx
		but actual pid-file is /var/run/nginx.pid
		try:
			nginx = software.whereis('nginx')
			if nginx:
				out = system2((nginx[0], '-V'))[1]
				m = re.search("--pid-path=(.*?)\s", out)
				if m:
						pid_file = m.group(1)
		except:
			pass
		'''
						
		initdv2.ParametrizedInitScript.__init__(
			self, 
			'nginx', 
			'/etc/init.d/nginx',
			pid_file = pid_file, 
			socks=[initdv2.SockParam(80)]
		)

	def status(self):
		status = initdv2.ParametrizedInitScript.status(self)
		if not status and self.socks:
			ip, port = self.socks[0].conn_address
			telnet = Telnet(ip, port)
			telnet.write('HEAD / HTTP/1.0\n\n')
			if 'server: nginx' in telnet.read_all().lower():
				return initdv2.Status.RUNNING
			return initdv2.Status.UNKNOWN
		return status

	def configtest(self):
		out = system2('%s -t' % self._nginx_binary, shell=True)[1]
		if 'failed' in out.lower():
			raise initdv2.InitdError("Configuration isn't valid: %s" % out)
		
	def stop(self):
		if not self.running:
			return True
		ret =  initdv2.ParametrizedInitScript.stop(self)
		time.sleep(1)
		return ret
	
	def restart(self):
		self.configtest()
		ret = initdv2.ParametrizedInitScript.restart(self)
		time.sleep(1)
		return ret
	
		
initdv2.explore('nginx', NginxInitScript)


# Nginx behaviours configuration options
class NginxOptions(Configurator.Container):
	'''
	www behaviour
	'''
	cnf_name = CNF_NAME
	
	class binary_path(Configurator.Option):
		'''
		Path to nginx binary
		'''
		name = CNF_SECTION + '/binary_path'
		required = True
		
		@property
		@cached
		def default(self):
			return firstmatched(lambda p: os.access(p, os.F_OK | os.X_OK), 
					('/usr/sbin/nginx',	'/usr/local/nginx/sbin/nginx'), '')

		@validators.validate(validators.executable)
		def _set_value(self, v):
			Configurator.Option._set_value(self, v)
			
		value = property(Configurator.Option._get_value, _set_value)


	class app_port(Configurator.Option):
		'''
		App role port
		'''
		name = CNF_SECTION + '/app_port'
		default = '80'
		required = True
		
		@validators.validate(validators.portnumber())
		def _set_value(self, v):
			Configurator.Option._set_value(self, v)
		
		value = property(Configurator.Option._get_value, _set_value)
		

	class app_include_path(Configurator.Option):
		'''
		App upstreams configuration file path.
		'''
		name = CNF_SECTION + '/app_include_path'
		default = '/etc/nginx/app-servers.include'
		required = True
		
	class https_include_path(Configurator.Option):
		'''
		HTTPS configuration file path.
		'''
		name = CNF_SECTION + '/https_include_path'
		default = '/etc/nginx/https.include'
		required = True


def get_handlers():
	return [NginxHandler()]


class NginxCnfController(CnfController):
	def __init__(self):
		cnf = bus.cnf; ini = cnf.rawini
		nginx_conf_path = os.path.join(os.path.dirname(ini.get(CNF_SECTION, APP_INC_PATH)), 'nginx.conf')
		CnfController.__init__(self, BEHAVIOUR, nginx_conf_path, 'nginx', {"on":'1',"'off'":'0','off':'0'})
		
	@property
	def _software_version(self):
		return software.software_info('nginx').version

		
class NginxHandler(ServiceCtlHandler):
	
	backends_xpath = "upstream[@value='backend']/server"
	localhost = '127.0.0.1:80'
	
	def __init__(self):
		ServiceCtlHandler.__init__(self, BEHAVIOUR, initdv2.lookup('nginx'), NginxCnfController())
				
		self._logger = logging.getLogger(__name__)
		
		bus.define_events("nginx_upstream_reload")
		bus.on(init=self.on_init, reload=self.on_reload)
		self.on_reload()			
		
	def on_init(self):
		bus.on(
			start = self.on_start, 
			before_host_up = self.on_before_host_up,
			before_reboot_finish = self.on_before_reboot_finish
		)
		
		if self._cnf.state == ScalarizrState.BOOTSTRAPPING:
			self._stop_service('Configuring')
			self._insert_iptables_rules()
	
	def on_reload(self):
		self._queryenv = bus.queryenv_service		
		self._cnf = bus.cnf
		
		ini = self._cnf.rawini
		self._nginx_binary = ini.get(CNF_SECTION, BIN_PATH)		
		self._https_inc_path = ini.get(CNF_SECTION, HTTPS_INC_PATH)
		self._app_inc_path = ini.get(CNF_SECTION, APP_INC_PATH)		
		self._app_port = ini.get(CNF_SECTION, APP_PORT)
		try:
			self._upstream_app_role = ini.get(CNF_SECTION, UPSTREAM_APP_ROLE)
		except ConfigParser.Error:
			self._upstream_app_role = None
	
	def accept(self, message, queue, behaviour=None, platform=None, os=None, dist=None):
		return BEHAVIOUR in behaviour and \
			(message.name == Messages.HOST_UP or \
			message.name == Messages.HOST_DOWN or \
			message.name == Messages.BEFORE_HOST_TERMINATE or \
			message.name == Messages.VHOST_RECONFIGURE or \
			message.name == Messages.UPDATE_SERVICE_CONFIGURATION)		

	def get_initialization_phases(self, hir_message):
		self._phase = 'Configure Nginx'
		self._step_update_vhosts = 'Update virtual hosts'
		self._step_reload_upstream = 'Reload upstream'
		
		return {'before_host_up': [{
			'name': self._phase,
			'steps': [self._step_update_vhosts, self._step_reload_upstream]
		}]}

	def on_start(self): 
		if self._cnf.state == ScalarizrState.RUNNING:
			self._update_vhosts()
			self._reload_upstream()						
		
	def on_before_host_up(self, message):
		with bus.initialization_op as op:
			with op.phase(self._phase):
				with op.step(self._step_update_vhosts):
					self._update_vhosts()

				with op.step(self._step_reload_upstream):
					self._reload_upstream()

				bus.fire('service_configured', service_name=SERVICE_NAME)
		
	def on_before_reboot_finish(self, *args, **kwargs):
		self._insert_iptables_rules()
	
	def on_HostUp(self, message):
		self._reload_upstream()
	
	
	def on_HostDown(self, message):
		self._reload_upstream()


	def on_BeforeHostTerminate(self, message):
		if not os.access(self._app_inc_path, os.F_OK):
			self._logger.debug('File %s not exists. Nothing to do', self._app_inc_path)
			return

		include = Configuration('nginx')	
		include.read(self._app_inc_path)
		
		server_ip = '%s:%s' % (message.local_ip or message.remote_ip, self._app_port)
		backends = include.get_list(self.backends_xpath)
		if server_ip in backends:
			include.remove(self.backends_xpath, server_ip)
			# Add 127.0.0.1 If it was the last backend
			if len(backends) == 1:
				include.add(self.backends_xpath, self.localhost)

		include.write(self._app_inc_path)
		self._reload_service('%s is to be terminated' % server_ip)


	def on_VhostReconfigure(self, message):
		self._logger.info("Received virtual hosts update notification. Reloading virtual hosts configuration")
		self._update_vhosts()
		self._reload_upstream(True)


	def _test_config(self):
		self._logger.debug("Testing new configuration")
		try:
			self._init_script.configtest()
		except initdv2.InitdError, e:
			self._logger.error("Configuration error detected: %s Reverting configuration." % str(e))

			if os.path.isfile(self._app_inc_path):
				shutil.move(self._app_inc_path, self._app_inc_path+".junk")
			else:
				self._logger.debug('%s does not exist', self._app_inc_path)
			if os.path.isfile(self._app_inc_path+".save"):
				shutil.move(self._app_inc_path+".save", self._app_inc_path)
			else:
				self._logger.debug('%s does not exist', self._app_inc_path+".save")
		else:
			self._reload_service()

	def _dump_config(self, obj):
		output = cStringIO.StringIO()
		obj.write_fp(output, close = False)
		return output.getvalue()

	def _update_main_config(self):
		config_dir = os.path.dirname(self._app_inc_path)
		nginx_conf_path = os.path.join(config_dir, 'nginx.conf')
		
		if not hasattr(self, '_config'):
			try:
				self._config = Configuration('nginx')
				self._config.read(nginx_conf_path)
			except (Exception, BaseException), e:
				raise HandlerError('Cannot read/parse nginx main configuration file: %s' % str(e))
			
		# Patch nginx.conf 
		self._logger.debug('Update main configuration file')
		dump = self._dump_config(self._config)
			
		if self._app_inc_path in self._config.get_list('http/include'):
			self._logger.debug('File %s already included into nginx main config %s'% 
					(self._app_inc_path, nginx_conf_path))

			#preventing nginx from crashing if user removed upstream file
			if not os.path.exists(self._app_inc_path):
				self._config.remove('http/include', self._app_inc_path)
				self._logger.debug('include %s removed as file does not exist.' % self._app_inc_path)
				
		elif os.path.exists(self._app_inc_path):
				self._logger.debug("including path to upstream list into nginx main config")
				self._config.add('http/include', self._app_inc_path)
							
		if not 'http://backend' in self._config.get_list('http/server/location/proxy_pass') :
			# Comment http/server
			self._logger.debug('comment http/server section')
			self._config.comment('http/server')
			self._config.read(os.path.join(bus.share_path, "nginx/server.tpl"))
			
			if disttool.is_debian_based():
				# Comment /etc/nginx/sites-enabled/*
				try:
					i = self._config.get_list('http/include').index('/etc/nginx/sites-enabled/*')
					self._config.comment('http/include[%d]' % (i+1,))
					self._logger.debug('comment site-enabled include')
				except ValueError, IndexError:
					self._logger.debug('site-enabled include already commented')
			elif disttool.is_redhat_based():
				def_host_path = '/etc/nginx/conf.d/default.conf'
				if os.path.exists(def_host_path):
					default_host = Configuration('nginx')
					default_host.read(def_host_path)
					default_host.comment('server')
					default_host.write(def_host_path)
					

		if dump == self._dump_config(self._config):	
			self._logger.debug("Main nginx config wasn`t changed")
		else:
			# Write new nginx.conf 
			if not os.path.exists(nginx_conf_path + '.save'):
				shutil.copy(nginx_conf_path, nginx_conf_path + '.save')
			self._config.write(nginx_conf_path)		
	
	
	def _reload_upstream(self, force_reload=False):

		backend_include = Configuration('nginx')
		if os.path.exists(self._app_inc_path):
			backend_include.read(self._app_inc_path)
		else:
			backend_include.read(os.path.join(bus.share_path, 'nginx/app-servers.tpl'))

		# Create upstream hosts configuration
		if not self._upstream_app_role:
			kwds = dict(behaviour=BuiltinBehaviours.APP)
		else:
			kwds = dict(role_name=self._upstream_app_role)
		list_roles = self._queryenv.list_roles(**kwds)
		servers = []
		
		for app_serv in list_roles:
			for app_host in app_serv.hosts :
				server_str = '%s:%s' % (app_host.internal_ip or app_host.external_ip, self._app_port)
				servers.append(server_str)
		self._logger.debug("QueryEnv returned list of app servers: %s" % servers)
		
		# Add cloudfoundry routers
		for role in self._queryenv.list_roles(behaviour=BuiltinBehaviours.CF_ROUTER):
			for host in role.hosts:
				servers.append('%s:%s' % (host.internal_ip or host.external_ip, 2222))

		for entry in backend_include.get_list(self.backends_xpath):
			for server in servers:
				if entry.startswith(server):
					self._logger.debug("Server %s already in upstream list" % server)
					servers.remove(server)
					break
			else:
				self._logger.debug("Removing old entry %s from upstream list" % entry) 
				backend_include.remove(self.backends_xpath, entry)
		
		for server in servers:
			self._logger.debug("Adding new server %s to upstream list" % server)
			backend_include.add(self.backends_xpath, server)
			
		if not backend_include.get_list(self.backends_xpath):
			self._logger.debug("Scalr returned empty app hosts list. Adding localhost only")
			backend_include.add(self.backends_xpath, self.localhost)
		self._logger.info('Upstream servers: %s', ' '.join(backend_include.get_list(self.backends_xpath)))
		
		# Https configuration
		# openssl req -new -x509 -days 9999 -nodes -out cert.pem -keyout cert.key
		if os.access(self._https_inc_path, os.F_OK) \
				and os.access(self._cnf.key_path("https.crt"), os.F_OK) \
				and os.access(self._cnf.key_path("https.key"), os.F_OK):
			self._logger.debug('Add https include %s', self._https_inc_path)
			backend_include.set('include', self._https_inc_path, force=True)
		else:
			self._logger.debug('Removing https include %s', self._https_inc_path)
			try:
				backend_include.remove('include', self._https_inc_path)
			except NoPathError:
				pass

		
		old_include = None
		if os.path.isfile(self._app_inc_path):
			self._logger.debug("Reading old configuration from %s" % self._app_inc_path)
			old_include = Configuration('nginx')
			old_include.read(self._app_inc_path)
			
		if old_include \
				and not force_reload \
				and	backend_include.get_list(self.backends_xpath) == old_include.get_list(self.backends_xpath) :
			self._logger.debug("nginx upstream configuration unchanged")
		else:
			self._logger.debug("nginx upstream configuration was changed")
			
			if os.access(self._app_inc_path, os.F_OK):
				self._logger.debug('Backup file %s as %s', self._app_inc_path, self._app_inc_path + '.save')				
				shutil.move(self._app_inc_path, self._app_inc_path+".save")
				
			self._logger.debug('Write new %s', self._app_inc_path)
			backend_include.write(self._app_inc_path)
			
			self._update_main_config()
			
			self._test_config()

		bus.fire("nginx_upstream_reload")	

	

	def _insert_iptables_rules(self, *args, **kwargs):
		if iptables.enabled():
			iptables.FIREWALL.ensure([
				{"jump": "ACCEPT", "protocol": "tcp", "match": "tcp", "dport": "80"},
				{"jump": "ACCEPT", "protocol": "tcp", "match": "tcp", "dport": "443"},
			])

		"""
		iptables = IpTables()
		if iptables.enabled():
			iptables.insert_rule(None, RuleSpec(dport=80, jump='ACCEPT', protocol=P_TCP))
			iptables.insert_rule(None, RuleSpec(dport=443, jump='ACCEPT', protocol=P_TCP))
		"""


	def _update_vhosts(self):
		self._logger.debug("Requesting virtual hosts list")
		received_vhosts = self._queryenv.list_virtual_hosts()
		self._logger.debug("Virtual hosts list obtained (num: %d)", len(received_vhosts))
	
		https_config = ''
		cert_path = self._cnf.key_path("https.crt")
		pk_path = self._cnf.key_path("https.key")
		ssl_present = any(vhost.https for vhost in received_vhosts)

		if ssl_present:
			https_certificate = self._queryenv.get_https_certificate()

			if https_certificate[0]:
				msg = 'Writing ssl cert'
				cert = https_certificate[0]
				write_file(cert_path, cert, msg=msg, logger=self._logger)
			else:
				self._logger.error('Scalr returned empty SSL Cert')
				return

			if len(https_certificate)>1 and https_certificate[1]:
				msg = 'Writing ssl key'
				pk = https_certificate[1]
				write_file(pk_path, pk, msg=msg, logger=self._logger)
			else:
				self._logger.error('Scalr returned empty SSL Cert')
				return

			if https_certificate[2]:
				msg = 'Appending CA cert to cert file'
				cert = https_certificate[2]
				write_file(cert_path, '\n' + https_certificate[2], mode='a', msg=msg, logger=self._logger)
		else:
			self._logger.debug('No SSL vhosts obtained. Removing old SSL keys.')
			for key_path in (cert_path, pk_path):
				if os.path.exists(key_path):
					os.remove(key_path)
					self._logger.debug('%s deleted' % key_path)

		if received_vhosts:

			for vhost in received_vhosts:
				if vhost.hostname and vhost.type == 'nginx': #and vhost.https
					raw = vhost.raw.replace('/etc/aws/keys/ssl/https.crt',cert_path)
					raw = raw.replace('/etc/aws/keys/ssl/https.key',pk_path)
					https_config += raw + '\n'

			if https_config:
				if os.path.exists(self._https_inc_path)\
				and read_file(self._https_inc_path, logger=self._logger):
					time_suffix = str(datetime.now()).replace(' ','.')
					shutil.move(self._https_inc_path, self._https_inc_path + time_suffix)

				msg = 'Writing virtualhosts to https.include'
				write_file(self._https_inc_path, https_config, msg=msg, logger=self._logger)

		else:
			self._logger.debug('Scalr returned empty virtualhost list. Removing junk files.')
			if os.path.exists(self._https_inc_path):
				os.remove(self._https_inc_path)
				self._logger.debug('%s deleted' % self._https_inc_path)

