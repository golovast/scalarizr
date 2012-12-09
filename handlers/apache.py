'''
Created on Dec 25, 2009

@author: marat
@author: Dmytro Korsakov
'''

from __future__ import with_statement

# Core
from scalarizr.bus import bus
from scalarizr.config import BuiltinBehaviours, ScalarizrState
from scalarizr.service import CnfController
from scalarizr.handlers import HandlerError, ServiceCtlHandler, operation
from scalarizr.messaging import Messages

# Libs
from scalarizr.libs.metaconf import Configuration, ParseError, MetaconfError,\
	NoPathError, strip_quotes
from scalarizr.util import disttool, firstmatched, software, wait_until
from scalarizr.util import initdv2, system2, dynimp
from scalarizr.util.initdv2 import InitdError
from scalarizr.linux import iptables
from scalarizr.util.filetool import read_file, write_file
from scalarizr import filetool

# Stdlibs
import logging, os, re
from telnetlib import Telnet
import sys
import time
import shutil, pwd

BEHAVIOUR = SERVICE_NAME = BuiltinBehaviours.APP
CNF_SECTION = BEHAVIOUR
CNF_NAME = BEHAVIOUR + '.ini'
#APP_CONF_PATH = 'apache_conf_path'
APACHE_CONF_PATH = '/etc/apache2/apache2.conf' if disttool.is_debian_based() else '/etc/httpd/conf/httpd.conf'
VHOSTS_PATH = 'private.d/vhosts'
VHOST_EXTENSION = '.vhost.conf'
LOGROTATE_CONF_PATH = '/etc/logrotate.d/scalarizr_app'
LOGROTATE_CONF_DEB_RAW = """/var/log/http-*.log {
         weekly
         missingok
         rotate 52
         compress
         delaycompress
         notifempty
         create 640 root adm
         sharedscripts
         postrotate
                 if [ -f "`. /etc/apache2/envvars ; echo ${APACHE_PID_FILE:-/var/run/apache2.pid}`" ]; then
                         /etc/init.d/apache2 reload > /dev/null
                 fi
         endscript
}
"""

LOGROTATE_CONF_REDHAT_RAW = """/var/log/http-*.log {
     missingok
     notifempty
     sharedscripts
     delaycompress
     postrotate
         /sbin/service httpd reload > /dev/null 2>/dev/null || true
     endscript
}
"""

class ApacheInitScript(initdv2.ParametrizedInitScript):
	_apachectl = None

	def __init__(self):
		if disttool.is_redhat_based():
			self._apachectl = '/usr/sbin/apachectl'
			initd_script 	= '/etc/init.d/httpd'
			pid_file 		= '/var/run/httpd/httpd.pid' if disttool.version_info()[0] == 6 else '/var/run/httpd.pid' 
		elif disttool.is_debian_based():
			self._apachectl = '/usr/sbin/apache2ctl'		
			initd_script 	= '/etc/init.d/apache2'
			pid_file = None
			if os.path.exists('/etc/apache2/envvars'):
				pid_file = system2('/bin/sh', stdin='. /etc/apache2/envvars; echo -n $APACHE_PID_FILE')[0]
			if not pid_file:
				pid_file = '/var/run/apache2.pid'
		else:
			self._apachectl = '/usr/sbin/apachectl'
			initd_script 	= '/etc/init.d/apache2'
			pid_file 		= '/var/run/apache2.pid'
		
		initdv2.ParametrizedInitScript.__init__(
			self,
			'apache',
			initd_script,
			pid_file = pid_file
		)

	def reload(self):
		if self.running:
			self.configtest()
			out, err, retcode = system2(self._apachectl + ' graceful', shell=True)
			if retcode > 0:
				raise initdv2.InitdError('Cannot reload apache: %s' % err)
		else:
			raise InitdError('Service "%s" is not running' % self.name, InitdError.NOT_RUNNING)

	def status(self):
		status = initdv2.ParametrizedInitScript.status(self)
		# If 'running' and socks were passed
		if not status and self.socks:
			ip, port = self.socks[0].conn_address
			try:
				expected = 'server: apache'
				telnet = Telnet(ip, port)
				telnet.write('HEAD / HTTP/1.0\n\n')
				if expected in telnet.read_until(expected, 5).lower():
					return initdv2.Status.RUNNING
			except EOFError:
				pass
			return initdv2.Status.NOT_RUNNING
		return status

	def configtest(self):
		out = system2(self._apachectl +' configtest', shell=True)[1]
		if 'error' in out.lower():
			raise initdv2.InitdError("Configuration isn't valid: %s" % out)

	def start(self):
		ret = initdv2.ParametrizedInitScript.start(self)
		if self.pid_file:
			try:
				wait_until(lambda: os.path.exists(self.pid_file) or self._main_process_started(), sleep=0.2, timeout=30)
			except (Exception, BaseException), e:
				raise initdv2.InitdError("Cannot start Apache (%s)" % str(e))
		time.sleep(0.5)
		return True

	def restart(self):
		self.configtest()
		ret = initdv2.ParametrizedInitScript.restart(self)
		if self.pid_file:
			try:
				wait_until(lambda: os.path.exists(self.pid_file), sleep=0.2, timeout=5, 
						error_text="Apache pid file %s doesn't exists" % self.pid_file)
			except:
				raise initdv2.InitdError("Cannot start Apache: pid file %s hasn't been created" % self.pid_file)
		time.sleep(0.5)
		return ret
	
	def _main_process_started(self):
		res = False
		bin = '/usr/sbin/apache2' if disttool.is_debian_based() else '/usr/sbin/httpd'
		group = 'www-data' if disttool.is_debian_based() else 'apache'
		try:
			'''
			_first_ scalarizr start returns error:
			(ps (code: 1) <out>:  <err>:  <args>: ('ps', '-G', 'www-data', '-o', 'command', '--no-headers')
			'''
			out = system2(('ps', '-G', group, '-o', 'command', '--no-headers'), raise_exc=False)[0]
			res = True if len([p for p in out.split('\n') if bin in p]) else False
		except:
			pass
		return res

initdv2.explore('apache', ApacheInitScript)



class ApacheCnfController(CnfController):

	def __init__(self):
		CnfController.__init__(self, BEHAVIOUR, APACHE_CONF_PATH, 'apache', {'1':'on','0':'off'})

	@property
	def _software_version(self):
		return software.software_info('apache').version


def get_handlers ():
	return [ApacheHandler()]

def reload_apache_conf(f):
	def g(self,*args):
		self._config = Configuration('apache')
		try:
			self._config.read(self._httpd_conf_path)
		except (OSError, MetaconfError, ParseError), e:
			raise HandlerError('Cannot read Apache config %s : %s' % (self._httpd_conf_path, str(e)))
		f(self,*args)
	return g


class ApacheHandler(ServiceCtlHandler):
	
	_config = None
	_logger = None
	_queryenv = None
	_cnf = None
	'''
	@type _cnf: scalarizr.config.ScalarizrCnf
	'''

	def __init__(self):
		
		
		self._logger = logging.getLogger(__name__)		
		ServiceCtlHandler.__init__(self, SERVICE_NAME, initdv2.lookup('apache'), ApacheCnfController())
		
		bus.on(init=self.on_init, reload=self.on_reload)
		bus.define_events(
			'apache_rpaf_reload'
		)
		self.on_reload()


	def on_init(self):
		bus.on(
			start = self.on_start, 
			before_host_up = self.on_before_host_up,
			before_reboot_finish = self.on_before_reboot_finish
		)

		self._logger.debug('State: %s', self._cnf.state)
		if self._cnf.state == ScalarizrState.BOOTSTRAPPING:
			self._logger.debug('Bootstrapping routines')
			self._stop_service('Configuring')			
			self._insert_iptables_rules()

	def on_reload(self):
		self._queryenv = bus.queryenv_service		
		self._cnf = bus.cnf
		self._httpd_conf_path = APACHE_CONF_PATH
		self._config = Configuration('apache')
		self._config.read(self._httpd_conf_path)


	def accept(self, message, queue, behaviour=None, platform=None, os=None, dist=None):
		return BEHAVIOUR in behaviour and \
			(message.name == Messages.VHOST_RECONFIGURE or \
			message.name == Messages.UPDATE_SERVICE_CONFIGURATION or \
			message.name == Messages.HOST_UP or \
			message.name == Messages.HOST_DOWN or \
			message.name == Messages.BEFORE_HOST_TERMINATE)

	def get_initialization_phases(self, hir_message):
		self._phase = 'Configure Apache'
		self._step_update_vhosts = 'Update virtual hosts'
		self._step_reload_rpaf = 'Reload RPAF'
		
		return {'before_host_up': [{
			'name': self._phase,
			'steps': [self._step_update_vhosts, self._step_reload_rpaf]
		}]}
	
	def on_start(self):
		if self._cnf.state == ScalarizrState.RUNNING:
			self._update_vhosts()			
			self._rpaf_reload()

	def on_before_host_up(self, message):
		
		with bus.initialization_op as op:
			with op.phase(self._phase):
				with op.step(self._step_update_vhosts):
					self._update_vhosts()
				with op.step(self._step_reload_rpaf):
					self._rpaf_reload()
				bus.fire('service_configured', service_name=SERVICE_NAME)

	def on_before_reboot_finish(self, *args, **kwargs):
		self._insert_iptables_rules()

	def on_HostUp(self, message):
		if message.local_ip and message.behaviour and BuiltinBehaviours.WWW in message.behaviour:
			self._rpaf_modify_proxy_ips([message.local_ip], operation='add')

	def on_HostDown(self, message):
		if message.local_ip and message.behaviour and BuiltinBehaviours.WWW in message.behaviour:
			self._rpaf_modify_proxy_ips([message.local_ip], operation='remove')
	
	on_BeforeHostTerminate = on_HostDown

	@reload_apache_conf
	def on_VhostReconfigure(self, message):
		self._logger.info("Received virtual hosts update notification. Reloading virtual hosts configuration")
		self._update_vhosts()
		self._reload_service('virtual hosts have been updated')

	def _insert_iptables_rules(self):
		if iptables.enabled():
			iptables.FIREWALL.ensure([
				{"jump": "ACCEPT", "protocol": "tcp", "match": "tcp", "dport": "80"},
				{"jump": "ACCEPT", "protocol": "tcp", "match": "tcp", "dport": "443"},
			])

			"""
			iptables.insert_rule(None, RuleSpec(dport=80, jump='ACCEPT', protocol=P_TCP))		
			iptables.insert_rule(None, RuleSpec(dport=443, jump='ACCEPT', protocol=P_TCP))
			"""

	def _rpaf_modify_proxy_ips(self, ips, operation=None):
		self._logger.debug('Modify RPAFproxy_ips (operation: %s, ips: %s)', operation, ','.join(ips))
		file = firstmatched(
			lambda x: os.access(x, os.F_OK),
			('/etc/httpd/conf.d/mod_rpaf.conf', '/etc/apache2/mods-available/rpaf.conf')
		)
		if file:
			rpaf = Configuration('apache')
			rpaf.read(file)
			
			if operation == 'add' or operation == 'remove':
				proxy_ips = set(re.split(r'\s+', rpaf.get('.//RPAFproxy_ips')))
				if operation == 'add':
					proxy_ips |= set(ips)
				else:
					proxy_ips -= set(ips)
			elif operation == 'update':
				proxy_ips = set(ips)
			if not proxy_ips:
				proxy_ips.add('127.0.0.1')
				
			self._logger.info('RPAFproxy_ips: %s', ' '.join(proxy_ips))
			rpaf.set('.//RPAFproxy_ips', ' '.join(proxy_ips))
			
			#fixing bug in rpaf 0.6-2
			if disttool.is_debian_based():
				pm = dynimp.package_mgr()
				if '0.6-2' == pm.installed('libapache2-mod-rpaf'):
					try:
						self._logger.debug('Patching IfModule value in rpaf.conf')
						rpaf.set("./IfModule[@value='mod_rpaf.c']", {'value': 'mod_rpaf-2.0.c'})
					except NoPathError:
						pass
			
			rpaf.write(file)
			st = os.stat(self._httpd_conf_path)
			os.chown(file, st.st_uid, st.st_gid)
			
			
			self._reload_service('Applying new RPAF proxy IPs list')
		else:
			self._logger.debug('Nothing to do with rpaf: mod_rpaf configuration file not found')


	def _rpaf_reload(self):
		
		lb_hosts = []
		for role in self._queryenv.list_roles(behaviour=BuiltinBehaviours.WWW):
			for host in role.hosts:
				lb_hosts.append(host.internal_ip)
		self._rpaf_modify_proxy_ips(lb_hosts, operation='update')
		bus.fire('apache_rpaf_reload')


	def _update_vhosts(self):
		vhosts_path = os.path.join(bus.etc_path, VHOSTS_PATH)
		if not os.path.exists(vhosts_path):
			if not vhosts_path:
				self._logger.error('Property vhosts_path is empty.')
			else:
				self._logger.info("Virtual hosts dir %s doesn't exist. Creating", vhosts_path)
				try:
					os.makedirs(vhosts_path)
					self._logger.debug("Virtual hosts dir %s created", vhosts_path)
				except OSError, e:
					self._logger.error("Cannot create dir %s. %s", vhosts_path, e.strerror)
					raise

		self.server_root = self._get_server_root()
		
		cert_path = bus.etc_path + '/private.d/keys'
		self._patch_ssl_conf(cert_path)
		
		self._logger.debug("Requesting virtual hosts list")
		received_vhosts = self._queryenv.list_virtual_hosts()
		self._logger.debug("Virtual hosts list obtained (num: %d)", len(received_vhosts))
		
		self._logger.debug("Deleting old vhosts configuration files")
		for fname in os.listdir(vhosts_path):
			if '000-default' == fname:
				continue

			old_vhost_path = os.path.join(vhosts_path, fname)
			for vhost in received_vhosts:
				new_vhost_path = self.get_vhost_filename(vhost.hostname, vhost.https)
				if new_vhost_path == old_vhost_path:
					break
			else:
				if os.path.isfile(old_vhost_path):
					try:
						self._logger.debug("Removing old vhost: %s" % old_vhost_path)
						os.remove(old_vhost_path)
					except OSError, e:
						self._logger.error('Cannot delete vhost file %s. %s', old_vhost_path, e.strerror)
				
				if os.path.islink(old_vhost_path):
					try:
						os.unlink(old_vhost_path)
					except OSError, e:
						self._logger.error('Cannot delete vhost link %s. %s', old_vhost_path, e.strerror)					
		self._logger.debug("Old vhosts configuration files deleted")
		

		self._logger.debug("Creating new vhosts configuration files")
		https_certificate = None
		for vhost in received_vhosts:
			if (None == vhost.hostname) or (None == vhost.raw):
				continue
			
			self._logger.debug("Processing %s", vhost.hostname)
			if vhost.https:
				try:
					if not https_certificate:
						self._logger.debug("Retrieving ssl cert and private key from Scalr.")
						https_certificate = self._queryenv.get_https_certificate()
				except:
					self._logger.error('Cannot retrieve ssl cert and private key from Scalr.')
					raise
				else:
					if not https_certificate[0]:
						self._logger.error("Scalr returned empty SSL cert")
					elif not https_certificate[1]:
						self._logger.error("Scalr returned empty SSL key")
					else:
						self._logger.debug("Saving SSL certificates for %s",vhost.hostname)
						key_error_message = 'Cannot write SSL key files to %s.' % cert_path
						cert_error_message = 'Cannot write SSL certificate files to %s.' % cert_path
						ca_cert_error_message = 'Cannot write CA certificate to %s.' % cert_path
						
						for key_file in ['https.key', vhost.hostname + '.key']:
							write_file(os.path.join(cert_path,key_file), https_certificate[1], error_msg=key_error_message, logger=self._logger)
							os.chmod(cert_path + '/' + key_file, 0644)
													
						for cert_file in ['https.crt', vhost.hostname + '.crt']:
							write_file(os.path.join(cert_path,cert_file), https_certificate[0], error_msg=cert_error_message, logger=self._logger)
							os.chmod(cert_path + '/' + cert_file, 0644)
							
						if https_certificate[2]:
							for filename in ('https-ca.crt', vhost.hostname + '-ca.crt'):
								write_file(os.path.join(cert_path, filename), https_certificate[2], error_msg=ca_cert_error_message, logger=self._logger)
								os.chmod(os.path.join(cert_path, filename), 0644)
				
				self._logger.debug('Enabling SSL virtual host %s', vhost.hostname)
				
				vhost_fullpath = self.get_vhost_filename(vhost.hostname, ssl=True)
				raw = vhost.raw.replace('/etc/aws/keys/ssl', cert_path)
				vhost_error_message = 'Cannot write vhost file %s.' % vhost_fullpath
				write_file(vhost_fullpath, raw, error_msg=vhost_error_message, logger = self._logger)
				
				self._create_vhost_paths(vhost_fullpath) 	

				self._logger.debug("Checking apache SSL mod")
				self._check_mod_ssl()
				
				self._logger.debug("Changing paths in ssl.conf")
				self._patch_ssl_conf(cert_path)
				
			else:
				self._logger.debug('Enabling virtual host %s', vhost.hostname)
				vhost_fullpath = self.get_vhost_filename(vhost.hostname)
				vhost_error_message = 'Cannot write vhost file %s.' % vhost_fullpath
				write_file(vhost_fullpath, vhost.raw, error_msg=vhost_error_message, logger=self._logger)
				self._logger.debug("Done %s processing", vhost.hostname)
				self._create_vhost_paths(vhost_fullpath)
		self._logger.debug("New vhosts configuration files created")
		
		
		if disttool.is_debian_based():
			self._patch_default_conf_deb()
		elif not self._config.get_list('NameVirtualHost'):
			self._config.add('NameVirtualHost', '*:80')
		
		self._logger.debug("Checking that vhosts directory included in main apache config")
		
		includes = self._config.get_list('Include')
		
		inc_mask = vhosts_path + '/*' + VHOST_EXTENSION
		if not inc_mask in includes:
			self._config.add('Include', inc_mask)
			self._config.write(self._httpd_conf_path)

		self._logger.debug("Creating logrotate config")
		self._create_logrotate_conf(LOGROTATE_CONF_PATH)
			
	def get_vhost_filename(self, hostname, ssl=False):
		end = VHOST_EXTENSION if not ssl else '-ssl' + VHOST_EXTENSION
		return os.path.join(bus.etc_path, VHOSTS_PATH, hostname + end)

	def _create_logrotate_conf(self, logrotate_conf_path):
		if not os.path.exists(logrotate_conf_path):
			if disttool.is_debian_based():
				write_file(logrotate_conf_path, LOGROTATE_CONF_DEB_RAW, logger=self._logger)
			else:
				write_file(logrotate_conf_path, LOGROTATE_CONF_REDHAT_RAW, logger=self._logger)
				

	def _patch_ssl_conf(self, cert_path):
		
		key_path = os.path.join(cert_path, 'https.key')
		crt_path = os.path.join(cert_path, 'https.crt')
		ca_crt_path = os.path.join(cert_path, 'https-ca.crt')
		
		key_path_default = '/etc/pki/tls/private/localhost.key' if disttool.is_redhat_based() else '/etc/ssl/private/ssl-cert-snakeoil.key'
		crt_path_default = '/etc/pki/tls/certs/localhost.crt' if disttool.is_redhat_based() else '/etc/ssl/certs/ssl-cert-snakeoil.pem'
		
		ssl_conf_path = os.path.join(self.server_root, 'conf.d/ssl.conf' if disttool.is_redhat_based() else 'sites-available/default-ssl')
		if os.path.exists(ssl_conf_path):			
			ssl_conf = Configuration('apache')
			ssl_conf.read(ssl_conf_path)
			
			#removing old paths
			old_crt_path = None
			old_key_path = None
			old_ca_crt_path = None
			
			try:
				old_crt_path = ssl_conf.get(".//SSLCertificateFile")
			except NoPathError, e:
				pass
			finally:
				if os.path.exists(crt_path):
					ssl_conf.set(".//SSLCertificateFile", crt_path, force=True)
				elif old_crt_path and not os.path.exists(old_crt_path):
					self._logger.debug("Certificate file not found. Setting to default %s" % crt_path_default)
					ssl_conf.set(".//SSLCertificateFile", crt_path_default, force=True)
					#ssl_conf.comment(".//SSLCertificateFile")
					
			try:
				old_key_path = ssl_conf.get(".//SSLCertificateKeyFile")
			except NoPathError, e:
				pass
			finally:	
				if os.path.exists(key_path):
					ssl_conf.set(".//SSLCertificateKeyFile", key_path, force=True)
				elif old_key_path and not os.path.exists(old_key_path):
					self._logger.debug("Certificate key file not found. Setting to default %s" % key_path_default)
					ssl_conf.set(".//SSLCertificateKeyFile", key_path_default, force=True)	
					#ssl_conf.comment(".//SSLCertificateKeyFile")
					
			try:
				old_ca_crt_path = ssl_conf.get(".//SSLCertificateChainFile")
			except NoPathError, e:
				pass	
			finally:
				if os.path.exists(ca_crt_path):
					try:
						ssl_conf.set(".//SSLCertificateChainFile", ca_crt_path)
					except NoPathError:
						# XXX: ugly hack
						parent = ssl_conf.etree.find('.//SSLCertificateFile/..')
						before_el = ssl_conf.etree.find('.//SSLCertificateFile')
						ch = ssl_conf._provider.create_element(ssl_conf.etree, './/SSLCertificateChainFile', ca_crt_path)
						ch.text = ca_crt_path
						parent.insert(list(parent).index(before_el), ch)
				elif old_ca_crt_path and not os.path.exists(old_ca_crt_path):
					ssl_conf.comment(".//SSLCertificateChainFile")	
					
			ssl_conf.write(ssl_conf_path)
		#else:
		#	raise HandlerError("Apache's ssl configuration file %s doesn't exist" % ssl_conf_path)


	def _check_mod_ssl(self):
		if disttool.is_debian_based():
			self._check_mod_ssl_deb()
		elif disttool.is_redhat_based():
			self._check_mod_ssl_redhat()


	def _check_mod_ssl_deb(self):
		base = os.path.dirname(self._httpd_conf_path)
		
		path = {}
		path['ports.conf'] = base + '/ports.conf'
		path['mods-available'] = base + '/mods-available'
		path['mods-enabled'] = base + '/mods-enabled'
		path['mods-available/ssl.conf'] = path['mods-available'] + '/ssl.conf'
		path['mods-available/ssl.load'] = path['mods-available'] + '/ssl.load'
		path['mods-enabled/ssl.conf'] = path['mods-enabled'] + '/ssl.conf'
		path['mods-enabled/ssl.load'] = path['mods-enabled'] + '/ssl.load'

		self._logger.debug('Ensuring mod_ssl enabled')
		if not os.path.exists(path['mods-enabled/ssl.load']):
			self._logger.info('Enabling mod_ssl')
			system2(('/usr/sbin/a2enmod', 'ssl'))

		self._logger.debug('Ensuring NameVirtualHost *:443')
		if os.path.exists(path['ports.conf']):
			conf = Configuration('apache')
			conf.read(path['ports.conf'])
			i = 0
			for section in conf.get_dict('IfModule'):
				i += 1
				if section['value'] in ('mod_ssl.c', 'mod_gnutls.c'):
					conf.set('IfModule[%d]/Listen' % i, '443', True)					
					conf.set('IfModule[%d]/NameVirtualHost' % i, '*:443', True)
			conf.write(path['ports.conf'])


	def _check_mod_ssl_redhat(self):
		mod_ssl_file = os.path.join(self.server_root, 'modules', 'mod_ssl.so')
		
		if not os.path.exists(mod_ssl_file):
			
			inst_cmd = '/usr/bin/yum -y install mod_ssl'
			self._logger.info('%s does not exist. Trying "%s" ' % (mod_ssl_file, inst_cmd))
			system2(inst_cmd, shell=True)
			
		else:			
			#ssl.conf part
			ssl_conf_path = os.path.join(self.server_root, 'conf.d', 'ssl.conf')
			
			if not os.path.exists(ssl_conf_path):
				self._logger.error("SSL config %s doesn`t exist", ssl_conf_path)
			else:
				ssl_conf = Configuration('apache')
				ssl_conf.read(ssl_conf_path)
				
				if ssl_conf.empty:
					self._logger.error("SSL config file %s is empty. Filling in with minimal configuration.", ssl_conf_path)
					ssl_conf.add('Listen', '443')
					ssl_conf.add('NameVirtualHost', '*:443')
				else:
					if not ssl_conf.get_list('NameVirtualHost'):
						self._logger.debug("NameVirtualHost directive not found in %s", ssl_conf_path)
						if not ssl_conf.get_list('Listen'):
							self._logger.debug("Listen directive not found in %s. ", ssl_conf_path)
							self._logger.debug("Patching %s with Listen & NameVirtualHost directives.",	ssl_conf_path)
							ssl_conf.add('Listen', '443')
							ssl_conf.add('NameVirtualHost', '*:443')
						else:
							self._logger.debug("NameVirtualHost directive inserted after Listen directive.")
							ssl_conf.add('NameVirtualHost', '*:443', 'Listen')
				ssl_conf.write(ssl_conf_path)
			
			loaded_in_main = [module for module in self._config.get_list('LoadModule') if 'mod_ssl.so' in module]
			
			if not loaded_in_main:
				if os.path.exists(ssl_conf_path):
					loaded_in_ssl = [module for module in ssl_conf.get_list('LoadModule') if 'mod_ssl.so' in module]
					if not loaded_in_ssl:
						self._config.add('LoadModule', 'ssl_module modules/mod_ssl.so')
						self._config.write(self._httpd_conf_path)

	def _get_server_root(self):
		if disttool.is_debian_based():
			server_root = '/etc/apache2'
		
		elif disttool.is_redhat_based():
			self._logger.debug("Searching in apache config file %s to find server root", self._httpd_conf_path)
			
			try:
				server_root = strip_quotes(self._config.get('ServerRoot'))
				server_root = re.sub(r'^["\'](.+)["\']$', r'\1', server_root)
			except NoPathError:
				self._logger.warning("ServerRoot not found in apache config file %s", self._httpd_conf_path)
				server_root = os.path.dirname(self._httpd_conf_path)
				self._logger.debug("Use %s as ServerRoot", server_root)
		return server_root

	def _patch_default_conf_deb(self):
		self._logger.debug("Replacing NameVirtualhost and Virtualhost ports especially for debian-based linux")
		default_vhost_path = os.path.join(
					os.path.dirname(self._httpd_conf_path),
					'sites-enabled',
					'000-default')
		if os.path.exists(default_vhost_path):
			default_vhost = Configuration('apache')
			default_vhost.read(default_vhost_path)
			default_vhost.set('NameVirtualHost', '*:80', force=True)
			#default_vhost.set('VirtualHost', '*:80', force=True)
			default_vhost.write(default_vhost_path)
						
			error_message = 'Cannot read default vhost config file %s' % default_vhost_path
			dv = read_file(default_vhost_path, error_msg=error_message, logger=self._logger)
			vhost_regexp = re.compile('<VirtualHost\s+\*>')
			dv = vhost_regexp.sub( '<VirtualHost *:80>', dv)
			error_message = 'Cannot write to default vhost config file %s' % default_vhost_path
			write_file(default_vhost_path, dv, error_msg=error_message, logger=self._logger)
			
		else:
			self._logger.debug('Cannot find default vhost config file %s. Nothing to patch' % default_vhost_path)

	def _create_vhost_paths(self, vhost_path):
			vhost = Configuration('apache')
			vhost.read(vhost_path)
			list_logs = vhost.get_list('.//ErrorLog') + vhost.get_list('.//CustomLog')

			dir_list = []
			for log_file in list_logs:
				log_dir = os.path.dirname(log_file)
				if (log_dir not in dir_list) and (not os.path.exists(log_dir)): 
					dir_list.append(log_dir)

			for log_dir in dir_list:
				try:
					os.makedirs(log_dir)
					self._logger.debug('Created log directory %s', log_dir)
				except OSError, e:
					self._logger.error('Couldn`t create directory %s. %s', 
							log_dir, e.strerror)
			
			if os.path.exists(vhost_path):
				self._logger.debug('Vhost config file path: %s.'
					'Trying to read vhost config file' % vhost_path)
				try:
					vh = Configuration('apache', filename=vhost_path)
					for item in vh.items('VirtualHost'):
						if item[0]=='DocumentRoot':
							doc_root = item[1][:-1] if item[1][-1]=='/' else item[1]
							if not os.path.exists(doc_root):
								self._logger.debug('Trying to create virtual host: %s'
									% doc_root)
								try:
									pwd.getpwnam('apache')
									uname = 'apache'
								except:
									uname = 'www-data'
								finally:
									self._logger.debug('User name: %s' % uname)
									tmp = '/'.join(doc_root.split('/')[:-1])
									self._logger.debug('Trying to create directories:'
										' %s' % tmp)
									if not os.path.exists(tmp):
										os.makedirs(tmp, 0755)
										self._logger.debug('Created parent directories:'
											' %s' % tmp)
									shutil.copytree(os.path.join(bus.share_path,
										'apache/html'), doc_root)
									self._logger.debug('Copied documentroot files: %s'
										 % ', '.join(os.listdir(doc_root)))
									filetool.rchown(uname, doc_root)
									self._logger.debug('Changed owner to %s: %s'
										 % (uname, ', '.join(os.listdir(doc_root))))
				except:
					self._logger.warn('Failed to create DocumentRoot structure in %s.'
						' Error: %s',	doc_root, sys.exc_value)
			else:
				self._logger.warn("Vhost config file `%s` not found.", vhost_path)
