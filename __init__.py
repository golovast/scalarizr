
import sys
import urllib2
if sys.version_info < (2, 6):
	from scalarizr.util import compat
	compat.patch()


# Core
from scalarizr import config, rpc
from scalarizr.bus import bus
from scalarizr.config import CmdLineIni, ScalarizrCnf, ScalarizrState, ScalarizrOptions, STATE
from scalarizr.handlers import MessageListener
from scalarizr.messaging import MessageServiceFactory, MessageService, MessageConsumer
from scalarizr.messaging.p2p import P2pConfigOptions
from scalarizr.platform import PlatformFactory, UserDataOptions
from scalarizr.queryenv import QueryEnvService
from scalarizr.storage import Storage
from scalarizr.api.binding import jsonrpc_http
from scalarizr.storage.util.loop import listloop

# Utils
from scalarizr.util import initdv2, fstool, filetool, log, PeriodicalExecutor
from scalarizr.util import SqliteLocalObject, daemonize, system2, disttool, firstmatched, format_size, dynimp
from scalarizr.util.filetool import write_file, read_file
from scalarizr.util import wait_until

# Stdlibs
import cStringIO
import logging
import logging.config
import os, sys, re, shutil, time, uuid
import binascii, string, traceback
import sqlite3 as sqlite
import threading, socket, signal
from ConfigParser import ConfigParser
from optparse import OptionParser, OptionGroup
from urlparse import urlparse, urlunparse
import pprint
import select
import wsgiref.simple_server
from scalarizr.util import sqlite_server, wait_until

class ScalarizrError(BaseException):
	pass

class NotConfiguredError(BaseException):
	pass


__version__ = open(os.path.join(os.path.dirname(__file__), 'version')).read().strip()


EMBED_SNMPD = True
NET_SNMPD = False

SNMP_RESTART_DELAY = 5 # Seconds

PID_FILE = '/var/run/scalarizr.pid' 

LOGGING_CONFIG = '''
[loggers]
keys=root,scalarizr

[handlers]
keys=console,file,file_debug,scalr

[formatters]
keys=simple

[logger_root]
level=DEBUG
handlers=file,file_debug,scalr

[logger_scalarizr]
level=DEBUG
qualname=scalarizr
handlers=file,file_debug,scalr
propagate=0

[handler_console]
class=StreamHandler
level=INFO
formatter=simple
args=(sys.stdout,)

[handler_file]
class=scalarizr.util.log.RotatingFileHandler
level=INFO
formatter=simple
args=('/var/log/scalarizr.log', 'a+', 5242880, 5, 0600)

[handler_file_debug]
class=scalarizr.util.log.RotatingFileHandler
level=DEBUG
formatter=simple
args=('/var/log/scalarizr_debug.log', 'a+', 5242880, 5, 0600)


[handler_scalr]
class=scalarizr.util.log.MessagingHandler
level=INFO
args=(20, "30s")

[formatter_simple]
format=%(asctime)s - %(levelname)s - %(name)s - %(message)s
'''

_running = False
'''
True when scalarizr daemon should be running
'''

_pid = None
'''
Scalarizr main process PID
'''

_snmp_pid = None
'''
Embed SNMP server process PID
'''

_snmp_scheduled_start_time = None
'''
Next time when SNMP process should be forked
'''

_msg_thread = None

_logging_configured = False


_api_routes = {
	'haproxy': 'scalarizr.api.haproxy.HAProxyAPI',
	'sysinfo': 'scalarizr.api.sysinfo.SysInfoAPI',
	'storage': 'scalarizr.api.storage.StorageAPI',
	'redis': 'scalarizr.api.redis.RedisAPI'
}


class ScalarizrInitScript(initdv2.ParametrizedInitScript):
	def __init__(self):
		initdv2.ParametrizedInitScript.__init__(self, 
			'scalarizr', 
			'/etc/init.d/scalarizr', 
			socks=[initdv2.SockParam(8013)]
		)


class ScalrUpdClientScript(initdv2.ParametrizedInitScript):
	def __init__(self):
		initdv2.ParametrizedInitScript.__init__(self, 
			'scalr-upd-client', 
			'/etc/init.d/scalr-upd-client',
			pid_file='/var/run/scalr-upd-client.pid'
		)


def _init():
	optparser = bus.optparser
	bus.base_path = os.path.realpath(os.path.dirname(__file__) + "/../..")
	
	#dynimp.setup()
	
	_init_logging()
	logger = logging.getLogger(__name__)	
	
	# Initialize configuration
	if not bus.etc_path:
		etc_places = [
			"/etc/scalr",
			"/etc/scalarizr", 
			"/usr/etc/scalarizr", 
			"/usr/local/etc/scalarizr",
			os.path.join(bus.base_path, 'etc')
		]
		if optparser and optparser.values.etc_path:
			# Insert command-line passed etc_path into begining
			etc_places.insert(0, optparser.values.etc_path)
			
		bus.etc_path = firstmatched(lambda p: os.access(p, os.F_OK), etc_places)
		if not bus.etc_path:
			raise ScalarizrError('Cannot find scalarizr configuration dir. Search path: %s' % ':'.join(etc_places))
	cnf = ScalarizrCnf(bus.etc_path)
	if not os.path.exists(cnf.private_path()):
		os.makedirs(cnf.private_path())
	bus.cnf = cnf
	
	
	# Find shared resources dir
	if not bus.share_path:
		share_places = [
			'/usr/share/scalr',
			'/usr/local/share/scalr',
			os.path.join(bus.base_path, 'share')
		]
		bus.share_path = firstmatched(lambda p: os.access(p, os.F_OK), share_places)
		if not bus.share_path:
			raise ScalarizrError('Cannot find scalarizr share dir. Search path: %s' % ':'.join(share_places))

	
	# Registering in init.d
	initdv2.explore("scalarizr", ScalarizrInitScript)

	

DB_NAME = 'db.sqlite'
DB_SCRIPT = 'db.sql'

def _db_connect(file=None):
	logger = logging.getLogger(__name__)
	cnf = bus.cnf
	file = file or cnf.private_path(DB_NAME)
	logger.debug("Open SQLite database (file: %s)" % (file))	
	
	conn = sqlite.connect(file, 5.0)
	conn.row_factory = sqlite.Row
	conn.text_factory = sqlite.OptimizedUnicode
	#conn.executescript("PRAGMA journal_mode=OFF;")	
	return conn

def _init_db(file=None):
	logger = logging.getLogger(__name__)	
	cnf = bus.cnf

	# Check that database exists (after rebundle for example)	
	db_file = file or cnf.private_path(DB_NAME)
	if not os.path.exists(db_file) or not os.stat(db_file).st_size:
		logger.debug("Database doesn't exist, creating new one from script")
		_create_db(file)
		
	# Configure database connection pool
	t = sqlite_server.SQLiteServerThread(_db_connect)
	t.setDaemon(True)
	t.start()
	sqlite_server.wait_for_server_thread(t)
	bus.db = t.connection
	

	
def _create_db(db_file=None, script_file=None):	
	logger = logging.getLogger(__name__)
	#conn = bus.db
	#logger.debug('conn: %s', conn)
	conn = _db_connect()
	conn.executescript(open(script_file or os.path.join(bus.share_path, DB_SCRIPT)).read())
	conn.commit()
	conn.close()
	
	#conn.commit()
	system2('sync', shell=True)	

def _init_logging():
	optparser = bus.optparser
	
	# Configure logging
	if sys.version_info < (2,6):
		# Fix logging handler resolve for python 2.5
		from scalarizr.util.log import fix_py25_handler_resolving		
		fix_py25_handler_resolving()
	
	logging.config.fileConfig(cStringIO.StringIO(LOGGING_CONFIG))
	globals()['_logging_configured'] = True
	logger = logging.getLogger(__name__)
	
	# During server import user must see all scalarizr activity in his terminal
	# Add console handler if it doesn't configured in logging.ini	
	if optparser and optparser.values.import_server:
		if not any(isinstance(hdlr, logging.StreamHandler) \
				and (hdlr.stream == sys.stdout or hdlr.stream == sys.stderr) 
				for hdlr in logger.handlers):
			hdlr = logging.StreamHandler(sys.stdout)
			hdlr.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(name)s - %(message)s"))
			logger.addHandler(hdlr)	


def _init_platform():
	logger = logging.getLogger(__name__)
	cnf = bus.cnf; ini = cnf.rawini
	
	# Initialize platform
	logger.debug("Initialize platform")
	name = ini.get('general', 'platform')
	if name:
		bus.platform = PlatformFactory().new_platform(name)
	else:
		raise ScalarizrError("Platform not defined")

def _init_services():
	logger = logging.getLogger(__name__)
	cnf = bus.cnf; ini = cnf.rawini
	server_id = ini.get('general', 'server_id')
	queryenv_url = ini.get('general', 'queryenv_url')
	messaging_adp = ini.get('messaging', 'adapter')

	# Set base URL
	pr = urlparse(queryenv_url)
	bus.scalr_url = urlunparse((pr.scheme, pr.netloc, '', '', '', ''))
	logger.debug("Got scalr url: '%s'" % bus.scalr_url)

	# Create periodical executor for background tasks (cleanup, rotate, gc, etc...)
	bus.periodical_executor = PeriodicalExecutor()

	logger.debug("Initialize QueryEnv client")
	queryenv = QueryEnvService(queryenv_url, server_id, cnf.key_path(cnf.DEFAULT_KEY), '2008-12-16')
	queryenv = QueryEnvService(queryenv_url, server_id, cnf.key_path(cnf.DEFAULT_KEY), queryenv.get_latest_version())
	bus.queryenv_service = queryenv
	bus.queryenv_version = tuple(map(int, queryenv.api_version.split('-')))
	
	logger.debug("Initialize messaging")
	factory = MessageServiceFactory()
	try:
		params = dict(ini.items("messaging_" + messaging_adp))
		params[P2pConfigOptions.SERVER_ID] = server_id
		params[P2pConfigOptions.CRYPTO_KEY_PATH] = cnf.key_path(cnf.DEFAULT_KEY)
		
		msg_service = factory.new_service(messaging_adp, **params)
		bus.messaging_service = msg_service
	except (BaseException, Exception):
		raise ScalarizrError("Cannot create messaging service adapter '%s'" % (messaging_adp))
		
	logger.debug('Initialize message handlers')
	consumer = msg_service.get_consumer()
	consumer.listeners.append(MessageListener())
	
	logger.debug('Schedule SNMP process')
	globals()['_snmp_scheduled_start_time'] = time.time()		

	Storage.maintain_volume_table = True
	
	if not bus.api_server:
		api_app = jsonrpc_http.WsgiApplication(rpc.RequestHandler(_api_routes), 
											cnf.key_path(cnf.DEFAULT_KEY))
		bus.api_server = wsgiref.simple_server.make_server('0.0.0.0', 8010, api_app)


def _start_services():
	logger = logging.getLogger(__name__)
	# Create message server thread
	msg_service = bus.messaging_service
	consumer = msg_service.get_consumer()
	msg_thread = threading.Thread(target=consumer.start, name="Message server")

	# Start SNMP
	_start_snmp_server()

	# Start message server
	msg_thread.start()
	globals()['_msg_thread'] = msg_thread
	
	# Start API server
	api_server = bus.api_server
	logger.info('Starting API server on http://0.0.0.0:8010')
	api_thread = threading.Thread(target=api_server.serve_forever, name='API server')
	api_thread.start()

	# Start periodical executor
	ex = bus.periodical_executor
	ex.start()

def _apply_user_data(cnf):
	logger = logging.getLogger(__name__)
	platform = bus.platform
	cnf = bus.cnf
	
	if cnf.state == ScalarizrState.RUNNING and bus.scalr_version >= (3, 1, 0):
		logger.debug('Scalr version: %s', bus.scalr_version)
		queryenv = bus.queryenv_service
		userdata = queryenv.get_server_user_data()
		def g(key):
			return userdata.get(key, '')
	else:
		def g(key):
			value = platform.get_user_data(key)
			return value if value is not None else ''	
	
	logger.debug('Applying user-data to configuration')
	logger.debug('User-data:\n%s', pprint.pformat(platform.get_user_data()))
	updates = dict(
		general={
			'server_id' : g(UserDataOptions.SERVER_ID),
			'role_name' : g(UserDataOptions.ROLE_NAME),
			'queryenv_url' : g(UserDataOptions.QUERYENV_URL),
			'cloud_storage_path': g(UserDataOptions.CLOUD_STORAGE_PATH),
			'farm_role_id' : g(UserDataOptions.FARMROLE_ID),
			'env_id' : g(UserDataOptions.ENV_ID), 
			'farm_id' : g(UserDataOptions.FARM_ID),
			'role_id' : g(UserDataOptions.ROLE_ID),
			'region' : g(UserDataOptions.REGION) 
		},
		messaging_p2p={
			'producer_url' : g(UserDataOptions.MESSAGE_SERVER_URL),
		},
		snmp={
			'security_name' : 'notConfigUser',			
			'community_name' : g(UserDataOptions.FARM_HASH)
		}
	)
	behaviour = g(UserDataOptions.BEHAVIOUR)
	if behaviour:
		if behaviour == 'base':
			behaviour = ''
		updates['general']['behaviour'] = behaviour
		
	if not cnf.rawini.has_option('general', 'scalr_id') and \
			bus.scalr_version >= (3, 5, 7):
		queryenv = bus.queryenv_service
		global_config = queryenv.get_global_config()['params']
		updates['general']['scalr_id'] = global_config['scalr.id']


	cnf.update_ini('config.ini', updates)
	cnf.write_key(cnf.DEFAULT_KEY, g(UserDataOptions.CRYPTO_KEY))
	
	logger.debug('Reloading configuration after user-data applying')
	cnf.bootstrap(force_reload=True)




def _detect_scalr_version():
	pl = bus.platform
	cnf = bus.cnf
	if pl and cnf.state != ScalarizrState.IMPORTING:
		if pl.get_user_data('cloud_storage_path'):
			if pl.get_user_data('env_id'):
				return (3, 5, 3)
			if pl.get_user_data('behaviors'):
				return (3, 1, 0)
		else:
			return (2, 1)
	return (2, 0)

	
def init_script():
	_init()
	_init_db()
	
	cnf = bus.cnf
	cnf.bootstrap()
	ini = cnf.rawini
	
	logger = logging.getLogger(__name__)
	logger.debug("Initialize script messaging")

	# Script producer url is scalarizr consumer url. 
	# Script can't handle any messages by himself. Leave consumer url blank
	adapter = ini.get(config.SECT_MESSAGING, config.OPT_ADAPTER)	
	kwargs = dict(ini.items("messaging_" + adapter))
	kwargs[P2pConfigOptions.SERVER_ID] = ini.get(config.SECT_GENERAL, config.OPT_SERVER_ID)
	kwargs[P2pConfigOptions.CRYPTO_KEY_PATH] = cnf.key_path(cnf.DEFAULT_KEY)
	kwargs[P2pConfigOptions.PRODUCER_URL] = kwargs[P2pConfigOptions.CONSUMER_URL]
	
	factory = MessageServiceFactory()		
	msg_service = factory.new_service(adapter, **kwargs)
	bus.messaging_service = msg_service


def _start_snmp_server():
	logger = logging.getLogger(__name__)
	# Start SNMP server in a separate process
	pid = os.fork()
	if pid == 0:
		from scalarizr.snmp.agent import SnmpServer
		globals()['_pid'] = 0
		cnf = bus.cnf; ini = cnf.rawini		
		snmp_server = SnmpServer(
			port=int(ini.get(config.SECT_SNMP, config.OPT_PORT)),
			security_name=ini.get(config.SECT_SNMP, config.OPT_SECURITY_NAME),
			community_name=ini.get(config.SECT_SNMP, config.OPT_COMMUNITY_NAME)
		)
		bus.snmp_server = snmp_server
		
		try:
			snmp_server.start()
			logger.info('[pid: %d] SNMP process terminated', os.getpid())
			sys.exit(0)
		except SystemExit:
			raise
		except (BaseException, Exception), e:
			logger.warn('Caught SNMP error: %s', str(e))
			sys.exit(1)
	else:
		globals()["_snmp_pid"] = pid

def onSIGHUP(*args):
	pid = os.getpid()
	logger = logging.getLogger(__name__)
	logger.debug('Received SIGHUP (pid: %d)', pid)
	if pid != _pid:
		return
	
	logger.info('Reloading scalarizr')
	signal.signal(signal.SIGCHLD, signal.SIG_IGN)
	globals()["_running"] = False
	bus.fire('shutdown')
	_shutdown_services()
	
	
	globals()["_running"] = True
	signal.signal(signal.SIGCHLD, onSIGCHILD)		
	cnf = bus.cnf
	cnf.bootstrap(force_reload=True)
	_init_services()
	_start_services()
	bus.fire('reload')
	

def onSIGTERM(*args):
	pid = os.getpid()	
	logger = logging.getLogger(__name__)
	logger.debug('Received SIGTERM (pid: %d)', pid)
		
	if pid == _pid:
		# Main process
		logger.debug('Shutdown main process (pid: %d)', pid)
		_shutdown()
	else:
		# SNMP process
		logger.debug('Shutdown SNMP server process (pid: %d)', pid)
		snmp = bus.snmp_server
		snmp.stop()

def onSIGCHILD(*args):
	logger = logging.getLogger(__name__)
	#logger.debug("Received SIGCHILD")
	
	if globals()["_running"] and _snmp_pid:
		try:
			# Restart SNMP process if it terminates unexpectedly
			pid, sts = os.waitpid(_snmp_pid, os.WNOHANG)
			'''
			logger.debug(
				'Child terminated (pid: %d, status: %s, WIFEXITED: %s, '
				'WEXITSTATUS: %s, WIFSIGNALED: %s, WTERMSIG: %s)', 
				pid, sts, os.WIFEXITED(sts), 
				os.WEXITSTATUS(sts), os.WIFSIGNALED(sts), os.WTERMSIG(sts)
			)
			'''
			if pid == _snmp_pid and not (os.WIFEXITED(sts) and os.WEXITSTATUS(sts) == 0):
				logger.warning(
					'SNMP process [pid: %d] died unexpectedly. Restarting it', 
					_snmp_pid
				)
				globals()['_snmp_scheduled_start_time'] = time.time() + SNMP_RESTART_DELAY
				globals()['_snmp_pid'] = None
		except OSError:
			pass	
	

def _shutdown(*args):
	logger = logging.getLogger(__name__)
	globals()["_running"] = False

	try:
		bus.fire("shutdown")
	except:
		logger.debug('Shutdown hooks exception', exc_info=sys.exc_info())
		
	try:
		logger.info("[pid: %d] Stopping scalarizr %s", os.getpid(), __version__)
		_shutdown_services()
	except:
		logger.debug('Shutdown services exception', exc_info=sys.exc_info())
	finally:
		if os.path.exists(PID_FILE):
			os.remove(PID_FILE)
		
	logger.info('[pid: %d] Scalarizr terminated', os.getpid())

def _shutdown_services(force=False):
	logger = logging.getLogger(__name__)
	
	# Shutdown SNMP
	if _snmp_pid:
		logger.debug('Send SIGTERM to SNMP process (pid: %d)', _snmp_pid)
		try:
			os.kill(_snmp_pid, signal.SIGTERM)
		except (Exception, BaseException), e:
			logger.debug("Can't kill SNMP process: %s" % e)
		globals()['_snmp_pid'] = None
	
	# Shutdown messaging
	logger.debug('Shutdowning external messaging')
	msg_service = bus.messaging_service
	msg_service.get_consumer().shutdown(force=True)
	msg_service.get_producer().shutdown()
	bus.messaging_service = None
	
	# Shutdown API server
	logger.debug('Shutdowning API server')
	api_server = bus.api_server
	api_server.shutdown()
	bus.api_server = None

	# Shutdown periodical executor
	logger.debug('Shutdowning periodical executor')
	ex = bus.periodical_executor
	ex.shutdown()
	bus.periodical_executor = None


def _cleanup_after_rebundle():
	cnf = bus.cnf
	pl = bus.platform
	logger = logging.getLogger(__name__)
	
	if 'volumes' not in pl.features:
		# Destory mysql storages
		if os.path.exists(cnf.private_path('storage/mysql.json')):
			logger.info('Cleanuping old MySQL storage')
			vol = Storage.create(Storage.restore_config(cnf.private_path('storage/mysql.json')))
			vol.destroy(force=True)				
	
	# Reset private configuration
	priv_path = cnf.private_path()
	for file in os.listdir(priv_path):
		if file in ('.user-data', '.update'):
			continue
		path = os.path.join(priv_path, file)
		os.remove(path) if (os.path.isfile(path) or os.path.islink(path)) else shutil.rmtree(path)
	system2('sync', shell=True)

def do_validate_cnf():
	errors = list()
	def on_error(o, e, errors=errors):
		errors.append(e)
		print >> sys.stderr, 'error: [%s] %s' % (o.name, e)
		
	cnf = bus.cnf
	cnf.bootstrap()
	cnf.validate(on_error)
	return len(errors)

def do_configure():
	optparser = bus.optparser
	cnf = bus.cnf
	cnf.reconfigure(
		values=CmdLineIni.to_kvals(optparser.values.cnf), 
		silent=optparser.values.import_server, 
		yesall=optparser.values.yesall
	)

def do_keygen():
	from scalarizr.util import cryptotool
	print cryptotool.keygen()	


def main():
	try:
		logger = logging.getLogger(__name__)
	except (BaseException, Exception), e:
		print >> sys.stderr, "error: Cannot initiate logging. %s" % (e)
		sys.exit(1)
			
	try:
		optparser = bus.optparser = OptionParser()
		optparser.add_option('-v', '--version', dest='version', action='store_true',
				help='Show version information')
		optparser.add_option('-c', '--etc-path', dest='etc_path',
				help='Configuration directory path')
		optparser.add_option("-z", dest="daemonize", action="store_true", default=False,
				help='Daemonize process')
		optparser.add_option('-l', dest='debug', action='store_true', default=False,
				help='Enable debug log')
		optparser.add_option('-n', '--configure', dest='configure', action="store_true", default=False, 
				help="Configure Scalarizr in the interactive mode by default. " 
				+ "Use '-y -o' to configure Scalarizr non-interactively")
		optparser.add_option("-k", "--gen-key", dest="gen_key", action="store_true", default=False,
				help='Generate crypto key')
		optparser.add_option('-t', dest='validate_cnf', action='store_true', default=False,
				help='Validate configuration')
		optparser.add_option('-m', '--import', dest="import_server", action="store_true", default=False, 
				help='Import service into Scalr')
		optparser.add_option('-y', dest="yesall", action="store_true", default=False,
				help='Answer "yes" to all questions')
		optparser.add_option('-o', dest='cnf', action='append',
				help='Runtime .ini option key=value')
		
		if ('cloud-location=' in sys.argv or 'region=' in sys.argv) and 'platform=ec2' in sys.argv:
			region = urllib2.urlopen('http://169.254.169.254/latest/meta-data/placement/availability-zone').read().strip()[:-1]
			try:
				sys.argv[sys.argv.index('region=')] += region
			except ValueError:
				sys.argv += ['-o', 'region=' + region]		
		
		optparser.parse_args()

		
		# Daemonize process
		if optparser.values.daemonize:
			daemonize()

		if optparser.values.version:
			# Report scalarizr version
			print 'Scalarizr %s' % __version__
			sys.exit()

		elif optparser.values.gen_key:
			# Generate key-pair
			do_keygen()
			sys.exit()

		logger.debug("Initialize scalarizr...")
		_init()

		# Starting scalarizr daemon initialization
		globals()['_pid'] = pid = os.getpid()		
		logger.info('[pid: %d] Starting scalarizr %s', pid, __version__)
		
		# Check for another running scalarzir 
		if os.path.exists(PID_FILE):
			try:
				another_pid = int(read_file(PID_FILE).strip())
			except ValueError:
				pass
			else:
				if pid != another_pid and os.path.exists('/proc/%s/status' % (another_pid,)):
					logger.error('Cannot start scalarizr: Another process (pid: %s) already running', another_pid)
					sys.exit(1)
					
		# Write PID
		write_file(PID_FILE, str(pid))
			
		cnf = bus.cnf
		cnf.on('apply_user_data', _apply_user_data)
		
		if optparser.values.configure:
			do_configure()
			sys.exit()
			
		elif optparser.values.import_server:
			print "Starting import process..."
			print "Don't terminate Scalarizr until Scalr will create the new role"
			cnf.state = ScalarizrState.IMPORTING
			# Load Command-line configuration options and auto-configure Scalarizr
			values = CmdLineIni.to_kvals(optparser.values.cnf)
			if not values.get('server_id'):
				values['server_id'] = str(uuid.uuid4())
			cnf.reconfigure(values=values, silent=True, yesall=True)
		
		# Load INI files configuration
		cnf.bootstrap(force_reload=True)
		ini = cnf.rawini

		# Initialize platform module
		_init_platform()
		pl = bus.platform


		# Check that service started after dirty bundle
		if ini.has_option(config.SECT_GENERAL, config.OPT_SERVER_ID):
		
			# XXX: nimbula's user-data is uploaded by ssh
			server_id = ini.get(config.SECT_GENERAL, config.OPT_SERVER_ID)
			if pl.name in ('nimbula', 'rackspace') and cnf.state != ScalarizrState.IMPORTING:
				if cnf.state == ScalarizrState.REBUNDLING:
					# XXX: temporary workaround
					# XXX: rackspace injects files and boots OS in a parallell. There were situations when
					# .user-data file was stale and new server started from rebundled image
					# toughts that he's an old server and continue rebundling  
					time.sleep(30)
					
				udfile = cnf.private_path('.user-data')
				wait_until(lambda: os.path.exists(udfile), 
						timeout=60, error_text="User-data file %s doesn't exist" % udfile)					
			
			ud_server_id = pl.get_user_data(UserDataOptions.SERVER_ID)
			if server_id and ud_server_id and server_id != ud_server_id:
				logger.info('Server was started after rebundle. Performing some cleanups')
				_cleanup_after_rebundle()
				cnf.state = ScalarizrState.BOOTSTRAPPING

		if cnf.state == ScalarizrState.UNKNOWN:
			cnf.state = ScalarizrState.BOOTSTRAPPING
		
		'''
		if cnf.state == ScalarizrState.REBUNDLING:
			server_id = ini.get(config.SECT_GENERAL, config.OPT_SERVER_ID)
			ud_server_id = pl.get_user_data(UserDataOptions.SERVER_ID)
			if server_id and ud_server_id and server_id != ud_server_id:
				logger.info('Server was started after rebundle. Performing some cleanups')
				_cleanup_after_rebundle()
				cnf.state = ScalarizrState.BOOTSTRAPPING
		'''

		# Initialize local database
		_init_db()
	
	
		STATE['global.start_after_update'] = int(bool(STATE['global.version'] and STATE['global.version'] != __version__)) 
		STATE['global.version'] = __version__
		
		if STATE['global.start_after_update'] and ScalarizrState.RUNNING:
			logger.info('Scalarizr was updated to %s', __version__)
		
		if cnf.state == ScalarizrState.UNKNOWN:
			cnf.state = ScalarizrState.BOOTSTRAPPING
			
		# At first startup platform user-data should be applied
		if cnf.state == ScalarizrState.BOOTSTRAPPING:
			cnf.fire('apply_user_data', cnf)

			upd = ScalrUpdClientScript()
			if not upd.running:
				try:
					upd.start()
				except:
					logger.warn("Can't start Scalr Update Client. Error: %s", sys.exc_info()[1])

		
		# Check Scalr version
		if not bus.scalr_version:
			version_file = cnf.private_path('.scalr-version')
			if os.path.exists(version_file):
				bus.scalr_version = tuple(read_file(version_file).strip().split('.'))
			else:
				bus.scalr_version = _detect_scalr_version()
				write_file(version_file, '.'.join(map(str, bus.scalr_version)))
			
		# Apply Command-line passed configuration options
		cnf.update(CmdLineIni.to_ini_sections(optparser.values.cnf))
		
		# Validate configuration
		num_errors = do_validate_cnf()
		if num_errors or optparser.values.validate_cnf:
			sys.exit(int(not num_errors or 1))		
		
		# Initialize scalarizr services
		_init_services()
		if cnf.state == ScalarizrState.RUNNING:
			# ReSync user-data
			cnf.fire('apply_user_data', cnf)		
		
		bus.fire('init')
		
		# Install signal handlers
		signal.signal(signal.SIGCHLD, onSIGCHILD)	
		signal.signal(signal.SIGTERM, onSIGTERM)
		signal.signal(signal.SIGHUP, onSIGHUP)

		_start_services()
		
		# Fire start
		globals()["_running"] = True
		try:
			bus.fire("start")
		except (BaseException, Exception), e:
			logger.exception(e)
	
		try:
			while _running:
				# Recover SNMP 
				if _running and not _snmp_pid and time.time() >= _snmp_scheduled_start_time:
					_start_snmp_server()
				
				#_msg_thread.join(0.2)
				try:
					select.select([], [], [], 30)
				except select.error, e:
					if e.args[0] == 4:
						# Interrupted syscall
						continue
					raise
				
		except KeyboardInterrupt:
			logger.debug('Mainloop: KeyboardInterrupt')
			pass
		finally:
			logger.debug('Mainloop: finally')
			if _running and os.getpid() == _pid:
				_shutdown()
		logger.debug('Mainloop: leave')
			
	except (BaseException, Exception), e:
		if isinstance(e, SystemExit):
			raise
		elif isinstance(e, KeyboardInterrupt):
			pass
		else:
			if _logging_configured:
				logger.exception(e)
			else:
				print >> sys.stderr, 'error: %s' % e
			sys.exit(1)
