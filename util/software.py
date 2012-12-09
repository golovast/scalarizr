'''
Created on Sep 10, 2010

@author: marat
'''
from scalarizr.util import disttool, system2
from scalarizr import linux
import os, re, zipfile, string, glob

__all__ = ('all_installed', 'software_info', 'explore', 'whereis')

def all_installed():
	ret = []
	for getinfo_func in software_list.itervalues():
		try:
			ret.append(getinfo_func())
		except:
			pass
	return ret
		

def software_info(name):
	if not software_list.has_key(name):
		raise Exception("Unknown software: %s" % name)
	return software_list[name]()

def explore(name, lookup_fn):
	if name in software_list.keys():
		
		raise Exception("'%s' software has been already explored" % name)
	software_list[name] = lookup_fn
	
def which(name):
	try:
		return whereis(name)[0]
	except IndexError:
		raise LookupError("Command '%s' not found" % (name, ))
	
def whereis(name):
	'''
	Search executable in /bin /sbin /usr/bin /usr/sbin /usr/libexec /usr/local/bin /usr/local/sbin
	@rtype: tuple
	'''
	places = ['/bin', '/sbin', '/usr/bin', '/usr/sbin', '/usr/libexec', '/usr/local/bin', '/usr/local/sbin']
	return tuple([os.path.join(place, name) for place in places if os.path.exists(os.path.join(place, name))])

def system_info(verbose=False):
		
	def check_module(module):
		return not system2((modprobe, '-n', module), raise_exc=False)[-1]
	
	ret = {}
	ret['software'] = []			
	installed_list = all_installed()
	for software_info in installed_list:
		v = dict(
			name=software_info.name,
			version='.'.join([str(x) for x in software_info.version])
		)
		if verbose:
			v['string_version'] = software_info.string_version		
		
		ret['software'].append(v)

	
	ret['os'] = {}	
	ret['os']['version'] 		= ' '.join(disttool.linux_dist())
	ret['os']['string_version'] = ' '.join(disttool.uname()).strip()

	ret['dist'] = {
		'distributor': linux.os['name'].lower(),
		'release': linux.os['release'],
		'codename': linux.os['codename']
	}
	
	modprobe = whereis('modprobe')[0]
	ret['storage'] = {}
	ret['storage']['fstypes'] = []
	
	for fstype in ['xfs', 'ext3', 'ext4']:
		retcode = system2((modprobe, '-n', fstype), raise_exc=False)[-1]
		exe = whereis('mkfs.%s' % fstype)
		if not retcode and exe:
			ret['storage']['fstypes'].append(fstype)

	# Raid levels support detection
	if whereis('mdadm'):
		for module in  ('raid0', 'raid1', 'raid456'):
			ret['storage'][module] = 1 if check_module(module) else 0

	# Lvm2 support detection
	if whereis('dmsetup') and all(map(check_module, ('dm_mod', 'dm_snapshot'))):
		ret['storage']['lvm'] = 1
	else:
		ret['storage']['lvm'] = 0
						
	return ret


class SoftwareError(BaseException):
	pass

class SoftwareInfo:
	name = None	
	version = None	
	'''
	@param version: tuple(major, minor, bugfix)
	'''
	string_version = None
	
	def __init__(self, name, version, string_version):
		self.name    		= name
		self.string_version = string_version
		ver_nums		= map(int, version.split('.'))
		if len(ver_nums) < 3: 
			for _ in range(len(ver_nums), 3):
				ver_nums.append(0)
		self.version = tuple(ver_nums)
		
software_list = dict()

def mysql_software_info():
	
	binaries = whereis('mysqld')
	if not binaries:
		raise SoftwareError("Can't find executable for MySQL server")

	version_string = system2((binaries[0], '-V'))[0].strip()
	if not version_string:
		raise SoftwareError

	res = re.search('Ver\s+([\d\.]+)', version_string)
	if res:
		version = res.group(1)
		return SoftwareInfo('mysql', version, version_string)
	raise SoftwareError


explore('mysql', mysql_software_info)

def nginx_software_info():
	binaries = whereis('nginx')
	if not binaries:
		raise SoftwareError("Can't find executable for Nginx server")
	
	out = system2((binaries[0], '-V'))[1]
	if not out:
		raise SoftwareError
	
	version_string = out.splitlines()[0]
	res = re.search('[\d\.]+', version_string)
	if res:
		version = res.group(0)
		return SoftwareInfo('nginx', version, out)
	raise SoftwareError


explore('nginx', nginx_software_info)

def memcached_software_info():
	binaries = whereis('memcached')
	if not binaries:
		raise SoftwareError("Can't find executable for Memcached")
	
	out = system2((binaries[0], '-h'))[0]
	if not out:
		raise SoftwareError
	
	version_string = out.splitlines()[0]
	
	res = re.search('memcached\s+([\d\.]+)', version_string)
	if res:
		version = res.group(1)
		return SoftwareInfo('memcached', version, version_string)
	raise SoftwareError

explore('memcached', memcached_software_info)

def php_software_info():
	binaries = whereis('php')
	if not binaries:
		raise SoftwareError("Can't find executable for php interpreter")
	
	out = system2((binaries[0], '-v'))[0]
	if not out:
		raise SoftwareError
	
	version_string = out.splitlines()[0]
	
	res = re.search('PHP\s+([\d\.]+)', version_string)
	
	if res:
		version = res.group(1)
		return SoftwareInfo('php',version, out)
	raise SoftwareError

explore('php', php_software_info)

def python_software_info():
	binaries = whereis('python')
	if not binaries:
		raise SoftwareError("Can't find executable for python interpreter")

	version_string = system2((binaries[0], '-V'))[1].strip()
	if not version_string:
		raise SoftwareError

	version_string = version_string.splitlines()[0]

	res = re.search('Python\s+([\d\.]+)', version_string)

	if res:
		version = res.group(1)
		return SoftwareInfo('python', version, version_string)

	raise SoftwareError

explore('python', python_software_info)

def mysqlproxy_software_info():
	binaries = whereis('mysql-proxy')
	if not binaries:
		raise SoftwareError("Can't find executable for mysql-proxy")

	version_string = system2((binaries[0], '-V'))[0].strip()
	if not version_string:
		raise SoftwareError

	version_string = version_string.splitlines()[0]

	res = re.search('mysql-proxy\s+([\d\.]+)', version_string)

	if res:
		version = res.group(1)
		return SoftwareInfo('mysql-proxy', version, version_string)

	raise SoftwareError

explore('mysql-proxy', mysqlproxy_software_info)

def apache_software_info():

	binary_name = "httpd" if disttool.is_redhat_based() else "apache2"
	binaries = whereis(binary_name)
	if not binaries:
		raise SoftwareError("Can't find executable for apache http server")
		
	out = system2((binaries[0], '-V'))[0]
	if not out:
		raise SoftwareError
	
	version_string = out.splitlines()[0]
	res = re.search('[\d\.]+', version_string)
	if res:
		version = res.group(0)
	
		return SoftwareInfo('apache', version, out)
	raise SoftwareError


explore('apache', apache_software_info)
	

def tomcat_software_info():
	
	tomcat_dir = [os.path.join('/usr/share', location) for location in os.listdir('/usr/share') if 'tomcat' in location]
	if not tomcat_dir:
		raise SoftwareError("Can't find tomcat server location")
	
	catalina_path = os.path.join(tomcat_dir[0], 'lib/catalina.jar')
	
	if not os.path.exists(catalina_path):
		raise SoftwareError("Version script doesn't exist")
		
	catalina = zipfile.ZipFile(catalina_path, 'r')
	try:
		properties_path = 'org/apache/catalina/util/ServerInfo.properties'
		if not properties_path in catalina.namelist():
			raise SoftwareError("ServerInfo.properties file isn't in catalina.jar")
		
		properties = catalina.read(properties_path)
		properties = re.sub(re.compile('^#.*$', re.M), '', properties).strip()
		
		res = re.search('^server.info=Apache\s+Tomcat/([\d\.]+)', properties, re.M)
		if res:
			version = res.group(1)
			return SoftwareInfo('tomcat', version, properties)
		raise SoftwareError
	finally:
		catalina.close()
		
explore('tomcat', tomcat_software_info)

def varnish_software_info():	
	binaries = whereis('varnishd')
	if not binaries:
		raise SoftwareError("Can't find executable for varnish HTTP accelerator")

	out = system2((binaries[0], '-V'))[1].strip()
	if not out:
		raise SoftwareError
	
	version_string = out.splitlines()[0]
	
	res = re.search('varnish-([\d\.]+)', version_string)
	
	if res:
		version = res.group(1)
		return SoftwareInfo('varnish', version, out)
	
	raise SoftwareError

explore('varnish', varnish_software_info)

def rails_software_info():
	binaries = whereis('gem')
	
	if not binaries:
		raise SoftwareError("Can't find executable for ruby gem packet manager")

	out = system2((binaries[0], 'list', 'rails'))[0].strip()
	
	if not out:
		raise SoftwareError	

	res = re.search('\(([\d\.]+)\)', out)
	
	if res:
		version = res.group(1)
		return SoftwareInfo('rails', version, '')
	
	raise SoftwareError

explore('rails', rails_software_info)

def cassandra_software_info():
	cassandra_path = '/usr/share/cassandra/apache-cassandra.jar'
	
	if not os.path.exists(cassandra_path):
		raise SoftwareError("Can't find apache-cassandra.jar file with Cassandra version info")

	cassandra = zipfile.ZipFile(cassandra_path)
	
	try:
		properties_path = 'META-INF/MANIFEST.MF'
		
		if not properties_path in cassandra.namelist():
			raise SoftwareError("MANIFEST.MF file isn't in apache-cassandra.jar")
		
		properties = cassandra.read(properties_path)
		
		res = re.search('^Implementation-Version:\s*([\d\.]+)', properties, re.M)
		if res:
			version = res.group(1)
			return SoftwareInfo('cassandra', version, '')
		raise SoftwareError
	finally:
		cassandra.close()

explore('cassandra', cassandra_software_info)


def rabbitmq_software_info():

	binaries = whereis('rabbitmq-server')
	if not binaries:
		raise SoftwareError("Can't find executable for rabbitmq server")
	
	# Start rabbitmq server with broken parameters
	# in order to receive version
	env = dict(RABBITMQ_NODE_IP_ADDRESS='256.0.0.0', RABBITMQ_LOG_BASE='/tmp', RABBITMQ_NODENAME='version_test')
	out = system2((binaries[0]), env=env, raise_exc=False, silent=True)[0]
	if not out:
		raise SoftwareError
	
	res = re.search('\|\s+v([\d\.]+)\s+\+---\+', out)
	if res:
		version = res.group(1)
	
		return SoftwareInfo('rabbitmq', version, version)
	raise SoftwareError

explore('rabbitmq', rabbitmq_software_info)


def redis_software_info():

	binary_name = "redis-server" # if disttool.is_redhat_based() else "redis-cli"
	binaries = whereis(binary_name)
	if not binaries:
		raise SoftwareError("Can't find executable for redis server")
		
	out = system2((binaries[0], '-v'))[0]
	if not out:
		raise SoftwareError
	
	version_string = out.splitlines()[0]
	res = re.search('[\d\.]+', version_string)
	if res:
		version = res.group(0)
	
		return SoftwareInfo('redis', version, out)
	raise SoftwareError
explore('redis', redis_software_info)


def chef_software_info():	
	binaries = whereis('chef-client')
	if not binaries:
		raise SoftwareError("Can't find executable for chef client")

	version_string = system2((binaries[0], '-v'))[0].strip()
	if not version_string:
		raise SoftwareError
	
	res = re.search('Chef:\s+([\d\.]+)', version_string)
	
	if res:
		version = res.group(1)
		return SoftwareInfo('chef', version, version_string)
	
	raise SoftwareError
explore('chef', chef_software_info)


def postgresql_software_info():
	versions_dirs = glob.glob('/usr/lib/p*sql/*')
	versions_dirs.extend(glob.glob('/usr/p*sql*/'))
	versions_dirs.sort()
	versions_dirs.reverse()
	for version in versions_dirs:
		bin_path = os.path.join(version, 'bin/postgres')
		if os.path.isfile(bin_path):
			version_string = system2((bin_path, '--version'))[0].strip()
			version = version_string.split()[-1]
			return SoftwareInfo('postgresql', version, version_string)
	else:
		raise SoftwareError

explore('postgresql', postgresql_software_info)