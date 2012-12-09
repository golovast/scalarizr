'''
Created on Aug 28, 2012

@author: marat
'''
from __future__ import with_statement


import logging
import re
import string
import time

from scalarizr import linux
from urlparse import urlparse

LOG = logging.getLogger(__name__)

class PackageMgr(object):

	INFO_CANDIDATE_KEY = 'candidate'
	INFO_INSTALLED_KEY = 'installed'

	def install(self, name, version=None, updatedb=False, **kwds):
		''' Installs a `version` of package `name` '''
		raise NotImplementedError()

	def remove(self, name, purge=False):
		''' Removes package with given name. '''
		raise NotImplementedError()

	def info(self, name):
		'''
		Returns info about package
		Example:
			{'installed': '2.6.7-ubuntu1',
			'candidate': '2.6.7-ubuntu5'}
		installed is None, if package not installed
		candidate is None, if latest version of package is installed
		'''
		raise NotImplementedError()

	def updatedb(self):
		''' Updates package manager internal database '''
		raise NotImplementedError()


class AptPackageMgr(PackageMgr):
	def apt_get_command(self, command, **kwds):
		kwds.update(env={
			'DEBIAN_FRONTEND': 'noninteractive', 
			'DEBIAN_PRIORITY': 'critical',
			'PATH': '/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/usr/games'
		})
		return linux.system(('/usr/bin/apt-get',
						'-q', '-y', '--force-yes',
						'-o Dpkg::Options::=--force-confold') + \
						tuple(filter(None, command.split())), **kwds)
		

	def apt_cache_command(self, command, **kwds):
		return linux.system(('/usr/bin/apt-cache',) + tuple(filter(None, command.split())), **kwds)


	def apt_policy(self, name):
		installed_re = re.compile(r'^\s{2}Installed: (.+)$')
		candidate_re = re.compile(r'^\s{2}Candidate: (.+)$')
		installed = candidate = None
		
		for line in self.apt_cache_command('policy %s' % name)[0].splitlines():
			m = installed_re.match(line)
			if m:
				installed = m.group(1)
				if installed == '(none)':
					installed = None
				continue
			
			m = candidate_re.match(line)
			if m:
				candidate = m.group(1)
				continue
			
		return installed, candidate
	

	def updatedb(self):
		self.apt_get_command('update')		

	def install(self, name, version=None, updatedb=False, **kwds):
		if version:
			name += '=%s' % version
		if updatedb:
			self.updatedb()
		for _ in range(0, 30):
			try:
				self.apt_get_command('install %s' % name, raise_exc=True)
				break
			except linux.LinuxError, e:
				if not 'E: Could not get lock' in e.err:
					raise
				time.sleep(2)

	def remove(self, name, purge=False):
		command = 'purge' if purge else 'remove'
		for _ in xrange(0, 30):
			try:
				self.apt_get_command('%s %s' % (command, name), raise_exc=True)
				break
			except linux.LinuxError, e:
				if not 'E: Could not get lock' in e.err:
					raise
				time.sleep(2)

	def info(self, name):
		installed, candidate = self.apt_policy(name)
		return {self.INFO_INSTALLED_KEY: installed,
				self.INFO_CANDIDATE_KEY: candidate if installed != candidate else None}
				

class RpmVersion(object):
	
	def __init__(self, version):
		self.version = version
		self._re_not_alphanum = re.compile(r'^[^a-zA-Z0-9]+')
		self._re_digits = re.compile(r'^(\d+)')
		self._re_alpha = re.compile(r'^([a-zA-Z]+)')
	
	def __iter__(self):
		ver = self.version
		while ver:
			ver = self._re_not_alphanum.sub('', ver)
			if not ver:
				break

			if ver and ver[0].isdigit():
				token = self._re_digits.match(ver).group(1)
			else:
				token = self._re_alpha.match(ver).group(1)
			
			yield token
			ver = ver[len(token):]
			
		raise StopIteration()
	
	def __cmp__(self, other):
		iter2 = iter(other)
		
		for tok1 in self:
			try:
				tok2 = iter2.next()
			except StopIteration:
				return 1
		
			if tok1.isdigit() and tok2.isdigit():
				c = cmp(int(tok1), int(tok2))
				if c != 0:
					return c
			elif tok1.isdigit() or tok2.isdigit():
				return 1 if tok1.isdigit() else -1
			else:
				c = cmp(tok1, tok2)
				if c != 0:
					return c
			
		try:
			iter2.next()
			return -1
		except StopIteration:
			return 0


class YumPackageMgr(PackageMgr):

	def yum_command(self, command, **kwds):
		return linux.system((('/usr/bin/yum', '-d0', '-y') + tuple(filter(None, command.split()))), **kwds)
	
	
	def rpm_ver_cmp(self, v1, v2):
		return cmp(RpmVersion(v1), RpmVersion(v2))

	
	def yum_list(self, name):
		out = self.yum_command('list --showduplicates %s' % name)[0].strip()
		
		version_re = re.compile(r'[^\s]+\s+([^\s]+)')
		lines = map(string.strip, out.splitlines())
		
		try:
			line = lines[lines.index('Installed Packages')+1]
			installed = version_re.match(line).group(1)
		except ValueError:
			installed = None

		if 'Available Packages' in lines:		
			versions = [version_re.match(line).group(1) for line in lines[lines.index('Available Packages')+1:]]
		else:
			versions = []
		
		return installed, versions


	def updatedb(self):
		self.yum_command('clean expire-cache')		


	def install(self, name, version=None, updatedb=False, **kwds):
		if version:
			name += '-%s' % version
		if updatedb:
			self.updatedb()
		self.yum_command('install %s' %  name, raise_exc=True)


	def remove(self, name, purge=False):
		self.yum_command('remove '+name, raise_exc=True)


	def info(self, name):
		installed, candidates = self.yum_list(name)
		return {self.INFO_INSTALLED_KEY: installed,
				self.INFO_CANDIDATE_KEY: candidates[-1] if candidates else None}



class RPMPackageMgr(PackageMgr):

	def rpm_command(self, command, **kwds):
		return linux.system((('/usr/bin/rpm', ) + tuple(filter(None, command.split()))), **kwds)

	def install(self, name, version=None, updatedb=False, **kwds):
		''' Installs a package from file or url with `name' '''
		self.rpm_command('-Uvh '+name, raise_exc=True, **kwds)

	def remove(self, name, purge=False):
		self.rpm_command('-e '+name, raise_exc=True)

	def _version_from_name(self, name):
		''' Returns version of package that contains in its name
			Example: 
				name = vim-common-7.3.682-1.fc17.x86_64
				version = 7.3.682-1.fc17.x86_64
		'''
		# TODO: remove architecture info from version string
		name = urlparse(name).path.split('/')[-1]
		name = name.replace('.rpm', '')

		version = re.findall(r'-[0-9][^-]*\..*', name)[0][1:]
		return version

	def info(self, name):
		name = urlparse(name).path.split('/')[-1]
		name = name.replace('.rpm', '')

		out, _, code = self.rpm_command('-q '+name, raise_exc=False)
		installed = not code
		installed_version = self._version_from_name(out)

		return {self.INFO_INSTALLED_KEY: installed_version if installed else None,
				self.INFO_CANDIDATE_KEY: None}

	def updatedb(self):
		pass


def package_mgr():
	if linux.os['family'] in ('RedHat', 'Oracle'):
		return YumPackageMgr()
	return AptPackageMgr()


EPEL_RPM_URL = 'http://download.fedoraproject.org/pub/epel/6/i386/epel-release-6-7.noarch.rpm'
def epel_repository():
	'''
	Ensure EPEL repository for RHEL based servers.
	Figure out linux.os['arch'], linux.os['release']
	'''
	if linux.os['family'] != 'RedHat' or linux.os['name'] == 'Fedora':
		return

	mgr = RPMPackageMgr()
	installed = mgr.info(EPEL_RPM_URL)[PackageMgr.INFO_INSTALLED_KEY]
	if not installed:
		mgr.install(EPEL_RPM_URL)


def apt_source(name, sources, gpg_keyserver=None, gpg_keyid=None):
	'''
	@param sources: list of apt sources.list entries.
	Example:
		['deb http://repo.percona.com/apt ${codename} main',
		'deb-src http://repo.percona.com/apt ${codename} main']
		All ${var} templates should be replaced with 
		scalarizr.linux.os['var'] substitution
	if gpg_keyserver:
		apt-key adv --keyserver ${gpg_keyserver} --recv ${gpg_keyid}
	Creates file /etc/apt/sources.list.d/${name}
	'''
	if linux.os['family'] in ('RedHat', 'Oracle'):
		return

	def _vars(s):
		vars_ = re.findall('\$\{.+?\}', s)
		return map((lambda(name): name[2:-1]), vars_) #2 is len of '${'

	def _substitude(s):
		for var in _vars(s):
			s = s.replace('${'+var+'}', linux.os[var])
		return s

	prepared_sources = map(_substitude, sources)
	with open('/etc/apt/sources.list.d/' + name, 'w+') as fp:
		fp.write('\n'.join(prepared_sources))

	if gpg_keyserver and gpg_keyid:
		linux.system(('apt-key', 'adv', 
					  '--keyserver', gpg_keyserver,
					  '--recv', gpg_keyid),
					 raise_exc=False)


def installed(name, version=None, updatedb=False):
	'''
	Ensure that package installed
	'''
	mgr = package_mgr()
	if updatedb:
		mgr.updatedb()

	installed = mgr.info(name)[PackageMgr.INFO_INSTALLED_KEY]
	if not installed:
		mgr.install(name, version)


def latest(name, updatedb=False):
	'''
	Ensure that latest version of package installed 
	'''
	mgr = package_mgr()
	if updatedb:
		mgr.updatedb()

	info_dict = mgr.info(name)
	candidate = info_dict[PackageMgr.INFO_CANDIDATE_KEY]
	installed = info_dict[PackageMgr.INFO_INSTALLED_KEY]

	if candidate or not installed:
		mgr.install(name, candidate)


def removed(name, purge=False):
	'''
	Ensure that package removed (purged)
	'''
	mgr = package_mgr()
	installed = mgr.info(name)[PackageMgr.INFO_INSTALLED_KEY]
	if purge or installed:
		mgr.remove(name, purge)


