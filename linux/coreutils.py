'''
Created on Aug 28, 2012

@author: marat
'''

import os
import glob
import shutil

from scalarizr import linux
from scalarizr.linux import pkgmgr


def sync():
	return linux.system(('/bin/sync', ))


def dd(**kwds):
	short = []
	for k, v in kwds.items():
		short.append('%s=%s' % (k, v))
	return linux.system(linux.build_cmd_args(
				executable='/bin/dd',
				short=short))


def sfdisk():
	raise NotImplementedError()


def modprobe(module_name, **long_kwds):
	return linux.system(linux.build_cmd_args(
				executable='/sbin/modprobe', 
				long=long_kwds,
				params=[module_name]), 
			error_text='Kernel module %s is not available' % module_name)
	

def dmsetup(action, *params, **long_kwds):
	if not os.path.exists('/sbin/dmsetup'):
		if linux.os.debian_family:
			package = 'dmsetup'
		else:
			package = 'device-mapper'
		pkgmgr.installed(package)

	return linux.system(linux.build_cmd_args(
			executable='/sbin/dmsetup', 
			short=[action], 
			long=long_kwds, 
			params=params))	
	
	
def losetup(*args, **long_kwds):
	return linux.system(linux.build_cmd_args(
				executable='/sbin/losetup', 
				long=long_kwds, 
				params=args))

	
def losetup_all(flip=False):
	'''
	Alias to losetup --all with parsing output into python dict.
	When flip=True filenames becomes keys and devices are values
	Example:
		{'/dev/loop0': '/mnt/loop0',
 		'/dev/loop1': '/mnt/loop1',
 		'/dev/loop2': '/mnt/loop2'}
	'''
	ret = list()
	out = losetup(all=True)[0].strip()
	for line in out.splitlines():
		cols = line.split()
		device = cols[0][:-1]
		filename = cols[-1][1:-1]
		ret.append((device, filename) if not flip else (filename, device))
	return dict(ret)


def touch(filename):
	open(filename, "w+").close()


def chown_r(path, owner, group=None):
	# TODO: port python implementation from scalarizr.util.filetool
	return linux.system(linux.build_cmd_args(
				executable='/bin/chown', 
				long={'recursive': True}, 
				params=[owner if not group else owner + ':' + group, path]))	


def remove(path):
	if os.path.isfile(path):
		os.remove(path)	
	elif os.path.isdir(path):
		shutil.rmtree(path)


def clean_dir(path):
	if not os.path.isdir(path):
		raise Exception('No such directory: %s' % path)

	content = glob.glob(os.path.join(path, '*'))
	for item in content:
		remove(item)


def blkid(device_path, **kwargs):
	ret = dict()
	kwargs.update({'o': 'export'})
	args = ['/sbin/blkid']
	for k,v in kwargs.items():
		if type(v) == bool:
			args.append('-%s' % k)
		else:
			args.extend(['-%s' % k, str(v)])

	args.append(device_path)

	out = linux.system(args)[0].splitlines()
	for line in out:
		line = line.strip()
		if line:
			k,v = line.split('=',1)
			ret[k.lower()] = v

	return ret








