
import os

from scalarizr.linux import pkgmgr


def rsync(src, dst, **long_kwds):
	if not os.path.exists('/usr/bin/rsync'):
		pkgmgr.installed('rsync')

	raise NotImplementedError()
