# Operations with chains:
# - append
# - insert
# - replace
# - remove

# Predefined chains:
# INPUT FORWARD OUTPUT 	PREROUTING 	POSTROUTING

from __future__ import with_statement

import sys
if sys.version_info[0:2] >= (2, 7):
	from collections import OrderedDict
else:
	from scalarizr.externals.collections import OrderedDict
import shlex
import os
import re
from copy import copy
import logging

from scalarizr import linux
from scalarizr.linux import redhat

LOG = logging.getLogger(__name__)


IPTABLES_BIN = '/sbin/iptables'
IPTABLES_SAVE = '/sbin/iptables-save'
IPTABLES_RESTORE = '/sbin/iptables-restore'

# from iptables --help, must cover all short options
_OPTIONS = {
	"-A": "--append",
	"-D": "--delete",
	"-I": "--insert",
	"-R": "--replace",
	"-L": "--list",
	"-S": "--list-rules",
	"-F": "--flush",
	"-Z": "--zero",
	"-N": "--new",
	"-X": "--delete-chain",
	"-P": "--policy",
	"-E": "--rename-chain",
	"-p": "--protocol",
	"-s": "--source",
	"-d": "--destination",
	"-i": "--in-interface",
	"-j": "--jump",
	"-g": "--goto",
	"-m": "--match",
	"-n": "--numeric",
	"-o": "--out-interface",
	"-t": "--table",
	"-v": "--verbose",
	"-x": "--exact",
	"-f": "--fragment",
	"-V": "--version",
}


def iptables(**long_kwds):

	ordered_long = OrderedDict()
	for key in ("protocol", "match"):
		if key in long_kwds:
			ordered_long[key] = long_kwds.pop(key)
	ordered_long.update(long_kwds)

	return linux.system(linux.build_cmd_args(executable=IPTABLES_BIN,
		long=ordered_long))


def iptables_save(filename=None, *short_args, **long_kwds):
	# file name is a path string or file-like object
	# if filename is None return output
	kwds = {}
	if isinstance(filename, basestring):
		filename = open(filename, 'w+')
	if hasattr(filename, 'write'):
		kwds['stdout'] = filename
	out = linux.system(linux.build_cmd_args(executable=IPTABLES_SAVE,
		short=short_args, long=long_kwds), **kwds)[0]
	return out


def iptables_restore(filename, *short_args, **long_kwds):
	if isinstance(filename, basestring):
		filename = open(filename)
	linux.system(linux.build_cmd_args(executable=IPTABLES_RESTORE,
		short=short_args, long=long_kwds), stdin=filename)


def save():
	'''
	on RHEL call 'service iptables save'
	on Ubuntu:
		- touch or create /etc/network/if-pre-up.d/iptables.sh
			$ cat /etc/network/if-pre-up.d/iptables.sh
			#!/bin/bash
			iptables-restore < /etc/iptables.rules
		- iptables-save > /etc/iptables.rules
	'''
	if linux.os["family"] in ("RedHat", "Oracle"):
		linux.system(linux.build_cmd_args(executable="service", short=['iptables',
																	   'save']))
	elif linux.os["family"] == "Debian":
		with open('/etc/network/if-pre-up.d/iptables.sh', 'w') as fp:
			fp.write('#!/bin/bash\n'
					 'iptables-restore < /etc/iptables.rules')

		iptables_save('/etc/iptables.rules')


class _Chain(object):

	def __init__(self, chain):
		self.name = chain

	def append(self, rule):
		return iptables(append=self.name, **rule)

	def insert(self, index, rule):
		if isinstance(index, int):
			insert = [self.name, index]
		else:
			insert = self.name
		return iptables(insert=insert, **rule)

	def replace(self, index, rule):
		return iptables(replace=[self.name, index], **rule)

	def remove(self, arg):
		if isinstance(arg, int):
			delete = [self.name, arg]
			rule = {}
		elif isinstance(arg, dict):
			delete = self.name
			rule = arg
		return iptables(delete=delete, **rule)

	def list(self, table=None):
		"""
		List iptables rules. On systems with older iptables (that don't
		support --list-rules arg) --list will be used with a very limited
		parsing.
		"""

		# args for both cases
		list_rules_kwargs = {"list-rules": self.name}
		list_kwargs = {"list": self.name, "numeric": True}
		if table:
			list_rules_kwargs["table"] = table
			list_kwargs["table"] = table

		try:
			out = iptables(**list_rules_kwargs)[0]
		except linux.LinuxError, e:
			if "Unknown arg `--list-rules'" in e.err:
				out = iptables(**list_kwargs)[0]
				return self._parse_list(out)
			else:
				raise
		else:
			return self._parse_list_rules(out)

	def _parse_list_rules(self, output):
		result = []

		def build_ruledict(option, element):
			"""
			For use in reduce(build_ruledict, arglist). This will result in
			filling the last dict from 'result' with {'option': 'value'} pairs
			from ['--option', 'value', ...] arglist. Multiple or no values per
			option is acceptable.
			"""

			ruledict = result[-1]
			key = option[2:]
			val = ruledict.setdefault(key, True)

			if element.startswith('--'):  # element is the new option
				return element
			else:
				if val == True:
					ruledict[key] = element
				elif not hasattr(val, 'append'):
					ruledict[key] = [val, element]
				else:
					ruledict[key].append(element)
				return option


		# parse
		for rule in output.splitlines():
			args = shlex.split(rule)
			if "-P" in args or "-N" in args:
				continue  # dull rules

			# convert all short options to long
			for i, arg in enumerate(args):
				if len(arg) == 2 and arg.startswith('-'):
					args[i] = _OPTIONS[arg]  # will crash on unknown short opt

			# build a new ruledict in result
			result.append({})
			reduce(build_ruledict, args)

			# postprocess: hide append option
			del result[-1]["append"]  #? except KeyError: pass

		return result

	def _parse_list(self, output):
		"""
		The problem with parsing iptables --list output is that only the first
		5 columns have names and the rest of data is in some arbitrary format.
		This method is supposed to understand this rest in a couple of simple
		cases: "(tcp|udp) (dpt:\d+|dpts:\d+:\d+)". All other cases will be kept
		as a single string under "_unparsed" key in the rule dict.
		"""

		headers = ["jump", "protocol", "opt", "source", "destination"]
		defaults = [None, "all", "--", "0.0.0.0/0", "0.0.0.0/0"]

		result = []
		outlist = output.splitlines()[2:]  # strip the 2 header lines
		for rulestr in outlist:
			# preprocess input
			if rulestr.startswith(' ' * 11):  # empty target
				_headers = headers[1:]
				rulestr = rulestr.lstrip()  # not necessary, for readability
			else:
				_headers = headers

			# build the rule
			ruleitems = rulestr.split()
			rule = dict(zip(_headers, ruleitems))  # known columns
			rest = ruleitems[len(_headers):]  # unknown
			# rest can be: [], ['tcp', 'dpt:8008'], ['tcp', 'dpts:6379:6395']
			# or [...]
			if len(rest) == 2 and rest[0] in ("tcp", "udp") and \
					rest[1].startswith(("dpt:", "dpts:")):
				rule["match"] = rest[0]
				rule["dport"] = rest[1].split(':', 1)[1]
			elif not rest:
				pass
			else:
				rule["_unparsed"] = ' '.join(rest)

			# postprocess
			# remove defaults
			for key, default in zip(headers, defaults):
				if rule.has_key(key) and rule[key] == default:
					del rule[key]
			# convert ips
			for key, val in rule.items():
				if _is_plain_ip(val):
					rule[key] = val + "/32"

			result.append(rule)

		return result

	def ensure(self, rules, append=False):
		# Insert or append missing rules.
		# NOTE: rule comparison is far from ideal, check _to_inner method
		# NOTE: existing rules don't have table attribute

		existing = self.list()
		for rule in reversed(rules):
			rule_repr = _to_inner(rule)
			if rule_repr not in existing:
				if not append:
					self.insert(None, rule)
					existing.insert(0, rule_repr)
				else:
					self.append(rule)
					existing.append(rule_repr)


#? Group this two functions in a Rule class?
def _to_inner(rule):
	"""
	Converts rule to its inner representation for comparison.

	1. "source": "192.168.0.1" -> "source": "192.168.0.1/32"
	2. "destination": "192.168.0.1" -> "destination": "192.168.0.1/32"
	3. "dport": 22 -> "dport": "22"

	TODO:

	"src": $value -> "source": $value
	"proto": $value -> "protocol": $value
	"syn": True -> "tcp-flags": "FIN,SYN,RST,ACK SYN"
	"protocol": "tcp" -> "protocol": "tcp", "match": "tcp" for all protocols
	"source": $ip1,$ip2 -> 2 rules for each ip
	"""
	inner = copy(rule)

	# 1
	if 'source' in inner and _is_plain_ip(inner["source"]):
		inner["source"] += "/32"
	# 2
	if 'destination' in inner and _is_plain_ip(inner["destination"]):
		inner["destination"] += "/32"
	# 3
	if 'dport' in inner and isinstance(inner["dport"], int):
		inner["dport"] = str(inner["dport"])

	return inner


def _is_plain_ip(s):
	return [n.isdigit() and 0 <= int(n) <= 255 for n in s.split('.')] ==\
		   [True] * 4


class _Chains(object):
	"""
	Note: doesn't represent the OS chains state.
	"""

	_predefined = (
		"INPUT",
		"FORWARD",
		"OUTPUT",
		"PREROUTING",
		"POSTROUTING",
	)
	_container = dict([(name, _Chain(name)) for name in _predefined])

	def __getitem__(self, name):
		return self._container.setdefault(name, _Chain(name))

	def __iter__(self):
		return iter(self._container)

	def __contains__(self, value):
		return value in self._container

	def add(self, name):
		iptables(**{"new-chain": name})

	def remove(self, name, force=False):
		if force and name not in self._predefined:  # cannot remove a builtin chain
			iptables(flush=name)
			# TODO: delete references
		iptables(**{"delete-chain": name})

		self._container.pop(name)


chains = _Chains()

INPUT = chains["INPUT"]
FORWARD = chains["FORWARD"]
OUTPUT = chains["OUTPUT"]
PREROUTING = chains["PREROUTING"]
POSTROUTING = chains["POSTROUTING"]
FIREWALL = INPUT  # default chain


def list(chain, table=None):
	return chains[chain].list(table)


def ensure(chain_rules, append=False):
	# {chain: [rule, ...]}
	for chain, rules in chain_rules.iteritems():
		chains[chain].ensure(rules, append)


def enabled():
	if linux.os['family'] in ('RedHat', 'Oracle'):
		out = redhat.chkconfig(list="iptables")[0]
		return bool(re.search(r"iptables.*?\s\d:on", out))
	else:
		return os.access(IPTABLES_BIN, os.X_OK)


def redhat_input_chain():
	if linux.os['family'] in ('RedHat', 'Oracle'):
		rh_fw_rules = [rule for rule in INPUT.list()
				if rule.has_key("jump") and rule["jump"].startswith("RH-Firewall-")]
		for rule in rh_fw_rules:
			if len(rule) == 1:  # if rule redirects everything
				return rule["jump"]  # "RH-Firewall-1-INPUT"
	return False


'''
Initialization.
'''
if enabled():

	# Without this first call 'service iptables save' fails with code:1
	iptables(list=True, numeric=True)

	rh_chain = redhat_input_chain()
	if rh_chain:
		FIREWALL = chains[rh_chain]

