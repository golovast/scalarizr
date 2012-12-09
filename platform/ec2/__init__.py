
from scalarizr.bus import bus
from scalarizr.platform import Ec2LikePlatform, PlatformError, PlatformFeatures
from scalarizr.storage.transfer import Transfer
from .storage import S3TransferProvider

from boto import connect_s3
from boto.ec2.connection import EC2Connection
from boto.ec2.regioninfo import RegionInfo
import urllib2, re, os


Transfer.explore_provider(S3TransferProvider)


"""
Platform configuration options
"""
OPT_ACCOUNT_ID = "account_id"
OPT_KEY = "key"
OPT_KEY_ID = "key_id"
OPT_EC2_CERT_PATH = "ec2_cert_path"
OPT_CERT_PATH = "cert_path"
OPT_PK_PATH = "pk_path"


"""
User data options 
"""
UD_OPT_S3_BUCKET_NAME = "s3bucket"



def get_platform():
	return Ec2Platform()

class Ec2Platform(Ec2LikePlatform):
	name = "ec2"

	_userdata_key = "latest/user-data"

	'''
	ec2_endpoints = {
		"us-east-1" 		: "ec2.amazonaws.com",
		"us-west-1" 		: "ec2.us-west-1.amazonaws.com",
		"us-west-2" 		: "ec2.us-west-2.amazonaws.com",
		"sa-east-1"			: "ec2.sa-east-1.amazonaws.com",
		"eu-west-1" 		: "ec2.eu-west-1.amazonaws.com",
		"ap-southeast-1" 	: "ec2.ap-southeast-1.amazonaws.com",
		"ap-northeast-1" 	: "ec2.ap-northeast-1.amazonaws.com"
		
	}
	s3_endpoints = {
		'us-east-1' 		: 's3.amazonaws.com',
		'us-west-1' 		: 's3-us-west-1.amazonaws.com',
		'us-west-2' 		: 's3-us-west-2.amazonaws.com',
		"sa-east-1"			: "s3-sa-east-1.amazonaws.com",
		'eu-west-1' 		: 's3-eu-west-1.amazonaws.com',
		'ap-southeast-1' 	: 's3-ap-southeast-1.amazonaws.com',
		'ap-northeast-1' 	: 's3-ap-northeast-1.amazonaws.com'
	}
	'''
	
	instance_store_devices = (
		'/dev/sda2', '/dev/sdb', '/dev/xvdb', 
		'/dev/sdc', '/dev/xvdc', 
		'/dev/sdd', '/dev/xvdd', 
		'/dev/sde', '/dev/xvde'
	)	

	_logger = None	
	_ec2_cert = None
	_cnf = None
	
	features = [PlatformFeatures.SNAPSHOTS, PlatformFeatures.VOLUMES]
	
	def get_account_id(self):
		return self.get_access_data("account_id").encode("ascii")
			
	def get_access_keys(self):
		# Keys must be in ASCII because hmac functions doesn't works with unicode		
		return (self.get_access_data("key_id").encode("ascii"), self.get_access_data("key").encode("ascii"))
			
	def get_cert_pk(self):
		return (self.get_access_data("cert").encode("ascii"), self.get_access_data("pk").encode("ascii"))
	
	def get_ec2_cert(self):
		if not self._ec2_cert:
			# XXX: not ok
			self._ec2_cert = self._cnf.read_key(os.path.join(bus.etc_path, self._cnf.rawini.get(self.name, OPT_EC2_CERT_PATH)), title="EC2 certificate")
		return self._ec2_cert
	
	def new_ec2_conn(self):
		""" @rtype: boto.ec2.connection.EC2Connection """
		region = self.get_region()
		endpoint = self._ec2_endpoint(region)
		self._logger.debug("Return ec2 connection (endpoint: %s)", endpoint)
		return EC2Connection(region=RegionInfo(name=region, endpoint=endpoint))

	def new_s3_conn(self):
		region = self.get_region()
		endpoint = self._s3_endpoint(region)
		self._logger.debug("Return s3 connection (endpoint: %s)", endpoint)
		return connect_s3(host=endpoint)
	
	def set_access_data(self, access_data):
		Ec2LikePlatform.set_access_data(self, access_data)
		key_id, key = self.get_access_keys()
		os.environ['AWS_ACCESS_KEY_ID'] = key_id
		os.environ['AWS_SECRET_ACCESS_KEY'] = key

	def clear_access_data(self):
		Ec2LikePlatform.clear_access_data(self)
		try:
			del os.environ['AWS_ACCESS_KEY_ID']
			del os.environ['AWS_SECRET_ACCESS_KEY']
		except KeyError:
			pass
		
	@property
	def cloud_storage_path(self):
		ret = Ec2LikePlatform.cloud_storage_path.fget(self)
		if not ret:
			bucket = self.get_user_data(UD_OPT_S3_BUCKET_NAME) or ''
			ret = 's3://' + bucket
		return ret


	def _ec2_endpoint(self, region):
		if region == 'us-east-1':
			return 'ec2.amazonaws.com'
		else:
			return 'ec2.%s.amazonaws.com' % region
		
	def _s3_endpoint(self, region):
		if region == 'us-east-1':
			return 's3.amazonaws.com'
		else:
			return 's3-%s.amazonaws.com' % region

