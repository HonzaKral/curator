#!/usr/bin/env python

import sys
import time
import logging
from datetime import timedelta

import elasticsearch

try:
	from logging import NullHandler
except ImportError:
	from logging import Handler

	class NullHandler(Handler):
		def emit(self, record):
			pass

__version__ = '1.1.0-dev'

# Elasticsearch versions supported
version_max  = (2, 0, 0)
version_min = (1, 0, 0)

logger = logging.getLogger(__name__)

DEFAULT_ARGS = {
	'host': 'localhost',
	'url_prefix': '',
	'port': 9200,
	'ssl': False,
	'timeout': 30,
	'dry_run': False,
	'debug': False,
	'log_level': 'INFO',
	'repo_type': 'fs',
    'create_repo': False,
    'delete_repo': False,
    'bucket': None,
    'region': None,
    'base_path': None,
    'access_key': None,
    'secret_key': None,
    'compress': True,
    'concurrent_streams': None,
    'chunk_size': None,
    'max_restore_bytes_per_sec': None,
    'max_snapshot_bytes_per_sec': None,
}

def make_parser():
	""" Creates an ArgumentParser to parse the command line options. """
	help_desc = 'Curator for Elasticsearch indices.  Can delete (by space or time), close, disable bloom filters and optimize (forceMerge) your indices.'
	try:
		import argparse
		parser = argparse.ArgumentParser(description=help_desc)
		parser.add_argument('-v', '--version', action='version', version='%(prog)s '+__version__)
	except ImportError:
		import optparse
		parser = optparse.OptionParser(description=help_desc, version='%prog '+ __version__)
		parser.parse_args_orig = parser.parse_args
		parser.parse_args = lambda: parser.parse_args_orig()[0]
		parser.add_argument = parser.add_option
	parser.add_argument('--host', help='Elasticsearch host. Default: localhost', default=DEFAULT_ARGS['host'])
	parser.add_argument('--url_prefix', help='Elasticsearch http url prefix. Default: none', default=DEFAULT_ARGS['url_prefix'])
	parser.add_argument('--port', help='Elasticsearch port. Default: 9200', default=DEFAULT_ARGS['port'], type=int)
	parser.add_argument('--ssl', help='Connect to Elasticsearch through SSL. Default: false', action='store_true', default=DEFAULT_ARGS['ssl'])
	parser.add_argument('-t', '--timeout', help='Connection timeout in seconds. Default: 30', default=DEFAULT_ARGS['timeout'], type=int)
	parser.add_argument('-n', '--dry-run', action='store_true', help='If true, does not perform any changes to the Elasticsearch indices.',
					default=DEFAULT_ARGS['dry_run'])
	parser.add_argument('-D', '--debug', dest='debug', action='store_true', help='Debug mode', default=DEFAULT_ARGS['debug'])
	parser.add_argument('-ll', '--loglevel', dest='log_level', action='store', help='Log level', default=DEFAULT_ARGS['log_level'], type=str)
	parser.add_argument('-l', '--logfile', dest='log_file', help='log file', type=str)
	parser.add_argument('--show-repositories', dest='show_repositories', action='store_true',
					help='[Snapshot] Show all registed repositories.')
	# Repository creation
	parser.add_argument('--repository', dest='repository', action='store', type=str,
    					help='Repository name')
	parser.add_argument('--create-repo', dest='create_repo', action='store_true', default=DEFAULT_ARGS['create_repo'],
    					help='Create indicated (--repository)')
	parser.add_argument('--delete-repo', dest='delete_repo', action='store_true', default=DEFAULT_ARGS['delete_repo'],
    					help='Delete indicated (--repository)')
	parser.add_argument('--repo-type', dest='repo_type', action='store', type=str, default=DEFAULT_ARGS['repo_type'],
    					help='Repository type, one of "fs", "s3"')
	parser.add_argument('--disable-compression', dest='compress', action='store_false', default=DEFAULT_ARGS['compress'],
    					help='Disable compression (enabled by default)')
	parser.add_argument('--concurrent_streams', dest='concurrent_streams', action='store', type=int, default=DEFAULT_ARGS['concurrent_streams'],
    					help='Number of streams (per node) preforming snapshot. Default: 5')
	parser.add_argument('--chunk_size', dest='chunk_size', action='store', type=str, default=DEFAULT_ARGS['chunk_size'],
    					help='Chunk size, e.g. 1g, 10m, 5k. Default is unbounded.')
	parser.add_argument('--max_restore_bytes_per_sec', dest='max_restore_bytes_per_sec', action='store', type=str, default=DEFAULT_ARGS['max_restore_bytes_per_sec'],
    					help='Throttles per node restore rate (per second). Default: 20mb')
	parser.add_argument('--max_snapshot_bytes_per_sec', dest='max_snapshot_bytes_per_sec', action='store', type=str, default=DEFAULT_ARGS['max_snapshot_bytes_per_sec'],
    					help='Throttles per node snapshot rate (per second). Default: 20mb')
    # 'fs' repository args
	parser.add_argument('--location', dest='location', action='store', type=str, default=None,
    					help='[fs] Shared file-system location. Must match remote path, & be accessible to all master & data nodes')
    # 's3' repository args
	parser.add_argument('--bucket', dest='bucket', action='store', type=str, default=DEFAULT_ARGS['bucket'],
    					help='[s3] Repository bucket name')
	parser.add_argument('--region', dest='region', action='store', type=str, default=DEFAULT_ARGS['region'],
    					help='[s3] S3 region. Defaults to US Standard')
	parser.add_argument('--base_path', dest='base_path', action='store', type=str, default=DEFAULT_ARGS['base_path'],
    					help='[s3] S3 base path. Defaults to root directory.')
	parser.add_argument('--access_key', dest='access_key', action='store', type=str, default=DEFAULT_ARGS['access_key'],
    					help='[s3] S3 access key. Defaults to value of cloud.aws.access_key')
	parser.add_argument('--secret_key', dest='secret_key', action='store', type=str, default=DEFAULT_ARGS['secret_key'],
    					help='[s3] S3 secret key. Defaults to value of cloud.aws.secret_key')
	return parser

def validate_args(myargs):
	"""Validate that arguments aren't stomping on each other or conflicting"""
	success = True
	messages = []
	if myargs.create_repo and not myargs.delete_repo: # myargs.create_repo is True
		if not myargs.repository and not myargs.repo_type:
			success = False
			messages.append('Cannot create repository without --repository and --repo_type arguments')
		if myargs.repo_type not in ['fs', 's3']:
			success = False
			messages.append('Incorrect repository type.')
		if myargs.repo_type == 'fs' and not myargs.location:
			success = False
			messages.append('Need to include --location with \'--repo-type fs\'.')
		if myargs.repo_type == 's3' and not myargs.bucket:
			success = False
			messages.append('Need to include --bucket with \'--repo-type s3\'.')
	elif myargs.delete_repo:
		if not myargs.repository:
			success = False
			messages.append('Cannot delete repository without --repository and --delete-repo flags')
	else:
		success = False
		messages.append('Need to specify one of --create-repo, --delete-repo, --show-repositories.')
	if success:
		return True
	else:
		return messages

def get_version(client):
	"""Return ES version number as a tuple"""
	version = client.info()['version']['number']
	return tuple(map(int, version.split('.')))

def _get_repository(client, repo_name):
	"""Get Snapshot Repository information"""
	try:
		return client.snapshot.get_repository(repository=repo_name)
	except elasticsearch.NotFoundError as e:
		logger.info("Repository {0} not found.  Error: {1}".format(repo_name, e))
		return None

def _create_repository(client, repo_name, body):
	"""Create repository with repo_name and body settings"""
	try:
		result = _get_repository(client, repo_name)
		if not result:
			client.snapshot.create_repository(repository=repo_name, body=body)
		elif result is not None and repo_name not in result:
			client.snapshot.create_repository(repository=repo_name, body=body)
		else:
			logger.error("Unable to create repository {0}.  A repository with that name already exists.".format(repo_name))
	except:
		logger.error("Unable to create repository {0}.  Check logs for more information.".format(repo_name))
		return False
	logger.info("Repository {0} creation initiated...".format(repo_name))
	test_result = _get_repository(client, repo_name)
	if repo_name in test_result:
		logger.info("Repository {0} created successfully.".format(repo_name))
		return True
	else:
		logger.error("Repository {0} failed validation...".format(repo_name))
		return False

def _delete_repository(client, repo_name):
	"""Delete repository with repo_name"""
	try:
		return client.snapshot.delete_repository(repository=repo_name)
	except elasticsearch.NotFoundError as e:
		logger.error("Error: {0}".format(e))
		return False


def create_repo_body(repo_type='fs',
					 compress=True, concurrent_streams=None, chunk_size=None, max_restore_bytes_per_sec=None, max_snapshot_bytes_per_sec=None,
					 location=None,
					 bucket=None, region=None, base_path=None, access_key=None, secret_key=None):
	"""Create the request body for creating a repository"""
	argdict = locals()
	body = {}
	body['type'] = argdict['repo_type']
	body['settings'] = {}
	settings = []
	maybes   = ['compress', 'concurrent_streams', 'chunk_size', 'max_restore_bytes_per_sec', 'max_snapshot_bytes_per_sec']
	s3      = ['bucket', 'region', 'base_path', 'access_key', 'secret_key']

	settings += [i for i in maybes if argdict[i]]
	# Type 'fs'
	if argdict['repo_type'] == 'fs':
		settings.append('location')
	# Type 's3'
	if argdict['repo_type'] == 's3':
		settings += [i for i in s3 if argdict[i]]
	for k in settings:
		body['settings'][k] = argdict[k]
	return body


def main():
	start = time.time()

	parser = make_parser()
	arguments = parser.parse_args()

	# Do not log and force dry-run if we opt to show indices.
	if arguments.show_repositories:
		arguments.log_file = '/dev/null'
		arguments.dry_run = True

	# Setup logging
	if arguments.debug:
		numeric_log_level = logging.DEBUG
	else:
		numeric_log_level = getattr(logging, arguments.log_level.upper(), None)
		if not isinstance(numeric_log_level, int):
			raise ValueError('Invalid log level: %s' % arguments.log_level)

	logging.basicConfig(level=numeric_log_level,
						format='%(asctime)s.%(msecs)03d %(levelname)-9s %(funcName)22s:%(lineno)-4d %(message)s',
						datefmt="%Y-%m-%dT%H:%M:%S",
						stream=open(arguments.log_file, 'a') if arguments.log_file else sys.stderr)
	logging.info("Job starting...")

	# Setting up NullHandler to handle nested elasticsearch.trace Logger instance in elasticsearch python client
	logging.getLogger('elasticsearch.trace').addHandler(NullHandler())

	if arguments.show_repositories:
		pass # Skip checking args if we're only showing stuff
	else:
		check_args = validate_args(arguments) # Returns either True or a list of errors
		if not check_args == True:
			logger.error('Malformed arguments: {0}'.format(';'.join(check_args)))
			print('See the help output: {0} --help'.format(sys.argv[0]))
			return
	client = elasticsearch.Elasticsearch(host=arguments.host, port=arguments.port, url_prefix=arguments.url_prefix, timeout=arguments.timeout, use_ssl=arguments.ssl)

	version_number = get_version(client)
	logger.debug('Detected Elasticsearch version {0}'.format(".".join(map(str,version_number))))
	if version_number >= version_max or version_number < version_min:
		print('Expected Elasticsearch version range > {0} < {1}'.format(".".join(map(str,version_min)),".".join(map(str,version_max))))
		print('ERROR: Incompatible with version {0} of Elasticsearch.  Exiting.'.format(".".join(map(str,version_number))))
		sys.exit(1)

	# Show repositories then exit
	if arguments.show_repositories:
		for repository in sorted(_get_repository(client, '_all').keys()):
			print('{0}'.format(repository))
		sys.exit(0)

	# Delete repository
	if arguments.delete_repo and arguments.repository and arguments.dry_run:
		logger.info('Would have attempted deleting repository {0}...'.format(arguments.repository))
	elif arguments.delete_repo and arguments.repository:
		logger.info('Deleting repository {0}...'.format(arguments.repository))
		_delete_repository(client, arguments.repository)
	# Create repository
	if arguments.create_repo and arguments.repository and arguments.dry_run:
		logger.info('Would have attempted creating repository {0}...'.format(arguments.repository))
	elif arguments.create_repo and arguments.repository:
		logger.info('Creating repository {0}...'.format(arguments.repository))
		_create_repository(client, arguments.repository, create_repo_body(repo_type=arguments.repo_type,
		compress=arguments.compress, concurrent_streams=arguments.concurrent_streams, chunk_size=arguments.chunk_size,
		max_restore_bytes_per_sec=arguments.max_restore_bytes_per_sec, max_snapshot_bytes_per_sec=arguments.max_snapshot_bytes_per_sec,
		location=arguments.location,
		bucket=arguments.bucket, region=arguments.region, base_path=arguments.base_path,
		access_key=arguments.access_key, secret_key=arguments.secret_key))
	
	logger.info('Done in {0}.'.format(timedelta(seconds=time.time()-start)))
	
if __name__ == '__main__':
		main()
