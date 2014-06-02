#!/usr/bin/env python

import sys
import time
import logging
from datetime import timedelta, datetime

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
    'prefix': 'logstash-',
    'separator': '.',
    'curation_style': 'time',
    'time_unit': 'days',

    'max_num_segments': 2,
    'dry_run': False,
    'debug': False,
    'log_level': 'INFO',
    'show_indices': False,
    'wait_for_completion': True,
    'ignore_unavailable': False,
    'include_global_state': False,
    'partial': False,
}

def make_parser():
    """ Creates an ArgumentParser to parse the command line options. """
    help_desc = 'Curator for Elasticsearch indices. See http://github.com/elasticsearch/curator/wiki'
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

    parser.add_argument('-p', '--prefix', help='Prefix for the indices. Indices that do not have this prefix are skipped. Default: logstash-', default=DEFAULT_ARGS['prefix'])
    parser.add_argument('-s', '--separator', help='TIME_UNIT separator. Default: .', default=DEFAULT_ARGS['separator'])

    parser.add_argument('-C', '--curation-style', dest='curation_style', action='store', help='Curate indices by [time, space] Default: time', default=DEFAULT_ARGS['curation_style'], type=str)
    parser.add_argument('-T', '--time-unit', dest='time_unit', action='store', help='Unit of time to reckon by: [days, hours] Default: days', default=DEFAULT_ARGS['time_unit'], type=str)
    # Standard features
    parser.add_argument('-d', '--delete', dest='delete_older', action='store', help='Delete indices older than DELETE_OLDER TIME_UNITs.', type=int)
    parser.add_argument('-c', '--close', dest='close_older', action='store', help='Close indices older than CLOSE_OLDER TIME_UNITs.', type=int)
    parser.add_argument('-b', '--bloom', dest='bloom_older', action='store', help='Disable bloom filter for indices older than BLOOM_OLDER TIME_UNITs.', type=int)
    parser.add_argument('-g', '--disk-space', dest='disk_space', action='store', help='Delete indices beyond DISK_SPACE gigabytes.', type=float)
    # Index routing
    parser.add_argument('-r', '--require', help='Apply REQUIRED_RULE to indices older than REQUIRE TIME_UNITs.', type=int)
    parser.add_argument('--required_rule', help='Index routing allocation rule to require. Ex. tag=ssd', type=str)
    # Optimize
    parser.add_argument('--max_num_segments', action='store', help='Maximum number of segments, post-optimize. Default: 2', type=int, default=DEFAULT_ARGS['max_num_segments'])
    parser.add_argument('-o', '--optimize', action='store', help='Optimize (Lucene forceMerge) indices older than OPTIMIZE TIME_UNITs.  Must increase timeout to stay connected throughout optimize operation, recommend no less than 3600.', type=int)
    # Meta-data
    parser.add_argument('-n', '--dry-run', action='store_true', help='If true, does not perform any changes to the Elasticsearch indices.', default=DEFAULT_ARGS['dry_run'])
    parser.add_argument('-D', '--debug', dest='debug', action='store_true', help='Debug mode', default=DEFAULT_ARGS['debug'])
    parser.add_argument('--loglevel', dest='log_level', action='store', help='Log level', default=DEFAULT_ARGS['log_level'], type=str)
    parser.add_argument('-l', '--logfile', dest='log_file', help='log file', type=str)
    parser.add_argument('--show-indices', dest='show_indices', action='store_true', help='Show indices matching prefix (nullifies other operations)', default=DEFAULT_ARGS['show_indices'])
    # Snapshot
    parser.add_argument('--repository', dest='repository', action='store', type=str,
                        help='Repository name')
    parser.add_argument('--snap-older', dest='snap_older', action='store', type=int,
                        help='[Snapshot] indices older than SNAP_OLDER TIME_UNITs.')
    parser.add_argument('--snap-latest', dest='snap_latest', action='store', type=int,
                        help='[Snapshot] Capture most recent (SNAP_LATEST) number of indices matching PREFIX.')
    parser.add_argument('--delete-snaps', dest='delete_snaps', action='store', type=int,
                        help='[Snapshot] delete snapshots older than DELETE_SNAPS TIME_UNITs.')
    parser.add_argument('--no_wait_for_completion', dest='wait_for_completion', action='store_false',
                        help='[Snapshot] Do not wait until complete to return. Waits by default.', default=DEFAULT_ARGS['wait_for_completion'])
    parser.add_argument('--ignore_unavailable', dest='ignore_unavailable', action='store_true',
                        help='[Snapshot] Ignore unavailable shards/indices. (Default=False)', default=DEFAULT_ARGS['ignore_unavailable'])
    parser.add_argument('--include_global_state', dest='include_global_state', action='store_true',
                        help='[Snapshot] Store cluster global state with snapshot. (Default=False)', default=DEFAULT_ARGS['include_global_state'])
    parser.add_argument('--partial', dest='partial', action='store_true',
                        help='[Snapshot] Do not fail if primary shard is unavailable. (Default=False)', default=DEFAULT_ARGS['partial'])
    parser.add_argument('--show-repositories', dest='show_repositories', action='store_true',
                        help='[Snapshot] Show all registed repositories.')
    parser.add_argument('--show-snapshots', dest='show_snapshots', action='store_true',
                        help='[Snapshot] Show all snapshots in REPOSITORY.')
    return parser


def validate_args(myargs):
    """Validate that arguments aren't stomping on each other or conflicting"""
    success = True
    messages = []
    if myargs.curation_style == 'time':
        if myargs.time_unit != 'days' and myargs.time_unit != 'hours':
            success = False
            messages.append('Values for --time-unit must be either "days" or "hours"')
            if myargs.disk_space:
                success = False
                messages.append('Cannot specify --disk-space and --curation-style "time"')
        if not myargs.delete_older and not myargs.close_older and not myargs.bloom_older and not myargs.optimize and not myargs.require and not myargs.snap_older and not myargs.snap_latest and not myargs.delete_snaps:
            success = False
            messages.append('Must specify at least one of --delete, --close, --bloom, --optimize, --require, --snap-older, --snap-latest, or --delete-snaps')
        if ((myargs.delete_older and myargs.delete_older < 1) or
            (myargs.close_older  and myargs.close_older  < 1) or
            (myargs.bloom_older  and myargs.bloom_older  < 1) or
            (myargs.optimize     and myargs.optimize     < 1) or
            (myargs.snap_older   and myargs.snap_older   < 1)):
            success = False
            messages.append('Values for --delete, --close, --bloom, --optimize, or --snap_older must be > 0')
        if myargs.optimize and myargs.timeout < 300:
            success = False
            messages.append('Timeout should be much higher for optimize transactions. Recommend no less than 3600 seconds')
        if myargs.snap_older and not myargs.repository:
            success = False
            messages.append('Cannot create snapshot without both --repository and --snap-older')
        if myargs.snap_older and myargs.timeout < 300:
            success = False
            messages.append('Timeout should be much higher for snapshot operations. Recommend no less than 3600 seconds')
        if myargs.snap_latest and myargs.timeout < 300:
            success = False
            messages.append('Timeout should be much higher for snapshot operations. Recommend no less than 3600 seconds')
    else: # Curation-style is 'space'
        if (myargs.delete_older or myargs.close_older or myargs.bloom_older or myargs.optimize or myargs.repository or myargs.snap_older):
            success = False
            messages.append('Cannot specify --curation-style "space" and any of --delete, --close, --bloom, --optimize, --repository, or --snap-older')
        if (myargs.disk_space == 0) or (myargs.disk_space < 0):
            success = False
            messages.append('Value for --disk-space must be greater than 0')
    if success:
        return True
    else:
        return messages

def get_index_time(index_timestamp, separator='.'):
    """ Gets the time of the index.

    :param index_timestamp: A string on the format YYYY.MM.DD[.HH]
    :return The creation time (datetime) of the index.
    """
    try:
        return datetime.strptime(index_timestamp, separator.join(('%Y', '%m', '%d', '%H')))
    except ValueError:
        return datetime.strptime(index_timestamp, separator.join(('%Y', '%m', '%d')))

def get_indices(client, prefix='logstash-'):
    """Return a sorted list of indices matching prefix"""
    return sorted(client.indices.get_settings(index=prefix+'*', params={'expand_wildcards': 'closed'}).keys())
    
def get_snaplist(client, repo_name, prefix='logstash-'):
    """Get _all snapshots containing prefix from repo_name and return a list"""
    retval = []
    try:
        allsnaps = client.snapshot.get(repository=repo_name, snapshot="_all")['snapshots']
        retval = [snap['snapshot'] for snap in allsnaps if 'snapshot' in snap.keys()]
        retval = [i for i in retval if prefix in i]
    except elasticsearch.NotFoundError as e:
        logger.error("Error: {0}".format(e))
    return retval

def get_snapped_indices(client, repo_name, prefix='logstash-'):
    """Return all indices in snapshots which succeeded and match prefix"""
    from itertools import chain
    try:
        allsnaps = client.snapshot.get(repository=repo_name, snapshot="_all")['snapshots']
        allindices = chain.from_iterable(s['indices'] for s in allsnaps if s['state'] == 'SUCCESS')
        return set(i for i in allindices if i.startswith(prefix))
    except elasticsearch.NotFoundError as e:
        logger.error("Error: {0}".format(e))
        return []

def get_version(client):
    """Return ES version number as a tuple"""
    version = client.info()['version']['number']
    return tuple(map(int, version.split('.')))

def get_object_list(client, prefix='logstash-', data_type='index', repo_name=None):
    """Return a list of indices or snapshots"""
    if data_type == 'index':
        object_list = get_indices(client, prefix)
    elif data_type == 'snapshot':
        if repo_name:
            object_list = get_snaplist(client, repo_name)
        else:
            logger.error('Repository name not specified. Returning empty list.')
            object_list = []
    else:
        object_list = []
        logger.error('data_type \'{0}\' is neither \'index\' nor \'snapshot\'.  Returning empty list.'.format(data_type))
    return object_list
    
def find_expired_data(client, time_unit, unit_count, object_list, separator='.', prefix='logstash-', utc_now=None):
    """ Generator that yields expired objects (indices or snapshots).

    :return: Yields tuples on the format ``(name, expired_by)`` where name
        is the name of the expired object and expired_by is the interval (timedelta) that the
        object was expired by.
    """
    # time-injection for test purposes only
    utc_now = utc_now if utc_now else datetime.utcnow()
    # reset to start of the period to be sure we are not retiring a human by mistake
    utc_now = utc_now.replace(minute=0, second=0, microsecond=0)

    if time_unit == 'hours':
        required_parts = 4
    else:
        required_parts = 3
        utc_now = utc_now.replace(hour=0)

    cutoff = utc_now - timedelta(**{time_unit: (unit_count - 1)})

    for object_name in object_list:

        unprefixed_object_name = object_name[len(prefix):]

        # find the timestamp parts (i.e ['2011', '01', '05'] from '2011.01.05') using the configured separator
        parts = unprefixed_object_name.split(separator)

        # verify we have a valid cutoff - hours for 4-part indices, days for 3-part
        if len(parts) != required_parts:
            logger.debug('Skipping {0} because it is of a type (hourly or daily) that I\'m not asked to evaluate.'.format(object_name))
            continue

        try:
            object_time = get_index_time(unprefixed_object_name, separator=separator)
        except ValueError:
            logger.error('Could not find a valid timestamp for {0}'.format(object_name))
            continue

        # if the index is older than the cutoff
        if object_time < cutoff:
            yield object_name, cutoff-object_time

        else:
            logger.info('{0} is {1} above the cutoff.'.format(object_name, object_time-cutoff))

def find_overusage_indices(client, disk_space_to_keep, separator='.', prefix='logstash-'):
    """ Generator that yields over usage indices.

    :return: Yields tuples on the format ``(index_name, 0)`` where index_name
    is the name of the expired index. The second element is only here for
    compatiblity reasons.
    """

    disk_usage = 0.0
    disk_limit = disk_space_to_keep * 2**30

    stats = client.indices.status(index=prefix+'*')
    sorted_indices = sorted(
        (
            (index_name, index_stats['index']['primary_size_in_bytes'])
            for (index_name, index_stats) in stats['indices'].items()
        ),
        reverse=True
    )

    for index_name, index_size in sorted_indices:
        disk_usage += index_size

        if disk_usage > disk_limit:
            yield index_name, 0
        else:
            logger.info('skipping {0}, disk usage is {1:.3f} GB and disk limit is {2:.3f} GB.'.format(index_name, disk_usage/2**30, disk_limit/2**30))

def index_closed(client, index_name):
    """Return True if index is closed"""
    index_metadata = client.cluster.state(
        index=index_name,
        metric='metadata',
    )
    return index_metadata['metadata']['indices'][index_name]['state'] == 'close'

def create_snapshot_body(indices, ignore_unavailable=False, include_global_state=False, partial=False):
    """Create the request body for creating a snapshot"""
    body = {
        "ignore_unavailable": ignore_unavailable,
        "include_global_state": include_global_state,
        "partial": partial,
    }
    if type(indices) is not type(list()):   # in case of a single value passed
        indices = [indices]
    body["indices"] = ','.join(sorted(indices))
    return body
    
def _get_repository(client, repo_name):
    """Get Repository information"""
    try:
        return client.snapshot.get_repository(repository=repo_name)
    except elasticsearch.NotFoundError as e:
        logger.info("Repository {0} not found.  Error: {1}".format(repo_name, e))
        return None

def _get_snapshot(client, repo_name, snap_name):
    """Get information about a snapshot (or snapshots)"""
    try:
        return client.snapshot.get(repository=repo_name, snapshot=snap_name)
    except elasticsearch.NotFoundError as e:
        logger.info("Snapshot or repository {0} not found.  Error: {1}".format(snap_name, e))
        return None

def _create_snapshot(client, snap_name, prefix='logstash-', repo_name=None, ignore_unavailable=False, include_global_state=False, partial=False, wait_for_completion=True):
    """Create a snapshot (or snapshots). Overwrite failures"""
    # Return True when it was skipped
    if not repo_name:
        logger.error("Unable to create snapshot. Repository name not provided.")
        return True
    try:
        successes = get_snapped_indices(client, repo_name, prefix=prefix)
        snaps = get_snaplist(client, repo_name, prefix=prefix)
        closed = index_closed(client, snap_name)
        body=create_snapshot_body(snap_name, ignore_unavailable=ignore_unavailable, include_global_state=include_global_state, partial=partial)
        if not snap_name in snaps and not snap_name in successes and not closed:
            client.snapshot.create(repository=repo_name, snapshot=snap_name, body=body, wait_for_completion=wait_for_completion)
        elif snap_name in snaps and not snap_name in successes and not closed:
            logger.warn("Previous snapshot was unsuccessful.  Deleting snapshot {0} and trying again.".format(snap_name))
            _delete_snapshot(client, repo_name, snap_name)
            client.snapshot.create(repository=repo_name, snapshot=snap_name, body=body, wait_for_completion=wait_for_completion)
        elif closed:
            logger.info("Skipping: Index {0} is closed.".format(snap_name))
            return True
        else:
            logger.info("Skipping: A snapshot with name '{0}' already exists.".format(snap_name))
            return True
    except elasticsearch.RequestError as e:
        logger.error("Unable to create snapshot {0}.  Error: {1} Check logs for more information.".format(snap_name, e))
        return True

def _delete_snapshot(client, snap_name, **kwargs):
    """Delete a snapshot (or snapshots)"""
    # kwargs is here to preserve expected number of args passed by index_loop
    client.snapshot.delete(repository=kwargs['repo_name'], snapshot=snap_name)
    
def _close_index(client, index_name, **kwargs):
    if index_closed(client, index_name):
        logger.info('Skipping index {0}: Already closed.'.format(index_name))
        return True
    else:
        client.indices.close(index=index_name)

def _delete_index(client, index_name, **kwargs):
    client.indices.delete(index=index_name)

def _optimize_index(client, index_name, max_num_segments=2, **kwargs):
    if index_closed(client, index_name): # Don't try to optimize a closed index
        logger.info('Skipping index {0}: Already closed.'.format(index_name))
        return True
    else:
        shards, segmentcount = get_segmentcount(client, index_name)
        logger.debug('Index {0} has {1} shards and {2} segments total.'.format(index_name, shards, segmentcount))
        if segmentcount > (shards * max_num_segments):
            logger.info('Optimizing index {0} to {1} segments per shard.  Please wait...'.format(index_name, max_num_segments))
            client.indices.optimize(index=index_name, max_num_segments=max_num_segments)
        else:
            logger.info('Skipping index {0}: Already optimized.'.format(index_name))
            return True

def _bloom_index(client, index_name, **kwargs):
    if index_closed(client, index_name): # Don't try to disable bloom filter on a closed index.  It will re-open them
        logger.info('Skipping index {0}: Already closed.'.format(index_name))
        return True
    else:
        client.indices.put_settings(index=index_name, body='index.codec.bloom.load=false')
        
def _require_index(client, index_name, attr, **kwargs):
    key = attr.split('=')[0]
    value = attr.split('=')[1]
    if index_closed(client, index_name):
      logger.info('Skipping index {0}: Already closed.'.format(index_name))
      return True
    else:
      logger.info('Updating index setting index.routing.allocation.require.{0}={1}'.format(key,value))
      client.indices.put_settings(index=index_name, body='index.routing.allocation.require.{0}={1}'.format(key,value))

OP_MAP = {
    'close'       : (_close_index, {'op': 'close', 'verbed': 'closed', 'gerund': 'Closing'}),
    'delete'      : (_delete_index, {'op': 'delete', 'verbed': 'deleted', 'gerund': 'Deleting'}),
    'optimize'    : (_optimize_index, {'op': 'optimize', 'verbed': 'optimized', 'gerund': 'Optimizing'}),
    'bloom'       : (_bloom_index, {'op': 'disable bloom filter for', 'verbed': 'bloom filter disabled', 'gerund': 'Disabling bloom filter for'}),
    'require'     : (_require_index, {'op': 'update require allocation rules for', 'verbed':'index routing allocation updated', 'gerund': 'Updating required index routing allocation rules for'}),
    'snapshot'    : (_create_snapshot, {'op': 'create snapshot for', 'verbed':'created snapshot', 'gerund': 'Initiating snapshot for'}),
    'delete_snaps': (_delete_snapshot, {'op': 'delete snapshot for', 'verbed':'deleted snapshot', 'gerund': 'Deleting snapshot for'}),
}

def snap_latest_indices(client, count, prefix='logstash-', dry_run=False, **kwargs):
    """Snapshot 'count' most recent indices matching prefix"""
    indices = [] # initialize...
    indices = get_indices(client, prefix)
    for index_name in indices[-count:]:
        if dry_run:
            logger.info('Would have attempted creating snapshot for {0}.'.format(index_name))
            continue
        else:
            if not index_closed(client, index_name):
                logger.info('Attempting to create snapshot for {0}...'.format(index_name))
            else:
                logger.warn('Unable to perform snapshot on closed index {0}'.format(index_name))
                continue
        
        skipped = _create_snapshot(client, index_name, prefix, **kwargs)
            
        if skipped:
            continue
        # if no error was raised and we got here that means the operation succeeded
        logger.info('Snapshot operation for index {0} succeeded.'.format(index_name))
    logger.info('Snapshot \'latest\' {0} indices operations completed.'.format(count))

def index_loop(client, operation, expired_indices, dry_run=False, by_space=False, **kwargs):
    op, words = OP_MAP[operation]
    for index_name, expiration in expired_indices:
        if dry_run and not by_space:
            logger.info('Would have attempted {0} index {1} because it is {2} older than the calculated cutoff.'.format(words['gerund'].lower(), index_name, expiration))
            continue
        elif dry_run and by_space:
            logger.info('Would have attempted {0} index {1} due to space constraints.'.format(words['gerund'].lower(), index_name))
            continue

        if not by_space:
            logger.info('Attempting to {0} index {1} because it is {2} older than cutoff.'.format(words['op'], index_name, expiration))
        else:
            logger.info('Attempting to {0} index {1} due to space constraints.'.format(words['op'].lower(), index_name))

        skipped = op(client, index_name, **kwargs)

        if skipped:
            continue

        # if no error was raised and we got here that means the operation succeeded
        logger.info('{0}: Successfully {1}.'.format(index_name, words['verbed']))
    if 'for' in words['op']:
        w = words['op'][:-4]
    else:
        w = words['op']
    logger.info('{0} index operations completed.'.format(w.upper()))

def get_segmentcount(client, index_name):
    """Return a list of shardcount, segmentcount"""
    shards = client.indices.segments(index=index_name)['indices'][index_name]['shards']
    segmentcount = 0
    totalshards = 0 # We will increment this manually to capture all replicas...
    for shardnum in shards:
        for shard in range(0,len(shards[shardnum])):
            segmentcount += shards[shardnum][shard]['num_search_segments']
            totalshards += 1
    return totalshards, segmentcount

def main():
    start = time.time()

    parser = make_parser()
    arguments = parser.parse_args()
    argdict = arguments.__dict__

    # Do not log and force dry-run if we opt to show indices.
    if arguments.show_indices or arguments.show_repositories or arguments.show_snapshots:
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

    if arguments.show_indices or arguments.show_repositories or arguments.show_snapshots:
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

    # Show indices then exit
    if arguments.show_indices:
        for index_name in get_indices(client, arguments.prefix):
            print('{0}'.format(index_name))
        sys.exit(0)
    # Show repositories then exit
    if arguments.show_repositories:
        for repository in sorted(_get_repository(client, '_all').keys()):
            print('{0}'.format(repository))
        sys.exit(0)
    # Show snapshots from repository then exit
    if arguments.show_snapshots:
        if not arguments.repository:
            print("Must specify --repository with this option.")
        else:
            for snapshot in get_snaplist(client, arguments.repository, prefix=arguments.prefix):
                print('{0}'.format(snapshot))
        sys.exit(0)
    # Delete by space first
    if arguments.disk_space:
        logger.info('Deleting indices by disk usage over {0} gigabytes'.format(arguments.disk_space))
        expired_indices = find_overusage_indices(client, arguments.disk_space, arguments.separator, arguments.prefix)
        index_loop(client, 'delete', expired_indices, arguments.dry_run, by_space=True)
    else: # Everything else is by time
        index_list = get_object_list(client, prefix=arguments.prefix)
    # Delete by time
    if arguments.delete_older:
        logger.info('Deleting indices older than {0} {1}...'.format(arguments.delete_older, arguments.time_unit))
        expired_indices = find_expired_data(client, time_unit=arguments.time_unit, unit_count=arguments.delete_older, object_list=index_list, separator=arguments.separator, prefix=arguments.prefix)
        index_loop(client, 'delete', expired_indices, arguments.dry_run)
    # Re-capture index_list after deletion, in case some were removed.
    index_list = get_object_list(client, prefix=arguments.prefix)
    # Close by time
    if arguments.close_older:
        logger.info('Closing indices older than {0} {1}...'.format(arguments.close_older, arguments.time_unit))
        expired_indices = find_expired_data(client, time_unit=arguments.time_unit, unit_count=arguments.close_older, object_list=index_list, separator=arguments.separator, prefix=arguments.prefix)
        index_loop(client, 'close', expired_indices, arguments.dry_run)
    # Disable bloom filter by time
    if arguments.bloom_older:
        logger.info('Disabling bloom filter on indices older than {0} {1}...'.format(arguments.bloom_older, arguments.time_unit))
        expired_indices = find_expired_data(client, time_unit=arguments.time_unit, unit_count=arguments.bloom_older, object_list=index_list, separator=arguments.separator, prefix=arguments.prefix)
        index_loop(client, 'bloom', expired_indices, arguments.dry_run)
    # Optimize index
    if arguments.optimize:
        logger.info('Optimizing indices older than {0} {1}...'.format(arguments.optimize, arguments.time_unit))
        expired_indices = find_expired_data(client, time_unit=arguments.time_unit, unit_count=arguments.optimize, object_list=index_list, separator=arguments.separator, prefix=arguments.prefix)
        index_loop(client, 'optimize', expired_indices, arguments.dry_run, max_num_segments=arguments.max_num_segments)
    # Required routing rules
    if arguments.require:
        logger.info('Updating required routing allocation rules on indices older than {0} {1}...'.format(arguments.require, arguments.time_unit))
        expired_indices = find_expired_data(client, time_unit=arguments.time_unit, unit_count=arguments.require, object_list=index_list, separator=arguments.separator, prefix=arguments.prefix)
        index_loop(client, 'require', expired_indices, arguments.dry_run, attr=arguments.required_rule)
    # Delete snapshot
    if arguments.delete_snaps and arguments.repository:
        snapshot_list = get_object_list(client, prefix=arguments.prefix, data_type='snapshot', repo_name=arguments.repository)
        logger.info('Deleting snapshots older than {0} {1}...'.format(arguments.delete_snaps, arguments.time_unit))
        expired_snaps = find_expired_data(client, time_unit=arguments.time_unit, unit_count=arguments.delete_snaps, object_list=snapshot_list,  separator=arguments.separator, prefix=arguments.prefix)
        index_loop(client, 'delete_snaps', expired_snaps, arguments.dry_run, prefix=arguments.prefix, repo_name=arguments.repository)
    # Snap latest
    if arguments.snap_latest and arguments.repository:
        logger.info('Adding snapshot of {0} most recent indices matching prefix \'{1}\' to repository {2}...'.format(arguments.snap_latest, arguments.prefix, arguments.repository))
        snap_latest_indices(client, arguments.snap_latest, prefix=arguments.prefix, dry_run=arguments.dry_run, 
                            repo_name=arguments.repository, ignore_unavailable=arguments.ignore_unavailable, 
                            include_global_state=arguments.include_global_state, partial=arguments.partial, 
                            wait_for_completion=arguments.wait_for_completion)
    # Take snapshot
    if arguments.snap_older and arguments.repository:
        logger.info('Adding snapshot of indices older than {0} {1} to repository {2}...'.format(arguments.snap_older, arguments.time_unit, arguments.repository))
        expired_indices = find_expired_data(client, time_unit=arguments.time_unit, unit_count=arguments.snap_older, object_list=index_list, separator=arguments.separator, prefix=arguments.prefix)
        index_loop(client, 'snapshot', expired_indices, arguments.dry_run, prefix=arguments.prefix, 
                    repo_name=arguments.repository, ignore_unavailable=arguments.ignore_unavailable, 
                    include_global_state=arguments.include_global_state, partial=arguments.partial, 
                    wait_for_completion=arguments.wait_for_completion)

    logger.info('Done in {0}.'.format(timedelta(seconds=time.time()-start)))

if __name__ == '__main__':
    main()
