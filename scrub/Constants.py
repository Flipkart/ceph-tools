SHALLOW_BUCKET_SCRUB = "shallow_bucket_scrub"
DEEP_BUCKET_SCRUB = "deep_bucket_scrub"
USER_SCRUB = "user_scrub"

ACCESS_KEY = "access_key"
SECRET_KEY = "secret_key"
HOST = "host"
TYPE = "type"
LOG_FILE = "log_file"
BUCKET_NAME = "bucket_name"
NUM_THREADS = "num_threads"
OUTPUT_FILE_NAME = "output_file_name"
bucket_data_pool = "bucket_data_pool"
bucket_data_cache_pool = "bucket_data_cache_pool"
CACHE_TIERING_ENABLED = "cache_tiering_enabled"
CONF_FILE_PATH = "conf_file_path"
USER_NAME = "user_name"
NUM_BUCKETS_TO_PROCESS_PARALLELLY = "num_buckets_to_process_parallelly"
TOTAL_THREADS = "total_threads"
OUTPUT_FILE_NAME_FORMAT = "output_file_name_format"

CONF_SECTION_SCRUBBER = "scrubber"
CONF_SECTION_BUCKET_SCRUBBER = "bucket_scrubber"
CONF_SECTION_SHALLOW_SCRUBBER = "shallow_bucket_scrubber"
CONF_SECTION_USER_SCRUBBER = "user_scrubber"


PROGRESS_BAR_MESSAGE = "Processed:"
PROGRESS_BAR_MARKER = "#"

#Rados object name: BucketMarker_ObjectName
RADOS_OBJECT_NAME_FORMAT = '%s_%s'

LOGGER_FORMAT  = '%(asctime)s %(levelname)-8s %(message)s'
LOGGER_DATE_FORMAT = '%Y-%m-%d %H:%M:%S'
LOGGER_APP_ID = 'scrub'

SNAPSHOT_FILE = ".snapshot"

# Default number of objects that are retrieved in every iteration
KEYS_PER_QUERY_DEFAULT = 10000
# Max number of objects a bucket can have
MAX_NO_OF_OBJECTS = 99999999
#To-do: Build a CommandRetriever which checks the version of ceph and builds the command
COMMAND_BUCKET_STATS = 'sudo radosgw-admin bucket stats --bucket=%s'

TEMP_FILE = '/tmp/temp_file.tmp'
