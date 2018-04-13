import atexit
from common.ArgumentParser import ArgumentParser
from DeepBucketScrubber import DeepBucketScrubber
from ShallowBucketScrubber import ShallowBucketScrubber
from Cluster import Cluster
import common.Constants as Constants
from common.GlobalConfigManager import GlobalConfigManager
from UserScrubber import UserScrubber
from utils.Util import Util


def get_deep_bucket_scrubber(global_config_manager, cluster):
    bucket_name = global_config_manager.get_config(Constants.CONF_SECTION_BUCKET_SCRUBBER, Constants.BUCKET_NAME)
    num_threads = int(global_config_manager.get_config(Constants.CONF_SECTION_BUCKET_SCRUBBER, Constants.NUM_THREADS))
    output_file_name = global_config_manager.get_config(Constants.CONF_SECTION_BUCKET_SCRUBBER, Constants.OUTPUT_FILE_NAME)
    log_file = global_config_manager.get_config(Constants.CONF_SECTION_SCRUBBER, Constants.LOG_FILE)
    return DeepBucketScrubber(cluster, bucket_name, num_threads, output_file_name, log_file, show_progress_bar=True)


def get_shallow_bucket_scrubber(global_config_manager, cluster):
    bucket_name = global_config_manager.get_config(Constants.CONF_SECTION_BUCKET_SCRUBBER, Constants.BUCKET_NAME)
    num_threads = int(global_config_manager.get_config(Constants.CONF_SECTION_BUCKET_SCRUBBER, Constants.NUM_THREADS))
    output_file_name = global_config_manager.get_config(Constants.CONF_SECTION_BUCKET_SCRUBBER, Constants.OUTPUT_FILE_NAME)
    bucket_data_pool = global_config_manager.get_config(Constants.CONF_SECTION_SHALLOW_SCRUBBER, Constants.bucket_data_pool)
    bucket_data_cache_pool = global_config_manager.get_config(Constants.CONF_SECTION_SHALLOW_SCRUBBER, Constants.bucket_data_cache_pool)
    cache_tiering_enabled = global_config_manager.get_config(Constants.CONF_SECTION_SHALLOW_SCRUBBER, Constants.CACHE_TIERING_ENABLED)
    conf_file_path = global_config_manager.get_config(Constants.CONF_SECTION_SHALLOW_SCRUBBER, Constants.CONF_FILE_PATH)
    log_file = global_config_manager.get_config(Constants.CONF_SECTION_SCRUBBER, Constants.LOG_FILE)
    return ShallowBucketScrubber(cluster, bucket_name, cache_tiering_enabled, bucket_data_pool, bucket_data_cache_pool, conf_file_path, num_threads, output_file_name, log_file, show_progress_bar=True)


def get_user_scrubber(global_config_manager, cluster):
    user_name = global_config_manager.get_config(Constants.CONF_SECTION_USER_SCRUBBER, Constants.USER_NAME)
    num_buckets_to_process_parallelly = int(global_config_manager.get_config(Constants.CONF_SECTION_USER_SCRUBBER, Constants.NUM_BUCKETS_TO_PROCESS_PARALLELLY))
    total_threads = int(global_config_manager.get_config(Constants.CONF_SECTION_USER_SCRUBBER, Constants.TOTAL_THREADS))
    bucket_data_pool = global_config_manager.get_config(Constants.CONF_SECTION_SHALLOW_SCRUBBER, Constants.bucket_data_pool)
    bucket_data_cache_pool = global_config_manager.get_config(Constants.CONF_SECTION_SHALLOW_SCRUBBER, Constants.bucket_data_cache_pool)
    conf_file_path = global_config_manager.get_config(Constants.CONF_SECTION_SHALLOW_SCRUBBER, Constants.CONF_FILE_PATH)
    output_file_name_format = global_config_manager.get_config(Constants.CONF_SECTION_USER_SCRUBBER, Constants.OUTPUT_FILE_NAME_FORMAT)
    log_file = global_config_manager.get_config(Constants.CONF_SECTION_SCRUBBER, Constants.LOG_FILE)
    return UserScrubber(cluster, user_name, num_buckets_to_process_parallelly, total_threads, bucket_data_pool,
                        bucket_data_cache_pool, conf_file_path, output_file_name_format, log_file)


def store_snapshot(scrubber):
    if scrubber.has_completed():
        return

    snapshot = scrubber.get_snapshot()
    Util.dump_object_to_file(snapshot, Constants.SNAPSHOT_FILE)


def register_exit_handlers(arg):
    atexit.register(store_snapshot, arg)

if __name__ == '__main__':
    args = ArgumentParser().get_args()

    conf_file = args.config_file

    global_config_manager = GlobalConfigManager(conf_file)
    access_key = global_config_manager.get_config(Constants.CONF_SECTION_SCRUBBER, Constants.ACCESS_KEY)
    secret_key = global_config_manager.get_config(Constants.CONF_SECTION_SCRUBBER, Constants.SECRET_KEY)
    host = global_config_manager.get_config(Constants.CONF_SECTION_SCRUBBER, Constants.HOST)
    scrub_type = global_config_manager.get_config(Constants.CONF_SECTION_SCRUBBER, Constants.TYPE)

    cluster = Cluster(access_key, secret_key, host)

    if scrub_type == Constants.DEEP_BUCKET_SCRUB:
        scrubber = get_deep_bucket_scrubber(global_config_manager, cluster)
    elif scrub_type == Constants.SHALLOW_BUCKET_SCRUB:
        scrubber = get_shallow_bucket_scrubber(global_config_manager, cluster)
    elif scrub_type == Constants.USER_SCRUB:
        scrubber = get_user_scrubber(global_config_manager, cluster)
    else:
        raise ValueError("Unknown type")

    register_exit_handlers(scrubber)

    if args.resume:
        scrubber.resume()
    else:
        scrubber.run()
