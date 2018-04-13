from ConfigManager import ConfigManager
from RadosBucketScrubber import RadosBucketScrubber
from FileUtil import FileUtil
import sys

HELP_FILE_LOCATION = './help/scrub_bucket_rados'


def print_help():
    FileUtil.print_file(HELP_FILE_LOCATION)


def show_help(args):
    return len(args) == 0 or args[0] == "--help"


if __name__ == '__main__':
    args = sys.argv[1:]

    if show_help(args):
        print_help()
        sys.exit(0)

    conf_file = args[0]
    config_manager = ConfigManager(conf_file)

    cluster_name = config_manager.get_config_value("cluster_name")
    bucket_name = config_manager.get_config_value("bucket_name")
    num_threads = config_manager.get_config_value("num_threads")
    base_tier_pool = config_manager.get_config_value("base_tier_pool")
    cache_tier_pool = config_manager.get_config_value("cache_tier_pool")
    conf_file_path = config_manager.get_config_value("conf_file_path")
    output_file_name = config_manager.get_config_value("output_file_name")

    print "Bucket to be scrubbed %s" % bucket_name
    print "Number of threads %d" % num_threads
    bucket_scraper = RadosBucketScrubber(cluster_name, bucket_name, base_tier_pool, cache_tier_pool, conf_file_path, num_threads, output_file_name)
    bucket_scraper.run()
