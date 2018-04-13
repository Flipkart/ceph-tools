from ConfigManager import ConfigManager
from BotoBucketScrubber import BotoBucketScrubber
from FileUtil import FileUtil
import sys

HELP_FILE_LOCATION = './help/scrub_bucket_boto'


def print_help():
    FileUtil.print_file(HELP_FILE_LOCATION)


def show_help(args):
    return len(args) == 0 or args[0] == "--help"


if __name__ == '__main__':
    args = sys.argv[1:]

    if show_help(args):
        print_help()
        sys.exit(0)

    config_file = args[0]
    config_manager = ConfigManager(config_file)

    cluster_name = config_manager.get_config_value("cluster_name")
    bucket_name = config_manager.get_config_value("bucket_name")
    num_threads = config_manager.get_config_value("num_threads")
    output_file_name = config_manager.get_config_value("output_file_name")

    print "Bucket to be scrubbed %s" % bucket_name
    print "Number of threads %d" % num_threads
    bucket_scraper = BotoBucketScrubber(cluster_name, bucket_name, num_threads, output_file_name)
    bucket_scraper.run()
