from ConfigManager import ConfigManager
from FileUtil import FileUtil
import sys
from UserScrubber import UserScrubber

HELP_FILE_LOCATION = './help/scrub_user'


def print_help():
    FileUtil.print_file(HELP_FILE_LOCATION)


def show_help(args):
    return len(args) == 0 or args[0] == "--help" or args[0] == "gtarocks"


if __name__ == '__main__':
    args = sys.argv[1:]

    if show_help(args):
        print_help()
        sys.exit(0)

    conf_file = args[0]
    config_manager = ConfigManager(conf_file)

    cluster_name = config_manager.get_config_value("cluster_name")
    user_name = config_manager.get_config_value("user_name")
    num_buckets_to_process_parallelly = config_manager.get_config_value("num_buckets_to_process_parallelly")
    total_threads = config_manager.get_config_value("total_threads")
    base_tier_pool = config_manager.get_config_value("base_tier_pool")
    cache_tier_pool = config_manager.get_config_value("cache_tier_pool")
    conf_file_path = config_manager.get_config_value("conf_file_path")
    output_file_name_format = config_manager.get_config_value("output_file_name_format")

    scrubber = UserScrubber(cluster_name, user_name, num_buckets_to_process_parallelly, total_threads, base_tier_pool, cache_tier_pool, conf_file_path, output_file_name_format)
    scrubber.run()
