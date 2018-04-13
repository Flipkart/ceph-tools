from ConfigManager import ConfigManager
from FileUtil import FileUtil
from ParserUtil import ParserUtil
import PGScrubber as Scrubber
import sys
from Util import Util

INPUT_FILE_CLEANSED_NAME = '%s_cleansed'
HELP_FILE_LOCATION = "./help/scrub_pg"

def cleanse_input_file(file_name, out_filename):
    ParserUtil.extract_empty_files(file_name, out_filename)

def print_help():
    FileUtil.print_file(HELP_FILE_LOCATION)

def show_help(args):
    return len(args) < 1 or len(args) > 1 or args[0] == "--help"

if __name__ == '__main__':
    start_time = Util.get_timestamp()

    args = sys.argv[1:]

    if show_help(args):
        print_help()
        sys.exit(0)

    conf_file = args[0]
    config_manager = ConfigManager(conf_file)

    cluster_name = config_manager.get_config_value("cluster_name")
    input_file = config_manager.get_config_value("input_file")
    input_file_bucket_meta = config_manager.get_config_value("input_file_bucket_meta")
    output_file_good_objects = config_manager.get_config_value("output_file_good_objects")
    output_file_bad_objects = config_manager.get_config_value("output_file_bad_objects")
    output_file_missed_buckets = config_manager.get_config_value("output_file_missed_buckets")
    output_file_missed_objects = config_manager.get_config_value("output_file_missed_objects")

    #Cleanse input file
    input_file_cleansed = INPUT_FILE_CLEANSED_NAME % input_file
    cleanse_input_file(input_file, input_file_cleansed)

    file_names = FileUtil.get_file_content(input_file_cleansed)
    bucket_metas = FileUtil.get_file_content(input_file_bucket_meta)
    scrubber = Scrubber.PGScrubber(cluster_name, file_names, bucket_metas, output_file_good_objects, output_file_bad_objects, output_file_missed_buckets, output_file_missed_objects)
    scrubber.run()

    print "Total time %s seconds" % Util.get_lapsed_time(start_time)