from ClusterUtil import ClusterUtil
from CredentialManager import CredentialManager
from FileUtil import FileUtil
import sys
from Util import Util

HELP_FILE_LOCATION = './help/validate_objects'

ITERATION = 33
NUM_BUCKETS_PER_ITERATION = 1
OUTPUT_FILE_GOOD = 'good_objects%d'
OUTPUT_FILE_BAD = 'bad_objects%d'

cluster_util = ClusterUtil("prod-d42sa")


def print_help():
    FileUtil.print_file(HELP_FILE_LOCATION)


def show_help(args):
    return len(args) == 0 or args[0] == "--help" or args[0] == "gtarocks"

def validate_objects(all_objects, objects_to_validate):
    file_obj_good = FileUtil.get_file_obj_for_write(OUTPUT_FILE_GOOD)
    file_obj_bad = FileUtil.get_file_obj_for_write(OUTPUT_FILE_BAD)

    for obj in objects_to_validate:
        if obj in all_objects:
            FileUtil.write_file(file_obj_good, obj)
        else:
            FileUtil.write_file(file_obj_bad, obj)

    FileUtil.close_file(file_obj_good)
    FileUtil.close_file(file_obj_bad)

def extract_all_objects(buckets):
    all_objects = {}
    count = 0
    credential_manager = CredentialManager()
    credentials = credential_manager.get_credentials("prod-d42sa")
    admin_host = credentials["admin_host"]
    total = 0
    for bucket in buckets:
        if count >= 8:
            break
        print "Getting all objects of bucket %s" % bucket
        # num_objects = cluster_util.get_num_of_objects(bucket, admin_host)
        # print "Bucket %s numofobjects: %d" % (bucket, num_objects)
        # total = total + num_objects
        objects = cluster_util.get_objects(bucket, 8, 50000)
        # print "bucket %s has %d objects" % (bucket, len(objects))
        for object in objects:
            all_objects[object.name] = True
        count = count + 1
    return all_objects

def get_bucket_per_iteration(all_buckets):
    return all_buckets[(ITERATION - 1) * NUM_BUCKETS_PER_ITERATION : ITERATION * NUM_BUCKETS_PER_ITERATION]

def refresh_global_vars(iteration):
    global ITERATION
    global OUTPUT_FILE_BAD
    global OUTPUT_FILE_GOOD
    ITERATION = iteration
    OUTPUT_FILE_GOOD = OUTPUT_FILE_GOOD % iteration
    OUTPUT_FILE_BAD = OUTPUT_FILE_BAD % iteration


if __name__ == '__main__':
    start_time = Util.get_timestamp()

    args = sys.argv[1:]

    if show_help(args):
        print_help()
        sys.exit(0)

    file_name = args[0]
    result_file_name = args[1]
    iteration = int(args[2])

    refresh_global_vars(iteration)

    object_infos = FileUtil.get_file_content(file_name)
    potential_bad_objects = FileUtil.get_file_content(result_file_name)

    buckets = {}
    objects = {}

    for object_info in object_infos:
        bucket_name = object_info.split(' ')[0]
        #object_name = object_info.split(' ')[1]
        buckets[bucket_name] = True
        #objects[object_name] = True

    for obj in potential_bad_objects:
        objects[obj] = True

    all_buckets = list(buckets.keys())

    print "All buckets"
    print len(buckets)

    buckets_to_process = get_bucket_per_iteration(all_buckets)

    print "Buckets to process in this iteration"
    print buckets_to_process
    all_objects = extract_all_objects(buckets_to_process)
    print len(all_objects)

    validate_objects(all_objects, objects)

    print "Total time %s secs" % (Util.get_lapsed_time(start_time))