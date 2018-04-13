from Cluster import Cluster
from ClusterUtil import ClusterUtil
from FileUtil import FileUtil
from multiprocessing.dummy import Pool as ThreadPool
from Util import Util


class BucketScrubber(object):
    #Static members
    temp_file_name = '/tmp/temp_file.tmp'

    def __init__(self, cluster_name, bucket_name, num_threads, output_file_name):
        self.cluster = Cluster(cluster_name)
        self.bucket_name = bucket_name
        self.num_threads = num_threads
        self.output_file_invalid_objects = output_file_name
        self.cluster_util = ClusterUtil(cluster_name)
        self.file_obj = FileUtil.get_file_obj_for_write(self.output_file_invalid_objects)
        self.object_list = []

    def prepare_object_list(self):
        start_time = Util.get_timestamp()

        print "Preparing object list.."
        self.object_list = self.cluster_util.get_objects(self.bucket_name, 8)
        print "Total objects %d" % len(self.object_list)

        print ("Object list preparation took %s seconds" % Util.get_lapsed_time(start_time))

    # Scraps OBJECT_LIST
    def scrap_objects(self):
        start_time = Util.get_timestamp()

        pool = ThreadPool(self.num_threads)
        pool.map(self.scrap_object, self.object_list)
        pool.close()
        pool.join()

        print ("Object scraping took %s seconds" % Util.get_lapsed_time(start_time))

    def close_file(self):
        FileUtil.close_file(self.file_obj)

    def run(self):
        start_time = Util.get_timestamp()

        try:
            print "Scrubbing bucket %s" % self.bucket_name
            self.prepare_object_list()
            self.scrap_objects()
            self.close_file()
            print "Scrubbing got over for bucket %s" % self.bucket_name
            print ("Total time %s seconds" % Util.get_lapsed_time(start_time))
        except Exception as ex:
            print "Exception happened while scrubbing bucket %s" % self.bucket_name
            print str(ex)
            pass
