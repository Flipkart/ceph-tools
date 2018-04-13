from AtomicCounter import AtomicCounter
from Cluster import Cluster
from ClusterUtil import ClusterUtil
from FileUtil import FileUtil
from multiprocessing.dummy import Pool as ThreadPool
from ProgressBar import ProgressBar
from Util import Util


class BucketScrubber(object):
    #Static members --> Move to boto bucket scrubber
    temp_file_name = '/tmp/temp_file.tmp'

    def __init__(self, cluster_name, bucket_name, num_threads, output_file_name):
        self.cluster = Cluster(cluster_name)
        self.bucket_name = bucket_name
        self.num_threads = num_threads
        self.output_file_invalid_objects = output_file_name
        self.cluster_util = ClusterUtil(cluster_name)

        self.file_obj = FileUtil.get_file_obj_for_write(self.output_file_invalid_objects)
        self.object_list = []
        self.total_objects = 0
        self.atomic_counter = AtomicCounter()
        self.progress_bar = None

    def start_progress_bar(self):
        self.progress_bar = ProgressBar(self.total_objects)
        self.progress_bar.start()

    def end_progress_bar(self):
        self.progress_bar.finish()

    def prepare_object_list(self):
        start_time = Util.get_timestamp()

        print "Preparing object list.."
        self.object_list = self.cluster_util.get_objects(self.bucket_name, 20000)
        # self.object_list = self.cluster_util.get_all_objects(self.bucket_name)
        self.total_objects = len(self.object_list)
        print "Total objects %d" % self.total_objects

        # self._init_progress_bar()

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
            # Add open file
            self.prepare_object_list()

            self.start_progress_bar()
            self.scrap_objects()
            self.end_progress_bar()

            self.close_file()
            print "Scrubbing got over for bucket %s" % self.bucket_name
            print ("Total time %s seconds" % Util.get_lapsed_time(start_time))
        except Exception as ex:
            print "Exception happened while scrubbing bucket %s" % self.bucket_name
            print str(ex)
            pass
