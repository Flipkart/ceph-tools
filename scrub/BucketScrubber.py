from common.AtomicCounter import AtomicCounter
from utils.ClusterUtil import ClusterUtil
import common.Constants as Constants
from utils.FileUtil import FileUtil
from common.Logger import Logger
from multiprocessing.dummy import Pool as ThreadPool
from common.ProgressBar import ProgressBar
from Scrubber import Scrubber
from utils.Util import Util


class BucketScrubber(Scrubber):
    def __init__(self, cluster, bucket_name, num_threads, output_file_name, log_file, show_progress_bar=True):
        self.cluster = cluster
        self.bucket_name = bucket_name
        self.num_threads = num_threads
        self.output_file_invalid_objects = output_file_name
        self.log_file = log_file

        self.logger = Logger(log_file)
        self.cluster_util = ClusterUtil(cluster, self.logger)
        self.file_obj = None
        self.object_list = []
        self.processed_objects = {}
        self.total_objects = 0
        self.atomic_counter = AtomicCounter()
        self.progress_bar = None
        self.show_progress_bar = show_progress_bar

    def start_progress_bar(self):
        if self.show_progress_bar:
            self.progress_bar = ProgressBar(self.total_objects)
            self.progress_bar.start()

    def end_progress_bar(self):
        if self.show_progress_bar:
            self.progress_bar.finish()

    def has_completed(self):
        return self.total_objects == len(self.processed_objects)

    def prepare_object_list(self):
        start_time = Util.get_timestamp()

        self.logger.info("Preparing object list")
        self.object_list = self.cluster_util.get_objects(self.bucket_name, 8)
        # self.object_list = self.cluster_util.get_all_objects(self.bucket_name)
        self.total_objects = len(self.object_list)
        self.logger.info("Total objects %d" % self.total_objects)
        self.logger.info("Object list preparation took %s seconds" % Util.get_lapsed_time(start_time))

    # scrubs OBJECT_LIST
    def scrub_objects(self):
        start_time = Util.get_timestamp()

        pool = ThreadPool(self.num_threads)
        pool.map(self.scrub_object, self.object_list)
        pool.close()
        pool.join()

        self.logger.info("Object scrubbing took %s seconds" % Util.get_lapsed_time(start_time))

    def open_file(self):
        self.file_obj = FileUtil.get_file_obj_for_write(self.output_file_invalid_objects)

    def close_file(self):
        FileUtil.close_file(self.file_obj)

    def get_snapshot(self):
        snapshot = {}
        snapshot["bucket_name"] = self.bucket_name
        snapshot["processed_objects"] = self.processed_objects
        return snapshot

    def trim_object_list(self):
        snapshot = Util.get_object_from_file(Constants.SNAPSHOT_FILE)
        bucket_name = snapshot["bucket_name"]
        processed_objects = snapshot["processed_objects"]

        self.prepare_object_list()

        objects_to_process = []
        for object in self.object_list:
            if object.name not in processed_objects:
                objects_to_process.append(object)

        self.logger.info("Total number of objects %d" % len(self.object_list))
        self.object_list = objects_to_process
        self.total_objects = len(objects_to_process)
        self.logger.info("Number of objects that were not processed %d" % self.total_objects)

    def resume(self):
        self.run(resume_operation=True)

    def run(self, resume_operation = False):
        start_time = Util.get_timestamp()

        try:
            self.logger.info("Scrubbing bucket %s" % self.bucket_name)
            self.prepare_object_list()

            if resume_operation:
                self.trim_object_list()

            self.open_file()

            self.start_progress_bar()
            self.scrub_objects()
            self.end_progress_bar()
            self.close_file()
            self.logger.info("Scrubbing got over for bucket %s" % self.bucket_name)
            self.logger.info("Total time %s seconds" % Util.get_lapsed_time(start_time))
        except Exception as ex:
            self.logger.info("Exception happened while scrubbing bucket %s\nException: %s" % (self.bucket_name, str(ex)))
            pass
