from AtomicCounter import AtomicCounter
from ShallowBucketScrubber import ShallowBucketScrubber
import Constants
from ClusterUtil import ClusterUtil
from Logger import Logger
from multiprocessing.dummy import Pool as ThreadPool
from ProgressBar import ProgressBar
from Scrubber import Scrubber
from Util import Util


class UserScrubber(Scrubber):

    def __init__(self, cluster, user_id, num_threads, total_threads, bucket_data_pool, bucket_data_cache_pool, conf_file_path, output_file_name_format, log_file):
        self.cluster = cluster
        self.user_id = user_id
        self.num_threads = num_threads
        self.total_threads = total_threads
        self.bucket_data_pool = bucket_data_pool
        self.bucket_data_cache_pool = bucket_data_cache_pool
        self.conf_file_path = conf_file_path
        self.output_file_name_format = output_file_name_format
        self.logger = Logger(log_file)
        self.cluster_util = ClusterUtil(cluster, self.logger)
        self.bucket_list = []
        self.log_file = log_file


        self.processed_buckets = {}
        self.total_buckets = 0
        self.atomic_counter = AtomicCounter()
        self.progress_bar = None

    def start_progress_bar(self):
        self.progress_bar = ProgressBar(self.total_buckets)
        self.progress_bar.start()

    def end_progress_bar(self):
        self.progress_bar.finish()

    def prepare_bucket_list(self):
        start_time = Util.get_timestamp()

        self.logger.info("Preparing bucket list")
        self.bucket_list = self.cluster_util.get_all_buckets(self.user_id)
        self.total_buckets = len(self.bucket_list)

        self.logger.info("Total time for preparing bucket list %d" % Util.get_lapsed_time(start_time))

    def scrub_bucket(self, bucket):
        if self.atomic_counter.get_count() > 1:
            raise ValueError("Hey raised custom exception")
        self.logger.info("Scrubbing bucket %s" % bucket.name)
        scrubber = ShallowBucketScrubber(self.cluster, bucket.name, self.bucket_data_pool, self.bucket_data_cache_pool,
                                       self.conf_file_path, self.total_threads / self.num_threads,
                                       self.output_file_name_format % bucket.name, self.log_file, show_progress_bar=False)
        scrubber.run()
        self.logger.info("Scrubbing got over for bucket %s" % bucket.name)
        self.processed_buckets[bucket.name] = True
        self.atomic_counter.increment()
        self.progress_bar.update(self.atomic_counter.get_count())

    def scrub_buckets(self):
        start_time = Util.get_timestamp()

        pool = ThreadPool(self.num_threads)
        pool.map(self.scrub_bucket, self.bucket_list)
        pool.close()
        pool.join()

        self.logger.info("Bucket scrubing took %s seconds" % Util.get_lapsed_time(start_time))

    def trim_bucket_list(self):
        snapshot = Util.get_object_from_file(Constants.SNAPSHOT_FILE)
        user_id = snapshot["user_id"]
        processed_buckets = snapshot["processed_buckets"]

        self.prepare_bucket_list()

        buckets_to_process = []
        for bucket in self.bucket_list:
            if bucket.name not in processed_buckets:
                buckets_to_process.append(bucket)

        self.logger.info("Total number of buckets %d" % len(self.bucket_list))
        self.bucket_list = buckets_to_process
        self.total_buckets = len(buckets_to_process)
        self.logger.info("Number of objects that were not processed %d" % self.total_buckets)

    def get_snapshot(self):
        snapshot = {}
        snapshot["user_id"] = self.user_id
        snapshot["processed_buckets"] = self.processed_buckets
        return snapshot

    def has_completed(self):
        return self.total_buckets == len(self.processed_buckets)

    def resume(self):
        self.run(resume_operation=True)

    def run(self, resume_operation=False):
        start_time = Util.get_timestamp()

        self.logger.info("User scrubbing has started")

        self.prepare_bucket_list()
        if resume_operation:
            self.trim_bucket_list()
        self.start_progress_bar()
        self.scrub_buckets()
        self.end_progress_bar()
        self.logger.info("Total time %s secs" % (Util.get_lapsed_time(start_time)))

