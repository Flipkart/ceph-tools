from RadosBucketScrubber import RadosBucketScrubber
from ClusterUtil import ClusterUtil
from multiprocessing.dummy import Pool as ThreadPool
from Util import Util


class UserScrubber:

    def __init__(self, cluster_name, user_id, num_threads, total_threads, base_tier_pool, cache_tier_pool, conf_file_path, output_file_name_format):
        self.cluster_name = cluster_name
        self.user_id = user_id
        self.num_threads = num_threads
        self.total_threads = total_threads
        self.base_tier_pool = base_tier_pool
        self.cache_tier_pool = cache_tier_pool
        self.conf_file_path = conf_file_path
        self.output_file_name_format = output_file_name_format
        self.cluster_util = ClusterUtil(cluster_name)
        self.bucket_list = []

    def prepare_bucket_list(self):
        start_time = Util.get_timestamp()

        print "Preparing bucket list"
        self.bucket_list = self.cluster_util.get_all_buckets(self.user_id)

        print "Total time for preparing bucket list %d" % Util.get_lapsed_time(start_time)

    def scrub_bucket(self, bucket):
        print "Scrubbing bucket %s" % bucket.name
        scrubber = RadosBucketScrubber(self.cluster_name, bucket.name, self.base_tier_pool, self.cache_tier_pool, self.conf_file_path, self.total_threads / self.num_threads, self.output_file_name_format % bucket.name)
        scrubber.run()
        print "Scrubbing got over for bucket %s" % bucket.name

    def scrub_buckets(self):
        start_time = Util.get_timestamp()

        pool = ThreadPool(self.num_threads)
        pool.map(self.scrub_bucket, self.bucket_list)
        pool.close()
        pool.join()

        print ("Object scraping took %s seconds" % Util.get_lapsed_time(start_time))

    def run(self):
        start_time = Util.get_timestamp()

        print "User scrubbing has started"
        self.prepare_bucket_list()
        self.scrub_buckets()

        print "Total time %s secs" % (Util.get_lapsed_time(start_time))

