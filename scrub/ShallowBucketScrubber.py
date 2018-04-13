from BucketScrubber import BucketScrubber
from FileUtil import FileUtil
from Logger import Logger
# import rados


class ShallowBucketScrubber(BucketScrubber):
    def __init__(self, cluster, bucket_name, cache_tiering_enabled, bucket_data_pool, bucket_data_cache_pool, conf_file, num_threads, output_file_name, log_file, show_progress_bar=True):
        super(ShallowBucketScrubber, self).__init__(cluster, bucket_name, num_threads, output_file_name, log_file, show_progress_bar)
        self.bucket_marker = self.get_bucket_marker(bucket_name)
        self.setup_cluster(conf_file)

        self.cache_tiering_enabled = cache_tiering_enabled
        self.create_io_contexts(bucket_data_pool, bucket_data_cache_pool)
        self.logger = Logger(log_file)

    def setup_cluster(self, conf_file):
        try:
            self.rados_cluster = rados.Rados(conffile=conf_file)
            self.rados_cluster.connect()
        except Exception as ex:
            self.logger.info("Exception happened while setting up clusters:\n%s" % str(ex))
            raise ValueError("Scrub tools is not configured properly")

    def create_io_contexts(self, bucket_data_pool, bucket_data_cache_pool):
        try:
            self.ioctx_base_tier = self.rados_cluster.open_ioctx(bucket_data_pool)
            self.ioctx_cache_tier = self.cache_tiering_enabled and self.rados_cluster.open_ioctx(bucket_data_cache_pool)
        except Exception as ex:
            self.logger.info("Exception happened while creating contexts:\n%s" % str(ex))
            raise ValueError("Scrub tools is not configured properly")

    def get_bucket_marker(self, bucket_name):
        return self.cluster_util.get_bucket_marker(bucket_name)

    def check_cache_tier(self, rados_object_name):
        if self.cache_tiering_enabled is False:
            return True
        try:
            self.cluster_util.get_size_from_pool(self.ioctx_cache_tier, rados_object_name)
            return True
        except Exception as ex:
            return False

    def check_base_tier(self, rados_object_name):
        try:
            self.cluster_util.get_size_from_pool(self.ioctx_base_tier, rados_object_name)
            return True
        except Exception as ex:
            return False

    def scrub_object(self, object):
        rados_object_name = self.cluster_util.get_rados_object_name(self.bucket_marker, object.name)
        if self.check_cache_tier(rados_object_name) is False and self.check_base_tier(rados_object_name) is False:
            self.logger.info("Not able to retrieve object: %s" % object.name)
            FileUtil.write_file(self.file_obj, object.name)
        self.processed_objects[object.name] = True
        self.atomic_counter.increment()
        if self.show_progress_bar:
            self.progress_bar.update(self.atomic_counter.get_count())
