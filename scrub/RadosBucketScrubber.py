from BucketScrubber import BucketScrubber
from FileUtil import FileUtil
import rados
from Util import Util


class RadosBucketScrubber(BucketScrubber):

    #Rados object name: BucketMarker_ObjectName
    rados_object_name_format = '%s_%s'

    def __init__(self, cluster_name, bucket_name, base_tier_pool, cache_tier_pool, conf_file, num_threads, output_file_name):
        super(RadosBucketScrubber, self).__init__(cluster_name, bucket_name, num_threads, output_file_name)
        self.bucket_marker = self.get_bucket_marker(bucket_name)
        self.setup_cluster(conf_file)
        self.create_io_contexts(base_tier_pool, cache_tier_pool)
        self.logger = Util.get_logger(__name__)

    def setup_cluster(self, conf_file):
        try:
            self.rados_cluster = rados.Rados(conffile=conf_file)
            self.rados_cluster.connect()
        except Exception as ex:
            print "Exception happened while setting up clusters:\n%s" % str(ex)

    def create_io_contexts(self, base_tier_pool, cache_tier_pool):
        try:
            self.ioctx_base_tier = self.rados_cluster.open_ioctx(base_tier_pool)
            self.ioctx_cache_tier = self.rados_cluster.open_ioctx(cache_tier_pool)
        except Exception as ex:
            print "Exception happened while creating contexts:\n%s" % str(ex)

    def get_bucket_marker(self, bucket_name):
        return self.cluster_util.get_bucket_marker(bucket_name)

    def scrap_object(self, object):
        try:
            rados_object_name = self.rados_object_name_format % (self.bucket_marker, object.name)
            size_in_cache_tier = self.cluster_util.get_size_from_pool(self.ioctx_cache_tier, rados_object_name)
            #print "Cache size " + str(size_in_cache_tier)
            if size_in_cache_tier == 0:
                print "Not found in cache " + rados_object_name
                print "Checking in data pool.."
                size_in_base_tier = self.cluster_util.get_size_from_pool(self.ioctx_base_tier, rados_object_name)

                if size_in_base_tier == 0:
                    print "Not found in both pools.. " + rados_object_name
                else:
                    print "Found in base tier " + rados_object_name
        except Exception as ex:
            print "Not able to retrieve object: %s" % object.name
            print "Exception: %s" % str(ex)
            FileUtil.write_file(self.file_obj, object.name)
            pass
