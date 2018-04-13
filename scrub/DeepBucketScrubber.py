from BucketScrubber import BucketScrubber
import Constants
from FileUtil import FileUtil
from Logger import Logger

class DeepBucketScrubber(BucketScrubber):

    def __init__(self, cluster, bucket_name, num_threads, output_file_name, log_file, show_progress_bar):
        super(DeepBucketScrubber, self).__init__(cluster, bucket_name, num_threads, output_file_name, log_file, show_progress_bar)
        self.logger = Logger(log_file)

    def __write_object(self, object_name):
        FileUtil.write_file(self.file_obj, object_name)

    def scrub_object(self, object):
        try:
            self.cluster_util.download_object(object, Constants.TEMP_FILE)
            md5sum = FileUtil.compute_md5_hash(Constants.TEMP_FILE)
            md5sum_in_meta = object.etag.strip('"').strip("'")
            if md5sum != md5sum_in_meta:
                self.logger.info("MD5 hashes are different: %s" % object.name)
                self.__write_object(object.name)
        except Exception as ex:
            self.logger.info("Not able to retrieve object: %s" % object.name)
            self.logger.info("Exception: %s" % str(ex))
            self.__write_object(object.name)
            pass
        finally:
            self.processed_objects[object.name] = True
            self.atomic_counter.increment()
            if self.show_progress_bar:
                self.progress_bar.update(self.atomic_counter.get_count())
