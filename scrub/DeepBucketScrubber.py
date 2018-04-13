from BucketScrubber import BucketScrubber
import Constants
from FileUtil import FileUtil
from Logger import Logger

class DeepBucketScrubber(BucketScrubber):

    def __init__(self, cluster, bucket_name, num_threads, output_file_name, log_file, show_progress_bar):
        super(DeepBucketScrubber, self).__init__(cluster, bucket_name, num_threads, output_file_name, log_file, show_progress_bar)
        self.logger = Logger(log_file)

    def scrub_object(self, object):
        # if self.atomic_counter.get_count() >= 10:
        #     raise ValueError("Hey raised custom exception")
        try:
            #self.logger.info(object.name)
            self.cluster_util.download_object(object, Constants.TEMP_FILE)
        except Exception as ex:
            self.logger.info("Not able to retrieve object: %s" % object.name)
            self.logger.info("Exception: %s" % str(ex))
            FileUtil.write_file(self.file_obj, object.name)
            pass
        finally:
            self.processed_objects[object.name] = True
            self.atomic_counter.increment()

            if self.show_progress_bar:
                self.progress_bar.update(self.atomic_counter.get_count())
