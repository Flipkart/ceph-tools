from BucketScrubber import BucketScrubber
from FileUtil import FileUtil
from Util import Util


class BotoBucketScrubber(BucketScrubber):

    def __init__(self, cluster_name, bucket_name, num_threads, output_file_name):
        super(BotoBucketScrubber, self).__init__(cluster_name, bucket_name, num_threads, output_file_name)
        self.logger = Util.get_logger("logs/%s.log" % __name__)

    def scrap_object(self, object):
        try:
            #self.logger.info(object.name)
            self.cluster_util.download_object(object, self.temp_file_name)
        except Exception as ex:
            print "Not able to retrieve object: %s" % object.name
            print "Exception: %s" % str(ex)
            FileUtil.write_file(self.file_obj, object.name)
            pass
        finally:
            self.atomic_counter.increment()
            self.progress_bar.update(self.atomic_counter.get_count())

