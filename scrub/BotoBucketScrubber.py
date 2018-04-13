from BucketScrubber import BucketScrubber
from FileUtil import FileUtil


class BotoBucketScrubber(BucketScrubber):
    def __init__(self, cluster_name, bucket_name, num_threads, output_file_name):
        super(BotoBucketScrubber, self).__init__(cluster_name, bucket_name, num_threads, output_file_name)

    def scrap_object(self, object):
        try:
            self.cluster_util.download_object(object, self.temp_file_name)
        except Exception as ex:
            print "Not able to retrieve object: %s" % object.name
            print "Exception: %s" % str(ex)
            FileUtil.write_file(self.file_obj, object.name)
            pass
