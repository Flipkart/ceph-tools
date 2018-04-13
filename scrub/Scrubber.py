from abc import ABCMeta, abstractmethod


class Scrubber():
    __metaclass__ = ABCMeta
    #
    # @abstractmethod
    # def __init__(self, cluster, bucket_name, num_threads, output_file_name, log_file, show_progress_bar=True):
    #     pass

    @abstractmethod
    def has_completed(self):
        pass

    @abstractmethod
    def get_snapshot(self):
        pass

    @abstractmethod
    def run(self):
        pass

    @abstractmethod
    def resume(self):
        pass
