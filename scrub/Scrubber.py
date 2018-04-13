from abc import ABCMeta, abstractmethod


class Scrubber():
    __metaclass__ = ABCMeta

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
