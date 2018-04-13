from itertools import count


class AtomicCounter(object):

    def __init__(self):
        self.__count = count(1)
        self.__cur_count = 1

    def increment(self):
        self.__cur_count = self.__count.next()

    def get_count(self):
        return self.__cur_count
