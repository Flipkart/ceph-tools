from argparse import ArgumentParser as ArgParser
from Singleton import Singleton


class ArgumentParser(object):
    __metaclass__ = Singleton

    def __init__(self):
        arg_parser = ArgParser()
        arg_parser.add_argument("config_file", help="Config file which has to be used for scrub operation")
        arg_parser.add_argument("-r", "--resume", help="Resumes the scrub operation", action="store_true")
        self.__args = arg_parser.parse_args()

    def get_args(self):
        return self.__args