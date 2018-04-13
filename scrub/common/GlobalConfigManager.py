from ConfigParser import ConfigParser


class GlobalConfigManager(object):
    def __init__(self, config_file):
        self.__parser = ConfigParser()
        self.__parser.read(config_file)

    def get_config(self, section_name, config_key):
        return self.__parser.get(section_name, config_key)
