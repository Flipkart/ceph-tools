from FileUtil import FileUtil

class ConfigManager:

    def __init__(self, config_file):
        self.__configs = FileUtil.load_json(config_file)

    def get_all_configs(self):
        return self.__configs

    def get_config_value(self, key):
        if key in self.__configs:
            return self.__configs[key]
        return None