from ConfigManager import ConfigManager
import sys

if __name__ == '__main__':
    args = sys.argv[1:]

    conf_file = args[0]
    config_manager = ConfigManager(conf_file)

    