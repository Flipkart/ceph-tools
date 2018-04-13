import Constants
import logging
import sys


class Logger(object):
    def __init__(self, log_file):
        self.__logger = self.__prepare_logger(log_file)

    def __prepare_logger(self, log_file):
        formatter = logging.Formatter(fmt=Constants.LOGGER_FORMAT,
                                      datefmt=Constants.LOGGER_DATE_FORMAT)
        handler = logging.FileHandler(log_file)
        handler.setFormatter(formatter)
        screen_handler = logging.StreamHandler(stream=sys.stdout)
        screen_handler.setFormatter(formatter)
        logger = logging.getLogger(Constants.LOGGER_APP_ID)
        logger.setLevel(logging.INFO)
        logger.addHandler(handler)
        logger.addHandler(screen_handler)
        return logger

    def info(self, log_text):
        self.__logger.info(log_text)
