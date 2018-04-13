import logging
import paramiko
import json
import re
import sys
import time


class Util(object):

    @staticmethod
    def get_timestamp():
        return time.time()

    @staticmethod
    # To-do: Add more support for returning lapsed time in mins, hours
    def get_lapsed_time(start_time, end_time=None):
        if end_time == None:
            end_time = time.time()
        return end_time - start_time

    @staticmethod
    def trim_white_spaces(str):
        return re.sub(' +', ' ', str)

    @staticmethod
    def run_command(command):
        print "run_command"

    @staticmethod
    def get_remote_ssh_connection(host):
        ssh = paramiko.SSHClient()
        ssh.load_system_host_keys()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(host)
        return ssh

    @staticmethod
    def run_command_remote(command, host):
        ssh = Util.get_remote_ssh_connection(host)
        ssh_stdin, ssh_stdout, ssh_stderr = ssh.exec_command(command)
        ssh_stdin.close()
        output = ssh_stdout.read()
        return output

    @staticmethod
    def parse_json(json_raw):
        return json.loads(json_raw)

    @staticmethod
    def get_logger(file_name):
        formatter = logging.Formatter(fmt='%(asctime)s %(levelname)-8s %(message)s',
                                      datefmt='%Y-%m-%d %H:%M:%S')
        handler = logging.FileHandler(file_name)
        handler.setFormatter(formatter)
        screen_handler = logging.StreamHandler(stream=sys.stdout)
        screen_handler.setFormatter(formatter)
        logger = logging.getLogger("scrub")
        logger.setLevel(logging.INFO)
        logger.addHandler(handler)
        logger.addHandler(screen_handler)
        return logger