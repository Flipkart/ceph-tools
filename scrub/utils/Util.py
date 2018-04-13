from utils.FileUtil import FileUtil
import hashlib
from common.Logger import Logger
import paramiko
import pickle
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
    def get_logger(log_file):
        return Logger(log_file)

    @staticmethod
    def dump_object_to_file(object, file_name):
        file_obj = FileUtil.get_file_obj_for_write(file_name)
        pickle.dump(object, file_obj)

    @staticmethod
    def get_object_from_file(file_name):
        file_obj = FileUtil.get_file_obj_for_read(file_name)
        return pickle.load(file_obj)

