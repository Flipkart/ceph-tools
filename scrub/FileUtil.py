import hashlib
import json
import os

OUTPUT_LINE_SEPERATOR = '\n'

class FileUtil(object):

    @staticmethod
    def get_file_content(file_name):
        with open(file_name) as f:
            content = f.readlines()
            # f.close()
        content = [x.strip() for x in content]
        return content

    @staticmethod
    def get_file_content_raw(file_name):
        return FileUtil.get_file_obj_for_read(file_name).read()

    @staticmethod
    def load_json(file_name):
        return json.load(open(file_name))

    @staticmethod
    def get_file_obj_for_write(file_name):
        return open(file_name, "w")

    @staticmethod
    def get_file_obj_for_read(file_name):
        return open(file_name, "r")

    @staticmethod
    # To-do: Get an optional parameter for output_line_seperator
    def write_file(file_obj, content):
        file_obj.write(content + OUTPUT_LINE_SEPERATOR)

    @staticmethod
    def close_file(file_obj):
        file_obj.close()

    @staticmethod
    def print_file(file_loc):
        file_obj = FileUtil.get_file_obj_for_read(file_loc)
        content = file_obj.read()
        print content
        FileUtil.close_file(file_obj)

    @staticmethod
    def create_log_directory(directory):
        if not os.path.exists(directory):
            os.makedirs(directory)

    @staticmethod
    def compute_md5_hash(file_name):
        try:
            file_content = FileUtil.get_file_content_raw(file_name)
            checksum = hashlib.md5(file_content).hexdigest()
            return checksum
        except Exception, e:
            print "Exception occurred while calculating checksum :" + str(e)
