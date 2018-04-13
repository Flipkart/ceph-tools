from FileUtil import FileUtil
import re
from Util import Util

# To-do: Add examples of the input formats

BUCKET_ID_REGEX = r'--.*\\u'
OBJECT_NAME_REGEX = r'\\u.*__head'
OBJECT_SIZE_REGEX = r'.*--'
BUCKET_META_BUCKET_ID_REGEX = r'\.bucket\.meta.*:'
BUCKET_META_BUCKET_NAME_REGEX = r':.*'


class ParserUtil(object):

    @staticmethod
    # Extract size of the object from rados stat
    # Used by ShallowBucketScrubber
    def extract_size_from_stat(rados_stat):
        return int(rados_stat.split(',')[0][1:-1])

    @staticmethod
    # Used by PGScrubber
    def get_bucket_id_from_file_name(name):
        obj = re.search(BUCKET_ID_REGEX, name)
        bucket_id_raw = obj.group()
        # Remove '--' in the beginning and '\\u' in the end
        return bucket_id_raw[len('--'): bucket_id_raw.find('\\u')]

    @staticmethod
    # Used by PGScrubber
    def get_object_name_from_file_name(name):
        obj = re.search(OBJECT_NAME_REGEX, name)
        if obj == None:
            print "DOES NOT MATCH " + name
        object_name_raw = obj.group()
        # Remove '\\u' in the beginning and '__head' in the end
        return object_name_raw[len('\\u'): object_name_raw.find('__head')]

    @staticmethod
    # Used by PGScrubber
    def get_object_size_from_file_name(name):
        obj = re.search(OBJECT_SIZE_REGEX, name)
        object_size_raw = obj.group()
        # Remove '--' at the end
        return object_size_raw[: len('--') * -1]

    @staticmethod
    # Used by PGScrubber
    def get_bucket_name_from_bucket_meta(bucket_meta):
        obj = re.search(BUCKET_META_BUCKET_ID_REGEX, bucket_meta)
        bucket_id_raw = obj.group()
        # Remove .bucket.meta. in the beginning and : in the end
        return bucket_id_raw[len('.bucket.meta.'): len(':') * -1]

    @staticmethod
    # Used by PGScrubber
    def get_bucket_id_from_bucket_meta(bucket_meta):
        obj = re.search(BUCKET_META_BUCKET_NAME_REGEX, bucket_meta)
        bucket_name_raw = obj.group()
        # Remove : in the beginning
        return bucket_name_raw[len(':'):]

    @staticmethod
    def is_proper_line(line):
        return len(line.split(' ')) == 9

    @staticmethod
    def cleanse_filenames(filenames):
        filenames_cleansed = []
        for line in filenames:
            line_cleansed = Util.trim_white_spaces(line)
            if ParserUtil.is_proper_line(line_cleansed):
                filenames_cleansed.append(line_cleansed)
        return filenames_cleansed

    @staticmethod
    # Used by PGScrubber
    def extract_empty_files(filenames_file, output_file_name):
        content = FileUtil.get_file_content(filenames_file)
        out_file_obj = FileUtil.get_file_obj_for_write(output_file_name)

        content_cleansed = ParserUtil.cleanse_filenames(content)

        for line in content_cleansed:
            info = line.split(' ')
            size = int(info[4])
            if size == 0:
                file_name = info[8]
                FileUtil.write_file(out_file_obj, "%d--%s" % (size, file_name))

        FileUtil.close_file(out_file_obj)
