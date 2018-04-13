from ClusterUtil import ClusterUtil
from FileUtil import FileUtil
from ParserUtil import ParserUtil

class PGScrubber:
    # To-do: Replace this method once a proper decoding method is found
    replace_dict = {
        '\\u': '_',
        '\\s': '/'
    }

    def __init__(self, cluster_name, file_names_in_pg, bucket_metas, output_file_good_objects, output_file_bad_objects, output_file_missed_buckets, output_file_missed_objects):
        self.cluster_name = cluster_name
        self.cluster_util = ClusterUtil(cluster_name)
        self.file_names_in_pg = file_names_in_pg
        self.bucket_metas = bucket_metas
        self.output_file_good_objects = output_file_good_objects
        self.output_file_bad_objects = output_file_bad_objects
        self.output_file_missed_buckets = output_file_missed_buckets
        self.output_file_missed_objects = output_file_missed_objects
        self.bucket_info = {}
        self.meta_info = {}

    # To-do: Replace this method once a proper decoding method is found
    def cleanse(self, str):
        # To-do: optimize this by reducing iterations
        for ch in self.replace_dict:
            str = str.replace(ch, self.replace_dict[ch])
        return str

    def build_bucket_info(self):
        print "Building bucket info"

        for file_name in self.file_names_in_pg:
            try:
                bucket_id = self.cleanse(ParserUtil.get_bucket_id_from_file_name(file_name))
                object_name = self.cleanse(ParserUtil.get_object_name_from_file_name(file_name))
                object_size = ParserUtil.get_object_size_from_file_name(file_name)

                if bucket_id not in self.bucket_info:
                    self.bucket_info[bucket_id] = []

                self.bucket_info[bucket_id].append({'name': object_name, 'size': object_size})

            except Exception as ex:
                # Skip if the line is not in correct format
                print "Skipping line %s" % file_name
                pass

    def build_meta_info(self):
        for bucket_meta in self.bucket_metas:
            try:
                bucket_id = ParserUtil.get_bucket_id_from_bucket_meta(bucket_meta)
                bucket_name = ParserUtil.get_bucket_name_from_bucket_meta(bucket_meta)
                self.meta_info[bucket_id] = bucket_name
            except Exception as ex:
                # Skip if the line is not in correct format
                print "Skipping line %s" % bucket_meta
                pass

    def scrub(self):
        good_obj_file_obj = FileUtil.get_file_obj_for_write(self.output_file_good_objects)
        bad_obj_file_obj = FileUtil.get_file_obj_for_write(self.output_file_bad_objects)
        missed_bucket_file_obj = FileUtil.get_file_obj_for_write(self.output_file_missed_buckets)
        missed_obj_file_obj = FileUtil.get_file_obj_for_write(self.output_file_missed_objects)

        for bucket_id in self.bucket_info:
            print "Processing Bucket id " + bucket_id

            if bucket_id not in self.meta_info:
                FileUtil.write_file(missed_bucket_file_obj, bucket_id)
                continue

            bucket_name = self.meta_info[bucket_id]

            for object in self.bucket_info[bucket_id]:
                object_name = object['name']
                object_size = object['size']

                object_info = self.cluster_util.get_object(bucket_name, object_name)

                if object_info is None:
                    FileUtil.write_file(missed_obj_file_obj, "%s %s" % (bucket_name, object_name))
                    continue

                owner_uid = self.cluster_util.get_object_owner_uid(bucket_name)

                if object_info.size == int(object_size):
                    FileUtil.write_file(good_obj_file_obj, "%s %s %s" % (owner_uid, bucket_name, object_name))
                else:
                    FileUtil.write_file(bad_obj_file_obj, "%s %s %s original_size %d actual_size %d" % (owner_uid, bucket_name, object_name, object_info.size, object_size))

        FileUtil.close_file(good_obj_file_obj)
        FileUtil.close_file(bad_obj_file_obj)
        FileUtil.close_file(missed_obj_file_obj)
        FileUtil.close_file(missed_bucket_file_obj)

    def run(self):
        print "Starting PG scrub"
        self.build_bucket_info()
        self.build_meta_info()
        self.scrub()
        print "Scrubbing complete"

