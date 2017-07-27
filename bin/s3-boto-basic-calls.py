#!/usr/bin/env python
# -*- coding: utf-8 -*-

# http://boto.cloudhackers.com/en/latest/index.html
# http://boto.cloudhackers.com/en/latest/getting_started.html
# http://boto.cloudhackers.com/en/latest/ref/s3.html
# http://boto.cloudhackers.com/en/latest/s3_tut.html
# https://github.com/boto/boto
# https://github.com/mumrah/s3-multipart
# https://aws.amazon.com/documentation/s3/

import os
import sys
import time
import datetime
import math

import boto
import boto.s3 as s3
import boto.s3.connection
from filechunkio import FileChunkIO

# ceph dev VM
endpoint = 'localhost'
port = 8000
access_key = '0555b35654ad1656d804'
secret_key = 'h7GhxuBLTrlhVUyxSPUKUV8r/2EI4ngqJxD7iBdBYLhwluN30JaT3Q=='

KEY_STRING = 'xXx-string'
VAL_STRING = 'botox'
KEY_FILE = 'xXx-file'
MULTI_PART_SIZE = (2 * 1024 * 1024)
RESP_CHUNK_SIZE = (1 * 1024 * 1024)

global bucket, key
bucket = None
key = None


def connect():
    global conn
    conn = boto.connect_s3(
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        host=endpoint,
        port=port,
        is_secure=False,
        calling_format=s3.connection.OrdinaryCallingFormat())
    return


def create():
    # global, to be used for file upload & download
    now = datetime.datetime.now().strftime("%Y-%m-%d__%H-%M-%S")
    bucket = conn.create_bucket(now)
    # bucket = conn.create_bucket('xxx-bkt-%s' % int(time.time()))
    key = bucket.new_key(KEY_STRING)
    key.set_contents_from_string(VAL_STRING)
    print key.get_contents_as_string()
    return


def _delete_bucket(bucket):
    print "Deleting bucket: %s" % (bucket.name)
    # mpu_list = bucket.get_all_multipart_uploads()
    mpu_list = bucket.list_multipart_uploads()
    # print mpu_list, vars(mpu_list)
    for mpu in mpu_list:
        print mpu
        mpu.cancel_upload()
    for key in bucket.list():
        key.delete()
    bucket.delete()
    return


def delete():
    if len(sys.argv) < 3 and sys.argv[1] != '--full':
        bucket_name = '--all'
    else:
        bucket_name = sys.argv[2]
    for bucket in conn.get_all_buckets():
        if bucket_name == bucket.name or '--all' == bucket_name:
            _delete_bucket(bucket)
    return


def list_keys(bucket):
    print "Keys: "
    for key in bucket.list():
        print key.name, key.size
    print
    print "Bucket MPUs:"
    # mpu_list = bucket.get_all_multipart_uploads()
    mpu_list = bucket.list_multipart_uploads()
    # print mpu_list, vars(mpu_list)
    for mpu in mpu_list:
        print mpu
    return


def process_bucket(bucket):
    list_keys(bucket)
    print
    print "Incomplete MPUs:"
    bucket_size = 0
    mpu_list = bucket.list_multipart_uploads()
    # print mpu_list, vars(mpu_list)
    for mpu in mpu_list:
        parts = mpu.get_all_parts()
        if not parts:
            continue
        print '# Incomplete multi-part upload -- bucket: %s, key: %s, parts: %d' % (
            mpu.bucket_name, mpu.key_name, len(parts))
        for part in parts:
            print 'Part # %d -- timestamp: %s -- size: %d' % (
                part.part_number, part.last_modified, part.size)
            bucket_size += part.size
    return bucket_size


def list_buckets():
    for bucket in conn.get_all_buckets():
        print '{name}\t{created}'.format(
            name=bucket.name, created=bucket.creation_date)
    return


def list_all(bucket_list):
    if isinstance(bucket_list, list):
        for bucket in bucket_list:
            print '********************************************************************************'
            print '{name}\t{created}'.format(
                name=bucket.name, created=bucket.creation_date)
            print '********************************************************************************'
            process_bucket(bucket)
            print
    elif isinstance(bucket_list, boto.s3.bucket.Bucket):
        bucket = bucket_list
        print '********************************************************************************'
        print '{name}\t{created}'.format(name=bucket.name, created="")
        # created=bucket.creation_date)
        print '********************************************************************************'
        process_bucket(bucket)
    return


def __file_upload():
    filename = sys.argv[2]
    file_size = os.path.getsize(filename)
    print filename, file_size
    if file_size < MULTI_PART_SIZE:
        key = bucket.new_key(KEY_FILE)
        key.set_contents_from_filename(filename)
    else:
        # large file upload
        key = KEY_FILE
        mpu = bucket.initiate_multipart_upload(key)
        part_count = int(math.ceil(file_size / float(MULTI_PART_SIZE)))
        for part_num in range(part_count):
            part_offset = MULTI_PART_SIZE * part_num
            part_size = min(MULTI_PART_SIZE, file_size - part_offset)
            with FileChunkIO(
                    filename, 'r', offset=part_offset, bytes=part_size) as fp:
                print str(part_num).rjust(3), \
                    str(part_offset).rjust(10), \
                    str(part_size).rjust(10)
                mpu.upload_part_from_file(fp, part_num=part_num + 1)
        mpu.complete_upload()
    bucket.get_all_multipart_uploads()
    return


def file_upload():
    # global, to be used for download
    global filename, file_size
    filename = sys.argv[2]
    file_size = os.path.getsize(filename)
    print filename, file_size
    # large file upload
    key = KEY_FILE
    mpu = bucket.initiate_multipart_upload(key)
    part_count = int(math.ceil(file_size / float(MULTI_PART_SIZE)))
    for part_num in range(part_count):
        part_offset = part_num * MULTI_PART_SIZE
        part_size = min(MULTI_PART_SIZE, file_size - part_offset)
        print str(part_num).rjust(3), \
            str(part_offset).rjust(10), \
            str(part_size).rjust(10)
        with FileChunkIO(
                filename, 'r', offset=part_offset, bytes=part_size) as fp:
            mpu.upload_part_from_file(fp, part_num=part_num + 1)
    mpu.complete_upload()
    bucket.get_all_multipart_uploads()
    return


# https://github.com/mumrah/s3-multipart
def file_download(bucket=globals()['bucket'], key=globals()['key']):
    print bucket, key
    resp = conn.make_request('HEAD', bucket=bucket, key=key)
    print resp.getheaders()
    val_size = int(resp.getheader('content-length'))
    part_count = int(math.ceil(val_size / float(MULTI_PART_SIZE)))
    with open(KEY_FILE, 'wb') as dest_fp:
        for part_num in range(part_count):
            part_offset = part_num * MULTI_PART_SIZE
            part_size = min(MULTI_PART_SIZE, val_size - part_offset)
            start = part_offset
            end = start + part_size - 1
            print str(part_num).rjust(3), \
                str(part_offset).rjust(10), \
                str(part_size).rjust(10), \
                str(start).rjust(10), \
                str(end).rjust(10)

            resp = conn.make_request(
                "GET",
                bucket=bucket,
                key=key,
                headers={'Range': "bytes=%d-%d" % (start, end)})
            chunk_count = int(math.ceil(part_size / float(RESP_CHUNK_SIZE)))
            for chunk_num in range(chunk_count):
                chunk_offset = part_offset + (chunk_num * RESP_CHUNK_SIZE)
                chunk_size = min(RESP_CHUNK_SIZE, val_size - chunk_offset)
                print str(chunk_num).rjust(3), \
                    str(chunk_offset).rjust(10), \
                    str(chunk_size).rjust(10)
                buf = resp.read(chunk_size)
                dest_fp.write(buf)
                pass
    return


def main():
    full_mode = False
    create_mode = False
    del_mode = False
    file_push_mode = False
    file_pull_mode = False
    list_mode = False
    if len(sys.argv) > 1:
        if sys.argv[1] == '--full':
            full_mode = True
        elif sys.argv[1] == '--create':
            create_mode = True
        elif sys.argv[1] == '--del':
            if len(sys.argv) < 3:
                print "Usage: " + sys.argv[0] + " --del [ --all | <bucket> ]"
                return
            del_mode = True
        elif sys.argv[1] == '--push':
            if len(sys.argv) < 3:
                print "Usage: " + sys.argv[0] + " --push /path/to/file"
                return
            file_push_mode = True
        elif sys.argv[1] == '--pull':
            if len(sys.argv) < 4:
                print "Usage: " + sys.argv[0] + " --pull <bucket> <key>"
                return
            file_pull_mode = True
        elif sys.argv[1] == '--list':
            if len(sys.argv) < 3:
                print "Usage: " + sys.argv[0] + " --list [ --all | <bucket> ]"
                return
            list_mode = True

    # boto.set_stream_logger('xXx')
    connect()

    if full_mode is True:
        create()
        list_all(conn.get_all_buckets())
        delete()
        list_all(conn.get_all_buckets())

    elif create_mode is True:
        create()
        list_buckets()

    elif del_mode is True:
        delete()
        list_buckets()

    elif file_push_mode is True:
        create()
        file_upload()
        list_buckets()
        file_download()

    elif file_pull_mode is True:
        file_download(bucket=sys.argv[2], key=sys.argv[3])

    elif list_mode is True:
        if '--all' == sys.argv[2]:
            list_all(conn.get_all_buckets())
        else:
            list_all(conn.lookup(sys.argv[2]))

    else:
        list_buckets()

    return


if __name__ == '__main__':
    main()
