import json
import subprocess
from ast import literal_eval
import boto
import boto.s3.connection
import re
import datetime


def get_conn(user):
    info=subprocess.check_output("radosgw-admin user info --uid="+str(user), shell=True)
    access_key=re.findall("\"access_key\": \"(.+?)\"", info)[0]
    secret_key=re.findall("\"secret_key\": \"(.+?)\"", info)[0].replace('\\','')
    conn = boto.connect_s3(
            aws_access_key_id = access_key,
            aws_secret_access_key = secret_key,
            host = '10.47.2.33', # refer go/d42-user-guide for endpoint
            port = 80,
            is_secure=False,
            calling_format = boto.s3.connection.OrdinaryCallingFormat(),)
    return conn

def get_prefixes(conn):
    prefixes=set()
    for bucket in conn.get_all_buckets():
        #print bucket.name
        prefix=re.findall("(?P<prefix>\d+__mysql)__\d{4}-\d{2}-\d{2}__\d{2}-\d{2}-\d{2}__\w{4}",bucket.name)
        if len(prefix)>0:
            prefixes.add(prefix[0])
    return list(prefixes)

def x(cmd):
    '''
    Execute the shell command CMD.
    '''
    p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    (stdout, stderr) = p.communicate()
    (stdout, stderr, p.pid)
    print "Return is %s " % (p.returncode)
    if p.returncode == 0:
        return 0
    else:
        return 1
        
def del_older_buckets(user):
    rpo_names=[]
    conn=get_conn(user)
    prefixes=get_prefixes(conn)
    for prefix in prefixes:
        meta=prefix+'--meta'
        meta_file=conn.get_bucket(meta).get_key('catalog')
        for rpo in json.loads(meta_file.get_contents_as_string())['rpo_list']:
            rpo_names.append(str(rpo['backup_name']))

    print 'rpo names of user', rpo_names
    for b in conn.get_all_buckets():
        if (b.name in rpo_names) or ('meta' in b.name):
            continue
        print '\nBucket: ' + b.name,
        # Check for MPU data and calculate the total storage used
        if len(re.findall("\d+__mysql__\d{4}-\d{2}-\d{2}__\d{2}-\d{2}-\d{2}__\w{4}", b.name))==1:
            total_size = 0
            for mpu in b.get_all_multipart_uploads():
                ptotalsize = 0
                for p in mpu.get_all_parts():
                    ptotalsize += p.size
                total_size += ptotalsize

            if total_size > 0:
                for mpu in b.get_all_multipart_uploads():
                    mpu.cancel_upload()
                print 'MPU data deleted!',
            else:
                print 'No MPU',
            print "deleting bucket",
            x("radosgw-admin bucket rm --bucket="+b.name+" --purge-objects")
            print "bucket deleted"
            
out1=subprocess.check_output('radosgw-admin metadata list user', shell=True)
#print out1
out2= literal_eval(out1)
count=1
for i in out2:
    if 'mysqlbackup' in i:
        out3=subprocess.check_output('radosgw-admin bucket list --uid='+i+' | wc -l', shell=True)
        if int(out3)>15:
            print count,i, out3,
            count+=1
            del_older_buckets(i)
