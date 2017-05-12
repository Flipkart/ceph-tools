import boto
import boto.s3.connection
from rgwadmin import RGWAdmin

# Users and Buckets from the primary server are created in the secondary server

rgw_sa = RGWAdmin(access_key=<access_key>, secret_key=<secret_key>,
               server=<primary_server_endpoint>, secure=False) #Add keys here of rgwadmin admin account and endpoint
rgw_samit = RGWAdmin(access_key=<access_key>, secret_key=<secret_key>,
               server=<secondary_server_endpoint>, secure=False) #Add keys here of rgwadmin admin account and endpoint

sa_users=rgw_sa.get_users()
samit_users=rgw_samit.get_users()
users={} #dict of the users with their access and secret keys
for user in sa_users:
    user_details=rgw_sa.get_user(user)
    users[user]={}
    users[user]['ak']=user_details['keys'][0]['access_key']
    users[user]['sk']=user_details['keys'][0]['secret_key']

    if user not in samit_users:
        rgw_samit.create_user(uid=user, display_name=user_details['display_name'],
                              email=user_details['email'],access_key=user_details['keys'][0]['access_key'],
                              secret_key=user_details['keys'][0]['secret_key'], generate_key=False)
	    print "User created in SAmit", user

count=1
for user in users.keys():
    access_key=users[user]['ak']
    secret_key=users[user]['sk']
    print count,user
    count+=1
    conn_sa = boto.connect_s3(
            aws_access_key_id = access_key,
            aws_secret_access_key = secret_key,
            host = <primary_server_endpoint>, # SA ELB endpoint
            port = 80,
            is_secure=False,
            calling_format = boto.s3.connection.OrdinaryCallingFormat(),)
    conn_samit = boto.connect_s3(
            aws_access_key_id = access_key,
            aws_secret_access_key = secret_key,
            host = <secondary_server_endpoint>, # SAMIT ELB endpoint
            port = 80,
            is_secure=False,
            calling_format = boto.s3.connection.OrdinaryCallingFormat(),)
    try:
        sa_buckets=conn_sa.get_all_buckets()
        samit_buckets=conn_samit.get_all_buckets()
        #getting the buckets which are in primary but not in secondary
		new_buckets=list(set([str(bucket.name) for bucket in sa_buckets])-set([str(bucket.name) for bucket in samit_buckets]))
        for bucket in new_buckets:
            b=conn_sa.get_bucket(bucket)
            b_new=conn_samit.create_bucket(bucket)
            b_new.set_acl(b.get_acl())
            print user,'creating new bucket', bucket
    except:
        print '#####CHECK for the user', user
