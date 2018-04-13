class Cluster(object):
    def __init__(self, access_key, secret_key, host):
        self.admin_access_key = access_key
        self.admin_secret_key = secret_key
        self.host = host

