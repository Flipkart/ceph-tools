import rados
try:
	cluster = rados.Rados(conffile='/etc/rados_test/prod-d42sa/ceph.conf')
	cluster.connect()
	print "\nCluster ID: " + cluster.get_fsid()
except Exception as ex:
	print str(ex)
