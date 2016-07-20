#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Map OSD(s) to host node(s)

import sys
import subprocess
import json


def main():
    if len(sys.argv) < 2:
        # without args, print something nice
        sys.exit(0)

    cmd = ['sudo', 'ceph', 'osd', 'tree', '--format=json']
    osd_tree_json = subprocess.check_output(cmd, stderr=subprocess.STDOUT)
    osd_tree = json.loads(osd_tree_json)
    osd_hosts = [node for node in osd_tree['nodes'] if node['type'] == 'host']
    osd_nodes = [node for node in osd_tree['nodes'] if node['type'] == 'osd']

    def find_osd(osd_name):
        for idx, node in enumerate(osd_nodes):
            if node['name'] == osd_name:
                return node
        return None

    def find_host(osd_id):
        for idx1, node in enumerate(osd_hosts):
            for idx2, child_id in enumerate(node['children']):
                if child_id == osd_id:
                    return node
        return None

    json_output = False
    osd_map = dict()
    skipped = list()

    def add_osd_to_map(host, osd):
        if host not in osd_map.keys():
            osd_map[host] = list()
        osd_map[host].append(osd)
        return None

    def print_map(json_output):
        if json_output:
            print json.dumps(osd_map)
            return
        for host in list(osd_map.keys()):
            print "Host %s has: %s" % \
                (host, ' '.join([osd for osd in osd_map[host]]))

    for arg in sys.argv[1:]:
        if arg == '--json' or arg == '-json':
            json_output = True
        else:
            osd_node = find_osd(arg)
            if not osd_node:
                skipped.append(arg)
                continue
            osd_host = find_host(osd_node['id'])
            if not osd_host:
                skipped.append(arg)
                continue
            # NOTE: foll string processing is FK / D42 specific ???
            host_name = osd_host['name']
            for suffix in ('-hdd', '-ssd'):
                if host_name.endswith(suffix):
                    host_name = host_name[:-len(suffix)]
            retval = add_osd_to_map(host_name, osd_node['name'])
            if not retval:
                skipped.append(arg)

    print_map(json_output)


if __name__ == '__main__':
    main()
