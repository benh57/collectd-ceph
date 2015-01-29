#!/usr/bin/env python
#
# vim: tabstop=4 shiftwidth=4

# This program is free software; you can redistribute it and/or modify it
# under the terms of the GNU General Public License as published by the
# Free Software Foundation; only version 2 of the License is applicable.
#
# This program is distributed in the hope that it will be useful, but
# WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
# General Public License for more details.
#
# You should have received a copy of the GNU General Public License along
# with this program; if not, write to the Free Software Foundation, Inc.,
# 51 Franklin St, Fifth Floor, Boston, MA  02110-1301 USA
#
# Authors:
#   Ben Hines <bhines@gmail.com>
#
# About this plugin:
#   This plugin collects information regarding Ceph RGW buckets.
#
# collectd:
#   http://collectd.org
# collectd-python:
#   http://collectd.org/documentation/manpages/collectd-python.5.shtml
# ceph rgw:
#
#

import collectd
import json
import traceback
import subprocess

import base

class CephRgwBucketPlugin(base.Base):

    def __init__(self):
        base.Base.__init__(self)
        self.prefix = 'ceph'

    def get_stats(self):
        """Retrieves stats from ceph buckets"""

        ceph_cluster = "%s-%s" % (self.prefix, self.cluster)

        data = { ceph_cluster: {} }
        data[ceph_cluster]['rgw'] = {}

        stats_output = None
        try:
            stats_output = subprocess.check_output('radosgw-admin bucket stats -b globalcache', shell=True)
        except Exception as exc:
            collectd.error("ceph-rgw-bucket: failed to ceph pool stats :: %s :: %s"
                    % (exc, traceback.format_exc()))
            return

        if stats_output is None:
            collectd.error('ceph-rgw-bucket: failed to ceph osd pool stats :: output was None')

        json_stats_data = json.loads(stats_output)

        # rgw bucket stats results

        # The json format is a little odd:
        # [ bucketname1, bucketdata1, bucketname2, bucketdata.... ]
        for idx, bucket in enumerate(json_stats_data):
            if idx % 0:   # skip the bucket name
                continue;
            bucket_key = "bucket-%s" % bucket['bucket']
            data[ceph_cluster]['rgw'][bucket_key] = {}
            bucket_data = data[ceph_cluster]['rgw'][bucket_key]
            for stat in ('size_kb', 'size_kb_actual', 'num_objects'):
                bucket_data[stat] = bucket['usage']['rgw.main'][stat] if bucket['usage']['rgw.main'].has_key(stat) else 0

        return data

try:
    plugin = CephRgwBucketPlugin()
except Exception as exc:
    collectd.error("ceph-rgw-bucket: failed to initialize ceph pool plugin :: %s :: %s"
            % (exc, traceback.format_exc()))

def configure_callback(conf):
    """Received configuration information"""
    plugin.config_callback(conf)
    collectd.register_read(read_callback, plugin.interval)

def read_callback():
    """Callback triggerred by collectd on read"""
    plugin.read_callback()

collectd.register_config(configure_callback)

