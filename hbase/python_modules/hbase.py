#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# HBase gmond module for Ganglia
#
# Copyright (C) 2011 by Michael T. Conigliaro <mike [at] conigliaro [dot] org>.
# All rights reserved.
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
# THE SOFTWARE.
#

import sys
import os
import threading
import urllib
import time
import json
import traceback

descriptors = list()
Desc_Skel   = {}
_Lock = threading.Lock() # synchronization lock


class UpdateMetricThread(threading.Thread):

    def __init__(self, params):
        threading.Thread.__init__(self)
        self.running      = False
        self.shuttingdown = False
        self.refresh_rate = 10
        self.metric       = {}
        self.metric["hbase_total_request_per_second"] = 0;
        self.metric["last_hbase_total_requests"] = 0;
        self.metric["last_time"] = time.time()
        self.metric["cur_time"] = time.time()

    def shutdown(self):
        self.shuttingdown = True
        if not self.running:
            return
        self.join()

    def run(self):
        self.running = True

        while not self.shuttingdown:
            _Lock.acquire()
            self.update_metric()
            _Lock.release()
            time.sleep(self.refresh_rate)

        self.running = False

    def getjmx(self,host,port,qry):
        url = 'http://'+host+':'+port+'/jmx?qry='+qry
        jmx = urllib.urlopen(url)
        json_out = json.loads(jmx.read().replace('\n',''))
        jmx.close()
        return json_out

    def update_metric(self):
        try:
            region_server_json= self.getjmx('localhost', '60030', 'Hadoop:service=HBase,name=RegionServer,sub=Server')

            self.metric["cur_hbase_total_requests"] = region_server_json['beans'][0]['totalRequestCount']
            self.metric["cur_time"] = time.time()
            self.metric["hbase_total_request_per_second"] = \
                    (self.metric["cur_hbase_total_requests"] - self.metric["last_hbase_total_requests"]) / \
                    (self.metric["cur_time"] - self.metric["last_time"])

            # adjust the last values
            self.metric["last_hbase_total_requests"] = self.metric["cur_hbase_total_requests"]
            self.metric["last_time"] = self.metric["cur_time"]

        except Exception, e:
            print str(e)

    def metric_of(self, name):
        val = 0
        if name in self.metric:
            _Lock.acquire()
            val = self.metric[name]
            _Lock.release()
        return val

_Worker_Thread = UpdateMetricThread({})

def create_desc(skel, prop):
    d = skel.copy()
    for k,v in prop.iteritems():
        d[k] = v
    return d

def metric_of(name):
    return int(_Worker_Thread.metric_of(name))

def metric_cleanup():
    _Worker_Thread.shutdown()

def metric_init(lparams):
    """Initialize metric descriptors"""

    _Worker_Thread.update_metric()
    _Worker_Thread.start()

    # initialize skeleton of descriptors
    Desc_Skel = {
        "name"        : "XXX",
        "call_back"   : metric_of,
        "time_max"    : 60,
        "value_type"  : "uint",
        "units"       : "XXX",
        "slope"       : "XXX", # zero|positive|negative|both
        "format"      : "%d",
        "description" : "XXX",
        "groups"      : "hbase",
        }

    query_skel = create_desc(Desc_Skel, {
        "name"       : "XXX",
        "units"      : "request/sec",
        "slope"      : "both",
        "format"     : "%d",
        "description": "XXX",
        })

    descriptors.append(create_desc(query_skel, {
        "name"       : "hbase_total_request_per_second",
        "description": "total qps", }));

    return descriptors

# the following code is for debugging and testing
if __name__ == '__main__':
    try:
        descriptors = metric_init({})
        while True:
            for d in descriptors:
                v = d['call_back'](d['name'])
                print ('value for %s is '+d['format']) % (d['name'],  v)
            print
            time.sleep(5)
    except KeyboardInterrupt, e:
        print str(e)
        time.sleep(0.2)
        os._exit(1)
    except StandardError, e:
        print str(e)
        os._exit(1)
