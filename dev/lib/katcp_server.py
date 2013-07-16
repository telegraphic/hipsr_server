#! /usr/bin/env python
# encoding: utf-8
"""
katcp_server.py
===============

KATCP Server class for hipsr.
"""

import time, sys, os, socket, random, select, re

try:
    import ujson as json
    USES_UJSON = True
except:
    import json
    USES_UJSON = False

from   hipsr_core.katcp_helpers import stitch, snap, squashData, squashSpectrum, getSpectrum
import hipsr_core.config as config
import mpserver

class KatcpServer(mpserver.MpServer):
    """ Server to control ROACH boards"""
    def __init__(self, printQueue, mainQueue, katcpQueue, hdfQueue, plotterQueue, flavor):
        self.name = 'katcp_server'
        self.printQueue       = printQueue
        self.queue            = katcpQueue
        self.plotterQueue     = plotterQueue
        self.hdfQueue         = hdfQueue
        self.flavor           = flavor
        self.debug            = False
        self.timestamp        = time.time()
        super(KatcpServer, self).__init__(self.name, printQueue, mainQueue)
    
    def serverMain(self):
        """ Thread run method. Fetch data from roach"""

        while self.server_enabled:
            # Get input queue info (FPGA object)
            [fpga, timestamp]  = self.queue.get()
            #print "%s started: %s"%(self.getName(), fpga.host)
            beam_id = config.roachlist[fpga.host]

            # Grab data from the FPGA
            if fpga.is_connected():
                time.sleep(random.random()/10) # Spread out
                data = getSpectrum(fpga, self.flavor)
                #self.mprint(self.flavor)
                #self.mprint(data["xx"].shape)
                data["timestamp"] = self.timestamp
                hdfData = {'raw_data': { beam_id : data }}
                self.hdfQueue.put(hdfData)

                plotData = squashSpectrum(data)
                msgdata = {beam_id : {
                             'xx' : plotData['xx'],
                             'yy' : plotData['yy'],
                             'timestamp': time.time()}
                           }

                msg = self.toJsonDict(msgdata)
                self.plotterQueue.put(msg)

            # Signal to queue task complete
            self.queue.task_done()
