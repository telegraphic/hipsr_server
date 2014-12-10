#! /usr/bin/env python
# encoding: utf-8
"""
katcp_server.py
===============

KATCP Server class for hipsr.
"""

import time, sys, os, signal, random
from datetime import datetime
from optparse import OptionParser
import multiprocessing

import numpy as np
import threading, Queue

# Imports from hipsr_core
import hipsr_core.katcp_wrapper as katcp_wrapper
import hipsr_core.katcp_helpers as katcp_helpers
from   hipsr_core.katcp_helpers import squashData, squashSpectrum, getSpectrum
import hipsr_core.config as config

try:
    import ujson as json
    USES_UJSON = True
except:
    import json
    USES_UJSON = False

class KatcpThread(threading.Thread):
    """ Server to control ROACH boards"""
    def __init__(self, queue, queue_out, queue_plotter):
        threading.Thread.__init__(self)
        self.queue          = queue
        self.queue_out      = queue_out
        self.queue_plotter  = queue_plotter
        self.server_enabled = True

    def toJson(self, npDict):
        """ Converts a dictionary of numpy arrays into a dictionary of lists."""
        for key in npDict.keys():
            for datakey in npDict[key]:
                try:
                    npDict[key][datakey] = npDict[key][datakey].tolist()
                except AttributeError:
                    pass
                    #print npDict
                    #mprint("katcpServer toJSON is acting strange...")
                    #raise

        if USES_UJSON:
            return json.dumps(npDict, double_precision=3)
        else:
            return json.dumps(npDict)

    def run(self):
        """ Thread run method. Fetch data from roach"""
        try:
          while self.server_enabled:
            # Get input queue info (FPGA object)
            [fpga, flavor] = self.queue.get()
            beam_id = config.roachlist[fpga.host]

            # Grab data from the FPGA
            time.sleep(float(beam_id.split("_")[1]) / 26)         # Spread out
            data = getSpectrum(fpga, flavor)
            #data["timestamp"] = self.timestamp
            hdfData = {'raw_data': { beam_id : data }}
            plotData = squashSpectrum(data)

            self.queue_out.put(hdfData)

            msgdata = {beam_id: {
                           'xx': plotData['xx'],
                           'yy': plotData['yy'],
                           'timestamp': time.time()}
                       }

            msg = self.toJson(msgdata)
            self.queue_plotter.put(msg)

            # Signal to queue task complete
            self.queue.task_done()
        except RuntimeError:
            raise RuntimeError("FPGA %s (%s) is not responding or has crashed" % (fpga.host, beam_id))


class KatcpServer(threading.Thread):
    """ Server to control ROACH boards"""
    def __init__(self, printQueue, mainQueue, hdfQueue, katcpQueue, plotterQueue, flavor, dummyMode=False):
        threading.Thread.__init__(self)
        self.name = 'katcp_server'

        if dummyMode:
            import lib.dummy_katcp_wrapper as katcp_wrapper
        else:
            import hipsr_core.katcp_wrapper as katcp_wrapper

        self.printQueue       = printQueue
        self.mainQueue        = mainQueue
        self.plotterQueue     = plotterQueue
        self.hdfQueue         = hdfQueue
        self.katcpQueue       = katcpQueue
        self.flavor           = flavor
        self.server_enabled   = True

        # Internal threads
        self.threadQueue          = Queue.Queue()
        self.threadQueue_out      = Queue.Queue()
        self.threadQueue_plotter  = Queue.Queue()

        self.fpgalist  = [katcp_wrapper.FpgaClient(roach, config.katcp_port, timeout=10) for roach in config.roachlist]

        for roach in config.roachlist:
            self.mprint("%s %s"%(roach, config.katcp_port))

        for i in range(len(self.fpgalist)):
           t = KatcpThread(self.threadQueue, self.threadQueue_out, self.threadQueue_plotter)
           t.setDaemon(True)
           t.start()

        #super(KatcpServer, self).__init__(self.name, printQueue, mainQueue)

    def mprint(self, msg):
        """ Send a message to the multiprocessing print queue """
        self.printQueue.put(msg)

    def triggerDataCapture(self):
        """ Starts multiple KATCP servers to collect data from ROACH boards

        Spawns multiple threads, with each thread retrieving from a single board.
        A queue is used to block until all threads have completed.
        """
        # Run threads using queue
        for fpga in self.fpgalist:
            if fpga.is_connected():
                self.threadQueue.put([fpga, self.flavor])
            else:
                self.mprint("Warning: %s not connected"%fpga.host)

        # Make sure all threads have completed
        self.threadQueue.join()

    def safeExit(self):
        """ Attempt to close safely. """
        self.mprint("katcp_server: Closing FPGA connections")
        for fpga in self.fpgalist:
            fpga.stop()
        self.mprint("katcp_server: FPGA connections closed.")
        self.server_enabled = False

    def serverMain(self):
        """ Thread run method. Fetch data from roach"""
        # Start servers threads up
        while self.server_enabled:
            # Get input queue info (FPGA object)
            #self.mprint("HERE2!")
            msg = self.katcpQueue.get()
            #self.mprint("HERE3!")
            for key in msg.keys():
                if key == 'check_acc':
                    if self.fpgalist[0].is_connected():
                        acc_new = self.fpgalist[0].read_int('o_acc_cnt')
                        self.mainQueue.put({'acc_new': acc_new})
                    else:
                        self.mprint("katcp_server: warning: %s is not connected."%self.fpgalist[0].host)
                        self.mainQueue.put({'acc_new' : msg[key]})

                if key == 'new_acc':
                    #self.mprint("HERE!")
                    self.triggerDataCapture()
                    while not self.threadQueue_out.empty():
                        self.hdfQueue.put(self.threadQueue_out.get())
                    while not self.threadQueue_plotter.empty():
                        self.plotterQueue.put(self.threadQueue_plotter.get())
                if key == 'timestamp':
                    self.timestamp = msg['timestamp']
    def run(self):
        try:
            self.serverMain()
            return 0

        except KeyboardInterrupt:
             self.mprint("%s: Keyboard interrupt."%self.name)
             self.safeExit()

        except:
            #self.mainQueue.put({'crash' : self.name})
            self.safeExit()
            raise
