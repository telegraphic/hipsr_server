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
import hipsr_core.katcp_wrapper as config
import hipsr_core.katcp_wrapper as katcp_wrapper
import hipsr_core.katcp_helpers as katcp_helpers
import hipsr_core.config as config
import multiprocessing
import mpserver

import threading, Queue

class katcpThread(threading.Thread):
    """ Server to control ROACH boards"""
    def __init__(self, queue, queue_out, queue_plotter):
        threading.Thread.__init__(self)
        self.queue = queue
        self.queue_out = queue_out
        self.queue_plotter = queue_plotter
        self.server_enabled = True
    
    def toJson(self, npDict):
        """ Converts a dictionary of numpy arrays into a dictionary of lists."""
        for key in npDict.keys():
            for datakey in npDict[key]:
                try:
                    npDict[key][datakey] = npDict[key][datakey].tolist()
                except:
                    warn("katcpServer toJSON is acting strange...", RuntimeWarning)
        
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
            time.sleep(random.random()/10) # Spread out 
            data = getSpectrum(fpga, flavor)
            data["timestamp"] = timestamp
            hdfData = {'raw_data': { beam_id : data }}     
            plotData = squashSpectrum(data)
            
            self.queue_out.put(hdfData)
            
            msgdata = {beam_id : {
                         'xx' : plotData['xx'], 
                         'yy' : plotData['yy'], 
                         'timestamp': time.time()}
                       }
                   
            msg = self.toJson(msgdata)
            self.queue_plotter.append(msg)
            
            # Signal to queue task complete
            self.queue.task_done()
        except:
          raise

class KatcpServer(mpserver.MpServer):
    """ Server to control ROACH boards"""
    def __init__(self, printQueue, mainQueue, hdfQueue, katcpQueue, plotterQueue, flavor):
        self.name = 'katcp_server'
        
        self.printQueue       = printQueue
        self.plotterQueue     = plotterQueue
        self.hdfQueue         = hdfQueue
        self.katcpQueue       = katcpQueue
        self.flavor           = flavor
        
        # Internal threads
        self.threadQueue      = Queue.Queue()
        self.threadQueue_out  = Queue.Queue()
        self.threadQueue_plotter  = Queue.Queue()
        
        #self.fpgalist  = [katcp_wrapper.FpgaClient(roach, config.katcp_port, timeout=10) for roach in config.roachlist]
        
        import corr
        self.fpgalist  = [corr.katcp_wrapper.FpgaClient('192.168.0.49', 7147, timeout=10)]
        
        for roach in config.roachlist:
            self.mprint("%s %s"%(roach, config.katcp_port))
        
        for i in range(len(self.fpgalist)):
           t = katcpThread(self.threadQueue, self.threadQueue_out, self.threadQueue_plotter)
           t.setDaemon(True)
           t.start()
        
        super(KatcpServer, self).__init__(self.name, printQueue, mainQueue)
    
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

            
