#! /usr/bin/env python
# encoding: utf-8
"""
mpserver.py
===========

A multiprocessing process-based server class, upon which all the different servers
used in hipsr-server.py are based upon. This class sets up the queues with which
processes communicate with each other.

"""

import time, sys, os, socket, random, select, re
import multiprocessing
import numpy as np

try:
    import ujson as json
    USES_UJSON = True
except:
    import json
    USES_UJSON = False


class MpServer(multiprocessing.Process):
    """ UDP data server for hipsr-gui plotter """
    def __init__(self, name, printQueue, mainQueue):
        multiprocessing.Process.__init__(self)
        
        self.name           = name
        self.printQueue     = printQueue
        self.mainQueue      = mainQueue
        self.server_enabled = True

    def mprint(self, msg):
        """ Send a message to the multiprocessing print queue """
        self.printQueue.put(msg)
    
    def toJsonCmd(self, cmd, val):
        """ Converts a command value pair into a JSON encoded python dictionary."""
        return json.dumps({cmd : val})

    def toJsonDict(self, npDict):
        """ Converts a dictionary of numpy arrays into a JSON encoded dictionary of lists."""
        for key in npDict.keys():
            for datakey in npDict[key]:
                try:
                    if type(npDict[key][datakey]) == type(np.array([0])):
                        npDict[key][datakey] = npDict[key][datakey].tolist()
                except:
                    print key, datakey, npDict[key][datakey]
                    self.mprint("%s: WARNING: toJsonDict is acting strange"%self.name)
                    #raise
        
        if USES_UJSON:            
            return json.dumps(npDict, double_precision=3)
        else:
            return json.dumps(npDict)
    
    def safeExit(self):
        """ Attempt to die with more dignity. """
        self.server_enabled = False

    def serverMain(self):
        """ Main server process. Should be overwritten by server instance. """
        while True:
            time.sleep(1)
        
    def run(self):
        try:
            self.serverMain()
            return 0
        
        except KeyboardInterrupt:
             self.mprint("%s: Keyboard interrupt."%self.name)
             self.safeExit()
        
        except:
            self.mainQueue.put({'crash' : self.name})
            raise
