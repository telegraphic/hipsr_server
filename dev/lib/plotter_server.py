#! /usr/bin/env python
# encoding: utf-8
"""
plotter_server.py
===============

plotter server class for hipsr.
"""

import time, sys, os, socket, random, select, re
import mpserver
try:
    import ujson as json
    USES_UJSON = True
except:
    import json
    USES_UJSON = False

class PlotterServer(mpserver.MpServer):
    """ UDP data server for hipsr-gui plotter """
    def __init__(self, host, port, printQueue, mainQueue, plotterQueue):
        self.name = 'plotter_server'
        self.host = host
        self.port = port
        self.socket     = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        
        self.plotterQueue = plotterQueue
        
        super(PlotterServer, self).__init__(self.name, printQueue, mainQueue)

    def serverMain(self):
        """ Main loop"""
        self.socket.connect((self.host, self.port))
        self.mprint("Plotter : serving UDP packets on %s port %s... "%(self.host, self.port))
        
        while self.server_enabled:
            #self.mprint("Info: plotter server enabled")
            try:
                msg = self.plotterQueue.get()
                #self.mprint(json.loads(msg).keys())
                try:
                    #self.mprint("sending UDP packet to %s"%self.host)
                    self.socket.send(msg)
                    time.sleep(0.01)
                except:
                    #self.mprint("Warning: cannot connect to UDP plotter. Sleeping.")
                    time.sleep(0.01)
                    #raise
            except:
                #time.sleep()
               raise
