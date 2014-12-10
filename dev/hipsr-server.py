#! /usr/bin/env python
# encoding: utf-8
"""
hipsr-mbcal.py
==============

HIPSR wideband spectrometer server script for noise diode calibration. 

This script reprograms and reconfigures the roach boards, creates a data file, and then 
begins collecting data from TCS and the roach boards.

A seperate thread is created for each roach board, so that reading and writing data can be done
in parallel. In addition, there is a thread which acts as a server that listend for TCS messages.
To write to the HDF data file, these threads append an I/O requests to a FIFO (Queue.Queue) which
is constantly checked by yet another thread.

Copyright (c) 2013 The HIPSR collaboration. All rights reserved.
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
from lib.tcs_server import TcsServer
from lib.plotter_server import PlotterServer
#from lib.katcp_server import KatcpServer
from lib.hdf_server import HdfServer
from lib.katcp_server import KatcpServer, KatcpThread
from lib.checkpids import checkpids

try:
    import ujson as json
    USES_UJSON = True
except:
    import json
    USES_UJSON = False

# Python metadata
__version__  = config.__version__
__author__   = config.__author__
__email__    = config.__email__
__license__  = config.__license__
__modified__ = datetime.fromtimestamp(os.path.getmtime(os.path.abspath( __file__ )))

def nbprint():
     """ Non-blocking print from queue"""
     try:
         if not printQueue.empty():
             print printQueue.get(block=False)
             return False # i.e. is_empty = False
         else:
             return True
     except:
         time.sleep(1e-6)


def mprint(msg):
    """ Send a message to the multiprocessing print queue """
    printQueue.put(msg)

def changeFlavor(current_flavor, new_flavor):
    """ Change flavor of firmware """

    if current_flavor != new_flavor:
        mprint("Changing from %s to %s"%(current_flavor, new_flavor))
        mprint("Reprogramming...")
        katcp_helpers.reprogram(new_flavor)
        katcp_helpers.reconfigure(new_flavor)
    return new_flavor

#START OF MAIN:
if __name__ == '__main__':

    # Check if there is another server script running
    checkpids()

    # Option parsing to allow command line arguments to be parsed
    p = OptionParser()
    p.set_usage('hipsr_server.py [options]')
    p.set_description(__doc__)
    p.add_option("-f", "--flavor", dest="flavor", type="string", default='hipsr_400_8192',
             help="Firmware flavor to run. Defaults to hipsr_400_8192")
    p.add_option("-v", "--verbose", dest="verbose", action='store_true', help="Turn on debugging (verbose mode)")
    p.add_option("-s", "--skip", dest="skip_reprogram", action="store_true",
                 help="Skip reprogramming and reconfiguring ROACH boards.")
    p.add_option("-t", "--test", dest="test", action="store_true",
                 help="Run in test mode, will write to ./test, and expects messages from the python dummy_TCS script.")
    p.add_option("-d", "--dummy", dest="dummy", action="store_true",
                 help="Run in dummy mode -- uses fake roach boards. For debugging only.")
    (options, args) = p.parse_args(sys.argv[1:])

    try:
        print "\nHIPSR SERVER"
        print "------------"
        print "Version:       %s"%__version__
        print "Last modified: %s"%__modified__
        print "Report bugs to %s\n"%__email__

        if options.dummy:
            # Overwrite katcp_wrapper with dummy wrapper for debugging
            print "WARNING: STARTING IN DUMMY MODE!\n NO REAL DATA WILL BE TAKEN."
            import lib.dummy_katcp_wrapper as katcp_wrapper
            katcp_helpers.katcp_wrapper = katcp_wrapper
            KatcpServer.katcp_wrapper   = katcp_wrapper



        now = time.gmtime(time.time())
        now_str    = "%d-%02d-%02d_%02d%02d%02d"%(now.tm_year, now.tm_mon, now.tm_mday, now.tm_hour, now.tm_min, now.tm_sec)

        if options.test: config.data_dir = './test'
        dir_path    = os.path.join(config.data_dir, 'data')

        if not os.path.exists(dir_path):
            print "Creating directory %s"%dir_path
            os.makedirs(dir_path)


        print "\nConfiguration"
        print "-------------"
        print "TCS host:        %15s    port: %5s"%(config.tcs_server,     config.tcs_port)
        print "Plotter host:    %15s    port: %5s"%(config.plotter_host, config.plotter_port)
        print "KATCP port:       %s"%config.katcp_port
        print "FPGA firmware:    %s"%config.fpga_config[options.flavor]["firmware"]

        # Configuration parameters
        boffile      = config.fpga_config[options.flavor]["firmware"]
        reprogram    = config.reprogram
        reconfigure  = config.reconfigure
        plotter_host = config.plotter_host
        plotter_port = config.plotter_port
        tcs_server   = config.tcs_server
        tcs_port     = config.tcs_port
        roachlist    = config.roachlist
        katcp_port   = config.katcp_port

        # Setup communiction queues
        printQueue     = multiprocessing.Queue()
        mainQueue      = multiprocessing.Queue()
        tcsQueue       = multiprocessing.Queue()
        hdfQueue       = multiprocessing.Queue()
        plotterQueue   = multiprocessing.Queue()
        katcpQueue     = Queue.Queue()
        

        # From this point, using print queue only
        mprint("\nStarting TCS server")
        mprint("-------------------"  )

        if options.test:
            tcsThread = TcsServer('localhost', 8080, printQueue, mainQueue, hdfQueue, plotterQueue, debug=options.verbose)
        else:
            server, port = config.tcs_server, config.tcs_port,
            tcsThread = TcsServer(server, port, printQueue, mainQueue, hdfQueue, plotterQueue, debug=options.verbose)
        tcsThread.daemon = True
        tcsThread.start()
        tcsThread.send_udp = True
        if options.verbose:
            tcsThread.debug = True
        if not options.dummy: time.sleep(0.1)


        mprint("\nStarting Plotter server")
        mprint("-----------------------"  )
        if options.test:
            plotterThread = PlotterServer('localhost', 59012, printQueue, mainQueue, plotterQueue)
        else:
            plotterThread = PlotterServer(config.plotter_host, config.plotter_port, printQueue, mainQueue, plotterQueue)

        plotterThread.daemon = True
        plotterThread.start()
        if options.verbose:
            plotterThread.debug = True
        if not options.dummy:
            time.sleep(0.1)

        mprint("\nStarting HDF server")
        mprint("--------------------" )
        hdfThread = HdfServer(dir_path, mainQueue, printQueue, hdfQueue, tcsQueue, flavor=options.flavor)
        hdfThread.daemon = True
        hdfThread.start()
            
            
        # Connect to ROACH boards
        mprint("\nConfiguring FPGAs")
        mprint("-----------------\n")
        
        is_empty = False
        while not is_empty:
            is_empty = nbprint()
            time.sleep(1e-6)
        
        fpgalist  = [katcp_wrapper.FpgaClient(roach, config.katcp_port, timeout=10) for roach in config.roachlist]
        if not options.dummy:
            time.sleep(0.5)
        if not options.skip_reprogram:
            katcp_helpers.reprogram(options.flavor)
            katcp_helpers.reconfigure(options.flavor)
        else:
            print "skipping reprogramming..."
            print "skipping reconfiguration.."
        if not options.dummy:
            time.sleep(1)
            for fpga in fpgalist:
                fpga.stop()
            
        
        mprint("\nStarting KATCP servers")
        mprint("------------------------")
        katcpServer = KatcpServer(printQueue, mainQueue, hdfQueue, katcpQueue, plotterQueue, flavor=options.flavor, dummyMode=options.dummy)
        #katcpThread = KatcpServer(printQueue, mainQueue,  hdfQueue, katcpQueue, plotterQueue, 
        katcpServer.daemon = True
        katcpServer.start()
        #mprint("%i KATCP server daemons started."%len(fpgalist))
        
        # Now to start data accumulation while loop
        mprint("\n Starting data capture")
        mprint("------------------------")
        #getSpectraThreaded(fpgalist, katcpQueue)
        acc_old, acc_new = 0, 0
        current_ra, current_dec = 0, 0
        allSystemsGo     = True
        crash = False
        hdf_write_enable = False
        current_flavor = options.flavor
        #print "HERE %s"%options.flavor
        
        while allSystemsGo:

            try:
                # Print all items in print queue
                is_empty = False
                while not is_empty:
                    is_empty = nbprint()
                    time.sleep(1e-6)
                
                while not mainQueue.empty():
                    msg = mainQueue.get()
                    #print "HERE: %s"%msg
                    for key in msg.keys():
                        if options.verbose:
                            print key, msg[key]
                        if key == 'write_enable':
                            hdf_write_enable = msg[key]
                        if key == 'crash':
                            print "ERROR: %s has crashed"%msg[key]
                            hdfQueue.put({'safe_exit': True})
                            crash = True
                            allSystemsGo = False
                            time.sleep(0.01)
                        if key == 'kill':
                            print "INFO: kill command received, shutting down"
                            hdfQueue.put({'safe_exit': True})
                            allSystemsGo = False
                            break
                        if key == 'update_ra':
                            current_ra = msg[key]
                        if key == 'update_dec':
                            current_dec = msg[key]
                        if key == 'flavor':
                            if current_flavor != msg[key]:
                                katcpQueue.put({'change_flavor' : msg[key]})
                                hdfQueue.put({'change_flavor' : msg[key]})
                                current_flavor = msg[key]
                                #current_flavor = changeFlavor(current_flavor, msg[key])
                        if key == 'acc_new':
                            acc_new = msg[key]

                if acc_new > acc_old:
                    if hdf_write_enable:
                        wr_en = "[WE]"
                    else:
                        wr_en = "[WD]"

                    timestamp = time.time()
                    now = time.gmtime(timestamp)
                    now_fmt = "%d-%02d-%02d %02d:%02d:%02d"%(now.tm_year, now.tm_mon, now.tm_mday, now.tm_hour, now.tm_min, now.tm_sec)

                    ra, dec = float(current_ra), float(current_dec)
                    print("%s UTC: %s, RA: %02.2f, DEC: %02.2f, Acc: %i"%(wr_en, now_fmt, ra, dec, acc_new))
                    acc_old = acc_new
                    katcpQueue.put({'new_acc': True})
                    katcpQueue.put({'timestamp': timestamp})
                   
                katcpQueue.put({'check_acc': acc_old})
                time.sleep(0.5)
            except:
                allSystemsGo = False
                crash = True
                mprint("Exception caught!")
                raise

        if not allSystemsGo:
            if crash:
                mprint("ERROR: One or more server threads has crashed! This script will now close.")
            else:
                mprint("INFO: Shutting down.")
            # Print all items in print queue
            try:
                hdfQueue.put({'safe_exit': ''})
                time.sleep(0.1)
            except:
                raise
            is_empty = False
            #mprint("INFO: Flushing print queue")
            while not is_empty:
                is_empty = nbprint()
                time.sleep(1e-6)
            hdfQueue.put({'safe_exit': ''})
            hdfThread.join(0.1)
            tcsThread.join(0.1)
            plotterThread.join(0.1)
            #katcpThread.join(0.1)
            #print("INFO: Threads killed")

    except KeyboardInterrupt:
        allSystemsGo = False
        mprint("Keyboard interrupt caught. Closing threads...")
        hdfQueue.put({'safe_exit': ''})
        time.sleep(0.1)
        is_empty = False
        while not is_empty:
            is_empty = nbprint()
            time.sleep(1e-6)
        try:
            katcp.terminate()
            hdfThread.terminate()
            tcsThread.terminate()
            plotterThread.terminate()
            #katcpThread.join()
        except:
            pass
        print("INFO: Threads killed")

    # Make sure it really dies. HDF thread seems to keep this as a zombie
    print "Exiting."
    os.kill(int(os.getpid()), signal.SIGINT)
    os.kill(int(os.getpid()), signal.SIGTERM)
    os.kill(int(os.getpid()), signal.SIGKILL)
