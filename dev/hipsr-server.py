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

import time, sys, os, signal
from datetime import datetime
from optparse import OptionParser
import multiprocessing

import numpy as np
import threading, Queue

# Imports from hipsr_core
import hipsr_core.katcp_wrapper as katcp_wrapper
import hipsr_core.katcp_helpers as katcp_helpers
import hipsr_core.config as config
from lib.tcs_server import TcsServer
from lib.plotter_server import PlotterServer
from lib.katcp_server import KatcpServer
from lib.hdf_server import HdfServer

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
    logfile = open(logfile_path, 'a')
    logfile.write(msg + "\n")
    logfile.close()


def getSpectraThreaded(fpgalist, queue):
    """ Starts multiple KATCP servers to collect data from ROACH boards
    
    Spawns multiple threads, with each thread retrieving from a single board.
    A queue is used to block until all threads have completed.   
    """
    # Run threads using queue
    timestamp = time.time()
    for fpga in fpgalist:
        katcpQueue.put([fpga, timestamp])
      
    # Make sure all threads have completed
    katcpQueue.join()


#START OF MAIN:
if __name__ == '__main__':
    
    # Option parsing to allow command line arguments to be parsed
    p = OptionParser()
    p.set_usage('hipsr_server.py [options]')
    p.set_description(__doc__)
    p.add_option("-p", "--projectid", dest="project_id", type="string", default=None,
                 help="Project ID")
    p.add_option("-f", "--flavor", dest="flavor", type="string", default='hipsr_400_8192',
             help="Firmware flavor to run. Defaults to hipsr_400_8192")
    p.add_option("-v", "--verbose", dest="verbose", action='store_true', help="Turn on debugging (verbose mode)")
    p.add_option("-s", "--skip", dest="skip_reprogram", action="store_true",
                 help="Skip reprogramming and reconfiguring ROACH boards.")
    p.add_option("-t", "--test", dest="test", action="store_true",
                 help="Run in test mode, will write to ./test")
    p.add_option("-d", "--dummy", dest="dummy", action="store_true",
                 help="Run in dummy mode -- uses fake roach boards and fake TCS! For debugging.")
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

        if options.project_id:
            project_id = options.project_id
        else:
            project_id = raw_input("Please enter your project ID: ")

        # Start logger
        print "\nStarting logger"
        print "-----------------"

        now = time.gmtime(time.time())
        now_str    = "%d-%02d-%02d_%02d%02d%02d"%(now.tm_year, now.tm_mon, now.tm_mday, now.tm_hour, now.tm_min, now.tm_sec)
        loggerstamp  = "%s_%s.log"%(project_id, now_str)

        if options.test: config.data_dir = './test'
        dir_path    = os.path.join(config.data_dir, project_id)

        if not os.path.exists(dir_path):
            print "Creating directory %s"%dir_path
            os.makedirs(dir_path)

        logfile_path = os.path.join(dir_path, loggerstamp)

        print "Logfile %s created in %s"%(loggerstamp, dir_path)

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
        katcpQueue     = multiprocessing.JoinableQueue()

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
        hdfThread = HdfServer(project_id, dir_path, mainQueue, printQueue, hdfQueue, tcsQueue, flavor=options.flavor)
        hdfThread.daemon = True
        hdfThread.start()

        # Connect to ROACH boards
        mprint("\nConfiguring FPGAs")
        mprint("-----------------\n")
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

        mprint("\nStarting KATCP servers")
        mprint("------------------------")
        katcp_servers = []
        for i in range(len(fpgalist)):
           t = KatcpServer(printQueue, mainQueue, katcpQueue, hdfQueue, plotterQueue, flavor=options.flavor)
           t.daemon = True
           katcp_servers.append(t)
           t.start()
        mprint("%i KATCP server daemons started."%len(fpgalist))

        # Now to start data accumulation while loop
        mprint("\n Starting data capture")
        mprint("------------------------")
        getSpectraThreaded(fpgalist, katcpQueue)
        acc_old, acc_new = fpgalist[0].read_int('o_acc_cnt'), fpgalist[0].read_int('o_acc_cnt')
        allSystemsGo     = True
        crash = False
        hdf_write_enable = False

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
                        if key == 'write_enable':
                            hdf_write_enable = msg[key]
                        if key == 'crash':
                            print "ERROR: %s has crashed"%msg[key]
                            hdfQueue.put({'safe_exit': True})
                            crash = True
                            allSystemsGo = False
                            time.sleep(0.1)
                        if key == 'kill':
                            print "INFO: kill command received, shutting down"
                            hdfQueue.put({'safe_exit': True})
                            allSystemsGo = False
                            break
                        if key == 'flavor':
                            changeFlavor(msg[key])

                if acc_new > acc_old:
                    if hdf_write_enable: wr_en = "[WE]"
                    else: wr_en = "[WD]"
                    timestamp = time.time()
                    now = time.gmtime(timestamp)
                    now_fmt = "%d-%02d-%02d %02d:%02d:%02d"%(now.tm_year, now.tm_mon, now.tm_mday, now.tm_hour, now.tm_min, now.tm_sec)

                    ra, dec = float(tcsThread.scan_pointing["MB01_raj"]), float(tcsThread.scan_pointing["MB01_dcj"])
                    mprint("%s UTC: %s, RA: %02.2f, DEC: %02.2f, Acc: %i"%(wr_en, now_fmt, ra, dec, acc_new))
                    acc_old=acc_new
                    getSpectraThreaded(fpgalist, katcpQueue)

                acc_new = fpgalist[0].read_int('o_acc_cnt')
                time.sleep(0.5)
            except:
                allSystemsGo = False
                crash = True
                mprint("Exception caught!")

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
            for t in katcp_servers:
                t.join(0.1)
            hdfQueue.put({'safe_exit': ''})
            hdfThread.join(0.1)
            tcsThread.join(0.1)
            plotterThread.join(0.1)
            katcpQueue.join()
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
            for t in katcp_servers:
                t.terminate()
            hdfThread.terminate()
            tcsThread.terminate()
            plotterThread.terminate()
            katcpQueue.join()
        except:
            pass
        print("INFO: Threads killed")

    # Make sure it really dies. HDF thread seems to keep this as a zombie
    print "Exiting."
    os.kill(int(os.getpid()),  signal.SIGTERM)
