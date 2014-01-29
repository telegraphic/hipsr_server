#! /usr/bin/env python
# encoding: utf-8
"""
hipsr-digitizer-rms.py
======================

Compute RMS of digitizers.


Copyright (c) 2013 The HIPSR collaboration. All rights reserved.
"""

import time, os, sys
from datetime import datetime
import numpy as np

from hipsr_core import katcp_helpers
import hipsr_core.config as config
import hipsr_core.katcp_wrapper as katcp_wrapper

from lib.colorterm import colorterm as cprint


# Python metadata
__version__  = config.__version__
__author__   = config.__author__
__email__    = config.__email__
__license__  = config.__license__
__modified__ = datetime.fromtimestamp(os.path.getmtime(os.path.abspath( __file__ )))


def color_code(val):
    """ Color coding RMS values """

    if val < 2:
        return cprint.red("%2.2f"%val)
    elif val < 4:
        return cprint.yellow("%2.2f"%val)
    elif val > 20:
        return cprint.red("%2.2f"%val)
    elif val > 10:
        return cprint.yellow("%2.2f"%val)
    else
        return "%2.2f"%val

if __name__ == '__main__':

    try:
        # Start in expert mode if any flag is received
        mode = sys.argv[1]
        expert_mode = True
    except IndexError:
        expert_mode = False

    roachlist    = config.roachlist
    katcp_port   = config.katcp_port

    fpgalist = [katcp_wrapper.FpgaClient(roach, katcp_port) for roach in roachlist]
    time.sleep(0.1)
    
    # Make sure ROACH boards are programmed
    try:
        fpgalist[0].read_int("mux_sel")
    except:
        print "Please wait, reprogramming ROACH boards..."
        katcp_helpers.reprogram("hipsr_400_8192")
        katcp_helpers.reconfigure("hipsr_400_8192")
        time.sleep(0.5)
        print "OK"
    
    # Read RMS levels  
    print cprint.green("\nROACH RMS LEVELS:")
    print cprint.green("-----------------\n\n")

    print "    --------------------------"
    print "    | BEAM |  POL A |  POL B |"
    print "    |------------------------|"

    to_print = []
    for fpga in fpgalist:        
        levels = katcp_helpers.getSpectrum(fpga, 'rms_levels')
        rms_x, rms_y = color_code(levels['rms_x']), color_code(levels['rms_y'])
        to_print.append("    |  %02i  |  %s |  %s |"%(roachlist[fpga.host], rms_x, rms_y)

    to_print.sort()

    for line in to_print:
        print line

    if expert_mode:
        # Read NAR levels
        print cprint.green("\nROACH NAR LEVELS:")
        print cprint.green("-----------------")
        for fpga in fpgalist:
            levels = katcp_helpers.getSpectrum(fpga, 'rms_levels')
            n_bits_x, n_bits_y = np.log2(levels['nar_x_on']), np.log2(levels['nar_y_on'])
            if n_bits_x > 30 or n_bits_y > 30:
                print cprint.red( "%s NAR power (bits): %2.2f (A), %2.2f (B)"%(roachlist[fpga.host], n_bits_x, n_bits_y))
            else:
                print "%s NAR power (bits): %2.2f (A), %2.2f (B)"%(roachlist[fpga.host], n_bits_x, n_bits_y)

    # Close connections
    for fpga in fpgalist:
        fpga.stop()
